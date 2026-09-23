"""Instance settings and the project they open.

Two files, two lifetimes:

``graetl.toml`` - **the instance**
    Belongs to the installation: which address to serve on, where the code
    editor comes from, which Python spawns runners, and default runtime knobs.
    It follows the machine, not the data.

``project.toml`` - **the project** (see :mod:`graetl.project`)
    Belongs to the warehouse: its identity and logo, its target database, its
    file root, its pipelines. It is committed to the project's own git
    repository and moves between machines with the data.

One instance opens exactly one project. :class:`Settings` is the two of them
together, and is what the server, the CLI and the runner are handed. A project
can be absent - that is the state the console's project picker exists for - so
anything that needs one goes through :meth:`Settings.require_project`.

Resolution order for *which* project: an explicit path, then
``GRAETL_PROJECT``, then ``[project] path`` in ``graetl.toml``, then a project
folder found by walking up from the working directory.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from graetl.project import (
    PROJECT_FILENAME,
    Project,
    ProjectError,
    find_project,
    read_toml,
)
from graetl.store.db import Database, Target, connect

CONFIG_FILENAME = "graetl.toml"
PIPELINE_CONFIG_FILENAME = "pipeline.toml"
PIPELINE_ENTRY_FILENAME = "pipeline.py"

# Modules: any file named "<name>.module.py" anywhere under the pipeline folder
# is loaded automatically and defines exactly one module. Plain .py files next to
# it are helpers - imported by the module that needs them, never auto-loaded.
MODULES_DIRNAME = "modules"
MODULE_FILE_SUFFIX = ".module.py"

# Node libraries: any file named "<name>.nodes.py" is loaded automatically and
# every public top-level function in it becomes a node, with no decorator. The
# @node decorator only overrides the defaults (or opts a helper out).
NODES_FILE_SUFFIX = ".nodes.py"
MODULE_CONFIG_SUFFIX = ".module.toml"
MODULE_CONFIG_FILENAME = "module.toml"  # folder-wide defaults

# Folders that hold runtime state, never pipeline code.
RUNTIME_DIRNAMES = frozenset({"data", "logs", "profiles", ".cache", "__pycache__"})

GRAPH_SUFFIX = ".graph"
GRAPHLIB_SUFFIX = ".graphlib"

# Code editor. The UI prefers a copy vendored into the install (offline, and the
# only option on an air-gapped machine); this is the fallback it tries next.
# `python -m graetl.cli vendor-monaco` downloads that copy.
MONACO_VERSION = "0.52.2"
MONACO_CDN = f"https://cdn.jsdelivr.net/npm/monaco-editor@{MONACO_VERSION}/min/vs"


class NoProjectOpen(RuntimeError):
    """An operation needed a project and this instance has none open."""


def _find_instance_root(start: Path) -> Path:
    cur = start.resolve()
    for candidate in (cur, *cur.parents):
        if (candidate / CONFIG_FILENAME).exists():
            return candidate
        if (candidate / "backend" / "graetl").is_dir() and (candidate / "pyproject.toml").exists():
            return candidate
    return cur


@dataclass(slots=True)
class Settings:
    root: Path
    project: Project | None = None
    host: str = "127.0.0.1"
    port: int = 8777
    log_retention_runs: int = 50
    console_buffer_lines: int = 2000
    stop_grace_seconds: float = 20.0
    heartbeat_seconds: float = 3.0
    control_poll_seconds: float = 0.5
    #: Default number of modules of one execution layer to run at once.
    parallel_modules: int = 1
    #: Entities claimed per batch inside a module.
    entity_batch_size: int = 500
    #: Retries when a parallel module loses a write race.
    lock_retries: int = 5
    #: Entries kept per cached pipeline function (see graetl.sdk.cached).
    cache_size: int = 4096
    python_executable: str | None = None
    #: Where the browser loads the Monaco editor from when no copy is vendored
    #: into ``static/vendor/vs``. Set it to "" to force the offline fallback.
    monaco_url: str = MONACO_CDN
    raw: dict[str, Any] = field(default_factory=dict)

    # --------------------------------------------------------------- project

    @property
    def has_project(self) -> bool:
        return self.project is not None

    def require_project(self) -> Project:
        if self.project is None:
            raise NoProjectOpen(
                "no GraETL project is open. Pass one to `graetl serve <project>`, "
                f"set GRAETL_PROJECT, or run from inside a folder with a {PROJECT_FILENAME}."
            )
        return self.project

    @property
    def target(self) -> Target:
        return self.require_project().target

    @property
    def pipelines_dir(self) -> Path:
        return self.require_project().pipelines_dir

    @property
    def files_root(self) -> Path:
        return self.require_project().files_root

    def pipeline_dir(self, pipeline_id: str) -> Path:
        return self.pipelines_dir / pipeline_id

    def logs_dir(self, pipeline_id: str) -> Path:
        return self.pipeline_dir(pipeline_id) / "logs"

    def profiles_dir(self, pipeline_id: str) -> Path:
        return self.pipeline_dir(pipeline_id) / "profiles"

    def run_log_path(self, pipeline_id: str, run_id: int) -> Path:
        return self.logs_dir(pipeline_id) / f"run_{run_id:06d}.jsonl"

    def ensure_dirs(self) -> None:
        self.require_project().ensure_dirs()

    # -------------------------------------------------------------- database

    def open_database(self) -> Database:
        """A fresh connection to the project's target. The caller closes it."""
        return connect(self.target)

    def core_store(self) -> Any:
        from graetl.store.core import CoreStore

        return CoreStore(self.open_database())

    def state_store(self, pipeline_id: str) -> Any:
        from graetl.store.state import StateStore

        return StateStore(self.open_database(), pipeline_id)


def load_settings(
    root: str | os.PathLike[str] | None = None,
    *,
    project: str | os.PathLike[str] | None = None,
    require_project: bool = False,
) -> Settings:
    base = Path(root) if root else _find_instance_root(Path(os.environ.get("GRAETL_ROOT", ".")))
    base = base.resolve()
    data = read_toml(base / CONFIG_FILENAME)

    server = _section(data, "server")
    runtime = dict(_section(data, "runtime"))
    ui = _section(data, "ui")

    opened = _resolve_project(base, data, project)
    if opened is None and require_project:
        raise NoProjectOpen(
            "no GraETL project given. Pass a folder to `graetl serve <project>`, set "
            f"GRAETL_PROJECT, or create one with `graetl new-project <folder>`."
        )

    raw: dict[str, Any] = dict(data)
    if opened is not None:
        # A project overrides the instance for anything it chooses to set: the
        # knobs that matter (parallelism, batch size, cache) are properties of
        # the warehouse, not of the machine that happens to be running it.
        runtime.update(_section(opened.raw, "runtime"))
        for key, value in opened.raw.items():
            if key in ("runtime", "project", "target", "files", "paths"):
                continue
            raw[key] = value
        raw["runtime"] = runtime

    return Settings(
        root=base,
        project=opened,
        host=str(os.environ.get("GRAETL_HOST") or server.get("host", "127.0.0.1")),
        port=int(os.environ.get("GRAETL_PORT") or server.get("port", 8777)),
        log_retention_runs=int(runtime.get("log_retention_runs", 50)),
        console_buffer_lines=int(runtime.get("console_buffer_lines", 2000)),
        stop_grace_seconds=float(runtime.get("stop_grace_seconds", 20.0)),
        heartbeat_seconds=float(runtime.get("heartbeat_seconds", 3.0)),
        control_poll_seconds=float(runtime.get("control_poll_seconds", 0.5)),
        parallel_modules=int(
            os.environ.get("GRAETL_PARALLEL_MODULES") or runtime.get("parallel_modules", 1)
        ),
        entity_batch_size=int(runtime.get("entity_batch_size", 500)),
        lock_retries=int(runtime.get("lock_retries", 5)),
        cache_size=int(runtime.get("cache_size", 4096)),
        python_executable=runtime.get("python_executable"),
        monaco_url=str(
            os.environ.get("GRAETL_MONACO_URL")
            if os.environ.get("GRAETL_MONACO_URL") is not None
            else ui.get("monaco_url", MONACO_CDN)
        ).rstrip("/"),
        raw=raw,
    )


def _resolve_project(
    base: Path, data: dict[str, Any], explicit: str | os.PathLike[str] | None
) -> Project | None:
    candidate: Path | None = None
    if explicit:
        candidate = Path(explicit)
    elif os.environ.get("GRAETL_PROJECT"):
        candidate = Path(os.environ["GRAETL_PROJECT"])
    else:
        configured = _section(data, "project").get("path")
        if configured:
            candidate = Path(str(configured))
            if not candidate.is_absolute():
                candidate = base / candidate
        else:
            # An instance root that is itself a project opens it; otherwise
            # walk up from the working directory, the way git finds a repo.
            candidate = find_project(base) or find_project(
                os.environ.get("GRAETL_ROOT") or "."
            )
    if candidate is None:
        return None
    return Project.load(candidate)


def _section(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    return value if isinstance(value, dict) else {}


def load_pipeline_config(pipeline_dir: Path) -> dict[str, Any]:
    """Read ``pipeline.toml`` next to the entry file (may be absent)."""
    return read_toml(pipeline_dir / PIPELINE_CONFIG_FILENAME)


__all__ = [
    "CONFIG_FILENAME",
    "GRAPHLIB_SUFFIX",
    "GRAPH_SUFFIX",
    "MODULES_DIRNAME",
    "MODULE_CONFIG_FILENAME",
    "MODULE_CONFIG_SUFFIX",
    "MODULE_FILE_SUFFIX",
    "MONACO_CDN",
    "MONACO_VERSION",
    "NODES_FILE_SUFFIX",
    "PIPELINE_CONFIG_FILENAME",
    "PIPELINE_ENTRY_FILENAME",
    "RUNTIME_DIRNAMES",
    "NoProjectOpen",
    "Project",
    "ProjectError",
    "Settings",
    "load_pipeline_config",
    "load_settings",
    "read_toml",
]
