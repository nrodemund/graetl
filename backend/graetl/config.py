"""Project layout + settings.

The whole portable state of a GraETL installation lives in the ``pipelines``
folder: the internal database (``pipelines/etl.db``) plus one folder per
pipeline containing its code, configuration, data, logs and entity state.
Copying that folder copies the installation.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

CONFIG_FILENAME = "graetl.toml"
PIPELINE_CONFIG_FILENAME = "pipeline.toml"
PIPELINE_ENTRY_FILENAME = "pipeline.py"
CORE_DB_FILENAME = "etl.db"
STATE_DB_FILENAME = "state.db"

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

# Visual node-flow assets (compiled and executed in a later version; for now they
# are discovered and listed so the UI and the definition know about them).
GRAPH_SUFFIX = ".graph"
GRAPHLIB_SUFFIX = ".graphlib"

# Code editor. The UI prefers a copy vendored into the install (offline, and the
# only option on an air-gapped machine); this is the fallback it tries next.
# `python -m graetl.cli vendor-monaco` downloads that copy.
MONACO_VERSION = "0.52.2"
MONACO_CDN = f"https://cdn.jsdelivr.net/npm/monaco-editor@{MONACO_VERSION}/min/vs"


def _find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for candidate in (cur, *cur.parents):
        if (candidate / CONFIG_FILENAME).exists():
            return candidate
        if (candidate / "pipelines").is_dir() and (candidate / "pyproject.toml").exists():
            return candidate
    return cur


@dataclass(slots=True)
class Settings:
    root: Path
    pipelines_dir: Path
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

    @property
    def core_db_path(self) -> Path:
        return self.pipelines_dir / CORE_DB_FILENAME

    def pipeline_dir(self, pipeline_id: str) -> Path:
        return self.pipelines_dir / pipeline_id

    def state_db_path(self, pipeline_id: str) -> Path:
        return self.pipeline_dir(pipeline_id) / STATE_DB_FILENAME

    def logs_dir(self, pipeline_id: str) -> Path:
        return self.pipeline_dir(pipeline_id) / "logs"

    def profiles_dir(self, pipeline_id: str) -> Path:
        return self.pipeline_dir(pipeline_id) / "profiles"

    def run_log_path(self, pipeline_id: str, run_id: int) -> Path:
        return self.logs_dir(pipeline_id) / f"run_{run_id:06d}.jsonl"

    def ensure_dirs(self) -> None:
        self.pipelines_dir.mkdir(parents=True, exist_ok=True)


def load_settings(root: str | os.PathLike[str] | None = None) -> Settings:
    base = Path(root) if root else _find_project_root(Path(os.environ.get("GRAETL_ROOT", ".")))
    base = base.resolve()
    data: dict[str, Any] = {}
    cfg_file = base / CONFIG_FILENAME
    if cfg_file.exists():
        with cfg_file.open("rb") as fh:
            data = tomllib.load(fh)

    server = data.get("server", {}) if isinstance(data.get("server"), dict) else {}
    runtime = data.get("runtime", {}) if isinstance(data.get("runtime"), dict) else {}
    paths = data.get("paths", {}) if isinstance(data.get("paths"), dict) else {}
    ui = data.get("ui", {}) if isinstance(data.get("ui"), dict) else {}

    pipelines_dir = Path(
        os.environ.get("GRAETL_PIPELINES_DIR") or paths.get("pipelines") or (base / "pipelines")
    )
    if not pipelines_dir.is_absolute():
        pipelines_dir = (base / pipelines_dir).resolve()

    settings = Settings(
        root=base,
        pipelines_dir=pipelines_dir,
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
        raw=data,
    )
    return settings


def load_pipeline_config(pipeline_dir: Path) -> dict[str, Any]:
    """Read ``pipeline.toml`` next to the entry file (may be absent)."""
    return read_toml(pipeline_dir / PIPELINE_CONFIG_FILENAME)


def read_toml(path: Path) -> dict[str, Any]:
    """Read a TOML file, returning ``{}`` when it does not exist."""
    if not path.exists():
        return {}
    with path.open("rb") as fh:
        return tomllib.load(fh)
