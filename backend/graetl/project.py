"""A GraETL **project**: one data warehouse and the pipelines that fill it.

GraETL the tool and the data it manages are separate things, and they belong in
separate repositories. The tool is installed; a project is *opened*. One running
instance opens exactly one project - there is no project switcher, because a
run, a lock and an entity id only mean anything relative to one warehouse.

Layout::

    <project>/
        project.toml          identity, target database, file root, runtime
        logo.png              shown in the console header
        .gitignore            written on create: files/, *.db, logs/, ...
        .env                  optional, git-ignored: secrets for ${VAR} in the DSN
        files/                target file path - DICOM and friends land here
        pipelines/<id>/       exactly as before

The project folder is meant to **be a git repository**. Everything in it is
either source (pipeline code, graphs, configuration) or explicitly ignored
runtime state, so ``git init`` in a fresh project is immediately sensible.

``project.toml``::

    schema = 1

    [project]
    name = "sicdb"
    title = "SICdb Warehouse"
    logo = "logo.png"

    [target]
    system = "postgres"          # or "sqlite"
    dsn = "postgresql://etl:${PGPASSWORD}@db.internal:5432/warehouse"
    schema = "graetl"            # where GraETL's own tables go
    # system = "sqlite" uses:
    # path = "warehouse.db"

    [files]
    root = "files"               # absolute paths are allowed

Secrets never belong in a tracked file, so any ``${VAR}`` in the DSN is
expanded from the environment, falling back to a git-ignored ``.env`` beside
``project.toml``. ``GRAETL_TARGET_DSN`` overrides the DSN outright.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from graetl.store.db import Target

PROJECT_FILENAME = "project.toml"
PROJECT_SCHEMA = 1
DEFAULT_SQLITE_TARGET = "warehouse.db"
DEFAULT_FILES_DIRNAME = "files"
DEFAULT_PIPELINES_DIRNAME = "pipelines"
LOGO_NAMES = ("logo.png", "logo.svg", "logo.jpg", "logo.jpeg", "logo.webp")

_VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class ProjectError(RuntimeError):
    """A project folder is missing, malformed, or configured impossibly."""


# ------------------------------------------------------------------- the model


@dataclass(slots=True)
class Project:
    root: Path
    name: str
    title: str = ""
    description: str = ""
    logo: str | None = None
    target: Target = field(default_factory=Target)
    files_root: Path = field(default_factory=Path)
    pipelines_dir: Path = field(default_factory=Path)
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def config_path(self) -> Path:
        return self.root / PROJECT_FILENAME

    @property
    def logo_path(self) -> Path | None:
        if not self.logo:
            return None
        candidate = self.root / self.logo
        return candidate if candidate.exists() else None

    def pipeline_dir(self, pipeline_id: str) -> Path:
        return self.pipelines_dir / pipeline_id

    def ensure_dirs(self) -> None:
        self.pipelines_dir.mkdir(parents=True, exist_ok=True)
        self.files_root.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict[str, Any]:
        """What the console needs to render the project header."""
        return {
            "name": self.name,
            "title": self.title or self.name,
            "description": self.description,
            "root": str(self.root),
            "logo": bool(self.logo_path),
            "files_root": str(self.files_root),
            "pipelines_dir": str(self.pipelines_dir),
            "target": {
                "system": self.target.system,
                "describe": self.target.describe(),
                "schema": self.target.schema if self.target.is_postgres else None,
            },
        }

    # ------------------------------------------------------------------ load

    @classmethod
    def load(cls, root: str | os.PathLike[str]) -> Project:
        root = Path(root).expanduser().resolve()
        config = root / PROJECT_FILENAME
        if not config.exists():
            raise ProjectError(f"{root} is not a GraETL project ({PROJECT_FILENAME} is missing)")
        data = read_toml(config)
        schema = int(data.get("schema", PROJECT_SCHEMA) or PROJECT_SCHEMA)
        if schema > PROJECT_SCHEMA:
            raise ProjectError(
                f"{config} was written by a newer GraETL (project schema {schema}, "
                f"this one understands {PROJECT_SCHEMA})"
            )
        meta = _section(data, "project")
        env = _read_env_file(root / ".env")

        name = str(meta.get("name") or root.name).strip()
        logo = meta.get("logo")
        if not logo:
            logo = next((n for n in LOGO_NAMES if (root / n).exists()), None)

        paths = _section(data, "paths")
        pipelines_dir = _resolve(root, paths.get("pipelines"), DEFAULT_PIPELINES_DIRNAME)
        files_root = _resolve(
            root, _section(data, "files").get("root"), DEFAULT_FILES_DIRNAME
        )

        return cls(
            root=root,
            name=name,
            title=str(meta.get("title") or name),
            description=str(meta.get("description") or ""),
            logo=str(logo) if logo else None,
            target=_load_target(root, _section(data, "target"), env),
            files_root=files_root,
            pipelines_dir=pipelines_dir,
            raw=data,
        )

    # ---------------------------------------------------------------- create

    @classmethod
    def create(
        cls,
        root: str | os.PathLike[str],
        *,
        name: str | None = None,
        title: str = "",
        description: str = "",
        system: str = "sqlite",
        dsn: str = "",
        schema: str = "graetl",
        sqlite_path: str = DEFAULT_SQLITE_TARGET,
    ) -> Project:
        """Write a new project folder. Refuses to overwrite an existing one."""
        root = Path(root).expanduser().resolve()
        if (root / PROJECT_FILENAME).exists():
            raise ProjectError(f"{root} is already a GraETL project")
        if system not in ("sqlite", "postgres"):
            raise ProjectError(f"unsupported target system {system!r} - use sqlite or postgres")
        if system == "postgres" and not dsn:
            raise ProjectError("a postgres project needs a dsn")
        name = (name or root.name).strip()
        root.mkdir(parents=True, exist_ok=True)

        target_block = (
            f'system = "postgres"\ndsn = "{dsn}"\nschema = "{schema}"'
            if system == "postgres"
            else f'system = "sqlite"\npath = "{sqlite_path}"'
        )
        (root / PROJECT_FILENAME).write_text(
            PROJECT_TEMPLATE.format(
                schema=PROJECT_SCHEMA,
                name=name,
                title=_toml_escape(title or name),
                description=_toml_escape(description),
                target=target_block,
                files=DEFAULT_FILES_DIRNAME,
            ),
            encoding="utf-8",
        )
        gitignore = root / ".gitignore"
        if not gitignore.exists():
            gitignore.write_text(GITIGNORE_TEMPLATE, encoding="utf-8")
        project = cls.load(root)
        project.ensure_dirs()
        return project


# ------------------------------------------------------------------- the files


def resolve_file(project: Project, *parts: str) -> Path:
    """Resolve a path under the project's file root, refusing to escape it.

    Modules write files - DICOM series, exports, attachments - and the name
    usually comes from the source data. ``..`` in a patient identifier must not
    be able to write outside the warehouse, so this is the only sanctioned way
    to turn one into a path.
    """
    base = project.files_root.resolve()
    candidate = base.joinpath(*parts).resolve()
    if candidate != base and base not in candidate.parents:
        raise ProjectError(f"{Path(*parts)} resolves outside the project file root")
    return candidate


# ------------------------------------------------------------------ discovery


def find_project(start: str | os.PathLike[str] | None = None) -> Path | None:
    """Walk upwards looking for a project folder, like git does for ``.git``."""
    cur = Path(start or ".").expanduser().resolve()
    for candidate in (cur, *cur.parents):
        if (candidate / PROJECT_FILENAME).exists():
            return candidate
    return None


def user_state_dir() -> Path:
    """Where the recent-projects list lives - per user, not per project."""
    if os.name == "nt":  # pragma: no cover - exercised on Windows
        base = Path(os.environ.get("APPDATA") or Path.home() / "AppData" / "Roaming")
        return base / "GraETL"
    return Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local" / "state") / "graetl"


def recent_projects() -> list[dict[str, Any]]:
    """Recently opened projects, most recent first, skipping ones now gone."""
    path = user_state_dir() / "recent.json"
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if not isinstance(entries, list):
        return []
    out: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict) or not entry.get("root"):
            continue
        if (Path(entry["root"]) / PROJECT_FILENAME).exists():
            out.append(entry)
    return out


def remember_project(project: Project, limit: int = 12) -> None:
    """Put a project at the top of the recent list. Best-effort."""
    entry = {
        "root": str(project.root),
        "name": project.name,
        "title": project.title,
        "target": project.target.system,
    }
    entries = [e for e in recent_projects() if e.get("root") != entry["root"]]
    entries.insert(0, entry)
    path = user_state_dir() / "recent.json"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(entries[:limit], indent=2), encoding="utf-8")
    except OSError:  # pragma: no cover - a missing recent list is harmless
        pass


def forget_project(root: str | os.PathLike[str]) -> None:
    entries = [e for e in recent_projects() if e.get("root") != str(root)]
    path = user_state_dir() / "recent.json"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(entries, indent=2), encoding="utf-8")
    except OSError:  # pragma: no cover
        pass


# ------------------------------------------------------------------- internals


def _section(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    return value if isinstance(value, dict) else {}


def _resolve(root: Path, value: Any, default: str) -> Path:
    path = Path(str(value)) if value else Path(default)
    return path if path.is_absolute() else (root / path).resolve()


def _load_target(root: Path, block: dict[str, Any], env: dict[str, str]) -> Target:
    system = str(block.get("system") or "sqlite").strip().lower()
    if system in ("postgres", "postgresql", "pg"):
        dsn = os.environ.get("GRAETL_TARGET_DSN") or str(block.get("dsn") or "")
        dsn = _expand(dsn, env)
        if not dsn:
            raise ProjectError(
                "the target is postgres but no dsn is configured "
                "(set [target] dsn in project.toml or GRAETL_TARGET_DSN)"
            )
        return Target(system="postgres", dsn=dsn, schema=str(block.get("schema") or "graetl"))
    if system != "sqlite":
        raise ProjectError(
            f"unsupported target system {system!r} - GraETL supports sqlite and postgres"
        )
    return Target(system="sqlite", path=_resolve(root, block.get("path"), DEFAULT_SQLITE_TARGET))


def _expand(text: str, env: dict[str, str]) -> str:
    """Replace ``${VAR}`` from the environment, then ``.env``.

    An unset variable is left as-is rather than blanked, so the resulting
    connection error names the placeholder instead of failing obscurely.
    """

    def swap(match: re.Match[str]) -> str:
        key = match.group(1)
        return os.environ.get(key) or env.get(key) or match.group(0)

    return _VAR.sub(swap, text)


def _read_env_file(path: Path) -> dict[str, str]:
    """A deliberately small ``.env`` reader: ``KEY=value``, ``#`` comments."""
    out: dict[str, str] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        out[key.strip()] = value
    return out


def _toml_escape(text: str) -> str:
    return str(text).replace("\\", "\\\\").replace('"', '\\"')


def read_toml(path: Path) -> dict[str, Any]:
    try:  # Python 3.11+
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover
        import tomli as tomllib  # type: ignore[no-redef]
    if not path.exists():
        return {}
    with path.open("rb") as fh:
        return tomllib.load(fh)


PROJECT_TEMPLATE = '''\
# GraETL project - a data warehouse and the pipelines that fill it.
# This folder is meant to be its own git repository.
schema = {schema}

[project]
name = "{name}"
title = "{title}"
description = "{description}"
# logo = "logo.png"   # shown in the console header; picked up automatically
                      # if a logo.png/svg/jpg sits beside this file

[target]
# Where the pipelines write, and where GraETL keeps its own bookkeeping so a
# module's data write and its state row commit in one transaction.
# Supported: sqlite, postgres.
{target}
# A ${{VAR}} in the dsn is expanded from the environment, then from a
# git-ignored .env beside this file - so no password is ever committed.

[files]
# Target file path: where modules put files they produce (DICOM, exports,
# attachments). Use ctx.file("sub", "name.dcm") to write inside it.
root = "{files}"

[runtime]
# Anything here overrides the instance-wide graetl.toml for this project.
# parallel_modules = 1
# entity_batch_size = 500
# cache_size = 4096

[graphs]
# pure_modules = ["math", "statistics"]
# reflect_allow = ["math", "pandas"]
'''

GITIGNORE_TEMPLATE = """\
# GraETL project - runtime state. Everything else here is source and belongs
# in git: pipeline code, graphs, node libraries, configuration.

# The warehouse itself and any sqlite sidecars
*.db
*.db-wal
*.db-shm
*.sqlite
*.sqlite3

# Produced files, run logs, profiles, caches
files/
pipelines/**/logs/
pipelines/**/profiles/
pipelines/**/data/
pipelines/**/.cache/

# Secrets
.env

# Python
__pycache__/
*.py[cod]
.venv/
"""
