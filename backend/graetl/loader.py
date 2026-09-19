"""Discovery and loading of pipeline folders.

A pipeline folder is any directory under ``pipelines/`` that contains the
standardized entry file ``pipeline.py`` exporting a module-level ``pipeline``
object. Modules are files named ``<name>.module.py``, found anywhere in the
hierarchy below it::

    pipelines/<id>/
        pipeline.py                        entry file: Pipeline + shared functions
        pipeline.toml                      configuration
        *.graphlib                         node-flow functions, pipeline wide
        modules/
            vitals/
                load_vital_signals.module.py     ONE module
                load_vital_signals.module.toml   metadata for that module
                module.toml                      defaults for the whole folder
                parsing.py                       helper - imported, never auto-loaded
                vitals.nodes.py                  node library - every function is a node
                cleanup.graph                    node-flow module
                helpers.graphlib                 node-flow functions

Two kinds of ``.py`` file are executed at load time: ``*.module.py`` (one module
each) and ``*.nodes.py`` (a node library - every public top-level function in it
becomes a node, with no decorator needed). Every other ``.py`` file is a helper
that runs when the module that needs it imports it - which keeps loading cheap
and makes "run just this one module" predictable.

``.graph`` and ``.graphlib`` files are inventoried here so the registry and the
UI know about them; compiling and executing them comes with the visual editor.
"""

from __future__ import annotations

import importlib.util
import inspect
import os
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from graetl.config import (
    GRAPH_SUFFIX,
    GRAPHLIB_SUFFIX,
    MODULE_CONFIG_FILENAME,
    MODULE_CONFIG_SUFFIX,
    MODULE_FILE_SUFFIX,
    MODULES_DIRNAME,
    NODES_FILE_SUFFIX,
    PIPELINE_ENTRY_FILENAME,
    RUNTIME_DIRNAMES,
    Settings,
    load_pipeline_config,
    read_toml,
)
from graetl.sdk import pipeline as pipeline_sdk
from graetl.sdk.errors import PipelineDefinitionError
from graetl.sdk.pipeline import Pipeline


@dataclass(slots=True)
class LoadedPipeline:
    id: str
    folder: Path
    pipeline: Pipeline
    config: dict[str, Any]
    module_folders: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class PipelineFolder:
    id: str
    path: Path

    @property
    def entry(self) -> Path:
        return self.path / PIPELINE_ENTRY_FILENAME


def discover_folders(settings: Settings) -> list[PipelineFolder]:
    settings.ensure_dirs()
    out: list[PipelineFolder] = []
    for child in sorted(settings.pipelines_dir.iterdir()):
        if not child.is_dir() or child.name.startswith((".", "_")):
            continue
        if (child / PIPELINE_ENTRY_FILENAME).exists():
            out.append(PipelineFolder(id=child.name, path=child))
    return out


# --------------------------------------------------------------------- loading


def _exec_file(
    path: Path,
    module_name: str,
    extra_sys_path: Path | None = None,
    *,
    isolate_imports: Path | None = None,
) -> Any:
    """Execute a Python file as a module, always from source.

    Compiling the source directly (instead of going through the import
    machinery) means a stale ``__pycache__`` entry can never serve old code -
    pipeline files are edited in the UI, sometimes within the same second.

    ``isolate_imports`` drops helper modules imported from that directory out of
    ``sys.modules`` afterwards, so two module files in different folders can
    both ``import helpers`` without colliding.
    """
    added: list[str] = []
    for candidate in (extra_sys_path,):
        if candidate is None:
            continue
        text = str(candidate)
        if text not in sys.path:
            sys.path.insert(0, text)
            added.append(text)
    before = set(sys.modules) if isolate_imports else set()
    try:
        source = path.read_text(encoding="utf-8")
        spec = importlib.util.spec_from_file_location(module_name, path)
        module = importlib.util.module_from_spec(spec) if spec else None
        if module is None:  # pragma: no cover - defensive
            raise PipelineDefinitionError(f"cannot import {path}")
        module.__file__ = str(path)
        sys.modules[module_name] = module
        try:
            exec(compile(source, str(path), "exec"), module.__dict__)  # noqa: S102
        except BaseException:
            sys.modules.pop(module_name, None)
            raise
        return module
    finally:
        for text in added:
            try:
                sys.path.remove(text)
            except ValueError:  # pragma: no cover
                pass
        if isolate_imports is not None:
            root = str(isolate_imports.resolve())
            for name in set(sys.modules) - before:
                helper = sys.modules.get(name)
                origin = getattr(helper, "__file__", None)
                if origin and str(Path(origin).resolve()).startswith(root + os.sep):
                    sys.modules.pop(name, None)


#: Generated from a ``.graphlib`` graph. Loaded before module files, because a
#: module (hand-written or generated) may call the functions it registers.
GRAPHLIB_MODULE_SUFFIX = ".graphlib.py"


def _rel(path: Path, base: Path) -> str:
    return path.relative_to(base).as_posix()


def _assets(folder: Path, base: Path) -> tuple[list[str], list[str]]:
    """``(.graph files, .graphlib files)`` inside one folder, relative to ``base``."""
    graphs = sorted(_rel(p, base) for p in folder.glob(f"*{GRAPH_SUFFIX}") if p.is_file())
    graphlibs = sorted(_rel(p, base) for p in folder.glob(f"*{GRAPHLIB_SUFFIX}") if p.is_file())
    return graphs, graphlibs


def _graph_modules(folder: Path, base: Path) -> list[dict[str, Any]]:
    """Each ``*.graph`` file is a module of its own.

    ``dosomething2.graph`` -> module ``dosomething2``, by way of the generated
    ``dosomething2.module.py``. A graph that has not been compiled yet is
    reported as *pending*: it is a real module the registry knows about, it just
    has no code behind it until ``graetl compile`` runs.
    """
    out: list[dict[str, Any]] = []
    for path in sorted(folder.glob(f"*{GRAPH_SUFFIX}")):
        if not path.is_file():
            continue
        name = path.name[: -len(GRAPH_SUFFIX)]
        compiled = (folder / f"{name}{MODULE_FILE_SUFFIX}").exists()
        out.append(
            {
                "name": name,
                "file": _rel(path, base),
                "source": "graph",
                "compiled": compiled,
                "output": _rel(folder / f"{name}{MODULE_FILE_SUFFIX}", base) if compiled else None,
            }
        )
    return out


def _walk_code_dirs(folder: Path):
    """Every directory below the pipeline folder that may hold code."""
    yield folder
    for path in sorted(folder.rglob("*")):
        if not path.is_dir():
            continue
        parts = path.relative_to(folder).parts
        if any(p in RUNTIME_DIRNAMES or p.startswith((".", "_")) for p in parts):
            continue
        yield path


def discover_module_files(folder: Path) -> list[Path]:
    """Every ``*.module.py`` file below the pipeline folder, in a stable order."""
    out: list[Path] = []
    for directory in _walk_code_dirs(folder):
        out.extend(sorted(p for p in directory.glob(f"*{MODULE_FILE_SUFFIX}") if p.is_file()))
    return out


def module_name_of(path: Path) -> str:
    """``load_vital_signals.module.py`` -> ``load_vital_signals``."""
    return path.name[: -len(MODULE_FILE_SUFFIX)]


def load_module_files(folder: Path, pipeline: Pipeline, *, pipeline_id: str) -> list[dict[str, Any]]:
    """Execute every ``*.module.py`` and inventory the node-flow files beside them."""
    safe_id = pipeline_id.replace("-", "_")
    by_folder: dict[str, dict[str, Any]] = {}

    def folder_entry(directory: Path) -> dict[str, Any]:
        rel = _rel(directory, folder) if directory != folder else "."
        if rel not in by_folder:
            graphs, graphlibs = _assets(directory, folder)
            by_folder[rel] = {
                "name": directory.name if directory != folder else pipeline_id,
                "folder": rel,
                "has_python": False,
                "modules": [],
                "graphs": graphs,
                "graph_modules": _graph_modules(directory, folder),
                "graphlibs": graphlibs,
                "meta": read_toml(directory / MODULE_CONFIG_FILENAME),
                "pending": False,
            }
        return by_folder[rel]

    for path in discover_module_files(folder):
        name = module_name_of(path)
        rel_file = _rel(path, folder)
        directory = path.parent
        entry = folder_entry(directory)
        entry["has_python"] = True

        # <name>.module.toml wins over the folder-wide module.toml.
        meta = {
            **entry["meta"],
            **read_toml(directory / f"{name}{MODULE_CONFIG_SUFFIX}"),
            "graphs": entry["graphs"],
            "graphlibs": entry["graphlibs"],
        }
        before = {m.name for m in pipeline.modules}
        try:
            with pipeline_sdk.loading(
                pipeline,
                module_name=name,
                file=rel_file,
                folder=_rel(directory, folder) if directory != folder else None,
                meta=meta,
            ):
                _exec_file(
                    path,
                    f"graetl_pipeline_{safe_id}_module_{name.replace('-', '_')}",
                    extra_sys_path=directory,
                    isolate_imports=directory,
                )
        except Exception as exc:  # noqa: BLE001 - re-raised with the file that failed
            raise PipelineDefinitionError(
                f"{rel_file}: {type(exc).__name__}: {exc}"
            ) from exc

        defined = [m.name for m in pipeline.modules if m.name not in before]
        if len(defined) > 1:
            raise PipelineDefinitionError(
                f"{rel_file} defines {len(defined)} modules ({', '.join(defined)}) - "
                "one .module.py file holds exactly one module. "
                "Move the others into their own <name>.module.py files."
            )
        if not defined:
            raise PipelineDefinitionError(
                f"{rel_file} defines no module - it needs one @pipeline.module function "
                "(helper code belongs in a plain .py file next to it)."
            )
        entry["modules"].extend(defined)

    # A .graph with no compiled .module.py beside it is a module the registry
    # knows about but cannot run yet.
    for directory in _walk_code_dirs(folder):
        graphs = _graph_modules(directory, folder)
        if graphs:
            entry = folder_entry(directory)
            entry["pending"] = any(not g["compiled"] for g in graphs)

    return [by_folder[key] for key in sorted(by_folder) if key != "."] + (
        [by_folder["."]] if "." in by_folder else []
    )


def load_entry_only(folder: Path, *, pipeline_id: str | None = None) -> Pipeline:
    """Execute ``pipeline.py`` and the node libraries, and nothing else.

    The graph compiler needs the pipeline's registered functions - they are what
    ``fn:`` nodes resolve to - but it must not load the module files, because it
    is about to generate some of them.
    """
    folder = Path(folder).resolve()
    entry = folder / PIPELINE_ENTRY_FILENAME
    if not entry.exists():
        raise PipelineDefinitionError(f"missing entry file: {entry}")
    pid = pipeline_id or folder.name
    module = _exec_file(
        entry, f"graetl_entry_{pid.replace('-', '_')}", extra_sys_path=folder
    )
    obj = getattr(module, "pipeline", None)
    if not isinstance(obj, Pipeline):
        for value in vars(module).values():
            if isinstance(value, Pipeline):
                obj = value
                break
    if not isinstance(obj, Pipeline):
        raise PipelineDefinitionError(
            f"{entry} must define a module-level `pipeline = Pipeline(...)` object"
        )
    obj.id = pid
    # Node libraries are hand-written and register nothing but functions, so
    # loading them here is safe - and necessary, because a `fn:` node may point
    # at one and the compiler has to resolve it.
    load_node_files(folder, obj, pipeline_id=pid)
    return obj


def discover_graphlib_files(folder: Path) -> list[Path]:
    """Every generated ``*.graphlib.py`` below the pipeline folder."""
    out: list[Path] = []
    for directory in _walk_code_dirs(folder):
        out.extend(
            sorted(p for p in directory.glob(f"*{GRAPHLIB_MODULE_SUFFIX}") if p.is_file())
        )
    return out


def discover_node_files(folder: Path) -> list[Path]:
    """Every ``*.nodes.py`` node library below the pipeline folder."""
    out: list[Path] = []
    for directory in _walk_code_dirs(folder):
        out.extend(sorted(p for p in directory.glob(f"*{NODES_FILE_SUFFIX}") if p.is_file()))
    return out


def _is_node_candidate(value: Any, module_name: str) -> bool:
    """A public function *defined in this file* - not one it imported."""
    if not inspect.isfunction(value):
        return False
    if value.__name__.startswith("_"):
        return False
    if getattr(value, "graetl_skip", False):
        return False
    # An import brings someone else's function into the namespace; registering
    # it would put `json.dumps` in the palette because a file imported json.
    return value.__module__ == module_name


def load_node_files(folder: Path, pipeline: Pipeline, *, pipeline_id: str) -> list[dict[str, Any]]:
    """Run every ``*.nodes.py`` and register the functions it defines as nodes.

    This is the low-ceremony way to add nodes: write plain functions in a file
    named ``text.nodes.py`` and they are all nodes, named after themselves, with
    pins from their signatures. ``@node(...)`` only overrides what is inferred.

    Purity is inferred from the signature: a function taking ``ctx`` first can
    reach the database, the log and the run, so it is impure and sits in the
    execution chain; one that does not is pure and folds into the expression
    that uses it. ``@node(pure=...)`` settles it either way.
    """
    safe_id = pipeline_id.replace("-", "_")
    libraries: list[dict[str, Any]] = []
    for path in discover_node_files(folder):
        name = path.name[: -len(NODES_FILE_SUFFIX)]
        rel_file = _rel(path, folder)
        module_name = f"graetl_pipeline_{safe_id}_nodes_{name.replace('-', '_')}"
        try:
            with pipeline_sdk.loading(pipeline, file=rel_file):
                module = _exec_file(
                    path,
                    module_name,
                    extra_sys_path=path.parent,
                    isolate_imports=path.parent,
                )
        except Exception as exc:  # noqa: BLE001 - re-raised with the file that failed
            raise PipelineDefinitionError(
                f"{rel_file}: {type(exc).__name__}: {exc}"
            ) from exc

        # The file name groups the nodes in the palette: text.nodes.py -> "Text".
        default_category = name.replace("_", " ").replace("-", " ").strip().title()
        registered: list[str] = []
        for attribute, value in vars(module).items():
            if attribute.startswith("_") or not _is_node_candidate(value, module_name):
                continue
            if getattr(value, "graetl_pure", None) is None:
                value.graetl_pure = not _takes_ctx(value)
            if not getattr(value, "graetl_description", ""):
                value.graetl_description = (value.__doc__ or "").strip().split("\n")[0]
            if not getattr(value, "graetl_category", ""):
                value.graetl_category = default_category
            if not hasattr(value, "graetl_title"):
                value.graetl_title = ""
            key = getattr(value, "graetl_name", None) or attribute
            existing = pipeline.functions.get(key)
            if existing is not None:
                origin = getattr(existing, "graetl_source", "pipeline.py")
                raise PipelineDefinitionError(
                    f"{rel_file}: node {key!r} is already defined by {origin} - "
                    "rename one of them, or use @node(name=...)"
                )
            value.graetl_source = rel_file
            if getattr(value, "graetl_cache", False):
                # Wrap last: functools.wraps copies the annotations above onto
                # the wrapper, and the registry must hold the cached version.
                from graetl.sdk.caching import cached

                value = cached(
                    value, maxsize=getattr(value, "graetl_cache_size", None), name=key
                )
            registered.append(pipeline.register_function(value, name=key))
        libraries.append({"file": rel_file, "name": name, "nodes": sorted(registered)})
    return libraries


def _takes_ctx(fn: Callable[..., Any]) -> bool:
    """Does this function want the run context as its first parameter?"""
    try:
        first = next(iter(inspect.signature(fn).parameters))
    except (ValueError, TypeError, StopIteration):
        return False
    return first in ("ctx", "context")


def load_graphlib_files(folder: Path, pipeline: Pipeline, *, pipeline_id: str) -> list[str]:
    """Run the generated function files so their ``@pipeline.function`` calls register.

    These come from ``.graphlib`` graphs and load before any module, since a
    module - hand-written or generated - may call them.
    """
    safe_id = pipeline_id.replace("-", "_")
    loaded: list[str] = []
    for path in discover_graphlib_files(folder):
        name = path.name[: -len(GRAPHLIB_MODULE_SUFFIX)]
        rel_file = _rel(path, folder)
        try:
            with pipeline_sdk.loading(pipeline, file=rel_file):
                _exec_file(
                    path,
                    f"graetl_pipeline_{safe_id}_graphfn_{name.replace('-', '_')}",
                    extra_sys_path=path.parent,
                    isolate_imports=path.parent,
                )
        except Exception as exc:  # noqa: BLE001 - re-raised with the file that failed
            raise PipelineDefinitionError(
                f"{rel_file}: {type(exc).__name__}: {exc}"
            ) from exc
        loaded.append(rel_file)
    return loaded


def load_pipeline(
    folder: Path, *, pipeline_id: str | None = None, compile_graphs: bool = True
) -> LoadedPipeline:
    """Import ``<folder>/pipeline.py`` plus every module folder underneath it.

    Any graph whose generated Python is missing or out of date is compiled
    first, so editing a graph and running the pipeline needs no separate step.
    """
    folder = Path(folder).resolve()
    entry = folder / PIPELINE_ENTRY_FILENAME
    if not entry.exists():
        raise PipelineDefinitionError(f"missing entry file: {entry}")

    pid = pipeline_id or folder.name

    if compile_graphs:
        from graetl.graph.build import build_pipeline_graphs

        report = build_pipeline_graphs(
            folder, pipeline_id=pid, config=load_pipeline_config(folder)
        )
        if report.error:
            raise PipelineDefinitionError(report.error)
        failed = [b for b in report.builds if b.status == "failed"]
        if failed:
            details = "; ".join(f"{b.source.name}: {b.error}" for b in failed)
            raise PipelineDefinitionError(f"graph did not compile - {details}")

    module = _exec_file(
        entry, f"graetl_pipeline_{pid.replace('-', '_')}", extra_sys_path=folder
    )

    obj = getattr(module, "pipeline", None)
    if obj is None:
        for value in vars(module).values():
            if isinstance(value, Pipeline):
                obj = value
                break
    if not isinstance(obj, Pipeline):
        raise PipelineDefinitionError(
            f"{entry} must define a module-level `pipeline = Pipeline(...)` object"
        )
    if obj.id != pid:
        # The folder name is authoritative for the registry.
        obj.id = pid

    # Node libraries first: a function graph or a module may call what they
    # register, and they call nothing themselves.
    node_libraries = load_node_files(folder, obj, pipeline_id=pid)
    graph_functions = load_graphlib_files(folder, obj, pipeline_id=pid)
    module_folders = load_module_files(folder, obj, pipeline_id=pid)
    _, graphlibs = _assets(folder, folder)
    all_graphs = [g for entry in module_folders for g in entry["graph_modules"]]
    pending = [g for g in all_graphs if not g["compiled"]]
    # A compiled graph *is* the module of the same name; only a hand-written
    # .module.py next to an uncompiled graph is a genuine conflict.
    clash = {g["name"] for g in pending} & {m.name for m in obj.modules}
    if clash:
        raise PipelineDefinitionError(
            f"module name(s) defined twice, as .module.py and as .graph: {', '.join(sorted(clash))}"
        )
    obj.assets = {
        "graphlibs": graphlibs,
        "graph_functions": graph_functions,
        "node_libraries": node_libraries,
        "module_folders": module_folders,
        "pending_modules": pending,
    }
    obj.validate()
    return LoadedPipeline(
        id=pid,
        folder=folder,
        pipeline=obj,
        config=load_pipeline_config(folder),
        module_folders=module_folders,
    )


def inspect_folder(folder: Path, *, pipeline_id: str | None = None) -> dict[str, Any]:
    """Load a pipeline and return its JSON definition, or an error payload."""
    try:
        loaded = load_pipeline(folder, pipeline_id=pipeline_id)
    except Exception as exc:  # noqa: BLE001 - reported to the UI
        return {
            "ok": False,
            "id": pipeline_id or Path(folder).name,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(limit=6),
        }
    definition = loaded.pipeline.to_dict()
    definition["config"] = loaded.config
    return {"ok": True, "id": loaded.id, "definition": definition}


# ------------------------------------------------------------------ templates

PIPELINE_TEMPLATES: dict[str, str] = {
    "stateful": '''"""{title}

Stateful pipeline: keeps per-entity state so runs can resume exactly where they
stopped, and reprocess only entities whose source revision moved.

Modules live in their own folders under ``modules/``; this file owns the
pipeline object, its lifecycle and the shared functions modules call.
"""

from graetl.sdk import Entity, Pipeline

pipeline = Pipeline(
    id="{pid}",
    title="{title}",
    description="{description}",
    stateful=True,
)


@pipeline.setup
def setup(ctx):
    """Open connections / load resources once per run."""
    ctx.info("Setting up {title}")
    # ctx.resources["src"] = sqlalchemy.create_engine(ctx.setting("source.dsn"))

    # The pipeline state database is yours to shape - create your target tables
    # here. Writes from modules commit together with the entity state row.
    ctx.db.execute(
        """
        CREATE TABLE IF NOT EXISTS results (
            entity_id  TEXT PRIMARY KEY,
            value      TEXT,
            updated_at TEXT
        )
        """
    )


@pipeline.entities
def discover(ctx):
    """Yield the entities this pipeline works on."""
    for i in range(1, 6):
        yield Entity(
            id=f"E{{i:04d}}",
            label=f"Example entity {{i}}",
            source_updated_at=None,  # put the source's last-changed timestamp here
            data={{"n": i}},
        )


@pipeline.function("label_for")
def label_for(entity_id, n):
    """Shared helper - callable from any module via ctx.fn("label_for")."""
    return f"{{entity_id}} (#{{n}})"


@pipeline.teardown
def teardown(ctx):
    ctx.info("Done")
''',
    "stateless": '''"""{title}

Stateless pipeline: no per-entity state, just ordered tasks and run metrics.
"""

from graetl.sdk import Pipeline

pipeline = Pipeline(
    id="{pid}",
    title="{title}",
    description="{description}",
    stateful=False,
)


@pipeline.task("extract", version=1)
def extract(ctx):
    ctx.info("Reading source")
    ctx.metric("rows_read", 0)


@pipeline.task("load", version=1)
def load(ctx):
    ctx.info("Writing target")
    ctx.metric("rows_written", 0)
''',
    "empty": '''"""{title}"""

from graetl.sdk import Pipeline

# A pipeline without modules and without entities is valid: it simply succeeds
# immediately, and the run metrics show that nothing had to be done.
pipeline = Pipeline(
    id="{pid}",
    title="{title}",
    description="{description}",
    stateful=False,
)
''',
}

MODULE_TEMPLATE = '''"""{title}

One .module.py file holds exactly one module. The module name comes from the
file name; helper code belongs in plain .py files next to it, imported here.
"""

from graetl.sdk import get_pipeline

pipeline = get_pipeline()


@pipeline.module(version=1, execution_layer={layer})
def {name}(ctx, entity):
    ctx.checkpoint()          # honours pause / stop
    ctx.metric("processed", 1)
    ctx.db.execute(
        "INSERT OR REPLACE INTO results (entity_id, value, updated_at) "
        "VALUES (?, ?, datetime('now'))",
        (entity.id, str(entity.data)),
    )
'''

MODULE_CONFIG_TEMPLATE = """# Metadata for this module. Shown in the UI, available as Module.meta.
# A module.toml (without a name prefix) provides defaults for a whole folder.
title = "{title}"
description = ""
tags = []
# execution_layer = {layer}
"""


def scaffold_module(
    pipeline_dir: Path,
    name: str,
    *,
    title: str | None = None,
    folder: str | None = None,
    execution_layer: int = 0,
    with_config: bool = True,
) -> Path:
    """Create ``modules/<folder>/<name>.module.py`` (plus its ``.module.toml``).

    ``folder`` defaults to the module name, giving each module room for helper
    files and node-flow graphs.
    """
    target = pipeline_dir / MODULES_DIRNAME / (folder if folder is not None else name)
    module_file = target / f"{name}{MODULE_FILE_SUFFIX}"
    if module_file.exists():
        raise FileExistsError(f"module file already exists: {module_file}")
    target.mkdir(parents=True, exist_ok=True)
    title = title or name.replace("_", " ").replace("-", " ").title()
    module_file.write_text(
        MODULE_TEMPLATE.format(
            title=title, name=name.replace("-", "_"), layer=execution_layer
        ),
        encoding="utf-8",
    )
    if with_config:
        config = target / f"{name}{MODULE_CONFIG_SUFFIX}"
        if not config.exists():
            config.write_text(
                MODULE_CONFIG_TEMPLATE.format(title=title, layer=execution_layer),
                encoding="utf-8",
            )
    return module_file


NODES_TEMPLATE = '''"""{title} nodes.

A node library. Every public function in this file is a node on any graph in
this pipeline - no decorator, no registration. Pins come from the signature and
the name comes from the function.

Purity is inferred: a function that does **not** take ``ctx`` is pure, so it has
no execution pins and folds into the expression that uses it. Take ``ctx`` (or
say ``@node(pure=False)``) when the function has side effects.

Use ``@node(...)`` only to override: ``category=`` groups it in the palette
(this file's name is the default), ``title=`` renames it there, ``pure=``
settles purity, and ``skip=True`` keeps a helper out of the palette. Names
starting with an underscore are helpers automatically.
"""

from graetl.sdk import node


def {name}(value):
    """One line here becomes the node's description in the palette."""
    return value


@node(pure=False)
def record_{name}(ctx, entity, value):
    """Takes ctx, so it is impure: it sits in the execution chain."""
    ctx.log(f"{{entity.id}}: {{value}}")
'''


def scaffold_nodes(pipeline_dir: Path, path: str) -> Path:
    """Create a ``*.nodes.py`` node library with a worked example inside."""
    target = pipeline_dir / path
    if target.exists():
        raise FileExistsError(f"file already exists: {target}")
    name = target.name[: -len(NODES_FILE_SUFFIX)]
    identifier = _identifier_or(name, "example")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        NODES_TEMPLATE.format(
            title=name.replace("_", " ").replace("-", " ").title(), name=identifier
        ),
        encoding="utf-8",
    )
    return target


def _identifier_or(text: str, fallback: str) -> str:
    cleaned = "".join(c if c.isalnum() or c == "_" else "_" for c in text).strip("_")
    return cleaned if cleaned.isidentifier() else fallback


def scaffold_pipeline(
    settings: Settings,
    pipeline_id: str,
    *,
    title: str | None = None,
    description: str = "",
    template: str = "stateful",
) -> Path:
    """Create a new pipeline folder from a template."""
    folder = settings.pipeline_dir(pipeline_id)
    if folder.exists():
        raise FileExistsError(f"pipeline folder already exists: {folder}")
    if template not in PIPELINE_TEMPLATES:
        raise ValueError(f"unknown template {template!r}")
    folder.mkdir(parents=True)
    (folder / "data").mkdir(exist_ok=True)
    (folder / MODULES_DIRNAME).mkdir(exist_ok=True)
    title = title or pipeline_id.replace("_", " ").replace("-", " ").title()
    (folder / PIPELINE_ENTRY_FILENAME).write_text(
        PIPELINE_TEMPLATES[template].format(pid=pipeline_id, title=title, description=description),
        encoding="utf-8",
    )
    (folder / "pipeline.toml").write_text(
        "# Configuration for this pipeline - reachable as ctx.config / ctx.setting('a.b')\n"
        "[source]\n"
        '# dsn = "sqlite:///data/source.db"\n\n'
        "[options]\n"
        "# batch_size = 500\n",
        encoding="utf-8",
    )
    (folder / "README.md").write_text(
        f"# {title}\n\n{description or 'Describe what this pipeline does.'}\n", encoding="utf-8"
    )
    if template == "stateful":
        scaffold_module(folder, "process", title="Process", execution_layer=10)
    return folder
