"""Compiling every graph in a pipeline folder.

The rule is simple enough to keep in your head:

===========================  ===========================================
``vitals.graph``             ``vitals.module.py``     a module, auto-loaded
``compute_bmi.graphlib``     ``compute_bmi.graphlib.py``  a pipeline function
===========================  ===========================================

The generated file sits next to its graph, carries a digest of the graph it
came from, and is a perfectly ordinary Python file: the loader runs it without
knowing a graph was involved. Compiling again when nothing changed is a no-op,
so this is cheap to call on every load.

If you ever want to stop using the visual editor for a module, delete the
``.graph`` and keep the ``.py``. That is the whole migration.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

from graetl.config import GRAPH_SUFFIX, GRAPHLIB_SUFFIX
from graetl.graph.compiler import (
    CompileError,
    compile_graphlib_file,
    compile_module_file,
    graph_digest,
    library_digest,
)
from graetl.graph.model import Graph, GraphError, Library
from graetl.graph.registry import NodeRegistry

#: Marker the compiler writes into every generated file.
DIGEST_RE = re.compile(r"^#\s*graetl:graph-digest\s+(\S+)\s*$", re.MULTILINE)
SOURCE_RE = re.compile(r"^#\s*graetl:generated-from\s+(.+?)\s*$", re.MULTILINE)


def is_generated(path: Path) -> str | None:
    """The graph a ``.py`` file was generated from, or ``None`` if hand-written."""
    try:
        head = path.read_text(encoding="utf-8")[:2000]
    except OSError:
        return None
    match = SOURCE_RE.search(head)
    return match.group(1) if match else None


def _digest_of(path: Path) -> str | None:
    try:
        match = DIGEST_RE.search(path.read_text(encoding="utf-8")[:2000])
    except OSError:
        return None
    return match.group(1) if match else None


@dataclass
class GraphBuild:
    """What happened to one graph file."""

    source: Path
    output: Path | None = None
    status: str = "pending"      # compiled | current | failed
    #: A ``.graph`` file is one module graph...
    graph: Graph | None = None
    #: ...and a ``.graphlib`` file is a library of one or more functions.
    library: Library | None = None
    error: str | None = None
    warnings: list[str] = field(default_factory=list)

    @property
    def names(self) -> list[str]:
        """Every module or function this file defines."""
        if self.library is not None:
            return list(self.library.names)
        return [self.graph.name] if self.graph else []

    @property
    def ok(self) -> bool:
        return self.graph is not None or self.library is not None

    def to_dict(self, base: Path | None = None) -> dict[str, Any]:
        def rel(path: Path | None) -> str | None:
            if path is None:
                return None
            try:
                return path.relative_to(base).as_posix() if base else path.as_posix()
            except ValueError:  # pragma: no cover - outside the pipeline folder
                return path.as_posix()

        return {
            "source": rel(self.source),
            "output": rel(self.output),
            "status": self.status,
            "kind": "library" if self.library else (self.graph.kind if self.graph else None),
            "name": self.library.name if self.library
                    else (self.graph.name if self.graph else self.source.stem),
            "functions": list(self.library.names) if self.library else None,
            "error": self.error,
            "warnings": list(self.warnings),
        }


@dataclass
class BuildReport:
    pipeline_id: str
    folder: Path
    builds: list[GraphBuild] = field(default_factory=list)
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and all(b.status != "failed" for b in self.builds)

    @property
    def changed(self) -> list[GraphBuild]:
        return [b for b in self.builds if b.status == "compiled"]

    @property
    def warnings(self) -> list[str]:
        out: list[str] = []
        for build in self.builds:
            out.extend(f"{build.source.name}: {w}" for w in build.warnings)
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_id": self.pipeline_id,
            "ok": self.ok,
            "error": self.error,
            "compiled": len(self.changed),
            "graphs": [b.to_dict(self.folder) for b in self.builds],
            "warnings": self.warnings,
        }

    def summary(self) -> str:
        if self.error:
            return f"graph build failed: {self.error}"
        failed = [b for b in self.builds if b.status == "failed"]
        parts = [
            f"{len(self.changed)} compiled",
            f"{len([b for b in self.builds if b.status == 'current'])} up to date",
        ]
        if failed:
            parts.append(f"{len(failed)} failed")
        if self.warnings:
            parts.append(f"{len(self.warnings)} warning(s)")
        return ", ".join(parts)


# ------------------------------------------------------------------ discovery


def discover_graph_files(folder: Path) -> tuple[list[Path], list[Path]]:
    """``(.graph files, .graphlib files)`` anywhere under a pipeline folder."""
    from graetl.loader import _walk_code_dirs

    graphs: list[Path] = []
    graphlibs: list[Path] = []
    for directory in _walk_code_dirs(Path(folder)):
        graphs.extend(sorted(p for p in directory.glob(f"*{GRAPH_SUFFIX}") if p.is_file()))
        graphlibs.extend(
            sorted(p for p in directory.glob(f"*{GRAPHLIB_SUFFIX}") if p.is_file())
        )
    return graphs, graphlibs


def output_for(path: Path, document: Graph | Library) -> Path:
    if isinstance(document, Library):
        return path.with_name(f"{document.name}.graphlib.py")
    suffix = ".module.py" if document.kind == "module" else ".graphlib.py"
    return path.with_name(f"{document.name}{suffix}")


# -------------------------------------------------------------------- build


def build_pipeline_graphs(
    folder: str | Path,
    *,
    pipeline_id: str | None = None,
    force: bool = False,
    write: bool = True,
    config: dict[str, Any] | None = None,
) -> BuildReport:
    """Compile every graph in a pipeline folder into Python beside it."""
    folder = Path(folder).resolve()
    report = BuildReport(pipeline_id=pipeline_id or folder.name, folder=folder)

    graph_files, graphlib_files = discover_graph_files(folder)
    if not graph_files and not graphlib_files:
        return report

    # pipeline.py alone: it registers the functions that fn: nodes resolve to,
    # and it must not need the module files we are about to generate.
    try:
        from graetl.loader import load_entry_only

        pipeline = load_entry_only(folder, pipeline_id=report.pipeline_id)
    except Exception as exc:  # noqa: BLE001 - surfaced to the caller
        report.error = f"cannot load pipeline.py: {type(exc).__name__}: {exc}"
        return report

    graph_settings = (config or {}).get("graphs", {}) if config else {}
    registry = NodeRegistry(
        functions=dict(pipeline.functions),
        pure_modules=graph_settings.get("pure_modules"),
        reflect_allow=graph_settings.get("reflect_allow"),
    )

    # Function graphs first: module graphs may call them, and resolving a
    # graph: node needs the callee's declared parameters.
    functions: list[GraphBuild] = []
    for path in graphlib_files:
        build = _load_library(path)
        functions.append(build)
        if build.library is not None:
            for graph in build.library.functions:
                registry.graphs[graph.name] = graph

    modules = [_load_module(path) for path in graph_files]

    duplicates = _duplicate_names(functions + modules)
    if duplicates:
        report.error = f"two graphs share a name: {', '.join(sorted(duplicates))}"
        return report

    for build in functions + modules:
        report.builds.append(build)
        if not build.ok:
            continue
        _compile_one(build, registry, force=force, write=write)

    return report


def _load_module(path: Path) -> GraphBuild:
    build = GraphBuild(source=path)
    try:
        graph = Graph.load(path)
    except GraphError as exc:
        build.status = "failed"
        build.error = str(exc)
        return build
    if graph.kind != "module":
        build.status = "failed"
        build.error = (
            f"{path.name} declares kind {graph.kind!r}; a {GRAPH_SUFFIX} file must be "
            "kind 'module'"
        )
        return build
    if graph.name != path.name[: -len(GRAPH_SUFFIX)]:
        build.status = "failed"
        build.error = (
            f"graph name {graph.name!r} does not match the file name {path.name!r} - "
            "the file name decides what the module is called"
        )
        return build
    build.graph = graph
    return build


def _load_library(path: Path) -> GraphBuild:
    """A ``.graphlib`` holds one or more functions; the file name names the file,
    and each function names itself."""
    build = GraphBuild(source=path)
    try:
        library = Library.load(path)
    except GraphError as exc:
        build.status = "failed"
        build.error = str(exc)
        return build
    stem = path.name[: -len(GRAPHLIB_SUFFIX)]
    if library.name != stem:
        build.status = "failed"
        build.error = (
            f"library name {library.name!r} does not match the file name {path.name!r} - "
            "the file name decides what the generated module is called"
        )
        return build
    if not library.functions:
        build.status = "failed"
        build.error = f"{path.name} defines no functions"
        return build
    build.library = library
    return build


def _duplicate_names(builds: Sequence[GraphBuild]) -> set[str]:
    """Function and module names share one namespace across the pipeline."""
    seen: dict[str, int] = {}
    for build in builds:
        for name in build.names:
            seen[name] = seen.get(name, 0) + 1
    return {name for name, count in seen.items() if count > 1}


def _compile_one(
    build: GraphBuild, registry: NodeRegistry, *, force: bool, write: bool
) -> None:
    document = build.library or build.graph
    assert document is not None
    target = output_for(build.source, document)
    build.output = target

    digest = (
        library_digest(document) if isinstance(document, Library) else graph_digest(document)
    )
    if not force and target.exists() and _digest_of(target) == digest:
        build.status = "current"
        return

    if target.exists() and is_generated(target) is None:
        build.status = "failed"
        build.error = (
            f"{target.name} exists and was not generated from a graph - refusing to "
            "overwrite hand-written code. Rename the graph, or delete the file."
        )
        return

    try:
        if isinstance(document, Library):
            compiled = compile_graphlib_file(document, registry)
        else:
            compiled = compile_module_file(document, registry)
    except (CompileError, GraphError) as exc:
        build.status = "failed"
        build.error = str(exc)
        return
    except Exception as exc:  # noqa: BLE001 - never let a bad graph kill a load
        build.status = "failed"
        build.error = f"{type(exc).__name__}: {exc}"
        return

    build.warnings = list(compiled.warnings)
    if write:
        try:
            target.write_text(compiled.source, encoding="utf-8")
        except OSError as exc:
            build.status = "failed"
            build.error = f"cannot write {target}: {exc}"
            return
    build.status = "compiled"


def compile_one_graph(
    path: str | Path,
    *,
    folder: str | Path | None = None,
    pipeline_id: str | None = None,
    config: dict[str, Any] | None = None,
    document: Graph | Library | None = None,
) -> tuple[str, list[str]]:
    """Compile a single graph and return ``(source, warnings)`` without writing.

    Used by the UI to preview the Python a graph produces while it is being
    edited. ``document`` compiles an unsaved version of the graph at ``path``:
    the editor previews what it is holding, not what is on disk.
    """
    path = Path(path).resolve()
    base = Path(folder).resolve() if folder else path.parent
    if document is None:
        document = (
            Library.load(path) if path.name.endswith(GRAPHLIB_SUFFIX) else Graph.load(path)
        )

    from graetl.loader import load_entry_only

    pipeline = load_entry_only(base, pipeline_id=pipeline_id or base.name)
    graph_settings = (config or {}).get("graphs", {}) if config else {}
    registry = NodeRegistry(
        functions=dict(pipeline.functions),
        pure_modules=graph_settings.get("pure_modules"),
        reflect_allow=graph_settings.get("reflect_allow"),
    )
    _, graphlibs = discover_graph_files(base)
    for other in graphlibs:
        if other == path:
            continue
        try:
            loaded = Library.load(other)
        except GraphError:
            continue
        for graph in loaded.functions:
            registry.graphs[graph.name] = graph
    # A library previewing itself must still see its own other functions.
    if isinstance(document, Library):
        for graph in document.functions:
            registry.graphs.setdefault(graph.name, graph)

    compiled = (
        compile_graphlib_file(document, registry)
        if isinstance(document, Library) or document.kind == "function"
        else compile_module_file(document, registry)
    )
    return compiled.source, list(compiled.warnings)


def clean_generated(folder: str | Path) -> list[Path]:
    """Delete every generated file whose graph is gone. Returns what it removed."""
    folder = Path(folder).resolve()
    graph_files, graphlib_files = discover_graph_files(folder)
    alive = {p.name for p in graph_files} | {p.name for p in graphlib_files}
    removed: list[Path] = []
    from graetl.loader import _walk_code_dirs

    for directory in _walk_code_dirs(folder):
        for path in sorted(directory.glob("*.py")):
            origin = is_generated(path)
            if origin and origin not in alive:
                path.unlink()
                removed.append(path)
    return removed


def iter_graphs(folder: str | Path) -> Iterable[Graph]:
    """Every module graph and every library function under a pipeline folder."""
    graph_files, graphlib_files = discover_graph_files(Path(folder))
    for path in graphlib_files:
        try:
            yield from Library.load(path).functions
        except GraphError:
            continue
    for path in graph_files:
        try:
            yield Graph.load(path)
        except GraphError:
            continue
