"""The GraETL HTTP + WebSocket API and the console it serves.

Built directly on Starlette/uvicorn (an ASGI app) - no heavyweight web
framework, so ``uv sync`` stays small and startup is instant.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Callable

from pydantic import ValidationError
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import FileResponse, HTMLResponse, JSONResponse, Response
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket, WebSocketDisconnect

from graetl import __version__
from graetl.config import (
    GRAPH_SUFFIX,
    GRAPHLIB_SUFFIX,
    MODULE_FILE_SUFFIX,
    NODES_FILE_SUFFIX,
    PIPELINE_ENTRY_FILENAME,
    Settings,
    load_pipeline_config,
    load_settings,
)
from graetl.loader import scaffold_module, scaffold_nodes, scaffold_pipeline
from graetl.server.hub import EventHub
from graetl.server.schemas import (
    CreateFileRequest,
    CreateModuleRequest,
    CreatePipelineRequest,
    ResetEntityRequest,
    ResetStateRequest,
    StartRunRequest,
    UpdatePipelineRequest,
    CreateFolderRequest,
    CreateGraphRequest,
    MoveFileRequest,
    WriteFileRequest,
    PreviewGraphRequest,
    WriteGraphRequest,
)
from graetl.project import (
    Project,
    ProjectError,
    forget_project,
    recent_projects,
    remember_project,
)
from graetl.server.supervisor import Supervisor, SupervisorError
from graetl.store.core import CoreStore
from graetl.store.state import StateStore
from graetl.utils import now_iso

PACKAGE_DIR = Path(__file__).resolve().parent
UI_DIR = PACKAGE_DIR / "static"
VENDOR_DIR = UI_DIR / "vendor"
MAX_UPLOAD_BYTES = 64 * 1024 * 1024
EDITABLE_SUFFIXES = {".py", ".toml", ".json", ".md", ".txt", ".sql", ".csv", ".yaml", ".yml", ".ini"}
MAX_EDIT_BYTES = 2_000_000
TERMINAL = ("succeeded", "failed", "stopped", "crashed")


def _json(data: Any, status_code: int = 200) -> JSONResponse:
    return JSONResponse(json.loads(json.dumps(data, default=str)), status_code=status_code)


def _qp(request: Request, name: str, default: Any = None, cast: Callable | None = None) -> Any:
    value = request.query_params.get(name)
    if value is None or value == "":
        return default
    if cast is None:
        return value
    try:
        return cast(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"invalid value for {name!r}") from exc


def _flag(request: Request, name: str, default: bool = False) -> bool:
    value = request.query_params.get(name)
    if value is None:
        return default
    return value.lower() in ("1", "true", "yes", "on")


async def _body(request: Request, model: type) -> Any:
    try:
        raw = await request.body()
        payload = json.loads(raw) if raw else {}
        return model(**payload)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=json.loads(exc.json())) from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="invalid JSON body") from exc


class _Deferred:
    """Stands in for the store or supervisor until a project is open.

    One instance opens one project, but it may be started without one - the
    console then shows the project picker. Every endpoint that needs a
    warehouse reaches it through one of these and gets a clean 503 until one is
    chosen, instead of the server refusing to start at all.
    """

    def __init__(self, what: str) -> None:
        self._what = what
        self._real: Any = None

    def bind(self, real: Any) -> None:
        self._real = real

    def unbind(self) -> Any:
        real, self._real = self._real, None
        return real

    @property
    def ready(self) -> bool:
        return self._real is not None

    def __getattr__(self, name: str) -> Any:
        if self._real is None:
            raise HTTPException(
                status_code=503,
                detail="no GraETL project is open - choose one first",
            )
        return getattr(self._real, name)


def create_app(settings: Settings | None = None) -> Starlette:
    settings = settings or load_settings()

    store = _Deferred("store")
    supervisor = _Deferred("supervisor")
    hub = EventHub(buffer_size=settings.console_buffer_lines)

    def open_project(project: Project) -> None:
        """Bind this instance to a project. Called once, at startup or by the picker."""
        settings.project = project
        settings.ensure_dirs()
        real_store = settings.core_store()
        store.bind(real_store)
        supervisor.bind(Supervisor(settings, real_store, hub))
        remember_project(project)

    if settings.has_project:
        open_project(settings.require_project())

    # ----------------------------------------------------------- helpers

    def require_pipeline(pipeline_id: str) -> dict[str, Any]:
        pipeline = store.get_pipeline(pipeline_id)
        if pipeline is None:
            raise HTTPException(status_code=404, detail=f"unknown pipeline {pipeline_id!r}")
        return pipeline

    def require_run(run_id: int) -> dict[str, Any]:
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"unknown run {run_id}")
        return run

    def safe_pipeline_path(pipeline_id: str, relative: str) -> Path:
        base = settings.pipeline_dir(pipeline_id).resolve()
        target = (base / relative).resolve()
        if base != target and base not in target.parents:
            raise HTTPException(status_code=400, detail="path escapes the pipeline folder")
        return target

    def state_summary(pipeline_id: str) -> dict[str, Any]:
        try:
            with settings.state_store(pipeline_id) as state:
                return {
                    "entities": state.count_entities(),
                    "modules": state.module_summary(),
                    "statuses": state.status_counts(),
                }
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "entities": 0, "modules": [], "statuses": {}}

    def pipeline_payload(pipeline: dict[str, Any]) -> dict[str, Any]:
        pid = pipeline["id"]
        payload = dict(pipeline)
        payload["active_run"] = store.active_run_for(pid)
        payload["last_run"] = store.last_finished_run(pid)
        payload["stats"] = store.pipeline_stats(pid)
        if pipeline.get("stateful"):
            payload["state_summary"] = state_summary(pid)
        return payload

    def read_log(log_path: str | None, limit: int) -> list[dict[str, Any]]:
        if not log_path:
            return []
        path = Path(log_path)
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        out: list[dict[str, Any]] = []
        for line in lines[-limit:]:
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
        return out

    # ------------------------------------------------------------ endpoints

    async def health(request: Request) -> Response:
        return _json(
            {
                "ok": True,
                "version": __version__,
                "root": str(settings.root),
                "project": settings.project.to_dict() if settings.project else None,
                "project_open": store.ready,
                "pipelines_dir": str(settings.pipelines_dir) if settings.has_project else None,
                "database": settings.target.describe() if settings.has_project else None,
                "active_runs": len(supervisor.processes) if supervisor.ready else 0,
                "ui": "builtin",
                # Where the browser should load the code editor from. A vendored
                # copy wins; otherwise the configured URL (empty = offline
                # fallback editor).
                "monaco_vendored": (VENDOR_DIR / "vs" / "loader.js").exists(),
                "monaco_url": settings.monaco_url,
                "time": now_iso(),
            }
        )

    async def overview(request: Request) -> Response:
        pipelines = [pipeline_payload(p) for p in store.list_pipelines()]
        runs = store.list_runs(limit=25)
        active = [r for r in runs if r["status"] not in TERMINAL]
        return _json(
            {
                "pipelines": pipelines,
                "recent_runs": runs,
                "active_runs": active,
                "counts": {
                    "pipelines": len(pipelines),
                    "stateful": sum(1 for p in pipelines if p["stateful"]),
                    "running": len(active),
                    "failing": sum(
                        1
                        for p in pipelines
                        if (p.get("last_run") or {}).get("status") in ("failed", "crashed")
                    ),
                },
            }
        )

    async def list_pipelines(request: Request) -> Response:
        return _json([pipeline_payload(p) for p in store.list_pipelines()])

    async def sync_pipelines(request: Request) -> Response:
        await asyncio.to_thread(supervisor.sync_pipelines)
        return _json([pipeline_payload(p) for p in store.list_pipelines()])

    async def create_pipeline(request: Request) -> Response:
        body: CreatePipelineRequest = await _body(request, CreatePipelineRequest)
        try:
            await asyncio.to_thread(
                scaffold_pipeline,
                settings,
                body.id,
                title=body.title,
                description=body.description,
                template=body.template,
            )
        except FileExistsError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        await asyncio.to_thread(supervisor.sync_pipelines)
        return _json(pipeline_payload(require_pipeline(body.id)), status_code=201)

    async def get_pipeline(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        return _json(pipeline_payload(require_pipeline(pid)))

    async def update_pipeline(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        body: UpdatePipelineRequest = await _body(request, UpdatePipelineRequest)
        if body.enabled is not None:
            store.set_pipeline_enabled(pid, body.enabled)
        hub.publish_threadsafe("system", {"kind": "pipelines_changed"})
        return _json(pipeline_payload(require_pipeline(pid)))

    async def delete_pipeline(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        if store.active_run_for(pid):
            raise HTTPException(status_code=409, detail="pipeline has an active run")
        delete_files = _flag(request, "delete_files")
        if delete_files:
            folder = settings.pipeline_dir(pid)
            if folder.exists():
                await asyncio.to_thread(shutil.rmtree, folder)
        store.delete_pipeline(pid)
        hub.publish_threadsafe("system", {"kind": "pipelines_changed"})
        return _json({"ok": True, "deleted": pid, "files_deleted": delete_files})

    async def create_module(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        if store.active_run_for(pid):
            raise HTTPException(status_code=409, detail="cannot add a module while a run is active")
        body: CreateModuleRequest = await _body(request, CreateModuleRequest)
        try:
            await asyncio.to_thread(
                scaffold_module,
                settings.pipeline_dir(pid),
                body.name,
                title=body.title,
                folder=body.folder,
                execution_layer=body.execution_layer,
            )
        except FileExistsError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await asyncio.to_thread(supervisor.sync_pipelines)
        return _json(pipeline_payload(require_pipeline(pid)), status_code=201)

    async def list_files(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        base = settings.pipeline_dir(pid)
        out: list[dict[str, Any]] = []
        if base.exists():
            for path in sorted(base.rglob("*")):
                rel = path.relative_to(base)
                if any(
                    part.startswith(".") or part in RUNTIME_FOLDERS for part in rel.parts
                ):
                    continue
                out.append(_file_entry(path, base))
        return _json(out)

    async def read_file(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        rel = _qp(request, "path")
        if not rel:
            raise HTTPException(status_code=400, detail="path is required")
        target = safe_pipeline_path(pid, rel)
        if not target.is_file():
            raise HTTPException(status_code=404, detail="file not found")
        if target.stat().st_size > MAX_EDIT_BYTES:
            raise HTTPException(status_code=413, detail="file too large to open in the editor")
        return _json(
            {"path": rel, "content": target.read_text(encoding="utf-8", errors="replace")}
        )

    def check_python(path: Path, content: str) -> dict[str, Any] | None:
        """Compile a .py file so the editor can show a syntax error before saving."""
        if path.suffix != ".py":
            return None
        try:
            compile(content, str(path.name), "exec")
        except SyntaxError as exc:
            return {
                "message": exc.msg or "syntax error",
                "line": exc.lineno,
                "column": exc.offset,
                "text": (exc.text or "").rstrip(),
            }
        return None

    async def create_file(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        body: CreateFileRequest = await _body(request, CreateFileRequest)
        target = safe_pipeline_path(pid, body.path)
        if target.exists():
            raise HTTPException(status_code=409, detail=f"{body.path} already exists")
        if target.suffix not in EDITABLE_SUFFIXES:
            raise HTTPException(status_code=400, detail=f"{target.suffix} files are not editable")
        # A node library starts with a worked example rather than an empty file:
        # the whole point is that you should not have to look anything up.
        if not body.content and body.path.endswith(NODES_FILE_SUFFIX):
            scaffold_nodes(settings.pipeline_dir(pid), body.path)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(body.content, encoding="utf-8")
        await asyncio.to_thread(supervisor.sync_pipelines)
        return _json({"ok": True, "path": body.path}, status_code=201)

    async def check_file(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        rel = _qp(request, "path") or "check.py"
        body: WriteFileRequest = await _body(request, WriteFileRequest)
        problem = check_python(Path(rel), body.content)
        return _json({"ok": problem is None, "error": problem})

    async def write_file(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        if store.active_run_for(pid):
            raise HTTPException(status_code=409, detail="cannot edit while a run is active")
        rel = _qp(request, "path")
        if not rel:
            raise HTTPException(status_code=400, detail="path is required")
        body: WriteFileRequest = await _body(request, WriteFileRequest)
        target = safe_pipeline_path(pid, rel)
        if target.suffix not in EDITABLE_SUFFIXES:
            raise HTTPException(status_code=400, detail=f"{target.suffix} files are not editable")
        problem = check_python(target, body.content)
        if problem and not _flag(request, "force"):
            raise HTTPException(
                status_code=422,
                detail={
                    "detail": f"{problem['message']} (line {problem['line']})",
                    "syntax_error": problem,
                },
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body.content, encoding="utf-8")
        await asyncio.to_thread(supervisor.sync_pipelines)
        payload = pipeline_payload(require_pipeline(pid))
        payload["saved"] = rel
        return _json(payload)

    # ------------------------------------------------------------------ graphs
    #
    # A graph is the source; the .py beside it is the build output. These
    # endpoints deal only in graphs - compiling happens on save, so the editor
    # never has to think about it.

    def _graph_config(pid: str) -> dict[str, Any]:
        return load_pipeline_config(settings.pipeline_dir(pid))

    def _graph_registry(pid: str):
        """A node registry for one pipeline: its functions and its function graphs."""
        from graetl.graph.build import discover_graph_files
        from graetl.graph.model import GraphError, Library
        from graetl.graph.registry import NodeRegistry
        from graetl.loader import load_entry_only

        folder = settings.pipeline_dir(pid)
        pipeline = load_entry_only(folder, pipeline_id=pid)
        graph_settings = _graph_config(pid).get("graphs", {}) or {}
        registry = NodeRegistry(
            functions=dict(pipeline.functions),
            pure_modules=graph_settings.get("pure_modules"),
            reflect_allow=graph_settings.get("reflect_allow"),
        )
        _, graphlibs = discover_graph_files(folder)
        for path in graphlibs:
            try:
                library = Library.load(path)
            except GraphError:
                continue
            for graph in library.functions:
                registry.graphs[graph.name] = graph
        return registry

    async def list_graphs(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        from graetl.graph.build import build_pipeline_graphs

        report = await asyncio.to_thread(
            build_pipeline_graphs,
            settings.pipeline_dir(pid),
            pipeline_id=pid,
            write=False,
            config=_graph_config(pid),
        )
        return _json(report.to_dict())

    def _resolve_nodes(graph: Any, registry: Any) -> tuple[dict[str, Any], list[dict[str, str]]]:
        """Every node's shape, for the canvas, plus whatever would not resolve."""
        from graetl.graph import core_nodes
        from graetl.graph.model import GraphError

        definitions: dict[str, Any] = {}
        problems: list[dict[str, str]] = []
        if registry is None:
            return definitions, problems
        for node in graph.nodes:
            try:
                resolved = (
                    core_nodes.resolve(node, graph)
                    if node.kind == "core"
                    else registry.resolve(node)
                )
                definitions[node.id] = resolved.to_dict()
            except GraphError as exc:
                problems.append({"node": node.id, "error": str(exc)})
        return definitions, problems

    async def read_graph(request: Request) -> Response:
        """The document plus every node's resolved shape, for the canvas.

        A ``.graph`` is one module graph. A ``.graphlib`` is a library of one or
        more functions, and the whole library comes back at once: the editor
        holds it, switches between its functions without a round trip, and saves
        the file as a whole.
        """
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        rel = _qp(request, "path")
        if not rel:
            raise HTTPException(status_code=400, detail="path is required")
        path = safe_pipeline_path(pid, rel)
        if not path.is_file():
            raise HTTPException(status_code=404, detail=f"no such graph: {rel}")
        from graetl.graph.model import Graph as GraphDoc
        from graetl.graph.model import GraphError, Library

        is_library = path.name.endswith(GRAPHLIB_SUFFIX)
        try:
            document = Library.load(path) if is_library else GraphDoc.load(path)
        except GraphError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None

        problems: list[dict[str, str]] = []
        try:
            registry = _graph_registry(pid)
        except Exception as exc:  # noqa: BLE001 - a broken pipeline.py must not hide the graph
            registry = None
            problems.append({"node": "", "error": f"pipeline.py did not load: {exc}"})

        if is_library:
            resolved: dict[str, Any] = {}
            for graph in document.functions:
                definitions, issues = _resolve_nodes(graph, registry)
                resolved[graph.name] = {"definitions": definitions, "problems": issues}
                problems.extend(issues)
            return _json(
                {
                    "path": rel,
                    "kind": "library",
                    "library": document.to_dict(),
                    "resolved": resolved,
                    "problems": problems,
                    "output": _graph_output_name(document, rel),
                }
            )

        definitions, issues = _resolve_nodes(document, registry)
        problems.extend(issues)
        return _json(
            {
                "path": rel,
                "kind": "module",
                "graph": document.to_dict(),
                "definitions": definitions,
                "problems": problems,
                "output": _graph_output_name(document, rel),
            }
        )

    def _graph_output_name(document: Any, rel: str) -> str:
        from graetl.graph.model import Library

        suffix = (
            ".graphlib.py"
            if isinstance(document, Library) or document.kind == "function"
            else ".module.py"
        )
        parent = rel.rsplit("/", 1)[0] + "/" if "/" in rel else ""
        return f"{parent}{document.name}{suffix}"

    async def write_graph(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        if store.active_run_for(pid):
            raise HTTPException(status_code=409, detail="cannot edit while a run is active")
        rel = _qp(request, "path")
        if not rel:
            raise HTTPException(status_code=400, detail="path is required")
        body: WriteGraphRequest = await _body(request, WriteGraphRequest)
        path = safe_pipeline_path(pid, rel)
        if path.suffix not in (GRAPH_SUFFIX, GRAPHLIB_SUFFIX):
            raise HTTPException(
                status_code=400,
                detail=f"a graph file must end in {GRAPH_SUFFIX} or {GRAPHLIB_SUFFIX}",
            )
        from graetl.graph.model import Graph as GraphDoc
        from graetl.graph.model import GraphError, Library

        try:
            document = (
                Library.from_dict(body.graph, path=path)
                if path.suffix == GRAPHLIB_SUFFIX
                else GraphDoc.from_dict(body.graph, path=path)
            )
        except GraphError as exc:
            raise HTTPException(status_code=422, detail={"detail": str(exc)}) from None

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(document.dumps(), encoding="utf-8")

        result: dict[str, Any] = {"saved": rel}
        if body.compile:
            from graetl.graph.build import build_pipeline_graphs

            report = await asyncio.to_thread(
                build_pipeline_graphs,
                settings.pipeline_dir(pid),
                pipeline_id=pid,
                config=_graph_config(pid),
            )
            result["build"] = report.to_dict()
        await asyncio.to_thread(supervisor.sync_pipelines)
        result["pipeline"] = pipeline_payload(require_pipeline(pid))
        return _json(result)

    async def create_graph(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        body: CreateGraphRequest = await _body(request, CreateGraphRequest)
        path = safe_pipeline_path(pid, body.path)
        if path.exists():
            raise HTTPException(status_code=409, detail=f"{body.path} already exists")
        suffix = GRAPH_SUFFIX if body.kind == "module" else GRAPHLIB_SUFFIX
        if path.suffix != suffix:
            raise HTTPException(
                status_code=400, detail=f"a {body.kind} graph must end in {suffix}"
            )
        from graetl.graph.model import Graph as GraphDoc
        from graetl.graph.model import Library
        from graetl.graph.model import Node as GraphNode

        name = path.name[: -len(suffix)]
        graph = GraphDoc(
            name=name,
            kind=body.kind,  # type: ignore[arg-type]
            title=body.title or name.replace("_", " ").title(),
            description=body.description,
            execution_layer=body.execution_layer,
            nodes=[
                GraphNode(
                    id="entry",
                    op={"entity": "core:entry", "batch": "core:entry_batch",
                        "once": "core:entry_once"}[body.entry if body.kind == "module" else "entity"],
                    pos=(80.0, 200.0),
                    config=(
                        {"size": body.batch_size}
                        if body.kind == "module" and body.entry == "batch" and body.batch_size
                        else {}
                    ),
                ),
                GraphNode(id="return", op="core:return", pos=(560.0, 200.0)),
            ],
            path=path,
        )
        # A .graphlib is a library: it starts with one function and can hold more.
        document: Any = graph
        if body.kind != "module":
            document = Library(
                name=name,
                functions=[graph],
                title=body.title or name.replace("_", " ").title(),
                description=body.description,
                path=path,
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(document.dumps(), encoding="utf-8")
        await asyncio.to_thread(supervisor.sync_pipelines)
        return _json({"path": body.path, "graph": document.to_dict()}, status_code=201)

    async def compile_graphs(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        from graetl.graph.build import build_pipeline_graphs

        report = await asyncio.to_thread(
            build_pipeline_graphs,
            settings.pipeline_dir(pid),
            pipeline_id=pid,
            force=_flag(request, "force"),
            config=_graph_config(pid),
        )
        await asyncio.to_thread(supervisor.sync_pipelines)
        return _json(report.to_dict())

    async def preview_graph(request: Request) -> Response:
        """The Python a graph compiles to, without writing it.

        ``POST`` with a document previews unsaved editor state; ``GET`` previews
        what is on disk.
        """
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        rel = _qp(request, "path")
        if not rel:
            raise HTTPException(status_code=400, detail="path is required")
        path = safe_pipeline_path(pid, rel)
        if not path.is_file():
            raise HTTPException(status_code=404, detail=f"no such graph: {rel}")
        from graetl.graph.build import compile_one_graph
        from graetl.graph.model import Graph as GraphDoc
        from graetl.graph.model import GraphError, Library

        document = None
        if request.method == "POST":
            body: PreviewGraphRequest = await _body(request, PreviewGraphRequest)
            try:
                document = (
                    Library.from_dict(body.graph, path=path)
                    if path.name.endswith(GRAPHLIB_SUFFIX)
                    else GraphDoc.from_dict(body.graph, path=path)
                )
            except GraphError as exc:
                return _json({"ok": False, "error": str(exc), "source": "", "warnings": []})

        try:
            source, warnings = await asyncio.to_thread(
                compile_one_graph,
                path,
                folder=settings.pipeline_dir(pid),
                pipeline_id=pid,
                config=_graph_config(pid),
                document=document,
            )
        except Exception as exc:  # noqa: BLE001 - reported to the editor
            return _json({"ok": False, "error": str(exc), "source": "", "warnings": []})
        return _json({"ok": True, "source": source, "warnings": warnings})

    async def node_catalog(request: Request) -> Response:
        """Every node the palette can offer for this pipeline."""
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        try:
            registry = _graph_registry(pid)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=422, detail=str(exc)) from None
        module = _qp(request, "module")
        if module:
            # The Import dialog: what is inside this module, and what can its
            # submodules offer? Reflection you can look at rather than recall.
            from graetl.graph import browse as browse_module
            from graetl.graph.model import GraphError

            allow = tuple((_graph_config(pid).get("graphs", {}) or {}).get("reflect_allow") or ())
            try:
                if _flag(request, "methods"):
                    return _json(browse_module.members_of(module, allow=allow))
                return _json(browse_module.browse(module, allow=allow))
            except GraphError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from None

        suggest = _qp(request, "suggest")
        if suggest is not None:
            # A wire dropped on empty canvas: what would you plausibly do with
            # a value of this type?
            from graetl.graph import suggest as suggest_nodes

            return _json(
                {
                    "nodes": suggest_nodes.for_pin(
                        suggest, _qp(request, "dir", "out"), registry
                    )
                }
            )

        op = _qp(request, "op")
        if op:
            from graetl.graph.model import GraphError

            try:
                config = json.loads(_qp(request, "config", "{}") or "{}")
                return _json(registry.describe(op, config))
            except (GraphError, ValueError) as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from None
        return _json({"nodes": registry.catalog()})

    # -------------------------------------------------------------- file tree
    #
    # The Files tab is a real tree, so it needs the operations a tree implies:
    # move, rename, delete, new folder, and dropping files in from the desktop.
    # Two GraETL-specific rules run through all of them:
    #
    #   * a generated .py belongs to its graph - you rename or delete the graph,
    #     and the .py follows. Touching it directly is refused.
    #   * a module's name is its file name, and entity state is keyed by module
    #     name. Renaming carries the state across; deleting drops it.

    RUNTIME_FOLDERS = {"__pycache__", "logs", "profiles"}

    def _file_entry(path: Path, base: Path) -> dict[str, Any]:
        from graetl.graph.build import is_generated

        rel = path.relative_to(base)
        is_file = path.is_file()
        name = path.name
        kind = "dir"
        module = None
        if is_file:
            if name.endswith(MODULE_FILE_SUFFIX):
                kind, module = "module", name[: -len(MODULE_FILE_SUFFIX)]
            elif name.endswith(NODES_FILE_SUFFIX):
                kind = "nodes"
            elif name.endswith(".graphlib.py"):
                kind = "graph_function"
            elif name.endswith(GRAPH_SUFFIX):
                kind, module = "graph", name[: -len(GRAPH_SUFFIX)]
            elif name.endswith(GRAPHLIB_SUFFIX):
                kind = "graphlib"
            elif path.suffix == ".py":
                kind = "python"
            elif path.suffix in (".toml", ".ini", ".cfg"):
                kind = "config"
            elif path.suffix in (".csv", ".json", ".xlsx", ".parquet"):
                kind = "data"
            elif path.suffix in (".db", ".sqlite", ".sqlite3"):
                kind = "database"
            else:
                kind = "file"
        return {
            "path": rel.as_posix(),
            "name": name,
            "type": "file" if is_file else "dir",
            "kind": kind,
            "size": path.stat().st_size if is_file else None,
            "editable": is_file and path.suffix in EDITABLE_SUFFIXES,
            "generated": is_generated(path) if is_file and path.suffix == ".py" else None,
            "module": module,
        }

    def _module_of(path: Path) -> str | None:
        """The module a file defines, hand-written or drawn."""
        if path.name.endswith(MODULE_FILE_SUFFIX):
            return path.name[: -len(MODULE_FILE_SUFFIX)]
        if path.name.endswith(GRAPH_SUFFIX):
            return path.name[: -len(GRAPH_SUFFIX)]
        return None

    def _module_names_under(path: Path) -> list[str]:
        """Every module defined at or under this path, in a stable order.

        A graph counts: its generated .module.py carries the same name, and
        renaming the graph renames the module.
        """
        if path.is_file():
            name = _module_of(path)
            return [name] if name else []
        out: list[str] = []
        for child in sorted(path.rglob("*")):
            if not child.is_file():
                continue
            name = _module_of(child)
            # A graph and its generated file are one module, not two.
            if name and name not in out:
                out.append(name)
        return out

    def _rename_inside_graph(path: Path) -> None:
        """A document's ``name`` is its file name; keep it in step.

        For a library that is the library's own name (what the generated
        ``.py`` is called); the functions inside keep their names.
        """
        from graetl.graph.model import Graph as GraphDoc
        from graetl.graph.model import GraphError as GraphDocError
        from graetl.graph.model import Library

        for suffix in (GRAPH_SUFFIX, GRAPHLIB_SUFFIX):
            if not path.name.endswith(suffix):
                continue
            loader = Library.load if suffix == GRAPHLIB_SUFFIX else GraphDoc.load
            try:
                document = loader(path)
            except GraphDocError:
                return  # a malformed graph is reported by the build, not here
            stem = path.name[: -len(suffix)]
            if document.name != stem:
                document.name = stem
                path.write_text(document.dumps(), encoding="utf-8")
            return

    def _guard_editable(pid: str, path: Path, action: str) -> None:
        from graetl.graph.build import is_generated

        base = settings.pipeline_dir(pid).resolve()
        if path.resolve() == base:
            raise HTTPException(status_code=400, detail="the pipeline folder itself stays put")
        if path.name == PIPELINE_ENTRY_FILENAME and path.parent.resolve() == base:
            raise HTTPException(
                status_code=400, detail=f"pipeline.py is the entry file and cannot be {action}"
            )
        if path.is_file() and path.suffix == ".py":
            origin = is_generated(path)
            if origin:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"{path.name} is generated from {origin} - {action.rstrip('d')} the "
                        "graph instead and this file follows"
                    ),
                )

    def _companion(path: Path) -> Path | None:
        """The generated file that belongs to a graph, if it is there."""
        if path.name.endswith(GRAPH_SUFFIX):
            companion = path.with_name(path.name[: -len(GRAPH_SUFFIX)] + MODULE_FILE_SUFFIX)
        elif path.name.endswith(GRAPHLIB_SUFFIX):
            companion = path.with_name(path.name[: -len(GRAPHLIB_SUFFIX)] + ".graphlib.py")
        else:
            return None
        return companion if companion.is_file() else None

    def _rebuild_graphs(pid: str) -> dict[str, Any] | None:
        """Recompile after a move or delete, so generated files follow their graph."""
        from graetl.graph.build import build_pipeline_graphs, discover_graph_files

        folder = settings.pipeline_dir(pid)
        graphs, graphlibs = discover_graph_files(folder)
        if not graphs and not graphlibs:
            return None
        return build_pipeline_graphs(
            folder, pipeline_id=pid, config=_graph_config(pid)
        ).to_dict()

    async def move_file(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        if store.active_run_for(pid):
            raise HTTPException(status_code=409, detail="cannot move files while a run is active")
        body: MoveFileRequest = await _body(request, MoveFileRequest)
        source = safe_pipeline_path(pid, body.source)
        target = safe_pipeline_path(pid, body.target)
        if not source.exists():
            raise HTTPException(status_code=404, detail=f"no such path: {body.source}")
        if target.exists():
            raise HTTPException(status_code=409, detail=f"{body.target} already exists")
        if source in target.parents:
            raise HTTPException(status_code=400, detail="a folder cannot be moved into itself")
        _guard_editable(pid, source, "moved")
        if any(part in RUNTIME_FOLDERS or part.startswith(".") for part in Path(body.target).parts):
            raise HTTPException(status_code=400, detail="that destination is a runtime folder")

        before = _module_names_under(source)
        companion = _companion(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        source.rename(target)
        moved = [{"from": body.source, "to": body.target}]
        if target.is_file():
            _rename_inside_graph(target)
        else:
            for child in sorted(target.rglob("*")):
                if child.is_file():
                    _rename_inside_graph(child)

        if companion is not None:
            # Keep a graph and its generated file together.
            new_companion = _companion_target(target)
            if new_companion is not None and not new_companion.exists():
                companion.rename(new_companion)
                moved.append(
                    {
                        "from": companion.relative_to(settings.pipeline_dir(pid)).as_posix(),
                        "to": new_companion.relative_to(settings.pipeline_dir(pid)).as_posix(),
                    }
                )
            else:
                companion.unlink()

        after = _module_names_under(target)
        migrated = 0
        renames = [(a, b) for a, b in zip(before, after) if a != b]
        if body.migrate_state and renames:
            with settings.state_store(pid) as state:
                for old_name, new_name in renames:
                    migrated += state.rename_module(old_name, new_name)

        build = _rebuild_graphs(pid)
        await asyncio.to_thread(supervisor.sync_pipelines)
        return _json(
            {
                "ok": True,
                "moved": moved,
                "renamed_modules": [{"from": a, "to": b} for a, b in renames],
                "state_rows_migrated": migrated,
                "build": build,
                "pipeline": pipeline_payload(require_pipeline(pid)),
            }
        )

    def _companion_target(path: Path) -> Path | None:
        if path.name.endswith(GRAPH_SUFFIX):
            return path.with_name(path.name[: -len(GRAPH_SUFFIX)] + MODULE_FILE_SUFFIX)
        if path.name.endswith(GRAPHLIB_SUFFIX):
            return path.with_name(path.name[: -len(GRAPHLIB_SUFFIX)] + ".graphlib.py")
        return None

    async def delete_file(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        if store.active_run_for(pid):
            raise HTTPException(
                status_code=409, detail="cannot delete files while a run is active"
            )
        rel = _qp(request, "path")
        if not rel:
            raise HTTPException(status_code=400, detail="path is required")
        path = safe_pipeline_path(pid, rel)
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"no such path: {rel}")
        _guard_editable(pid, path, "deleted")

        modules = _module_names_under(path)
        companion = _companion(path)
        removed = [rel]
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        if companion is not None:
            companion.unlink()
            removed.append(companion.relative_to(settings.pipeline_dir(pid)).as_posix())

        dropped = 0
        if modules:
            with settings.state_store(pid) as state:
                for name in modules:
                    dropped += state.drop_module(name)

        build = _rebuild_graphs(pid)
        await asyncio.to_thread(supervisor.sync_pipelines)
        return _json(
            {
                "ok": True,
                "removed": removed,
                "modules_dropped": modules,
                "state_rows_dropped": dropped,
                "build": build,
                "pipeline": pipeline_payload(require_pipeline(pid)),
            }
        )

    async def create_folder(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        body: CreateFolderRequest = await _body(request, CreateFolderRequest)
        target = safe_pipeline_path(pid, body.path)
        if target.exists():
            raise HTTPException(status_code=409, detail=f"{body.path} already exists")
        target.mkdir(parents=True)
        return _json({"ok": True, "path": body.path}, status_code=201)

    async def upload_file(request: Request) -> Response:
        """One file per request, raw body - no multipart dependency needed."""
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        if store.active_run_for(pid):
            raise HTTPException(status_code=409, detail="cannot add files while a run is active")
        rel = _qp(request, "path")
        if not rel:
            raise HTTPException(status_code=400, detail="path is required")
        target = safe_pipeline_path(pid, rel)
        if target.exists() and not _flag(request, "overwrite"):
            raise HTTPException(status_code=409, detail=f"{rel} already exists")
        payload = await request.body()
        if len(payload) > MAX_UPLOAD_BYTES:
            raise HTTPException(
                status_code=413,
                detail=f"{rel} is larger than {MAX_UPLOAD_BYTES // (1024 * 1024)} MB",
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        await asyncio.to_thread(supervisor.sync_pipelines)
        return _json({"ok": True, "path": rel, "size": len(payload)}, status_code=201)

    async def list_entities(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        search = _qp(request, "search")
        status = _qp(request, "status")
        module = _qp(request, "module")
        limit = min(int(_qp(request, "limit", 100, int)), 2000)
        offset = int(_qp(request, "offset", 0, int))
        with settings.state_store(pid) as state:
            return _json(
                {
                    "total": state.count_entities(),
                    "matching": state.count_matching_entities(
                        search=search, status=status, module=module
                    ),
                    "limit": limit,
                    "offset": offset,
                    "entities": state.list_entities(
                        limit=limit,
                        offset=offset,
                        search=search,
                        status=status,
                        module=module,
                        order=_qp(request, "order", "entity_id"),
                    ),
                    "modules": state.module_summary(),
                    "statuses": state.status_counts(),
                }
            )

    async def get_entity(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        entity_id = request.path_params["entity_id"]
        require_pipeline(pid)
        with settings.state_store(pid) as state:
            entity = state.get_entity(entity_id)
        if entity is None:
            raise HTTPException(status_code=404, detail="unknown entity")
        return _json(entity)

    async def reset_entity(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        entity_id = request.path_params["entity_id"]
        require_pipeline(pid)
        if store.active_run_for(pid):
            raise HTTPException(status_code=409, detail="cannot reset while a run is active")
        body: ResetEntityRequest = await _body(request, ResetEntityRequest)
        with settings.state_store(pid) as state:
            removed = state.reset_entity(entity_id, body.module)
            entity = state.get_entity(entity_id)
        return _json({"ok": True, "reset": removed, "entity": entity})

    async def reset_state(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        if store.active_run_for(pid):
            raise HTTPException(status_code=409, detail="cannot reset state while a run is active")
        body: ResetStateRequest = await _body(request, ResetStateRequest)
        with settings.state_store(pid) as state:
            if body.module:
                n = state.reset_module(body.module)
            else:
                state.reset_all(drop_entities=body.drop_entities)
                n = -1
        hub.publish_threadsafe("system", {"kind": "pipelines_changed"})
        return _json({"ok": True, "reset": n})

    async def list_runs(request: Request) -> Response:
        statuses = request.query_params.getlist("status") or None
        return _json(
            store.list_runs(
                pipeline_id=_qp(request, "pipeline_id"),
                limit=min(int(_qp(request, "limit", 50, int)), 500),
                offset=int(_qp(request, "offset", 0, int)),
                statuses=statuses,
            )
        )

    async def list_pipeline_runs(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        return _json(
            store.list_runs(
                pipeline_id=pid,
                limit=min(int(_qp(request, "limit", 50, int)), 500),
                offset=int(_qp(request, "offset", 0, int)),
            )
        )

    async def start_run(request: Request) -> Response:
        pid = request.path_params["pipeline_id"]
        require_pipeline(pid)
        body: StartRunRequest = await _body(request, StartRunRequest)
        try:
            run = await asyncio.to_thread(
                supervisor.start_run,
                pid,
                mode=body.mode,
                params=body.merged_params(),
                trigger="manual",
            )
        except SupervisorError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return _json(run, status_code=201)

    async def get_run(request: Request) -> Response:
        run_id = int(request.path_params["run_id"])
        run = require_run(run_id)
        run["steps"] = store.list_steps(run_id)
        run["events"] = store.list_events(run_id, limit=100)
        return _json(run)

    async def get_steps(request: Request) -> Response:
        run_id = int(request.path_params["run_id"])
        require_run(run_id)
        return _json(store.list_steps(run_id))

    async def get_console(request: Request) -> Response:
        run_id = int(request.path_params["run_id"])
        run = require_run(run_id)
        limit = min(int(_qp(request, "limit", 1000, int)), 20000)
        events = hub.replay(f"run:{run_id}", limit) or read_log(run.get("log_path"), limit)
        levels = _qp(request, "levels")
        if levels:
            wanted = {lvl.strip() for lvl in levels.split(",") if lvl.strip()}
            events = [
                e for e in events if e.get("kind") != "log" or e.get("level", "info") in wanted
            ]
        return _json({"run_id": run_id, "status": run["status"], "events": events})

    async def pause_run(request: Request) -> Response:
        run_id = int(request.path_params["run_id"])
        require_run(run_id)
        try:
            return _json(await asyncio.to_thread(supervisor.pause, run_id))
        except SupervisorError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    async def resume_run(request: Request) -> Response:
        run_id = int(request.path_params["run_id"])
        require_run(run_id)
        try:
            return _json(await asyncio.to_thread(supervisor.resume, run_id))
        except SupervisorError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    async def stop_run(request: Request) -> Response:
        run_id = int(request.path_params["run_id"])
        require_run(run_id)
        try:
            return _json(
                await asyncio.to_thread(supervisor.stop, run_id, force=_flag(request, "force"))
            )
        except SupervisorError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    async def download_log(request: Request) -> Response:
        run_id = int(request.path_params["run_id"])
        run = require_run(run_id)
        path = Path(run.get("log_path") or "")
        if not path.exists():
            raise HTTPException(status_code=404, detail="no log file for this run")
        return FileResponse(path, filename=path.name, media_type="application/x-ndjson")

    # ----------------------------------------------------------- websockets

    async def ws_system(websocket: WebSocket) -> None:
        await websocket.accept()
        queue = hub.subscribe("system")
        try:
            await websocket.send_json({"kind": "hello", "ts": now_iso()})
            while True:
                message = await queue.get()
                await websocket.send_json(message)
        except (WebSocketDisconnect, RuntimeError, asyncio.CancelledError):
            pass
        finally:
            hub.unsubscribe("system", queue)

    async def ws_run(websocket: WebSocket) -> None:
        run_id = int(websocket.path_params["run_id"])
        await websocket.accept()
        topic = f"run:{run_id}"
        queue = hub.subscribe(topic)
        try:
            run = store.get_run(run_id)
            backlog = hub.replay(topic, settings.console_buffer_lines)
            if not backlog and run:
                backlog = read_log(run.get("log_path"), settings.console_buffer_lines)
            await websocket.send_json(
                json.loads(
                    json.dumps(
                        {"kind": "snapshot", "ts": now_iso(), "run": run, "events": backlog},
                        default=str,
                    )
                )
            )
            while True:
                message = await queue.get()
                await websocket.send_json(json.loads(json.dumps(message, default=str)))
        except (WebSocketDisconnect, RuntimeError, asyncio.CancelledError):
            pass
        finally:
            hub.unsubscribe(topic, queue)

    # ------------------------------------------------------------------- ui
    #
    # One console, served straight from the package: plain ES modules, no build
    # step, no Node. Everything under static/ is fair game; anything else falls
    # through to index.html so the hash router can take it.

    async def spa(request: Request) -> Response:
        rel = request.path_params.get("path", "") or ""
        if rel:
            candidate = (UI_DIR / rel).resolve()
            if candidate.is_file() and UI_DIR.resolve() in candidate.parents:
                return FileResponse(candidate)
        index = UI_DIR / "index.html"
        if index.exists():
            return FileResponse(index)
        return HTMLResponse(
            "<html><body style='font-family:system-ui;padding:3rem'>"
            "<h1>GraETL</h1><p>API is running. The UI was not found.</p></body></html>"
        )

    # ----------------------------------------------------------------- routes

    # ------------------------------------------------------------- project

    async def project_info(request: Request) -> Response:
        """What the console needs to draw the header - or the picker."""
        if not settings.has_project:
            return _json({"open": False, "recent": recent_projects()})
        project = settings.require_project()
        payload = project.to_dict()
        payload["open"] = store.ready
        try:
            store.list_pipelines()
            payload["target"]["reachable"] = True
        except Exception as exc:  # noqa: BLE001 - shown to the user verbatim
            payload["target"]["reachable"] = False
            payload["target"]["error"] = str(exc)
        return _json(payload)

    async def project_logo(request: Request) -> Response:
        path = settings.project.logo_path if settings.has_project else None
        if path is None:
            raise HTTPException(status_code=404, detail="this project has no logo")
        return FileResponse(path)

    async def project_recent(request: Request) -> Response:
        return _json({"recent": recent_projects()})

    async def project_open(request: Request) -> Response:
        """Finish startup by choosing a project.

        A GraETL instance opens exactly one project, so this only ever
        completes an instance that started without one; swapping projects means
        restarting.
        """
        if store.ready:
            raise HTTPException(
                status_code=409,
                detail="this instance already has a project open - restart to change it",
            )
        body = await request.json()
        path = str((body or {}).get("path") or "").strip()
        if not path:
            raise HTTPException(status_code=400, detail="path is required")
        try:
            project = Project.load(path)
        except ProjectError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        try:
            open_project(project)
        except Exception as exc:  # noqa: BLE001 - usually the target being down
            settings.project = None
            store.unbind()
            supervisor.unbind()
            raise HTTPException(
                status_code=502, detail=f"could not open the target database: {exc}"
            ) from exc
        supervisor.bind_loop(asyncio.get_running_loop())
        await asyncio.to_thread(supervisor.sync_pipelines)
        return _json(project.to_dict())

    async def project_create(request: Request) -> Response:
        body = await request.json() or {}
        try:
            project = Project.create(
                str(body.get("path") or ""),
                name=body.get("name") or None,
                title=str(body.get("title") or ""),
                system=str(body.get("system") or "sqlite"),
                dsn=str(body.get("dsn") or ""),
                schema=str(body.get("schema") or "graetl"),
            )
        except ProjectError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except OSError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not store.ready:
            try:
                open_project(project)
            except Exception as exc:  # noqa: BLE001
                settings.project = None
                store.unbind()
                supervisor.unbind()
                raise HTTPException(
                    status_code=502, detail=f"created, but the target is unreachable: {exc}"
                ) from exc
            supervisor.bind_loop(asyncio.get_running_loop())
        return _json(project.to_dict(), status_code=201)

    async def project_forget(request: Request) -> Response:
        body = await request.json() or {}
        forget_project(str(body.get("path") or ""))
        return _json({"ok": True, "recent": recent_projects()})

    async def project_browse(request: Request) -> Response:
        """List folders so the picker can walk the disk without a native dialog."""
        raw = _qp(request, "path") or ""
        base = Path(raw).expanduser() if raw else Path.home()
        try:
            base = base.resolve()
            entries = sorted(
                (p for p in base.iterdir() if p.is_dir() and not p.name.startswith(".")),
                key=lambda p: p.name.lower(),
            )
        except OSError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _json(
            {
                "path": str(base),
                "parent": str(base.parent) if base.parent != base else None,
                "entries": [
                    {
                        "name": p.name,
                        "path": str(p),
                        "project": (p / "project.toml").exists(),
                    }
                    for p in entries[:500]
                ],
            }
        )

    routes = [
        Route("/api/health", health),
        Route("/api/project", project_info),
        Route("/api/project/logo", project_logo),
        Route("/api/project/open", project_open, methods=["POST"]),
        Route("/api/project/create", project_create, methods=["POST"]),
        Route("/api/project/forget", project_forget, methods=["POST"]),
        Route("/api/project/browse", project_browse),
        Route("/api/projects/recent", project_recent),
        Route("/api/overview", overview),
        Route("/api/pipelines", list_pipelines),
        Route("/api/pipelines", create_pipeline, methods=["POST"]),
        Route("/api/pipelines/sync", sync_pipelines, methods=["POST"]),
        Route("/api/pipelines/{pipeline_id}", get_pipeline),
        Route("/api/pipelines/{pipeline_id}", update_pipeline, methods=["PATCH"]),
        Route("/api/pipelines/{pipeline_id}", delete_pipeline, methods=["DELETE"]),
        Route("/api/pipelines/{pipeline_id}/modules", create_module, methods=["POST"]),
        Route("/api/pipelines/{pipeline_id}/files", list_files),
        Route("/api/pipelines/{pipeline_id}/file", read_file),
        Route("/api/pipelines/{pipeline_id}/file/new", create_file, methods=["POST"]),
        Route("/api/pipelines/{pipeline_id}/file/check", check_file, methods=["POST"]),
        Route("/api/pipelines/{pipeline_id}/file", write_file, methods=["PUT"]),
        Route("/api/pipelines/{pipeline_id}/file/move", move_file, methods=["POST"]),
        Route("/api/pipelines/{pipeline_id}/file", delete_file, methods=["DELETE"]),
        Route("/api/pipelines/{pipeline_id}/folder", create_folder, methods=["POST"]),
        Route("/api/pipelines/{pipeline_id}/upload", upload_file, methods=["PUT"]),
        Route("/api/pipelines/{pipeline_id}/graphs", list_graphs),
        Route("/api/pipelines/{pipeline_id}/graphs", create_graph, methods=["POST"]),
        Route("/api/pipelines/{pipeline_id}/graphs/compile", compile_graphs, methods=["POST"]),
        Route("/api/pipelines/{pipeline_id}/graph", read_graph),
        Route("/api/pipelines/{pipeline_id}/graph", write_graph, methods=["PUT"]),
        Route("/api/pipelines/{pipeline_id}/graph/preview", preview_graph),
        Route("/api/pipelines/{pipeline_id}/graph/preview", preview_graph, methods=["POST"]),
        Route("/api/pipelines/{pipeline_id}/nodes", node_catalog),
        Route("/api/pipelines/{pipeline_id}/entities", list_entities),
        Route("/api/pipelines/{pipeline_id}/entities/{entity_id}", get_entity),
        Route(
            "/api/pipelines/{pipeline_id}/entities/{entity_id}/reset",
            reset_entity,
            methods=["POST"],
        ),
        Route("/api/pipelines/{pipeline_id}/state/reset", reset_state, methods=["POST"]),
        Route("/api/pipelines/{pipeline_id}/runs", list_pipeline_runs),
        Route("/api/pipelines/{pipeline_id}/runs", start_run, methods=["POST"]),
        Route("/api/runs", list_runs),
        Route("/api/runs/{run_id:int}", get_run),
        Route("/api/runs/{run_id:int}/steps", get_steps),
        Route("/api/runs/{run_id:int}/console", get_console),
        Route("/api/runs/{run_id:int}/pause", pause_run, methods=["POST"]),
        Route("/api/runs/{run_id:int}/resume", resume_run, methods=["POST"]),
        Route("/api/runs/{run_id:int}/stop", stop_run, methods=["POST"]),
        Route("/api/runs/{run_id:int}/log/download", download_log),
        WebSocketRoute("/api/ws/system", ws_system),
        WebSocketRoute("/api/ws/runs/{run_id:int}", ws_run),
    ]
    if (UI_DIR / "assets").is_dir():
        routes.append(Mount("/assets", StaticFiles(directory=UI_DIR / "assets"), name="assets"))
    # A vendored Monaco is served from the package, so an air-gapped install can
    # have the full editor without any network at all.
    if VENDOR_DIR.is_dir():
        routes.append(Mount("/vendor", StaticFiles(directory=VENDOR_DIR), name="vendor"))
    routes.append(Route("/{path:path}", spa))

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        # Without a project there is nothing to reap or sync yet; the console
        # shows the picker and project_open() does this work when one is chosen.
        if store.ready:
            supervisor.bind_loop(asyncio.get_running_loop())
            for run_id in store.reap_stale_runs():
                store.add_event(
                    run_id, kind="status", level="warning", message="Marked as crashed at startup."
                )
            await asyncio.to_thread(supervisor.sync_pipelines)
        try:
            yield
        finally:
            if store.ready:
                await asyncio.to_thread(supervisor.shutdown)
                store.close()

    async def http_exception(request: Request, exc: HTTPException) -> Response:
        return _json({"detail": exc.detail}, status_code=exc.status_code)

    app = Starlette(
        debug=False,
        routes=routes,
        lifespan=lifespan,
        middleware=[
            Middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_methods=["*"],
                allow_headers=["*"],
            )
        ],
        exception_handlers={HTTPException: http_exception},
    )
    app.state.settings = settings
    app.state.store = store
    app.state.hub = hub
    app.state.supervisor = supervisor
    return app


def get_app() -> Starlette:  # pragma: no cover - uvicorn factory entry point
    return create_app()
