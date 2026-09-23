"""``graetl`` command line interface."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from graetl import __version__
from graetl.config import CONFIG_FILENAME, MONACO_VERSION, load_settings
from graetl.project import Project, ProjectError, recent_projects, remember_project
from graetl.loader import (
    discover_folders,
    inspect_folder,
    load_pipeline,
    scaffold_module,
    scaffold_pipeline,
)
from graetl.runner.events import EventWriter
from graetl.runner.executor import Executor
from graetl.store.core import CoreStore, RunStatus
from graetl.store.state import StateStore

LEVEL_COLORS = {
    "debug": "\033[90m",
    "info": "\033[0m",
    "success": "\033[92m",
    "warning": "\033[93m",
    "error": "\033[91m",
}
RESET = "\033[0m"

DEFAULT_CONFIG = """# GraETL instance configuration.
# This file belongs to the installation, not to any one project: a project
# (a data warehouse) carries its own project.toml and its own git repository.
[server]
host = "127.0.0.1"
port = 8777

[project]
# The project this instance opens when none is given on the command line.
# path = "../warehouse-sicdb"

[runtime]
log_retention_runs = 50
console_buffer_lines = 2000
stop_grace_seconds = 20
"""


def _color(text: str, level: str) -> str:
    if not sys.stdout.isatty():
        return text
    return f"{LEVEL_COLORS.get(level, '')}{text}{RESET}"


class ConsoleWriter(EventWriter):
    """EventWriter that prints human readable lines instead of the JSON protocol."""

    def __init__(self) -> None:  # noqa: D107
        super().__init__(stream=sys.stdout)

    def emit(self, kind: str, payload=None) -> None:  # type: ignore[override]
        payload = payload or {}
        if kind == "log":
            level = payload.get("level", "info")
            step = payload.get("step")
            prefix = f"[{step}] " if step else ""
            print(_color(f"{prefix}{payload.get('message', '')}", level))
        elif kind == "step_start":
            selected = payload.get("selected")
            extra = f" ({selected} item(s))" if selected else ""
            print(_color(f"-> {payload.get('step')}{extra}", "info"))
        elif kind == "step_end":
            status = payload.get("status", "succeeded")
            print(
                _color(
                    f"<- {payload.get('step')}: {status} "
                    f"processed={payload.get('processed', 0)} "
                    f"skipped={payload.get('skipped', 0)} "
                    f"failed={payload.get('failed', 0)} "
                    f"in {payload.get('duration_ms', 0)} ms",
                    "error" if status == "failed" else "success",
                )
            )
        elif kind == "run_end":
            status = payload.get("status")
            print(_color(f"\nRun {status}", "success" if status == "succeeded" else "error"))
            if payload.get("error"):
                print(_color(str(payload["error"]), "error"))


# ---------------------------------------------------------------- commands


def cmd_init(args: argparse.Namespace) -> int:
    """Write an instance configuration. Projects are made with new-project."""
    root = Path(args.path or ".").resolve()
    root.mkdir(parents=True, exist_ok=True)
    cfg = root / CONFIG_FILENAME
    if not cfg.exists():
        cfg.write_text(DEFAULT_CONFIG, encoding="utf-8")
    print(f"GraETL instance configured at {root}")
    print(f"  config  {cfg}")
    print("Create a warehouse with: graetl new-project <folder>")
    return 0


def cmd_new_project(args: argparse.Namespace) -> int:
    root = Path(args.path).resolve()
    try:
        project = Project.create(
            root,
            name=args.name,
            title=args.title or "",
            system=args.target,
            dsn=args.dsn or "",
            schema=args.schema,
        )
    except ProjectError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    # A project is data, and data wants history: the target and produced files
    # are already ignored by the generated .gitignore.
    if not args.no_git and not (root / ".git").exists():
        import subprocess

        try:
            subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root, check=True)
            print("  git       initialised (nothing committed yet)")
        except (OSError, subprocess.CalledProcessError):
            print("  git       not initialised (git unavailable)")

    settings = load_settings(args.root, project=root)
    try:
        store = settings.core_store()
        store.close()
        reachable = f"ready ({project.target.describe()})"
    except Exception as exc:  # noqa: BLE001 - the target may legitimately be down
        reachable = f"NOT reachable yet: {exc}"
    remember_project(project)
    print(f"GraETL project created at {root}")
    print(f"  config    {project.config_path}")
    print(f"  pipelines {project.pipelines_dir}")
    print(f"  files     {project.files_root}")
    print(f"  target    {reachable}")
    return 0


def cmd_import_legacy(args: argparse.Namespace) -> int:
    """Bring a pre-project installation's pipelines folder into a project."""
    from graetl.migrate import describe_legacy, import_legacy

    settings = load_settings(args.root, project=args.project, require_project=True)
    project = settings.require_project()
    found = describe_legacy(args.old)
    if not found["pipelines"] and not found["core_db"]:
        print(f"nothing to import from {found['root']}", file=sys.stderr)
        return 1
    print(f"Importing {found['root']} into {project.title} ({project.target.describe()})")
    for entry in found["pipelines"]:
        print(f"  {entry['id']:<24} {entry['entities']:>9} entities  "
              f"{entry['bytes'] / 1e6:>8.1f} MB  "
              f"{len(entry['data_tables'])} data table(s)")
    if args.dry_run:
        print("(dry run - nothing was written)")
    report = import_legacy(
        args.old, project, move_data=not args.no_data, dry_run=args.dry_run
    )
    print()
    for line in report.lines():
        print("  " + line)
    if report.collisions:
        print("\nSome table names appeared in more than one pipeline and were left alone;"
              "\nrename them in the old folder and import again.", file=sys.stderr)
        return 2
    return 0


def cmd_projects(args: argparse.Namespace) -> int:
    entries = recent_projects()
    if not entries:
        print("No projects opened yet. Create one with: graetl new-project <folder>")
        return 0
    for entry in entries:
        print(f"{entry.get('title') or entry.get('name'):<32} "
              f"{entry.get('target', '?'):<9} {entry['root']}")
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    """Say what this instance is, what it has open, and whether it works."""
    settings = load_settings(args.root, project=args.project)
    print(f"GraETL {__version__}")
    print(f"  instance  {settings.root}")
    if not settings.has_project:
        print("  project   none open")
        print("            pass one to `graetl serve <project>` or set GRAETL_PROJECT")
        return 1
    project = settings.project
    assert project is not None
    print(f"  project   {project.title} ({project.root})")
    print(f"  pipelines {project.pipelines_dir}")
    print(f"  files     {project.files_root}")
    print(f"  target    {project.target.describe()}")
    if project.target.is_postgres:
        from graetl.store.db import postgres_driver

        try:
            _module, name = postgres_driver()
            note = "" if name != "pgwire" else "  (bundled fallback; pip install psycopg for more)"
            print(f"  driver    {name}{note}")
        except Exception as exc:  # noqa: BLE001
            print(f"  driver    unavailable: {exc}")
    try:
        store = settings.core_store()
        n = len(store.list_pipelines())
        store.close()
        print(f"  database  reachable, {n} pipeline(s) registered")
    except Exception as exc:  # noqa: BLE001
        print(f"  database  UNREACHABLE: {exc}")
        return 1
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    settings = load_settings(
        args.root, project=getattr(args, "project_path", None) or args.project
    )
    from graetl.server.app import create_app

    host = args.host or settings.host
    port = args.port or settings.port
    print(f"GraETL {__version__} - http://{host}:{port}")
    if settings.project is not None:
        print(f"  project {settings.project.title} - {settings.project.root}")
        print(f"  target  {settings.project.target.describe()}")
    else:
        print("  no project open - choose one in the browser")
    uvicorn.run(create_app(settings), host=host, port=port, log_level=args.log_level)
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    settings = load_settings(args.root, project=args.project)
    folders = discover_folders(settings)
    if not folders:
        print(f"No pipelines found in {settings.pipelines_dir}")
        return 0
    for folder in folders:
        info = inspect_folder(folder.path, pipeline_id=folder.id)
        if info.get("ok"):
            d = info["definition"]
            kind = "stateful" if d["stateful"] else "stateless"
            print(
                f"{folder.id:<28} {kind:<10} "
                f"{len(d['modules'])} module(s), {len(d['tasks'])} task(s)  {d['title']}"
            )
        else:
            print(f"{folder.id:<28} ERROR      {info.get('error')}")
    return 0


def cmd_new(args: argparse.Namespace) -> int:
    settings = load_settings(args.root, project=args.project)
    folder = scaffold_pipeline(
        settings,
        args.id,
        title=args.title,
        description=args.description or "",
        template=args.template,
    )
    print(f"Created pipeline {args.id} at {folder}")
    return 0


def cmd_new_module(args: argparse.Namespace) -> int:
    settings = load_settings(args.root, project=args.project)
    pipeline_dir = settings.pipeline_dir(args.pipeline)
    if not pipeline_dir.is_dir():
        print(f"no such pipeline: {args.pipeline}")
        return 1
    folder = scaffold_module(pipeline_dir, args.name, title=args.title)
    print(f"Created module folder {folder}")
    return 0


def cmd_inspect(args: argparse.Namespace) -> int:
    settings = load_settings(args.root, project=args.project)
    info = inspect_folder(settings.pipeline_dir(args.id), pipeline_id=args.id)
    print(json.dumps(info, indent=2))
    return 0 if info.get("ok") else 1


def cmd_run(args: argparse.Namespace) -> int:
    """Run a pipeline in the foreground (developer mode - same process)."""
    settings = load_settings(args.root, project=args.project)
    store = settings.core_store()
    try:
        loaded = load_pipeline(settings.pipeline_dir(args.id), pipeline_id=args.id)
        store.upsert_pipeline(
            pipeline_id=args.id,
            title=loaded.pipeline.title,
            description=loaded.pipeline.description,
            folder=str(loaded.folder),
            stateful=loaded.pipeline.stateful,
            tags=loaded.pipeline.tags,
            definition=loaded.pipeline.to_dict(),
        )
        run = store.create_run(pipeline_id=args.id, mode=args.mode, trigger="cli")
        run_id = int(run["id"])
        from graetl.utils import now_iso

        store.update_run(run_id, status=RunStatus.RUNNING, started_at=now_iso())
        params = {"profile": bool(args.profile), "debug": bool(args.debug)}
        if args.steps:
            params["steps"] = args.steps
        if args.entity:
            params["entity_ids"] = args.entity
        if args.limit:
            params["limit_entities"] = int(args.limit)
            params["sample"] = args.sample
        if args.parallel:
            params["parallel"] = int(args.parallel)
        executor = Executor(
            loaded, settings, run_id=run_id, mode=args.mode, params=params, writer=ConsoleWriter()
        )
        result = executor.run()
        status = {
            "succeeded": RunStatus.SUCCEEDED,
            "failed": RunStatus.FAILED,
            "stopped": RunStatus.STOPPED,
        }.get(result.status, RunStatus.FAILED)
        store.finish_run(run_id, status=status, error=result.error, metrics=result.metrics)
        print(json.dumps(
            {k: v for k, v in result.metrics.items() if k not in ("steps",)}, indent=2, default=str
        ))
        return 0 if status == RunStatus.SUCCEEDED else 1
    finally:
        store.close()


def cmd_runs(args: argparse.Namespace) -> int:
    settings = load_settings(args.root, project=args.project)
    store = settings.core_store()
    try:
        for run in store.list_runs(pipeline_id=args.id, limit=args.limit):
            print(
                f"#{run['id']:<6} {run['pipeline_id']:<24} {run['status']:<10} "
                f"{run['mode']:<12} {run.get('duration_ms') or '-':>8} ms  "
                f"{run.get('finished_at') or run.get('created_at')}"
            )
    finally:
        store.close()
    return 0


def cmd_state(args: argparse.Namespace) -> int:
    settings = load_settings(args.root, project=args.project)
    with settings.state_store(args.id) as state:
        print(f"entities: {state.count_entities()}")
        for row in state.module_summary():
            parts = ", ".join(f"{k}={v}" for k, v in row.items() if k not in ("module", "total"))
            print(f"  {row['module']:<24} total={row['total']}  {parts}")
    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    settings = load_settings(args.root, project=args.project)
    with settings.state_store(args.id) as state:
        if args.module:
            n = state.reset_module(args.module)
            print(f"reset {n} state row(s) for module {args.module}")
        else:
            state.reset_all(drop_entities=args.drop_entities)
            print("reset all module state" + (" and entities" if args.drop_entities else ""))
    return 0


def cmd_vendor_monaco(args: argparse.Namespace) -> int:
    """Put a copy of the Monaco editor inside the install, for offline use.

    The UI prefers ``static/vendor/vs`` over any CDN, so after this the editor
    works on a machine with no internet at all. Either copy an existing
    ``node_modules/monaco-editor/min/vs`` with ``--from``, or let this download
    the npm tarball.
    """
    import shutil
    import tarfile
    import tempfile
    import urllib.request

    from graetl.server.app import VENDOR_DIR

    target = VENDOR_DIR / "vs"
    if target.exists():
        if not args.force:
            print(f"Monaco is already vendored at {target} (use --force to replace it)")
            return 0
        shutil.rmtree(target)
    target.parent.mkdir(parents=True, exist_ok=True)

    if args.source:
        source = Path(args.source).expanduser().resolve()
        if source.name != "vs":
            for candidate in (source / "min" / "vs", source / "vs"):
                if candidate.is_dir():
                    source = candidate
                    break
        if not (source / "loader.js").is_file():
            print(f"no Monaco loader.js under {source}")
            return 1
        shutil.copytree(source, target)
        print(f"Copied Monaco from {source} to {target}")
        return 0

    version = args.monaco_version
    url = f"https://registry.npmjs.org/monaco-editor/-/monaco-editor-{version}.tgz"
    print(f"Downloading {url}")
    with tempfile.TemporaryDirectory() as tmp:
        archive = Path(tmp) / "monaco.tgz"
        try:
            with urllib.request.urlopen(url, timeout=120) as response:
                archive.write_bytes(response.read())
        except OSError as exc:
            print(f"download failed: {exc}")
            print("Offline? Install monaco-editor with npm somewhere and use --from.")
            return 1
        with tarfile.open(archive) as tar:
            prefix = "package/min/vs/"
            members = [m for m in tar.getmembers() if m.name.startswith(prefix)]
            if not members:
                print("tarball did not contain package/min/vs")
                return 1
            for member in members:
                rel = Path(member.name[len(prefix):])
                if member.isdir() or not rel.parts:
                    continue
                dest = target / rel
                if target.resolve() not in dest.resolve().parents:
                    continue  # never write outside the vendor folder
                dest.parent.mkdir(parents=True, exist_ok=True)
                extracted = tar.extractfile(member)
                if extracted is not None:
                    dest.write_bytes(extracted.read())
    total = sum(f.stat().st_size for f in target.rglob("*") if f.is_file())
    print(f"Vendored Monaco {version} into {target} ({total // 1024} KB)")
    print("Restart the server; the editor now loads without internet access.")
    return 0


def cmd_compile(args: argparse.Namespace) -> int:
    """Compile every graph in a pipeline into Python beside it."""
    from graetl.config import load_pipeline_config
    from graetl.graph.build import build_pipeline_graphs, clean_generated

    settings = load_settings(args.root, project=args.project)
    folder = settings.pipeline_dir(args.id)
    if not folder.is_dir():
        print(f"no such pipeline: {args.id}")
        return 1

    if args.clean:
        for path in clean_generated(folder):
            print(f"removed {path.name} (its graph is gone)")

    report = build_pipeline_graphs(
        folder,
        pipeline_id=args.id,
        force=args.force,
        write=not args.check,
        config=load_pipeline_config(folder),
    )
    if report.error:
        print(_color(report.error, "error"))
        return 1
    if not report.builds:
        print("no .graph or .graphlib files in this pipeline")
        return 0

    for build in report.builds:
        mark = {"compiled": "->", "current": "  ", "failed": "!!"}.get(build.status, "  ")
        level = {"failed": "error", "compiled": "success"}.get(build.status, "info")
        target = build.output.name if build.output else "-"
        print(_color(f"{mark} {build.source.name:<32} {build.status:<9} {target}", level))
        if build.error:
            print(_color(f"     {build.error}", "error"))
        for warning in build.warnings:
            print(_color(f"     warning: {warning}", "warning"))

    print()
    print(report.summary())
    if args.check and report.changed:
        print(_color("graphs are out of date - run `graetl compile` to update them", "warning"))
        return 1
    return 0 if report.ok else 1


def cmd_graph(args: argparse.Namespace) -> int:
    """Print the Python one graph compiles to, without writing anything."""
    from graetl.config import load_pipeline_config
    from graetl.graph.build import compile_one_graph

    settings = load_settings(args.root, project=args.project)
    folder = settings.pipeline_dir(args.id)
    path = folder / args.path
    if not path.is_file():
        print(f"no such graph: {path}")
        return 1
    try:
        source, warnings = compile_one_graph(
            path, folder=folder, pipeline_id=args.id, config=load_pipeline_config(folder)
        )
    except Exception as exc:  # noqa: BLE001 - this is a CLI
        print(_color(f"{type(exc).__name__}: {exc}", "error"))
        return 1
    for warning in warnings:
        print(_color(f"# warning: {warning}", "warning"))
    print(source, end="")
    return 0


# ------------------------------------------------------------------ parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="graetl", description="GraETL command line")
    parser.add_argument("--version", action="version", version=f"GraETL {__version__}")
    parser.add_argument("--root", default=None, help="instance root (holds graetl.toml)")
    parser.add_argument(
        "--project", default=None, help="project folder (holds project.toml)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("new-project", help="create a warehouse project folder")
    p.add_argument("path")
    p.add_argument("--name", default=None)
    p.add_argument("--title", default=None)
    p.add_argument("--target", default="sqlite", choices=["sqlite", "postgres"])
    p.add_argument("--dsn", default=None, help="libpq DSN when --target postgres")
    p.add_argument("--schema", default="graetl", help="schema for GraETL's own tables")
    p.add_argument("--no-git", action="store_true", help="skip git init")
    p.set_defaults(func=cmd_new_project)

    p = sub.add_parser(
        "import-legacy", help="import a pre-project pipelines folder into this project"
    )
    p.add_argument("old", help="the old pipelines/ folder")
    p.add_argument("--no-data", action="store_true",
                   help="bookkeeping only; leave the warehouse tables behind")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_import_legacy)

    p = sub.add_parser("projects", help="list recently opened projects")
    p.set_defaults(func=cmd_projects)

    p = sub.add_parser("doctor", help="report instance, project and target health")
    p.set_defaults(func=cmd_doctor)

    p = sub.add_parser("init", help="write an instance configuration file")
    p.add_argument("path", nargs="?", default=".")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("serve", help="start the GraETL server + UI")
    # Positional, because "serve this warehouse" is the common case. Omit it and
    # the console shows the project picker.
    p.add_argument("project_path", nargs="?", default=None,
                   help="project folder (holds project.toml); omit to pick one in the UI")
    p.add_argument("--host", default=None)
    p.add_argument("--port", type=int, default=None)
    p.add_argument("--log-level", default="info")
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("list", help="list pipelines")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("new", help="scaffold a new pipeline")
    p.add_argument("id")
    p.add_argument("--title", default=None)
    p.add_argument("--description", default=None)
    p.add_argument(
        "--template", default="stateful", choices=["stateful", "stateless", "empty"]
    )
    p.set_defaults(func=cmd_new)

    p = sub.add_parser("new-module", help="scaffold a module folder inside a pipeline")
    p.add_argument("pipeline")
    p.add_argument("name")
    p.add_argument("--title", default=None)
    p.set_defaults(func=cmd_new_module)

    p = sub.add_parser("inspect", help="print a pipeline definition as JSON")
    p.add_argument("id")
    p.set_defaults(func=cmd_inspect)

    p = sub.add_parser("run", help="run a pipeline in the foreground")
    p.add_argument("id")
    p.add_argument("--mode", default="incremental",
                   choices=["incremental", "full", "retry-failed"])
    p.add_argument("--steps", nargs="*", default=None, help="only these modules/tasks")
    p.add_argument("--entity", nargs="*", default=None, help="only these entity ids")
    p.add_argument("--limit", type=int, default=None, help="stop after N entities")
    p.add_argument("--sample", default="first", choices=["first", "random"])
    p.add_argument("--parallel", type=int, default=None, help="modules per layer at once")
    p.add_argument("--debug", action="store_true", help="emit ctx.debug() output")
    p.add_argument("--profile", action="store_true")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("compile", help="compile a pipeline's node graphs to Python")
    p.add_argument("id")
    p.add_argument("--force", action="store_true", help="recompile even when up to date")
    p.add_argument("--check", action="store_true",
                   help="report what would change without writing (exit 1 if stale)")
    p.add_argument("--clean", action="store_true",
                   help="first delete generated files whose graph is gone")
    p.set_defaults(func=cmd_compile)

    p = sub.add_parser("graph", help="print the Python one graph compiles to")
    p.add_argument("id")
    p.add_argument("path", help="graph file, relative to the pipeline folder")
    p.set_defaults(func=cmd_graph)

    p = sub.add_parser("runs", help="show run history")
    p.add_argument("id", nargs="?", default=None)
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_runs)

    p = sub.add_parser("state", help="show entity state summary")
    p.add_argument("id")
    p.set_defaults(func=cmd_state)

    p = sub.add_parser(
        "vendor-monaco", help="download the code editor into the install for offline use"
    )
    p.add_argument(
        "--from", dest="source", default=None,
        help="copy from an existing folder instead (e.g. node_modules/monaco-editor)",
    )
    p.add_argument("--monaco-version", default=MONACO_VERSION)
    p.add_argument("--force", action="store_true", help="replace an existing vendored copy")
    p.set_defaults(func=cmd_vendor_monaco)

    p = sub.add_parser("reset", help="reset entity/module state")
    p.add_argument("id")
    p.add_argument("--module", default=None)
    p.add_argument("--drop-entities", action="store_true")
    p.set_defaults(func=cmd_reset)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args) or 0)
    except KeyboardInterrupt:  # pragma: no cover
        print("\ninterrupted")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
