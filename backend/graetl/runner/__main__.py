"""Runner process entry point.

Started by the GraETL server as a *separate OS process* so that pipeline code
can never take the server down:

    python -m graetl.runner run --root <project> --pipeline <id> --run-id <n>

It also serves the cheap ``inspect`` mode used to read a pipeline definition
without executing anything in the server process.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from pathlib import Path

from graetl.config import load_settings
from graetl.loader import inspect_folder, load_pipeline
from graetl.runner.control import ControlWatcher
from graetl.runner.events import EventWriter, StreamCapture
from graetl.runner.executor import Executor

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_STOPPED = 2
EXIT_LOAD_ERROR = 3

_STATUS_EXIT = {"succeeded": EXIT_OK, "failed": EXIT_FAILED, "stopped": EXIT_STOPPED}


def _heartbeat_loop(writer: EventWriter, interval: float, stop: threading.Event) -> None:
    while not stop.wait(interval):
        writer.emit("heartbeat", {"pid": os.getpid()})


def cmd_run(args: argparse.Namespace) -> int:
    settings = load_settings(args.root)
    # Cached pipeline functions size themselves from configuration, before any
    # pipeline code is imported.
    from graetl.sdk import caching

    caching.set_default_maxsize(settings.cache_size)
    writer = EventWriter(sys.__stdout__)

    # Everything the pipeline prints becomes a structured console line.
    out_capture = StreamCapture(writer, "info", "stdout")
    err_capture = StreamCapture(writer, "error", "stderr")
    sys.stdout = out_capture  # type: ignore[assignment]
    sys.stderr = err_capture  # type: ignore[assignment]

    params = json.loads(args.params) if args.params else {}
    stop_heartbeat = threading.Event()
    control = ControlWatcher(
        settings.core_db_path,
        args.run_id,
        poll_seconds=settings.control_poll_seconds,
        on_change=lambda what: writer.emit(
            "status",
            {"status": {"pause": "paused", "resume": "running", "stop": "stopping"}[what]},
        ),
    )

    def _handle_signal(signum, _frame):  # pragma: no cover - platform dependent
        writer.emit("log", {"level": "warning", "message": f"Received signal {signum}; stopping."})
        control.apply("stop")

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _handle_signal)
        except (ValueError, OSError):  # pragma: no cover
            pass

    hb = threading.Thread(
        target=_heartbeat_loop,
        args=(writer, settings.heartbeat_seconds, stop_heartbeat),
        daemon=True,
    )
    hb.start()
    control.start()

    profiler = None
    if params.get("profile"):
        import cProfile

        profiler = cProfile.Profile()
        profiler.enable()

    exit_code = EXIT_FAILED
    try:
        try:
            loaded = load_pipeline(settings.pipeline_dir(args.pipeline), pipeline_id=args.pipeline)
        except Exception as exc:  # noqa: BLE001
            import traceback

            writer.emit(
                "run_end",
                {
                    "status": "failed",
                    "error": f"Could not load pipeline: {type(exc).__name__}: {exc}",
                    "metrics": {},
                    "traceback": traceback.format_exc(limit=8),
                },
            )
            return EXIT_LOAD_ERROR

        executor = Executor(
            loaded,
            settings,
            run_id=args.run_id,
            mode=args.mode,
            params=params,
            writer=writer,
            control=control,
        )
        result = executor.run()
        exit_code = _STATUS_EXIT.get(result.status, EXIT_FAILED)
    finally:
        if profiler is not None:
            profiler.disable()
            _dump_profile(profiler, settings, args.pipeline, args.run_id, writer)
        stop_heartbeat.set()
        control.close()
        try:
            out_capture.flush()
            err_capture.flush()
        finally:
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
    return exit_code


def _dump_profile(profiler, settings, pipeline_id: str, run_id: int, writer: EventWriter) -> None:
    import io
    import pstats

    profiles_dir = settings.profiles_dir(pipeline_id)
    profiles_dir.mkdir(parents=True, exist_ok=True)
    path = profiles_dir / f"run_{run_id:06d}.prof"
    try:
        profiler.dump_stats(str(path))
        buf = io.StringIO()
        stats = pstats.Stats(profiler, stream=buf).sort_stats("cumulative")
        stats.print_stats(25)
        writer.emit(
            "profile",
            {"path": str(path), "top": buf.getvalue()[:8000]},
        )
    except Exception as exc:  # pragma: no cover  # noqa: BLE001
        writer.emit("log", {"level": "warning", "message": f"Could not write profile: {exc}"})


def cmd_inspect(args: argparse.Namespace) -> int:
    folder = Path(args.folder)
    result = inspect_folder(folder, pipeline_id=args.id or folder.name)
    sys.__stdout__.write(json.dumps(result))
    sys.__stdout__.flush()
    return EXIT_OK if result.get("ok") else EXIT_LOAD_ERROR


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m graetl.runner")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="execute one run of a pipeline")
    run.add_argument("--root", default=None, help="project root (contains pipelines/)")
    run.add_argument("--pipeline", required=True)
    run.add_argument("--run-id", type=int, required=True)
    run.add_argument("--mode", default="incremental",
                     choices=["incremental", "full", "retry-failed"])
    run.add_argument("--params", default=None, help="JSON object with run parameters")
    run.set_defaults(func=cmd_run)

    inspect = sub.add_parser("inspect", help="print a pipeline definition as JSON")
    inspect.add_argument("--folder", required=True)
    inspect.add_argument("--id", default=None)
    inspect.set_defaults(func=cmd_inspect)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except KeyboardInterrupt:  # pragma: no cover
        return EXIT_STOPPED


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
