"""Process supervision: start, observe, pause, resume and stop pipeline runs.

Every run is an isolated OS process (``python -m graetl.runner run ...``). The
server only ever talks to it through
  * ``runs.control`` in ``etl.db``  (pause / resume / stop),
  * its stdout event stream (console, progress, metrics).

That means pipeline code cannot crash the server, and a stuck run can always be
terminated - gracefully first, forcefully after the grace period.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from graetl.config import Settings
from graetl.loader import discover_folders
from graetl.runner.events import parse_line
from graetl.server.hub import EventHub
from graetl.store.core import TERMINAL_STATUSES, CoreStore, RunStatus
from graetl.utils import now_iso

BACKEND_DIR = str(Path(__file__).resolve().parents[2])


class SupervisorError(RuntimeError):
    pass


@dataclass
class RunProcess:
    run_id: int
    pipeline_id: str
    process: subprocess.Popen
    log_path: Path
    started_at: float = field(default_factory=time.time)
    reader: threading.Thread | None = None
    log_file: Any = None
    saw_run_end: bool = False
    final_status: str | None = None
    final_error: str | None = None
    final_metrics: dict[str, Any] = field(default_factory=dict)
    stopping_since: float | None = None
    last_stats_write: float = 0.0


class Supervisor:
    def __init__(self, settings: Settings, store: CoreStore, hub: EventHub) -> None:
        self.settings = settings
        self.store = store
        self.hub = hub
        self.processes: dict[int, RunProcess] = {}
        self._lock = threading.RLock()
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self.hub.bind_loop(loop)

    # ------------------------------------------------------------- discovery

    def inspect(self, folder: Path, pipeline_id: str) -> dict[str, Any]:
        """Read a pipeline definition in a throw-away process.

        Pipeline code is never imported into the server process: a syntax
        error, an infinite loop at import time or a segfaulting driver can only
        take down this short-lived child.
        """
        python = self.settings.python_executable or sys.executable
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        existing = env.get("PYTHONPATH")
        env["PYTHONPATH"] = BACKEND_DIR + (os.pathsep + existing if existing else "")
        try:
            proc = subprocess.run(
                [
                    python,
                    "-B",
                    "-m",
                    "graetl.runner",
                    "inspect",
                    "--folder",
                    str(folder),
                    "--id",
                    pipeline_id,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=60,
                env=env,
                cwd=str(folder),
            )
        except subprocess.TimeoutExpired:
            return {"ok": False, "id": pipeline_id, "error": "inspection timed out after 60s"}
        stdout = (proc.stdout or "").strip()
        if stdout:
            try:
                return json.loads(stdout.splitlines()[-1])
            except ValueError:
                pass
        return {
            "ok": False,
            "id": pipeline_id,
            "error": (proc.stderr or stdout or "inspection failed").strip()[:4000],
        }

    def sync_pipelines(self, *, deep: bool = True) -> list[dict[str, Any]]:
        """Scan the pipelines folder and refresh the registry."""
        folders = discover_folders(self.settings)
        seen: list[str] = []
        for folder in folders:
            seen.append(folder.id)
            info = self.inspect(folder.path, folder.id) if deep else {"ok": True}
            if info.get("ok"):
                definition = info.get("definition", {})
                self.store.upsert_pipeline(
                    pipeline_id=folder.id,
                    title=definition.get("title", folder.id),
                    description=definition.get("description"),
                    folder=str(folder.path),
                    stateful=bool(definition.get("stateful")),
                    tags=definition.get("tags", []),
                    definition=definition,
                    definition_error=None,
                )
            else:
                existing = self.store.get_pipeline(folder.id)
                self.store.upsert_pipeline(
                    pipeline_id=folder.id,
                    title=(existing or {}).get("title") or folder.id,
                    description=(existing or {}).get("description"),
                    folder=str(folder.path),
                    stateful=bool((existing or {}).get("stateful")),
                    tags=(existing or {}).get("tags", []),
                    definition=(existing or {}).get("definition"),
                    definition_error=info.get("error"),
                )
        removed = self.store.prune_missing_pipelines(seen)
        if removed:
            self.hub.publish_threadsafe("system", {"kind": "pipelines_removed", "ids": removed})
        pipelines = self.store.list_pipelines()
        self.hub.publish_threadsafe("system", {"kind": "pipelines_changed"})
        return pipelines

    # ------------------------------------------------------------------ start

    def start_run(
        self,
        pipeline_id: str,
        *,
        mode: str = "incremental",
        params: dict[str, Any] | None = None,
        trigger: str = "manual",
        parent_run_id: int | None = None,
    ) -> dict[str, Any]:
        pipeline = self.store.get_pipeline(pipeline_id)
        if pipeline is None:
            raise SupervisorError(f"unknown pipeline: {pipeline_id}")
        if not pipeline.get("enabled", True):
            raise SupervisorError(f"pipeline {pipeline_id} is disabled")
        with self._lock:
            active = self.store.active_run_for(pipeline_id)
            if active:
                raise SupervisorError(
                    f"pipeline {pipeline_id} already has an active run (#{active['id']})"
                )
            run = self.store.create_run(
                pipeline_id=pipeline_id,
                mode=mode,
                trigger=trigger,
                params=params or {},
                parent_run_id=parent_run_id,
            )
            run_id = int(run["id"])
            log_path = self.settings.run_log_path(pipeline_id, run_id)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            self.store.update_run(
                run_id,
                status=RunStatus.STARTING,
                started_at=now_iso(),
                log_path=str(log_path),
            )
            try:
                process = self._spawn(pipeline_id, run_id, mode, params or {})
            except Exception as exc:  # noqa: BLE001
                self.store.finish_run(
                    run_id, status=RunStatus.FAILED, error=f"could not start run: {exc}"
                )
                self._publish_run(run_id)
                raise SupervisorError(str(exc)) from exc

            rp = RunProcess(
                run_id=run_id,
                pipeline_id=pipeline_id,
                process=process,
                log_path=log_path,
            )
            rp.log_file = log_path.open("a", encoding="utf-8")
            self.processes[run_id] = rp
            self.store.update_run(run_id, pid=process.pid, status=RunStatus.RUNNING)
            self.store.add_event(
                run_id, kind="status", message=f"Run started (pid {process.pid}, mode {mode})"
            )
            rp.reader = threading.Thread(
                target=self._read_output, args=(rp,), name=f"graetl-run-{run_id}", daemon=True
            )
            rp.reader.start()
        self._publish_run(run_id)
        self._prune_logs(pipeline_id)
        return self.store.get_run(run_id)  # type: ignore[return-value]

    def _spawn(self, pipeline_id: str, run_id: int, mode: str, params: dict) -> subprocess.Popen:
        python = self.settings.python_executable or sys.executable
        cmd = [
            python,
            "-u",
            "-B",  # never write __pycache__ into pipeline folders (files are edited live)
            "-m",
            "graetl.runner",
            "run",
            "--root",
            str(self.settings.root),
            "--project",
            str(self.settings.require_project().root),
            "--pipeline",
            pipeline_id,
            "--run-id",
            str(run_id),
            "--mode",
            mode,
            "--params",
            json.dumps(params or {}),
        ]
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        env["GRAETL_ROOT"] = str(self.settings.root)
        existing = env.get("PYTHONPATH")
        env["PYTHONPATH"] = BACKEND_DIR + (os.pathsep + existing if existing else "")

        kwargs: dict[str, Any] = {}
        if os.name == "nt":  # pragma: no cover - Windows only
            kwargs["creationflags"] = (
                subprocess.CREATE_NEW_PROCESS_GROUP | getattr(subprocess, "CREATE_NO_WINDOW", 0)
            )
        else:
            kwargs["start_new_session"] = True

        return subprocess.Popen(
            cmd,
            cwd=str(self.settings.pipeline_dir(pipeline_id)),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            **kwargs,
        )

    # ------------------------------------------------------------ output pump

    def _read_output(self, rp: RunProcess) -> None:
        assert rp.process.stdout is not None
        try:
            for raw in rp.process.stdout:
                event = parse_line(raw)
                if event is None:
                    continue
                try:
                    self._handle_event(rp, event)
                except Exception as exc:  # pragma: no cover  # noqa: BLE001
                    self.hub.publish_threadsafe(
                        f"run:{rp.run_id}",
                        {
                            "kind": "log",
                            "ts": now_iso(),
                            "level": "error",
                            "message": f"supervisor error handling event: {exc}",
                        },
                    )
        except Exception:  # pragma: no cover - pipe broken
            pass
        finally:
            self._finalize(rp)

    def _handle_event(self, rp: RunProcess, event: dict[str, Any]) -> None:
        kind = event.get("kind")
        run_id = rp.run_id

        if rp.log_file is not None:
            try:
                rp.log_file.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")
                rp.log_file.flush()
            except Exception:  # pragma: no cover
                pass

        if kind == "heartbeat":
            self.store.heartbeat(run_id)
            return

        if kind == "resource":
            # Throttled: telemetry arrives every 2s, the row is refreshed every ~6s.
            now = time.time()
            if now - getattr(rp, "last_stats_write", 0.0) > 6.0:
                rp.last_stats_write = now
                stats = {k: v for k, v in event.items() if k not in ("kind", "ts")}
                self.store.update_run(run_id, stats_json=stats)
            self.hub.publish_threadsafe(f"run:{run_id}", event)
            return

        if kind == "run_start":
            self.store.update_run(run_id, status=RunStatus.RUNNING, phase="running")
        elif kind == "status":
            status = event.get("status")
            mapping = {
                "paused": RunStatus.PAUSED,
                "running": RunStatus.RUNNING,
                "stopping": RunStatus.STOPPING,
            }
            if status in mapping:
                self.store.update_run(run_id, status=mapping[status])
                self.store.add_event(run_id, kind="status", message=f"Run {status}")
                self._publish_run(run_id)
        elif kind == "step_start":
            self.store.upsert_step(
                run_id,
                str(event.get("step")),
                kind=str(event.get("step_kind", "module")),
                version=int(event.get("version", 1) or 1),
                status="running",
                selected=int(event.get("selected", 0) or 0),
                started_at=event.get("ts"),
            )
            self.store.heartbeat(run_id, phase=str(event.get("step")))
        elif kind == "step_end":
            self.store.upsert_step(
                run_id,
                str(event.get("step")),
                status=str(event.get("status", "succeeded")),
                selected=int(event.get("selected", 0) or 0),
                processed=int(event.get("processed", 0) or 0),
                skipped=int(event.get("skipped", 0) or 0),
                failed=int(event.get("failed", 0) or 0),
                duration_ms=int(event.get("duration_ms", 0) or 0),
                metrics_json=event.get("metrics") or {},
                error=event.get("error"),
                finished_at=event.get("ts"),
            )
        elif kind == "progress":
            self.store.heartbeat(
                run_id,
                phase=event.get("step"),
                done=int(event.get("done") or 0),
                total=int(event.get("total") or 0),
            )
        elif kind == "run_end":
            rp.saw_run_end = True
            rp.final_status = str(event.get("status", "failed"))
            rp.final_error = event.get("error")
            rp.final_metrics = event.get("metrics") or {}

        self.hub.publish_threadsafe(f"run:{run_id}", event)

    def _finalize(self, rp: RunProcess) -> None:
        code = rp.process.wait()
        try:
            if rp.process.stdout is not None:
                rp.process.stdout.close()
        except Exception:  # pragma: no cover
            pass
        status = rp.final_status
        error = rp.final_error
        if not rp.saw_run_end:
            if rp.stopping_since is not None:
                status, error = RunStatus.STOPPED, "Run was terminated."
            else:
                status = RunStatus.CRASHED
                error = f"Runner process exited unexpectedly (exit code {code})."
        mapped = {
            "succeeded": RunStatus.SUCCEEDED,
            "failed": RunStatus.FAILED,
            "stopped": RunStatus.STOPPED,
        }.get(str(status), status or RunStatus.CRASHED)

        self.store.finish_run(
            rp.run_id, status=mapped, error=error, exit_code=code, metrics=rp.final_metrics
        )
        self.store.add_event(
            rp.run_id,
            kind="status",
            level="info" if mapped == RunStatus.SUCCEEDED else "error",
            message=f"Run finished: {mapped}" + (f" - {error}" if error else ""),
        )
        if rp.log_file is not None:
            try:
                rp.log_file.close()
            except Exception:  # pragma: no cover
                pass
        with self._lock:
            self.processes.pop(rp.run_id, None)
        run = self.store.get_run(rp.run_id)
        self.hub.publish_threadsafe(
            f"run:{rp.run_id}", {"kind": "run_finished", "ts": now_iso(), "run": run}
        )
        self.hub.publish_threadsafe(
            "system", {"kind": "run_changed", "ts": now_iso(), "run": run}
        )

    def _publish_run(self, run_id: int) -> None:
        run = self.store.get_run(run_id)
        self.hub.publish_threadsafe("system", {"kind": "run_changed", "ts": now_iso(), "run": run})

    # ------------------------------------------------------------- lifecycle

    def pause(self, run_id: int) -> dict[str, Any]:
        run = self._require_active(run_id)
        if run["status"] in (RunStatus.PAUSED, RunStatus.PAUSING):
            return run
        self.store.signal(run_id, "pause")
        self.store.update_run(run_id, status=RunStatus.PAUSING)
        self.store.add_event(run_id, kind="control", message="Pause requested")
        self._publish_run(run_id)
        return self.store.get_run(run_id)  # type: ignore[return-value]

    def resume(self, run_id: int) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise SupervisorError(f"unknown run {run_id}")
        if run["status"] in (RunStatus.PAUSED, RunStatus.PAUSING) and run_id in self.processes:
            self.store.signal(run_id, "resume")
            self.store.update_run(run_id, status=RunStatus.RUNNING)
            self.store.add_event(run_id, kind="control", message="Resume requested")
            self._publish_run(run_id)
            return self.store.get_run(run_id)  # type: ignore[return-value]
        if run["status"] in TERMINAL_STATUSES or run_id not in self.processes:
            # The process is gone: continue the work in a fresh run.
            return self.start_run(
                run["pipeline_id"],
                mode="incremental",
                params=run.get("params") or {},
                trigger="resume",
                parent_run_id=run_id,
            )
        raise SupervisorError(f"run {run_id} cannot be resumed from status {run['status']}")

    def stop(self, run_id: int, *, force: bool = False) -> dict[str, Any]:
        run = self._require_active(run_id)
        rp = self.processes.get(run_id)
        if rp is None:
            self.store.finish_run(run_id, status=RunStatus.STOPPED, error="Run was not running.")
            self._publish_run(run_id)
            return self.store.get_run(run_id)  # type: ignore[return-value]
        rp.stopping_since = time.time()
        self.store.signal(run_id, "stop")
        self.store.update_run(run_id, status=RunStatus.STOPPING)
        self.store.add_event(
            run_id, kind="control", message="Stop requested" + (" (force)" if force else "")
        )
        self._publish_run(run_id)
        if force:
            self.kill(run_id)
        else:
            threading.Thread(
                target=self._stop_watchdog, args=(run_id,), daemon=True
            ).start()
        return self.store.get_run(run_id)  # type: ignore[return-value]

    def _stop_watchdog(self, run_id: int) -> None:
        deadline = time.time() + self.settings.stop_grace_seconds
        while time.time() < deadline:
            rp = self.processes.get(run_id)
            if rp is None or rp.process.poll() is not None:
                return
            time.sleep(0.25)
        rp = self.processes.get(run_id)
        if rp is not None and rp.process.poll() is None:
            self.store.add_event(
                run_id,
                kind="control",
                level="warning",
                message=f"Run did not stop within {self.settings.stop_grace_seconds:.0f}s - "
                "terminating the process.",
            )
            self.kill(run_id)

    def kill(self, run_id: int) -> None:
        rp = self.processes.get(run_id)
        if rp is None:
            return
        proc = rp.process
        if proc.poll() is not None:
            return
        try:
            if os.name == "nt":  # pragma: no cover - Windows only
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                    capture_output=True,
                    check=False,
                )
            else:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                time.sleep(1.0)
                if proc.poll() is None:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):  # pragma: no cover
            try:
                proc.kill()
            except Exception:
                pass

    def _require_active(self, run_id: int) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise SupervisorError(f"unknown run {run_id}")
        if run["status"] in TERMINAL_STATUSES:
            raise SupervisorError(f"run {run_id} already finished ({run['status']})")
        return run

    # -------------------------------------------------------------- shutdown

    def shutdown(self, *, timeout: float = 10.0) -> None:
        run_ids = list(self.processes)
        for run_id in run_ids:
            try:
                self.store.signal(run_id, "stop")
            except Exception:  # pragma: no cover
                pass
        deadline = time.time() + timeout
        while time.time() < deadline and self.processes:
            time.sleep(0.2)
        for run_id in list(self.processes):
            self.kill(run_id)

    def _prune_logs(self, pipeline_id: str) -> None:
        keep = self.settings.log_retention_runs
        logs_dir = self.settings.logs_dir(pipeline_id)
        if keep <= 0 or not logs_dir.exists():
            return
        files = sorted(logs_dir.glob("run_*.jsonl"))
        for path in files[:-keep]:
            try:
                path.unlink()
            except OSError:  # pragma: no cover
                pass
