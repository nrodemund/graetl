"""The internal GraETL database: ``pipelines/etl.db``.

Holds the pipeline registry, the run history, per-module run statistics and
the control channel the server uses to talk to running runner processes.
Per-entity state lives in each pipeline's own ``state.db`` (see state.py).
"""

from __future__ import annotations

import functools
import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterable

from graetl.store.sqlite import apply_migrations, connect
from graetl.utils import dumps, loads, now_iso


class RunStatus:
    QUEUED = "queued"
    STARTING = "starting"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    STOPPING = "stopping"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    STOPPED = "stopped"
    CRASHED = "crashed"


TERMINAL_STATUSES = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.STOPPED, RunStatus.CRASHED}
)
ACTIVE_STATUSES = frozenset(
    {
        RunStatus.QUEUED,
        RunStatus.STARTING,
        RunStatus.RUNNING,
        RunStatus.PAUSING,
        RunStatus.PAUSED,
        RunStatus.STOPPING,
    }
)

MIGRATIONS: list[str] = [
    # 1 - initial schema
    """
    CREATE TABLE IF NOT EXISTS pipelines (
        id                TEXT PRIMARY KEY,
        title             TEXT NOT NULL,
        description       TEXT,
        folder            TEXT NOT NULL,
        stateful          INTEGER NOT NULL DEFAULT 0,
        enabled           INTEGER NOT NULL DEFAULT 1,
        tags_json         TEXT NOT NULL DEFAULT '[]',
        definition_json   TEXT,
        definition_error  TEXT,
        definition_hash   TEXT,
        last_seen_at      TEXT,
        created_at        TEXT NOT NULL,
        updated_at        TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS runs (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        pipeline_id       TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
        parent_run_id     INTEGER REFERENCES runs(id) ON DELETE SET NULL,
        status            TEXT NOT NULL,
        mode              TEXT NOT NULL DEFAULT 'incremental',
        trigger           TEXT NOT NULL DEFAULT 'manual',
        params_json       TEXT NOT NULL DEFAULT '{}',
        pid               INTEGER,
        control           TEXT,
        control_seq       INTEGER NOT NULL DEFAULT 0,
        heartbeat_at      TEXT,
        phase             TEXT,
        progress_done     INTEGER NOT NULL DEFAULT 0,
        progress_total    INTEGER NOT NULL DEFAULT 0,
        metrics_json      TEXT NOT NULL DEFAULT '{}',
        log_path          TEXT,
        error             TEXT,
        exit_code         INTEGER,
        created_at        TEXT NOT NULL,
        started_at        TEXT,
        finished_at       TEXT,
        duration_ms       INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON runs(pipeline_id, id DESC);
    CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);

    CREATE TABLE IF NOT EXISTS run_steps (
        run_id            INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
        step              TEXT NOT NULL,
        kind              TEXT NOT NULL,
        version           INTEGER NOT NULL DEFAULT 1,
        status            TEXT NOT NULL DEFAULT 'pending',
        seq               INTEGER NOT NULL DEFAULT 0,
        selected          INTEGER NOT NULL DEFAULT 0,
        processed         INTEGER NOT NULL DEFAULT 0,
        skipped           INTEGER NOT NULL DEFAULT 0,
        failed            INTEGER NOT NULL DEFAULT 0,
        duration_ms       INTEGER NOT NULL DEFAULT 0,
        metrics_json      TEXT NOT NULL DEFAULT '{}',
        error             TEXT,
        started_at        TEXT,
        finished_at       TEXT,
        PRIMARY KEY (run_id, step)
    );

    CREATE TABLE IF NOT EXISTS run_events (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id            INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
        ts                TEXT NOT NULL,
        level             TEXT NOT NULL DEFAULT 'info',
        kind              TEXT NOT NULL DEFAULT 'status',
        message           TEXT NOT NULL DEFAULT '',
        data_json         TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_run_events_run ON run_events(run_id, id);

    CREATE TABLE IF NOT EXISTS settings (
        key               TEXT PRIMARY KEY,
        value_json        TEXT NOT NULL,
        updated_at        TEXT NOT NULL
    );
    """,
    # 2 - live process telemetry of the running runner
    """
    ALTER TABLE runs ADD COLUMN stats_json TEXT;
    """,
]


class CoreStore:
    """Thin repository over ``pipelines/etl.db``."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()
        self.conn: sqlite3.Connection = connect(self.path)
        apply_migrations(self.conn, MIGRATIONS)

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # pragma: no cover - defensive
            pass

    def __enter__(self) -> CoreStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ pipelines

    def upsert_pipeline(
        self,
        *,
        pipeline_id: str,
        title: str,
        folder: str,
        description: str | None = None,
        stateful: bool = False,
        tags: Iterable[str] = (),
        definition: dict[str, Any] | None = None,
        definition_error: str | None = None,
    ) -> dict[str, Any]:
        ts = now_iso()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO pipelines (id, title, description, folder, stateful, enabled,
                                       tags_json, definition_json, definition_error,
                                       last_seen_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    folder = excluded.folder,
                    stateful = excluded.stateful,
                    tags_json = excluded.tags_json,
                    definition_json = excluded.definition_json,
                    definition_error = excluded.definition_error,
                    last_seen_at = excluded.last_seen_at,
                    updated_at = excluded.updated_at
                """,
                (
                    pipeline_id,
                    title,
                    description,
                    folder,
                    1 if stateful else 0,
                    dumps(list(tags)),
                    dumps(definition) if definition is not None else None,
                    definition_error,
                    ts,
                    ts,
                    ts,
                ),
            )
        return self.get_pipeline(pipeline_id)  # type: ignore[return-value]

    def get_pipeline(self, pipeline_id: str) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT * FROM pipelines WHERE id = ?", (pipeline_id,)).fetchone()
        return _pipeline_row(row) if row else None

    def list_pipelines(self) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM pipelines ORDER BY title COLLATE NOCASE").fetchall()
        return [_pipeline_row(r) for r in rows]

    def set_pipeline_enabled(self, pipeline_id: str, enabled: bool) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE pipelines SET enabled = ?, updated_at = ? WHERE id = ?",
                (1 if enabled else 0, now_iso(), pipeline_id),
            )

    def delete_pipeline(self, pipeline_id: str) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM pipelines WHERE id = ?", (pipeline_id,))

    def prune_missing_pipelines(self, seen_ids: Iterable[str]) -> list[str]:
        """Remove registry rows for pipeline folders that no longer exist."""
        seen = set(seen_ids)
        existing = {r["id"] for r in self.conn.execute("SELECT id FROM pipelines").fetchall()}
        gone = sorted(existing - seen)
        if gone:
            with self.conn:
                self.conn.executemany(
                    "DELETE FROM pipelines WHERE id = ?", [(pid,) for pid in gone]
                )
        return gone

    # ----------------------------------------------------------------------- runs

    def create_run(
        self,
        *,
        pipeline_id: str,
        mode: str = "incremental",
        trigger: str = "manual",
        params: dict[str, Any] | None = None,
        parent_run_id: int | None = None,
    ) -> dict[str, Any]:
        ts = now_iso()
        with self.conn:
            cur = self.conn.execute(
                """
                INSERT INTO runs (pipeline_id, parent_run_id, status, mode, trigger,
                                  params_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (pipeline_id, parent_run_id, RunStatus.QUEUED, mode, trigger, dumps(params or {}), ts),
            )
            run_id = int(cur.lastrowid)
        return self.get_run(run_id)  # type: ignore[return-value]

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        return _run_row(row) if row else None

    def list_runs(
        self,
        *,
        pipeline_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
        statuses: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM runs"
        clauses: list[str] = []
        args: list[Any] = []
        if pipeline_id:
            clauses.append("pipeline_id = ?")
            args.append(pipeline_id)
        statuses = list(statuses or [])
        if statuses:
            clauses.append(f"status IN ({','.join('?' * len(statuses))})")
            args.extend(statuses)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
        args.extend([limit, offset])
        return [_run_row(r) for r in self.conn.execute(sql, args).fetchall()]

    def active_run_for(self, pipeline_id: str) -> dict[str, Any] | None:
        placeholders = ",".join("?" * len(ACTIVE_STATUSES))
        row = self.conn.execute(
            f"SELECT * FROM runs WHERE pipeline_id = ? AND status IN ({placeholders}) "
            "ORDER BY id DESC LIMIT 1",
            (pipeline_id, *sorted(ACTIVE_STATUSES)),
        ).fetchone()
        return _run_row(row) if row else None

    def last_finished_run(self, pipeline_id: str) -> dict[str, Any] | None:
        placeholders = ",".join("?" * len(TERMINAL_STATUSES))
        row = self.conn.execute(
            f"SELECT * FROM runs WHERE pipeline_id = ? AND status IN ({placeholders}) "
            "ORDER BY id DESC LIMIT 1",
            (pipeline_id, *sorted(TERMINAL_STATUSES)),
        ).fetchone()
        return _run_row(row) if row else None

    def update_run(self, run_id: int, **fields: Any) -> dict[str, Any] | None:
        if not fields:
            return self.get_run(run_id)
        for json_field in ("params_json", "metrics_json", "stats_json"):
            if json_field in fields and not isinstance(fields[json_field], str):
                fields[json_field] = dumps(fields[json_field])
        assignments = ", ".join(f"{k} = ?" for k in fields)
        with self.conn:
            self.conn.execute(
                f"UPDATE runs SET {assignments} WHERE id = ?", (*fields.values(), run_id)
            )
        return self.get_run(run_id)

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        error: str | None = None,
        exit_code: int | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        run = self.get_run(run_id)
        if run is None:
            return None
        ts = now_iso()
        duration = None
        started = run.get("started_at")
        if started:
            from datetime import datetime

            try:
                t0 = datetime.fromisoformat(started.replace("Z", "+00:00"))
                t1 = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                duration = int((t1 - t0).total_seconds() * 1000)
            except ValueError:  # pragma: no cover
                duration = None
        fields: dict[str, Any] = {
            "status": status,
            "finished_at": ts,
            "duration_ms": duration,
            "control": None,
            "pid": None,
            "phase": None,
        }
        if error is not None:
            fields["error"] = error
        if exit_code is not None:
            fields["exit_code"] = exit_code
        if metrics is not None:
            fields["metrics_json"] = dumps(metrics)
        return self.update_run(run_id, **fields)

    def signal(self, run_id: int, control: str | None) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE runs SET control = ?, control_seq = control_seq + 1 WHERE id = ?",
                (control, run_id),
            )

    def read_control(self, run_id: int) -> tuple[str | None, int]:
        row = self.conn.execute(
            "SELECT control, control_seq FROM runs WHERE id = ?", (run_id,)
        ).fetchone()
        if row is None:
            return None, 0
        return row["control"], int(row["control_seq"])

    def heartbeat(self, run_id: int, *, phase: str | None = None, done: int | None = None,
                  total: int | None = None) -> None:
        fields: dict[str, Any] = {"heartbeat_at": now_iso()}
        if phase is not None:
            fields["phase"] = phase
        if done is not None:
            fields["progress_done"] = done
        if total is not None:
            fields["progress_total"] = total
        assignments = ", ".join(f"{k} = ?" for k in fields)
        with self.conn:
            self.conn.execute(
                f"UPDATE runs SET {assignments} WHERE id = ?", (*fields.values(), run_id)
            )

    def reap_stale_runs(self, *, alive_pids: set[int] | None = None) -> list[int]:
        """Mark runs that were active while the server died as crashed."""
        placeholders = ",".join("?" * len(ACTIVE_STATUSES))
        rows = self.conn.execute(
            f"SELECT id, pid FROM runs WHERE status IN ({placeholders})",
            tuple(sorted(ACTIVE_STATUSES)),
        ).fetchall()
        reaped: list[int] = []
        for row in rows:
            pid = row["pid"]
            if alive_pids is not None and pid in alive_pids:
                continue
            self.finish_run(
                int(row["id"]),
                status=RunStatus.CRASHED,
                error="Run was interrupted (GraETL server restarted or the process died).",
            )
            reaped.append(int(row["id"]))
        return reaped

    # ---------------------------------------------------------------- run steps

    def upsert_step(self, run_id: int, step: str, **fields: Any) -> None:
        kind = fields.pop("kind", "module")
        version = fields.pop("version", 1)
        seq = fields.pop("seq", 0)
        if "metrics_json" in fields and not isinstance(fields["metrics_json"], str):
            fields["metrics_json"] = dumps(fields["metrics_json"])
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO run_steps (run_id, step, kind, version, seq)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id, step) DO NOTHING
                """,
                (run_id, step, kind, version, seq),
            )
            if fields:
                assignments = ", ".join(f"{k} = ?" for k in fields)
                self.conn.execute(
                    f"UPDATE run_steps SET {assignments} WHERE run_id = ? AND step = ?",
                    (*fields.values(), run_id, step),
                )

    def list_steps(self, run_id: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM run_steps WHERE run_id = ? "
            "ORDER BY IFNULL(started_at, '9999'), seq, step",
            (run_id,),
        ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            d["metrics"] = loads(d.pop("metrics_json", None), {})
            out.append(d)
        return out

    # ------------------------------------------------------------------- events

    def add_event(
        self,
        run_id: int,
        *,
        kind: str = "status",
        level: str = "info",
        message: str = "",
        data: dict[str, Any] | None = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO run_events (run_id, ts, level, kind, message, data_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, now_iso(), level, kind, message, dumps(data) if data else None),
            )

    def list_events(self, run_id: int, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM run_events WHERE run_id = ? ORDER BY id DESC LIMIT ?", (run_id, limit)
        ).fetchall()
        out = []
        for row in reversed(rows):
            d = dict(row)
            d["data"] = loads(d.pop("data_json", None))
            out.append(d)
        return out

    # ----------------------------------------------------------------- settings

    def get_setting(self, key: str, default: Any = None) -> Any:
        row = self.conn.execute("SELECT value_json FROM settings WHERE key = ?", (key,)).fetchone()
        return loads(row["value_json"], default) if row else default

    def set_setting(self, key: str, value: Any) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT INTO settings (key, value_json, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, "
                "updated_at = excluded.updated_at",
                (key, dumps(value), now_iso()),
            )

    # -------------------------------------------------------------- aggregates

    def pipeline_stats(self, pipeline_id: str, window: int = 20) -> dict[str, Any]:
        rows = self.conn.execute(
            "SELECT status, duration_ms FROM runs WHERE pipeline_id = ? AND finished_at IS NOT NULL "
            "ORDER BY id DESC LIMIT ?",
            (pipeline_id, window),
        ).fetchall()
        total = len(rows)
        succeeded = sum(1 for r in rows if r["status"] == RunStatus.SUCCEEDED)
        durations = [r["duration_ms"] for r in rows if r["duration_ms"]]
        return {
            "runs_considered": total,
            "success_rate": round(succeeded / total, 3) if total else None,
            "avg_duration_ms": int(sum(durations) / len(durations)) if durations else None,
        }


def _synchronize(cls: type) -> type:
    """Serialize every public method on one connection (server + reader threads)."""

    def wrap(fn):
        @functools.wraps(fn)
        def inner(self, *args, **kwargs):
            with self._lock:
                return fn(self, *args, **kwargs)

        return inner

    for name, attr in list(vars(cls).items()):
        if name.startswith("_") or not callable(attr) or isinstance(attr, (staticmethod, classmethod)):
            continue
        setattr(cls, name, wrap(attr))
    return cls


_synchronize(CoreStore)


def _pipeline_row(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    d["stateful"] = bool(d.get("stateful"))
    d["enabled"] = bool(d.get("enabled"))
    d["tags"] = loads(d.pop("tags_json", None), [])
    d["definition"] = loads(d.pop("definition_json", None))
    return d


def _run_row(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    d["params"] = loads(d.pop("params_json", None), {})
    d["metrics"] = loads(d.pop("metrics_json", None), {})
    d["stats"] = loads(d.pop("stats_json", None), {})
    return d
