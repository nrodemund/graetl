"""GraETL's own bookkeeping: the pipeline registry and the run history.

These tables live in the **project's target database**, in GraETL's namespace
(``graetl_*`` on SQLite, schema ``graetl`` on PostgreSQL) - see
:mod:`graetl.store.db`. Per-entity state is next door in :mod:`graetl.store.state`.

SQL here is written once in SQLite's spelling; ``[[table]]`` markers and ``?``
placeholders are translated per backend by the dialect.
"""

from __future__ import annotations

import functools
import threading
from pathlib import Path
from typing import Any, Iterable

from graetl.store.db import Database, Target, apply_migrations, connect
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
    CREATE TABLE IF NOT EXISTS [[pipelines]] (
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

    CREATE TABLE IF NOT EXISTS [[runs]] (
        id                [[pk]],
        pipeline_id       TEXT NOT NULL REFERENCES [[pipelines]](id) ON DELETE CASCADE,
        parent_run_id     BIGINT REFERENCES [[runs]](id) ON DELETE SET NULL,
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
        stats_json        TEXT,
        log_path          TEXT,
        error             TEXT,
        exit_code         INTEGER,
        created_at        TEXT NOT NULL,
        started_at        TEXT,
        finished_at       TEXT,
        duration_ms       BIGINT
    );
    CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON [[runs]](pipeline_id, id DESC);
    CREATE INDEX IF NOT EXISTS idx_runs_status ON [[runs]](status);

    CREATE TABLE IF NOT EXISTS [[run_steps]] (
        run_id            BIGINT NOT NULL REFERENCES [[runs]](id) ON DELETE CASCADE,
        step              TEXT NOT NULL,
        kind              TEXT NOT NULL,
        version           INTEGER NOT NULL DEFAULT 1,
        status            TEXT NOT NULL DEFAULT 'pending',
        seq               INTEGER NOT NULL DEFAULT 0,
        selected          INTEGER NOT NULL DEFAULT 0,
        processed         INTEGER NOT NULL DEFAULT 0,
        skipped           INTEGER NOT NULL DEFAULT 0,
        failed            INTEGER NOT NULL DEFAULT 0,
        duration_ms       BIGINT NOT NULL DEFAULT 0,
        metrics_json      TEXT NOT NULL DEFAULT '{}',
        error             TEXT,
        started_at        TEXT,
        finished_at       TEXT,
        PRIMARY KEY (run_id, step)
    );

    CREATE TABLE IF NOT EXISTS [[run_events]] (
        id                [[pk]],
        run_id            BIGINT NOT NULL REFERENCES [[runs]](id) ON DELETE CASCADE,
        ts                TEXT NOT NULL,
        level             TEXT NOT NULL DEFAULT 'info',
        kind              TEXT NOT NULL DEFAULT 'status',
        message           TEXT NOT NULL DEFAULT '',
        data_json         TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_run_events_run ON [[run_events]](run_id, id);

    CREATE TABLE IF NOT EXISTS [[settings]] (
        key               TEXT PRIMARY KEY,
        value_json        TEXT NOT NULL,
        updated_at        TEXT NOT NULL
    );
    """,
]


class CoreStore:
    """Thin repository over GraETL's own tables in the project target."""

    def __init__(self, db: Database | str | Path | Target) -> None:
        self.db = _as_database(db)
        self._lock = threading.RLock()
        apply_migrations(self.db, MIGRATIONS, "core")

    @property
    def conn(self) -> Any:
        """The underlying driver connection (diagnostics only)."""
        return self.db.conn

    def close(self) -> None:
        self.db.close()

    def __enter__(self) -> CoreStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _write(self, sql: str, params: Iterable[Any] = ()) -> Any:
        """One statement in its own transaction - the old ``with self.conn``."""
        self.db.begin("immediate")
        try:
            cur = self.db.execute(sql, tuple(params))
            self.db.commit()
            return cur
        except Exception:
            self.db.rollback()
            raise

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
        self._write(
            """
            INSERT INTO [[pipelines]] (id, title, description, folder, stateful, enabled,
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
        row = self.db.fetchone("SELECT * FROM [[pipelines]] WHERE id = ?", (pipeline_id,))
        return _pipeline_row(row) if row else None

    def list_pipelines(self) -> list[dict[str, Any]]:
        rows = self.db.fetchall("SELECT * FROM [[pipelines]] ORDER BY LOWER(title)")
        return [_pipeline_row(r) for r in rows]

    def set_pipeline_enabled(self, pipeline_id: str, enabled: bool) -> None:
        self._write(
            "UPDATE [[pipelines]] SET enabled = ?, updated_at = ? WHERE id = ?",
            (1 if enabled else 0, now_iso(), pipeline_id),
        )

    def delete_pipeline(self, pipeline_id: str) -> None:
        self._write("DELETE FROM [[pipelines]] WHERE id = ?", (pipeline_id,))

    def prune_missing_pipelines(self, seen_ids: Iterable[str]) -> list[str]:
        """Remove registry rows for pipeline folders that no longer exist."""
        seen = set(seen_ids)
        existing = {r["id"] for r in self.db.fetchall("SELECT id FROM [[pipelines]]")}
        gone = sorted(existing - seen)
        for pid in gone:
            self._write("DELETE FROM [[pipelines]] WHERE id = ?", (pid,))
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
        self.db.begin("immediate")
        try:
            run_id = self.db.insert(
                """
                INSERT INTO [[runs]] (pipeline_id, parent_run_id, status, mode, trigger,
                                      params_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pipeline_id,
                    parent_run_id,
                    RunStatus.QUEUED,
                    mode,
                    trigger,
                    dumps(params or {}),
                    ts,
                ),
            )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise
        return self.get_run(run_id)  # type: ignore[return-value]

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        row = self.db.fetchone("SELECT * FROM [[runs]] WHERE id = ?", (run_id,))
        return _run_row(row) if row else None

    def list_runs(
        self,
        *,
        pipeline_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
        statuses: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM [[runs]]"
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
        return [_run_row(r) for r in self.db.fetchall(sql, args)]

    def active_run_for(self, pipeline_id: str) -> dict[str, Any] | None:
        placeholders = ",".join("?" * len(ACTIVE_STATUSES))
        row = self.db.fetchone(
            f"SELECT * FROM [[runs]] WHERE pipeline_id = ? AND status IN ({placeholders}) "
            "ORDER BY id DESC LIMIT 1",
            (pipeline_id, *sorted(ACTIVE_STATUSES)),
        )
        return _run_row(row) if row else None

    def last_finished_run(self, pipeline_id: str) -> dict[str, Any] | None:
        placeholders = ",".join("?" * len(TERMINAL_STATUSES))
        row = self.db.fetchone(
            f"SELECT * FROM [[runs]] WHERE pipeline_id = ? AND status IN ({placeholders}) "
            "ORDER BY id DESC LIMIT 1",
            (pipeline_id, *sorted(TERMINAL_STATUSES)),
        )
        return _run_row(row) if row else None

    def update_run(self, run_id: int, **fields: Any) -> dict[str, Any] | None:
        if not fields:
            return self.get_run(run_id)
        for json_field in ("params_json", "metrics_json", "stats_json"):
            if json_field in fields and not isinstance(fields[json_field], str):
                fields[json_field] = dumps(fields[json_field])
        assignments = ", ".join(f"{k} = ?" for k in fields)
        self._write(
            f"UPDATE [[runs]] SET {assignments} WHERE id = ?", (*fields.values(), run_id)
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
                t0 = datetime.fromisoformat(str(started).replace("Z", "+00:00"))
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
        self._write(
            "UPDATE [[runs]] SET control = ?, control_seq = control_seq + 1 WHERE id = ?",
            (control, run_id),
        )

    def read_control(self, run_id: int) -> tuple[str | None, int]:
        row = self.db.fetchone(
            "SELECT control, control_seq FROM [[runs]] WHERE id = ?", (run_id,)
        )
        if row is None:
            return None, 0
        return row["control"], int(row["control_seq"])

    def heartbeat(
        self,
        run_id: int,
        *,
        phase: str | None = None,
        done: int | None = None,
        total: int | None = None,
    ) -> None:
        fields: dict[str, Any] = {"heartbeat_at": now_iso()}
        if phase is not None:
            fields["phase"] = phase
        if done is not None:
            fields["progress_done"] = done
        if total is not None:
            fields["progress_total"] = total
        assignments = ", ".join(f"{k} = ?" for k in fields)
        self._write(
            f"UPDATE [[runs]] SET {assignments} WHERE id = ?", (*fields.values(), run_id)
        )

    def reap_stale_runs(self, *, alive_pids: set[int] | None = None) -> list[int]:
        """Mark runs that were active while the server died as crashed."""
        placeholders = ",".join("?" * len(ACTIVE_STATUSES))
        rows = self.db.fetchall(
            f"SELECT id, pid FROM [[runs]] WHERE status IN ({placeholders})",
            tuple(sorted(ACTIVE_STATUSES)),
        )
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
        self.db.begin("immediate")
        try:
            self.db.execute(
                """
                INSERT INTO [[run_steps]] (run_id, step, kind, version, seq)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id, step) DO NOTHING
                """,
                (run_id, step, kind, version, seq),
            )
            if fields:
                assignments = ", ".join(f"{k} = ?" for k in fields)
                self.db.execute(
                    f"UPDATE [[run_steps]] SET {assignments} WHERE run_id = ? AND step = ?",
                    (*fields.values(), run_id, step),
                )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def list_steps(self, run_id: int) -> list[dict[str, Any]]:
        rows = self.db.fetchall(
            "SELECT * FROM [[run_steps]] WHERE run_id = ? "
            "ORDER BY COALESCE(started_at, '9999'), seq, step",
            (run_id,),
        )
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
        self._write(
            "INSERT INTO [[run_events]] (run_id, ts, level, kind, message, data_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (run_id, now_iso(), level, kind, message, dumps(data) if data else None),
        )

    def list_events(self, run_id: int, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.db.fetchall(
            "SELECT * FROM [[run_events]] WHERE run_id = ? ORDER BY id DESC LIMIT ?",
            (run_id, limit),
        )
        out = []
        for row in reversed(rows):
            d = dict(row)
            d["data"] = loads(d.pop("data_json", None))
            out.append(d)
        return out

    # ----------------------------------------------------------------- settings

    def get_setting(self, key: str, default: Any = None) -> Any:
        row = self.db.fetchone("SELECT value_json FROM [[settings]] WHERE key = ?", (key,))
        return loads(row["value_json"], default) if row else default

    def set_setting(self, key: str, value: Any) -> None:
        self._write(
            "INSERT INTO [[settings]] (key, value_json, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, "
            "updated_at = excluded.updated_at",
            (key, dumps(value), now_iso()),
        )

    # -------------------------------------------------------------- aggregates

    def pipeline_stats(self, pipeline_id: str, window: int = 20) -> dict[str, Any]:
        rows = self.db.fetchall(
            "SELECT status, duration_ms FROM [[runs]] "
            "WHERE pipeline_id = ? AND finished_at IS NOT NULL ORDER BY id DESC LIMIT ?",
            (pipeline_id, window),
        )
        total = len(rows)
        succeeded = sum(1 for r in rows if r["status"] == RunStatus.SUCCEEDED)
        durations = [r["duration_ms"] for r in rows if r["duration_ms"]]
        return {
            "runs_considered": total,
            "success_rate": round(succeeded / total, 3) if total else None,
            "avg_duration_ms": int(sum(durations) / len(durations)) if durations else None,
        }


def _as_database(db: Database | str | Path | Target) -> Database:
    """Accept an open database, a Target, or a bare SQLite path.

    The path form keeps tests and one-off scripts short - a store pointed at a
    file is simply a project whose target is that file.
    """
    if isinstance(db, Database):
        return db
    if isinstance(db, Target):
        return connect(db)
    return connect(Target(system="sqlite", path=Path(db)))


def _synchronize(cls: type) -> type:
    """Serialize every public method on one connection (server + reader threads)."""

    def wrap(fn):
        @functools.wraps(fn)
        def inner(self, *args, **kwargs):
            with self._lock:
                return fn(self, *args, **kwargs)

        return inner

    for name, attr in list(vars(cls).items()):
        if (
            name.startswith("_")
            or not callable(attr)
            or isinstance(attr, (staticmethod, classmethod, property))
        ):
            continue
        setattr(cls, name, wrap(attr))
    return cls


_synchronize(CoreStore)


def _pipeline_row(row: Any) -> dict[str, Any]:
    d = dict(row)
    d["stateful"] = bool(d.get("stateful"))
    d["enabled"] = bool(d.get("enabled"))
    d["tags"] = loads(d.pop("tags_json", None), [])
    d["definition"] = loads(d.pop("definition_json", None))
    return d


def _run_row(row: Any) -> dict[str, Any]:
    d = dict(row)
    d["params"] = loads(d.pop("params_json", None), {})
    d["metrics"] = loads(d.pop("metrics_json", None), {})
    d["stats"] = loads(d.pop("stats_json", None), {})
    return d
