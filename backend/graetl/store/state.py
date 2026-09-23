"""Per-entity state: what makes a pipeline resumable.

These tables live in the **project's target database** alongside the warehouse
tables the pipelines write - that co-location is the whole point, see below.
One project holds many pipelines, so every row carries a ``pipeline`` column
and a :class:`StateStore` is scoped to one of them.

``entities``
    One row per business entity the pipeline knows about (an ICU admission, a
    CT session, a customer, ...). ``source_updated_at`` is the **source
    revision**: the point in time the entity last changed in the source system.
    In live systems that value moves forward, which is what makes an already
    processed entity dirty again.

``entity_module_state``
    One row per (entity, module). It records which module *version* processed
    the entity and **which source revision was processed**
    (``processed_source_updated_at``). A module has work to do for an entity when

        * no state row exists, or
        * the row is not ``done``, or
        * the module version changed, or
        * ``processed_source_updated_at < entities.source_updated_at``.

``module_locks``
    One row per module while it is being executed, with a heartbeat. Exactly one
    worker - in this process or any other - may run a given module at a time.

Transactional safety
--------------------
Module code writes its data through the very same connection (``ctx.db``), into
the very same database. ``entity_transaction()`` opens one transaction that
covers *both* the module's data writes and the state row update, so a crash can
never leave "state says done, data was never written". This holds identically on
SQLite and PostgreSQL, which is why GraETL keeps its bookkeeping in the target
rather than in a database of its own. Writes to systems outside this connection
are at-least-once: the state row is only flipped to ``done`` after the module
returns, so an interrupted entity is retried on resume.

Concurrency
-----------
Each module worker owns its own :class:`StateStore` (its own connection). Two
transaction modes:

``immediate``
    Takes the write lock up front. Used when modules run one at a time - no
    contention, no retries, the simplest possible behaviour. On PostgreSQL there
    is nothing to take up front, so this is an ordinary ``BEGIN``.
``deferred``
    Takes the write lock at the first write, so parallel modules only serialise
    around their writes rather than around their whole body. A write conflict
    raises :class:`LockConflict` *before* any state row is written, and the
    caller retries the entity. Nothing is ever recorded as failed because of
    contention.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from graetl.store.db import Database, LockConflict, Target, apply_migrations, connect
from graetl.utils import dumps, loads, now_iso, to_iso

__all__ = [
    "EntityModuleState",
    "LockConflict",
    "StateStore",
    "WorkItem",
    "is_lock_error",
    "worker_id",
]


def is_lock_error(exc: BaseException) -> bool:
    """True for SQLite's "someone else is writing" errors, which are retryable.

    Kept for callers that have no store at hand; a :class:`StateStore` asks its
    own dialect, which also knows PostgreSQL's serialization failures.
    """
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    text = str(exc).lower()
    return "locked" in text or "busy" in text


def worker_id() -> str:
    """Identifies one worker uniquely across threads, processes and machines."""
    return f"{socket.gethostname()}:{os.getpid()}:{threading.get_ident()}:{uuid.uuid4().hex[:8]}"


STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"

MIGRATIONS: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS [[entities]] (
        pipeline           TEXT NOT NULL,
        entity_id          TEXT NOT NULL,
        label              TEXT,
        source_updated_at  TEXT,
        payload_json       TEXT NOT NULL DEFAULT '{}',
        discovered_at      TEXT NOT NULL,
        last_seen_at       TEXT NOT NULL,
        deleted_at         TEXT,
        PRIMARY KEY (pipeline, entity_id)
    );
    CREATE INDEX IF NOT EXISTS idx_entities_seen ON [[entities]](pipeline, last_seen_at);
    CREATE INDEX IF NOT EXISTS idx_entities_revision ON [[entities]](pipeline, source_updated_at);
    CREATE INDEX IF NOT EXISTS idx_entities_discovered ON [[entities]](pipeline, discovered_at);

    CREATE TABLE IF NOT EXISTS [[entity_module_state]] (
        pipeline                     TEXT NOT NULL,
        entity_id                    TEXT NOT NULL,
        module                       TEXT NOT NULL,
        module_version               INTEGER NOT NULL DEFAULT 1,
        status                       TEXT NOT NULL DEFAULT 'pending',
        source_updated_at            TEXT,
        processed_source_updated_at  TEXT,
        processed_at                 TEXT,
        attempts                     INTEGER NOT NULL DEFAULT 0,
        run_id                       BIGINT,
        duration_ms                  BIGINT,
        error                        TEXT,
        result_json                  TEXT,
        updated_at                   TEXT NOT NULL,
        PRIMARY KEY (pipeline, entity_id, module)
    );
    CREATE INDEX IF NOT EXISTS idx_ems_module_status
        ON [[entity_module_state]](pipeline, module, status);
    CREATE INDEX IF NOT EXISTS idx_ems_run ON [[entity_module_state]](run_id);
    CREATE INDEX IF NOT EXISTS idx_ems_entity ON [[entity_module_state]](pipeline, entity_id);

    CREATE TABLE IF NOT EXISTS [[pipeline_meta]] (
        pipeline   TEXT NOT NULL,
        key        TEXT NOT NULL,
        value_json TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (pipeline, key)
    );

    CREATE TABLE IF NOT EXISTS [[module_locks]] (
        pipeline     TEXT NOT NULL,
        module       TEXT NOT NULL,
        owner        TEXT NOT NULL,
        run_id       BIGINT,
        acquired_at  TEXT NOT NULL,
        heartbeat_at TEXT NOT NULL,
        PRIMARY KEY (pipeline, module)
    );
    """,
]

#: A lock whose heartbeat is older than this is considered abandoned.
LOCK_STALE_SECONDS = 90.0


@dataclass(slots=True)
class EntityModuleState:
    entity_id: str
    module: str
    module_version: int
    status: str
    source_updated_at: str | None
    processed_source_updated_at: str | None
    processed_at: str | None
    attempts: int
    error: str | None = None
    result: Any = None


@dataclass(slots=True)
class WorkItem:
    entity_id: str
    label: str | None
    source_updated_at: str | None
    payload: dict[str, Any]
    attempts: int
    previous_status: str | None


class StateStore:
    """Entity state for one pipeline. Also the connection module code writes through."""

    def __init__(self, db: Database | str | Path | Target, pipeline: str = "default") -> None:
        self.db = _as_database(db)
        self.pipeline = pipeline
        #: Identity of this connection, used for module locks.
        self.worker = worker_id()
        apply_migrations(self.db, MIGRATIONS, "state")

    @property
    def conn(self) -> Database:
        """What ``ctx.db`` hands to module code."""
        return self.db

    @property
    def path(self) -> Path | None:
        target = self.db.target
        return target.path if target and not target.is_postgres else None

    def close(self) -> None:
        self.db.close()

    def __enter__(self) -> StateStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _is_lock_error(self, exc: BaseException) -> bool:
        return self.db.dialect.is_lock_error(exc)

    # ---------------------------------------------------------------- entities

    def upsert_entity(
        self,
        entity_id: str,
        *,
        label: str | None = None,
        source_updated_at: Any = None,
        payload: dict[str, Any] | None = None,
    ) -> bool:
        """Insert or refresh an entity. Returns True when it is new or changed."""
        ts = now_iso()
        rev = to_iso(source_updated_at)
        cur = self.db.fetchone(
            "SELECT source_updated_at FROM [[entities]] WHERE pipeline = ? AND entity_id = ?",
            (self.pipeline, entity_id),
        )
        if cur is None:
            self.db.execute(
                "INSERT INTO [[entities]] (pipeline, entity_id, label, source_updated_at, "
                "payload_json, discovered_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (self.pipeline, entity_id, label, rev, dumps(payload or {}), ts, ts),
            )
            return True
        changed = rev is not None and rev != cur["source_updated_at"]
        self.db.execute(
            "UPDATE [[entities]] SET label = COALESCE(?, label), "
            "source_updated_at = COALESCE(?, source_updated_at), "
            "payload_json = COALESCE(?, payload_json), last_seen_at = ?, deleted_at = NULL "
            "WHERE pipeline = ? AND entity_id = ?",
            (
                label,
                rev,
                dumps(payload) if payload is not None else None,
                ts,
                self.pipeline,
                entity_id,
            ),
        )
        return changed

    def upsert_entities(self, entities: Iterable[tuple[str, str | None, Any, dict | None]]) -> dict:
        """Bulk upsert; returns {'new': n, 'changed': n, 'seen': n}."""
        stats = {"new": 0, "changed": 0, "seen": 0}
        self.begin("immediate")
        try:
            for entity_id, label, rev, payload in entities:
                existed = self.db.fetchone(
                    "SELECT 1 AS hit FROM [[entities]] WHERE pipeline = ? AND entity_id = ?",
                    (self.pipeline, entity_id),
                )
                changed = self.upsert_entity(
                    entity_id, label=label, source_updated_at=rev, payload=payload
                )
                stats["seen"] += 1
                if not existed:
                    stats["new"] += 1
                elif changed:
                    stats["changed"] += 1
            self.commit()
        except Exception:
            self.rollback()
            raise
        return stats

    def soft_delete_unseen(self, cutoff_iso: str) -> int:
        cur = self.db.execute(
            "UPDATE [[entities]] SET deleted_at = ? "
            "WHERE pipeline = ? AND deleted_at IS NULL AND last_seen_at < ?",
            (now_iso(), self.pipeline, cutoff_iso),
        )
        return cur.rowcount or 0

    def count_entities(self, include_deleted: bool = False) -> int:
        sql = "SELECT COUNT(*) AS n FROM [[entities]] WHERE pipeline = ?"
        if not include_deleted:
            sql += " AND deleted_at IS NULL"
        return int(self.db.fetchone(sql, (self.pipeline,))["n"])

    def _entity_filter(
        self, search: str | None, status: str | None, module: str | None
    ) -> tuple[list[str], list[Any]]:
        where = ["e.pipeline = ?", "e.deleted_at IS NULL"]
        args: list[Any] = [self.pipeline]
        if search:
            where.append("(e.entity_id [[ilike]] ? OR COALESCE(e.label,'') [[ilike]] ?)")
            args.extend([f"%{search}%", f"%{search}%"])
        if status or module:
            sub = (
                "SELECT 1 FROM [[entity_module_state]] s "
                "WHERE s.pipeline = e.pipeline AND s.entity_id = e.entity_id"
            )
            sub_args: list[Any] = []
            if status:
                sub += " AND s.status = ?"
                sub_args.append(status)
            if module:
                sub += " AND s.module = ?"
                sub_args.append(module)
            where.append(f"EXISTS ({sub})")
            args.extend(sub_args)
        return where, args

    def count_matching_entities(
        self,
        *,
        search: str | None = None,
        status: str | None = None,
        module: str | None = None,
    ) -> int:
        """How many entities match the filters - what the UI pages through."""
        where, args = self._entity_filter(search, status, module)
        sql = "SELECT COUNT(*) AS n FROM [[entities]] e WHERE " + " AND ".join(where)
        return int(self.db.fetchone(sql, args)["n"])

    def list_entities(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        search: str | None = None,
        status: str | None = None,
        module: str | None = None,
        order: str = "entity_id",
    ) -> list[dict[str, Any]]:
        where, args = self._entity_filter(search, status, module)
        order_sql = {
            "entity_id": "e.entity_id",
            "recent": "e.last_seen_at DESC, e.entity_id",
            "discovered": "e.discovered_at DESC, e.entity_id",
            "revision": "e.source_updated_at DESC, e.entity_id",
        }.get(order, "e.entity_id")
        sql = (
            "SELECT e.* FROM [[entities]] e WHERE "
            + " AND ".join(where)
            + f" ORDER BY {order_sql} LIMIT ? OFFSET ?"
        )
        rows = self.db.fetchall(sql, [*args, limit, offset])
        out: list[dict[str, Any]] = []
        ids = [r["entity_id"] for r in rows]
        states = self.states_for(ids)
        for row in rows:
            d = dict(row)
            d.pop("pipeline", None)
            d["payload"] = loads(d.pop("payload_json", None), {})
            d["modules"] = states.get(d["entity_id"], [])
            out.append(d)
        return out

    def states_for(self, entity_ids: Sequence[str]) -> dict[str, list[dict[str, Any]]]:
        if not entity_ids:
            return {}
        placeholders = ",".join("?" * len(entity_ids))
        rows = self.db.fetchall(
            f"SELECT * FROM [[entity_module_state]] WHERE pipeline = ? "
            f"AND entity_id IN ({placeholders}) ORDER BY module",
            (self.pipeline, *entity_ids),
        )
        out: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            d = dict(row)
            d.pop("pipeline", None)
            d["result"] = loads(d.pop("result_json", None))
            out.setdefault(d["entity_id"], []).append(d)
        return out

    def get_entity(self, entity_id: str) -> dict[str, Any] | None:
        row = self.db.fetchone(
            "SELECT * FROM [[entities]] WHERE pipeline = ? AND entity_id = ?",
            (self.pipeline, entity_id),
        )
        if row is None:
            return None
        d = dict(row)
        d.pop("pipeline", None)
        d["payload"] = loads(d.pop("payload_json", None), {})
        d["modules"] = self.states_for([entity_id]).get(entity_id, [])
        return d

    # ------------------------------------------------------------ work queue

    def _work_where(
        self,
        version: int,
        *,
        mode: str,
        requires: Sequence[tuple[str, int | None]],
        depends_on: Sequence[str],
        cascade: bool,
        apply_requirements: bool,
        after: str | None,
        entity_ids: Sequence[str] | None,
    ) -> tuple[str, list[Any]]:
        """The WHERE clause shared by select_work() and count_work()."""
        args: list[Any] = [self.pipeline]
        where = ["e.pipeline = ?", "e.deleted_at IS NULL"]

        if entity_ids:
            where.append(f"e.entity_id IN ({','.join('?' * len(entity_ids))})")
            args.extend(entity_ids)

        if after is not None:
            # Results are ordered by entity_id, so this pages forward through the
            # backlog: an entity already attempted stays behind the cursor.
            where.append("e.entity_id > ?")
            args.append(after)

        if mode == "full":
            pass  # everything is work
        elif mode == "retry-failed":
            where.append("s.status = 'failed'")
        else:  # incremental / resume
            clause = (
                "("
                " s.entity_id IS NULL"
                " OR s.status <> 'done'"
                " OR s.module_version <> ?"
                " OR (e.source_updated_at IS NOT NULL"
                "     AND (s.processed_source_updated_at IS NULL"
                "          OR s.processed_source_updated_at < e.source_updated_at))"
            )
            args.append(version)
            upstream = [name for name, _ in requires] + list(depends_on)
            if cascade and upstream:
                placeholders = ",".join("?" * len(upstream))
                clause += (
                    " OR EXISTS (SELECT 1 FROM [[entity_module_state]] u"
                    "            WHERE u.pipeline = e.pipeline AND u.entity_id = e.entity_id"
                    f"              AND u.module IN ({placeholders})"
                    "              AND u.processed_at IS NOT NULL"
                    "              AND (s.processed_at IS NULL OR u.processed_at > s.processed_at))"
                )
                args.extend(upstream)
            where.append(clause + ")")

        gates = [*requires, *((name, None) for name in depends_on)] if apply_requirements else []
        for dep_name, dep_version in gates:
            clause = (
                "EXISTS (SELECT 1 FROM [[entity_module_state]] d "
                "WHERE d.pipeline = e.pipeline AND d.entity_id = e.entity_id "
                "AND d.module = ? AND d.status IN ('done', 'skipped')"
            )
            args.append(dep_name)
            if dep_version is not None:
                clause += (
                    " AND d.module_version = ?"
                    " AND (e.source_updated_at IS NULL"
                    "      OR (d.processed_source_updated_at IS NOT NULL"
                    "          AND d.processed_source_updated_at >= e.source_updated_at))"
                )
                args.append(dep_version)
            where.append(clause + ")")

        return " AND ".join(where), args

    #: The join that hangs one module's state row off each entity.
    _JOIN = (
        "FROM [[entities]] e LEFT JOIN [[entity_module_state]] s "
        "ON s.pipeline = e.pipeline AND s.entity_id = e.entity_id AND s.module = ?"
    )

    def select_work(
        self,
        module: str,
        version: int,
        *,
        mode: str = "incremental",
        requires: Sequence[tuple[str, int | None]] = (),
        depends_on: Sequence[str] = (),
        cascade: bool = True,
        apply_requirements: bool = True,
        limit: int | None = None,
        after: str | None = None,
        order: str = "entity_id",
        entity_ids: Sequence[str] | None = None,
    ) -> list[WorkItem]:
        """Entities this module still has to process, in a deterministic order.

        ``requires`` lists ``(module, version)`` pairs that must be **up to
        date** for the entity first - processed at that version, at or beyond
        the entity's current source revision. That is what an execution layer
        compiles down to: every module in a lower layer must have caught up
        before this one sees the entity. ``depends_on`` is the looser form: the
        upstream module must simply have finished, at any version.

        ``skipped`` counts as up to date: the module looked at the entity and
        deliberately had nothing to do, so downstream work is not blocked.

        With ``cascade`` (the default), an entity also becomes due when any
        required module processed it *after* this one did - so bumping an
        upstream module's version reprocesses everything derived from it
        instead of leaving stale data behind.

        ``order="random"`` picks an arbitrary sample, which is what a profile
        run over N entities uses.
        """
        where, where_args = self._work_where(
            version,
            mode=mode,
            requires=requires,
            depends_on=depends_on,
            cascade=cascade,
            apply_requirements=apply_requirements,
            after=after,
            entity_ids=entity_ids,
        )
        sql = (
            "SELECT e.entity_id, e.label, e.source_updated_at, e.payload_json, "
            "COALESCE(s.attempts, 0) AS attempts, s.status AS previous_status "
            + self._JOIN
            + " WHERE "
            + where
            + (" ORDER BY random()" if order == "random" else " ORDER BY e.entity_id")
        )
        args: list[Any] = [module, *where_args]
        if limit:
            sql += " LIMIT ?"
            args.append(limit)
        rows = self.db.fetchall(sql, args)
        return [
            WorkItem(
                entity_id=r["entity_id"],
                label=r["label"],
                source_updated_at=r["source_updated_at"],
                payload=loads(r["payload_json"], {}),
                attempts=int(r["attempts"]),
                previous_status=r["previous_status"],
            )
            for r in rows
        ]

    def count_work(
        self,
        module: str,
        version: int,
        *,
        mode: str = "incremental",
        requires: Sequence[tuple[str, int | None]] = (),
        depends_on: Sequence[str] = (),
        cascade: bool = True,
        apply_requirements: bool = True,
        entity_ids: Sequence[str] | None = None,
    ) -> int:
        """How much work a module has, without materialising it (100k-entity safe)."""
        where, where_args = self._work_where(
            version,
            mode=mode,
            requires=requires,
            depends_on=depends_on,
            cascade=cascade,
            apply_requirements=apply_requirements,
            after=None,
            entity_ids=entity_ids,
        )
        sql = "SELECT COUNT(*) AS n " + self._JOIN + " WHERE " + where
        return int(self.db.fetchone(sql, [module, *where_args])["n"])

    # --------------------------------------------------------- state changes

    def mark_running(self, entity_id: str, module: str, version: int, run_id: int | None) -> None:
        ts = now_iso()
        self.begin("immediate")
        self.db.execute(
            """
            INSERT INTO [[entity_module_state]]
                (pipeline, entity_id, module, module_version, status, attempts, run_id, updated_at)
            VALUES (?, ?, ?, ?, 'running', 1, ?, ?)
            ON CONFLICT(pipeline, entity_id, module) DO UPDATE SET
                status = 'running',
                module_version = excluded.module_version,
                attempts = [[entity_module_state]].attempts + 1,
                run_id = excluded.run_id,
                error = NULL,
                updated_at = excluded.updated_at
            """,
            (self.pipeline, entity_id, module, version, run_id, ts),
        )
        self.commit()

    def _write_final(
        self,
        entity_id: str,
        module: str,
        version: int,
        *,
        status: str,
        run_id: int | None,
        source_updated_at: str | None,
        duration_ms: int | None,
        error: str | None,
        result: Any,
    ) -> None:
        ts = now_iso()
        processed_rev = source_updated_at if status in (STATUS_DONE, STATUS_SKIPPED) else None
        self.db.execute(
            """
            INSERT INTO [[entity_module_state]]
                (pipeline, entity_id, module, module_version, status, source_updated_at,
                 processed_source_updated_at, processed_at, attempts, run_id,
                 duration_ms, error, result_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
            ON CONFLICT(pipeline, entity_id, module) DO UPDATE SET
                module_version = excluded.module_version,
                status = excluded.status,
                source_updated_at = excluded.source_updated_at,
                processed_source_updated_at = COALESCE(
                    excluded.processed_source_updated_at,
                    [[entity_module_state]].processed_source_updated_at),
                processed_at = COALESCE(
                    excluded.processed_at, [[entity_module_state]].processed_at),
                run_id = excluded.run_id,
                duration_ms = excluded.duration_ms,
                error = excluded.error,
                result_json = excluded.result_json,
                updated_at = excluded.updated_at
            """,
            (
                self.pipeline,
                entity_id,
                module,
                version,
                status,
                source_updated_at,
                processed_rev,
                ts if status in (STATUS_DONE, STATUS_SKIPPED) else None,
                run_id,
                duration_ms,
                error,
                dumps(result) if result is not None else None,
                ts,
            ),
        )

    @contextmanager
    def entity_transaction(
        self,
        entity_id: str,
        module: str,
        version: int,
        *,
        run_id: int | None,
        source_updated_at: str | None,
        mode: str = "immediate",
    ) -> Iterator[dict[str, Any]]:
        """Run a module for one entity inside a single transaction.

        Module data writes made through ``ctx.db`` and the final state row
        update commit together, or not at all.

        ``mode="deferred"`` (used when modules run in parallel) takes the write
        lock only at the first write. If another worker wins the race, the whole
        entity is rolled back and :class:`LockConflict` is raised **without**
        writing a state row, so the caller can simply try the entity again.
        """
        outcome: dict[str, Any] = {"status": STATUS_DONE, "result": None, "duration_ms": None}
        self.begin(mode)
        try:
            yield outcome
            self._write_final(
                entity_id,
                module,
                version,
                status=outcome.get("status", STATUS_DONE),
                run_id=run_id,
                source_updated_at=source_updated_at,
                duration_ms=outcome.get("duration_ms"),
                error=None,
                result=outcome.get("result"),
            )
            self.commit()
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            self.rollback()
            if self._is_lock_error(exc):
                # Pure contention: nothing happened, nothing is recorded, retry.
                raise LockConflict(str(exc)) from exc
            self._record_failure([_Item(entity_id, source_updated_at)], module, version, exc,
                                 run_id=run_id, duration_ms=outcome.get("duration_ms"))
            raise

    @contextmanager
    def batch_transaction(
        self,
        items: Sequence[Any],
        module: str,
        version: int,
        *,
        run_id: int | None,
        mode: str = "immediate",
    ) -> Iterator[dict[str, Any]]:
        """Run a module for a whole batch of entities inside ONE transaction.

        The batch is the unit: every entity in it is marked done together, or
        none is and the whole batch is retried on the next run. That is the same
        promise :meth:`entity_transaction` makes for one entity - "done but not
        written" stays impossible - and it is why a batch module cannot report
        a per-entity outcome.
        """
        outcome: dict[str, Any] = {"status": STATUS_DONE, "result": None, "duration_ms": None}
        self.begin(mode)
        try:
            yield outcome
            for item in items:
                self._write_final(
                    item.entity_id,
                    module,
                    version,
                    status=outcome.get("status", STATUS_DONE),
                    run_id=run_id,
                    source_updated_at=item.source_updated_at,
                    duration_ms=outcome.get("duration_ms"),
                    error=None,
                    result=outcome.get("result"),
                )
            self.commit()
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            self.rollback()
            if self._is_lock_error(exc):
                raise LockConflict(str(exc)) from exc
            self._record_failure(items, module, version, exc, run_id=run_id,
                                 duration_ms=outcome.get("duration_ms"))
            raise

    def _record_failure(
        self,
        items: Sequence[Any],
        module: str,
        version: int,
        exc: BaseException,
        *,
        run_id: int | None,
        duration_ms: int | None,
    ) -> None:
        """Write the outcome of a failed entity or batch, in its own transaction.

        Interruptions (operator stop / run abort) must not poison the work: it
        stays ``pending`` so the next run picks it up again.
        """
        pending = bool(getattr(exc, "graetl_pending", False)) or isinstance(exc, KeyboardInterrupt)
        try:
            self.begin("immediate")
            for item in items:
                self._write_final(
                    item.entity_id,
                    module,
                    version,
                    status=STATUS_PENDING if pending else STATUS_FAILED,
                    run_id=run_id,
                    source_updated_at=item.source_updated_at,
                    duration_ms=duration_ms,
                    error=None if pending else f"{type(exc).__name__}: {exc}",
                    result=None,
                )
            self.commit()
        except Exception:  # pragma: no cover - could not record it
            self.rollback()

    def reset_stale_running(self, run_id: int | None = None) -> int:
        """Interrupted work (status ``running``) becomes ``pending`` again."""
        self.begin("immediate")
        if run_id is None:
            cur = self.db.execute(
                "UPDATE [[entity_module_state]] SET status = 'pending', updated_at = ? "
                "WHERE pipeline = ? AND status = 'running'",
                (now_iso(), self.pipeline),
            )
        else:
            cur = self.db.execute(
                "UPDATE [[entity_module_state]] SET status = 'pending', updated_at = ? "
                "WHERE pipeline = ? AND status = 'running' AND run_id = ?",
                (now_iso(), self.pipeline, run_id),
            )
        n = cur.rowcount or 0
        self.commit()
        return n

    def reset_module(self, module: str) -> int:
        self.begin("immediate")
        cur = self.db.execute(
            "DELETE FROM [[entity_module_state]] WHERE pipeline = ? AND module = ?",
            (self.pipeline, module),
        )
        n = cur.rowcount or 0
        self.commit()
        return n

    def rename_module(self, old: str, new: str) -> int:
        """Carry a module's entity state across a rename.

        State is keyed by module name, so renaming the file would otherwise
        orphan every row and reprocess every entity. Rows already under the new
        name win - the file being renamed onto an existing module is a conflict
        the caller should have refused, and silently merging is worse than
        keeping what is already there.
        """
        self.begin("immediate")
        self.db.execute(
            "DELETE FROM [[module_locks]] WHERE pipeline = ? AND module = ?",
            (self.pipeline, old),
        )
        # SQLite's UPDATE OR IGNORE has no portable equivalent, so the rows that
        # would collide are dropped first and the rest are moved.
        self.db.execute(
            "DELETE FROM [[entity_module_state]] WHERE pipeline = ? AND module = ? "
            "AND entity_id IN (SELECT entity_id FROM [[entity_module_state]] "
            "                  WHERE pipeline = ? AND module = ?)",
            (self.pipeline, old, self.pipeline, new),
        )
        cur = self.db.execute(
            "UPDATE [[entity_module_state]] SET module = ? WHERE pipeline = ? AND module = ?",
            (new, self.pipeline, old),
        )
        moved = cur.rowcount or 0
        self.commit()
        return moved

    def drop_module(self, module: str) -> int:
        """Forget a module entirely - used when its file is deleted."""
        self.begin("immediate")
        self.db.execute(
            "DELETE FROM [[module_locks]] WHERE pipeline = ? AND module = ?",
            (self.pipeline, module),
        )
        cur = self.db.execute(
            "DELETE FROM [[entity_module_state]] WHERE pipeline = ? AND module = ?",
            (self.pipeline, module),
        )
        n = cur.rowcount or 0
        self.commit()
        return n

    def reset_entity(self, entity_id: str, module: str | None = None) -> int:
        """Forget what a single entity has been through - the next run redoes it."""
        self.begin("immediate")
        if module:
            cur = self.db.execute(
                "DELETE FROM [[entity_module_state]] "
                "WHERE pipeline = ? AND entity_id = ? AND module = ?",
                (self.pipeline, entity_id, module),
            )
        else:
            cur = self.db.execute(
                "DELETE FROM [[entity_module_state]] WHERE pipeline = ? AND entity_id = ?",
                (self.pipeline, entity_id),
            )
        n = cur.rowcount or 0
        self.commit()
        return n

    def reset_all(self, *, drop_entities: bool = False) -> None:
        self.begin("immediate")
        self.db.execute(
            "DELETE FROM [[entity_module_state]] WHERE pipeline = ?", (self.pipeline,)
        )
        if drop_entities:
            self.db.execute("DELETE FROM [[entities]] WHERE pipeline = ?", (self.pipeline,))
        self.commit()

    # ------------------------------------------------------------- summaries

    def module_summary(self) -> list[dict[str, Any]]:
        rows = self.db.fetchall(
            "SELECT module, status, COUNT(*) AS n FROM [[entity_module_state]] "
            "WHERE pipeline = ? GROUP BY module, status",
            (self.pipeline,),
        )
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            entry = out.setdefault(row["module"], {"module": row["module"], "total": 0})
            entry[row["status"]] = int(row["n"])
            entry["total"] += int(row["n"])
        return sorted(out.values(), key=lambda e: e["module"])

    def status_counts(self) -> dict[str, int]:
        rows = self.db.fetchall(
            "SELECT status, COUNT(*) AS n FROM [[entity_module_state]] "
            "WHERE pipeline = ? GROUP BY status",
            (self.pipeline,),
        )
        return {r["status"]: int(r["n"]) for r in rows}

    # ------------------------------------------------------------------ meta

    def get_meta(self, key: str, default: Any = None) -> Any:
        row = self.db.fetchone(
            "SELECT value_json FROM [[pipeline_meta]] WHERE pipeline = ? AND key = ?",
            (self.pipeline, key),
        )
        return loads(row["value_json"], default) if row else default

    def set_meta(self, key: str, value: Any) -> None:
        in_tx = self.db.in_transaction
        if not in_tx:
            self.begin("immediate")
        self.db.execute(
            "INSERT INTO [[pipeline_meta]] (pipeline, key, value_json, updated_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(pipeline, key) DO UPDATE SET value_json = excluded.value_json, "
            "updated_at = excluded.updated_at",
            (self.pipeline, key, dumps(value), now_iso()),
        )
        if not in_tx:
            self.commit()

    # ---------------------------------------------------------- module locks

    def acquire_module_lock(
        self, module: str, *, run_id: int | None = None, owner: str | None = None
    ) -> str:
        """Claim exclusive execution of one module, across threads and processes.

        A lock whose heartbeat stopped more than ``LOCK_STALE_SECONDS`` ago
        belongs to a worker that died and is taken over. Raises
        :class:`LockConflict` when somebody else is genuinely working on it.
        """
        me = owner or self.worker
        ts = now_iso()
        self.begin("immediate")
        try:
            row = self.db.fetchone(
                "SELECT owner, heartbeat_at FROM [[module_locks]] "
                "WHERE pipeline = ? AND module = ?",
                (self.pipeline, module),
            )
            if row is not None and row["owner"] != me:
                age = _age_seconds(row["heartbeat_at"])
                if age is not None and age < LOCK_STALE_SECONDS:
                    self.rollback()
                    raise LockConflict(
                        f"module {module!r} is already being executed by {row['owner']} "
                        f"(last heartbeat {age:.0f}s ago)"
                    )
            self.db.execute(
                "INSERT INTO [[module_locks]] "
                "(pipeline, module, owner, run_id, acquired_at, heartbeat_at) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(pipeline, module) DO UPDATE SET owner = excluded.owner, "
                "run_id = excluded.run_id, acquired_at = excluded.acquired_at, "
                "heartbeat_at = excluded.heartbeat_at",
                (self.pipeline, module, me, run_id, ts, ts),
            )
            self.commit()
        except LockConflict:
            raise
        except BaseException:
            self.rollback()
            raise
        return me

    def heartbeat_module_lock(self, module: str, owner: str) -> None:
        try:
            self.begin("immediate")
            self.db.execute(
                "UPDATE [[module_locks]] SET heartbeat_at = ? "
                "WHERE pipeline = ? AND module = ? AND owner = ?",
                (now_iso(), self.pipeline, module, owner),
            )
            self.commit()
        except Exception:  # pragma: no cover - a missed beat is harmless
            self.rollback()

    def release_module_lock(self, module: str, owner: str) -> None:
        try:
            self.begin("immediate")
            self.db.execute(
                "DELETE FROM [[module_locks]] WHERE pipeline = ? AND module = ? AND owner = ?",
                (self.pipeline, module, owner),
            )
            self.commit()
        except Exception:  # pragma: no cover
            self.rollback()

    def list_module_locks(self) -> list[dict[str, Any]]:
        rows = self.db.fetchall(
            "SELECT * FROM [[module_locks]] WHERE pipeline = ?", (self.pipeline,)
        )
        return [dict(r) for r in rows]

    @contextmanager
    def module_lock(self, module: str, *, run_id: int | None = None) -> Iterator[str]:
        owner = self.acquire_module_lock(module, run_id=run_id)
        try:
            yield owner
        finally:
            self.release_module_lock(module, owner)

    # ---------------------------------------------------- transaction control

    def begin(self, mode: str = "immediate") -> None:
        self.db.begin(mode)

    def commit(self) -> None:
        self.db.commit()

    def rollback(self) -> None:
        self.db.rollback()


@dataclass(slots=True)
class _Item:
    """Adapter so a single entity can go through the same failure path as a batch."""

    entity_id: str
    source_updated_at: str | None


def _as_database(db: Database | str | Path | Target) -> Database:
    if isinstance(db, Database):
        return db
    if isinstance(db, Target):
        return connect(db)
    return connect(Target(system="sqlite", path=Path(db)))


def _age_seconds(iso: str | None) -> float | None:
    if not iso:
        return None
    from datetime import datetime, timezone

    try:
        then = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except ValueError:  # pragma: no cover
        return None
    if then.tzinfo is None:
        then = then.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - then).total_seconds()
