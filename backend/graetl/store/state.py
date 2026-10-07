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
        * the row is not ``done``/``skipped``, or
        * the module version changed, or
        * ``processed_source_updated_at < entities.source_updated_at``, or
        * a required module processed the entity after this one did
          (``processed_seq``, a commit-ordered sequence - cascade).

    Each row also records ``code_hash`` and ``run_id``: which code, at which
    version, in which run produced it.

``module_locks``
    One row per module while it is being executed, with a heartbeat. Exactly one
    worker - in this process or any other - may run a given module at a time.
    Taking a lock is one conditional upsert; every unit of work re-checks it
    inside its own transaction before writing "done" (fencing), so a worker
    whose lock was taken over can never record a result.

``module_outputs``
    Which warehouse tables each module owns rows in (``ctx.write``) or upserts
    into (``ctx.upsert``). See :mod:`graetl.store.outputs`.

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

from graetl.store.db import (
    Database,
    LockConflict,
    LockLost,
    Target,
    apply_migrations,
    connect,
)
from graetl.utils import dumps, loads, now_iso, to_iso

__all__ = [
    "EntityModuleState",
    "LockConflict",
    "LockLost",
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

#: "Leave the stored payload alone" marker for the bulk entity upsert.
_KEEP = "__graetl_keep_payload__"

#: The entity a ``scope="once"`` module's owned rows belong to.
ONCE_ENTITY = "*"


class _RetryLater(Exception):
    """Internal: the module raised RetryEntity - roll back, record pending."""

    def __init__(self, result: Any) -> None:
        super().__init__("retry later")
        self.result = result

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
    # 2: version safety and owned outputs.
    #
    # code_hash      digest of the module source that produced the row, so a
    #                code change without a version bump is detectable.
    # processed_seq  commit-ordered processing number. Cascade compares these
    #                instead of wall-clock timestamps, which can tie within a
    #                millisecond or run backwards after a clock correction.
    # module_outputs which warehouse tables each module owns rows in.
    """
    ALTER TABLE [[entity_module_state]] ADD COLUMN code_hash TEXT;
    ALTER TABLE [[entity_module_state]] ADD COLUMN processed_seq BIGINT;
    CREATE INDEX IF NOT EXISTS idx_ems_seq ON [[entity_module_state]](processed_seq);
    [[create_seq]]
    CREATE TABLE IF NOT EXISTS [[module_outputs]] (
        pipeline      TEXT NOT NULL,
        module        TEXT NOT NULL,
        table_name    TEXT NOT NULL,
        mode          TEXT NOT NULL,
        registered_at TEXT NOT NULL,
        PRIMARY KEY (pipeline, module, table_name)
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
    code_hash: str | None = None


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
        #: module -> lock owner, for every module lock this store holds. Each
        #: entity transaction re-checks it before saying "done" (fencing).
        self._fences: dict[str, str] = {}
        #: The unit of work currently open: module, entities, version, run.
        self.unit: dict[str, Any] | None = None
        from graetl.store.outputs import OutputWriter

        self.outputs = OutputWriter(self)

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
        """Bulk upsert; returns {'new': n, 'changed': n, 'seen': n}.

        One SELECT for the whole batch and two ``executemany`` calls, instead of
        three statements per entity: discovery over 100k entities is otherwise
        dominated by round trips.
        """
        ts = now_iso()
        # Last occurrence wins when a source yields the same id twice.
        batch: dict[str, tuple[str | None, str | None, dict | None]] = {}
        for entity_id, label, rev, payload in entities:
            batch[str(entity_id)] = (label, to_iso(rev), payload)
        stats = {"new": 0, "changed": 0, "seen": len(batch)}
        if not batch:
            return stats
        self.begin("immediate")
        try:
            existing: dict[str, str | None] = {}
            ids = list(batch)
            for start in range(0, len(ids), 500):
                chunk = ids[start : start + 500]
                rows = self.db.fetchall(
                    "SELECT entity_id, source_updated_at FROM [[entities]] "
                    f"WHERE pipeline = ? AND entity_id IN ({','.join('?' * len(chunk))})",
                    (self.pipeline, *chunk),
                )
                existing.update({r["entity_id"]: r["source_updated_at"] for r in rows})
            rows = []
            for entity_id, (label, rev, payload) in batch.items():
                new = entity_id not in existing
                if new:
                    stats["new"] += 1
                elif rev is not None and rev != existing[entity_id]:
                    stats["changed"] += 1
                rows.append(
                    (
                        self.pipeline,
                        entity_id,
                        label,
                        rev,
                        dumps(payload) if payload is not None else ("{}" if new else _KEEP),
                        ts,
                        ts,
                    )
                )
            # One multi-row upsert. A known entity reported without a payload
            # must keep the stored one - that is what the sentinel says.
            self.db.insert_many(
                "INSERT INTO [[entities]] (pipeline, entity_id, label, source_updated_at, "
                "payload_json, discovered_at, last_seen_at)",
                rows,
                "ON CONFLICT(pipeline, entity_id) DO UPDATE SET "
                "label = COALESCE(excluded.label, [[entities]].label), "
                "source_updated_at = COALESCE(excluded.source_updated_at, "
                "[[entities]].source_updated_at), "
                f"payload_json = CASE WHEN excluded.payload_json = '{_KEEP}' "
                "THEN [[entities]].payload_json ELSE excluded.payload_json END, "
                "last_seen_at = excluded.last_seen_at, deleted_at = NULL",
            )
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
        max_attempts: int = 0,
        split_gates: bool = False,
    ) -> tuple[str, list[Any]] | tuple[str, list[Any], str, list[Any]]:
        """The WHERE clause shared by select_work() and count_work().

        With ``split_gates`` the requirement gates come back separately, as a
        boolean expression, so one query can count "due" and "due and ready".
        """
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
                " OR s.status NOT IN ('done', 'skipped')"
                " OR s.module_version <> ?"
                " OR (e.source_updated_at IS NOT NULL"
                "     AND (s.processed_source_updated_at IS NULL"
                "          OR s.processed_source_updated_at < e.source_updated_at))"
            )
            args.append(version)
            upstream = [name for name, _ in requires] + list(depends_on)
            if cascade and upstream:
                placeholders = ",".join("?" * len(upstream))
                # An upstream row processed after this one means this module
                # consumed an older result. The commit-ordered sequence decides;
                # rows written before it existed fall back to timestamps.
                clause += (
                    " OR EXISTS (SELECT 1 FROM [[entity_module_state]] u"
                    "            WHERE u.pipeline = e.pipeline AND u.entity_id = e.entity_id"
                    f"              AND u.module IN ({placeholders})"
                    "              AND u.status IN ('done', 'skipped')"
                    "              AND (s.processed_at IS NULL"
                    "                   OR (u.processed_seq IS NOT NULL AND s.processed_seq IS NOT NULL"
                    "                       AND u.processed_seq > s.processed_seq)"
                    "                   OR ((u.processed_seq IS NULL OR s.processed_seq IS NULL)"
                    "                       AND u.processed_at > s.processed_at)))"
                )
                args.extend(upstream)
            where.append(clause + ")")
            if max_attempts > 0:
                # Failed too often at this version and this source revision:
                # blocked until the code or the entity changes (or retry-failed).
                where.append(
                    "NOT (s.entity_id IS NOT NULL AND s.status = 'failed'"
                    " AND s.attempts >= ? AND s.module_version = ?"
                    " AND COALESCE(s.source_updated_at, '') = COALESCE(e.source_updated_at, ''))"
                )
                args.extend([max_attempts, version])

        gate_parts: list[str] = []
        gate_args: list[Any] = []
        gates = [*requires, *((name, None) for name in depends_on)] if apply_requirements else []
        for dep_name, dep_version in gates:
            clause = (
                "EXISTS (SELECT 1 FROM [[entity_module_state]] d "
                "WHERE d.pipeline = e.pipeline AND d.entity_id = e.entity_id "
                "AND d.module = ? AND d.status IN ('done', 'skipped')"
            )
            gate_args.append(dep_name)
            if dep_version is not None:
                clause += (
                    " AND d.module_version = ?"
                    " AND (e.source_updated_at IS NULL"
                    "      OR (d.processed_source_updated_at IS NOT NULL"
                    "          AND d.processed_source_updated_at >= e.source_updated_at))"
                )
                gate_args.append(dep_version)
            gate_parts.append(clause + ")")

        if split_gates:
            gate_sql = " AND ".join(gate_parts) if gate_parts else "1 = 1"
            return " AND ".join(where), args, gate_sql, gate_args
        return " AND ".join(where + gate_parts), args + gate_args

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
        max_attempts: int = 0,
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

        ``max_attempts`` (incremental mode) leaves out entities that already
        failed that many times at this version and source revision.

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
            max_attempts=max_attempts,
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
        max_attempts: int = 0,
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
            max_attempts=max_attempts,
        )
        sql = "SELECT COUNT(*) AS n " + self._JOIN + " WHERE " + where
        return int(self.db.fetchone(sql, [module, *where_args])["n"])

    def work_breakdown(
        self,
        module: str,
        version: int,
        *,
        mode: str = "incremental",
        requires: Sequence[tuple[str, int | None]] = (),
        cascade: bool = True,
        entity_ids: Sequence[str] | None = None,
        max_attempts: int = 0,
    ) -> dict[str, int]:
        """``{"due": n, "ready": n, "waiting": n}`` in ONE scan.

        ``due`` is everything this module still has to do, ``ready`` the part
        whose upstream is up to date (what a run will process now), and
        ``waiting`` the rest. Counting both separately costs two full scans.
        """
        where, where_args, gate_sql, gate_args = self._work_where(  # type: ignore[misc]
            version,
            mode=mode,
            requires=requires,
            depends_on=(),
            cascade=cascade,
            apply_requirements=True,
            after=None,
            entity_ids=entity_ids,
            max_attempts=max_attempts,
            split_gates=True,
        )
        sql = (
            "SELECT COUNT(*) AS due, "
            f"COALESCE(SUM(CASE WHEN {gate_sql} THEN 1 ELSE 0 END), 0) AS ready "
            + self._JOIN
            + " WHERE "
            + where
        )
        row = self.db.fetchone(sql, [*gate_args, module, *where_args])
        due, ready = int(row["due"]), int(row["ready"])
        return {"due": due, "ready": ready, "waiting": due - ready}

    # --------------------------------------------------------- state changes

    def mark_running(self, entity_id: str, module: str, version: int, run_id: int | None) -> None:
        """Flag an entity as being worked on, in a commit of its own.

        The runner no longer calls this per entity - it doubled the number of
        commits for no safety gain, since the work itself commits atomically
        with its state row. Kept for tools and tests that want the flag.
        """
        ts = now_iso()
        self.begin("immediate")
        self.db.execute(
            """
            INSERT INTO [[entity_module_state]]
                (pipeline, entity_id, module, module_version, status, attempts, run_id, updated_at)
            VALUES (?, ?, ?, ?, 'running', 0, ?, ?)
            ON CONFLICT(pipeline, entity_id, module) DO UPDATE SET
                status = 'running',
                run_id = excluded.run_id,
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
        code_hash: str | None = None,
        count_attempt: bool = True,
    ) -> None:
        """Write one entity's outcome. Called inside the unit's transaction.

        ``attempts`` counts tries at *this* version and source revision: a new
        version or a changed entity starts again from one, so ``max_attempts``
        never blocks an entity the code or the source has moved on from.
        """
        ts = now_iso()
        processed = status in (STATUS_DONE, STATUS_SKIPPED)
        seq = "[[seq_next]]" if processed else "NULL"
        bump = 1 if count_attempt else 0
        self.db.execute(
            f"""
            INSERT INTO [[entity_module_state]]
                (pipeline, entity_id, module, module_version, status, source_updated_at,
                 processed_source_updated_at, processed_at, attempts, run_id,
                 duration_ms, error, result_json, updated_at, code_hash, processed_seq)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {seq})
            ON CONFLICT(pipeline, entity_id, module) DO UPDATE SET
                attempts = CASE
                    WHEN [[entity_module_state]].module_version = excluded.module_version
                     AND COALESCE([[entity_module_state]].source_updated_at, '')
                         = COALESCE(excluded.source_updated_at, '')
                    THEN [[entity_module_state]].attempts + {bump}
                    ELSE {bump} END,
                module_version = excluded.module_version,
                status = excluded.status,
                source_updated_at = excluded.source_updated_at,
                processed_source_updated_at = COALESCE(
                    excluded.processed_source_updated_at,
                    [[entity_module_state]].processed_source_updated_at),
                processed_at = COALESCE(
                    excluded.processed_at, [[entity_module_state]].processed_at),
                processed_seq = COALESCE(
                    excluded.processed_seq, [[entity_module_state]].processed_seq),
                code_hash = COALESCE(excluded.code_hash, [[entity_module_state]].code_hash),
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
                source_updated_at if processed else None,
                ts if processed else None,
                bump,
                run_id,
                duration_ms,
                error,
                dumps(result) if result is not None else None,
                ts,
                code_hash if processed else None,
            ),
        )

    # ------------------------------------------------------------ fencing

    def check_fence(self, module: str) -> None:
        """Make sure this store still holds ``module``'s lock - inside the transaction.

        Called right before a unit's state rows are written. On PostgreSQL the
        lock row is read ``FOR SHARE``, so a concurrent takeover (an UPDATE of
        that row) waits until this transaction has committed; on SQLite the
        writer lock already serialises the two. Either way a worker whose lock
        was taken over can never record "done".

        A store that never took the lock (tools, tests, the server) is not
        fenced.
        """
        owner = self._fences.get(module)
        if owner is None:
            return
        row = self.db.fetchone(
            "SELECT owner FROM [[module_locks]] WHERE pipeline = ? AND module = ?[[for_share]]",
            (self.pipeline, module),
        )
        if row is None or row["owner"] != owner:
            raise LockLost(
                f"module {module!r}: lock lost to {row['owner'] if row else 'nobody'} "
                "- another worker took it over; this worker's results were rolled back"
            )

    # ------------------------------------------------------------ units

    @contextmanager
    def _unit(
        self,
        items: Sequence[Any],
        module: str,
        version: int,
        *,
        run_id: int | None,
        mode: str,
        scope: str,
        code_hash: str | None,
    ) -> Iterator[dict[str, Any]]:
        """One transaction around a module's work for ``items`` and their state rows.

        Inside it, before the module body runs, every row the module owns for
        these entities is deleted (see :mod:`graetl.store.outputs`), so what is
        left at commit is exactly what this execution wrote - the same module,
        version and entity always produce the same rows, however often it runs
        and whatever an older version left behind.
        """
        outcome: dict[str, Any] = {"status": STATUS_DONE, "result": None, "duration_ms": None}
        self.begin(mode)
        self.unit = {
            "module": module,
            "version": version,
            "run_id": run_id,
            "scope": scope,
            "entity_ids": [i.entity_id for i in items],
        }
        try:
            self.outputs.clear_owned(module, self.unit["entity_ids"])
            yield outcome
            status = outcome.get("status", STATUS_DONE)
            if status == STATUS_PENDING:
                # RetryEntity: "not now". Nothing this attempt wrote is kept.
                raise _RetryLater(outcome.get("result"))
            self.check_fence(module)
            for item in items:
                self._write_final(
                    item.entity_id,
                    module,
                    version,
                    status=status,
                    run_id=run_id,
                    source_updated_at=item.source_updated_at,
                    duration_ms=outcome.get("duration_ms"),
                    error=None,
                    result=outcome.get("result"),
                    code_hash=code_hash,
                )
            self.commit()
            self.outputs.committed()
        except _RetryLater as later:
            self.rollback()
            self.outputs.rolled_back()
            self._record_outcome(
                items, module, version, status=STATUS_PENDING, error=None,
                result=later.result, run_id=run_id, duration_ms=outcome.get("duration_ms"),
            )
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            self.rollback()
            self.outputs.rolled_back()
            if isinstance(exc, LockLost):
                raise  # not ours to record any more
            if self._is_lock_error(exc):
                # Pure contention: nothing happened, nothing is recorded, retry.
                raise LockConflict(str(exc)) from exc
            if isinstance(exc, LockConflict):
                raise
            self._record_failure(items, module, version, exc,
                                 run_id=run_id, duration_ms=outcome.get("duration_ms"))
            raise
        finally:
            self.unit = None

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
        code_hash: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Run a module for one entity inside a single transaction.

        Module data writes made through ``ctx.db`` / ``ctx.write`` and the final
        state row update commit together, or not at all.

        ``mode="deferred"`` (used when modules run in parallel) takes the write
        lock only at the first write. If another worker wins the race, the whole
        entity is rolled back and :class:`LockConflict` is raised **without**
        writing a state row, so the caller can simply try the entity again.
        """
        with self._unit(
            [_Item(entity_id, source_updated_at)], module, version,
            run_id=run_id, mode=mode, scope="entity", code_hash=code_hash,
        ) as outcome:
            yield outcome

    @contextmanager
    def batch_transaction(
        self,
        items: Sequence[Any],
        module: str,
        version: int,
        *,
        run_id: int | None,
        mode: str = "immediate",
        code_hash: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Run a module for a whole batch of entities inside ONE transaction.

        The batch is the unit: every entity in it is marked done together, or
        none is and the whole batch is retried on the next run. That is the same
        promise :meth:`entity_transaction` makes for one entity - "done but not
        written" stays impossible - and it is why a batch module cannot report
        a per-entity outcome.
        """
        with self._unit(
            items, module, version,
            run_id=run_id, mode=mode, scope="batch", code_hash=code_hash,
        ) as outcome:
            yield outcome

    @contextmanager
    def once_transaction(
        self, module: str, version: int, *, run_id: int | None, mode: str = "immediate"
    ) -> Iterator[None]:
        """The transaction around a ``scope="once"`` module.

        No entity state is written, but the module's owned rows (entity ``*``)
        are replaced as a whole and the lock is re-checked before commit.
        """
        self.begin(mode)
        self.unit = {
            "module": module, "version": version, "run_id": run_id,
            "scope": "once", "entity_ids": [ONCE_ENTITY],
        }
        try:
            self.outputs.clear_owned(module, [ONCE_ENTITY])
            yield
            self.check_fence(module)
            self.commit()
            self.outputs.committed()
        except BaseException as exc:
            self.rollback()
            self.outputs.rolled_back()
            if self._is_lock_error(exc) and not isinstance(exc, LockConflict):
                raise LockConflict(str(exc)) from exc
            raise
        finally:
            self.unit = None

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
        stays ``pending`` so the next run picks it up again - and an
        interruption is not an attempt, so it never counts toward
        ``max_attempts``.
        """
        pending = bool(getattr(exc, "graetl_pending", False)) or isinstance(exc, KeyboardInterrupt)
        self._record_outcome(
            items, module, version,
            status=STATUS_PENDING if pending else STATUS_FAILED,
            error=None if pending else f"{type(exc).__name__}: {exc}",
            result=None, run_id=run_id, duration_ms=duration_ms,
            count_attempt=not pending,
        )

    def _record_outcome(
        self,
        items: Sequence[Any],
        module: str,
        version: int,
        *,
        status: str,
        error: str | None,
        result: Any,
        run_id: int | None,
        duration_ms: int | None,
        count_attempt: bool = True,
    ) -> None:
        try:
            self.begin("immediate")
            # A worker that lost its lock must not overwrite the new owner's rows.
            self.check_fence(module)
            for item in items:
                self._write_final(
                    item.entity_id,
                    module,
                    version,
                    status=status,
                    run_id=run_id,
                    source_updated_at=item.source_updated_at,
                    duration_ms=duration_ms,
                    error=error,
                    result=result,
                    count_attempt=count_attempt,
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
        try:
            self.outputs.rename_module(old, new)
        except BaseException:
            self.rollback()
            self.outputs.rolled_back()
            raise
        self.commit()
        return moved

    def drop_module(self, module: str, *, purge_outputs: bool = True) -> int:
        """Forget a module entirely - used when its file is deleted.

        Its owned rows go too (``purge_outputs``): a warehouse that keeps the
        output of a module that no longer exists is not reproducible from its
        pipeline definition. Upserted rows in shared tables are left alone.
        """
        self.begin("immediate")
        if purge_outputs:
            try:
                self.outputs.purge_module(module)
            except BaseException:
                self.rollback()
                self.outputs.rolled_back()
                raise
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

    def module_health(
        self, modules: Sequence[tuple[str, int, str | None]]
    ) -> dict[str, dict[str, Any]]:
        """Where every module stands against its *current* definition, in one scan.

        ``modules`` is ``(name, version, code_hash)`` as loaded now. Per module:

        ``current``          done/skipped at this version and the entity's revision
        ``drift``            ...of those, produced by different code at the same
                             version - the version should have been bumped
        ``outdated_version`` done by another version: will be reprocessed
        ``outdated_source``  the entity changed in the source since
        ``failed`` / ``pending``
        ``never``            entities this module has not touched yet
        ``versions``         rows per recorded version, for the history view
        """
        total = self.count_entities()
        rows = self.db.fetchall(
            "SELECT s.module AS module, s.module_version AS module_version, s.status AS status, "
            "s.code_hash AS code_hash, "
            "CASE WHEN e.source_updated_at IS NOT NULL AND (s.processed_source_updated_at IS NULL "
            "  OR s.processed_source_updated_at < e.source_updated_at) THEN 1 ELSE 0 END AS rev_stale, "
            "COUNT(*) AS n "
            "FROM [[entity_module_state]] s JOIN [[entities]] e "
            "  ON e.pipeline = s.pipeline AND e.entity_id = s.entity_id "
            "WHERE s.pipeline = ? AND e.deleted_at IS NULL "
            "GROUP BY s.module, s.module_version, s.status, s.code_hash, "
            "CASE WHEN e.source_updated_at IS NOT NULL AND (s.processed_source_updated_at IS NULL "
            "  OR s.processed_source_updated_at < e.source_updated_at) THEN 1 ELSE 0 END",
            (self.pipeline,),
        )
        current = {name: (int(version), code) for name, version, code in modules}
        out: dict[str, dict[str, Any]] = {}
        for name in current:
            out[name] = {
                "entities": total, "current": 0, "drift": 0, "outdated_version": 0,
                "outdated_source": 0, "failed": 0, "pending": 0, "never": total,
                "versions": {},
            }
        for r in rows:
            name = r["module"]
            if name not in out:
                continue  # state of a module that no longer exists
            entry = out[name]
            n = int(r["n"])
            version = int(r["module_version"])
            cur_version, cur_hash = current[name]
            entry["never"] -= n
            entry["versions"][str(version)] = entry["versions"].get(str(version), 0) + n
            status = r["status"]
            if status in (STATUS_DONE, STATUS_SKIPPED):
                if version != cur_version:
                    entry["outdated_version"] += n
                elif int(r["rev_stale"]):
                    entry["outdated_source"] += n
                else:
                    entry["current"] += n
                    if cur_hash and r["code_hash"] and r["code_hash"] != cur_hash:
                        entry["drift"] += n
            elif status == STATUS_FAILED:
                entry["failed"] += n
            else:
                entry["pending"] += n
        return out

    def drift_count(self, module: str, version: int, code_hash: str | None) -> int:
        """Entities processed at ``version`` by code other than ``code_hash``."""
        if not code_hash:
            return 0
        row = self.db.fetchone(
            "SELECT COUNT(*) AS n FROM [[entity_module_state]] WHERE pipeline = ? AND module = ? "
            "AND module_version = ? AND status IN ('done', 'skipped') "
            "AND code_hash IS NOT NULL AND code_hash <> ?",
            (self.pipeline, module, version, code_hash),
        )
        return int(row["n"])

    def blocked_count(self, module: str, version: int, max_attempts: int) -> int:
        if max_attempts <= 0:
            return 0
        row = self.db.fetchone(
            "SELECT COUNT(*) AS n FROM [[entity_module_state]] s JOIN [[entities]] e "
            "ON e.pipeline = s.pipeline AND e.entity_id = s.entity_id "
            "WHERE s.pipeline = ? AND s.module = ? AND s.status = 'failed' AND s.attempts >= ? "
            "AND s.module_version = ? AND e.deleted_at IS NULL "
            "AND COALESCE(s.source_updated_at, '') = COALESCE(e.source_updated_at, '')",
            (self.pipeline, module, max_attempts, version),
        )
        return int(row["n"])

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

    # ------------------------------------------------------------ planner

    def refresh_statistics(self) -> None:
        """Let the query planner see how big the state tables have become.

        After a bulk load PostgreSQL still believes the tables are empty until
        autovacuum gets round to them, and plans the work query as a nested
        loop: quadratic, seconds instead of milliseconds at a few thousand
        entities. The runner calls this after discovery and after a module that
        wrote many rows. SQLite gets ``PRAGMA optimize``, which only analyses
        what needs it.
        """
        try:
            if self.db.dialect.name == "postgres":
                self.db.execute("ANALYZE [[entities]]")
                self.db.execute("ANALYZE [[entity_module_state]]")
            else:
                self.db.execute("PRAGMA optimize")
        except Exception:  # pragma: no cover - statistics are an optimisation
            pass

    # ---------------------------------------------------------- module locks

    def acquire_module_lock(
        self, module: str, *, run_id: int | None = None, owner: str | None = None
    ) -> str:
        """Claim exclusive execution of one module, across threads and processes.

        A lock whose heartbeat stopped more than ``LOCK_STALE_SECONDS`` ago
        belongs to a worker that died and is taken over. Raises
        :class:`LockConflict` when somebody else is genuinely working on it.

        The claim is a single conditional upsert, so two workers racing for a
        stale lock cannot both win - on PostgreSQL as on SQLite. Every unit of
        work then re-checks ownership before it commits (:meth:`check_fence`).
        """
        from datetime import datetime, timedelta, timezone

        me = owner or self.worker
        ts = now_iso()
        cutoff = (
            (datetime.now(timezone.utc) - timedelta(seconds=LOCK_STALE_SECONDS))
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        self.begin("immediate")
        try:
            self.db.execute(
                "INSERT INTO [[module_locks]] "
                "(pipeline, module, owner, run_id, acquired_at, heartbeat_at) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(pipeline, module) DO UPDATE SET owner = excluded.owner, "
                "run_id = excluded.run_id, acquired_at = excluded.acquired_at, "
                "heartbeat_at = excluded.heartbeat_at "
                "WHERE [[module_locks]].owner = excluded.owner "
                "   OR [[module_locks]].heartbeat_at < ?",
                (self.pipeline, module, me, run_id, ts, ts, cutoff),
            )
            row = self.db.fetchone(
                "SELECT owner, heartbeat_at FROM [[module_locks]] "
                "WHERE pipeline = ? AND module = ?",
                (self.pipeline, module),
            )
            if row is None or row["owner"] != me:
                self.rollback()
                age = _age_seconds(row["heartbeat_at"]) if row else None
                raise LockConflict(
                    f"module {module!r} is already being executed by "
                    f"{row['owner'] if row else '?'}"
                    + (f" (last heartbeat {age:.0f}s ago)" if age is not None else "")
                )
            self.commit()
        except LockConflict:
            raise
        except BaseException:
            self.rollback()
            raise
        self._fences[module] = me
        return me

    def heartbeat_module_lock(self, module: str, owner: str) -> bool:
        """Refresh a lock. False when it is no longer ours (or the beat failed)."""
        try:
            self.begin("immediate")
            cur = self.db.execute(
                "UPDATE [[module_locks]] SET heartbeat_at = ? "
                "WHERE pipeline = ? AND module = ? AND owner = ?",
                (now_iso(), self.pipeline, module, owner),
            )
            ok = (cur.rowcount or 0) > 0
            self.commit()
            return ok
        except Exception:  # pragma: no cover - a missed beat is harmless
            self.rollback()
            return False

    def release_module_lock(self, module: str, owner: str) -> None:
        self._fences.pop(module, None)
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
