"""Per-pipeline entity state (``pipelines/<id>/state.db``).

This is the database that makes a pipeline *resumable*. Two tables:

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
Module code writes its data through the very same SQLite connection
(``ctx.db``). ``entity_transaction()`` opens one transaction that covers *both*
the module's data writes and the state row update, so a crash can never leave
"state says done, data was never written". Writes to systems outside this
connection are at-least-once: the state row is only flipped to ``done`` after
the module returns, so an interrupted entity is retried on resume.

Concurrency
-----------
Each module worker owns its own ``StateStore`` (its own SQLite connection) and
the database runs in WAL mode. Two transaction modes:

``immediate``
    Takes the write lock up front. Used when modules run one at a time - no
    contention, no retries, the simplest possible behaviour.
``deferred``
    Takes the write lock at the first write, so parallel modules only serialise
    around their writes rather than around their whole body. A write conflict
    raises :class:`LockConflict` *before* any state row is written, and the
    caller retries the entity. Nothing is ever recorded as failed because of
    contention.
"""

from __future__ import annotations

import os
import random
import socket
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from graetl.store.sqlite import apply_migrations, connect
from graetl.utils import dumps, loads, now_iso, to_iso

class LockConflict(RuntimeError):
    """Another writer held the database; the caller should retry the entity."""


def is_lock_error(exc: BaseException) -> bool:
    """True for SQLite's "someone else is writing" errors, which are retryable."""
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    text = str(exc).lower()
    return "locked" in text or "busy" in text


def worker_id() -> str:
    """Identifies one worker uniquely across threads, processes and machines."""
    return (
        f"{socket.gethostname()}:{os.getpid()}:{threading.get_ident()}:{uuid.uuid4().hex[:8]}"
    )


STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"

MIGRATIONS: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS entities (
        entity_id          TEXT PRIMARY KEY,
        label              TEXT,
        source_updated_at  TEXT,
        payload_json       TEXT NOT NULL DEFAULT '{}',
        discovered_at      TEXT NOT NULL,
        last_seen_at       TEXT NOT NULL,
        deleted_at         TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_entities_seen ON entities(last_seen_at);

    CREATE TABLE IF NOT EXISTS entity_module_state (
        entity_id                    TEXT NOT NULL,
        module                       TEXT NOT NULL,
        module_version               INTEGER NOT NULL DEFAULT 1,
        status                       TEXT NOT NULL DEFAULT 'pending',
        source_updated_at            TEXT,
        processed_source_updated_at  TEXT,
        processed_at                 TEXT,
        attempts                     INTEGER NOT NULL DEFAULT 0,
        run_id                       INTEGER,
        duration_ms                  INTEGER,
        error                        TEXT,
        result_json                  TEXT,
        updated_at                   TEXT NOT NULL,
        PRIMARY KEY (entity_id, module)
    );
    CREATE INDEX IF NOT EXISTS idx_ems_module_status ON entity_module_state(module, status);
    CREATE INDEX IF NOT EXISTS idx_ems_run ON entity_module_state(run_id);

    CREATE TABLE IF NOT EXISTS pipeline_meta (
        key        TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """,
    # 2 - indexes that matter once an entity table gets big
    """
    CREATE INDEX IF NOT EXISTS idx_entities_revision ON entities(source_updated_at);
    CREATE INDEX IF NOT EXISTS idx_entities_discovered ON entities(discovered_at);
    CREATE INDEX IF NOT EXISTS idx_ems_entity ON entity_module_state(entity_id);
    """,
    # 3 - one worker per module, across threads and processes
    """
    CREATE TABLE IF NOT EXISTS module_locks (
        module       TEXT PRIMARY KEY,
        owner        TEXT NOT NULL,
        run_id       INTEGER,
        acquired_at  TEXT NOT NULL,
        heartbeat_at TEXT NOT NULL
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

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        #: Identity of this connection, used for module locks.
        self.worker = worker_id()
        self.conn: sqlite3.Connection = connect(self.path)
        self.conn.isolation_level = None  # explicit transaction control
        apply_migrations(self.conn, MIGRATIONS)

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # pragma: no cover
            pass

    def __enter__(self) -> StateStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

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
        cur = self.conn.execute(
            "SELECT source_updated_at FROM entities WHERE entity_id = ?", (entity_id,)
        ).fetchone()
        if cur is None:
            self.conn.execute(
                "INSERT INTO entities (entity_id, label, source_updated_at, payload_json, "
                "discovered_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?)",
                (entity_id, label, rev, dumps(payload or {}), ts, ts),
            )
            return True
        changed = rev is not None and rev != cur["source_updated_at"]
        self.conn.execute(
            "UPDATE entities SET label = COALESCE(?, label), "
            "source_updated_at = COALESCE(?, source_updated_at), "
            "payload_json = COALESCE(?, payload_json), last_seen_at = ?, deleted_at = NULL "
            "WHERE entity_id = ?",
            (label, rev, dumps(payload) if payload is not None else None, ts, entity_id),
        )
        return changed

    def upsert_entities(self, entities: Iterable[tuple[str, str | None, Any, dict | None]]) -> dict:
        """Bulk upsert; returns {'new': n, 'changed': n, 'seen': n}."""
        stats = {"new": 0, "changed": 0, "seen": 0}
        self.begin("immediate")
        try:
            for entity_id, label, rev, payload in entities:
                existed = self.conn.execute(
                    "SELECT 1 FROM entities WHERE entity_id = ?", (entity_id,)
                ).fetchone()
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
        cur = self.conn.execute(
            "UPDATE entities SET deleted_at = ? WHERE deleted_at IS NULL AND last_seen_at < ?",
            (now_iso(), cutoff_iso),
        )
        return cur.rowcount or 0

    def count_entities(self, include_deleted: bool = False) -> int:
        sql = "SELECT COUNT(*) AS n FROM entities"
        if not include_deleted:
            sql += " WHERE deleted_at IS NULL"
        return int(self.conn.execute(sql).fetchone()["n"])

    @staticmethod
    def _entity_filter(
        search: str | None, status: str | None, module: str | None
    ) -> tuple[list[str], list[Any]]:
        where = ["e.deleted_at IS NULL"]
        args: list[Any] = []
        if search:
            where.append("(e.entity_id LIKE ? OR IFNULL(e.label,'') LIKE ?)")
            args.extend([f"%{search}%", f"%{search}%"])
        if status or module:
            sub = "SELECT 1 FROM entity_module_state s WHERE s.entity_id = e.entity_id"
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
        sql = "SELECT COUNT(*) AS n FROM entities e WHERE " + " AND ".join(where)
        return int(self.conn.execute(sql, args).fetchone()["n"])

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
            "SELECT e.* FROM entities e WHERE "
            + " AND ".join(where)
            + f" ORDER BY {order_sql} LIMIT ? OFFSET ?"
        )
        args = [*args, limit, offset]
        rows = self.conn.execute(sql, args).fetchall()
        out: list[dict[str, Any]] = []
        ids = [r["entity_id"] for r in rows]
        states = self.states_for(ids)
        for row in rows:
            d = dict(row)
            d["payload"] = loads(d.pop("payload_json", None), {})
            d["modules"] = states.get(d["entity_id"], [])
            out.append(d)
        return out

    def states_for(self, entity_ids: Sequence[str]) -> dict[str, list[dict[str, Any]]]:
        if not entity_ids:
            return {}
        placeholders = ",".join("?" * len(entity_ids))
        rows = self.conn.execute(
            f"SELECT * FROM entity_module_state WHERE entity_id IN ({placeholders}) "
            "ORDER BY module",
            tuple(entity_ids),
        ).fetchall()
        out: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            d = dict(row)
            d["result"] = loads(d.pop("result_json", None))
            out.setdefault(d["entity_id"], []).append(d)
        return out

    def get_entity(self, entity_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM entities WHERE entity_id = ?", (entity_id,)
        ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["payload"] = loads(d.pop("payload_json", None), {})
        d["modules"] = self.states_for([entity_id]).get(entity_id, [])
        return d

    # -------------------------------------------------------------- work queue

    def _work_where(
        self,
        module: str,
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
        args: list[Any] = [module]
        where = ["e.deleted_at IS NULL"]

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
                    " OR EXISTS (SELECT 1 FROM entity_module_state u"
                    f"            WHERE u.entity_id = e.entity_id AND u.module IN ({placeholders})"
                    "              AND u.processed_at IS NOT NULL"
                    "              AND (s.processed_at IS NULL OR u.processed_at > s.processed_at))"
                )
                args.extend(upstream)
            where.append(clause + ")")

        gates = (
            [*requires, *((name, None) for name in depends_on)] if apply_requirements else []
        )
        for dep_name, dep_version in gates:
            clause = (
                "EXISTS (SELECT 1 FROM entity_module_state d WHERE d.entity_id = e.entity_id "
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
        where, args = self._work_where(
            module,
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
            "IFNULL(s.attempts, 0) AS attempts, s.status AS previous_status "
            "FROM entities e LEFT JOIN entity_module_state s "
            "ON s.entity_id = e.entity_id AND s.module = ? "
            "WHERE " + where
            + (" ORDER BY RANDOM()" if order == "random" else " ORDER BY e.entity_id")
        )
        if limit:
            sql += " LIMIT ?"
            args.append(limit)
        rows = self.conn.execute(sql, args).fetchall()
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
        where, args = self._work_where(
            module,
            version,
            mode=mode,
            requires=requires,
            depends_on=depends_on,
            cascade=cascade,
            apply_requirements=apply_requirements,
            after=None,
            entity_ids=entity_ids,
        )
        sql = (
            "SELECT COUNT(*) AS n FROM entities e LEFT JOIN entity_module_state s "
            "ON s.entity_id = e.entity_id AND s.module = ? WHERE " + where
        )
        return int(self.conn.execute(sql, args).fetchone()["n"])

    # ----------------------------------------------------------- state changes

    def mark_running(self, entity_id: str, module: str, version: int, run_id: int | None) -> None:
        ts = now_iso()
        self.begin("immediate")
        self.conn.execute(
            """
            INSERT INTO entity_module_state
                (entity_id, module, module_version, status, attempts, run_id, updated_at)
            VALUES (?, ?, ?, 'running', 1, ?, ?)
            ON CONFLICT(entity_id, module) DO UPDATE SET
                status = 'running',
                module_version = excluded.module_version,
                attempts = entity_module_state.attempts + 1,
                run_id = excluded.run_id,
                error = NULL,
                updated_at = excluded.updated_at
            """,
            (entity_id, module, version, run_id, ts),
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
        self.conn.execute(
            """
            INSERT INTO entity_module_state
                (entity_id, module, module_version, status, source_updated_at,
                 processed_source_updated_at, processed_at, attempts, run_id,
                 duration_ms, error, result_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
            ON CONFLICT(entity_id, module) DO UPDATE SET
                module_version = excluded.module_version,
                status = excluded.status,
                source_updated_at = excluded.source_updated_at,
                processed_source_updated_at = COALESCE(
                    excluded.processed_source_updated_at,
                    entity_module_state.processed_source_updated_at),
                processed_at = COALESCE(excluded.processed_at, entity_module_state.processed_at),
                run_id = excluded.run_id,
                duration_ms = excluded.duration_ms,
                error = excluded.error,
                result_json = excluded.result_json,
                updated_at = excluded.updated_at
            """,
            (
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

        Module data writes made through ``self.conn`` and the final state row
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
            if is_lock_error(exc):
                # Pure contention: nothing happened, nothing is recorded, retry.
                raise LockConflict(str(exc)) from exc
            # Interruptions (operator stop / run abort) must not poison the entity:
            # it stays pending so the next run picks it up again.
            pending = bool(getattr(exc, "graetl_pending", False)) or isinstance(
                exc, KeyboardInterrupt
            )
            try:
                self.begin("immediate")
                self._write_final(
                    entity_id,
                    module,
                    version,
                    status=STATUS_PENDING if pending else STATUS_FAILED,
                    run_id=run_id,
                    source_updated_at=source_updated_at,
                    duration_ms=outcome.get("duration_ms"),
                    error=None if pending else f"{type(exc).__name__}: {exc}",
                    result=None,
                )
                self.commit()
            except sqlite3.OperationalError:  # pragma: no cover - could not record it
                self.rollback()
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
            if is_lock_error(exc):
                raise LockConflict(str(exc)) from exc
            pending = bool(getattr(exc, "graetl_pending", False)) or isinstance(
                exc, KeyboardInterrupt
            )
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
                        duration_ms=outcome.get("duration_ms"),
                        error=None if pending else f"{type(exc).__name__}: {exc}",
                        result=None,
                    )
                self.commit()
            except sqlite3.OperationalError:  # pragma: no cover - could not record it
                self.rollback()
            raise

    def reset_stale_running(self, run_id: int | None = None) -> int:
        """Interrupted work (status ``running``) becomes ``pending`` again."""
        self.begin("immediate")
        if run_id is None:
            cur = self.conn.execute(
                "UPDATE entity_module_state SET status = 'pending', updated_at = ? "
                "WHERE status = 'running'",
                (now_iso(),),
            )
        else:
            cur = self.conn.execute(
                "UPDATE entity_module_state SET status = 'pending', updated_at = ? "
                "WHERE status = 'running' AND run_id = ?",
                (now_iso(), run_id),
            )
        n = cur.rowcount or 0
        self.commit()
        return n

    def reset_module(self, module: str) -> int:
        self.begin("immediate")
        cur = self.conn.execute("DELETE FROM entity_module_state WHERE module = ?", (module,))
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
        self.conn.execute("DELETE FROM module_locks WHERE module = ?", (old,))
        cur = self.conn.execute(
            "UPDATE OR IGNORE entity_module_state SET module = ? WHERE module = ?",
            (new, old),
        )
        moved = cur.rowcount or 0
        self.conn.execute("DELETE FROM entity_module_state WHERE module = ?", (old,))
        self.commit()
        return moved

    def drop_module(self, module: str) -> int:
        """Forget a module entirely - used when its file is deleted."""
        self.begin("immediate")
        self.conn.execute("DELETE FROM module_locks WHERE module = ?", (module,))
        cur = self.conn.execute("DELETE FROM entity_module_state WHERE module = ?", (module,))
        n = cur.rowcount or 0
        self.commit()
        return n

    def reset_entity(self, entity_id: str, module: str | None = None) -> int:
        """Forget what a single entity has been through - the next run redoes it."""
        self.begin("immediate")
        if module:
            cur = self.conn.execute(
                "DELETE FROM entity_module_state WHERE entity_id = ? AND module = ?",
                (entity_id, module),
            )
        else:
            cur = self.conn.execute(
                "DELETE FROM entity_module_state WHERE entity_id = ?", (entity_id,)
            )
        n = cur.rowcount or 0
        self.commit()
        return n

    def reset_all(self, *, drop_entities: bool = False) -> None:
        self.begin("immediate")
        self.conn.execute("DELETE FROM entity_module_state")
        if drop_entities:
            self.conn.execute("DELETE FROM entities")
        self.commit()

    # -------------------------------------------------------------- summaries

    def module_summary(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT module, status, COUNT(*) AS n FROM entity_module_state GROUP BY module, status"
        ).fetchall()
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            entry = out.setdefault(row["module"], {"module": row["module"], "total": 0})
            entry[row["status"]] = int(row["n"])
            entry["total"] += int(row["n"])
        return sorted(out.values(), key=lambda e: e["module"])

    def status_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) AS n FROM entity_module_state GROUP BY status"
        ).fetchall()
        return {r["status"]: int(r["n"]) for r in rows}

    # ------------------------------------------------------------------- meta

    def get_meta(self, key: str, default: Any = None) -> Any:
        row = self.conn.execute(
            "SELECT value_json FROM pipeline_meta WHERE key = ?", (key,)
        ).fetchone()
        return loads(row["value_json"], default) if row else default

    def set_meta(self, key: str, value: Any) -> None:
        in_tx = self.conn.in_transaction
        if not in_tx:
            self.begin("immediate")
        self.conn.execute(
            "INSERT INTO pipeline_meta (key, value_json, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, "
            "updated_at = excluded.updated_at",
            (key, dumps(value), now_iso()),
        )
        if not in_tx:
            self.commit()

    # ------------------------------------------------------------ module locks

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
            row = self.conn.execute(
                "SELECT owner, heartbeat_at FROM module_locks WHERE module = ?", (module,)
            ).fetchone()
            if row is not None and row["owner"] != me:
                age = _age_seconds(row["heartbeat_at"])
                if age is not None and age < LOCK_STALE_SECONDS:
                    self.rollback()
                    raise LockConflict(
                        f"module {module!r} is already being executed by {row['owner']} "
                        f"(last heartbeat {age:.0f}s ago)"
                    )
            self.conn.execute(
                "INSERT INTO module_locks (module, owner, run_id, acquired_at, heartbeat_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(module) DO UPDATE SET owner = excluded.owner, "
                "run_id = excluded.run_id, acquired_at = excluded.acquired_at, "
                "heartbeat_at = excluded.heartbeat_at",
                (module, me, run_id, ts, ts),
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
            self.conn.execute(
                "UPDATE module_locks SET heartbeat_at = ? WHERE module = ? AND owner = ?",
                (now_iso(), module, owner),
            )
            self.commit()
        except sqlite3.OperationalError:  # pragma: no cover - a missed beat is harmless
            self.rollback()

    def release_module_lock(self, module: str, owner: str) -> None:
        try:
            self.begin("immediate")
            self.conn.execute(
                "DELETE FROM module_locks WHERE module = ? AND owner = ?", (module, owner)
            )
            self.commit()
        except sqlite3.OperationalError:  # pragma: no cover
            self.rollback()

    def list_module_locks(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self.conn.execute("SELECT * FROM module_locks").fetchall()]

    @contextmanager
    def module_lock(self, module: str, *, run_id: int | None = None) -> Iterator[str]:
        owner = self.acquire_module_lock(module, run_id=run_id)
        try:
            yield owner
        finally:
            self.release_module_lock(module, owner)

    # ------------------------------------------------------ transaction control

    def begin(self, mode: str = "immediate") -> None:
        """Open a transaction, waiting out other writers before giving up."""
        if self.conn.in_transaction:
            return
        statement = "BEGIN IMMEDIATE" if mode == "immediate" else "BEGIN"
        last: Exception | None = None
        for attempt in range(6):
            try:
                self.conn.execute(statement)
                return
            except sqlite3.OperationalError as exc:  # pragma: no cover - timing dependent
                if not is_lock_error(exc):
                    raise
                last = exc
                time.sleep(min(0.05 * (2**attempt), 1.0) * (0.5 + random.random()))
        raise LockConflict(f"could not start a transaction: {last}")

    def commit(self) -> None:
        if self.conn.in_transaction:
            self.conn.commit()

    def rollback(self) -> None:
        if self.conn.in_transaction:
            try:
                self.conn.rollback()
            except sqlite3.OperationalError:  # pragma: no cover
                pass


def _age_seconds(iso: str | None) -> float | None:
    if not iso:
        return None
    from datetime import datetime, timezone

    try:
        then = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:  # pragma: no cover
        return None
    return (datetime.now(timezone.utc) - then).total_seconds()
