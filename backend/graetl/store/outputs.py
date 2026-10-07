"""Owned outputs: what makes a module deterministic and repeatable.

The problem
-----------
A module that ``INSERT``s its results has two failure modes that no amount of
transactional care fixes:

* running it twice for the same entity doubles its rows, and
* a new version that produces *fewer* rows than the old one leaves the old
  version's extra rows behind - ``copy_labs`` v8 drops a lab code, and every
  entity still carries v7's rows for it.

The rule
--------
Every row written with :meth:`OutputWriter.write` (``ctx.write`` in module
code) is tagged with the entity and module that produced it::

    _graetl_entity   the entity id ("*" for a scope="once" module)
    _graetl_module   the module name
    _graetl_version  the module version
    _graetl_run      the run id

and GraETL remembers, in ``module_outputs``, every table a module has ever
written that way. When a module's unit of work for an entity opens, **all of
the module's rows for that entity are deleted from every one of those tables,
inside the same transaction** - before the module body runs. What the
transaction commits is therefore exactly what this execution produced, and a
failure rolls the deletion back with everything else.

So: same module, same version, same entity => same rows, however often it
runs, and a version bump replaces the previous version's output completely
(including tables the new version no longer writes to at all).

Keyed upserts
-------------
:meth:`OutputWriter.upsert` is for tables *shared* between entities - a
dimension of lab codes, a patient table fed by several admissions. Rows are
inserted or updated by key and are **not** deleted when the entity is
reprocessed, because another entity may have produced the same key. It is
idempotent (twice = once) but, unlike ``write``, does not retract a key an
older version produced. Use ``write`` unless rows are genuinely shared.

Schema
------
Tables are created on first write from the rows themselves, missing columns
are added, and the lineage columns plus an index on
``(_graetl_module, _graetl_entity)`` are added to a pre-existing table. Schema
changes run inside a SAVEPOINT, so a concurrent worker creating the same table
first costs a re-inspection, not the entity.
"""

from __future__ import annotations

import hashlib
import threading
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from graetl.utils import now_iso

if TYPE_CHECKING:  # pragma: no cover
    from graetl.store.state import StateStore

ENTITY_COL = "_graetl_entity"
MODULE_COL = "_graetl_module"
VERSION_COL = "_graetl_version"
RUN_COL = "_graetl_run"
LINEAGE = (ENTITY_COL, MODULE_COL, VERSION_COL, RUN_COL)

MODE_OWNED = "owned"
MODE_UPSERT = "upsert"

#: Schema changes from several workers in one process go one at a time.
_DDL_LOCK = threading.Lock()


class OutputError(ValueError):
    """Module code used ctx.write / ctx.upsert incorrectly."""


def _rows(rows: Any) -> list[dict[str, Any]]:
    """Accept a dict, an iterable of dicts, or a pandas DataFrame."""
    if rows is None:
        return []
    if isinstance(rows, Mapping):
        return [dict(rows)]
    to_dict = getattr(rows, "to_dict", None)
    if callable(to_dict) and hasattr(rows, "columns"):
        return [dict(r) for r in to_dict("records")]
    out = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise OutputError(f"rows must be mappings (dicts), got {type(row).__name__}")
        out.append(dict(row))
    return out


def _short(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]


class OutputWriter:
    """Owned-row writes for one :class:`StateStore` (one worker, one connection)."""

    def __init__(self, store: StateStore) -> None:
        self.store = store
        #: table -> known column names (lower-cased). Trusted only after commit.
        self._columns: dict[str, set[str]] = {}
        #: (module, table) registrations known to be committed.
        self._registered: set[tuple[str, str, str]] = set()
        #: module -> owned tables, loaded once per worker.
        self._owned: dict[str, list[str]] = {}
        #: unique indexes known to exist: (table, key tuple)
        self._unique: set[tuple[str, tuple[str, ...]]] = set()
        self._dirty = False

    @property
    def db(self):
        return self.store.db

    # -------------------------------------------------------------- registry

    def owned_tables(self, module: str) -> list[str]:
        if module not in self._owned:
            rows = self.db.fetchall(
                "SELECT table_name FROM [[module_outputs]] "
                "WHERE pipeline = ? AND module = ? AND mode = ? ORDER BY table_name",
                (self.store.pipeline, module, MODE_OWNED),
            )
            self._owned[module] = [r["table_name"] for r in rows]
        return self._owned[module]

    def registry(self) -> list[dict[str, Any]]:
        rows = self.db.fetchall(
            "SELECT module, table_name, mode, registered_at FROM [[module_outputs]] "
            "WHERE pipeline = ? ORDER BY module, table_name",
            (self.store.pipeline,),
        )
        return [dict(r) for r in rows]

    def _register(self, module: str, table: str, mode: str) -> None:
        key = (module, table, mode)
        if key in self._registered:
            return
        self.db.execute(
            "INSERT INTO [[module_outputs]] (pipeline, module, table_name, mode, registered_at) "
            "VALUES (?, ?, ?, ?, ?) ON CONFLICT(pipeline, module, table_name) DO UPDATE SET "
            "mode = CASE WHEN [[module_outputs]].mode = 'owned' THEN 'owned' ELSE excluded.mode END",
            (self.store.pipeline, module, table, mode, now_iso()),
        )
        self._dirty = True
        self._registered.add(key)
        if mode == MODE_OWNED:
            owned = self._owned.setdefault(module, [])
            if table not in owned:
                owned.append(table)

    # ------------------------------------------------------------ lifecycle

    def committed(self) -> None:
        self._dirty = False

    def rolled_back(self) -> None:
        """Forget anything learnt inside the transaction: its DDL is gone too."""
        if self._dirty:
            self._columns.clear()
            self._registered.clear()
            self._owned.clear()
            self._unique.clear()
        self._dirty = False

    # ----------------------------------------------------------------- clear

    def clear_owned(self, module: str, entity_ids: Sequence[str]) -> int:
        """Delete every row ``module`` owns for ``entity_ids``. Inside the unit's transaction."""
        removed = 0
        if not entity_ids:
            return 0
        q = self.db.dialect.quote
        for table in self.owned_tables(module):
            if self._table_columns(table) is None:
                continue  # dropped by hand; nothing to clear
            for start in range(0, len(entity_ids), 500):
                chunk = list(entity_ids[start : start + 500])
                cur = self.db.execute(
                    f"DELETE FROM {q(table)} WHERE {q(MODULE_COL)} = ? "
                    f"AND {q(ENTITY_COL)} IN ({','.join('?' * len(chunk))})",
                    (module, *chunk),
                )
                removed += max(cur.rowcount or 0, 0)
        return removed

    def purge_module(self, module: str) -> int:
        """Delete everything a module ever wrote as owned rows (module deleted)."""
        q = self.db.dialect.quote
        removed = 0
        for table in self.owned_tables(module):
            if self._table_columns(table) is None:
                continue
            cur = self.db.execute(f"DELETE FROM {q(table)} WHERE {q(MODULE_COL)} = ?", (module,))
            removed += max(cur.rowcount or 0, 0)
        self.db.execute(
            "DELETE FROM [[module_outputs]] WHERE pipeline = ? AND module = ?",
            (self.store.pipeline, module),
        )
        self._owned.pop(module, None)
        self._registered = {k for k in self._registered if k[0] != module}
        return removed

    def rename_module(self, old: str, new: str) -> None:
        """Carry lineage across a module rename, like its entity state."""
        q = self.db.dialect.quote
        for table in self.owned_tables(old):
            if self._table_columns(table) is not None:
                self.db.execute(
                    f"UPDATE {q(table)} SET {q(MODULE_COL)} = ? WHERE {q(MODULE_COL)} = ?",
                    (new, old),
                )
        self.db.execute(
            "DELETE FROM [[module_outputs]] WHERE pipeline = ? AND module = ? "
            "AND table_name IN (SELECT table_name FROM [[module_outputs]] "
            "                   WHERE pipeline = ? AND module = ?)",
            (self.store.pipeline, old, self.store.pipeline, new),
        )
        self.db.execute(
            "UPDATE [[module_outputs]] SET module = ? WHERE pipeline = ? AND module = ?",
            (new, self.store.pipeline, old),
        )
        self._owned.clear()
        self._registered.clear()

    # ----------------------------------------------------------------- write

    def _unit(self) -> dict[str, Any]:
        unit = self.store.unit
        if unit is None:
            raise OutputError(
                "ctx.write / ctx.upsert only work inside a module's unit of work "
                "(an entity, batch or once module) - lineage needs an entity and a module"
            )
        return unit

    def _entity_for(self, unit: dict[str, Any], entity: Any) -> str:
        if unit["scope"] == "once":
            if entity not in (None, unit["entity_ids"][0]):
                raise OutputError("a scope='once' module owns its rows as a whole - no entity=")
            return unit["entity_ids"][0]
        if entity is None:
            if len(unit["entity_ids"]) != 1:
                raise OutputError(
                    "a batch module must say which entity rows belong to: "
                    "ctx.write(table, rows, entity=e)"
                )
            return unit["entity_ids"][0]
        entity_id = str(getattr(entity, "id", entity))
        if entity_id not in unit["entity_ids"]:
            raise OutputError(
                f"entity {entity_id!r} is not part of this unit of work - its previous rows "
                "were not cleared, so writing for it would not be repeatable"
            )
        return entity_id

    def write(
        self,
        table: str,
        rows: Any,
        *,
        entity: Any = None,
        key: Sequence[str] | str | None = None,
    ) -> int:
        """Insert rows owned by the current entity and module. Returns the row count."""
        unit = self._unit()
        data = _rows(rows)
        entity_id = self._entity_for(unit, entity)
        keys = [key] if isinstance(key, str) else list(key or [])
        self._register_and_ensure(unit["module"], table, data, MODE_OWNED, keys)
        if not data:
            return 0
        lineage = {
            ENTITY_COL: entity_id,
            MODULE_COL: unit["module"],
            VERSION_COL: int(unit["version"]),
            RUN_COL: unit["run_id"],
        }
        return self._insert(table, data, lineage, keys)

    def upsert(
        self,
        table: str,
        rows: Any,
        *,
        key: Sequence[str] | str,
        entity: Any = None,
    ) -> int:
        """Insert-or-update rows by ``key`` in a table shared between entities."""
        unit = self._unit()
        keys = [key] if isinstance(key, str) else list(key or [])
        if not keys:
            raise OutputError("ctx.upsert needs key=[...] - the columns that identify a row")
        data = _rows(rows)
        entity_id = self._entity_for(unit, entity) if (entity is not None or
                                                        len(unit["entity_ids"]) == 1) else None
        self._register_and_ensure(unit["module"], table, data, MODE_UPSERT, keys)
        if not data:
            return 0
        lineage = {
            ENTITY_COL: entity_id,
            MODULE_COL: unit["module"],
            VERSION_COL: int(unit["version"]),
            RUN_COL: unit["run_id"],
        }
        return self._insert(table, data, lineage, keys)

    def _insert(
        self,
        table: str,
        data: list[dict[str, Any]],
        lineage: dict[str, Any],
        keys: list[str],
    ) -> int:
        dialect = self.db.dialect
        q = dialect.quote
        columns: list[str] = []
        seen: set[str] = set()
        for row in data:
            for name in row:
                if name not in seen and name not in LINEAGE:
                    seen.add(name)
                    columns.append(name)
        missing = [k for k in keys if k not in seen]
        if missing:
            raise OutputError(f"key column(s) {', '.join(missing)} missing from the rows")
        all_cols = columns + list(LINEAGE)
        head = f"INSERT INTO {q(table)} ({', '.join(q(c) for c in all_cols)})"
        suffix = ""
        if keys:
            updates = [c for c in all_cols if c not in keys]
            suffix = f"ON CONFLICT ({', '.join(q(k) for k in keys)}) DO UPDATE SET " + ", ".join(
                f"{q(c)} = excluded.{q(c)}" for c in updates
            )
            # Two rows with the same key in one statement is an error on
            # PostgreSQL ("cannot affect row a second time"): last one wins.
            dedup: dict[tuple, dict[str, Any]] = {}
            for row in data:
                dedup[tuple(row.get(k) for k in keys)] = row
            data = list(dedup.values())
        adapt = dialect.adapt
        params = [
            tuple([adapt(row.get(c)) for c in columns] + [lineage[c] for c in LINEAGE])
            for row in data
        ]
        return self.db.insert_many(head, params, suffix)

    # ----------------------------------------------------------------- schema

    def _table_columns(self, table: str) -> set[str] | None:
        if table not in self._columns:
            cols = self.db.table_columns(table)
            if cols is None:
                return None
            self._columns[table] = {c.lower() for c in cols}
        return self._columns[table]

    def _register_and_ensure(
        self,
        module: str,
        table: str,
        data: list[dict[str, Any]],
        mode: str,
        keys: list[str],
    ) -> None:
        samples: dict[str, Any] = {}
        order: list[str] = []
        for row in data:
            for name, value in row.items():
                if name in LINEAGE:
                    continue
                if name not in samples:
                    order.append(name)
                    samples[name] = value
                elif samples[name] is None and value is not None:
                    samples[name] = value
        for k in keys:
            if k not in samples:
                order.append(k)
                samples[k] = None

        known = self._table_columns(table)
        needs_table = known is None
        needs_cols = [] if needs_table else [c for c in order if c.lower() not in known]
        needs_lineage = [] if needs_table else [c for c in LINEAGE if c not in known]
        needs_unique = bool(keys) and (table, tuple(keys)) not in self._unique
        needs_register = (module, table, mode) not in self._registered
        if not (needs_table or needs_cols or needs_lineage or needs_unique or needs_register):
            return

        with _DDL_LOCK:
            self._dirty = True
            if needs_table or needs_cols or needs_lineage:
                self._apply_schema(table, order, samples)
            if needs_unique:
                self._ensure_unique(table, keys)
            if needs_register:
                self._register(module, table, mode)

    def _apply_schema(self, table: str, order: list[str], samples: dict[str, Any]) -> None:
        dialect = self.db.dialect
        q = dialect.quote
        for attempt in range(3):
            self._columns.pop(table, None)
            known = self._table_columns(table)
            try:
                with self.db.savepoint("graetl_ddl"):
                    if known is None:
                        defs = [f"{q(c)} {dialect.column_type(samples[c])}" for c in order]
                        defs += [
                            f"{q(ENTITY_COL)} TEXT",
                            f"{q(MODULE_COL)} TEXT",
                            f"{q(VERSION_COL)} INTEGER",
                            f"{q(RUN_COL)} BIGINT",
                        ]
                        self.db.execute(f"CREATE TABLE {q(table)} ({', '.join(defs)})")
                    else:
                        lineage_types = {
                            ENTITY_COL: "TEXT", MODULE_COL: "TEXT",
                            VERSION_COL: "INTEGER", RUN_COL: "BIGINT",
                        }
                        for c in order:
                            if c.lower() not in known:
                                self.db.execute(
                                    f"ALTER TABLE {q(table)} ADD COLUMN {q(c)} "
                                    f"{dialect.column_type(samples[c])}"
                                )
                        for c in LINEAGE:
                            if c not in known:
                                self.db.execute(
                                    f"ALTER TABLE {q(table)} ADD COLUMN {q(c)} {lineage_types[c]}"
                                )
                    index = f"ix_graetl_own_{_short(table)}"
                    self.db.execute(
                        f"CREATE INDEX IF NOT EXISTS {q(index)} ON {q(table)} "
                        f"({q(MODULE_COL)}, {q(ENTITY_COL)})"
                    )
                break
            except Exception as exc:  # noqa: BLE001 - another worker raced us
                text = str(exc).lower()
                if attempt < 2 and ("exists" in text or "duplicate" in text):
                    continue
                raise
        self._columns.pop(table, None)
        self._table_columns(table)

    def _ensure_unique(self, table: str, keys: list[str]) -> None:
        q = self.db.dialect.quote
        index = f"uq_graetl_{_short(table + '|' + '|'.join(keys))}"
        try:
            with self.db.savepoint("graetl_uq"):
                self.db.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {q(index)} ON {q(table)} "
                    f"({', '.join(q(k) for k in keys)})"
                )
        except Exception as exc:  # noqa: BLE001
            raise OutputError(
                f"cannot use ({', '.join(keys)}) as the key of {table}: {exc} - "
                "the table already holds duplicate keys"
            ) from exc
        self._unique.add((table, tuple(keys)))

