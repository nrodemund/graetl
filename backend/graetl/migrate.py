"""Import a pre-project GraETL installation into a project.

Before projects, one folder held everything: ``pipelines/etl.db`` for the
registry and run history, and one ``pipelines/<id>/state.db`` per pipeline
holding **both** that pipeline's entity state and the tables its modules wrote
- because back then ``ctx.db`` *was* the state database.

A project inverts that: one target database per warehouse, with GraETL's own
tables namespaced inside it. So importing means two different jobs:

* **bookkeeping** - entities, module state, meta, the registry and run history -
  is copied into GraETL's namespace with the pipeline name filled in, since one
  target now serves every pipeline in the project.
* **warehouse data** - every other table in the old ``state.db`` - is what the
  pipelines actually produced, and it is moved verbatim.

For a SQLite target both happen in one pass with ``ATTACH``, which is fast even
for a 60 GB state database and never rewrites a row it does not have to. For a
PostgreSQL target only the bookkeeping can be carried across automatically: the
data tables were created by SQLite DDL that PostgreSQL will not accept, and
guessing a translation would be worse than saying so. The report names every
table that was left behind.

Nothing is destroyed. The old folder is read-only throughout.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from graetl.project import Project
from graetl.store.core import CoreStore
from graetl.store.db import Database, connect
from graetl.store.state import StateStore

#: Tables the old per-pipeline state.db owned. Everything else in it is data.
LEGACY_STATE_TABLES = ("entities", "entity_module_state", "pipeline_meta", "module_locks")
LEGACY_CORE_TABLES = ("pipelines", "runs", "run_steps", "run_events", "settings")


@dataclass
class ImportReport:
    pipelines: list[str] = field(default_factory=list)
    entities: int = 0
    state_rows: int = 0
    meta_rows: int = 0
    runs: int = 0
    data_tables: list[str] = field(default_factory=list)
    skipped_tables: list[str] = field(default_factory=list)
    collisions: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def lines(self) -> list[str]:
        out = [f"pipelines   {len(self.pipelines)}: {', '.join(self.pipelines) or '-'}"]
        out.append(f"entities    {self.entities}")
        out.append(f"state rows  {self.state_rows}")
        out.append(f"meta rows   {self.meta_rows}")
        out.append(f"runs        {self.runs}")
        out.append(f"data tables {len(self.data_tables)}: {', '.join(self.data_tables) or '-'}")
        if self.collisions:
            out.append(f"COLLISIONS  {', '.join(self.collisions)}")
        if self.skipped_tables:
            out.append(f"not moved   {', '.join(self.skipped_tables)}")
        out.extend(self.notes)
        return out


def _tables(conn: sqlite3.Connection, schema: str = "main") -> list[str]:
    rows = conn.execute(
        f"SELECT name FROM {schema}.sqlite_master WHERE type = 'table' "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def _columns(conn: sqlite3.Connection, table: str, schema: str = "main") -> list[str]:
    return [r[1] for r in conn.execute(f'PRAGMA {schema}.table_info("{table}")').fetchall()]


def import_legacy(
    old_pipelines_dir: str | Path,
    project: Project,
    *,
    move_data: bool = True,
    dry_run: bool = False,
) -> ImportReport:
    """Copy a pre-project installation into ``project``'s target database."""
    old = Path(old_pipelines_dir).expanduser().resolve()
    report = ImportReport()
    if not old.is_dir():
        raise FileNotFoundError(f"{old} does not exist")

    db = connect(project.target)
    try:
        # Creating the stores is what applies the schema.
        CoreStore(db)
        for folder in sorted(p for p in old.iterdir() if p.is_dir()):
            state_db = folder / "state.db"
            if not state_db.exists():
                continue
            report.pipelines.append(folder.name)
            StateStore(db, folder.name)
            _import_pipeline(db, state_db, folder.name, report, move_data, dry_run)

        core_db = old / "etl.db"
        if core_db.exists():
            _import_core(db, core_db, report, dry_run)
    finally:
        db.close()
    return report


def _import_pipeline(
    db: Database,
    state_db: Path,
    pipeline: str,
    report: ImportReport,
    move_data: bool,
    dry_run: bool,
) -> None:
    if db.dialect.name != "sqlite":
        report.notes.append(
            f"{pipeline}: warehouse tables were left in {state_db} - a PostgreSQL target "
            "cannot take SQLite DDL, so move them with your own tooling"
        )
        _import_pipeline_rows(db, state_db, pipeline, report, dry_run)
        return

    conn: sqlite3.Connection = db.conn
    db.commit()
    conn.execute("ATTACH DATABASE ? AS legacy", (str(state_db),))
    try:
        legacy = set(_tables(conn, "legacy"))
        existing = set(_tables(conn))

        db.begin("immediate")
        # --- bookkeeping, with the pipeline column filled in
        if "entities" in legacy:
            cols = [c for c in _columns(conn, "entities", "legacy")]
            names = ", ".join(f'"{c}"' for c in cols)
            cur = conn.execute(
                f"INSERT OR IGNORE INTO graetl_entities (pipeline, {names}) "
                f"SELECT ?, {names} FROM legacy.entities",
                (pipeline,),
            )
            report.entities += cur.rowcount or 0
        if "entity_module_state" in legacy:
            cols = _columns(conn, "entity_module_state", "legacy")
            names = ", ".join(f'"{c}"' for c in cols)
            cur = conn.execute(
                f"INSERT OR IGNORE INTO graetl_entity_module_state (pipeline, {names}) "
                f"SELECT ?, {names} FROM legacy.entity_module_state",
                (pipeline,),
            )
            report.state_rows += cur.rowcount or 0
        if "pipeline_meta" in legacy:
            cur = conn.execute(
                "INSERT OR IGNORE INTO graetl_pipeline_meta "
                "(pipeline, key, value_json, updated_at) "
                "SELECT ?, key, value_json, updated_at FROM legacy.pipeline_meta",
                (pipeline,),
            )
            report.meta_rows += cur.rowcount or 0

        # --- warehouse data: every other table, moved as it stands
        if move_data:
            for table in sorted(legacy - set(LEGACY_STATE_TABLES)):
                if table in existing:
                    # Two pipelines wrote a table of the same name. Merging them
                    # silently could corrupt both, so the user decides.
                    report.collisions.append(f"{pipeline}.{table}")
                    continue
                ddl = conn.execute(
                    "SELECT sql FROM legacy.sqlite_master WHERE type='table' AND name = ?",
                    (table,),
                ).fetchone()
                if not ddl or not ddl[0]:  # pragma: no cover - defensive
                    continue
                conn.execute(ddl[0])
                conn.execute(f'INSERT INTO "{table}" SELECT * FROM legacy."{table}"')
                report.data_tables.append(table)
                for index in conn.execute(
                    "SELECT sql FROM legacy.sqlite_master WHERE type='index' AND tbl_name = ? "
                    "AND sql IS NOT NULL",
                    (table,),
                ).fetchall():
                    try:
                        conn.execute(index[0])
                    except sqlite3.OperationalError:  # pragma: no cover - duplicate name
                        pass
        else:
            report.skipped_tables.extend(sorted(legacy - set(LEGACY_STATE_TABLES)))

        if dry_run:
            db.rollback()
        else:
            db.commit()
    finally:
        db.rollback()
        conn.execute("DETACH DATABASE legacy")


def _import_pipeline_rows(
    db: Database, state_db: Path, pipeline: str, report: ImportReport, dry_run: bool
) -> None:
    """Row-by-row bookkeeping copy - the only option for a non-SQLite target."""
    src = sqlite3.connect(f"file:{state_db.as_posix()}?mode=ro", uri=True)
    src.row_factory = sqlite3.Row
    try:
        legacy = set(_tables(src))
        report.skipped_tables.extend(sorted(legacy - set(LEGACY_STATE_TABLES)))
        db.begin("immediate")
        for table, counter in (
            ("entities", "entities"),
            ("entity_module_state", "state_rows"),
            ("pipeline_meta", "meta_rows"),
        ):
            if table not in legacy:
                continue
            cols = _columns(src, table)
            names = ", ".join(f'"{c}"' for c in cols)
            marks = ", ".join("?" * (len(cols) + 1))
            moved = 0
            for row in src.execute(f"SELECT {names} FROM {table}"):
                db.execute(
                    f"INSERT INTO [[{table}]] (pipeline, {names}) VALUES ({marks}) "
                    "ON CONFLICT DO NOTHING",
                    (pipeline, *tuple(row)),
                )
                moved += 1
            setattr(report, counter, getattr(report, counter) + moved)
        db.rollback() if dry_run else db.commit()
    finally:
        src.close()


def _import_core(db: Database, core_db: Path, report: ImportReport, dry_run: bool) -> None:
    """The registry and run history. Run ids are kept so logs still line up."""
    src = sqlite3.connect(f"file:{core_db.as_posix()}?mode=ro", uri=True)
    src.row_factory = sqlite3.Row
    try:
        present = set(_tables(src))
        db.begin("immediate")
        for table in LEGACY_CORE_TABLES:
            if table not in present:
                continue
            target_cols = set(_target_columns(db, table))
            cols = [c for c in _columns(src, table) if c in target_cols]
            if not cols:  # pragma: no cover - schema drifted beyond recognition
                continue
            names = ", ".join(f'"{c}"' for c in cols)
            marks = ", ".join("?" * len(cols))
            moved = 0
            for row in src.execute(f"SELECT {names} FROM {table}"):
                db.execute(
                    f"INSERT INTO [[{table}]] ({names}) VALUES ({marks}) ON CONFLICT DO NOTHING",
                    tuple(row),
                )
                moved += 1
            if table == "runs":
                report.runs += moved
        db.rollback() if dry_run else db.commit()
    finally:
        src.close()


def _target_columns(db: Database, logical: str) -> Iterable[str]:
    physical = db.dialect.table(logical)
    if db.dialect.name == "sqlite":
        return [r[1] for r in db.conn.execute(f'PRAGMA table_info("{physical}")').fetchall()]
    schema, _, name = physical.partition(".")
    rows = db.fetchall(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ?",
        (schema, name),
    )
    return [r["column_name"] for r in rows]


def describe_legacy(old_pipelines_dir: str | Path) -> dict[str, Any]:
    """What an import would find, without touching anything."""
    old = Path(old_pipelines_dir).expanduser().resolve()
    found: dict[str, Any] = {"root": str(old), "pipelines": [], "core_db": None}
    if (old / "etl.db").exists():
        found["core_db"] = str(old / "etl.db")
    for folder in sorted(p for p in old.iterdir() if p.is_dir()) if old.is_dir() else []:
        state_db = folder / "state.db"
        if not state_db.exists():
            continue
        conn = sqlite3.connect(f"file:{state_db.as_posix()}?mode=ro", uri=True)
        try:
            tables = _tables(conn)
            entities = 0
            if "entities" in tables:
                entities = int(conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0])
            found["pipelines"].append(
                {
                    "id": folder.name,
                    "entities": entities,
                    "data_tables": [t for t in tables if t not in LEGACY_STATE_TABLES],
                    "bytes": state_db.stat().st_size,
                }
            )
        finally:
            conn.close()
    return found
