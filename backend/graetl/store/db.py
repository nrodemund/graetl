"""One database abstraction over SQLite and PostgreSQL.

A GraETL **project** is a data warehouse, and its target database holds two
things: the tables the pipelines write, and GraETL's own bookkeeping (the
pipeline registry, run history, entity state, module locks). They live in the
same database on purpose - a module's data write and its state row commit in
**one transaction**, so "state says done, data was never written" stays
impossible on both backends. That guarantee is the reason this layer exists.

GraETL's own tables are kept out of the warehouse's namespace:

===========  ==========================================
SQLite       ``graetl_entities``, ``graetl_runs``, ...
PostgreSQL   schema ``graetl``: ``graetl.entities``, ...
===========  ==========================================

Writing portable SQL
--------------------
SQL is written **once**, in SQLite's spelling, and translated per dialect:

* ``[[entities]]`` - a logical table name, replaced by the physical one.
* ``?`` - a parameter, rewritten to ``%s`` for PostgreSQL. The rewriter skips
  string literals, quoted identifiers and comments, so a ``?`` inside a literal
  survives.
* ``[[ilike]]`` - ``LIKE`` on SQLite (already case-insensitive for ASCII),
  ``ILIKE`` on PostgreSQL, so entity search behaves the same on both.
* ``[[pk]]`` - the auto-incrementing primary key declaration.

Everything else in GraETL's SQL is already common to both: ``ON CONFLICT ... DO
UPDATE``/``DO NOTHING`` with ``excluded``, ``COALESCE``, ``ORDER BY random()``,
and ISO-8601 timestamps compared as text.

Drivers
-------
PostgreSQL is reached through ``psycopg`` (v3) when it is installed, then
``psycopg2``, then the bundled :mod:`graetl.store.pgwire`. All three take
``%s`` parameters and return mapping-like rows, which is what makes one code
path possible. ``graetl doctor`` reports which one is in use.
"""

from __future__ import annotations

import importlib
import random
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

BUSY_TIMEOUT_MS = 15_000

#: Every table GraETL owns. The physical name is the dialect's business.
LOGICAL_TABLES = (
    "schema_version",
    "pipelines",
    "runs",
    "run_steps",
    "run_events",
    "settings",
    "entities",
    "entity_module_state",
    "module_locks",
    "pipeline_meta",
    "module_outputs",
)


class LockConflict(RuntimeError):
    """Another writer held the row or the database; the caller should retry.

    Never a failure of the work itself - nothing is recorded, and the entity or
    batch is simply attempted again.
    """


class LockLost(RuntimeError):
    """This worker no longer holds the module lock it is writing under.

    Raised inside an entity/batch transaction *before* its state row is
    written, so the transaction rolls back and nothing is recorded: another
    worker took the module over (its heartbeat went stale) and is now the only
    one allowed to say "done". Continuing would let two workers write the same
    module's results.
    """


class TargetUnreachable(RuntimeError):
    """The project's target database could not be opened.

    Carries a human-readable reason, because this is the error the project
    picker shows when someone opens a project whose warehouse is down.
    """


# --------------------------------------------------------------------- parsing


def rewrite_params(sql: str) -> str:
    """Turn ``?`` placeholders into ``%s``, leaving literals alone.

    A regex would corrupt ``WHERE note LIKE '%?%'``; this walks the string
    instead, skipping single-quoted literals (with ``''`` escapes),
    double-quoted identifiers, dollar-quoted bodies, ``--`` line comments and
    ``/* */`` block comments. Percent signs are doubled so the result is a
    valid format-style statement.
    """
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'" or ch == '"':
            quote = ch
            out.append(ch)
            i += 1
            while i < n:
                if sql[i] == quote:
                    if i + 1 < n and sql[i + 1] == quote:  # '' escape
                        out.append(sql[i : i + 2])
                        i += 2
                        continue
                    out.append(quote)
                    i += 1
                    break
                out.append("%%" if sql[i] == "%" else sql[i])
                i += 1
            continue
        if ch == "-" and sql.startswith("--", i):
            end = sql.find("\n", i)
            end = n if end == -1 else end
            out.append(sql[i:end].replace("%", "%%"))
            i = end
            continue
        if ch == "/" and sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            end = n if end == -1 else end + 2
            out.append(sql[i:end].replace("%", "%%"))
            i = end
            continue
        if ch == "?":
            out.append("%s")
            i += 1
            continue
        if ch == "%":
            out.append("%%")
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


# -------------------------------------------------------------------- dialects


class Dialect:
    """What differs between the backends. Everything else is shared SQL."""

    name: str = "sqlite"
    #: Declaration for an auto-incrementing integer primary key.
    pk: str = "INTEGER PRIMARY KEY AUTOINCREMENT"
    ilike: str = "LIKE"
    #: PostgreSQL has no equivalent of BEGIN IMMEDIATE; it takes row locks.
    supports_immediate: bool = True
    #: Row lock that conflicts with a concurrent UPDATE of the row. SQLite
    #: serialises writers on the whole database, so it needs none.
    for_share: str = ""

    def __init__(self, namespace: str = "graetl") -> None:
        self.namespace = namespace

    def table(self, logical: str) -> str:  # pragma: no cover - overridden
        raise NotImplementedError

    def sql(self, sql: str, *, params: bool = True) -> str:
        """Apply every dialect substitution to one statement.

        ``params=False`` skips the placeholder rewrite, for scripts that carry
        no parameters: doubling their percent signs would corrupt a ``LIKE``
        pattern that a driver is never going to interpolate.
        """
        for logical in LOGICAL_TABLES:
            marker = f"[[{logical}]]"
            if marker in sql:
                sql = sql.replace(marker, self.table(logical))
        sql = sql.replace("[[ilike]]", self.ilike).replace("[[pk]]", self.pk)
        if "[[" in sql:
            sql = (
                sql.replace("[[for_share]]", self.for_share)
                .replace("[[seq_next]]", self.seq_next())
                .replace("[[create_seq]]", self.create_seq())
            )
        return sql

    def seq_next(self) -> str:  # pragma: no cover - overridden
        """An expression yielding the next processing sequence number."""
        raise NotImplementedError

    def create_seq(self) -> str:  # pragma: no cover - overridden
        return ""

    def quote(self, name: str) -> str:
        """Quote a (possibly schema-qualified) identifier: ``dw.labs`` -> ``"dw"."labs"``."""
        parts = str(name).split(".")
        if not all(parts):
            raise ValueError(f"invalid table name {name!r}")
        return ".".join('"' + p.replace('"', '""') + '"' for p in parts)

    def column_type(self, value: Any) -> str:  # pragma: no cover - overridden
        raise NotImplementedError

    def adapt(self, value: Any) -> Any:
        """A Python value as the driver should receive it."""
        if isinstance(value, (dict, list, tuple)):
            import json

            return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
        return value

    def is_lock_error(self, exc: BaseException) -> bool:  # pragma: no cover - overridden
        raise NotImplementedError


class SqliteDialect(Dialect):
    name = "sqlite"
    pk = "INTEGER PRIMARY KEY AUTOINCREMENT"
    ilike = "LIKE"
    supports_immediate = True

    def table(self, logical: str) -> str:
        return f"{self.namespace}_{logical}"

    def seq_next(self) -> str:
        # SQLite serialises writers on the whole database, so MAX()+1 taken
        # inside the writing statement is already a strictly increasing,
        # commit-ordered sequence. The index makes it a single seek.
        return (
            f"(SELECT COALESCE(MAX(processed_seq), 0) + 1 "
            f"FROM {self.table('entity_module_state')})"
        )

    def column_type(self, value: Any) -> str:
        if isinstance(value, bool) or isinstance(value, int):
            return "INTEGER"
        if isinstance(value, float):
            return "REAL"
        if isinstance(value, (bytes, bytearray, memoryview)):
            return "BLOB"
        from decimal import Decimal

        if isinstance(value, Decimal):
            return "NUMERIC"
        return "TEXT"

    def adapt(self, value: Any) -> Any:
        from datetime import date, datetime, time as dtime
        from decimal import Decimal

        if isinstance(value, (datetime, date, dtime)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        return super().adapt(value)

    def is_lock_error(self, exc: BaseException) -> bool:
        if not isinstance(exc, sqlite3.OperationalError):
            return False
        text = str(exc).lower()
        return "locked" in text or "busy" in text


class PostgresDialect(Dialect):
    name = "postgres"
    # BIGINT identity rather than SERIAL: SERIAL is legacy, and run ids are the
    # one sequence that could plausibly outgrow 32 bits on a busy warehouse.
    pk = "BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY"
    ilike = "ILIKE"
    supports_immediate = False

    for_share = " FOR SHARE"

    def table(self, logical: str) -> str:
        return f"{self.namespace}.{logical}"

    def seq_next(self) -> str:
        # nextval() is called while the row is written, i.e. after everything
        # this transaction read was committed by its writers - so a consumer's
        # number is always greater than that of the upstream row it read.
        return f"nextval('{self.namespace}.state_seq')"

    def create_seq(self) -> str:
        return f"CREATE SEQUENCE IF NOT EXISTS {self.namespace}.state_seq;"

    def column_type(self, value: Any) -> str:
        from datetime import date, datetime, time as dtime
        from decimal import Decimal

        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "BIGINT"
        if isinstance(value, float):
            return "DOUBLE PRECISION"
        if isinstance(value, Decimal):
            return "NUMERIC"
        if isinstance(value, datetime):
            return "TIMESTAMPTZ" if value.tzinfo else "TIMESTAMP"
        if isinstance(value, date):
            return "DATE"
        if isinstance(value, dtime):
            return "TIME"
        if isinstance(value, (bytes, bytearray, memoryview)):
            return "BYTEA"
        return "TEXT"

    def sql(self, sql: str, *, params: bool = True) -> str:
        out = super().sql(sql, params=params)
        return rewrite_params(out) if params else out

    def is_lock_error(self, exc: BaseException) -> bool:
        from graetl.store import pgwire

        if pgwire.is_retryable(exc):
            return True
        # psycopg/psycopg2 raise their own classes; both expose the sqlstate.
        state = getattr(exc, "sqlstate", None) or getattr(
            getattr(exc, "diag", None), "sqlstate", None
        )
        return state in pgwire.RETRYABLE_SQLSTATES


# ---------------------------------------------------------------------- target


@dataclass(slots=True)
class Target:
    """Where a project's data and bookkeeping live.

    ``system`` is ``sqlite`` or ``postgres``. For SQLite, ``path`` is the
    database file. For PostgreSQL, ``dsn`` is a libpq connection string and
    ``schema`` is where GraETL's own tables go (the warehouse tables themselves
    go wherever the pipeline code puts them).
    """

    system: str = "sqlite"
    path: Path | None = None
    dsn: str = ""
    schema: str = "graetl"

    @property
    def is_postgres(self) -> bool:
        return self.system == "postgres"

    def describe(self) -> str:
        """A one-line, credential-free description for the UI and logs."""
        if self.system == "sqlite":
            return f"sqlite {self.path.name if self.path else '?'}"
        return f"postgres {redact_dsn(self.dsn)}"

    def dialect(self) -> Dialect:
        if self.is_postgres:
            return PostgresDialect(self.schema)
        return SqliteDialect("graetl")


def redact_dsn(dsn: str) -> str:
    """``postgresql://u:secret@host/db`` -> ``postgresql://u@host/db``.

    A DSN reaches the console, the run log and the project picker; the password
    in it must not.
    """
    if not dsn:
        return ""
    text = dsn
    if "://" in text:
        scheme, _, rest = text.partition("://")
        if "@" in rest:
            creds, _, hostpart = rest.rpartition("@")
            user = creds.split(":", 1)[0]
            return f"{scheme}://{user}@{hostpart}"
        return text
    return " ".join(
        part for part in text.split() if not part.lower().startswith("password=")
    )


# -------------------------------------------------------------------- database


class Database:
    """A single connection to the project's target, in one dialect.

    Deliberately thin: it is also what module code writes through as
    ``ctx.db``, so its surface stays close to a DB-API connection. One
    connection per worker - see the module docstring of
    :mod:`graetl.store.state`.
    """

    def __init__(self, conn: Any, dialect: Dialect, *, target: Target | None = None) -> None:
        self.conn = conn
        self.dialect = dialect
        self.target = target
        self._depth = 0
        self._in_tx = False

    # ------------------------------------------------------------- statements

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        """Run one statement. ``?`` placeholders and ``[[table]]`` markers are
        translated for the backend, so pipeline code written against SQLite
        runs unchanged against PostgreSQL."""
        if params is None:
            return self.conn.execute(self.dialect.sql(sql, params=False))
        return self.conn.execute(self.dialect.sql(sql), tuple(params))

    def executemany(self, sql: str, seq: Iterable[Sequence[Any]]) -> Any:
        params = [tuple(p) for p in seq]
        many = getattr(self.conn, "executemany", None)
        if many is None:  # pgwire: executemany lives on the cursor
            many = self.conn.cursor().executemany
        return many(self.dialect.sql(sql), params)

    def insert_many(
        self,
        head: str,
        rows: Sequence[Sequence[Any]],
        suffix: str = "",
        *,
        max_params: int = 30_000,
    ) -> int:
        """``head VALUES (...), (...) suffix`` for many rows, fast on both backends.

        ``head`` is ``INSERT INTO t (a, b)``; ``suffix`` may carry an ``ON
        CONFLICT`` clause. SQLite is in-process, so ``executemany`` is already
        cheap; PostgreSQL pays a network round trip per statement, so rows are
        sent as multi-row VALUES lists, chunked under the protocol's parameter
        limit.
        """
        if not rows:
            return 0
        width = len(rows[0])
        if self.dialect.name == "sqlite":
            one = "(" + ", ".join("?" * width) + ")"
            self.executemany(f"{head} VALUES {one} {suffix}", rows)
            return len(rows)
        per = max(1, max_params // max(width, 1))
        one = "(" + ", ".join("?" * width) + ")"
        for start in range(0, len(rows), per):
            chunk = rows[start : start + per]
            flat = [value for row in chunk for value in row]
            self.execute(f"{head} VALUES {', '.join([one] * len(chunk))} {suffix}", flat)
        return len(rows)

    def executescript(self, sql: str) -> None:
        """Run a multi-statement script (schema DDL)."""
        statement = self.dialect.sql(sql, params=False)
        if self.dialect.name == "sqlite":
            self.conn.executescript(statement)
        else:
            self.conn.execute(statement)

    def fetchone(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        return self.execute(sql, params).fetchone()

    def fetchall(self, sql: str, params: Sequence[Any] | None = None) -> list[Any]:
        return self.execute(sql, params).fetchall()

    def insert(self, sql: str, params: Sequence[Any], *, returning: str = "id") -> int:
        """INSERT and hand back the generated key.

        SQLite reads ``lastrowid``; PostgreSQL has no such thing, so the
        statement gets a ``RETURNING`` clause.
        """
        if self.dialect.name == "sqlite":
            cur = self.execute(sql, params)
            return int(cur.lastrowid)
        row = self.execute(f"{sql.rstrip().rstrip(';')} RETURNING {returning}", params).fetchone()
        return int(row[0])

    # ----------------------------------------------------------- transactions

    @property
    def in_transaction(self) -> bool:
        if self.dialect.name == "sqlite":
            return bool(self.conn.in_transaction)
        return self._in_tx

    def begin(self, mode: str = "immediate") -> None:
        """Open a transaction, waiting out other writers before giving up.

        ``immediate`` takes the write lock up front (SQLite) - used when
        modules run one at a time. ``deferred`` takes it at the first write, so
        parallel modules serialise only around their writes. PostgreSQL has no
        such distinction: it takes row locks as it goes, and a conflict surfaces
        as a retryable error rather than at BEGIN.
        """
        if self.in_transaction:
            return
        if self.dialect.name == "sqlite":
            statement = "BEGIN IMMEDIATE" if mode == "immediate" else "BEGIN"
        else:
            statement = "BEGIN"
        last: Exception | None = None
        for attempt in range(6):
            try:
                self.conn.execute(statement)
                self._in_tx = True
                return
            except Exception as exc:  # noqa: BLE001 - re-raised below
                if not self.dialect.is_lock_error(exc):
                    raise
                last = exc
                time.sleep(min(0.05 * (2**attempt), 1.0) * (0.5 + random.random()))
        raise LockConflict(f"could not start a transaction: {last}")

    def commit(self) -> None:
        if not self.in_transaction:
            return
        if self.dialect.name == "sqlite":
            self.conn.commit()
        else:
            self.conn.execute("COMMIT")
            self._in_tx = False

    def rollback(self) -> None:
        if not self.in_transaction:
            return
        try:
            if self.dialect.name == "sqlite":
                self.conn.rollback()
            else:
                self.conn.execute("ROLLBACK")
        except Exception:  # pragma: no cover - the connection is going away anyway
            pass
        finally:
            self._in_tx = False

    @contextmanager
    def savepoint(self, name: str = "graetl_sp") -> Iterator[None]:
        """A nested unit inside the current transaction.

        Used for schema changes made from module code: if a concurrent worker
        created the same table first, only the savepoint is rolled back and the
        surrounding entity transaction carries on.
        """
        self.execute(f"SAVEPOINT {name}")
        try:
            yield
        except BaseException:
            try:
                self.execute(f"ROLLBACK TO SAVEPOINT {name}")
                self.execute(f"RELEASE SAVEPOINT {name}")
            except Exception:  # pragma: no cover - the outer rollback cleans up
                pass
            raise
        else:
            self.execute(f"RELEASE SAVEPOINT {name}")

    def table_columns(self, table: str) -> list[str] | None:
        """Column names of a warehouse table, or None when it does not exist."""
        parts = str(table).split(".")
        if self.dialect.name == "sqlite":
            if len(parts) == 2:
                sql = f'PRAGMA {self.dialect.quote(parts[0])}.table_info({self.dialect.quote(parts[1])})'
            else:
                sql = f"PRAGMA table_info({self.dialect.quote(parts[0])})"
            rows = self.conn.execute(sql).fetchall()
            return [r["name"] for r in rows] if rows else None
        schema, name = (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])
        if schema is None:
            row = self.fetchone("SELECT current_schema() AS s")
            schema = row["s"]
        rows = self.fetchall(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            (schema, name),
        )
        return [r["column_name"] for r in rows] if rows else None

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # pragma: no cover - defensive
            pass

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# ------------------------------------------------------------------ connecting


def _connect_sqlite(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=15.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None  # explicit transaction control
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


#: Postgres drivers in order of preference. psycopg 3 is what a user should
#: install; pgwire is the bundled fallback so an air-gapped install still works.
PG_DRIVERS = ("psycopg", "psycopg2", "graetl.store.pgwire")


def postgres_driver() -> tuple[Any, str]:
    """The first available PostgreSQL driver, and its name."""
    errors: list[str] = []
    for name in PG_DRIVERS:
        try:
            return importlib.import_module(name), name.rsplit(".", 1)[-1]
        except ImportError as exc:  # pragma: no cover - depends on the install
            errors.append(f"{name}: {exc}")
    raise TargetUnreachable(  # pragma: no cover - pgwire is always importable
        "no PostgreSQL driver available (" + "; ".join(errors) + ")"
    )


def _connect_postgres(dsn: str) -> Any:
    driver, name = postgres_driver()
    try:
        if name == "psycopg":
            conn = driver.connect(dsn, autocommit=True, row_factory=_psycopg_rows(driver))
        elif name == "psycopg2":
            import psycopg2.extras  # type: ignore[import-not-found]

            conn = driver.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor)
            conn.autocommit = True
            conn = _Psycopg2Shim(conn)
        else:
            conn = driver.connect(dsn, autocommit=True)
    except Exception as exc:  # noqa: BLE001 - reported to the user as-is
        raise TargetUnreachable(f"{redact_dsn(dsn)}: {exc}") from exc
    return conn


def _psycopg_rows(driver: Any) -> Any:
    """psycopg 3 rows as mappings, matching sqlite3.Row and pgwire."""
    from psycopg.rows import dict_row  # type: ignore[import-not-found]

    return dict_row


class _Psycopg2Shim:
    """psycopg2 has no connection-level ``execute``; give it one."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        cur = self._conn.cursor()
        cur.execute(sql, params)
        return cur

    def executemany(self, sql: str, seq: Any) -> Any:
        cur = self._conn.cursor()
        cur.executemany(sql, seq)
        return cur

    def close(self) -> None:
        self._conn.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


def connect(target: Target) -> Database:
    """Open the project's target database and make sure GraETL's namespace exists."""
    dialect = target.dialect()
    if target.is_postgres:
        conn = _connect_postgres(target.dsn)
        db = Database(conn, dialect, target=target)
        db.execute(f'CREATE SCHEMA IF NOT EXISTS "{target.schema}"')
        # A login role named like GraETL's schema (user "graetl", schema
        # "graetl") puts that schema first on the default search_path
        # ("$user", public), so every unqualified warehouse table a pipeline
        # creates would land among GraETL's bookkeeping. Keep them apart.
        row = db.fetchone("SELECT current_schema() AS s")
        if row is not None and row["s"] == target.schema:
            db.execute("SET search_path TO public")
    else:
        if target.path is None:  # pragma: no cover - guarded by config loading
            raise TargetUnreachable("the sqlite target has no path")
        db = Database(_connect_sqlite(target.path), dialect, target=target)
    return db


# ------------------------------------------------------------------ migrations


def apply_migrations(db: Database, migrations: Sequence[str], component: str) -> int:
    """Apply the scripts a component needs, tracking how many have run.

    SQLite's ``PRAGMA user_version`` has no PostgreSQL equivalent and there is
    now one database for several components, so the count lives in a table
    keyed by component name.
    """
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS [[schema_version]] (
            component TEXT PRIMARY KEY,
            version   INTEGER NOT NULL
        );
        """
    )
    row = db.fetchone(
        "SELECT version FROM [[schema_version]] WHERE component = ?", (component,)
    )
    current = int(row["version"] if row is not None else 0)
    for index, script in enumerate(migrations):
        if index < current:
            continue
        db.begin("immediate")
        try:
            db.executescript(script)
            db.execute(
                "INSERT INTO [[schema_version]] (component, version) VALUES (?, ?) "
                "ON CONFLICT(component) DO UPDATE SET version = excluded.version",
                (component, index + 1),
            )
            db.commit()
        except Exception:
            db.rollback()
            raise
    return len(migrations)


def rows_to_dicts(rows: Iterable[Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


class Synchronized:
    """Mixin giving a store one lock around every public method.

    The server serves several requests at once off one connection; the runner
    gives each worker its own.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
