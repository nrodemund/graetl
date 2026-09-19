"""Shared SQLite plumbing.

Every database GraETL creates runs in WAL mode with a busy timeout, so the
server process, the runner process and the UI can read/write concurrently
without stepping on each other.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable

BUSY_TIMEOUT_MS = 15_000


def connect(path: str | Path, *, readonly: bool = False, timeout: float = 15.0) -> sqlite3.Connection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if readonly and path.exists():
        conn = sqlite3.connect(
            f"file:{path.as_posix()}?mode=ro", uri=True, timeout=timeout, check_same_thread=False
        )
    else:
        conn = sqlite3.connect(str(path), timeout=timeout, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    if not readonly:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def apply_migrations(conn: sqlite3.Connection, migrations: Iterable[str]) -> int:
    """Apply a list of SQL scripts, tracking the applied count in ``user_version``."""
    migrations = list(migrations)
    current = int(conn.execute("PRAGMA user_version").fetchone()[0])
    for index, script in enumerate(migrations):
        if index < current:
            continue
        with conn:
            conn.executescript(script)
            conn.execute(f"PRAGMA user_version = {index + 1}")
    return len(migrations)


def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]
