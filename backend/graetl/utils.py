"""Small shared helpers."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

_SLUG_RE = re.compile(r"[^a-z0-9_-]+")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    """Timestamp used everywhere in the databases: ISO-8601, UTC, millisecond precision."""
    return utcnow().isoformat(timespec="milliseconds").replace("+00:00", "Z")


def to_iso(value: Any) -> str | None:
    """Normalise anything timestamp-ish into the GraETL ISO format.

    Used for entity revisions (``source_updated_at``) so that string comparison
    is a valid ordering. ``None`` stays ``None``; plain strings are passed through
    (a source may legitimately use a non-time revision token such as an ETag).
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if isinstance(value, (int, float)):
        return (
            datetime.fromtimestamp(float(value), tz=timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
    return str(value)


def slugify(value: str) -> str:
    out = _SLUG_RE.sub("-", value.strip().lower()).strip("-")
    return out or "pipeline"


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


def loads(value: str | None, default: Any = None) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return default


def elapsed_ms(start: float, end: float) -> int:
    return int(round((end - start) * 1000))


def truncate(text: str, limit: int = 4000) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 15] + f"... (+{len(text) - limit + 15} chars)"
