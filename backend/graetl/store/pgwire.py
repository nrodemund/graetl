"""A dependency-free PostgreSQL client speaking the v3 frontend/backend protocol.

GraETL talks to PostgreSQL through ``psycopg`` (v3) when it is installed; that
is the supported, fast, fully featured path. This module is the fallback for
air-gapped installs where no wheels can be fetched: it implements just enough
of the wire protocol, on the standard library alone, to run GraETL's own SQL.
The public surface deliberately mirrors psycopg 3 (``connect``, ``Connection``,
``Cursor``, ``%s`` paramstyle, mapping-like rows) so calling code never has to
know which driver it got.

Deliberately NOT supported, because GraETL does not need it and each one would
cost far more than it is worth here:

* ``COPY`` in either direction (a ``CopyInResponse`` is failed and reported)
* ``LISTEN`` / ``NOTIFY`` delivery (NotificationResponse is read and dropped)
* server-side / named cursors, and ``FETCH``-based streaming
* the binary result format - everything goes over the wire as text
* prepared-statement caching (every extended-protocol execute re-Parses the
  unnamed statement)
* connection pooling, async, and cancel requests
* client-side array/composite/range adaptation
* full SASLprep for non-ASCII passwords (UTF-8 bytes are used as-is)

Threading: a ``Connection`` owns one socket guarded by a single ``RLock``. The
lock makes individual calls atomic, but a connection is still *not* safe to
share between threads - two threads interleaving ``execute``/``fetchall`` on
one connection will read each other's rows. GraETL gives every worker its own
connection; do the same.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
import socket
import ssl
import struct
import threading
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Iterable, Iterator, Mapping, NamedTuple, Sequence
from urllib.parse import parse_qsl, unquote, urlsplit

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "Row",
    "Column",
    "PgError",
    "OperationalError",
    "ProgrammingError",
    "IntegrityError",
    "SerializationError",
    "is_retryable",
]

PROTOCOL_VERSION = 196608  # 3.0, as major << 16 | minor
SSL_REQUEST_CODE = 80877103
DEFAULT_PORT = 5432
_RECV_CHUNK = 65536
_MAX_NOTICES = 100

#: sqlstates GraETL's runner retries rather than failing the module.
RETRYABLE_SQLSTATES = frozenset({"40001", "40P01", "55P03"})


# --------------------------------------------------------------------- errors


class PgError(Exception):
    """A backend ErrorResponse, or a driver-level failure with no sqlstate."""

    def __init__(
        self,
        message: str,
        *,
        sqlstate: str | None = None,
        severity: str | None = None,
        detail: str | None = None,
        hint: str | None = None,
        constraint: str | None = None,
        table: str | None = None,
        column: str | None = None,
        schema: str | None = None,
        position: str | None = None,
        where: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.sqlstate = sqlstate
        self.severity = severity
        self.detail = detail
        self.hint = hint
        self.constraint = constraint
        self.table = table
        self.column = column
        self.schema = schema
        self.position = position
        self.where = where

    def __str__(self) -> str:
        head = self.message
        if self.sqlstate:
            head = f"[{self.sqlstate}] {head}"
        if self.severity:
            head = f"{self.severity}: {head}"
        extras = [
            ("DETAIL", self.detail),
            ("HINT", self.hint),
            ("CONSTRAINT", self.constraint),
            ("TABLE", self.table),
        ]
        tail = "".join(f"\n{label}: {value}" for label, value in extras if value)
        return head + tail


class OperationalError(PgError):
    """Connection loss, IO failure, authentication refusal, server shutdown."""


class ProgrammingError(PgError):
    """Bad SQL or bad driver usage: syntax errors, undefined tables, bad params."""


class IntegrityError(PgError):
    """Constraint violation (sqlstate class 23)."""


class SerializationError(PgError):
    """Concurrency conflict the caller is expected to retry."""


def is_retryable(exc: BaseException) -> bool:
    """True for serialization_failure, deadlock_detected and lock_not_available."""
    return getattr(exc, "sqlstate", None) in RETRYABLE_SQLSTATES


def _error_class(sqlstate: str | None) -> type[PgError]:
    if not sqlstate:
        return PgError
    if sqlstate in RETRYABLE_SQLSTATES:
        return SerializationError
    cls = sqlstate[:2]
    if cls == "23":
        return IntegrityError
    # 08 connection exception, 53 insufficient resources, 57 operator intervention,
    # 58 external/system error, XX internal error - none of them are the caller's SQL.
    if cls in ("08", "53", "57", "58", "XX"):
        return OperationalError
    return ProgrammingError


#: ErrorResponse/NoticeResponse field codes we care about.
_FIELD_NAMES = {
    "S": "severity",
    "V": "severity_nonlocalized",
    "C": "sqlstate",
    "M": "message",
    "D": "detail",
    "H": "hint",
    "P": "position",
    "W": "where",
    "s": "schema",
    "t": "table",
    "c": "column",
    "n": "constraint",
}


def _build_error(fields: dict[str, str]) -> PgError:
    sqlstate = fields.get("sqlstate")
    cls = _error_class(sqlstate)
    return cls(
        fields.get("message", "unknown server error"),
        sqlstate=sqlstate,
        severity=fields.get("severity_nonlocalized") or fields.get("severity"),
        detail=fields.get("detail"),
        hint=fields.get("hint"),
        constraint=fields.get("constraint"),
        table=fields.get("table"),
        column=fields.get("column"),
        schema=fields.get("schema"),
        position=fields.get("position"),
        where=fields.get("where"),
    )


# ----------------------------------------------------------------------- rows


class Column(NamedTuple):
    """DB-API ``description`` entry; a plain 7-tuple with names attached."""

    name: str
    type_code: int
    display_size: None
    internal_size: int
    precision: int
    scale: int
    null_ok: None


class Row:
    """A result row that is both a sequence and a mapping.

    GraETL's storage layer was written against ``sqlite3.Row`` and uses both
    ``row["entity_id"]`` and ``row[0]``, and hands rows straight to ``dict()``.
    ``keys()`` plus ``__getitem__`` is what makes ``dict(row)`` work.
    """

    __slots__ = ("_keys", "_values", "_index")

    def __init__(self, keys: tuple[str, ...], values: tuple[Any, ...], index: Mapping[str, int]):
        self._keys = keys
        self._values = values
        self._index = index

    def keys(self) -> list[str]:
        return list(self._keys)

    def values(self) -> tuple[Any, ...]:
        return self._values

    def items(self) -> list[tuple[str, Any]]:
        return list(zip(self._keys, self._values))

    def get(self, key: str, default: Any = None) -> Any:
        position = self._index.get(key)
        return default if position is None else self._values[position]

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            try:
                return self._values[self._index[key]]
            except KeyError:
                raise KeyError(key) from None
        return self._values[key]  # int and slice both land here

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> Iterator[Any]:
        # Sequence-style, like sqlite3.Row and like a plain tuple; dict() still
        # gets the mapping behaviour because keys() takes precedence there.
        return iter(self._values)

    def __contains__(self, key: object) -> bool:
        return key in self._index

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Row):
            return self._keys == other._keys and self._values == other._values
        if isinstance(other, tuple):
            return self._values == other
        if isinstance(other, list):
            return list(self._values) == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._values)

    def __repr__(self) -> str:
        body = ", ".join(f"{k}={v!r}" for k, v in zip(self._keys, self._values))
        return f"Row({body})"


def _row_index(names: Sequence[str]) -> dict[str, int]:
    # First occurrence wins, matching psycopg: duplicate labels stay reachable
    # by position even though only one of them is reachable by name.
    index: dict[str, int] = {}
    for position, name in enumerate(names):
        index.setdefault(name, position)
    return index


# ------------------------------------------------------------- SQL placeholders

_IDENT_START = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_")
_IDENT_CHARS = _IDENT_START | set("0123456789$")


def _is_ident_char(ch: str) -> bool:
    return ch in _IDENT_CHARS or ch.isalpha()


def scan_placeholders(sql: str) -> tuple[str, list[Any]]:
    """Rewrite psycopg-style ``%s`` / ``%(name)s`` markers into ``$1..$n``.

    Returns the rewritten SQL and the ordered placeholder keys: ints for the
    positional form, names for the named form. This is a character scanner
    rather than a regex because a regex cannot tell a placeholder from a bare
    percent sign inside a string literal, a quoted identifier, a dollar-quoted
    body or a comment - all of which must survive untouched.
    """
    out: list[str] = []
    keys: list[Any] = []
    named_slots: dict[str, int] = {}
    total = len(sql)
    i = 0
    while i < total:
        ch = sql[i]

        if ch == "'":
            end = _scan_string(sql, i, escaped=_is_escape_string(sql, i))
            out.append(sql[i:end])
            i = end
            continue

        if ch == '"':
            end = _scan_quoted_identifier(sql, i)
            out.append(sql[i:end])
            i = end
            continue

        if ch == "$":
            end = _scan_dollar_quote(sql, i)
            if end is not None:
                out.append(sql[i:end])
                i = end
                continue
            out.append(ch)
            i += 1
            continue

        if ch == "-" and sql.startswith("--", i):
            end = sql.find("\n", i)
            end = total if end < 0 else end + 1
            out.append(sql[i:end])
            i = end
            continue

        if ch == "/" and sql.startswith("/*", i):
            end = _scan_block_comment(sql, i)
            out.append(sql[i:end])
            i = end
            continue

        if ch == "%":
            nxt = sql[i + 1] if i + 1 < total else ""
            if nxt == "%":
                out.append("%")  # %% is a literal percent, like psycopg
                i += 2
                continue
            if nxt == "s":
                if named_slots:
                    raise ProgrammingError("cannot mix %s and %(name)s placeholders in one query")
                keys.append(len(keys))
                out.append(f"${len(keys)}")
                i += 2
                continue
            if nxt == "(":
                close = sql.find(")", i + 2)
                if close < 0:
                    raise ProgrammingError("unterminated %(name)s placeholder")
                if not sql.startswith("s", close + 1):
                    raise ProgrammingError(
                        "named placeholder must end in 's', as in %(name)s"
                    )
                name = sql[i + 2 : close]
                if not name:
                    raise ProgrammingError("empty placeholder name in %()s")
                if keys and not named_slots:
                    raise ProgrammingError("cannot mix %s and %(name)s placeholders in one query")
                slot = named_slots.get(name)
                if slot is None:
                    keys.append(name)
                    slot = len(keys)
                    named_slots[name] = slot
                out.append(f"${slot}")
                i = close + 2
                continue
            # A lone percent is PostgreSQL's modulo operator; leave it alone.
            out.append(ch)
            i += 1
            continue

        out.append(ch)
        i += 1

    return "".join(out), keys


def _is_escape_string(sql: str, quote_at: int) -> bool:
    """True for ``E'...'``, where a backslash escapes the following character."""
    if quote_at == 0:
        return False
    prev = sql[quote_at - 1]
    if prev not in ("e", "E"):
        return False
    return quote_at - 1 == 0 or not _is_ident_char(sql[quote_at - 2])


def _scan_string(sql: str, start: int, *, escaped: bool) -> int:
    total = len(sql)
    i = start + 1
    while i < total:
        ch = sql[i]
        if escaped and ch == "\\":
            i += 2
            continue
        if ch == "'":
            if sql.startswith("''", i):
                i += 2
                continue
            return i + 1
        i += 1
    raise ProgrammingError("unterminated string literal in SQL")


def _scan_quoted_identifier(sql: str, start: int) -> int:
    total = len(sql)
    i = start + 1
    while i < total:
        if sql[i] == '"':
            if sql.startswith('""', i):
                i += 2
                continue
            return i + 1
        i += 1
    raise ProgrammingError("unterminated quoted identifier in SQL")


def _scan_dollar_quote(sql: str, start: int) -> int | None:
    """Return the index just past a ``$tag$...$tag$`` body, or None if not one."""
    total = len(sql)
    i = start + 1
    while i < total and (sql[i] == "_" or sql[i].isalnum()):
        i += 1
    if i >= total or sql[i] != "$":
        return None
    tag = sql[start : i + 1]
    # A tag follows identifier rules, so it cannot start with a digit - that is
    # what keeps a rewritten "$1" from being mistaken for an opening delimiter.
    if len(tag) > 2 and sql[start + 1].isdigit():
        return None
    end = sql.find(tag, i + 1)
    if end < 0:
        raise ProgrammingError(f"unterminated dollar-quoted string ({tag}) in SQL")
    return end + len(tag)


def _scan_block_comment(sql: str, start: int) -> int:
    total = len(sql)
    depth = 0
    i = start
    while i < total:
        if sql.startswith("/*", i):
            depth += 1
            i += 2
            continue
        if sql.startswith("*/", i):
            depth -= 1
            i += 2
            if depth == 0:
                return i
            continue
        i += 1
    raise ProgrammingError("unterminated block comment in SQL")


def _bind_params(keys: Sequence[Any], params: Any) -> list[str | None]:
    """Line the caller's params up with the placeholders found by the scanner."""
    if not keys:
        if params:
            raise ProgrammingError(
                "the query has no placeholders but parameters were passed"
            )
        return []

    if isinstance(keys[0], str):
        if not isinstance(params, Mapping):
            raise ProgrammingError(
                "the query uses %(name)s placeholders, so params must be a mapping"
            )
        values = []
        for position, name in enumerate(keys, start=1):
            if name not in params:
                raise ProgrammingError(f"no value supplied for placeholder %({name})s")
            values.append(_encode_param(params[name], position, name=str(name)))
        return values

    if isinstance(params, Mapping) or isinstance(params, (str, bytes)):
        raise ProgrammingError(
            "the query uses %s placeholders, so params must be a sequence"
        )
    try:
        seq = list(params)
    except TypeError:
        raise ProgrammingError("params must be a sequence or a mapping") from None
    if len(seq) != len(keys):
        raise ProgrammingError(
            f"the query has {len(keys)} placeholders but {len(seq)} parameters were passed"
        )
    return [_encode_param(value, i) for i, value in enumerate(seq, start=1)]


# ------------------------------------------------------------ type conversion

_FLOAT_LITERALS = {float("inf"): "Infinity", float("-inf"): "-Infinity"}


def _encode_param(value: Any, position: int, name: str | None = None) -> str | None:
    """Render one bind parameter as PostgreSQL text input, or None for NULL."""
    label = f"%({name})s" if name else f"${position}"
    if value is None:
        return None
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value:  # NaN
            return "NaN"
        return _FLOAT_LITERALS.get(value, repr(value))
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "\\x" + bytes(value).hex()
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        raise TypeError(
            f"parameter {position} ({label}) is a {type(value).__name__}; this driver does "
            "not JSON-encode parameters. Serialise it yourself and pass the str "
            "(GraETL uses graetl.utils.dumps)."
        )
    raise TypeError(
        f"parameter {position} ({label}) has unsupported type {type(value).__name__}"
    )


OID_BOOL = 16
OID_BYTEA = 17
OID_CHAR = 18
OID_NAME = 19
OID_INT8 = 20
OID_INT2 = 21
OID_INT4 = 23
OID_TEXT = 25
OID_JSON = 114
OID_FLOAT4 = 700
OID_FLOAT8 = 701
OID_VARCHAR = 1043
OID_DATE = 1082
OID_TIME = 1083
OID_TIMESTAMP = 1114
OID_TIMESTAMPTZ = 1184
OID_NUMERIC = 1700
OID_UUID = 2950
OID_JSONB = 3802

_INT_OIDS = frozenset({OID_INT2, OID_INT4, OID_INT8})
_FLOAT_OIDS = frozenset({OID_FLOAT4, OID_FLOAT8})
#: Timestamps stay text on purpose: GraETL stores ISO strings in TEXT columns
#: and compares watermarks as strings, so parsing them here would break that.
_TEXT_OIDS = frozenset(
    {
        OID_CHAR,
        OID_NAME,
        OID_TEXT,
        OID_JSON,
        OID_VARCHAR,
        OID_DATE,
        OID_TIME,
        OID_TIMESTAMP,
        OID_TIMESTAMPTZ,
        OID_UUID,
        OID_JSONB,
    }
)


def _decode_bytea(raw: bytes) -> bytes:
    text = raw.decode("ascii", "replace")
    if text.startswith("\\x"):
        return bytes.fromhex(text[2:])
    # Pre-9.0 "escape" output, still reachable via bytea_output = escape.
    out = bytearray()
    i = 0
    total = len(text)
    while i < total:
        if text[i] == "\\" and i + 1 < total:
            if text[i + 1] == "\\":
                out.append(0x5C)
                i += 2
                continue
            out.append(int(text[i + 1 : i + 4], 8))
            i += 4
            continue
        out.append(ord(text[i]))
        i += 1
    return bytes(out)


def _decode_value(oid: int, raw: bytes | None) -> Any:
    if raw is None:
        return None
    if oid == OID_BYTEA:
        return _decode_bytea(raw)
    text = raw.decode("utf-8", "replace")
    if oid == OID_BOOL:
        return text == "t"
    if oid in _INT_OIDS:
        return int(text)
    if oid in _FLOAT_OIDS:
        return float(text)
    if oid == OID_NUMERIC:
        return Decimal(text)
    if oid in _TEXT_OIDS:
        return text  # timestamps included, on purpose - see _TEXT_OIDS
    return text  # unknown OID: hand back the server's own text representation


# --------------------------------------------------------------- conninfo/DSN

_CONNINFO_ALIASES = {
    "dbname": "dbname",
    "database": "dbname",
    "user": "user",
    "username": "user",
    "password": "password",
    "host": "host",
    "hostaddr": "host",
    "port": "port",
    "sslmode": "sslmode",
    "sslrootcert": "sslrootcert",
    "application_name": "application_name",
    "connect_timeout": "connect_timeout",
    "options": "options",
}

VALID_SSLMODES = ("disable", "allow", "prefer", "require", "verify-ca", "verify-full")


def parse_conninfo(conninfo: str) -> dict[str, str]:
    """Parse either a ``postgresql://`` URL or a libpq ``key=value`` string."""
    text = conninfo.strip()
    if text.startswith(("postgresql://", "postgres://")):
        return _parse_url(text)
    return _parse_keywords(text)


def _parse_url(url: str) -> dict[str, str]:
    parts = urlsplit(url)
    out: dict[str, str] = {}
    if parts.username:
        out["user"] = unquote(parts.username)
    if parts.password:
        out["password"] = unquote(parts.password)
    if parts.hostname:
        out["host"] = unquote(parts.hostname)
    if parts.port:
        out["port"] = str(parts.port)
    dbname = parts.path.lstrip("/")
    if dbname:
        out["dbname"] = unquote(dbname)
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        mapped = _CONNINFO_ALIASES.get(key.lower())
        if mapped:
            out[mapped] = value
    return out


def _parse_keywords(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    i = 0
    total = len(text)
    while i < total:
        while i < total and text[i].isspace():
            i += 1
        if i >= total:
            break
        start = i
        while i < total and text[i] != "=" and not text[i].isspace():
            i += 1
        key = text[start:i]
        while i < total and text[i].isspace():
            i += 1
        if i >= total or text[i] != "=":
            raise ProgrammingError(f"malformed conninfo: no value for {key!r}")
        i += 1
        while i < total and text[i].isspace():
            i += 1
        value_chars: list[str] = []
        if i < total and text[i] == "'":
            i += 1
            while i < total and text[i] != "'":
                if text[i] == "\\" and i + 1 < total:
                    i += 1
                value_chars.append(text[i])
                i += 1
            if i >= total:
                raise ProgrammingError("malformed conninfo: unterminated quoted value")
            i += 1
        else:
            while i < total and not text[i].isspace():
                if text[i] == "\\" and i + 1 < total:
                    i += 1
                value_chars.append(text[i])
                i += 1
        mapped = _CONNINFO_ALIASES.get(key.lower())
        if mapped:
            out[mapped] = "".join(value_chars)
    return out


# -------------------------------------------------------------- socket + wire


class _Wire:
    """Buffered framing over a socket: send whole messages, read whole messages."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self._buf = bytearray()

    def send(self, data: bytes) -> None:
        try:
            self.sock.sendall(data)
        except OSError as exc:
            raise OperationalError(f"send failed: {exc}") from exc

    def recv_exact(self, count: int) -> bytes:
        while len(self._buf) < count:
            try:
                chunk = self.sock.recv(max(_RECV_CHUNK, count - len(self._buf)))
            except OSError as exc:
                raise OperationalError(f"receive failed: {exc}") from exc
            if not chunk:
                raise OperationalError("server closed the connection unexpectedly")
            self._buf.extend(chunk)
        out = bytes(self._buf[:count])
        del self._buf[:count]
        return out

    def read_message(self) -> tuple[str, bytes]:
        kind = self.recv_exact(1).decode("ascii", "replace")
        (length,) = struct.unpack(">i", self.recv_exact(4))
        if length < 4:
            raise OperationalError(f"malformed message length {length} for type {kind!r}")
        return kind, self.recv_exact(length - 4)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


def _msg(kind: str, payload: bytes) -> bytes:
    return kind.encode("ascii") + struct.pack(">i", len(payload) + 4) + payload


def _cstr(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def _read_cstr(payload: bytes, offset: int) -> tuple[str, int]:
    end = payload.index(b"\x00", offset)
    return payload[offset:end].decode("utf-8", "replace"), end + 1


# --------------------------------------------------------------------- SCRAM


def _normalize_password(password: str) -> bytes:
    """SASLprep is not implemented; ASCII passwords are already normalised."""
    return password.encode("utf-8")


def _hmac256(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, hashlib.sha256).digest()


def _xor(a: bytes, b: bytes) -> bytes:
    return bytes(x ^ y for x, y in zip(a, b))


class _ScramClient:
    """SCRAM-SHA-256 without channel binding (gs2 header ``n,,``)."""

    MECHANISM = "SCRAM-SHA-256"
    GS2_HEADER = "n,,"

    def __init__(self, user: str, password: str) -> None:
        self.password = _normalize_password(password)
        self.nonce = base64.b64encode(secrets.token_bytes(18)).decode("ascii")
        # PostgreSQL ignores the SCRAM username and authenticates as the startup
        # user, so the RFC-mandated "n=" field is sent empty, exactly like libpq.
        self.client_first_bare = f"n=,r={self.nonce}"
        self._server_signature = b""

    def first_message(self) -> bytes:
        return (self.GS2_HEADER + self.client_first_bare).encode("utf-8")

    def final_message(self, server_first: bytes) -> bytes:
        text = server_first.decode("utf-8")
        attrs = _scram_attrs(text)
        server_nonce = attrs.get("r", "")
        if not server_nonce.startswith(self.nonce):
            raise OperationalError("SCRAM: server nonce does not extend the client nonce")
        try:
            salt = base64.b64decode(attrs["s"])
            iterations = int(attrs["i"])
        except (KeyError, ValueError) as exc:
            raise OperationalError(f"SCRAM: malformed server-first-message: {text!r}") from exc

        salted = hashlib.pbkdf2_hmac("sha256", self.password, salt, iterations)
        client_key = _hmac256(salted, b"Client Key")
        stored_key = hashlib.sha256(client_key).digest()

        channel_binding = base64.b64encode(self.GS2_HEADER.encode("ascii")).decode("ascii")
        without_proof = f"c={channel_binding},r={server_nonce}"
        auth_message = f"{self.client_first_bare},{text},{without_proof}".encode("utf-8")

        client_signature = _hmac256(stored_key, auth_message)
        proof = base64.b64encode(_xor(client_key, client_signature)).decode("ascii")
        server_key = _hmac256(salted, b"Server Key")
        self._server_signature = _hmac256(server_key, auth_message)
        return f"{without_proof},p={proof}".encode("utf-8")

    def verify(self, server_final: bytes) -> None:
        attrs = _scram_attrs(server_final.decode("utf-8"))
        if "e" in attrs:
            raise OperationalError(f"SCRAM authentication failed: {attrs['e']}")
        try:
            signature = base64.b64decode(attrs["v"])
        except (KeyError, ValueError) as exc:
            raise OperationalError("SCRAM: missing server signature") from exc
        if not hmac.compare_digest(signature, self._server_signature):
            raise OperationalError("SCRAM: server signature mismatch, the server is not trusted")


def _scram_attrs(text: str) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for item in text.split(","):
        if len(item) > 1 and item[1] == "=":
            attrs[item[0]] = item[2:]
    return attrs


# -------------------------------------------------------------------- results


class _Result:
    """One statement's worth of output from the backend."""

    __slots__ = ("description", "rows", "rowcount", "command")

    def __init__(
        self,
        description: list[Column] | None,
        rows: list[Row],
        rowcount: int,
        command: str,
    ) -> None:
        self.description = description
        self.rows = rows
        self.rowcount = rowcount
        self.command = command


def _rowcount_from_tag(tag: str) -> int:
    parts = tag.split()
    if not parts:
        return -1
    if parts[0] == "INSERT" and len(parts) >= 3:
        try:
            return int(parts[2])
        except ValueError:
            return -1
    if parts[0] in ("UPDATE", "DELETE", "SELECT", "MOVE", "FETCH", "COPY", "MERGE"):
        try:
            return int(parts[-1])
        except ValueError:
            return -1
    return -1  # DDL and transaction control report no row count


class ConnectionInfo:
    """The psycopg-ish ``conn.info`` view over the startup ParameterStatus set."""

    def __init__(self, connection: Connection) -> None:
        self._conn = connection

    @property
    def parameters(self) -> dict[str, str]:
        return dict(self._conn._parameters)

    def parameter_status(self, name: str) -> str | None:
        return self._conn._parameters.get(name)

    @property
    def backend_pid(self) -> int:
        return self._conn._backend_pid

    @property
    def dbname(self) -> str:
        return self._conn._dbname

    @property
    def host(self) -> str:
        return self._conn._host

    @property
    def port(self) -> int:
        return self._conn._port

    @property
    def user(self) -> str:
        return self._conn._user

    @property
    def transaction_status(self) -> str:
        return self._conn._tx_status

    @property
    def server_version(self) -> int:
        """Numeric server version, e.g. 160013 for "16.13", as psycopg reports it."""
        raw = self._conn._parameters.get("server_version", "")
        return _parse_server_version(raw)


def _parse_server_version(raw: str) -> int:
    token = raw.strip().split()[0] if raw.strip() else ""
    numbers: list[int] = []
    for piece in token.split("."):
        digits = ""
        for ch in piece:
            if not ch.isdigit():
                break
            digits += ch
        if not digits:
            break
        numbers.append(int(digits))
    if not numbers:
        return 0
    major = numbers[0]
    minor = numbers[1] if len(numbers) > 1 else 0
    if major >= 10:  # one-part major since PostgreSQL 10
        return major * 10000 + minor
    patch = numbers[2] if len(numbers) > 2 else 0
    return major * 10000 + minor * 100 + patch


# ----------------------------------------------------------------- connection


class Connection:
    """A single session on one socket. See the module docstring on threading."""

    def __init__(
        self,
        wire: _Wire,
        *,
        host: str,
        port: int,
        user: str,
        dbname: str,
        autocommit: bool = False,
    ) -> None:
        self._wire: _Wire | None = wire
        self._lock = threading.RLock()
        self._host = host
        self._port = port
        self._user = user
        self._dbname = dbname
        self._parameters: dict[str, str] = {}
        self._backend_pid = 0
        self._backend_key = 0
        self._tx_status = "I"
        self._autocommit = autocommit
        self._closed = False
        self._broken = False
        self.notices: list[PgError] = []
        self.info = ConnectionInfo(self)

    # -------------------------------------------------------------- lifecycle

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        with self._lock:
            self._check_open()
            if bool(value) == self._autocommit:
                return
            if value and self._tx_status != "I":
                # Turning autocommit on mid-transaction would silently strand the
                # open transaction, so end it the way psycopg does: commit it.
                self._simple("COMMIT")
            self._autocommit = bool(value)

    @property
    def in_transaction(self) -> bool:
        """True when the backend's last ReadyForQuery said 'T' or 'E'."""
        return self._tx_status in ("T", "E")

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            wire, self._wire = self._wire, None
            if wire is None:
                return
            if not self._broken:
                try:
                    wire.send(_msg("X", b""))  # Terminate
                except PgError:
                    pass
            wire.close()

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        # psycopg 3 semantics: the block commits (or rolls back) and then closes.
        try:
            if not self._closed and not self._broken:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            self.close()

    # ------------------------------------------------------------- statements

    def cursor(self) -> Cursor:
        self._check_open()
        return Cursor(self)

    def execute(self, sql: str, params: Any = None) -> Cursor:
        """Convenience wrapper: make a cursor, run one statement, return it."""
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self) -> None:
        with self._lock:
            self._check_open()
            if self.in_transaction:
                self._simple("COMMIT")

    def rollback(self) -> None:
        with self._lock:
            self._check_open()
            if self.in_transaction:
                self._simple("ROLLBACK")

    # ----------------------------------------------------------------- internals

    def _check_open(self) -> None:
        if self._closed or self._wire is None:
            raise OperationalError("the connection is closed")
        if self._broken:
            raise OperationalError("the connection is broken and must be discarded")

    def _begin_if_needed(self) -> None:
        """Open a real transaction lazily so commit()/rollback() mean something."""
        if not self._autocommit and self._tx_status == "I":
            self._simple("BEGIN")

    def _simple(self, sql: str) -> list[_Result]:
        """Simple Query: one round trip, may carry several statements."""
        assert self._wire is not None
        self._wire.send(_msg("Q", _cstr(sql)))
        return self._read_until_ready()

    def _extended(self, sql: str, values: Sequence[str | None]) -> list[_Result]:
        """Parse/Bind/Describe/Execute/Sync on the unnamed statement and portal."""
        assert self._wire is not None
        # Parse: unnamed statement, no declared parameter types (0) so the server
        # infers each one from where it is used.
        parse = _cstr("") + _cstr(sql) + struct.pack(">h", 0)

        encoded: list[bytes | None] = [
            None if v is None else v.encode("utf-8") for v in values
        ]
        body = bytearray()
        body += _cstr("")  # portal
        body += _cstr("")  # statement
        body += struct.pack(">h", 0)  # all parameters in text format
        body += struct.pack(">h", len(encoded))
        for item in encoded:
            if item is None:
                body += struct.pack(">i", -1)
            else:
                body += struct.pack(">i", len(item)) + item
        body += struct.pack(">h", 0)  # all results in text format

        packet = (
            _msg("P", parse)
            + _msg("B", bytes(body))
            + _msg("D", b"P" + _cstr(""))  # describe the portal
            + _msg("E", _cstr("") + struct.pack(">i", 0))  # fetch every row
            + _msg("S", b"")
        )
        self._wire.send(packet)
        return self._read_until_ready()

    def _read_until_ready(self) -> list[_Result]:
        """Drain the backend through ReadyForQuery, then raise any error seen.

        Draining matters even on failure: a half-read response leaves the socket
        out of sync, and GraETL expects to keep using the connection after a
        rollback.
        """
        assert self._wire is not None
        wire = self._wire
        results: list[_Result] = []
        error: PgError | None = None
        names: tuple[str, ...] = ()
        oids: list[int] = []
        index: dict[str, int] = {}
        description: list[Column] | None = None
        rows: list[Row] = []

        while True:
            try:
                kind, payload = wire.read_message()
            except OperationalError:
                self._broken = True
                self._closed = True
                raise

            if kind == "D":  # DataRow
                (count,) = struct.unpack_from(">h", payload, 0)
                offset = 2
                values: list[Any] = []
                for column in range(count):
                    (length,) = struct.unpack_from(">i", payload, offset)
                    offset += 4
                    if length < 0:
                        values.append(None)
                        continue
                    raw = payload[offset : offset + length]
                    offset += length
                    oid = oids[column] if column < len(oids) else OID_TEXT
                    values.append(_decode_value(oid, raw))
                rows.append(Row(names, tuple(values), index))

            elif kind == "T":  # RowDescription
                description, names, oids = _parse_row_description(payload)
                index = _row_index(names)
                rows = []

            elif kind == "C":  # CommandComplete
                tag, _ = _read_cstr(payload, 0)
                results.append(_Result(description, rows, _rowcount_from_tag(tag), tag))
                description, names, oids, index, rows = None, (), [], {}, []

            elif kind == "I":  # EmptyQueryResponse
                results.append(_Result(None, [], -1, ""))
                description, names, oids, index, rows = None, (), [], {}, []

            elif kind == "Z":  # ReadyForQuery
                self._tx_status = payload[:1].decode("ascii", "replace") or "I"
                break

            elif kind == "E":  # ErrorResponse
                error = _build_error(_parse_fields(payload))

            elif kind == "N":  # NoticeResponse
                notice = _build_error(_parse_fields(payload))
                self.notices.append(notice)
                del self.notices[:-_MAX_NOTICES]

            elif kind == "S":  # ParameterStatus
                key, offset = _read_cstr(payload, 0)
                value, _ = _read_cstr(payload, offset)
                self._parameters[key] = value

            elif kind == "K":  # BackendKeyData
                self._backend_pid, self._backend_key = struct.unpack(">ii", payload[:8])

            elif kind == "s":  # PortalSuspended - only with a row limit, which we never set
                results.append(_Result(description, rows, len(rows), "SUSPENDED"))
                description, names, oids, index, rows = None, (), [], {}, []

            elif kind in ("1", "2", "3", "n", "t", "A"):
                # ParseComplete, BindComplete, CloseComplete, NoData,
                # ParameterDescription, NotificationResponse: nothing to do.
                pass

            elif kind in ("G", "H", "W"):  # Copy{In,Out,Both}Response
                if kind == "G":
                    wire.send(_msg("f", _cstr("COPY is not supported by graetl.pgwire")))
                error = error or ProgrammingError(
                    "COPY is not supported by this driver; install psycopg for COPY support"
                )

            else:
                raise OperationalError(f"unexpected backend message {kind!r}")

        if error is not None:
            raise error
        return results


def _parse_row_description(payload: bytes) -> tuple[list[Column], tuple[str, ...], list[int]]:
    (count,) = struct.unpack_from(">h", payload, 0)
    offset = 2
    columns: list[Column] = []
    names: list[str] = []
    oids: list[int] = []
    for _ in range(count):
        name, offset = _read_cstr(payload, offset)
        _table_oid, _attnum, type_oid, type_size, type_mod, _fmt = struct.unpack_from(
            ">ihihih", payload, offset
        )
        offset += 18
        names.append(name)
        oids.append(type_oid)
        columns.append(Column(name, type_oid, None, type_size, type_mod, type_mod, None))
    return columns, tuple(names), oids


def _parse_fields(payload: bytes) -> dict[str, str]:
    fields: dict[str, str] = {}
    offset = 0
    while offset < len(payload) and payload[offset] != 0:
        code = chr(payload[offset])
        value, offset = _read_cstr(payload, offset + 1)
        key = _FIELD_NAMES.get(code)
        if key:
            fields[key] = value
    return fields


# --------------------------------------------------------------------- cursor


class Cursor:
    """DB-API style cursor. Results are fetched eagerly into memory."""

    #: Default row count for fetchmany(), as DB-API asks for.
    arraysize = 1

    def __init__(self, connection: Connection) -> None:
        self._conn = connection
        self._results: list[_Result] = []
        self._position = 0
        self._rows: list[Row] = []
        self._offset = 0
        self._closed = False
        self.description: list[Column] | None = None
        self.rowcount: int = -1

    # -------------------------------------------------------------- lifecycle

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def connection(self) -> Connection:
        return self._conn

    def close(self) -> None:
        self._closed = True
        self._results = []
        self._rows = []

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    # -------------------------------------------------------------- execution

    def execute(self, sql: str, params: Any = None) -> Cursor:
        """Run one statement. With ``params is None`` the simple protocol is used,
        so a multi-statement DDL script works exactly like ``executescript``."""
        self._check_open()
        with self._conn._lock:
            self._conn._check_open()
            self._conn._begin_if_needed()
            if params is None:
                results = self._conn._simple(sql)
            else:
                converted, keys = scan_placeholders(sql)
                values = _bind_params(keys, params)
                results = self._conn._extended(converted, values)
        self._load(results)
        return self

    def executemany(self, sql: str, seq_of_params: Iterable[Any]) -> Cursor:
        """Run the same statement once per parameter set; ``rowcount`` is the total."""
        self._check_open()
        batches = list(seq_of_params)
        converted, keys = scan_placeholders(sql)
        total = 0
        last: list[_Result] = []
        with self._conn._lock:
            self._conn._check_open()
            self._conn._begin_if_needed()
            for params in batches:
                values = _bind_params(keys, params)
                last = self._conn._extended(converted, values)
                if last and last[0].rowcount > 0:
                    total += last[0].rowcount
        self._load(last)
        # Rows from the last batch stay fetchable, but the count spans them all.
        self.rowcount = total if batches else -1
        return self

    def executescript(self, sql: str) -> Cursor:
        """Run a multi-statement script over the simple protocol (DDL, migrations)."""
        self._check_open()
        with self._conn._lock:
            self._conn._check_open()
            self._conn._begin_if_needed()
            results = self._conn._simple(sql)
        self._load(results)
        return self

    def nextset(self) -> bool:
        """Advance to the next result set of a multi-statement script."""
        if self._position + 1 >= len(self._results):
            return False
        self._position += 1
        self._apply(self._results[self._position])
        return True

    def _load(self, results: list[_Result]) -> None:
        self._results = results
        self._position = 0
        if results:
            self._apply(results[0])
        else:
            self.description = None
            self.rowcount = -1
            self._rows = []
            self._offset = 0

    def _apply(self, result: _Result) -> None:
        self.description = result.description
        self.rowcount = result.rowcount
        self._rows = result.rows
        self._offset = 0

    def _check_open(self) -> None:
        if self._closed:
            raise ProgrammingError("the cursor is closed")

    # ---------------------------------------------------------------- fetching

    def fetchone(self) -> Row | None:
        self._check_open()
        if self._offset >= len(self._rows):
            return None
        row = self._rows[self._offset]
        self._offset += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[Row]:
        self._check_open()
        count = self.arraysize if size is None else size
        chunk = self._rows[self._offset : self._offset + max(count, 0)]
        self._offset += len(chunk)
        return chunk

    def fetchall(self) -> list[Row]:
        self._check_open()
        chunk = self._rows[self._offset :]
        self._offset = len(self._rows)
        return chunk

    def __iter__(self) -> Iterator[Row]:
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row

    def __repr__(self) -> str:
        return f"<pgwire.Cursor rowcount={self.rowcount} closed={self._closed}>"


# ------------------------------------------------------------------- connect()


def connect(
    conninfo: str | None = None,
    *,
    host: str | None = None,
    port: int | str | None = None,
    user: str | None = None,
    password: str | None = None,
    dbname: str | None = None,
    sslmode: str | None = None,
    connect_timeout: float | None = None,
    application_name: str = "graetl",
    autocommit: bool = False,
    sslrootcert: str | None = None,
) -> Connection:
    """Open a connection.

    ``conninfo`` may be a ``postgresql://`` URL or a libpq ``key=value`` string;
    explicit keyword arguments win over anything it carries, and ``PG*``
    environment variables fill the remaining gaps.
    """
    settings: dict[str, str] = {}
    if conninfo:
        settings.update(parse_conninfo(conninfo))

    overrides = {
        "host": host,
        "port": None if port is None else str(port),
        "user": user,
        "password": password,
        "dbname": dbname,
        "sslmode": sslmode,
        "sslrootcert": sslrootcert,
        "connect_timeout": None if connect_timeout is None else str(connect_timeout),
    }
    settings.update({k: v for k, v in overrides.items() if v is not None})

    env = os.environ
    final_host = settings.get("host") or env.get("PGHOST") or "localhost"
    final_port = int(settings.get("port") or env.get("PGPORT") or DEFAULT_PORT)
    final_user = settings.get("user") or env.get("PGUSER") or env.get("USER") or "postgres"
    final_password = settings.get("password") or env.get("PGPASSWORD") or ""
    final_dbname = settings.get("dbname") or env.get("PGDATABASE") or final_user
    final_sslmode = (settings.get("sslmode") or env.get("PGSSLMODE") or "prefer").lower()
    final_rootcert = settings.get("sslrootcert") or env.get("PGSSLROOTCERT")
    timeout_text = settings.get("connect_timeout")
    final_timeout = float(timeout_text) if timeout_text else None

    if final_sslmode not in VALID_SSLMODES:
        raise ProgrammingError(
            f"invalid sslmode {final_sslmode!r}; expected one of {', '.join(VALID_SSLMODES)}"
        )

    sock = _open_socket(final_host, final_port, final_timeout)
    try:
        sock = _maybe_wrap_tls(sock, final_host, final_sslmode, final_rootcert)
        wire = _Wire(sock)
        conn = Connection(
            wire,
            host=final_host,
            port=final_port,
            user=final_user,
            dbname=final_dbname,
            autocommit=autocommit,
        )
        _startup(conn, wire, final_user, final_dbname, final_password, application_name)
    except BaseException:
        try:
            sock.close()
        except OSError:
            pass
        raise
    # connect_timeout covers the handshake only; a long query must not time out.
    sock.settimeout(None)
    return conn


def _open_socket(host: str, port: int, timeout: float | None) -> socket.socket:
    last: Exception | None = None
    try:
        candidates = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    except OSError as exc:
        raise OperationalError(f"could not resolve host {host!r}: {exc}") from exc
    for family, socktype, proto, _canon, addr in candidates:
        sock = socket.socket(family, socktype, proto)
        try:
            sock.settimeout(timeout)
            sock.connect(addr)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return sock
        except OSError as exc:
            last = exc
            sock.close()
    raise OperationalError(f"could not connect to {host}:{port}: {last}")


def _ssl_context(sslmode: str, sslrootcert: str | None) -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    if sslmode in ("verify-ca", "verify-full"):
        if sslrootcert:
            ctx.load_verify_locations(cafile=sslrootcert)
        ctx.check_hostname = sslmode == "verify-full"
        ctx.verify_mode = ssl.CERT_REQUIRED
        return ctx
    # A warehouse behind the firewall usually has a self-signed certificate, and
    # refusing to connect would be worse than encrypting without verification.
    # Ask for verify-ca/verify-full when the chain actually matters.
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _maybe_wrap_tls(
    sock: socket.socket, host: str, sslmode: str, sslrootcert: str | None
) -> socket.socket:
    if sslmode == "disable":
        return sock
    try:
        sock.sendall(struct.pack(">ii", 8, SSL_REQUEST_CODE))
        reply = sock.recv(1)
    except OSError as exc:
        raise OperationalError(f"SSL negotiation failed: {exc}") from exc
    if reply == b"S":
        ctx = _ssl_context(sslmode, sslrootcert)
        try:
            return ctx.wrap_socket(sock, server_hostname=host if ctx.check_hostname else None)
        except ssl.SSLError as exc:
            raise OperationalError(f"TLS handshake failed: {exc}") from exc
    if reply == b"N":
        if sslmode in ("require", "verify-ca", "verify-full"):
            raise OperationalError(
                f"sslmode={sslmode} but the server refused a TLS connection"
            )
        return sock
    if reply == b"E":
        raise OperationalError("the server rejected the SSL request")
    raise OperationalError(f"unexpected reply {reply!r} to the SSL request")


def _startup(
    conn: Connection,
    wire: _Wire,
    user: str,
    dbname: str,
    password: str,
    application_name: str,
) -> None:
    params = {
        "user": user,
        "database": dbname,
        "application_name": application_name,
        "client_encoding": "UTF8",
        "DateStyle": "ISO",
    }
    body = bytearray(struct.pack(">i", PROTOCOL_VERSION))
    for key, value in params.items():
        body += _cstr(key) + _cstr(value)
    body += b"\x00"
    wire.send(struct.pack(">i", len(body) + 4) + bytes(body))

    scram: _ScramClient | None = None
    while True:
        kind, payload = wire.read_message()

        if kind == "R":  # Authentication*
            (code,) = struct.unpack_from(">i", payload, 0)
            data = payload[4:]
            if code == 0:  # AuthenticationOk
                continue
            if code == 3:  # CleartextPassword
                wire.send(_msg("p", _cstr(password)))
            elif code == 5:  # MD5Password
                wire.send(_msg("p", _cstr(_md5_password(user, password, data[:4]))))
            elif code == 10:  # SASL
                mechanisms = _cstr_list(data)
                if _ScramClient.MECHANISM not in mechanisms:
                    raise OperationalError(
                        "server offers only SASL mechanisms this driver cannot do: "
                        + ", ".join(mechanisms)
                    )
                scram = _ScramClient(user, password)
                first = scram.first_message()
                wire.send(
                    _msg(
                        "p",
                        _cstr(_ScramClient.MECHANISM) + struct.pack(">i", len(first)) + first,
                    )
                )
            elif code == 11:  # SASLContinue
                if scram is None:
                    raise OperationalError("unexpected SASLContinue before SASLInitialResponse")
                wire.send(_msg("p", scram.final_message(data)))
            elif code == 12:  # SASLFinal
                if scram is None:
                    raise OperationalError("unexpected SASLFinal before SASLInitialResponse")
                scram.verify(data)
            else:
                raise OperationalError(
                    f"authentication method {code} is not supported by this driver "
                    "(supported: trust, password, md5, scram-sha-256); install psycopg "
                    "for GSSAPI/SSPI"
                )
            continue

        if kind == "S":  # ParameterStatus
            key, offset = _read_cstr(payload, 0)
            value, _ = _read_cstr(payload, offset)
            conn._parameters[key] = value
        elif kind == "K":  # BackendKeyData
            conn._backend_pid, conn._backend_key = struct.unpack(">ii", payload[:8])
        elif kind == "Z":  # ReadyForQuery - the handshake is done
            conn._tx_status = payload[:1].decode("ascii", "replace") or "I"
            return
        elif kind == "E":
            raise _build_error(_parse_fields(payload))
        elif kind == "N":
            conn.notices.append(_build_error(_parse_fields(payload)))
        elif kind == "v":  # NegotiateProtocolVersion
            raise OperationalError("the server does not speak protocol 3.0")
        else:
            raise OperationalError(f"unexpected message {kind!r} during startup")


def _md5_password(user: str, password: str, salt: bytes) -> str:
    inner = hashlib.md5((password + user).encode("utf-8")).hexdigest()
    outer = hashlib.md5(inner.encode("ascii") + salt).hexdigest()
    return "md5" + outer


def _cstr_list(payload: bytes) -> list[str]:
    items: list[str] = []
    offset = 0
    while offset < len(payload) and payload[offset] != 0:
        value, offset = _read_cstr(payload, offset)
        items.append(value)
    return items
