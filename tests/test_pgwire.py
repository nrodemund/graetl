"""Tests for the stdlib PostgreSQL driver in ``graetl.store.pgwire``.

Runnable with either ``pytest`` or ``python -m unittest``, like the other test
modules. The pure-Python parts (the ``%s`` scanner, conninfo parsing, ``Row``)
run anywhere; everything else needs a live PostgreSQL and is skipped when none
is reachable at ``PGWIRE_TEST_HOST``/``PGWIRE_TEST_PORT`` (default
127.0.0.1:5433, user/password ``graetl``).

The authentication tests rewrite ``pg_hba.conf`` and reload the server, then
put it back. They skip themselves unless the test process can do both.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import unittest
from datetime import date, datetime
from datetime import time as time_of_day
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from graetl.store import pgwire  # noqa: E402
from graetl.store.pgwire import (  # noqa: E402
    IntegrityError,
    OperationalError,
    ProgrammingError,
    Row,
    SerializationError,
    is_retryable,
    parse_conninfo,
    scan_placeholders,
)

HOST = os.environ.get("PGWIRE_TEST_HOST", "127.0.0.1")
PORT = int(os.environ.get("PGWIRE_TEST_PORT", "5433"))
USER = os.environ.get("PGWIRE_TEST_USER", "graetl")
PASSWORD = os.environ.get("PGWIRE_TEST_PASSWORD", "graetl")
DBNAME = os.environ.get("PGWIRE_TEST_DB", "warehouse")

PGDATA = Path(os.environ.get("PGWIRE_TEST_PGDATA", "/var/tmp/pgdata"))
PGBIN = os.environ.get("PGWIRE_TEST_PGBIN", "/usr/lib/postgresql/16/bin")
PGOWNER = os.environ.get("PGWIRE_TEST_PGOWNER", "ubuntu")


def connect(**overrides):
    kwargs = dict(
        host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=DBNAME, connect_timeout=5
    )
    kwargs.update(overrides)
    return pgwire.connect(**kwargs)


def require_server() -> None:
    """Skip the calling test when no PostgreSQL answers on the test port."""
    try:
        conn = connect()
    except OperationalError as exc:
        raise unittest.SkipTest(f"no PostgreSQL at {HOST}:{PORT} ({exc})") from None
    conn.close()


class ServerTestCase(unittest.TestCase):
    """Base class for everything that needs a live server."""

    @classmethod
    def setUpClass(cls) -> None:
        require_server()


# --------------------------------------------------------------- pure Python


class TestPlaceholderScanner(unittest.TestCase):
    """The %s -> $n rewrite. No server involved."""

    def test_positional_placeholders_are_numbered_in_order(self) -> None:
        sql, keys = scan_placeholders("INSERT INTO t (a, b) VALUES (%s, %s)")
        self.assertEqual(sql, "INSERT INTO t (a, b) VALUES ($1, $2)")
        self.assertEqual(keys, [0, 1])

    def test_a_query_without_placeholders_is_returned_unchanged(self) -> None:
        sql, keys = scan_placeholders("SELECT 1")
        self.assertEqual(sql, "SELECT 1")
        self.assertEqual(keys, [])

    def test_double_percent_is_a_literal_percent(self) -> None:
        sql, keys = scan_placeholders("SELECT 50 %% 7, %s")
        self.assertEqual(sql, "SELECT 50 % 7, $1")
        self.assertEqual(keys, [0])

    def test_a_lone_percent_is_the_modulo_operator(self) -> None:
        sql, keys = scan_placeholders("SELECT a % b FROM t WHERE c = %s")
        self.assertEqual(sql, "SELECT a % b FROM t WHERE c = $1")
        self.assertEqual(keys, [0])

    def test_percent_s_inside_a_single_quoted_literal_is_untouched(self) -> None:
        sql, keys = scan_placeholders("SELECT '100%s done', %s")
        self.assertEqual(sql, "SELECT '100%s done', $1")
        self.assertEqual(keys, [0])

    def test_a_doubled_quote_does_not_end_the_literal(self) -> None:
        sql, keys = scan_placeholders("SELECT 'it''s %s ok' , %s")
        self.assertEqual(sql, "SELECT 'it''s %s ok' , $1")
        self.assertEqual(keys, [0])

    def test_a_backslash_escape_only_applies_inside_an_e_string(self) -> None:
        sql, _ = scan_placeholders(r"SELECT E'a\' %s still literal', %s")
        self.assertEqual(sql, r"SELECT E'a\' %s still literal', $1")
        # The same text without the E prefix ends at the escaped quote, so the
        # %s that follows is outside the literal and must be rewritten.
        sql2, keys2 = scan_placeholders(r"SELECT 'a\', %s")
        self.assertEqual(sql2, r"SELECT 'a\', $1")
        self.assertEqual(keys2, [0])

    def test_percent_s_inside_a_quoted_identifier_is_untouched(self) -> None:
        sql, keys = scan_placeholders('SELECT "odd %s column" FROM t WHERE x = %s')
        self.assertEqual(sql, 'SELECT "odd %s column" FROM t WHERE x = $1')
        self.assertEqual(keys, [0])

    def test_percent_s_inside_a_dollar_quoted_body_is_untouched(self) -> None:
        sql, keys = scan_placeholders(
            "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; -- %s $$ LANGUAGE sql; SELECT %s"
        )
        self.assertEqual(
            sql,
            "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; -- %s $$ LANGUAGE sql; SELECT $1",
        )
        self.assertEqual(keys, [0])

    def test_a_tagged_dollar_quote_is_untouched(self) -> None:
        sql, keys = scan_placeholders("SELECT $body$ %s and $$ inside $body$, %s")
        self.assertEqual(sql, "SELECT $body$ %s and $$ inside $body$, $1")
        self.assertEqual(keys, [0])

    def test_a_native_dollar_parameter_is_not_a_dollar_quote(self) -> None:
        # "$1$" cannot open a dollar quote (tags never start with a digit), so
        # the rest of the statement must still be scanned normally.
        sql, keys = scan_placeholders("SELECT $1$ , %s")
        self.assertEqual(sql, "SELECT $1$ , $1")
        self.assertEqual(keys, [0])

    def test_percent_s_inside_a_line_comment_is_untouched(self) -> None:
        sql, keys = scan_placeholders("SELECT 1 -- keep %s here\n, %s")
        self.assertEqual(sql, "SELECT 1 -- keep %s here\n, $1")
        self.assertEqual(keys, [0])

    def test_percent_s_inside_a_block_comment_is_untouched(self) -> None:
        sql, keys = scan_placeholders("SELECT /* %s /* nested %s */ still */ 1, %s")
        self.assertEqual(sql, "SELECT /* %s /* nested %s */ still */ 1, $1")
        self.assertEqual(keys, [0])

    def test_named_placeholders_map_to_numbered_slots(self) -> None:
        sql, keys = scan_placeholders(
            "UPDATE t SET a = %(val)s WHERE id = %(id)s AND b <> %(val)s"
        )
        self.assertEqual(sql, "UPDATE t SET a = $1 WHERE id = $2 AND b <> $1")
        self.assertEqual(keys, ["val", "id"])

    def test_mixing_placeholder_styles_is_rejected(self) -> None:
        with self.assertRaises(ProgrammingError):
            scan_placeholders("SELECT %s, %(a)s")
        with self.assertRaises(ProgrammingError):
            scan_placeholders("SELECT %(a)s, %s")

    def test_unterminated_constructs_are_rejected(self) -> None:
        for sql in ("SELECT 'oops", 'SELECT "oops', "SELECT $t$oops", "SELECT /* oops"):
            with self.subTest(sql=sql), self.assertRaises(ProgrammingError):
                scan_placeholders(sql)


class TestConninfoParsing(unittest.TestCase):
    def test_a_libpq_url_is_parsed(self) -> None:
        parsed = parse_conninfo("postgresql://alice:s3cr3t@db.internal:6543/warehouse")
        self.assertEqual(
            parsed,
            {
                "user": "alice",
                "password": "s3cr3t",
                "host": "db.internal",
                "port": "6543",
                "dbname": "warehouse",
            },
        )

    def test_url_query_parameters_become_settings(self) -> None:
        parsed = parse_conninfo(
            "postgres://h/db?sslmode=require&application_name=etl&connect_timeout=3"
        )
        self.assertEqual(parsed["sslmode"], "require")
        self.assertEqual(parsed["application_name"], "etl")
        self.assertEqual(parsed["connect_timeout"], "3")

    def test_url_components_are_percent_decoded(self) -> None:
        parsed = parse_conninfo("postgresql://a%40b:p%40ss%20word@h/d")
        self.assertEqual(parsed["user"], "a@b")
        self.assertEqual(parsed["password"], "p@ss word")

    def test_key_value_conninfo_is_parsed(self) -> None:
        parsed = parse_conninfo("host=127.0.0.1 port=5433 user=graetl dbname=warehouse")
        self.assertEqual(parsed["host"], "127.0.0.1")
        self.assertEqual(parsed["port"], "5433")
        self.assertEqual(parsed["user"], "graetl")
        self.assertEqual(parsed["dbname"], "warehouse")

    def test_key_value_quoting_and_aliases(self) -> None:
        parsed = parse_conninfo("host=h password='pa ss word' database=wh")
        self.assertEqual(parsed["password"], "pa ss word")
        self.assertEqual(parsed["dbname"], "wh")

    def test_a_value_less_key_is_rejected(self) -> None:
        with self.assertRaises(ProgrammingError):
            parse_conninfo("host")


class TestRowType(unittest.TestCase):
    def setUp(self) -> None:
        self.row = Row(("id", "name"), (7, "alpha"), {"id": 0, "name": 1})

    def test_a_row_behaves_as_a_sequence_and_a_mapping(self) -> None:
        self.assertEqual(self.row[0], 7)
        self.assertEqual(self.row["name"], "alpha")
        self.assertEqual(dict(self.row), {"id": 7, "name": "alpha"})
        self.assertEqual(self.row.keys(), ["id", "name"])
        self.assertEqual(len(self.row), 2)
        self.assertEqual(list(self.row), [7, "alpha"])
        self.assertEqual(self.row, (7, "alpha"))
        self.assertIn("name", self.row)

    def test_an_unknown_column_name_raises_key_error(self) -> None:
        with self.assertRaises(KeyError):
            self.row["nope"]
        self.assertIsNone(self.row.get("nope"))


# ------------------------------------------------------------------- server


class TestConnection(ServerTestCase):
    def test_a_plain_connection_reports_the_server_version(self) -> None:
        with connect() as conn:
            self.assertGreaterEqual(conn.info.server_version, 90000)
            self.assertGreater(conn.info.backend_pid, 0)
            self.assertEqual(conn.info.dbname, DBNAME)
            self.assertFalse(conn.closed)
        self.assertTrue(conn.closed)

    def test_the_application_name_reaches_the_server(self) -> None:
        conn = connect(application_name="pgwire-selftest")
        try:
            row = conn.execute("SHOW application_name").fetchone()
            self.assertEqual(row[0], "pgwire-selftest")
        finally:
            conn.close()

    def test_connecting_by_url_works(self) -> None:
        dsn = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=prefer"
        conn = pgwire.connect(dsn)
        try:
            self.assertEqual(conn.execute("SELECT current_database()").fetchone()[0], DBNAME)
        finally:
            conn.close()

    def test_connecting_by_key_value_conninfo_works(self) -> None:
        dsn = f"host={HOST} port={PORT} user={USER} password={PASSWORD} dbname={DBNAME}"
        conn = pgwire.connect(dsn)
        try:
            self.assertEqual(conn.execute("SELECT current_user").fetchone()[0], USER)
        finally:
            conn.close()

    def test_keyword_arguments_override_the_dsn(self) -> None:
        dsn = f"postgresql://nobody@{HOST}:{PORT}/does_not_exist"
        conn = pgwire.connect(dsn, user=USER, password=PASSWORD, dbname=DBNAME)
        try:
            self.assertEqual(conn.execute("SELECT current_database()").fetchone()[0], DBNAME)
        finally:
            conn.close()

    def test_sslmode_disable_skips_negotiation(self) -> None:
        conn = connect(sslmode="disable")
        try:
            self.assertEqual(conn.execute("SELECT 1").fetchone()[0], 1)
        finally:
            conn.close()

    def test_sslmode_require_fails_against_a_server_without_tls(self) -> None:
        with connect() as probe:
            has_ssl = probe.execute("SHOW ssl").fetchone()[0] == "on"
        if has_ssl:
            self.skipTest("the test server has TLS enabled")
        with self.assertRaises(OperationalError):
            connect(sslmode="require")

    def test_an_invalid_sslmode_is_rejected_before_any_socket(self) -> None:
        with self.assertRaises(ProgrammingError):
            connect(sslmode="sort-of")

    def test_a_bad_database_raises_an_error_with_a_sqlstate(self) -> None:
        with self.assertRaises(pgwire.PgError) as caught:
            connect(dbname="no_such_database_here")
        self.assertEqual(caught.exception.sqlstate, "3D000")

    def test_using_a_closed_connection_raises(self) -> None:
        conn = connect()
        conn.close()
        self.assertTrue(conn.closed)
        conn.close()  # idempotent
        with self.assertRaises(OperationalError):
            conn.execute("SELECT 1")


class TestAuthentication(unittest.TestCase):
    """Flip pg_hba.conf between md5 and scram-sha-256, then put trust back."""

    @classmethod
    def setUpClass(cls) -> None:
        require_server()
        cls.hba = PGDATA / "pg_hba.conf"
        if not cls.hba.is_file() or not os.access(cls.hba, os.W_OK):
            raise unittest.SkipTest(f"{cls.hba} is not writable by this test process")
        if _pg_ctl("reload").returncode != 0:
            raise unittest.SkipTest("cannot reload the server as the data directory owner")
        cls.original_hba = cls.hba.read_text()
        cls.addClassCleanup(cls._restore)

    @classmethod
    def _restore(cls) -> None:
        cls.hba.write_text(cls.original_hba)
        _pg_ctl("reload")
        _wait_for_auth(lambda: connect(sslmode="disable")).close()

    def _use_auth_method(self, method: str) -> None:
        lines = [
            "local   all             all                                     trust",
            f"host    all             all             127.0.0.1/32            {method}",
            f"host    all             all             ::1/128                 {method}",
        ]
        self.hba.write_text("\n".join(lines) + "\n")
        self.assertEqual(_pg_ctl("reload").returncode, 0)

    def _store_password_as(self, algorithm: str) -> None:
        """Re-hash the test role's password with the given algorithm."""
        with connect(sslmode="disable") as conn:
            conn.autocommit = True
            conn.execute(f"SET password_encryption = '{algorithm}'")
            conn.execute(f"ALTER ROLE {USER} PASSWORD '{PASSWORD}'")

    def test_scram_sha_256_authentication(self) -> None:
        self._store_password_as("scram-sha-256")
        self.addCleanup(self._restore)
        self._use_auth_method("scram-sha-256")
        conn = _wait_for_auth(lambda: connect(sslmode="disable"))
        try:
            self.assertEqual(conn.execute("SELECT current_user").fetchone()[0], USER)
        finally:
            conn.close()
        with self.assertRaises(pgwire.PgError):
            connect(password="wrong-password", sslmode="disable")

    def test_md5_authentication(self) -> None:
        self.addCleanup(self._restore)
        self.addCleanup(self._store_password_as, "scram-sha-256")
        self._store_password_as("md5")
        self._use_auth_method("md5")
        conn = _wait_for_auth(lambda: connect(sslmode="disable"))
        try:
            self.assertEqual(conn.execute("SELECT current_user").fetchone()[0], USER)
        finally:
            conn.close()
        with self.assertRaises(pgwire.PgError):
            connect(password="wrong-password", sslmode="disable")

    def test_cleartext_password_authentication(self) -> None:
        self.addCleanup(self._restore)
        self._use_auth_method("password")
        conn = _wait_for_auth(lambda: connect(sslmode="disable"))
        try:
            self.assertEqual(conn.execute("SELECT current_user").fetchone()[0], USER)
        finally:
            conn.close()


def _pg_ctl(action: str) -> subprocess.CompletedProcess:
    command = f"PATH={PGBIN}:$PATH pg_ctl -D {PGDATA} {action} -s"
    return subprocess.run(
        ["su", PGOWNER, "-c", command], capture_output=True, text=True, timeout=30
    )


def _wait_for_auth(factory, attempts: int = 30):
    """A reload is asynchronous, so retry briefly before giving up."""
    last: Exception | None = None
    for _ in range(attempts):
        try:
            return factory()
        except pgwire.PgError as exc:
            last = exc
            time.sleep(0.1)
    raise AssertionError(f"could not connect after the reload: {last}")


class TestQueries(ServerTestCase):
    TABLE = "pgwire_types"

    def setUp(self) -> None:
        self.conn = connect(autocommit=True)
        self.addCleanup(self.conn.close)
        self.conn.execute(f"DROP TABLE IF EXISTS {self.TABLE}")
        self.conn.execute(
            f"""
            CREATE TABLE {self.TABLE} (
                id          integer PRIMARY KEY,
                flag        boolean,
                small       smallint,
                big         bigint,
                ratio       double precision,
                amount      numeric(12, 4),
                label       text,
                short       varchar(16),
                blob        bytea,
                doc         jsonb,
                ident       uuid,
                at          timestamptz,
                on_day      date,
                at_time     time
            )
            """
        )
        self.addCleanup(self._drop)

    def _drop(self) -> None:
        if not self.conn.closed:
            self.conn.execute(f"DROP TABLE IF EXISTS {self.TABLE}")

    def _insert_full_row(self) -> None:
        self.conn.execute(
            f"""
            INSERT INTO {self.TABLE}
                (id, flag, small, big, ratio, amount, label, short, blob, doc,
                 ident, at, on_day, at_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                1,
                True,
                -7,
                2**40,
                1.5,
                Decimal("1234.5678"),
                "a text value",
                "short",
                b"\x00\x01binary\xff",
                '{"k": [1, 2]}',
                "11111111-2222-3333-4444-555555555555",
                datetime(2026, 3, 4, 5, 6, 7),
                date(2026, 3, 4),
                time_of_day(5, 6, 7),
            ),
        )

    def test_every_supported_type_survives_a_round_trip(self) -> None:
        self._insert_full_row()
        row = self.conn.execute(f"SELECT * FROM {self.TABLE} WHERE id = %s", (1,)).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["id"], 1)
        self.assertIs(row["flag"], True)
        self.assertEqual(row["small"], -7)
        self.assertEqual(row["big"], 2**40)
        self.assertEqual(row["ratio"], 1.5)
        self.assertEqual(row["amount"], Decimal("1234.5678"))
        self.assertIsInstance(row["amount"], Decimal)
        self.assertEqual(row["label"], "a text value")
        self.assertEqual(row["short"], "short")
        self.assertEqual(row["blob"], b"\x00\x01binary\xff")
        self.assertIsInstance(row["blob"], bytes)
        self.assertEqual(row["ident"], "11111111-2222-3333-4444-555555555555")
        self.assertIsInstance(row["doc"], str)
        self.assertIn('"k"', row["doc"])

    def test_timestamps_come_back_as_iso_strings(self) -> None:
        self._insert_full_row()
        row = self.conn.execute(
            f"SELECT at, on_day, at_time FROM {self.TABLE} WHERE id = 1"
        ).fetchone()
        for value in row:
            self.assertIsInstance(value, str)
        self.assertTrue(row["at"].startswith("2026-03-04 05:06:07"))
        self.assertEqual(row["on_day"], "2026-03-04")
        self.assertEqual(row["at_time"], "05:06:07")

    def test_nulls_go_out_and_come_back_as_none(self) -> None:
        self.conn.execute(
            f"INSERT INTO {self.TABLE} (id, label, flag, amount) VALUES (%s, %s, %s, %s)",
            (2, None, None, None),
        )
        row = self.conn.execute(
            f"SELECT label, flag, amount FROM {self.TABLE} WHERE id = 2"
        ).fetchone()
        self.assertEqual(dict(row), {"label": None, "flag": None, "amount": None})

    def test_a_dict_or_list_parameter_is_refused_by_name(self) -> None:
        with self.assertRaises(TypeError) as caught:
            self.conn.execute(
                f"INSERT INTO {self.TABLE} (id, doc) VALUES (%s, %s)", (3, {"k": 1})
            )
        self.assertIn("parameter 2", str(caught.exception))
        with self.assertRaises(TypeError):
            self.conn.execute("SELECT %s::text", ([1, 2],))

    def test_named_parameters_work_against_the_server(self) -> None:
        self.conn.execute(
            f"INSERT INTO {self.TABLE} (id, label) VALUES (%(id)s, %(label)s)",
            {"id": 4, "label": "named"},
        )
        row = self.conn.execute(
            f"SELECT label FROM {self.TABLE} WHERE id = %(id)s", {"id": 4}
        ).fetchone()
        self.assertEqual(row["label"], "named")

    def test_a_parameter_count_mismatch_is_reported(self) -> None:
        with self.assertRaises(ProgrammingError):
            self.conn.execute(f"SELECT * FROM {self.TABLE} WHERE id = %s", (1, 2))
        with self.assertRaises(ProgrammingError):
            self.conn.execute("SELECT 1", (1,))

    def test_rowcount_for_insert_update_delete_and_select(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            f"INSERT INTO {self.TABLE} (id, label) VALUES (%s, %s), (%s, %s), (%s, %s)",
            (10, "a", 11, "b", 12, "c"),
        )
        self.assertEqual(cur.rowcount, 3)
        cur.execute(f"UPDATE {self.TABLE} SET label = %s WHERE id >= %s", ("x", 11))
        self.assertEqual(cur.rowcount, 2)
        cur.execute(f"SELECT id FROM {self.TABLE} ORDER BY id")
        self.assertEqual(cur.rowcount, 3)
        cur.execute(f"DELETE FROM {self.TABLE} WHERE id = %s", (10,))
        self.assertEqual(cur.rowcount, 1)
        cur.close()

    def test_ddl_reports_no_rowcount(self) -> None:
        cur = self.conn.execute("CREATE TEMP TABLE pgwire_tmp_ddl (a int)")
        self.assertEqual(cur.rowcount, -1)
        self.assertIsNone(cur.description)

    def test_rows_are_both_mappings_and_tuples(self) -> None:
        self.conn.execute(f"INSERT INTO {self.TABLE} (id, label) VALUES (20, 'twenty')")
        row = self.conn.execute(
            f"SELECT id, label FROM {self.TABLE} WHERE id = 20"
        ).fetchone()
        self.assertEqual(row[0], 20)
        self.assertEqual(row["label"], "twenty")
        self.assertEqual(dict(row), {"id": 20, "label": "twenty"})
        self.assertEqual(row, (20, "twenty"))
        self.assertEqual(row.keys(), ["id", "label"])

    def test_fetch_variants_and_iteration(self) -> None:
        self.conn.execute(
            f"INSERT INTO {self.TABLE} (id) SELECT generate_series(30, 35)"
        )
        cur = self.conn.execute(f"SELECT id FROM {self.TABLE} ORDER BY id")
        self.assertEqual(cur.fetchone()["id"], 30)
        self.assertEqual([r["id"] for r in cur.fetchmany(2)], [31, 32])
        self.assertEqual([r["id"] for r in cur.fetchall()], [33, 34, 35])
        self.assertIsNone(cur.fetchone())
        self.assertEqual(cur.fetchall(), [])

        cur = self.conn.execute(f"SELECT id FROM {self.TABLE} ORDER BY id")
        self.assertEqual([r["id"] for r in cur], [30, 31, 32, 33, 34, 35])

    def test_a_cursor_is_a_context_manager(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT 42 AS answer")
            self.assertEqual(cur.fetchone()["answer"], 42)
        self.assertTrue(cur.closed)
        with self.assertRaises(ProgrammingError):
            cur.execute("SELECT 1")

    def test_executemany_runs_every_batch(self) -> None:
        cur = self.conn.cursor()
        cur.executemany(
            f"INSERT INTO {self.TABLE} (id, label) VALUES (%s, %s)",
            [(40, "a"), (41, "b"), (42, "c")],
        )
        self.assertEqual(cur.rowcount, 3)
        rows = self.conn.execute(
            f"SELECT id, label FROM {self.TABLE} WHERE id >= 40 ORDER BY id"
        ).fetchall()
        self.assertEqual([dict(r) for r in rows], [
            {"id": 40, "label": "a"},
            {"id": 41, "label": "b"},
            {"id": 42, "label": "c"},
        ])

    def test_a_multi_statement_script_runs_over_the_simple_protocol(self) -> None:
        script = """
            CREATE TABLE pgwire_script_a (id int PRIMARY KEY);
            CREATE TABLE pgwire_script_b (id int PRIMARY KEY, a_id int);
            INSERT INTO pgwire_script_a (id) VALUES (1), (2);
            INSERT INTO pgwire_script_b (id, a_id) VALUES (1, 1);
        """
        self.addCleanup(
            self.conn.execute, "DROP TABLE IF EXISTS pgwire_script_b, pgwire_script_a"
        )
        self.conn.cursor().executescript(script)
        count = self.conn.execute("SELECT count(*) FROM pgwire_script_a").fetchone()[0]
        self.assertEqual(count, 2)
        # execute() with no params takes the same path, so DDL scripts work there too.
        self.conn.execute(
            "INSERT INTO pgwire_script_a VALUES (3); UPDATE pgwire_script_a SET id = id"
        )
        self.assertEqual(
            self.conn.execute("SELECT count(*) FROM pgwire_script_a").fetchone()[0], 3
        )

    def test_nextset_walks_a_multi_statement_result(self) -> None:
        cur = self.conn.cursor()
        cur.execute("SELECT 1 AS n; SELECT 2 AS n; SELECT 3 AS n")
        self.assertEqual(cur.fetchone()["n"], 1)
        self.assertTrue(cur.nextset())
        self.assertEqual(cur.fetchone()["n"], 2)
        self.assertTrue(cur.nextset())
        self.assertEqual(cur.fetchone()["n"], 3)
        self.assertFalse(cur.nextset())

    def test_on_conflict_do_update_with_excluded(self) -> None:
        upsert = f"""
            INSERT INTO {self.TABLE} (id, label, big) VALUES (%s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                label = EXCLUDED.label,
                big = {self.TABLE}.big + EXCLUDED.big
            RETURNING id, label, big
        """
        cur = self.conn.execute(upsert, (50, "first", 10))
        self.assertEqual(dict(cur.fetchone()), {"id": 50, "label": "first", "big": 10})
        cur = self.conn.execute(upsert, (50, "second", 5))
        self.assertEqual(dict(cur.fetchone()), {"id": 50, "label": "second", "big": 15})

    def test_a_unique_violation_is_an_integrity_error(self) -> None:
        self.conn.execute(f"INSERT INTO {self.TABLE} (id) VALUES (60)")
        with self.assertRaises(IntegrityError) as caught:
            self.conn.execute(f"INSERT INTO {self.TABLE} (id) VALUES (60)")
        error = caught.exception
        self.assertEqual(error.sqlstate, "23505")
        self.assertEqual(error.table, self.TABLE)
        self.assertTrue(error.constraint)
        self.assertIn("23505", str(error))
        self.assertFalse(is_retryable(error))

    def test_an_undefined_table_is_a_programming_error(self) -> None:
        with self.assertRaises(ProgrammingError) as caught:
            self.conn.execute("SELECT * FROM definitely_not_here")
        self.assertEqual(caught.exception.sqlstate, "42P01")

    def test_an_empty_query_is_harmless(self) -> None:
        cur = self.conn.execute("")
        self.assertIsNone(cur.description)
        self.assertEqual(self.conn.execute("SELECT 1").fetchone()[0], 1)


class TestTransactions(ServerTestCase):
    TABLE = "pgwire_tx"

    def setUp(self) -> None:
        self.setup_conn = connect(autocommit=True)
        self.addCleanup(self.setup_conn.close)
        self.setup_conn.execute(f"DROP TABLE IF EXISTS {self.TABLE}")
        self.setup_conn.execute(
            f"CREATE TABLE {self.TABLE} (id integer PRIMARY KEY, label text)"
        )
        self.addCleanup(self.setup_conn.execute, f"DROP TABLE IF EXISTS {self.TABLE}")
        self.conn = connect()
        self.addCleanup(self.conn.close)

    def _count(self) -> int:
        return self.setup_conn.execute(f"SELECT count(*) FROM {self.TABLE}").fetchone()[0]

    def test_a_transaction_starts_lazily_and_commits(self) -> None:
        self.assertFalse(self.conn.in_transaction)
        self.conn.execute(f"INSERT INTO {self.TABLE} (id) VALUES (1)")
        self.assertTrue(self.conn.in_transaction)
        self.assertEqual(self._count(), 0)  # not visible to the other session yet
        self.conn.commit()
        self.assertFalse(self.conn.in_transaction)
        self.assertEqual(self._count(), 1)

    def test_rollback_discards_the_work(self) -> None:
        self.conn.execute(f"INSERT INTO {self.TABLE} (id) VALUES (2)")
        self.conn.rollback()
        self.assertFalse(self.conn.in_transaction)
        self.assertEqual(self._count(), 0)

    def test_autocommit_writes_immediately(self) -> None:
        self.conn.autocommit = True
        self.assertTrue(self.conn.autocommit)
        self.conn.execute(f"INSERT INTO {self.TABLE} (id) VALUES (3)")
        self.assertFalse(self.conn.in_transaction)
        self.assertEqual(self._count(), 1)

    def test_commit_and_rollback_outside_a_transaction_are_no_ops(self) -> None:
        self.conn.commit()
        self.conn.rollback()
        self.assertFalse(self.conn.in_transaction)

    def test_a_failed_statement_leaves_a_usable_connection_after_rollback(self) -> None:
        self.conn.execute(f"INSERT INTO {self.TABLE} (id, label) VALUES (4, 'keep')")
        with self.assertRaises(ProgrammingError):
            self.conn.execute("SELECT * FROM table_that_is_not_there")

        # The backend is now in a failed transaction: it says so, and it refuses
        # further work until the transaction is unwound.
        self.assertTrue(self.conn.in_transaction)
        self.assertEqual(self.conn.info.transaction_status, "E")
        with self.assertRaises(pgwire.PgError) as caught:
            self.conn.execute("SELECT 1")
        self.assertEqual(caught.exception.sqlstate, "25P02")

        self.conn.rollback()
        self.assertFalse(self.conn.in_transaction)
        self.assertEqual(self.conn.execute("SELECT 1").fetchone()[0], 1)
        self.assertEqual(self._count(), 0)  # the insert went away with the rollback

        self.conn.execute(f"INSERT INTO {self.TABLE} (id, label) VALUES (5, 'after')")
        self.conn.commit()
        self.assertEqual(self._count(), 1)

    def test_the_connection_context_manager_commits_then_closes(self) -> None:
        with connect() as conn:
            conn.execute(f"INSERT INTO {self.TABLE} (id) VALUES (6)")
        self.assertTrue(conn.closed)
        self.assertEqual(self._count(), 1)

    def test_the_connection_context_manager_rolls_back_on_error(self) -> None:
        with self.assertRaises(RuntimeError):
            with connect() as conn:
                conn.execute(f"INSERT INTO {self.TABLE} (id) VALUES (7)")
                raise RuntimeError("boom")
        self.assertTrue(conn.closed)
        self.assertEqual(self._count(), 0)

    def test_a_lock_conflict_surfaces_as_a_retryable_serialization_error(self) -> None:
        self.setup_conn.execute(f"INSERT INTO {self.TABLE} (id) VALUES (8)")
        holder = connect()
        self.addCleanup(holder.close)
        holder.execute(f"SELECT id FROM {self.TABLE} WHERE id = 8 FOR UPDATE")
        self.assertTrue(holder.in_transaction)

        with self.assertRaises(SerializationError) as caught:
            self.conn.execute(f"SELECT id FROM {self.TABLE} WHERE id = 8 FOR UPDATE NOWAIT")
        error = caught.exception
        self.assertEqual(error.sqlstate, "55P03")
        self.assertTrue(is_retryable(error))
        self.assertFalse(is_retryable(ValueError("unrelated")))

        # Both sessions stay usable once the loser rolls back.
        self.conn.rollback()
        holder.rollback()
        self.assertEqual(self.conn.execute("SELECT 1").fetchone()[0], 1)

    def test_is_retryable_covers_the_three_sqlstates(self) -> None:
        for sqlstate in ("40001", "40P01", "55P03"):
            self.assertTrue(is_retryable(SerializationError("x", sqlstate=sqlstate)))
        self.assertFalse(is_retryable(IntegrityError("x", sqlstate="23505")))


if __name__ == "__main__":
    unittest.main()
