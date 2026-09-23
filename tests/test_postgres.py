"""The storage layer against a real PostgreSQL server.

These tests exist because the promise GraETL makes - a module's data write and
its state row commit together, or neither does - is a property of the *target
database*, and is worth nothing if it only holds on SQLite. Everything here is
run against a live server, and skipped when there is none.

Set ``GRAETL_TEST_DSN`` to point at a different server. The bundled pure-Python
driver (:mod:`graetl.store.pgwire`) is used automatically when psycopg is not
installed, so these also exercise it end to end.
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from graetl.store.db import (  # noqa: E402
    Database,
    LockConflict,
    Target,
    apply_migrations,
    connect,
)
from graetl.store.core import CoreStore, RunStatus  # noqa: E402
from graetl.store.state import StateStore  # noqa: E402

DSN = os.environ.get("GRAETL_TEST_DSN", "postgresql://graetl:graetl@127.0.0.1:5433/warehouse")
SCHEMA = "graetl_test"


def pg_target(schema: str = SCHEMA) -> Target:
    return Target(system="postgres", dsn=DSN, schema=schema)


class PostgresTestCase(unittest.TestCase):
    """Each test starts from an empty schema, so nothing leaks between them."""

    schema = SCHEMA

    @classmethod
    def setUpClass(cls) -> None:
        try:
            db = connect(pg_target(cls.schema))
        except Exception as exc:  # noqa: BLE001 - no server is a skip, not a failure
            raise unittest.SkipTest(f"no PostgreSQL at {DSN}: {exc}") from exc
        db.close()

    def setUp(self) -> None:
        admin = connect(Target(system="postgres", dsn=DSN, schema="public"))
        admin.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE')
        admin.execute("DROP TABLE IF EXISTS scores")
        admin.close()
        self.target = pg_target(self.schema)
        self._open: list[Database | CoreStore | StateStore] = []

    def tearDown(self) -> None:
        for handle in reversed(self._open):
            try:
                handle.close()
            except Exception:  # pragma: no cover - teardown is best effort
                pass

    def track(self, handle):
        self._open.append(handle)
        return handle

    def db(self) -> Database:
        return self.track(connect(self.target))

    def core(self) -> CoreStore:
        return self.track(CoreStore(connect(self.target)))

    def state(self, pipeline: str = "demo") -> StateStore:
        return self.track(StateStore(connect(self.target), pipeline))


class TestBootstrap(PostgresTestCase):
    def test_connecting_creates_graetls_own_schema(self) -> None:
        db = self.db()
        row = db.fetchone(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?",
            (self.schema,),
        )
        self.assertIsNotNone(row, "connect() should create the namespace it was told to use")

    def test_migrations_are_idempotent_and_versioned_per_component(self) -> None:
        db = self.db()
        from graetl.store import core as core_module
        from graetl.store import state as state_module

        for _ in range(2):
            apply_migrations(db, core_module.MIGRATIONS, "core")
            apply_migrations(db, state_module.MIGRATIONS, "state")
        rows = {
            r["component"]: r["version"]
            for r in db.fetchall("SELECT component, version FROM [[schema_version]]")
        }
        self.assertEqual(rows["core"], len(core_module.MIGRATIONS))
        self.assertEqual(rows["state"], len(state_module.MIGRATIONS))

    def test_the_warehouse_namespace_is_separate_from_the_data(self) -> None:
        """GraETL's tables must not collide with tables a pipeline creates."""
        db = self.db()
        db.execute("CREATE TABLE IF NOT EXISTS public.entities (id TEXT PRIMARY KEY)")
        db.execute("INSERT INTO public.entities (id) VALUES ('mine')")
        store = self.state("demo")
        store.upsert_entities([("A", "a", None, {})])
        self.assertEqual(store.count_entities(), 1)
        self.assertEqual(
            db.fetchone("SELECT COUNT(*) AS n FROM public.entities")["n"],
            1,
            "a pipeline's own 'entities' table is untouched by GraETL's",
        )
        db.execute("DROP TABLE public.entities")


class TestCoreStoreOnPostgres(PostgresTestCase):
    def test_a_run_gets_its_id_from_returning(self) -> None:
        store = self.core()
        store.upsert_pipeline(pipeline_id="p", title="P", folder="/tmp/p")
        first = store.create_run(pipeline_id="p")
        second = store.create_run(pipeline_id="p")
        self.assertIsInstance(first["id"], int)
        self.assertGreater(second["id"], first["id"])
        self.assertEqual(first["status"], RunStatus.QUEUED)

    def test_pipelines_are_listed_case_insensitively(self) -> None:
        store = self.core()
        for pid, title in (("b", "beta"), ("a", "Alpha"), ("c", "Gamma")):
            store.upsert_pipeline(pipeline_id=pid, title=title, folder=f"/tmp/{pid}")
        self.assertEqual([p["title"] for p in store.list_pipelines()], ["Alpha", "beta", "Gamma"])

    def test_upsert_replaces_rather_than_duplicates(self) -> None:
        store = self.core()
        store.upsert_pipeline(pipeline_id="p", title="First", folder="/tmp/p")
        store.upsert_pipeline(pipeline_id="p", title="Second", folder="/tmp/p", stateful=True)
        rows = store.list_pipelines()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Second")
        self.assertTrue(rows[0]["stateful"])

    def test_run_lifecycle_control_steps_and_events(self) -> None:
        store = self.core()
        store.upsert_pipeline(pipeline_id="p", title="P", folder="/tmp/p")
        run = store.create_run(pipeline_id="p", mode="full", trigger="cli")
        rid = run["id"]

        store.update_run(rid, status=RunStatus.RUNNING, started_at="2026-01-01T00:00:00.000Z")
        store.signal(rid, "pause")
        self.assertEqual(store.read_control(rid), ("pause", 1))
        store.signal(rid, "resume")
        self.assertEqual(store.read_control(rid), ("resume", 2))

        store.heartbeat(rid, phase="modules", done=3, total=10)
        self.assertEqual(store.get_run(rid)["progress_done"], 3)

        store.upsert_step(rid, "work", kind="module", version=2, seq=1)
        store.upsert_step(rid, "work", status="succeeded", processed=7, metrics_json={"a": 1})
        steps = store.list_steps(rid)
        self.assertEqual(len(steps), 1, "upsert_step must update, not duplicate")
        self.assertEqual(steps[0]["processed"], 7)
        self.assertEqual(steps[0]["metrics"], {"a": 1})

        store.add_event(rid, message="hello", data={"k": "v"})
        events = store.list_events(rid)
        self.assertEqual(events[-1]["message"], "hello")
        self.assertEqual(events[-1]["data"], {"k": "v"})

        finished = store.finish_run(rid, status=RunStatus.SUCCEEDED, metrics={"processed": 7})
        self.assertEqual(finished["status"], RunStatus.SUCCEEDED)
        self.assertIsNotNone(finished["duration_ms"])
        self.assertEqual(finished["metrics"], {"processed": 7})
        self.assertIsNone(finished["control"], "finishing clears the control channel")

    def test_deleting_a_pipeline_takes_its_runs_with_it(self) -> None:
        store = self.core()
        store.upsert_pipeline(pipeline_id="p", title="P", folder="/tmp/p")
        rid = store.create_run(pipeline_id="p")["id"]
        store.delete_pipeline("p")
        self.assertIsNone(store.get_run(rid), "the foreign key should cascade")

    def test_stale_runs_are_reaped(self) -> None:
        store = self.core()
        store.upsert_pipeline(pipeline_id="p", title="P", folder="/tmp/p")
        rid = store.create_run(pipeline_id="p")["id"]
        store.update_run(rid, status=RunStatus.RUNNING, pid=999999)
        self.assertEqual(store.reap_stale_runs(alive_pids=set()), [rid])
        self.assertEqual(store.get_run(rid)["status"], RunStatus.CRASHED)

    def test_settings_and_stats(self) -> None:
        store = self.core()
        store.set_setting("theme", {"dark": True})
        self.assertEqual(store.get_setting("theme"), {"dark": True})
        store.set_setting("theme", {"dark": False})
        self.assertEqual(store.get_setting("theme"), {"dark": False})
        self.assertEqual(store.get_setting("missing", "fallback"), "fallback")

        store.upsert_pipeline(pipeline_id="p", title="P", folder="/tmp/p")
        for status in (RunStatus.SUCCEEDED, RunStatus.SUCCEEDED, RunStatus.FAILED):
            rid = store.create_run(pipeline_id="p")["id"]
            store.update_run(rid, started_at="2026-01-01T00:00:00.000Z")
            store.finish_run(rid, status=status)
        stats = store.pipeline_stats("p")
        self.assertEqual(stats["runs_considered"], 3)
        self.assertAlmostEqual(stats["success_rate"], 0.667, places=2)


class TestStateStoreOnPostgres(PostgresTestCase):
    def seed(self, store: StateStore) -> None:
        store.upsert_entities(
            [
                ("A", "Alpha", "2026-01-01T00:00:00Z", {"n": 1}),
                ("B", "Beta", "2026-01-01T00:00:00Z", {"n": 2}),
                ("C", "Gamma", "2026-01-01T00:00:00Z", {"n": 3}),
            ]
        )

    def test_upsert_reports_new_changed_and_unchanged(self) -> None:
        store = self.state()
        first = store.upsert_entities([("A", "Alpha", "2026-01-01T00:00:00Z", {})])
        self.assertEqual((first["new"], first["changed"]), (1, 0))
        same = store.upsert_entities([("A", "Alpha", "2026-01-01T00:00:00Z", {})])
        self.assertEqual((same["new"], same["changed"]), (0, 0))
        moved = store.upsert_entities([("A", "Alpha", "2026-02-01T00:00:00Z", {})])
        self.assertEqual((moved["new"], moved["changed"]), (0, 1))

    def test_search_is_case_insensitive(self) -> None:
        """SQLite's LIKE ignores ASCII case; PostgreSQL needs ILIKE to match."""
        store = self.state()
        self.seed(store)
        self.assertEqual(store.count_matching_entities(search="alpha"), 1)
        self.assertEqual(store.count_matching_entities(search="ALPHA"), 1)
        self.assertEqual(store.count_matching_entities(search="a"), 3)

    def test_paging_and_ordering(self) -> None:
        store = self.state()
        self.seed(store)
        page = store.list_entities(limit=2, offset=0)
        self.assertEqual([e["entity_id"] for e in page], ["A", "B"])
        self.assertEqual(page[0]["payload"], {"n": 1})
        self.assertEqual([e["entity_id"] for e in store.list_entities(limit=2, offset=2)], ["C"])

    def test_soft_delete_hides_an_entity(self) -> None:
        store = self.state()
        self.seed(store)
        store.upsert_entity("A", label="Alpha")  # refreshes last_seen_at
        self.assertEqual(store.soft_delete_unseen("2999-01-01T00:00:00Z"), 3)
        self.assertEqual(store.count_entities(), 0)
        self.assertEqual(store.count_entities(include_deleted=True), 3)

    def test_work_selection_across_modes(self) -> None:
        store = self.state()
        self.seed(store)
        self.assertEqual(store.count_work("m", 1), 3)

        with store.entity_transaction(
            "A", "m", 1, run_id=1, source_updated_at="2026-01-01T00:00:00Z"
        ):
            pass
        self.assertEqual(store.count_work("m", 1), 2, "a done entity drops out")
        self.assertEqual(store.count_work("m", 1, mode="full"), 3, "full mode ignores state")
        self.assertEqual(store.count_work("m", 2), 3, "a version bump makes everything due again")

        store.upsert_entities([("A", "Alpha", "2026-06-01T00:00:00Z", None)])
        self.assertEqual(store.count_work("m", 1), 3, "a newer source revision makes it dirty")

    def test_requirements_gate_and_cascade(self) -> None:
        store = self.state()
        self.seed(store)
        requires = [("upstream", 1)]
        self.assertEqual(
            store.count_work("down", 1, requires=requires), 0, "nothing upstream has run"
        )
        for eid in ("A", "B", "C"):
            with store.entity_transaction(
                eid, "upstream", 1, run_id=1, source_updated_at="2026-01-01T00:00:00Z"
            ):
                pass
        self.assertEqual(store.count_work("down", 1, requires=requires), 3)

        for eid in ("A", "B", "C"):
            with store.entity_transaction(
                eid, "down", 1, run_id=1, source_updated_at="2026-01-01T00:00:00Z"
            ):
                pass
        self.assertEqual(store.count_work("down", 1, requires=requires), 0)

        # Reprocessing upstream must pull everything derived from it along.
        with store.entity_transaction(
            "A", "upstream", 1, run_id=2, source_updated_at="2026-01-01T00:00:00Z"
        ):
            pass
        self.assertEqual(
            store.count_work("down", 1, requires=requires), 1, "cascade invalidation"
        )

    def test_skipped_counts_as_up_to_date(self) -> None:
        store = self.state()
        self.seed(store)
        with store.entity_transaction(
            "A", "up", 1, run_id=1, source_updated_at="2026-01-01T00:00:00Z"
        ) as outcome:
            outcome["status"] = "skipped"
        self.assertEqual(store.count_work("down", 1, requires=[("up", 1)], entity_ids=["A"]), 1)

    def test_a_module_commits_its_data_and_its_state_together(self) -> None:
        store = self.state()
        self.seed(store)
        store.db.execute("CREATE TABLE IF NOT EXISTS scores (entity_id TEXT PRIMARY KEY, n INT)")
        with store.entity_transaction(
            "A", "m", 1, run_id=1, source_updated_at="2026-01-01T00:00:00Z"
        ):
            store.db.execute("INSERT INTO scores (entity_id, n) VALUES (?, ?)", ("A", 10))
        self.assertEqual(store.db.fetchone("SELECT n FROM scores WHERE entity_id = 'A'")["n"], 10)
        self.assertEqual(store.states_for(["A"])["A"][0]["status"], "done")

    def test_a_failing_module_rolls_back_its_data_and_records_the_failure(self) -> None:
        """The heart of it: no half-written entity, on either backend."""
        store = self.state()
        self.seed(store)
        store.db.execute("CREATE TABLE IF NOT EXISTS scores (entity_id TEXT PRIMARY KEY, n INT)")
        with self.assertRaises(RuntimeError):
            with store.entity_transaction(
                "A", "m", 1, run_id=1, source_updated_at="2026-01-01T00:00:00Z"
            ):
                store.db.execute("INSERT INTO scores (entity_id, n) VALUES (?, ?)", ("A", 10))
                raise RuntimeError("boom")
        self.assertIsNone(
            store.db.fetchone("SELECT n FROM scores WHERE entity_id = 'A'"),
            "the module's own write must be gone",
        )
        row = store.states_for(["A"])["A"][0]
        self.assertEqual(row["status"], "failed")
        self.assertIn("boom", row["error"])
        self.assertEqual(store.count_work("m", 1, entity_ids=["A"]), 1, "and it is due again")

    def test_an_interruption_leaves_the_entity_pending_not_failed(self) -> None:
        store = self.state()
        self.seed(store)

        class Interrupted(RuntimeError):
            graetl_pending = True

        with self.assertRaises(Interrupted):
            with store.entity_transaction(
                "A", "m", 1, run_id=1, source_updated_at="2026-01-01T00:00:00Z"
            ):
                raise Interrupted("operator stopped the run")
        row = store.states_for(["A"])["A"][0]
        self.assertEqual(row["status"], "pending")
        self.assertIsNone(row["error"], "an interruption is not a failure")

    def test_a_batch_commits_or_rolls_back_as_one_unit(self) -> None:
        store = self.state()
        self.seed(store)
        store.db.execute("CREATE TABLE IF NOT EXISTS scores (entity_id TEXT PRIMARY KEY, n INT)")
        items = store.select_work("m", 1)
        self.assertEqual(len(items), 3)

        with self.assertRaises(RuntimeError):
            with store.batch_transaction(items, "m", 1, run_id=1):
                for item in items:
                    store.db.execute(
                        "INSERT INTO scores (entity_id, n) VALUES (?, ?)", (item.entity_id, 1)
                    )
                raise RuntimeError("poison")
        self.assertEqual(
            store.db.fetchone("SELECT COUNT(*) AS n FROM scores")["n"], 0, "all or nothing"
        )
        self.assertEqual(store.status_counts(), {"failed": 3})

        with store.batch_transaction(items, "m", 1, run_id=2):
            for item in items:
                store.db.execute(
                    "INSERT INTO scores (entity_id, n) VALUES (?, ?)", (item.entity_id, 1)
                )
        self.assertEqual(store.db.fetchone("SELECT COUNT(*) AS n FROM scores")["n"], 3)
        self.assertEqual(store.status_counts(), {"done": 3})

    def test_module_rename_drop_and_resets(self) -> None:
        store = self.state()
        self.seed(store)
        for eid in ("A", "B"):
            with store.entity_transaction(
                eid, "old", 1, run_id=1, source_updated_at="2026-01-01T00:00:00Z"
            ):
                pass
        # A row already under the new name wins over the one being renamed onto it.
        with store.entity_transaction(
            "A", "new", 1, run_id=1, source_updated_at="2026-01-01T00:00:00Z"
        ):
            pass
        self.assertEqual(store.rename_module("old", "new"), 1)
        names = {row["module"] for rows in store.states_for(["A", "B"]).values() for row in rows}
        self.assertEqual(names, {"new"})

        self.assertEqual(store.reset_entity("A"), 1)
        self.assertEqual(store.drop_module("new"), 1)
        self.assertEqual(store.status_counts(), {})

        self.seed(store)
        with store.entity_transaction(
            "A", "m", 1, run_id=1, source_updated_at="2026-01-01T00:00:00Z"
        ):
            pass
        store.reset_all(drop_entities=True)
        self.assertEqual(store.count_entities(), 0)

    def test_meta_round_trips(self) -> None:
        store = self.state()
        self.assertEqual(store.get_meta("cursor", "none"), "none")
        store.set_meta("cursor", {"at": "2026-01-01"})
        self.assertEqual(store.get_meta("cursor"), {"at": "2026-01-01"})
        store.set_meta("cursor", 42)
        self.assertEqual(store.get_meta("cursor"), 42)

    def test_stale_running_rows_are_healed(self) -> None:
        store = self.state()
        self.seed(store)
        store.mark_running("A", "m", 1, 7)
        self.assertEqual(store.status_counts(), {"running": 1})
        self.assertEqual(store.reset_stale_running(), 1)
        self.assertEqual(store.status_counts(), {"pending": 1})


class TestPipelineIsolation(PostgresTestCase):
    """One warehouse holds many pipelines; they must not see each other."""

    def test_entities_and_state_are_scoped_to_their_pipeline(self) -> None:
        left = self.state("left")
        right = self.state("right")
        left.upsert_entities([("A", "left A", "2026-01-01T00:00:00Z", {"side": "l"})])
        right.upsert_entities([("A", "right A", "2026-01-01T00:00:00Z", {"side": "r"})])

        self.assertEqual(left.count_entities(), 1)
        self.assertEqual(right.count_entities(), 1)
        self.assertEqual(left.get_entity("A")["label"], "left A")
        self.assertEqual(right.get_entity("A")["label"], "right A")
        self.assertEqual(right.get_entity("A")["payload"], {"side": "r"})

        with left.entity_transaction(
            "A", "m", 1, run_id=1, source_updated_at="2026-01-01T00:00:00Z"
        ):
            pass
        self.assertEqual(left.count_work("m", 1), 0)
        self.assertEqual(right.count_work("m", 1), 1, "the other pipeline still has work")
        self.assertEqual(right.states_for(["A"]), {})

    def test_resets_do_not_reach_across_pipelines(self) -> None:
        left = self.state("left")
        right = self.state("right")
        for store in (left, right):
            store.upsert_entities([("A", "A", "2026-01-01T00:00:00Z", {})])
            with store.entity_transaction(
                "A", "m", 1, run_id=1, source_updated_at="2026-01-01T00:00:00Z"
            ):
                pass
        left.reset_all(drop_entities=True)
        self.assertEqual(left.count_entities(), 0)
        self.assertEqual(right.count_entities(), 1)
        self.assertEqual(right.status_counts(), {"done": 1})

    def test_meta_and_locks_are_scoped_too(self) -> None:
        left = self.state("left")
        right = self.state("right")
        left.set_meta("cursor", "left")
        right.set_meta("cursor", "right")
        self.assertEqual(left.get_meta("cursor"), "left")
        self.assertEqual(right.get_meta("cursor"), "right")

        left.acquire_module_lock("shared", run_id=1)
        # Same module name, different pipeline: not a conflict.
        right.acquire_module_lock("shared", run_id=2)
        self.assertEqual(len(left.list_module_locks()), 1)
        self.assertEqual(len(right.list_module_locks()), 1)


class TestModuleLocksOnPostgres(PostgresTestCase):
    def test_a_second_worker_is_refused_and_a_dead_one_is_taken_over(self) -> None:
        first = self.state("demo")
        second = self.state("demo")
        owner = first.acquire_module_lock("calc", run_id=1)
        with self.assertRaises(LockConflict):
            second.acquire_module_lock("calc", run_id=2)

        first.db.execute(
            "UPDATE [[module_locks]] SET heartbeat_at = '2000-01-01T00:00:00.000Z' "
            "WHERE pipeline = ? AND module = ?",
            ("demo", "calc"),
        )
        taken = second.acquire_module_lock("calc", run_id=2)
        self.assertNotEqual(taken, owner)
        self.assertEqual(second.list_module_locks()[0]["run_id"], 2)

    def test_the_lock_is_released_when_the_context_exits(self) -> None:
        store = self.state("demo")
        with store.module_lock("m", run_id=1):
            self.assertEqual(len(store.list_module_locks()), 1)
        self.assertEqual(store.list_module_locks(), [])

    def test_contention_raises_lock_conflict_and_records_nothing(self) -> None:
        """A blocked writer must retry, never mark the entity failed."""
        writer = self.state("demo")
        blocker = self.state("demo")
        writer.db.execute("CREATE TABLE IF NOT EXISTS scores (entity_id TEXT PRIMARY KEY, n INT)")
        writer.upsert_entities([("A", "A", None, {})])
        writer.db.execute("INSERT INTO scores (entity_id, n) VALUES ('A', 0)")

        # Hold the row, then make the other worker fail fast rather than wait.
        blocker.begin("immediate")
        blocker.db.execute("UPDATE scores SET n = 1 WHERE entity_id = 'A'")
        writer.db.execute("SET lock_timeout = '150ms'")
        try:
            with self.assertRaises(LockConflict):
                with writer.entity_transaction(
                    "A", "m", 1, run_id=1, source_updated_at=None
                ):
                    writer.db.execute("UPDATE scores SET n = 2 WHERE entity_id = 'A'")
        finally:
            blocker.rollback()
        self.assertEqual(writer.states_for(["A"]), {}, "contention writes no state row at all")


class TestOneSqlTwoBackends(unittest.TestCase):
    """The same module code has to run unchanged on SQLite and PostgreSQL."""

    @classmethod
    def setUpClass(cls) -> None:
        try:
            connect(pg_target("graetl_both")).close()
        except Exception as exc:  # noqa: BLE001
            raise unittest.SkipTest(f"no PostgreSQL at {DSN}: {exc}") from exc

    def run_sequence(self, target: Target) -> list:
        store = StateStore(connect(target), "demo")
        try:
            store.upsert_entities(
                [("A", "Alpha", "2026-01-01T00:00:00Z", {"n": 1}),
                 ("B", "Beta", "2026-01-01T00:00:00Z", {"n": 2})]
            )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS scores (entity_id TEXT PRIMARY KEY, n INTEGER)"
            )
            for item in store.select_work("score", 1):
                with store.entity_transaction(
                    item.entity_id, "score", 1, run_id=1,
                    source_updated_at=item.source_updated_at,
                ):
                    # Written once, with '?' placeholders and an upsert - the
                    # two spellings every backend here understands.
                    store.db.execute(
                        "INSERT INTO scores (entity_id, n) VALUES (?, ?) "
                        "ON CONFLICT(entity_id) DO UPDATE SET n = excluded.n",
                        (item.entity_id, int(item.payload["n"]) * 10),
                    )
            rows = store.db.fetchall("SELECT entity_id, n FROM scores ORDER BY entity_id")
            return [
                [(r["entity_id"], r["n"]) for r in rows],
                store.status_counts(),
                store.count_work("score", 1),
                store.count_matching_entities(search="ALPHA"),
            ]
        finally:
            store.close()

    def test_identical_results_on_both(self) -> None:
        import tempfile

        admin = connect(Target(system="postgres", dsn=DSN, schema="public"))
        admin.execute('DROP SCHEMA IF EXISTS "graetl_both" CASCADE')
        admin.execute("DROP TABLE IF EXISTS scores")
        admin.close()

        with tempfile.TemporaryDirectory() as tmp:
            sqlite_result = self.run_sequence(
                Target(system="sqlite", path=Path(tmp) / "warehouse.db")
            )
        postgres_result = self.run_sequence(pg_target("graetl_both"))
        self.assertEqual(sqlite_result, postgres_result)
        self.assertEqual(sqlite_result[0], [("A", 10), ("B", 20)])
        self.assertEqual(sqlite_result[3], 1, "search is case-insensitive on both")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
