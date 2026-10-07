"""Production guarantees: determinism, version safety, fencing, efficiency paths.

Each test runs the real executor in-process against a real SQLite target (the
PostgreSQL suite repeats the store-level guarantees against a real server).
"""

from __future__ import annotations

import io
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "tests"))

from test_integration import make_project, write_module, write_pipeline  # noqa: E402

from graetl.config import load_settings  # noqa: E402
from graetl.loader import load_pipeline  # noqa: E402
from graetl.runner.events import EVENT_PREFIX, EventWriter  # noqa: E402
from graetl.runner.executor import Executor  # noqa: E402
from graetl.store.db import LockConflict, LockLost  # noqa: E402
from graetl.store.outputs import OutputError  # noqa: E402

PIPELINE = '''
from graetl.sdk import Entity, Pipeline

pipeline = Pipeline(id="det", title="Deterministic", stateful=True, max_attempts={max_attempts})

@pipeline.entities
def discover(ctx):
    for i in range({n}):
        yield Entity(id=f"C{{i}}", source_updated_at="2026-01-01T00:00:00Z", data={{"n": i}})
'''

LABS = '''
from graetl.sdk import get_pipeline
pipeline = get_pipeline()

@pipeline.module(version={version}, execution_layer=0, outputs=["labs"])
def copy_labs(ctx, entity):
    rows = [{{"case_id": entity.id, "code": f"L{{k}}", "value": k * 1.5}} for k in range({rows})]
    ctx.write("labs", rows)
'''


class Run:
    """One in-process run, with its events."""

    def __init__(self, root: Path, pid: str, **params) -> None:
        settings = load_settings(root, require_project=True)
        loaded = load_pipeline(settings.pipeline_dir(pid), pipeline_id=pid)
        self.stream = io.StringIO()
        mode = params.pop("mode", "incremental")
        self.executor = Executor(
            loaded, settings, run_id=params.pop("run_id", 1), mode=mode,
            params=params, writer=EventWriter(self.stream),
        )
        self.result = self.executor.run()
        self.metrics = self.result.metrics

    @property
    def events(self) -> list[dict]:
        return [
            json.loads(line[len(EVENT_PREFIX):])
            for line in self.stream.getvalue().splitlines()
            if line.startswith(EVENT_PREFIX)
        ]

    def logs(self, level: str | None = None) -> list[str]:
        return [e["message"] for e in self.events
                if e["kind"] == "log" and (level is None or e.get("level") == level)]


class ProductionCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = make_project(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def sql(self, query: str, params=()):
        conn = sqlite3.connect(self.root / "warehouse.db")
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute(query, params).fetchall()]
        finally:
            conn.close()

    def pipeline(self, n: int = 3, max_attempts: int = 1) -> None:
        write_pipeline(self.root, "det", PIPELINE.format(n=n, max_attempts=max_attempts))

    def module(self, folder: str, name: str, source: str) -> None:
        write_module(self.root, "det", folder, name, source)

    def store(self):
        return load_settings(self.root, require_project=True).state_store("det")

    def go(self, **params) -> Run:
        return Run(self.root, "det", **params)


# --------------------------------------------------------------- determinism


class TestOwnedOutputs(ProductionCase):
    def test_running_twice_gives_the_same_rows(self) -> None:
        self.pipeline()
        self.module("labs", "copy_labs", LABS.format(version=1, rows=3))
        first = self.go()
        self.assertEqual(first.result.status, "succeeded", first.logs("error"))
        self.assertEqual(self.sql("SELECT COUNT(*) AS n FROM labs")[0]["n"], 9)
        # Full mode reprocesses every entity: still exactly 9 rows, not 18.
        again = self.go(mode="full", run_id=2)
        self.assertEqual(again.metrics["processed"], 3)
        self.assertEqual(self.sql("SELECT COUNT(*) AS n FROM labs")[0]["n"], 9)
        rows = self.sql("SELECT DISTINCT _graetl_run AS r FROM labs")
        self.assertEqual([r["r"] for r in rows], [2], "every row was rewritten by run 2")

    def test_a_new_version_replaces_the_old_versions_rows_completely(self) -> None:
        self.pipeline()
        self.module("labs", "copy_labs", LABS.format(version=6, rows=4))
        self.go()
        self.assertEqual(self.sql("SELECT COUNT(*) AS n FROM labs")[0]["n"], 12)
        # v7 produces fewer rows per entity: nothing of v6 may survive.
        self.module("labs", "copy_labs", LABS.format(version=7, rows=1))
        bumped = self.go(run_id=2)
        self.assertEqual(bumped.metrics["processed"], 3)
        rows = self.sql("SELECT case_id, code, _graetl_version AS v FROM labs ORDER BY case_id")
        self.assertEqual(len(rows), 3)
        self.assertEqual({r["v"] for r in rows}, {7})
        self.assertEqual({r["code"] for r in rows}, {"L0"})

    def test_a_table_the_new_version_no_longer_writes_is_cleared_too(self) -> None:
        self.pipeline(n=2)
        self.module("labs", "copy_labs", '''
from graetl.sdk import get_pipeline
pipeline = get_pipeline()

@pipeline.module(version=1)
def copy_labs(ctx, entity):
    ctx.write("labs_old", {"case_id": entity.id})
''')
        self.go()
        self.assertEqual(self.sql("SELECT COUNT(*) AS n FROM labs_old")[0]["n"], 2)
        self.module("labs", "copy_labs", '''
from graetl.sdk import get_pipeline
pipeline = get_pipeline()

@pipeline.module(version=2)
def copy_labs(ctx, entity):
    ctx.write("labs_new", {"case_id": entity.id})
''')
        self.go(run_id=2)
        self.assertEqual(self.sql("SELECT COUNT(*) AS n FROM labs_old")[0]["n"], 0)
        self.assertEqual(self.sql("SELECT COUNT(*) AS n FROM labs_new")[0]["n"], 2)

    def test_a_failure_keeps_the_previous_rows(self) -> None:
        self.pipeline(n=2)
        self.module("labs", "copy_labs", LABS.format(version=1, rows=2))
        self.go()
        self.module("labs", "copy_labs", '''
from graetl.sdk import get_pipeline
pipeline = get_pipeline()

@pipeline.module(version=2)
def copy_labs(ctx, entity):
    ctx.write("labs", {"case_id": entity.id, "code": "X", "value": 0.0})
    if entity.id == "C1":
        raise RuntimeError("source went away")
''')
        run = self.go(run_id=2)
        self.assertEqual(run.metrics["failed"], 1)
        c0 = self.sql("SELECT code FROM labs WHERE case_id = 'C0'")
        c1 = self.sql("SELECT code FROM labs WHERE case_id = 'C1' ORDER BY code")
        self.assertEqual([r["code"] for r in c0], ["X"])
        self.assertEqual([r["code"] for r in c1], ["L0", "L1"], "rolled back to v1's rows")
        with self.store() as st:
            state = {m["entity_id"]: m for m in st.states_for(["C0", "C1"]).get("C1", [])}
        self.assertEqual(state["C1"]["status"], "failed")
        self.assertEqual(state["C1"]["module_version"], 2)

    def test_skipping_leaves_no_rows_and_is_not_redone(self) -> None:
        self.pipeline(n=2)
        self.module("labs", "copy_labs", LABS.format(version=1, rows=2))
        self.go()
        self.module("labs", "copy_labs", '''
from graetl.sdk import get_pipeline
pipeline = get_pipeline()

@pipeline.module(version=2)
def copy_labs(ctx, entity):
    ctx.skip("no labs any more")
''')
        run = self.go(run_id=2)
        self.assertEqual(run.metrics["skipped"], 2)
        self.assertEqual(self.sql("SELECT COUNT(*) AS n FROM labs")[0]["n"], 0)
        self.assertEqual(self.go(run_id=3).metrics["skipped"], 0, "skipped is up to date")

    def test_retry_entity_rolls_back_and_stays_pending(self) -> None:
        self.pipeline(n=1)
        self.module("labs", "copy_labs", LABS.format(version=1, rows=2))
        self.go()
        self.module("labs", "copy_labs", '''
from graetl.sdk import get_pipeline, RetryEntity
pipeline = get_pipeline()

@pipeline.module(version=2)
def copy_labs(ctx, entity):
    ctx.write("labs", {"case_id": entity.id, "code": "partial", "value": 1.0})
    raise RetryEntity("source not ready")
''')
        self.go(run_id=2)
        codes = [r["code"] for r in self.sql("SELECT code FROM labs ORDER BY code")]
        self.assertEqual(codes, ["L0", "L1"], "nothing of the retried attempt is kept")
        with self.store() as st:
            row = st.states_for(["C0"])["C0"][0]
        self.assertEqual(row["status"], "pending")

    def test_upsert_is_idempotent_across_entities_and_runs(self) -> None:
        self.pipeline(n=3)
        self.module("dim", "lab_codes", '''
from graetl.sdk import get_pipeline
pipeline = get_pipeline()

@pipeline.module(version=1)
def lab_codes(ctx, entity):
    ctx.upsert("lab_code", [{"code": "CREA", "unit": "mg/dl"}, {"code": "NA", "unit": "mmol/l"}],
               key="code")
''')
        self.go()
        self.go(mode="full", run_id=2)
        rows = self.sql("SELECT code, unit FROM lab_code ORDER BY code")
        self.assertEqual(rows, [{"code": "CREA", "unit": "mg/dl"}, {"code": "NA", "unit": "mmol/l"}])

    def test_batch_scope_attributes_rows_to_entities(self) -> None:
        self.pipeline(n=4)
        self.module("b", "bulk", '''
from graetl.sdk import get_pipeline
pipeline = get_pipeline()

@pipeline.module(version=1, scope="batch", batch_size=3)
def bulk(ctx, entities):
    for e in entities:
        ctx.write("bulk_out", {"id": e.id}, entity=e)
''')
        self.go()
        self.go(mode="full", run_id=2)
        rows = self.sql("SELECT id, _graetl_entity AS owner FROM bulk_out ORDER BY id")
        self.assertEqual([(r["id"], r["owner"]) for r in rows],
                         [("C0", "C0"), ("C1", "C1"), ("C2", "C2"), ("C3", "C3")])

    def test_batch_scope_refuses_rows_without_or_outside_its_entities(self) -> None:
        with self.store() as st:
            st.upsert_entities([("A", None, None, {}), ("B", None, None, {})])
            items = [type("I", (), {"entity_id": e, "source_updated_at": None})() for e in "AB"]
            with self.assertRaises(OutputError):
                with st.batch_transaction(items, "m", 1, run_id=1):
                    st.outputs.write("t", {"x": 1})
            with self.assertRaises(OutputError):
                with st.batch_transaction(items, "m", 1, run_id=1):
                    st.outputs.write("t", {"x": 1}, entity="Z")
            with self.assertRaises(OutputError):
                st.outputs.write("t", {"x": 1})  # no unit of work at all

    def test_once_scope_replaces_its_whole_output(self) -> None:
        self.pipeline(n=2)
        self.module("s", "summary", '''
from graetl.sdk import get_pipeline
pipeline = get_pipeline()

@pipeline.module(version=1, scope="once", execution_layer=5)
def summary(ctx):
    ctx.write("summary", [{"k": "a"}, {"k": "b"}])
''')
        self.go()
        self.go(run_id=2)
        self.assertEqual(self.sql("SELECT COUNT(*) AS n FROM summary")[0]["n"], 2)

    def test_new_columns_are_added_and_dataframes_accepted(self) -> None:
        self.pipeline(n=1)
        self.module("labs", "copy_labs", LABS.format(version=1, rows=1))
        self.go()
        self.module("labs", "copy_labs", '''
from graetl.sdk import get_pipeline
import pandas as pd
pipeline = get_pipeline()

@pipeline.module(version=2)
def copy_labs(ctx, entity):
    ctx.write("labs", pd.DataFrame([{"case_id": entity.id, "code": "A", "value": 1.0, "unit": "mg"}]))
''')
        run = self.go(run_id=2)
        self.assertEqual(run.result.status, "succeeded", run.logs("error"))
        self.assertEqual(self.sql("SELECT unit FROM labs")[0]["unit"], "mg")

    def test_rename_carries_lineage_and_drop_purges(self) -> None:
        self.pipeline(n=2)
        self.module("labs", "copy_labs", LABS.format(version=1, rows=1))
        self.go()
        with self.store() as st:
            st.rename_module("copy_labs", "labs_v2")
            self.assertEqual(st.outputs.owned_tables("labs_v2"), ["labs"])
        self.assertEqual({r["m"] for r in self.sql("SELECT _graetl_module AS m FROM labs")},
                         {"labs_v2"})
        with self.store() as st:
            st.drop_module("labs_v2")
        self.assertEqual(self.sql("SELECT COUNT(*) AS n FROM labs")[0]["n"], 0)


# ------------------------------------------------------------ version safety


class TestVersionSafety(ProductionCase):
    def test_done_records_version_code_and_run(self) -> None:
        self.pipeline(n=1)
        self.module("labs", "copy_labs", LABS.format(version=7, rows=1))
        run = self.go(run_id=42)
        module = next(m for m in run.executor.pipeline.modules if m.name == "copy_labs")
        self.assertTrue(module.code_hash)
        row = self.sql("SELECT * FROM graetl_entity_module_state WHERE entity_id = 'C0'")[0]
        self.assertEqual((row["status"], row["module_version"], row["run_id"]), ("done", 7, 42))
        self.assertEqual(row["code_hash"], module.code_hash)
        self.assertIsNotNone(row["processed_seq"])

    def test_code_change_without_version_bump_is_reported(self) -> None:
        self.pipeline(n=2)
        self.module("labs", "copy_labs", LABS.format(version=3, rows=1))
        self.go()
        self.module("labs", "copy_labs", LABS.format(version=3, rows=2))  # same v, new code
        run = self.go(run_id=2)
        self.assertEqual(run.metrics["processed"], 0, "a version is the reprocessing trigger")
        self.assertEqual(run.metrics["version_drift"], {"copy_labs": 2})
        self.assertTrue(any("bump the version" in m for m in run.logs("warning")))
        module = next(m for m in run.executor.pipeline.modules if m.name == "copy_labs")
        with self.store() as st:
            health = st.module_health([("copy_labs", 3, module.code_hash)])["copy_labs"]
        self.assertEqual((health["current"], health["drift"]), (2, 2))

    def test_strict_versions_refuses_to_mix_results(self) -> None:
        self.pipeline(n=2)
        self.module("labs", "copy_labs", LABS.format(version=3, rows=1))
        self.go()
        toml = (self.root / "graetl.toml").read_text()
        (self.root / "graetl.toml").write_text(toml + "strict_versions=true\n")
        self.module("labs", "copy_labs", LABS.format(version=3, rows=2))
        run = self.go(mode="full", run_id=2)
        self.assertEqual(run.result.status, "failed")
        self.assertEqual(self.sql("SELECT COUNT(*) AS n FROM labs")[0]["n"], 2, "nothing rewritten")

    def test_a_rename_is_not_a_code_change(self) -> None:
        from graetl.loader import code_digest

        folder = Path(self._tmp.name)
        a = folder / "a.module.py"
        b = folder / "b.module.py"
        a.write_text("x = 1\r\n")
        b.write_text("x = 1\n")
        self.assertEqual(code_digest(a, [], folder), code_digest(b, [], folder))

    def test_helpers_are_part_of_the_code_hash(self) -> None:
        self.pipeline(n=1)
        source = '''
from graetl.sdk import get_pipeline
from helpers import factor
pipeline = get_pipeline()

@pipeline.module(version=1)
def scaled(ctx, entity):
    ctx.write("scaled", {"v": factor()})
'''
        write_module(self.root, "det", "s", "scaled", source, **{"helpers.py": "def factor():\n    return 2\n"})
        h1 = Run(self.root, "det").executor.pipeline.modules[0].code_hash
        write_module(self.root, "det", "s", "scaled", source, **{"helpers.py": "def factor():\n    return 3\n"})
        h2 = Run(self.root, "det", run_id=2).executor.pipeline.modules[0].code_hash
        self.assertNotEqual(h1, h2)


class TestFencing(ProductionCase):
    def _stale(self, module: str) -> None:
        conn = sqlite3.connect(self.root / "warehouse.db")
        conn.execute("UPDATE graetl_module_locks SET heartbeat_at = '2000-01-01T00:00:00.000Z' "
                     "WHERE module = ?", (module,))
        conn.commit()
        conn.close()

    def test_a_worker_that_lost_its_lock_cannot_record_done(self) -> None:
        with self.store() as a, self.store() as b:
            a.upsert_entities([("E1", None, None, {})])
            a.db.execute("CREATE TABLE out (id TEXT)")
            a.acquire_module_lock("m", run_id=1)
            with self.assertRaises(LockConflict):
                b.acquire_module_lock("m", run_id=2)
            self._stale("m")
            b.acquire_module_lock("m", run_id=2)  # takeover of a dead worker
            with self.assertRaises(LockLost):
                with a.entity_transaction("E1", "m", 1, run_id=1, source_updated_at=None):
                    a.db.execute("INSERT INTO out (id) VALUES ('E1')")
            self.assertEqual(self.sql("SELECT COUNT(*) AS n FROM out")[0]["n"], 0)
            self.assertEqual(
                self.sql("SELECT COUNT(*) AS n FROM graetl_entity_module_state")[0]["n"], 0,
                "neither done nor failed: the new owner decides",
            )
            # The new owner works normally.
            with b.entity_transaction("E1", "m", 1, run_id=2, source_updated_at=None):
                b.db.execute("INSERT INTO out (id) VALUES ('E1')")
            self.assertEqual(self.sql("SELECT status FROM graetl_entity_module_state")[0]["status"], "done")

    def test_a_live_lock_is_never_taken_over(self) -> None:
        with self.store() as a, self.store() as b:
            a.acquire_module_lock("m")
            for _ in range(3):
                with self.assertRaises(LockConflict):
                    b.acquire_module_lock("m")
            self.assertTrue(a.heartbeat_module_lock("m", a.worker))
            self.assertFalse(b.heartbeat_module_lock("m", b.worker))


class TestLockKeeper(ProductionCase):
    def test_beats_while_held_and_notices_a_takeover(self) -> None:
        import time

        from graetl.runner.executor import LockKeeper

        with self.store() as st:
            owner = st.acquire_module_lock("m")
            before = self.sql("SELECT heartbeat_at FROM graetl_module_locks")[0]["heartbeat_at"]
            keeper = LockKeeper(self.store, interval=0.05)
            keeper.start()
            keeper.hold("m", owner)
            time.sleep(0.3)
            after = self.sql("SELECT heartbeat_at FROM graetl_module_locks")[0]["heartbeat_at"]
            self.assertGreater(after, before)
            self.assertFalse(keeper.lost("m"))
            conn = sqlite3.connect(self.root / "warehouse.db")
            conn.execute("UPDATE graetl_module_locks SET owner = 'someone-else'")
            conn.commit()
            conn.close()
            time.sleep(0.3)
            self.assertTrue(keeper.lost("m"))
            keeper.stop()


class TestParallelWrites(ProductionCase):
    def test_parallel_modules_write_owned_rows_without_losing_any(self) -> None:
        self.pipeline(n=25)
        for name in ("a", "b", "c"):
            self.module(name, name, f'''
from graetl.sdk import get_pipeline
pipeline = get_pipeline()

@pipeline.module(version=1)
def {name}(ctx, entity):
    ctx.write("out_{name}", [{{"id": entity.id, "k": k}} for k in range(3)])
''')
        run = self.go(parallel=3)
        self.assertEqual(run.result.status, "succeeded", run.logs("error"))
        self.assertEqual(run.metrics["processed"], 75)
        again = self.go(mode="full", parallel=3, run_id=2)
        self.assertEqual(again.metrics["processed"], 75)
        for name in ("a", "b", "c"):
            self.assertEqual(self.sql(f"SELECT COUNT(*) AS n FROM out_{name}")[0]["n"], 75)


# ---------------------------------------------------------- selection rules


class TestSelection(ProductionCase):
    def test_cascade_uses_the_processing_sequence_not_the_clock(self) -> None:
        with self.store() as st:
            st.upsert_entities([("E", None, None, {})])
            for module in ("up", "down"):
                with st.entity_transaction("E", module, 1, run_id=1, source_updated_at=None):
                    pass
            # Same millisecond for both - a clock comparison could not tell them apart.
            st.db.execute("UPDATE [[entity_module_state]] SET processed_at = '2026-01-01T00:00:00.000Z'")
            self.assertEqual(st.count_work("down", 1, requires=[("up", 1)]), 0)
            with st.entity_transaction("E", "up", 1, run_id=2, source_updated_at=None):
                pass
            st.db.execute("UPDATE [[entity_module_state]] SET processed_at = '2026-01-01T00:00:00.000Z'")
            self.assertEqual(st.count_work("down", 1, requires=[("up", 1)]), 1)

    def test_max_attempts_blocks_in_sql_and_a_new_version_unblocks(self) -> None:
        with self.store() as st:
            st.upsert_entities([("E", None, None, {})])
            for _ in range(2):
                with self.assertRaises(RuntimeError):
                    with st.entity_transaction("E", "m", 1, run_id=1, source_updated_at=None):
                        raise RuntimeError("boom")
            self.assertEqual(st.count_work("m", 1, max_attempts=2), 0)
            self.assertEqual(st.count_work("m", 1, max_attempts=3), 1)
            self.assertEqual(st.blocked_count("m", 1, 2), 1)
            self.assertEqual(st.count_work("m", 2, max_attempts=2), 1, "new version: try again")

    def test_breakdown_counts_ready_and_waiting_in_one_scan(self) -> None:
        with self.store() as st:
            st.upsert_entities([(e, None, None, {}) for e in "ABC"])
            with st.entity_transaction("A", "up", 1, run_id=1, source_updated_at=None):
                pass
            b = st.work_breakdown("down", 1, requires=[("up", 1)])
            self.assertEqual(b, {"due": 3, "ready": 1, "waiting": 2})

    def test_bulk_entity_upsert(self) -> None:
        with self.store() as st:
            stats = st.upsert_entities([("A", "a", "r1", {"x": 1}), ("B", None, None, None),
                                        ("A", "a2", "r1", {"x": 2})])
            self.assertEqual(stats, {"new": 2, "changed": 0, "seen": 2})
            stats = st.upsert_entities([("A", None, "r2", None), ("C", None, None, None)])
            self.assertEqual(stats, {"new": 1, "changed": 1, "seen": 2})
            a = st.get_entity("A")
            self.assertEqual((a["label"], a["source_updated_at"], a["payload"]), ("a2", "r2", {"x": 2}))
            self.assertEqual(st.get_entity("B")["payload"], {})


# ---------------------------------------------------------------- the view


class TestFlowAndNodes(ProductionCase):
    def test_flow_endpoint_describes_layers_health_and_tables(self) -> None:
        from starlette.testclient import TestClient

        from graetl.server.app import create_app

        self.pipeline(n=2)
        self.module("labs", "copy_labs", LABS.format(version=1, rows=1))
        self.module("score", "score", '''
from graetl.sdk import get_pipeline
pipeline = get_pipeline()

@pipeline.module(version=1, execution_layer=10, depends_on=["copy_labs"])
def score(ctx, entity):
    ctx.upsert("scores", {"case_id": entity.id, "s": 1}, key="case_id")
''')
        self.go()
        with TestClient(create_app(load_settings(self.root))) as client:
            client.post("/api/pipelines/sync")
            flow = client.get("/api/pipelines/det/flow").json()
        self.assertEqual(flow["layers"], [0, 10])
        mods = {m["name"]: m for m in flow["modules"]}
        self.assertEqual(mods["copy_labs"]["health"]["current"], 2)
        self.assertEqual(mods["score"]["depends_on"], ["copy_labs"])
        self.assertEqual(mods["score"]["work"]["due"], 0)
        self.assertEqual({t["table"]: t["writers"][0]["mode"] for t in flow["tables"]},
                         {"labs": "owned", "scores": "upsert"})

    def test_write_and_upsert_nodes_compile_to_ctx_calls(self) -> None:
        from graetl.graph.compiler import compile_module_file
        from graetl.graph.model import Graph
        from graetl.graph.registry import NodeRegistry

        doc = {
            "graetl_graph": 1, "kind": "module", "name": "g",
            "nodes": [
                {"id": "entry", "op": "core:entry", "pos": [0, 0]},
                {"id": "w", "op": "core:write_rows", "pos": [200, 0],
                 "values": {"table": "labs", "rows": []}},
                {"id": "u", "op": "core:upsert_rows", "pos": [400, 0],
                 "values": {"table": "codes", "rows": [], "key": "code"}},
            ],
            "links": [
                {"from": ["entry", "then"], "to": ["w", "exec"]},
                {"from": ["w", "then"], "to": ["u", "exec"]},
            ],
        }
        source = compile_module_file(Graph.from_dict(doc), NodeRegistry()).source
        self.assertIn('ctx.write("labs", [])', source)
        self.assertIn('ctx.upsert("codes", [], key="code")', source)
        compile(source, "g.module.py", "exec")


if __name__ == "__main__":
    unittest.main()
