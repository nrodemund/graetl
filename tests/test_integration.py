"""End-to-end tests: API, supervisor, isolated runner process, entity state.

Runnable with either ``pytest`` or ``python -m unittest``.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from starlette.testclient import TestClient  # noqa: E402

from graetl.config import load_settings  # noqa: E402
from graetl import loader  # noqa: E402
from graetl.graph.model import GraphError  # noqa: E402
from graetl.loader import inspect_folder, load_pipeline, scaffold_pipeline  # noqa: E402
from graetl.server.app import create_app  # noqa: E402
from graetl.store.core import CoreStore  # noqa: E402
from graetl.store.state import StateStore  # noqa: E402

SLOW_PIPELINE = '''
import time
from graetl.sdk import Entity, Pipeline

pipeline = Pipeline(id="slow", title="Slow", stateful=True)


@pipeline.setup
def setup(ctx):
    ctx.db.execute("CREATE TABLE IF NOT EXISTS out (id TEXT PRIMARY KEY)")


@pipeline.entities
def discover(ctx):
    for i in range(30):
        yield Entity(id=f"E{i:03d}", source_updated_at="2026-01-01T00:00:00Z")


@pipeline.module("work", version=1)
def work(ctx, entity):
    ctx.checkpoint()
    time.sleep(0.2)
    ctx.db.execute("INSERT OR REPLACE INTO out (id) VALUES (?)", (entity.id,))
    ctx.metric("done", 1)
'''

FAILING_PIPELINE = '''
from graetl.sdk import Entity, Pipeline

pipeline = Pipeline(id="flaky", title="Flaky", stateful=True, max_attempts=5)


@pipeline.setup
def setup(ctx):
    ctx.db.execute("CREATE TABLE IF NOT EXISTS ok (id TEXT PRIMARY KEY)")


@pipeline.entities
def discover(ctx):
    for i in range(4):
        yield Entity(id=f"E{i}", source_updated_at="2026-01-01T00:00:00Z")


@pipeline.module("work", version=1)
def work(ctx, entity):
    ctx.db.execute("INSERT OR REPLACE INTO ok (id) VALUES (?)", (entity.id,))
    if entity.id == "E2":
        raise RuntimeError("boom")
'''


def make_project(tmp: Path, *, target: str = "sqlite", dsn: str = "") -> Path:
    """An instance root that is also a project - the shortest useful fixture."""
    (tmp / "pipelines").mkdir(parents=True, exist_ok=True)
    (tmp / "graetl.toml").write_text(
        '[server]\nhost="127.0.0.1"\nport=8999\n[runtime]\nstop_grace_seconds=5\n'
        "control_poll_seconds=0.2\nheartbeat_seconds=1\n",
        encoding="utf-8",
    )
    if target == "postgres":
        block = f'system = "postgres"\ndsn = "{dsn}"\nschema = "graetl"'
    else:
        block = 'system = "sqlite"\npath = "warehouse.db"'
    (tmp / "project.toml").write_text(
        f'schema = 1\n[project]\nname = "test"\ntitle = "Test Warehouse"\n'
        f"[target]\n{block}\n[files]\nroot = \"files\"\n",
        encoding="utf-8",
    )
    return tmp


def write_pipeline(root: Path, pid: str, source: str) -> None:
    folder = root / "pipelines" / pid
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "pipeline.py").write_text(source, encoding="utf-8")


def minimal_graph(name: str, kind: str = "module", layer: int = 0) -> str:
    """The smallest valid graph document: an entry node and nothing else."""
    document: dict = {
        "graetl_graph": 1,
        "kind": kind,
        "name": name,
        "execution_layer": layer,
        "nodes": [{"id": "entry", "op": "core:entry", "pos": [0, 0]}],
        "links": [],
    }
    if kind == "function":
        document["inputs"] = []
        document["outputs"] = []
    return json.dumps(document)


def write_module(root: Path, pid: str, folder: str, name: str, source: str, **files: str) -> Path:
    """Create pipelines/<pid>/modules/<folder>/<name>.module.py plus extra files."""
    target = root / "pipelines" / pid / "modules" / folder
    target.mkdir(parents=True, exist_ok=True)
    (target / f"{name}.module.py").write_text(source, encoding="utf-8")
    for filename, content in files.items():
        (target / filename).write_text(content, encoding="utf-8")
    return target


def wait_for(fn, timeout: float = 30.0, interval: float = 0.15):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(interval)
    return last


class GraetlTestCase(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        self.root = make_project(Path(self._tmp.name))
        self.settings = load_settings(self.root)

    @property
    def warehouse(self) -> Path:
        """The project's target database file."""
        return self.root / "warehouse.db"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def client(self) -> TestClient:
        return TestClient(create_app(load_settings(self.root)))


class TestStateStore(GraetlTestCase):
    def test_incremental_selection_rules(self) -> None:
        store = StateStore(self.root / "state.db")
        store.upsert_entities(
            [
                ("A", None, "2026-01-01T00:00:00.000Z", {}),
                ("B", None, "2026-01-01T00:00:00.000Z", {}),
            ]
        )
        work = store.select_work("m", 1)
        self.assertEqual({w.entity_id for w in work}, {"A", "B"})

        # Processing A removes it from the work list.
        store.mark_running("A", "m", 1, 1)
        with store.entity_transaction(
            "A", "m", 1, run_id=1, source_updated_at="2026-01-01T00:00:00.000Z"
        ):
            pass
        self.assertEqual({w.entity_id for w in store.select_work("m", 1)}, {"B"})

        # A newer source revision makes it dirty again.
        store.upsert_entities([("A", None, "2026-02-01T00:00:00.000Z", {})])
        self.assertEqual({w.entity_id for w in store.select_work("m", 1)}, {"A", "B"})

        # ... and so does a new module version.
        store.mark_running("A", "m", 1, 1)
        with store.entity_transaction(
            "A", "m", 1, run_id=1, source_updated_at="2026-02-01T00:00:00.000Z"
        ):
            pass
        self.assertEqual({w.entity_id for w in store.select_work("m", 1)}, {"B"})
        self.assertEqual({w.entity_id for w in store.select_work("m", 2)}, {"A", "B"})
        store.close()

    def test_transaction_rolls_back_module_writes(self) -> None:
        store = StateStore(self.root / "state.db")
        store.conn.execute("CREATE TABLE t (id TEXT)")
        store.upsert_entities([("A", None, None, {})])
        with self.assertRaises(RuntimeError):
            with store.entity_transaction("A", "m", 1, run_id=1, source_updated_at=None):
                store.conn.execute("INSERT INTO t VALUES ('A')")
                raise RuntimeError("boom")
        # The data write was rolled back, the failure was recorded.
        self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM t").fetchone()[0], 0)
        state = store.states_for(["A"])["A"][0]
        self.assertEqual(state["status"], "failed")
        self.assertIn("boom", state["error"])
        store.close()

    def test_dependencies_gate_work(self) -> None:
        store = StateStore(self.root / "state.db")
        store.upsert_entities([("A", None, None, {})])
        self.assertEqual(store.select_work("b", 1, depends_on=["a"]), [])
        store.mark_running("A", "a", 1, 1)
        with store.entity_transaction("A", "a", 1, run_id=1, source_updated_at=None):
            pass
        self.assertEqual(len(store.select_work("b", 1, depends_on=["a"])), 1)
        store.close()


class TestApi(GraetlTestCase):
    def test_empty_pipeline_succeeds_immediately(self) -> None:
        scaffold_pipeline(self.settings, "empty_one", template="empty")
        with self.client() as client:
            pipelines = client.get("/api/pipelines").json()
            self.assertIn("empty_one", [p["id"] for p in pipelines])

            run = client.post("/api/pipelines/empty_one/runs", json={}).json()
            final = wait_for(
                lambda: (
                    client.get(f"/api/runs/{run['id']}").json()
                    if client.get(f"/api/runs/{run['id']}").json()["status"]
                    in ("succeeded", "failed", "crashed", "stopped")
                    else None
                )
            )
            self.assertEqual(final["status"], "succeeded")
            self.assertEqual(final["metrics"]["processed"], 0)
            self.assertEqual(final["metrics"]["failed"], 0)

    def test_run_pause_resume_stop_in_separate_process(self) -> None:
        write_pipeline(self.root, "slow", SLOW_PIPELINE)
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run = client.post("/api/pipelines/slow/runs", json={}).json()
            run_id = run["id"]

            running = wait_for(
                lambda: (
                    client.get(f"/api/runs/{run_id}").json()
                    if client.get(f"/api/runs/{run_id}").json()["status"] == "running"
                    else None
                )
            )
            self.assertEqual(running["status"], "running")
            self.assertTrue(running["pid"])

            # pause
            client.post(f"/api/runs/{run_id}/pause")
            paused = wait_for(
                lambda: (
                    client.get(f"/api/runs/{run_id}").json()
                    if client.get(f"/api/runs/{run_id}").json()["status"] == "paused"
                    else None
                )
            )
            self.assertEqual(paused["status"], "paused")
            done_at_pause = paused["progress_done"]
            time.sleep(1.0)
            still = client.get(f"/api/runs/{run_id}").json()
            self.assertEqual(still["status"], "paused")
            self.assertEqual(still["progress_done"], done_at_pause)

            # resume
            client.post(f"/api/runs/{run_id}/resume")
            resumed = wait_for(
                lambda: (
                    client.get(f"/api/runs/{run_id}").json()
                    if client.get(f"/api/runs/{run_id}").json()["status"] == "running"
                    else None
                )
            )
            self.assertEqual(resumed["status"], "running")

            # stop gracefully
            client.post(f"/api/runs/{run_id}/stop")
            stopped = wait_for(
                lambda: (
                    client.get(f"/api/runs/{run_id}").json()
                    if client.get(f"/api/runs/{run_id}").json()["status"]
                    in ("stopped", "succeeded", "failed", "crashed")
                    else None
                )
            )
            self.assertEqual(stopped["status"], "stopped")

            console = client.get(f"/api/runs/{run_id}/console").json()
            self.assertTrue(any(e.get("kind") == "entity" for e in console["events"]))

            # resuming a finished run starts a fresh run that picks up the rest
            second = client.post(f"/api/runs/{run_id}/resume").json()
            self.assertNotEqual(second["id"], run_id)
            self.assertEqual(second["parent_run_id"], run_id)
            final = wait_for(
                lambda: (
                    client.get(f"/api/runs/{second['id']}").json()
                    if client.get(f"/api/runs/{second['id']}").json()["status"]
                    in ("succeeded", "failed", "crashed", "stopped")
                    else None
                ),
                timeout=60,
            )
            self.assertEqual(final["status"], "succeeded")
            entities = client.get("/api/pipelines/slow/entities?limit=100").json()
            self.assertEqual(entities["total"], 30)
            done = [
                e
                for e in entities["entities"]
                if e["modules"] and e["modules"][0]["status"] == "done"
            ]
            self.assertEqual(len(done), 30)

    def test_failed_entities_are_isolated_and_retryable(self) -> None:
        write_pipeline(self.root, "flaky", FAILING_PIPELINE)
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run = client.post("/api/pipelines/flaky/runs", json={}).json()
            final = wait_for(
                lambda: (
                    client.get(f"/api/runs/{run['id']}").json()
                    if client.get(f"/api/runs/{run['id']}").json()["status"]
                    in ("succeeded", "failed", "crashed", "stopped")
                    else None
                )
            )
            self.assertEqual(final["status"], "failed")
            self.assertEqual(final["metrics"]["failed"], 1)
            self.assertEqual(final["metrics"]["processed"], 3)

            entities = client.get("/api/pipelines/flaky/entities").json()
            failed = [
                e
                for e in entities["entities"]
                if e["modules"] and e["modules"][0]["status"] == "failed"
            ]
            self.assertEqual([e["entity_id"] for e in failed], ["E2"])

            # the failing entity's own write was rolled back, the others persisted
            with self.settings.state_store("flaky") as state:
                ids = {r[0] for r in state.conn.execute("SELECT id FROM ok").fetchall()}
            self.assertEqual(ids, {"E0", "E1", "E3"})

            # retry-failed mode only picks up E2
            retry = client.post(
                "/api/pipelines/flaky/runs", json={"mode": "retry-failed"}
            ).json()
            final2 = wait_for(
                lambda: (
                    client.get(f"/api/runs/{retry['id']}").json()
                    if client.get(f"/api/runs/{retry['id']}").json()["status"]
                    in ("succeeded", "failed", "crashed", "stopped")
                    else None
                )
            )
            self.assertEqual(final2["metrics"]["work_items"], 1)

    def test_pipeline_crud_and_file_editing(self) -> None:
        with self.client() as client:
            created = client.post(
                "/api/pipelines",
                json={"id": "my_flow", "title": "My Flow", "template": "stateless"},
            )
            self.assertEqual(created.status_code, 201)
            files = client.get("/api/pipelines/my_flow/files").json()
            self.assertIn("pipeline.py", [f["path"] for f in files])

            content = client.get("/api/pipelines/my_flow/file?path=pipeline.py").json()["content"]
            self.assertIn("Pipeline(", content)
            updated = content.replace('title="My Flow"', 'title="Renamed"')
            resp = client.put(
                "/api/pipelines/my_flow/file?path=pipeline.py", json={"content": updated}
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["title"], "Renamed")

            deleted = client.delete("/api/pipelines/my_flow?delete_files=true")
            self.assertEqual(deleted.status_code, 200)
            self.assertEqual(client.get("/api/pipelines/my_flow").status_code, 404)

    def test_broken_pipeline_is_reported_not_fatal(self) -> None:
        write_pipeline(self.root, "broken", "this is not python(")
        with self.client() as client:
            pipelines = {p["id"]: p for p in client.get("/api/pipelines").json()}
            self.assertIn("broken", pipelines)
            self.assertTrue(pipelines["broken"]["definition_error"])
            run = client.post("/api/pipelines/broken/runs", json={}).json()
            final = wait_for(
                lambda: (
                    client.get(f"/api/runs/{run['id']}").json()
                    if client.get(f"/api/runs/{run['id']}").json()["status"]
                    in ("succeeded", "failed", "crashed", "stopped")
                    else None
                )
            )
            self.assertEqual(final["status"], "failed")
            self.assertIn("Could not load pipeline", final["error"])

    def test_websocket_console_stream(self) -> None:
        scaffold_pipeline(self.settings, "ws_demo", template="stateless")
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run = client.post("/api/pipelines/ws_demo/runs", json={}).json()
            with client.websocket_connect(f"/api/ws/runs/{run['id']}") as ws:
                snapshot = ws.receive_json()
                self.assertEqual(snapshot["kind"], "snapshot")
                kinds = {e.get("kind") for e in snapshot["events"]}
                deadline = time.time() + 20
                while "run_end" not in kinds and time.time() < deadline:
                    kinds.add(ws.receive_json().get("kind"))
                self.assertIn("run_end", kinds)

    def test_single_active_run_per_pipeline(self) -> None:
        write_pipeline(self.root, "slow", SLOW_PIPELINE)
        with self.client() as client:
            client.post("/api/pipelines/sync")
            first = client.post("/api/pipelines/slow/runs", json={})
            self.assertEqual(first.status_code, 201)
            second = client.post("/api/pipelines/slow/runs", json={})
            self.assertEqual(second.status_code, 409)
            client.post(f"/api/runs/{first.json()['id']}/stop?force=true")


FOLDER_PIPELINE = '''
from graetl.sdk import Entity, Pipeline

pipeline = Pipeline(id="folders", title="Folders", stateful=True)


@pipeline.setup
def setup(ctx):
    ctx.db.execute("CREATE TABLE IF NOT EXISTS out (id TEXT PRIMARY KEY, label TEXT)")


@pipeline.entities
def discover(ctx):
    for i in range(3):
        yield Entity(id=f"E{i}", source_updated_at="2026-01-01T00:00:00Z", data={"n": i})


@pipeline.function("shout")
def shout(text):
    """Shared helper."""
    return str(text).upper()
'''

LOAD_MODULE = '''
from graetl.sdk import get_pipeline

import naming   # plain helper file next to this one - never auto-loaded

pipeline = get_pipeline()


@pipeline.module(version=3, execution_layer=10)
def load_vital_signals(ctx, entity):
    """Docstring description."""
    ctx.db.execute(
        "INSERT OR REPLACE INTO out (id, label) VALUES (?, ?)",
        (entity.id, naming.label(ctx.fn("shout")(entity.id))),
    )
    ctx.metric("loaded", 1)
'''

NAMING_HELPER = '''
def label(text):
    return text
'''

VERIFY_MODULE = '''
from graetl.sdk import get_pipeline

pipeline = get_pipeline()


@pipeline.module(version=1, execution_layer=20)
def verify(ctx, entity):
    row = ctx.db.execute("SELECT label FROM out WHERE id = ?", (entity.id,)).fetchone()
    assert row is not None and row["label"] == entity.id.upper()
'''

VERIFY_TWICE_MODULE = '''
from graetl.sdk import get_pipeline

pipeline = get_pipeline()


@pipeline.module("verify_twice", version=1, execution_layer=30)
def second(ctx, entity):
    ctx.metric("verified_twice", 1)
'''

TWO_IN_ONE = '''
from graetl.sdk import get_pipeline

pipeline = get_pipeline()


@pipeline.module("a", version=1)
def a(ctx, entity):
    pass


@pipeline.module("b", version=1)
def b(ctx, entity):
    pass
'''


class TestModuleFiles(GraetlTestCase):
    def _make(self) -> Path:
        write_pipeline(self.root, "folders", FOLDER_PIPELINE)
        write_module(
            self.root,
            "folders",
            "vitals",
            "load_vital_signals",
            LOAD_MODULE,
            **{
                "load_vital_signals.module.toml": 'title = "Load vital signals"\n'
                'tags = ["clinical"]\nowner = "nik"\n',
                "naming.py": NAMING_HELPER,
                "cleanup.graph": minimal_graph("cleanup", layer=40),
                "helpers.graphlib": minimal_graph("helpers", "function"),
            },
        )
        write_module(self.root, "folders", "checks", "verify", VERIFY_MODULE)
        write_module(self.root, "folders", "checks", "verify_twice", VERIFY_TWICE_MODULE)
        (self.root / "pipelines" / "folders" / "shared.graphlib").write_text(
            minimal_graph("shared", "function"), encoding="utf-8"
        )
        return self.root / "pipelines" / "folders"

    def test_modules_are_discovered_with_metadata_and_assets(self) -> None:
        folder = self._make()
        info = inspect_folder(folder, pipeline_id="folders")
        self.assertTrue(info["ok"], info.get("error"))
        definition = info["definition"]

        modules = {m["name"]: m for m in definition["modules"]}
        # cleanup.graph is a module too - it compiled into cleanup.module.py.
        self.assertEqual(
            set(modules), {"load_vital_signals", "verify", "verify_twice", "cleanup"}
        )
        self.assertEqual(modules["cleanup"]["file"], "modules/vitals/cleanup.module.py")

        # the name comes from the file name; metadata from <name>.module.toml
        load = modules["load_vital_signals"]
        self.assertEqual(load["file"], "modules/vitals/load_vital_signals.module.py")
        self.assertEqual(load["folder"], "modules/vitals")
        self.assertEqual(load["version"], 3)
        self.assertEqual(load["execution_layer"], 10)
        self.assertEqual(load["title"], "Load vital signals")
        self.assertEqual(load["meta"]["owner"], "nik")
        self.assertEqual(load["meta"]["graphs"], ["modules/vitals/cleanup.graph"])
        self.assertEqual(load["meta"]["graphlibs"], ["modules/vitals/helpers.graphlib"])
        self.assertEqual(load["description"], "Docstring description.")

        # two modules can share a folder as long as they are separate files
        self.assertEqual(modules["verify"]["folder"], "modules/checks")
        self.assertEqual(modules["verify_twice"]["folder"], "modules/checks")

        # execution layers imply the requirements (cleanup.graph sits at 40)
        self.assertEqual(definition["layers"], [10, 20, 30, 40])
        self.assertEqual(modules["verify"]["requires"], ["load_vital_signals"])
        self.assertEqual(
            modules["verify_twice"]["requires"], ["load_vital_signals", "verify"]
        )

        self.assertEqual(definition["graphlibs"], ["shared.graphlib"])
        # .graphlib graphs register as pipeline functions alongside hand-written ones
        self.assertEqual(
            [f["name"] for f in definition["functions"]], ["helpers", "shared", "shout"]
        )

        inventory = {f["folder"]: f for f in definition["module_folders"]}
        self.assertEqual(inventory["modules/checks"]["modules"], ["verify", "verify_twice"])

    def test_plain_py_files_are_not_auto_loaded(self) -> None:
        folder = self._make()
        # A stray .py file that would explode if executed at load time.
        (folder / "modules" / "vitals" / "broken_helper.py").write_text(
            "raise RuntimeError('should never be imported')", encoding="utf-8"
        )
        info = inspect_folder(folder, pipeline_id="folders")
        self.assertTrue(info["ok"], info.get("error"))

    def test_one_module_per_file_is_enforced(self) -> None:
        self._make()
        write_module(self.root, "folders", "bad", "two", TWO_IN_ONE)
        info = inspect_folder(self.root / "pipelines" / "folders", pipeline_id="folders")
        self.assertFalse(info["ok"])
        self.assertIn("two.module.py defines 2 modules", info["error"])

    def test_module_file_without_a_module_is_reported(self) -> None:
        self._make()
        write_module(self.root, "folders", "empty", "nothing", "x = 1\n")
        info = inspect_folder(self.root / "pipelines" / "folders", pipeline_id="folders")
        self.assertFalse(info["ok"])
        self.assertIn("defines no module", info["error"])

    def test_each_graph_file_is_a_module_of_its_own(self) -> None:
        self._make()
        # A folder holding only a graph, as in modules/<name>/c.graph
        graph_only = self.root / "pipelines" / "folders" / "modules" / "resample"
        graph_only.mkdir(parents=True)
        (graph_only / "resample.graph").write_text(
            minimal_graph("resample", layer=50), encoding="utf-8"
        )

        definition = inspect_folder(
            self.root / "pipelines" / "folders", pipeline_id="folders"
        )["definition"]
        modules = {m["name"] for m in definition["modules"]}
        self.assertIn("resample", modules)
        self.assertIn("cleanup", modules)

        inventory = {f["folder"]: f for f in definition["module_folders"]}
        self.assertIn("modules/resample", inventory)
        self.assertTrue(
            inventory["modules/resample"]["has_python"],
            "the generated resample.module.py is a module file like any other",
        )
        self.assertFalse(
            inventory["modules/resample"]["pending"], "it compiled, so nothing is pending"
        )
        graphs = {g["name"]: g for g in inventory["modules/resample"]["graph_modules"]}
        self.assertTrue(graphs["resample"]["compiled"])
        self.assertEqual(graphs["resample"]["output"], "modules/resample/resample.module.py")
        self.assertEqual(definition["pending_modules"], [])

    def test_an_uncompiled_graph_is_reported_as_pending(self) -> None:
        """Before `graetl compile` runs, a graph is a module with no code yet."""
        self._make()
        folder = self.root / "pipelines" / "folders"
        (folder / "modules" / "vitals" / "cleanup.module.py").unlink(missing_ok=True)
        loaded = load_pipeline(folder, pipeline_id="folders", compile_graphs=False)
        pending = {m["name"] for m in loaded.pipeline.assets["pending_modules"]}
        self.assertIn("cleanup", pending)

    def test_graph_and_python_module_may_not_share_a_name(self) -> None:
        self._make()
        (self.root / "pipelines" / "folders" / "modules" / "vitals" / "verify.graph").write_text(
            minimal_graph("verify", layer=40), encoding="utf-8"
        )
        info = inspect_folder(self.root / "pipelines" / "folders", pipeline_id="folders")
        self.assertFalse(info["ok"])
        self.assertIn("duplicate module name: 'verify'", info["error"])

    def test_broken_module_file_names_the_file(self) -> None:
        self._make()
        write_module(self.root, "folders", "oops", "oops", "this is not python(")
        info = inspect_folder(self.root / "pipelines" / "folders", pipeline_id="folders")
        self.assertFalse(info["ok"])
        self.assertIn("modules/oops/oops.module.py", info["error"])

    def test_folder_modules_run_end_to_end(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run = client.post("/api/pipelines/folders/runs", json={}).json()
            final = wait_for(
                lambda: (
                    client.get(f"/api/runs/{run['id']}").json()
                    if client.get(f"/api/runs/{run['id']}").json()["status"]
                    in ("succeeded", "failed", "crashed", "stopped")
                    else None
                )
            )
            self.assertEqual(final["status"], "succeeded", final.get("error"))
            # 3 entities x 4 modules (three hand-written, one from cleanup.graph)
            self.assertEqual(final["metrics"]["processed"], 12)
            self.assertEqual(final["metrics"]["counters"]["loaded"], 3)

            steps = {s["step"] for s in client.get(f"/api/runs/{run['id']}/steps").json()}
            self.assertTrue({"load_vital_signals", "verify", "verify_twice"} <= steps)

        with self.settings.state_store("folders") as state:
            rows = dict(state.conn.execute("SELECT id, label FROM out").fetchall())
        self.assertEqual(rows, {"E0": "E0", "E1": "E1", "E2": "E2"})

    def test_create_module_folder_over_the_api(self) -> None:
        scaffold_pipeline(self.settings, "api_made", template="stateful")
        with self.client() as client:
            created = client.post(
                "/api/pipelines/api_made/modules",
                json={"name": "load_vital_signals", "title": "Load vital signals",
                      "execution_layer": 20},
            )
            self.assertEqual(created.status_code, 201, created.text)
            modules = {m["name"]: m for m in created.json()["definition"]["modules"]}
            self.assertIn("load_vital_signals", modules)
            self.assertEqual(modules["load_vital_signals"]["execution_layer"], 20)

            files = [f["path"] for f in client.get("/api/pipelines/api_made/files").json()]
            self.assertIn("modules/load_vital_signals/load_vital_signals.module.py", files)
            self.assertIn("modules/load_vital_signals/load_vital_signals.module.toml", files)

            again = client.post(
                "/api/pipelines/api_made/modules", json={"name": "load_vital_signals"}
            )
            self.assertEqual(again.status_code, 409)

    def test_scaffolded_pipeline_has_a_module_file(self) -> None:
        scaffold_pipeline(self.settings, "fresh", template="stateful")
        module_file = (
            self.root / "pipelines" / "fresh" / "modules" / "process" / "process.module.py"
        )
        self.assertTrue(module_file.exists())
        loaded = load_pipeline(self.root / "pipelines" / "fresh", pipeline_id="fresh")
        self.assertEqual([m.name for m in loaded.pipeline.modules], ["process"])
        self.assertEqual(loaded.pipeline.modules[0].execution_layer, 10)


LAYER_PIPELINE = '''
from graetl.sdk import Entity, Pipeline

pipeline = Pipeline(id="layered", title="Layered", stateful=True)


@pipeline.setup
def setup(ctx):
    ctx.db.execute("CREATE TABLE IF NOT EXISTS hits (id TEXT, module TEXT)")


@pipeline.entities
def discover(ctx):
    for i in range(4):
        yield Entity(id=f"E{i}", source_updated_at="2026-01-01T00:00:00Z")
'''

LAYER_MODULE = '''
from graetl.sdk import get_pipeline

pipeline = get_pipeline()


@pipeline.module(version={version}, execution_layer={layer})
def {name}(ctx, entity):
    ctx.db.execute("INSERT INTO hits (id, module) VALUES (?, ?)", (entity.id, "{name}"))
'''


class TestExecutionLayers(GraetlTestCase):
    def _make(self, *, fluid_version: int = 1) -> None:
        write_pipeline(self.root, "layered", LAYER_PIPELINE)
        write_module(
            self.root, "layered", "intake", "fluid_intake",
            LAYER_MODULE.format(name="fluid_intake", layer=10, version=fluid_version),
        )
        write_module(
            self.root, "layered", "labs", "creatinine",
            LAYER_MODULE.format(name="creatinine", layer=10, version=1),
        )
        write_module(
            self.root, "layered", "scores", "calc_apache_4",
            LAYER_MODULE.format(name="calc_apache_4", layer=20, version=1),
        )

    def _run(self, client, steps=None):
        body = {"steps": steps} if steps else {}
        run = client.post("/api/pipelines/layered/runs", json=body).json()
        return wait_for(
            lambda: (
                client.get(f"/api/runs/{run['id']}").json()
                if client.get(f"/api/runs/{run['id']}").json()["status"]
                in ("succeeded", "failed", "crashed", "stopped")
                else None
            )
        )

    def test_lower_layers_gate_and_bumping_them_cascades(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            first = self._run(client)
            self.assertEqual(first["status"], "succeeded", first.get("error"))
            self.assertEqual(first["metrics"]["processed"], 12)  # 4 entities x 3 modules

            # Nothing left to do.
            self.assertEqual(self._run(client)["metrics"]["processed"], 0)

            # Bump a layer-10 module: it reprocesses, and the layer-20 module that
            # depends on the whole lower layer is refreshed with it.
            self._make(fluid_version=2)
            client.post("/api/pipelines/sync")
            third = self._run(client)
            self.assertEqual(third["status"], "succeeded", third.get("error"))
            self.assertEqual(third["metrics"]["processed"], 8)  # fluid_intake + calc_apache_4

    def test_running_one_module_waits_for_its_lower_layers(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            self._run(client)

            # A stale lower layer: calc_apache_4 alone must not touch those entities.
            self._make(fluid_version=3)
            client.post("/api/pipelines/sync")
            gated = self._run(client, steps=["calc_apache_4"])
            self.assertEqual(gated["status"], "succeeded", gated.get("error"))
            self.assertEqual(gated["metrics"]["processed"], 0)
            self.assertEqual(gated["metrics"]["waiting_for_upstream"], 4)

            # Running everything brings the layer up to date, then the score follows.
            full = self._run(client)
            self.assertEqual(full["metrics"]["processed"], 8)

    def test_depends_on_may_not_point_at_a_higher_layer(self) -> None:
        self._make()
        write_module(
            self.root, "layered", "bad", "backwards",
            'from graetl.sdk import get_pipeline\n'
            "pipeline = get_pipeline()\n\n"
            '@pipeline.module(version=1, execution_layer=10, depends_on=["calc_apache_4"])\n'
            "def backwards(ctx, entity):\n    pass\n",
        )
        info = inspect_folder(self.root / "pipelines" / "layered", pipeline_id="layered")
        self.assertFalse(info["ok"])
        self.assertIn("higher layer", info["error"])


class TestCoreStore(GraetlTestCase):
    def test_reap_stale_runs(self) -> None:
        store = CoreStore(self.root / "etl.db")
        store.upsert_pipeline(pipeline_id="p", title="P", folder="x")
        run = store.create_run(pipeline_id="p")
        store.update_run(run["id"], status="running")
        reaped = store.reap_stale_runs()
        self.assertEqual(reaped, [run["id"]])
        self.assertEqual(store.get_run(run["id"])["status"], "crashed")
        store.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)


PARALLEL_PIPELINE = '''
from graetl.sdk import Entity, Pipeline

pipeline = Pipeline(id="par", title="Par", stateful=True, parallel_modules=4)


@pipeline.setup
def setup(ctx):
    ctx.db.execute(
        "CREATE TABLE IF NOT EXISTS hits ("
        " module TEXT, entity TEXT, worker TEXT, started REAL, ended REAL)"
    )


@pipeline.entities
def discover(ctx):
    for i in range(6):
        yield Entity(id=f"E{i}", source_updated_at="2026-01-01T00:00:00Z")
'''

PARALLEL_MODULE = '''
import threading
import time

from graetl.sdk import get_pipeline

pipeline = get_pipeline()


@pipeline.module(version=1, execution_layer={layer})
def {name}(ctx, entity):
    started = time.time()
    time.sleep(0.12)          # the slow part happens before any write
    ctx.db.execute(
        "INSERT INTO hits (module, entity, worker, started, ended) VALUES (?, ?, ?, ?, ?)",
        ("{name}", entity.id, str(threading.get_ident()), started, time.time()),
    )
'''

BARRIER_MODULE = '''
from graetl.sdk import get_pipeline

pipeline = get_pipeline()


@pipeline.module(version=1, execution_layer=20)
def summary(ctx, entity):
    done = ctx.db.execute(
        "SELECT COUNT(DISTINCT module) FROM hits WHERE entity = ?", (entity.id,)
    ).fetchone()[0]
    if done != 3:
        raise AssertionError(f"layer 20 ran with only {done}/3 layer-10 modules done")
    ctx.metric("summarised", 1)
'''


class TestParallelExecution(GraetlTestCase):
    def _make(self, *, with_barrier: bool = False) -> None:
        write_pipeline(self.root, "par", PARALLEL_PIPELINE)
        for name in ("alpha", "beta", "gamma"):
            write_module(
                self.root, "par", name, name, PARALLEL_MODULE.format(name=name, layer=10)
            )
        if with_barrier:
            write_module(self.root, "par", "summary", "summary", BARRIER_MODULE)

    def _run(self, client, **body):
        run = client.post("/api/pipelines/par/runs", json=body).json()
        return wait_for(
            lambda: (
                client.get(f"/api/runs/{run['id']}").json()
                if client.get(f"/api/runs/{run['id']}").json()["status"]
                in ("succeeded", "failed", "crashed", "stopped")
                else None
            ),
            timeout=60,
        )

    def _hits(self):
        with self.settings.state_store("par") as state:
            return [dict(r) for r in state.conn.execute("SELECT * FROM hits").fetchall()]

    def test_modules_of_a_layer_overlap_but_each_runs_single_threaded(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            final = self._run(client, parallel=3)
            self.assertEqual(final["status"], "succeeded", final.get("error"))
            self.assertEqual(final["metrics"]["processed"], 18)  # 6 entities x 3 modules
            self.assertEqual(final["metrics"]["failed"], 0)

        hits = self._hits()
        self.assertEqual(len(hits), 18)

        by_module: dict[str, list[dict]] = {}
        for hit in hits:
            by_module.setdefault(hit["module"], []).append(hit)
        self.assertEqual(set(by_module), {"alpha", "beta", "gamma"})
        for name, rows in by_module.items():
            self.assertEqual(len(rows), 6, name)
            # one worker per module: never two threads inside the same module
            self.assertEqual(len({r["worker"] for r in rows}), 1, f"{name} used >1 worker")
        # ... and different modules used different workers
        self.assertEqual(len({r["worker"] for r in hits}), 3)

        # they really ran at the same time
        overlapped = any(
            a["module"] != b["module"] and a["started"] < b["ended"] and b["started"] < a["ended"]
            for a in hits
            for b in hits
        )
        self.assertTrue(overlapped, "parallel modules never overlapped in time")

    def test_sequential_run_produces_the_same_state(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            final = self._run(client, parallel=1)
            self.assertEqual(final["status"], "succeeded", final.get("error"))
            self.assertEqual(final["metrics"]["processed"], 18)
        self.assertEqual(len(self._hits()), 18)
        with self.settings.state_store("par") as state:
            self.assertEqual(state.status_counts(), {"done": 18})
            self.assertEqual(state.list_module_locks(), [])  # every lock released

    def test_layers_are_barriers_even_when_parallel(self) -> None:
        self._make(with_barrier=True)
        with self.client() as client:
            client.post("/api/pipelines/sync")
            final = self._run(client, parallel=4)
            self.assertEqual(final["status"], "succeeded", final.get("error"))
            self.assertEqual(final["metrics"]["counters"]["summarised"], 6)

    def test_nothing_is_processed_twice_when_a_run_repeats(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            self._run(client, parallel=3)
            again = self._run(client, parallel=3)
            self.assertEqual(again["metrics"]["processed"], 0)
        self.assertEqual(len(self._hits()), 18)


class TestModuleLocks(GraetlTestCase):
    def test_second_worker_is_refused_and_a_dead_one_is_taken_over(self) -> None:
        from graetl.store.state import LockConflict

        path = self.warehouse
        with StateStore(path) as first, StateStore(path) as second:
            owner = first.acquire_module_lock("calc", run_id=1)
            with self.assertRaises(LockConflict):
                second.acquire_module_lock("calc", run_id=2)

            # A worker that stopped beating is considered dead and taken over.
            first.conn.execute(
                "UPDATE [[module_locks]] SET heartbeat_at = '2000-01-01T00:00:00.000Z' "
                "WHERE module = 'calc'"
            )
            taken = second.acquire_module_lock("calc", run_id=2)
            self.assertNotEqual(taken, owner)
            self.assertEqual(second.list_module_locks()[0]["run_id"], 2)

    def test_lock_is_released_after_the_context_exits(self) -> None:
        path = self.warehouse
        with StateStore(path) as store:
            with store.module_lock("m", run_id=1):
                self.assertEqual(len(store.list_module_locks()), 1)
            self.assertEqual(store.list_module_locks(), [])

    def test_contention_never_marks_an_entity_failed(self) -> None:
        """A blocked writer rolls back cleanly instead of recording a failure."""
        from graetl.store.state import LockConflict

        path = self.warehouse
        with StateStore(path) as writer, StateStore(path) as blocker:
            writer.conn.execute("CREATE TABLE t (id TEXT)")
            writer.upsert_entities([("A", None, None, {})])
            writer.conn.execute("PRAGMA busy_timeout = 150")
            blocker.begin("immediate")  # hold the write lock
            blocker.conn.execute("INSERT INTO t VALUES ('held')")
            with self.assertRaises(LockConflict):
                with writer.entity_transaction(
                    "A", "m", 1, run_id=1, source_updated_at=None, mode="deferred"
                ):
                    writer.conn.execute("INSERT INTO t VALUES ('A')")
            blocker.rollback()
            # No state row was written at all - the entity is simply still to do.
            self.assertEqual(writer.states_for(["A"]).get("A"), None)
            self.assertEqual(len(writer.select_work("m", 1)), 1)


BROWSE_PIPELINE = '''
from graetl.sdk import Entity, Pipeline

pipeline = Pipeline(id="browse", title="Browse", stateful=True)


@pipeline.setup
def setup(ctx):
    ctx.db.execute("CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY)")


@pipeline.entities
def discover(ctx):
    for i in range(40):
        yield Entity(
            id=f"E{i:03d}",
            label=f"Patient {i} ward {i % 4}",
            source_updated_at="2026-01-01T00:00:00Z",
        )
'''

NOISY_MODULE = '''
from graetl.sdk import get_pipeline

pipeline = get_pipeline()


@pipeline.module(version=1, execution_layer=10)
def touch(ctx, entity):
    ctx.debug(f"inspecting {entity.id} closely")
    ctx.info("touched")
    ctx.db.execute("INSERT OR REPLACE INTO seen (id) VALUES (?)", (entity.id,))
'''


class TestEntityBrowsing(GraetlTestCase):
    def _make(self) -> None:
        write_pipeline(self.root, "browse", BROWSE_PIPELINE)
        write_module(self.root, "browse", "touch", "touch", NOISY_MODULE)

    def _run(self, client, **body):
        run = client.post("/api/pipelines/browse/runs", json=body).json()
        final = wait_for(
            lambda: (
                client.get(f"/api/runs/{run['id']}").json()
                if client.get(f"/api/runs/{run['id']}").json()["status"]
                in ("succeeded", "failed", "crashed", "stopped")
                else None
            ),
            timeout=60,
        )
        return run["id"], final

    def test_paging_search_and_filters(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            self._run(client)

            page = client.get("/api/pipelines/browse/entities?limit=10&offset=0").json()
            self.assertEqual(page["total"], 40)
            self.assertEqual(page["matching"], 40)
            self.assertEqual(len(page["entities"]), 10)
            self.assertEqual(page["entities"][0]["entity_id"], "E000")

            second = client.get("/api/pipelines/browse/entities?limit=10&offset=10").json()
            self.assertEqual(second["entities"][0]["entity_id"], "E010")

            # search narrows both the rows and the count used for paging
            found = client.get("/api/pipelines/browse/entities?search=ward+3").json()
            self.assertEqual(found["total"], 40)
            self.assertEqual(found["matching"], 10)
            self.assertTrue(all("ward 3" in e["label"] for e in found["entities"]))

            # filter by module state
            done = client.get(
                "/api/pipelines/browse/entities?status=done&module=touch"
            ).json()
            self.assertEqual(done["matching"], 40)
            none = client.get("/api/pipelines/browse/entities?status=failed").json()
            self.assertEqual(none["matching"], 0)

            self.assertEqual(done["statuses"], {"done": 40})

    def test_entity_detail_and_reset(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            self._run(client)

            entity = client.get("/api/pipelines/browse/entities/E005").json()
            self.assertEqual(entity["entity_id"], "E005")
            self.assertEqual(entity["modules"][0]["module"], "touch")
            self.assertEqual(entity["modules"][0]["status"], "done")

            reset = client.post(
                "/api/pipelines/browse/entities/E005/reset", json={"module": "touch"}
            ).json()
            self.assertEqual(reset["reset"], 1)
            self.assertEqual(reset["entity"]["modules"], [])

            # ... and only that entity is due again
            _, final = self._run(client)
            self.assertEqual(final["metrics"]["processed"], 1)

    def test_debug_output_only_reaches_a_debug_run(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run_id, final = self._run(client)
            self.assertEqual(final["status"], "succeeded", final.get("error"))
            console = client.get(f"/api/runs/{run_id}/console?limit=5000").json()["events"]
            messages = [e.get("message", "") for e in console if e.get("kind") == "log"]
            self.assertTrue(any("touched" in m for m in messages))
            self.assertFalse(any("inspecting" in m for m in messages), "debug leaked")

            debug_id, debug_final = self._run(
                client, mode="full", steps=["touch"], limit_entities=2, debug=True
            )
            self.assertEqual(debug_final["status"], "succeeded", debug_final.get("error"))
            self.assertEqual(debug_final["metrics"]["processed"], 2)  # the sample size
            debug_console = client.get(f"/api/runs/{debug_id}/console?limit=5000").json()["events"]
            debug_messages = [
                e.get("message", "") for e in debug_console if e.get("level") == "debug"
            ]
            self.assertTrue(
                any("inspecting" in m for m in debug_messages),
                "debug output missing from a debug run",
            )

    def test_profile_run_over_a_sample(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run_id, final = self._run(
                client, mode="full", steps=["touch"], limit_entities=5,
                sample="random", profile=True,
            )
            self.assertEqual(final["status"], "succeeded", final.get("error"))
            self.assertEqual(final["metrics"]["processed"], 5)
            console = client.get(f"/api/runs/{run_id}/console?limit=5000").json()["events"]
            self.assertTrue(any(e.get("kind") == "profile" for e in console))
            profiles = list((self.settings.profiles_dir("browse")).glob("*.prof"))
            self.assertTrue(profiles, "no profile file was written")

    def test_telemetry_reaches_the_console_and_the_run_row(self) -> None:
        write_pipeline(self.root, "slow", SLOW_PIPELINE)
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run = client.post("/api/pipelines/slow/runs", json={}).json()
            sample = wait_for(
                lambda: next(
                    (
                        e
                        for e in client.get(f"/api/runs/{run['id']}/console?limit=500")
                        .json()["events"]
                        if e.get("kind") == "resource"
                    ),
                    None,
                ),
                timeout=30,
            )
            self.assertIsNotNone(sample, "no resource sample was emitted")
            self.assertIn("rss_mb", sample)
            self.assertIn("cpu_percent", sample)
            self.assertGreaterEqual(sample["rss_mb"], 1)
            client.post(f"/api/runs/{run['id']}/stop?force=true")


class TestFileEditing(GraetlTestCase):
    def test_create_check_and_save_with_syntax_validation(self) -> None:
        scaffold_pipeline(self.settings, "edit_me", template="stateful")
        with self.client() as client:
            created = client.post(
                "/api/pipelines/edit_me/file/new",
                json={"path": "modules/process/helpers.py", "content": "VALUE = 1\n"},
            )
            self.assertEqual(created.status_code, 201)
            files = [f["path"] for f in client.get("/api/pipelines/edit_me/files").json()]
            self.assertIn("modules/process/helpers.py", files)

            again = client.post(
                "/api/pipelines/edit_me/file/new",
                json={"path": "modules/process/helpers.py"},
            )
            self.assertEqual(again.status_code, 409)

            # the check endpoint reports the line without saving anything
            check = client.post(
                "/api/pipelines/edit_me/file/check?path=modules/process/helpers.py",
                json={"content": "def broken(:\n    pass\n"},
            ).json()
            self.assertFalse(check["ok"])
            self.assertEqual(check["error"]["line"], 1)

            # saving broken python is refused, and the file is untouched
            bad = client.put(
                "/api/pipelines/edit_me/file?path=modules/process/helpers.py",
                json={"content": "def broken(:\n    pass\n"},
            )
            self.assertEqual(bad.status_code, 422)
            self.assertIn("syntax_error", bad.json()["detail"])
            content = client.get(
                "/api/pipelines/edit_me/file?path=modules/process/helpers.py"
            ).json()["content"]
            self.assertEqual(content, "VALUE = 1\n")

            good = client.put(
                "/api/pipelines/edit_me/file?path=modules/process/helpers.py",
                json={"content": "VALUE = 2\n"},
            )
            self.assertEqual(good.status_code, 200)
            self.assertEqual(good.json()["saved"], "modules/process/helpers.py")

    def test_hidden_and_runtime_folders_stay_out_of_the_file_list(self) -> None:
        scaffold_pipeline(self.settings, "tidy", template="stateful")
        base = self.root / "pipelines" / "tidy"
        (base / ".git").mkdir()
        (base / ".git" / "HEAD").write_text("ref: refs/heads/main", encoding="utf-8")
        (base / "logs").mkdir(exist_ok=True)
        (base / "logs" / "run_000001.jsonl").write_text("{}", encoding="utf-8")
        with self.client() as client:
            paths = [f["path"] for f in client.get("/api/pipelines/tidy/files").json()]
        self.assertFalse([p for p in paths if p.startswith(".git")], paths)
        self.assertFalse([p for p in paths if p.startswith("logs")], paths)
        self.assertIn("pipeline.py", paths)


class TestEditorAssets(GraetlTestCase):
    """The Files tab must work whatever the machine can reach.

    Monaco is preferred, from a vendored copy first and a CDN second, but the
    UI ships a fallback editor so an air-gapped install still edits code.
    """

    def test_health_reports_where_the_editor_comes_from(self) -> None:
        with self.client() as client:
            health = client.get("/api/health").json()
        self.assertIn("monaco_url", health)
        self.assertIn("monaco_vendored", health)
        self.assertIsInstance(health["monaco_vendored"], bool)

    def test_monaco_url_is_configurable_and_can_be_switched_off(self) -> None:
        (self.root / "graetl.toml").write_text(
            '[ui]\nmonaco_url = ""\n', encoding="utf-8"
        )
        self.assertEqual(load_settings(self.root).monaco_url, "")

        (self.root / "graetl.toml").write_text(
            '[ui]\nmonaco_url = "http://intranet/monaco/vs/"\n', encoding="utf-8"
        )
        self.assertEqual(load_settings(self.root).monaco_url, "http://intranet/monaco/vs")

    def test_editor_modules_are_served(self) -> None:
        with self.client() as client:
            for path in ("/editor.js", "/fallback-editor.js", "/app.js", "/app.css"):
                response = client.get(path)
                self.assertEqual(response.status_code, 200, path)
                self.assertTrue(response.text.strip(), path)
            self.assertIn("createFallbackEditor", client.get("/editor.js").text)

    def test_vendor_folder_is_served_when_it_has_content(self) -> None:
        from graetl.server.app import VENDOR_DIR

        marker = VENDOR_DIR / "vs" / "__probe__.js"
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("// probe\n", encoding="utf-8")
        try:
            with self.client() as client:
                response = client.get("/vendor/vs/__probe__.js")
            self.assertEqual(response.status_code, 200)
            self.assertIn("probe", response.text)
        finally:
            marker.unlink()
            if not any(marker.parent.iterdir()):
                marker.parent.rmdir()

    def test_a_single_module_state_can_be_reset_over_the_api(self) -> None:
        """The right-click "reset this module's state" action."""
        write_pipeline(self.root, "browse", BROWSE_PIPELINE)
        write_module(self.root, "browse", "touch", "touch", NOISY_MODULE)
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run = client.post("/api/pipelines/browse/runs", json={}).json()
            wait_for(
                lambda: client.get(f"/api/runs/{run['id']}").json()
                if client.get(f"/api/runs/{run['id']}").json()["status"] == "succeeded"
                else None,
                timeout=60,
            )
            before = client.get("/api/pipelines/browse/entities?limit=1").json()
            self.assertEqual(before["statuses"], {"done": 40})

            reset = client.post(
                "/api/pipelines/browse/state/reset", json={"module": "touch"}
            )
            self.assertEqual(reset.status_code, 200)
            self.assertEqual(reset.json()["reset"], 40)

            after = client.get("/api/pipelines/browse/entities?limit=1").json()
            self.assertEqual(after["total"], 40, "entities themselves are kept")
            self.assertEqual(after["statuses"], {}, "module state is gone")


# --------------------------------------------------------------------- graphs

GRAPH_PIPELINE = '''
from graetl.sdk import Entity, Pipeline

pipeline = Pipeline(id="graphs", title="Graphs", stateful=True)


@pipeline.function("band", pure=True, category="Demo")
def band(ctx, score: int) -> str:
    return "high" if score >= 50 else "low"


@pipeline.function("save")
def save(ctx, entity_id: str, band: str) -> None:
    ctx.db.execute(
        "INSERT OR REPLACE INTO scored (id, band) VALUES (?, ?)", (entity_id, band)
    )


@pipeline.setup
def setup(ctx):
    ctx.db.execute("CREATE TABLE IF NOT EXISTS scored (id TEXT PRIMARY KEY, band TEXT)")


@pipeline.entities
def entities(ctx):
    for i in range(1, 5):
        yield Entity(id=f"E{i}", source_updated_at="2026-01-01T00:00:00Z",
                     data={"score": i * 25, "values": [i, i * 2]})
'''


def simple_graph(**over):
    """entity -> band -> branch -> save, with a loop and a pure sum."""
    graph = {
        "graetl_graph": 1, "kind": "module", "name": "scoring",
        "title": "Scoring", "description": "Band each entity.",
        "version": 1, "execution_layer": 10,
        "variables": [{"name": "total", "type": "int", "default": 0}],
        "nodes": [
            {"id": "entry", "op": "core:entry", "pos": [0, 200]},
            {"id": "data", "op": "core:get_attr", "config": {"name": "data"}, "pos": [180, 300]},
            {"id": "score", "op": "core:get_item", "config": {"safe": True},
             "values": {"key": "score", "default": 0}, "pos": [340, 300]},
            {"id": "band", "op": "fn:band", "pos": [520, 300]},
            {"id": "eid", "op": "core:get_attr", "config": {"name": "id"}, "pos": [180, 120]},
            {"id": "vals", "op": "core:get_item", "config": {"safe": True},
             "values": {"key": "values", "default": []}, "pos": [340, 460]},
            {"id": "loop", "op": "core:for_each", "pos": [520, 460], "title": "value"},
            {"id": "cur", "op": "core:get_var", "config": {"name": "total"}, "pos": [700, 540]},
            {"id": "add", "op": "core:binary_op", "config": {"op": "add"}, "pos": [860, 540]},
            {"id": "set", "op": "core:set_var", "config": {"name": "total"}, "pos": [1020, 460]},
            {"id": "is_high", "op": "core:binary_op", "config": {"op": "eq"},
             "values": {"b": "high"}, "pos": [700, 300]},
            {"id": "br", "op": "core:branch", "pos": [880, 200]},
            {"id": "log", "op": "core:log", "config": {"level": "info"},
             "values": {"message": "high scorer"}, "pos": [1060, 140]},
            {"id": "save", "op": "fn:save", "pos": [1240, 200]},
            {"id": "ret", "op": "core:return", "pos": [1420, 200]},
        ],
        "links": [
            {"from": ["entry", "then"], "to": ["loop", "exec"]},
            {"from": ["entry", "entity"], "to": ["data", "object"]},
            {"from": ["entry", "entity"], "to": ["eid", "object"]},
            {"from": ["data", "value"], "to": ["score", "container"]},
            {"from": ["data", "value"], "to": ["vals", "container"]},
            {"from": ["score", "value"], "to": ["band", "score"]},
            {"from": ["vals", "value"], "to": ["loop", "iterable"]},
            {"from": ["loop", "body"], "to": ["set", "exec"]},
            {"from": ["cur", "value"], "to": ["add", "a"]},
            {"from": ["loop", "item"], "to": ["add", "b"]},
            {"from": ["add", "result"], "to": ["set", "value"]},
            {"from": ["loop", "completed"], "to": ["br", "exec"]},
            {"from": ["band", "result"], "to": ["is_high", "a"]},
            {"from": ["is_high", "result"], "to": ["br", "condition"]},
            {"from": ["br", "true"], "to": ["log", "exec"]},
            {"from": ["log", "then"], "to": ["save", "exec"]},
            {"from": ["br", "false"], "to": ["save", "exec"]},
            {"from": ["eid", "value"], "to": ["save", "entity_id"]},
            {"from": ["band", "result"], "to": ["save", "band"]},
            {"from": ["save", "then"], "to": ["ret", "exec"]},
            {"from": ["band", "result"], "to": ["ret", "value"]},
        ],
        "comments": [
            {"id": "c1", "text": "Sum the values.", "pos": [500, 420], "size": [660, 220]}
        ],
    }
    graph.update(over)
    return graph


def write_graph(root: Path, pid: str, name: str, payload: dict, folder: str = "") -> Path:
    base = root / "pipelines" / pid / (folder or "")
    base.mkdir(parents=True, exist_ok=True)
    path = base / name
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


class TestGraphCompiler(GraetlTestCase):
    """A graph is source; the Python beside it is the build output."""

    def _make(self, graph: dict | None = None) -> Path:
        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        return write_graph(
            self.root, "graphs", "scoring.graph", graph or simple_graph(),
            folder="modules/scoring",
        )

    def test_a_graph_compiles_to_readable_python_that_runs(self) -> None:
        from graetl.graph.build import build_pipeline_graphs

        source_path = self._make()
        report = build_pipeline_graphs(self.settings.pipeline_dir("graphs"))
        self.assertTrue(report.ok, report.summary())
        target = source_path.with_name("scoring.module.py")
        self.assertTrue(target.exists())

        code = target.read_text(encoding="utf-8")
        # It is real Python, and it is the shape a person would have written.
        compile(code, str(target), "exec")
        self.assertIn("@pipeline.module(version=1, execution_layer=10)", code)
        self.assertIn("def scoring(ctx, entity):", code)
        self.assertIn("for value in ", code)          # core:for_each -> a real for
        self.assertIn("if band == ", code)            # core:branch -> a real if
        self.assertIn("total = total + value", code)  # variables are locals
        self.assertIn("# Sum the values.", code)      # comment boxes carry over
        self.assertIn("graetl:generated-from scoring.graph", code)
        # entity.id is shared by two branches, so it is computed once, above them.
        self.assertEqual(code.count("entity.id"), 1)

        with self.client() as client:
            client.post("/api/pipelines/sync")
            run = client.post("/api/pipelines/graphs/runs", json={}).json()
            final = wait_for(
                lambda: client.get(f"/api/runs/{run['id']}").json()
                if client.get(f"/api/runs/{run['id']}").json()["status"] == "succeeded"
                else None,
                timeout=60,
            )
        self.assertIsNotNone(final, "the graph-defined pipeline did not succeed")
        self.assertEqual(final["metrics"]["processed"], 4)

        state = sqlite3.connect(self.warehouse)
        rows = dict(state.execute("SELECT id, band FROM scored").fetchall())
        state.close()
        self.assertEqual(rows, {"E1": "low", "E2": "high", "E3": "high", "E4": "high"})

    def test_compiling_is_idempotent_and_skips_unchanged_graphs(self) -> None:
        from graetl.graph.build import build_pipeline_graphs

        self._make()
        folder = self.settings.pipeline_dir("graphs")
        first = build_pipeline_graphs(folder)
        self.assertEqual([b.status for b in first.builds], ["compiled"])
        again = build_pipeline_graphs(folder)
        self.assertEqual([b.status for b in again.builds], ["current"])

    def test_editing_a_graph_recompiles_on_the_next_load(self) -> None:
        path = self._make()
        loader.load_pipeline(self.settings.pipeline_dir("graphs"), pipeline_id="graphs")
        target = path.with_name("scoring.module.py")
        self.assertIn("execution_layer=10", target.read_text(encoding="utf-8"))

        graph = json.loads(path.read_text(encoding="utf-8"))
        graph["execution_layer"] = 30
        graph["version"] = 2
        path.write_text(json.dumps(graph), encoding="utf-8")

        loaded = loader.load_pipeline(self.settings.pipeline_dir("graphs"), pipeline_id="graphs")
        code = target.read_text(encoding="utf-8")
        self.assertIn("execution_layer=30", code)
        self.assertIn("version=2", code)
        module = next(m for m in loaded.pipeline.modules if m.name == "scoring")
        self.assertEqual(module.execution_layer, 30)
        self.assertEqual(module.version, 2)

    def test_a_generated_file_is_never_written_over_hand_written_code(self) -> None:
        from graetl.graph.build import build_pipeline_graphs

        path = self._make()
        hand = path.with_name("scoring.module.py")
        hand.write_text("# mine\n", encoding="utf-8")
        report = build_pipeline_graphs(self.settings.pipeline_dir("graphs"))
        self.assertFalse(report.ok)
        self.assertIn("refusing to overwrite", report.builds[0].error or "")
        self.assertEqual(hand.read_text(encoding="utf-8"), "# mine\n")

    def test_the_three_node_sources_all_resolve(self) -> None:
        """A decorated function, a function graph and true reflection."""
        from graetl.graph.model import Node
        from graetl.graph.registry import NodeRegistry
        from graetl.loader import load_entry_only

        self._make()
        pipeline = load_entry_only(self.settings.pipeline_dir("graphs"), pipeline_id="graphs")
        registry = NodeRegistry(functions=dict(pipeline.functions))

        decorated = registry.resolve(Node(id="a", op="fn:band"))
        self.assertTrue(decorated.pure, "pure=True on the decorator reaches the node")
        self.assertEqual([p.name for p in decorated.inputs(data_only=True)], ["score"])
        self.assertEqual(decorated.category, "Demo")
        self.assertFalse(
            any(p.name == "ctx" for p in decorated.pins), "ctx is implicit, never a pin"
        )

        reflected = registry.resolve(Node(id="b", op="py:math.floor"))
        self.assertTrue(reflected.pure, "math is a known side-effect-free module")
        self.assertEqual(reflected.imports, ("math",))
        self.assertTrue(reflected.outputs(data_only=True))

        impure = registry.resolve(Node(id="c", op="fn:save"))
        self.assertFalse(impure.pure)
        self.assertTrue(any(p.is_exec for p in impure.pins), "an impure node has exec pins")

        from graetl.graph.model import GraphError as GErr

        with self.assertRaises(GErr) as caught:
            registry.resolve(Node(id="d", op="fn:not_registered"))
        self.assertIn("not_registered", str(caught.exception))

    def test_reflection_can_be_restricted(self) -> None:
        from graetl.graph.model import GraphError as GErr
        from graetl.graph.model import Node
        from graetl.graph.registry import NodeRegistry

        registry = NodeRegistry(reflect_allow=["math"])
        self.assertTrue(registry.resolve(Node(id="a", op="py:math.floor")).pure)
        with self.assertRaises(GErr) as caught:
            registry.resolve(Node(id="b", op="py:shutil.rmtree"))
        self.assertIn("reflect_allow", str(caught.exception))

    def test_a_function_graph_becomes_a_pipeline_function(self) -> None:
        from graetl.graph.build import build_pipeline_graphs

        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        write_graph(self.root, "graphs", "double.graphlib", {
            "graetl_graph": 1, "kind": "function", "name": "double",
            "description": "Twice the number.", "pure": True,
            "inputs": [{"name": "number", "type": "float"}],
            "outputs": [{"name": "doubled", "type": "float"}],
            "nodes": [
                {"id": "entry", "op": "core:entry", "pos": [0, 0]},
                {"id": "mul", "op": "core:binary_op", "config": {"op": "mul"},
                 "values": {"b": 2}, "pos": [200, 60]},
                {"id": "ret", "op": "core:return", "pos": [400, 0]},
            ],
            "links": [
                {"from": ["entry", "then"], "to": ["ret", "exec"]},
                {"from": ["entry", "number"], "to": ["mul", "a"]},
                {"from": ["mul", "result"], "to": ["ret", "doubled"]},
            ],
        })
        folder = self.settings.pipeline_dir("graphs")
        report = build_pipeline_graphs(folder)
        self.assertTrue(report.ok, report.summary())
        generated = (folder / "double.graphlib.py").read_text(encoding="utf-8")
        self.assertIn('@pipeline.function("double"', generated)
        self.assertIn("def double(ctx, number):", generated)
        self.assertIn("return number * 2", generated)

        loaded = loader.load_pipeline(folder, pipeline_id="graphs")
        self.assertIn("double", loaded.pipeline.functions)

    def test_scope_errors_name_the_node_and_pin(self) -> None:
        """Reading a loop variable after the loop is a compile error, not a crash."""
        from graetl.graph.build import build_pipeline_graphs

        graph = simple_graph()
        # feed the loop item into a node that runs after the loop
        graph["links"].append({"from": ["loop", "item"], "to": ["ret", "value"]})
        graph["links"] = [
            link for link in graph["links"]
            if not (link["from"] == ["band", "result"] and link["to"] == ["ret", "value"])
        ]
        self._make(graph)
        report = build_pipeline_graphs(self.settings.pipeline_dir("graphs"))
        self.assertFalse(report.ok)
        self.assertIn("only exists inside", report.builds[0].error or "")

    def test_one_execution_pin_cannot_drive_two_nodes(self) -> None:
        from graetl.graph.build import build_pipeline_graphs

        graph = simple_graph()
        graph["links"].append({"from": ["entry", "then"], "to": ["save", "exec"]})
        self._make(graph)
        report = build_pipeline_graphs(self.settings.pipeline_dir("graphs"))
        self.assertFalse(report.ok)
        self.assertIn("Sequence", report.builds[0].error or "")

    def test_the_graph_api_serves_the_canvas_and_the_python(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            listing = client.get("/api/pipelines/graphs/graphs").json()
            self.assertTrue(listing["ok"])
            self.assertEqual(listing["graphs"][0]["name"], "scoring")

            document = client.get(
                "/api/pipelines/graphs/graph?path=modules/scoring/scoring.graph"
            ).json()
            self.assertEqual(document["graph"]["name"], "scoring")
            self.assertEqual(document["problems"], [])
            self.assertIn("entry", document["definitions"])
            self.assertEqual(document["output"], "modules/scoring/scoring.module.py")

            preview = client.get(
                "/api/pipelines/graphs/graph/preview?path=modules/scoring/scoring.graph"
            ).json()
            self.assertTrue(preview["ok"])
            self.assertIn("def scoring(ctx, entity):", preview["source"])

            catalog = client.get("/api/pipelines/graphs/nodes").json()["nodes"]
            ops = {n["op"] for n in catalog}
            self.assertIn("core:branch", ops)
            self.assertIn("fn:band", ops)

    def test_saving_a_graph_over_the_api_compiles_it(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            path = "modules/scoring/scoring.graph"
            document = client.get(f"/api/pipelines/graphs/graph?path={path}").json()["graph"]
            document["title"] = "Renamed"
            document["version"] = 7
            saved = client.put(
                f"/api/pipelines/graphs/graph?path={path}", json={"graph": document}
            )
            self.assertEqual(saved.status_code, 200)
            self.assertTrue(saved.json()["build"]["ok"])
        code = (
            self.settings.pipeline_dir("graphs") / "modules/scoring/scoring.module.py"
        ).read_text(encoding="utf-8")
        self.assertIn("version=7", code)

    def test_a_broken_graph_is_rejected_with_the_node_named(self) -> None:
        graph = simple_graph()
        graph["nodes"].append({"id": "oops", "op": "fn:no_such_function", "pos": [0, 0]})
        self._make(graph)
        with self.client() as client:
            client.post("/api/pipelines/sync")
            info = client.get("/api/pipelines").json()
        broken = [p for p in info if p["id"] == "graphs"]
        self.assertTrue(broken, "the pipeline should still be listed")
        self.assertIn("no_such_function", broken[0]["definition_error"] or "")


TREE_MODULE = '''
from graetl.sdk import get_pipeline

pipeline = get_pipeline()


@pipeline.module(version=1, execution_layer=10)
def scoring(ctx, entity):
    """Writes into the table GRAPH_PIPELINE's setup creates."""
    ctx.db.execute(
        "INSERT OR REPLACE INTO scored (id, band) VALUES (?, ?)",
        (entity.id, ctx.fn("band")(ctx, entity.data["score"])),
    )
'''


class TestGraphEditor(GraetlTestCase):
    """What the canvas asks the server for while a graph is being edited."""

    def _pipeline(self) -> None:
        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        write_graph(
            self.root, "graphs", "scoring.graph", simple_graph(), folder="modules/scoring"
        )

    def test_the_palette_offers_every_node_source(self) -> None:
        self._pipeline()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            catalog = client.get("/api/pipelines/graphs/nodes").json()["nodes"]
        ops = {entry["op"] for entry in catalog}
        self.assertIn("core:branch", ops, "built-ins")
        self.assertIn("fn:band", ops, "@pipeline.function")
        # Every entry carries what the palette shows and what placing it needs.
        for entry in catalog:
            self.assertTrue(entry["title"])
            self.assertIn("pins", entry)
            self.assertIn("category", entry)
        # One operator node, many faces: each one is offered separately.
        symbols = {e["meta"].get("symbol") for e in catalog if e["op"] == "core:binary_op"}
        self.assertIn("+", symbols)

    def test_one_op_can_be_described_for_a_node_that_is_not_placed_yet(self) -> None:
        """Dropping a reflected node needs its shape before it exists."""
        self._pipeline()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            floor = client.get("/api/pipelines/graphs/nodes?op=py:math.floor").json()
            self.assertEqual(floor["op"], "py:math.floor")
            self.assertTrue(floor["pure"])
            self.assertTrue([p for p in floor["pins"] if p["direction"] == "out"])

            # Config is what decides a parametric node's shape.
            attr = client.get(
                '/api/pipelines/graphs/nodes?op=core:get_attr&config={"name": "severity"}'
            ).json()
            self.assertEqual(attr["title"], ".severity")

            bad = client.get("/api/pipelines/graphs/nodes?op=py:no.such.thing")
            self.assertEqual(bad.status_code, 422)

    def test_preview_compiles_the_editors_unsaved_document(self) -> None:
        self._pipeline()
        document = simple_graph()
        document["title"] = "Renamed in the editor"
        with self.client() as client:
            client.post("/api/pipelines/sync")
            res = client.post(
                "/api/pipelines/graphs/graph/preview?path=modules/scoring/scoring.graph",
                json={"graph": document},
            ).json()
            self.assertTrue(res["ok"], res.get("error"))
            self.assertIn("Renamed in the editor", res["source"])
            # Nothing was written: the file on disk still says what it said.
            on_disk = client.get(
                "/api/pipelines/graphs/graph/preview?path=modules/scoring/scoring.graph"
            ).json()
            self.assertNotIn("Renamed in the editor", on_disk["source"])

    def test_a_broken_document_is_reported_not_raised(self) -> None:
        """The editor previews on every keystroke; it must never get a 500."""
        self._pipeline()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            res = client.post(
                "/api/pipelines/graphs/graph/preview?path=modules/scoring/scoring.graph",
                json={"graph": {"graetl_graph": 1, "kind": "module", "name": "scoring",
                                "nodes": [{"id": "x", "op": "nonsense:thing", "pos": [0, 0]}],
                                "links": []}},
            )
            self.assertEqual(res.status_code, 200)
            self.assertFalse(res.json()["ok"])
            self.assertTrue(res.json()["error"])

    def test_saving_from_the_canvas_writes_and_recompiles(self) -> None:
        self._pipeline()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            loaded = client.get(
                "/api/pipelines/graphs/graph?path=modules/scoring/scoring.graph"
            ).json()
            self.assertTrue(loaded["definitions"], "every node resolves for the canvas")
            self.assertFalse(loaded["problems"])

            document = loaded["graph"]
            # Exactly what dragging a node produces.
            document["nodes"][0]["pos"] = [64, 320]
            document["comments"] = [
                {"id": "c1", "text": "Scoring", "pos": [0, 0], "size": [400, 240]}
            ]
            saved = client.put(
                "/api/pipelines/graphs/graph?path=modules/scoring/scoring.graph",
                json={"graph": document, "compile": True},
            ).json()
            self.assertTrue(saved["build"]["ok"], saved["build"])

            again = client.get(
                "/api/pipelines/graphs/graph?path=modules/scoring/scoring.graph"
            ).json()
            self.assertEqual(again["graph"]["nodes"][0]["pos"], [64, 320])
            self.assertEqual(again["graph"]["comments"][0]["text"], "Scoring")
            generated = (
                self.settings.pipeline_dir("graphs") / "modules/scoring/scoring.module.py"
            )
            self.assertTrue(generated.exists())

    def test_a_link_to_a_pin_that_no_longer_exists_is_refused(self) -> None:
        """Config decides pins, so the canvas drops stale wires - and so does
        the server, rather than compiling something that cannot run."""
        self._pipeline()
        document = simple_graph()
        document["links"].append({"from": ["entry", "no_such_pin"], "to": ["band", "score"]})
        with self.client() as client:
            client.post("/api/pipelines/sync")
            res = client.post(
                "/api/pipelines/graphs/graph/preview?path=modules/scoring/scoring.graph",
                json={"graph": document},
            ).json()
            self.assertFalse(res["ok"])
            self.assertIn("no_such_pin", res["error"])

    def test_a_graph_has_exactly_one_entry(self) -> None:
        """Entry is the signature. Two would be two starting points."""
        from graetl.graph.model import Graph as GraphDoc
        from graetl.graph.model import GraphError

        document = simple_graph()
        document["nodes"].append({"id": "entry2", "op": "core:entry", "pos": [0, 600]})
        with self.assertRaises(GraphError) as caught:
            GraphDoc.from_dict(document)
        self.assertIn("entry", str(caught.exception))

        self._pipeline()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            refused = client.put(
                "/api/pipelines/graphs/graph?path=modules/scoring/scoring.graph",
                json={"graph": document},
            )
            self.assertEqual(refused.status_code, 422)
            # And the palette never offers one, so it cannot happen by accident.
            catalog = client.get("/api/pipelines/graphs/nodes").json()["nodes"]
            self.assertNotIn("core:entry", {e["op"] for e in catalog})

    def test_a_pin_suggests_what_you_would_do_with_it(self) -> None:
        """Dragging a wire off a pin and letting go is a question, not a slip."""
        self._pipeline()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            base = "/api/pipelines/graphs/nodes?suggest="

            entity = client.get(f"{base}Entity").json()["nodes"]
            titles = [e["title"] for e in entity]
            # Entity's own fields, read off the dataclass rather than a list
            # someone has to remember to update.
            self.assertEqual(titles[:4], [".id", ".source_updated_at", ".label", ".data"])
            self.assertTrue(all(e["suggested"] for e in entity))
            self.assertEqual(entity[0]["meta"]["config"], {"name": "id"})

            numbers = [e["title"] for e in client.get(f"{base}int").json()["nodes"]]
            self.assertIn("+", numbers)
            self.assertIn("To str", numbers)

            flow = [e["title"] for e in client.get(f"{base}exec").json()["nodes"]]
            self.assertIn("Branch", flow)
            self.assertIn("For each", flow)
            self.assertNotIn("Coalesce", flow, "an exec pin drives flow, not data")

            # An unknown type still gets the operations that fit anything.
            unknown = [e["title"] for e in client.get(f"{base}Whatever").json()["nodes"]]
            self.assertIn("Is none", unknown)

    def test_a_comment_can_leave_its_nodes_alone(self) -> None:
        self._pipeline()
        document = simple_graph()
        document["comments"] = [
            {"id": "c1", "text": "loose", "pos": [0, 0], "size": [300, 200],
             "moves_nodes": False},
            {"id": "c2", "text": "normal", "pos": [400, 0], "size": [300, 200]},
        ]
        with self.client() as client:
            client.post("/api/pipelines/sync")
            client.put(
                "/api/pipelines/graphs/graph?path=modules/scoring/scoring.graph",
                json={"graph": document},
            )
            back = client.get(
                "/api/pipelines/graphs/graph?path=modules/scoring/scoring.graph"
            ).json()["graph"]["comments"]
            self.assertIs(back[0]["moves_nodes"], False)
            # The default is on, and a default is not written to the file.
            self.assertNotIn("moves_nodes", back[1])

    def test_a_module_can_be_browsed_rather_than_recalled(self) -> None:
        """Reflection you can look at: what is inside this module?"""
        self._pipeline()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            data = client.get("/api/pipelines/graphs/nodes?module=statistics").json()
            names = {f["name"]: f for f in data["functions"]}
            self.assertIn("fmean", names)
            self.assertEqual(names["fmean"]["op"], "py:statistics.fmean")
            self.assertTrue(names["fmean"]["signature"].startswith("(data"))
            self.assertTrue(names["fmean"]["doc"])
            self.assertIn("NormalDist", {c["name"] for c in data["classes"]})

            # A package offers its children rather than pretending to be flat.
            package = client.get("/api/pipelines/graphs/nodes?module=json").json()
            self.assertIn("json.decoder", package["submodules"])

            missing = client.get("/api/pipelines/graphs/nodes?module=no_such_module_here")
            self.assertEqual(missing.status_code, 422)
            self.assertIn("no module named", missing.json()["detail"].lower())

    def test_a_types_methods_can_be_browsed(self) -> None:
        """`df.to_csv(...)` is a call on a value; no module-level name reaches it."""
        self._pipeline()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            data = client.get(
                "/api/pipelines/graphs/nodes?module=statistics.NormalDist&methods=1"
            ).json()
            methods = {m["name"]: m for m in data["methods"]}
            self.assertIn("from_samples", methods)
            self.assertEqual(methods["from_samples"]["op"], "core:method")
            self.assertEqual(methods["from_samples"]["config"], {"name": "from_samples"})

    def test_browsing_respects_the_reflection_allowlist(self) -> None:
        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        (self.settings.pipeline_dir("graphs") / "pipeline.toml").write_text(
            '[graphs]\nreflect_allow = ["statistics"]\n', encoding="utf-8"
        )
        with self.client() as client:
            client.post("/api/pipelines/sync")
            self.assertEqual(
                client.get("/api/pipelines/graphs/nodes?module=statistics").status_code, 200
            )
            refused = client.get("/api/pipelines/graphs/nodes?module=shutil")
            self.assertEqual(refused.status_code, 422)
            self.assertIn("reflect_allow", refused.json()["detail"])

    def test_a_method_node_compiles_to_a_method_call(self) -> None:
        from graetl.graph.build import compile_one_graph

        self._pipeline()
        path = self.settings.pipeline_dir("graphs") / "modules/scoring/export.graph"
        write_graph(self.root, "graphs", "export.graph", {
            "graetl_graph": 1, "kind": "module", "name": "export",
            "version": 1, "execution_layer": 5,
            "nodes": [
                {"id": "entry", "op": "core:entry", "pos": [0, 0]},
                {"id": "data", "op": "core:get_attr", "config": {"name": "data"},
                 "pos": [160, 160]},
                # object.get("score", 0) - a method, on a value, with arguments.
                {"id": "call", "op": "core:method",
                 "config": {"name": "get", "args": 2, "pure": True},
                 "values": {"arg_0": "score", "arg_1": 0}, "pos": [340, 160]},
                {"id": "log", "op": "core:log", "config": {"level": "info"}, "pos": [560, 0]},
            ],
            "links": [
                {"from": ["entry", "then"], "to": ["log", "exec"]},
                {"from": ["entry", "entity"], "to": ["data", "object"]},
                {"from": ["data", "value"], "to": ["call", "object"]},
                {"from": ["call", "result"], "to": ["log", "message"]},
            ],
        }, folder="modules/scoring")
        source, _ = compile_one_graph(
            path, folder=self.settings.pipeline_dir("graphs"), pipeline_id="graphs"
        )
        compile(source, "export.module.py", "exec")
        self.assertIn('entity.data.get("score", 0)', source)

    def test_an_impure_method_node_sits_in_the_execution_chain(self) -> None:
        """Writing a file is a statement, not something folded into an expression."""
        from graetl.graph.build import compile_one_graph

        self._pipeline()
        path = self.settings.pipeline_dir("graphs") / "modules/scoring/dump.graph"
        write_graph(self.root, "graphs", "dump.graph", {
            "graetl_graph": 1, "kind": "module", "name": "dump",
            "version": 1, "execution_layer": 5,
            "nodes": [
                {"id": "entry", "op": "core:entry", "pos": [0, 0]},
                {"id": "data", "op": "core:get_attr", "config": {"name": "data"},
                 "pos": [160, 160]},
                {"id": "call", "op": "core:method",
                 "config": {"name": "to_csv", "args": 1, "kwargs": ["index"]},
                 "values": {"arg_0": "out.csv", "index": False}, "pos": [360, 0]},
            ],
            "links": [
                {"from": ["entry", "then"], "to": ["call", "exec"]},
                {"from": ["entry", "entity"], "to": ["data", "object"]},
                {"from": ["data", "value"], "to": ["call", "object"]},
            ],
        }, folder="modules/scoring")
        source, _ = compile_one_graph(
            path, folder=self.settings.pipeline_dir("graphs"), pipeline_id="graphs"
        )
        compile(source, "dump.module.py", "exec")
        self.assertIn('entity.data.to_csv("out.csv", index=False)', source)

    def test_a_comment_keeps_its_colour(self) -> None:
        self._pipeline()
        document = simple_graph()
        document["comments"] = [
            {"id": "c1", "text": "lookup", "pos": [0, 0], "size": [300, 200],
             "color": "green"},
            {"id": "c2", "text": "plain", "pos": [400, 0], "size": [300, 200]},
        ]
        with self.client() as client:
            client.post("/api/pipelines/sync")
            client.put(
                "/api/pipelines/graphs/graph?path=modules/scoring/scoring.graph",
                json={"graph": document},
            )
            back = client.get(
                "/api/pipelines/graphs/graph?path=modules/scoring/scoring.graph"
            ).json()["graph"]["comments"]
            self.assertEqual(back[0]["color"], "green")
            # No colour is no key: the default is not written into the file.
            self.assertNotIn("color", back[1])

    def test_a_format_node_never_emits_a_backslash_inside_an_f_string(self) -> None:
        """A quote in a nested call is a SyntaxError before Python 3.12."""
        from graetl.graph.build import compile_one_graph

        self._pipeline()
        path = self.settings.pipeline_dir("graphs") / "modules/scoring/nested.graph"
        write_graph(self.root, "graphs", "nested.graph", {
            "graetl_graph": 1, "kind": "module", "name": "nested",
            "version": 1, "execution_layer": 5,
            "nodes": [
                {"id": "entry", "op": "core:entry", "pos": [0, 0]},
                {"id": "score", "op": "core:get_item", "config": {"safe": True},
                 "values": {"key": "score", "default": 0}, "pos": [120, 200]},
                {"id": "data", "op": "core:get_attr", "config": {"name": "data"},
                 "pos": [60, 200]},
                {"id": "band", "op": "fn:band", "pos": [300, 200]},
                {"id": "text", "op": "core:format",
                 "config": {"template": "{who} is {band!r} at {band:>8}"}, "pos": [460, 120]},
                {"id": "log", "op": "core:log", "config": {"level": "info"}, "pos": [640, 40]},
            ],
            "links": [
                {"from": ["entry", "then"], "to": ["log", "exec"]},
                {"from": ["entry", "entity"], "to": ["data", "object"]},
                {"from": ["data", "value"], "to": ["score", "container"]},
                {"from": ["score", "value"], "to": ["band", "score"]},
                {"from": ["band", "result"], "to": ["text", "band"]},
                {"from": ["entry", "entity"], "to": ["text", "who"]},
                {"from": ["text", "text"], "to": ["log", "message"]},
            ],
        }, folder="modules/scoring")

        source, _ = compile_one_graph(
            path, folder=self.settings.pipeline_dir("graphs"), pipeline_id="graphs"
        )
        # It parses - which is the whole assertion, on 3.10 and 3.11.
        compile(source, "nested.module.py", "exec")
        fstring = next(line for line in source.splitlines() if 'f"' in line)
        self.assertNotIn("\\", fstring, fstring)
        # The nested call was hoisted rather than inlined, and the format spec
        # and conversion survived.
        self.assertIn('ctx.fn("band")', source)
        self.assertIn("!r}", fstring)
        self.assertIn(":>8}", fstring)

    def test_editing_is_refused_while_a_run_is_active(self) -> None:
        write_pipeline(self.root, "slow", SLOW_PIPELINE)
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run = client.post("/api/pipelines/slow/runs", json={}).json()
            wait_for(
                lambda: client.get(f"/api/runs/{run['id']}").json()
                if client.get(f"/api/runs/{run['id']}").json()["status"] == "running"
                else None
            )
            res = client.put(
                "/api/pipelines/slow/graph?path=x.graph",
                json={"graph": json.loads(minimal_graph("x"))},
            )
            self.assertEqual(res.status_code, 409)
            client.post(f"/api/runs/{run['id']}/stop")


def module_graph(name: str, entry: str = "core:entry", **over) -> dict:
    """A module graph that logs something, with the given entry node."""
    hands = {"core:entry": "entity", "core:entry_batch": "entities"}.get(entry)
    nodes = [
        {"id": "entry", "op": entry, "pos": [0, 0], **over.pop("entry_config", {})},
        {"id": "log", "op": "core:log", "config": {"level": "info"},
         "values": {} if hands else {"message": "ran"}, "pos": [400, 0]},
    ]
    links = [{"from": ["entry", "then"], "to": ["log", "exec"]}]
    if hands:
        nodes.insert(1, {"id": "n", "op": "py:len" if hands == "entities"
                         else "core:get_attr", "config": {} if hands == "entities"
                         else {"name": "id"}, "pos": [200, 160]})
        nodes.insert(2, {"id": "msg", "op": "core:format",
                         "config": {"template": "saw {n}"}, "pos": [300, 160]})
        links += [
            {"from": ["entry", hands], "to": ["n", "obj" if hands == "entities" else "object"]},
            {"from": ["n", "result" if hands == "entities" else "value"], "to": ["msg", "n"]},
            {"from": ["msg", "text"], "to": ["log", "message"]},
        ]
    return {"graetl_graph": 1, "kind": "module", "name": name,
            "version": 1, "execution_layer": 10, "nodes": nodes, "links": links, **over}


class TestEntryKinds(GraetlTestCase):
    """Three ways a module can be called, decided by its entry node."""

    def test_each_entry_compiles_to_its_own_signature(self) -> None:
        from graetl.graph.build import build_pipeline_graphs

        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        for name, entry, expected, extra in (
            ("per_entity", "core:entry", "def per_entity(ctx, entity):", ""),
            ("in_bulk", "core:entry_batch", "def in_bulk(ctx, entities):", 'scope="batch"'),
            ("just_once", "core:entry_once", "def just_once(ctx):", 'scope="once"'),
        ):
            write_graph(self.root, "graphs", f"{name}.graph",
                        module_graph(name, entry), folder=f"modules/{name}")
        report = build_pipeline_graphs(self.settings.pipeline_dir("graphs"))
        self.assertTrue(report.ok, report.summary())
        for name, expected, extra in (
            ("per_entity", "def per_entity(ctx, entity):", None),
            ("in_bulk", "def in_bulk(ctx, entities):", 'scope="batch"'),
            ("just_once", "def just_once(ctx):", 'scope="once"'),
        ):
            code = (
                self.settings.pipeline_dir("graphs") / f"modules/{name}/{name}.module.py"
            ).read_text(encoding="utf-8")
            self.assertIn(expected, code)
            if extra:
                self.assertIn(extra, code)
            else:
                self.assertNotIn("scope=", code, "the default is not written out")

    def test_a_batch_graph_carries_its_size(self) -> None:
        from graetl.graph.build import build_pipeline_graphs

        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        write_graph(self.root, "graphs", "in_bulk.graph", module_graph(
            "in_bulk", "core:entry_batch", entry_config={"config": {"size": 250}},
        ), folder="modules/in_bulk")
        report = build_pipeline_graphs(self.settings.pipeline_dir("graphs"))
        self.assertTrue(report.ok, report.summary())
        code = (
            self.settings.pipeline_dir("graphs") / "modules/in_bulk/in_bulk.module.py"
        ).read_text(encoding="utf-8")
        self.assertIn("batch_size=250", code)

    def test_a_batch_is_one_transaction(self) -> None:
        """All of it is done, or none of it is - the promise batching makes."""
        write_pipeline(self.root, "bulky", BATCH_PIPELINE)
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run = client.post("/api/pipelines/bulky/runs", json={"mode": "full"}).json()
            final = wait_for(
                lambda: (
                    client.get(f"/api/runs/{run['id']}").json()
                    if client.get(f"/api/runs/{run['id']}").json()["status"] in ("succeeded", "failed", "crashed", "stopped")
                    else None
                )
            )
            self.assertEqual(final["status"], "failed", final.get("error"))

        with self.settings.state_store("bulky") as state:
            rows = state.conn.execute(
                "SELECT status, COUNT(*) FROM [[entity_module_state]] WHERE pipeline = 'bulky' AND module = 'good' "
                "GROUP BY status"
            ).fetchall()
            self.assertEqual(dict(rows), {"done": 6}, "the clean batches committed")
            rows = state.conn.execute(
                "SELECT status, COUNT(*) FROM [[entity_module_state]] WHERE pipeline = 'bulky' AND module = 'poison' "
                "GROUP BY status"
            ).fetchall()
            # The batch that raised took its whole batch down with it; nothing
            # in it is done, and no half-written rows were left behind.
            self.assertEqual(dict(rows).get("done", 0), 4, dict(rows))
            self.assertEqual(dict(rows).get("failed", 0), 2, dict(rows))
            # `poison` writes an "<id>x" row per entity before it raises. The
            # two clean batches left theirs; the failing one rolled its back,
            # so E4x and E5x are simply not there.
            marks = [
                row[0] for row in state.conn.execute(
                    "SELECT id FROM bulk_rows WHERE id LIKE '%x' ORDER BY id"
                )
            ]
            self.assertEqual(marks, ["E0x", "E1x", "E2x", "E3x"], marks)

    def test_a_once_module_runs_once_and_keeps_no_entity_state(self) -> None:
        write_pipeline(self.root, "bulky", BATCH_PIPELINE)
        with self.client() as client:
            client.post("/api/pipelines/sync")
            definition = client.get("/api/pipelines/bulky").json()["definition"]
            scopes = {m["name"]: m["scope"] for m in definition["modules"]}
            self.assertEqual(scopes, {"good": "batch", "poison": "batch", "summarise": "once"})

            run = client.post(
                "/api/pipelines/bulky/runs", json={"mode": "full", "steps": ["summarise"]}
            ).json()
            wait_for(
                lambda: (
                    client.get(f"/api/runs/{run['id']}").json()
                    if client.get(f"/api/runs/{run['id']}").json()["status"] in ("succeeded", "failed", "crashed", "stopped")
                    else None
                )
            )
        with self.settings.state_store("bulky") as state:
            self.assertEqual(
                state.conn.execute(
                    "SELECT COUNT(*) FROM [[entity_module_state]] WHERE pipeline = 'bulky' AND module = 'summarise'"
                ).fetchone()[0],
                0,
                "a once module has no entity state to keep",
            )
            self.assertEqual(
                state.conn.execute("SELECT n FROM tally").fetchone()[0], 1,
                "it ran exactly once",
            )

    def test_a_once_module_never_gates_an_entity(self) -> None:
        """It has no per-entity state, so it cannot be a per-entity requirement."""
        from graetl.sdk.pipeline import Module, Pipeline

        pipeline = Pipeline(id="p", stateful=True)
        once = Module(name="global", fn=lambda ctx: None, execution_layer=10, scope="once")
        below = Module(name="early", fn=lambda ctx, e: None, execution_layer=10)
        above = Module(name="late", fn=lambda ctx, e: None, execution_layer=20,
                       depends_on=("global",))
        pipeline.modules.extend([once, below, above])
        names = [name for name, _ in pipeline.requirements_for(above)]
        self.assertIn("early", names)
        self.assertNotIn("global", names)


BATCH_PIPELINE = '''"""Batches and a once-module, for the executor tests."""

from graetl.sdk import Entity, Pipeline

pipeline = Pipeline(id="bulky", title="Bulky", stateful=True)


@pipeline.setup
def setup(ctx):
    ctx.db.execute("CREATE TABLE IF NOT EXISTS bulk_rows (id TEXT PRIMARY KEY)")
    ctx.db.execute("CREATE TABLE IF NOT EXISTS tally (n INTEGER)")


@pipeline.entities
def discover(ctx):
    for i in range(6):
        yield Entity(id=f"E{i}", source_updated_at="2024-01-01T00:00:00Z")


@pipeline.module(version=1, execution_layer=10, scope="batch", batch_size=3)
def good(ctx, entities):
    for entity in entities:
        ctx.db.execute("INSERT OR REPLACE INTO bulk_rows (id) VALUES (?)", (entity.id,))


@pipeline.module(version=1, execution_layer=20, scope="batch", batch_size=2)
def poison(ctx, entities):
    """Writes, then dies on the batch holding E4 - so that batch must roll back."""
    for entity in entities:
        ctx.db.execute("INSERT OR REPLACE INTO bulk_rows (id) VALUES (?)", (entity.id + "x",))
    if any(e.id == "E4" for e in entities):
        raise RuntimeError("poison")


@pipeline.module(version=1, execution_layer=30, scope="once")
def summarise(ctx):
    ctx.db.execute("INSERT INTO tally (n) VALUES (1)")
'''


NODE_LIBRARY = '''"""Vitals helpers."""

import math

from graetl.sdk import node


def bmi(weight_kg, height_m):
    """Body mass index."""
    return weight_kg / (height_m ** 2)


@node(category="Risk", title="Risk score")
def risk(age, bmi_value):
    return math.log1p(age) * bmi_value


@node(pure=False)
def announce(ctx, message):
    ctx.log(message)


def _helper(x):
    return x


@node(skip=True)
def not_a_node(x):
    return x
'''


def library(*functions: dict, name: str = "helpers", **over) -> dict:
    """A ``.graphlib`` document holding the given function graphs."""
    return {"graetl_graph": 1, "kind": "library", "name": name,
            "functions": list(functions), **over}


def simple_function(name: str = "double", **over) -> dict:
    """number -> number * 2, as a function graph."""
    return {
        "kind": "function", "name": name, "description": "Twice the number.",
        "pure": True,
        "inputs": [{"name": "number", "type": "float"}],
        "outputs": [{"name": "doubled", "type": "float"}],
        "nodes": [
            {"id": "entry", "op": "core:entry", "pos": [0, 0]},
            {"id": "mul", "op": "core:binary_op", "config": {"op": "mul"},
             "values": {"b": 2}, "pos": [200, 60]},
            {"id": "ret", "op": "core:return", "pos": [400, 0]},
        ],
        "links": [
            {"from": ["entry", "then"], "to": ["ret", "exec"]},
            {"from": ["entry", "number"], "to": ["mul", "a"]},
            {"from": ["mul", "result"], "to": ["ret", "doubled"]},
        ],
        **over,
    }


class TestGraphLibraries(GraetlTestCase):
    """A ``.graphlib`` is a library: one file, one or more functions."""

    def test_the_old_one_function_shape_still_reads(self) -> None:
        """Files written before libraries existed must keep working."""
        from graetl.graph.build import build_pipeline_graphs
        from graetl.graph.model import Library

        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        path = write_graph(
            self.root, "graphs", "double.graphlib",
            {"graetl_graph": 1, **simple_function()},
        )
        loaded = Library.load(path)
        self.assertEqual(loaded.names, ["double"])
        self.assertEqual(loaded.name, "double", "the file names the library")

        report = build_pipeline_graphs(self.settings.pipeline_dir("graphs"))
        self.assertTrue(report.ok, report.summary())
        self.assertIn("def double(", path.with_name("double.graphlib.py").read_text())

    def test_several_functions_compile_into_one_file(self) -> None:
        from graetl.graph.build import build_pipeline_graphs

        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        path = write_graph(self.root, "graphs", "helpers.graphlib", library(
            simple_function("double"),
            simple_function("triple", description="Three times the number."),
        ))
        report = build_pipeline_graphs(self.settings.pipeline_dir("graphs"))
        self.assertTrue(report.ok, report.summary())

        code = path.with_name("helpers.graphlib.py").read_text(encoding="utf-8")
        self.assertIn("def double(", code)
        self.assertIn("def triple(", code)
        self.assertIn("Pipeline function(s): double, triple.", code)
        compile(code, "helpers.graphlib.py", "exec")

        # Both register, and both are callable as nodes.
        loaded = load_pipeline(self.settings.pipeline_dir("graphs"), pipeline_id="graphs")
        self.assertIn("double", loaded.pipeline.functions)
        self.assertIn("triple", loaded.pipeline.functions)
        self.assertEqual(loaded.pipeline.functions["triple"](None, 4), 8)

    def test_two_functions_cannot_share_a_name(self) -> None:
        from graetl.graph.build import build_pipeline_graphs

        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        write_graph(self.root, "graphs", "helpers.graphlib", library(
            simple_function("double"), simple_function("double"),
        ))
        report = build_pipeline_graphs(self.settings.pipeline_dir("graphs"))
        self.assertFalse(report.ok)
        self.assertIn("double", report.builds[0].error or "")

    def test_the_editor_reads_and_writes_a_whole_library(self) -> None:
        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        write_graph(self.root, "graphs", "helpers.graphlib", library(
            simple_function("double"), simple_function("triple"),
        ))
        with self.client() as client:
            client.post("/api/pipelines/sync")
            data = client.get("/api/pipelines/graphs/graph?path=helpers.graphlib").json()
            self.assertEqual(data["kind"], "library")
            self.assertEqual([f["name"] for f in data["library"]["functions"]],
                             ["double", "triple"])
            # Every function's nodes are resolved, so the editor can switch
            # between them without another round trip.
            self.assertEqual(sorted(data["resolved"]), ["double", "triple"])
            self.assertTrue(data["resolved"]["double"]["definitions"])
            self.assertEqual(data["output"], "helpers.graphlib.py")

            document = data["library"]
            document["functions"].append(simple_function("quadruple"))
            saved = client.put(
                "/api/pipelines/graphs/graph?path=helpers.graphlib",
                json={"graph": document, "compile": True},
            ).json()
            self.assertTrue(saved["build"]["ok"], saved["build"])
            again = client.get("/api/pipelines/graphs/graph?path=helpers.graphlib").json()
            self.assertEqual(len(again["library"]["functions"]), 3)
            self.assertIn(
                "def quadruple(",
                (self.settings.pipeline_dir("graphs") / "helpers.graphlib.py").read_text(),
            )

    def test_a_module_graph_calls_a_library_function(self) -> None:
        """What the grouping is for: the callee does not care which file it is in."""
        from graetl.graph.build import build_pipeline_graphs

        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        write_graph(self.root, "graphs", "helpers.graphlib", library(
            simple_function("double"), simple_function("triple"),
        ))
        write_graph(self.root, "graphs", "uses.graph", {
            "graetl_graph": 1, "kind": "module", "name": "uses",
            "version": 1, "execution_layer": 5,
            "nodes": [
                {"id": "entry", "op": "core:entry", "pos": [0, 0]},
                {"id": "n", "op": "core:literal", "config": {"value": 7}, "pos": [120, 150]},
                {"id": "t", "op": "graph:triple", "pos": [300, 150]},
                {"id": "log", "op": "core:log", "config": {"level": "info"}, "pos": [520, 0]},
            ],
            "links": [
                {"from": ["entry", "then"], "to": ["log", "exec"]},
                {"from": ["n", "value"], "to": ["t", "number"]},
                {"from": ["t", "doubled"], "to": ["log", "message"]},
            ],
        }, folder="modules/uses")
        report = build_pipeline_graphs(self.settings.pipeline_dir("graphs"))
        self.assertTrue(report.ok, report.summary())
        code = (
            self.settings.pipeline_dir("graphs") / "modules/uses/uses.module.py"
        ).read_text(encoding="utf-8")
        self.assertIn('ctx.fn("triple")(ctx, 7)', code)


CACHE_LIBRARY = '''"""Lookups worth doing once."""

from graetl.sdk import node

CALLS = []


@node(cache=True)
def unit_of(ctx, code):
    """One database round trip per distinct code."""
    CALLS.append(code)
    return {1: "g", 2: "ml"}.get(code, "?")


@node(cache=3)
def small(value):
    CALLS.append(value)
    return value


def uncached(value):
    CALLS.append(value)
    return value
'''


class TestFunctionCache(GraetlTestCase):
    """``cache=True``: the same arguments mean the same answer, once per run."""

    def _pipeline(self):
        from graetl.loader import load_pipeline

        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        target = self.root / "pipelines" / "graphs" / "modules" / "lookups"
        target.mkdir(parents=True, exist_ok=True)
        (target / "lookups.nodes.py").write_text(CACHE_LIBRARY, encoding="utf-8")
        return load_pipeline(self.settings.pipeline_dir("graphs"), pipeline_id="graphs").pipeline

    def test_a_cached_node_is_called_once_per_distinct_argument(self) -> None:
        pipeline = self._pipeline()
        unit_of = pipeline.functions["unit_of"]
        results = [unit_of("ctx", code) for code in (1, 2, 1, 9, 1, 2)]
        self.assertEqual(results, ["g", "ml", "g", "?", "g", "ml"])
        self.assertEqual(unit_of.cache.hits, 3)
        self.assertEqual(unit_of.cache.misses, 3)

    def test_the_run_context_is_not_part_of_the_key(self) -> None:
        """ctx is per-worker; keying on it would defeat the cache entirely."""
        pipeline = self._pipeline()
        unit_of = pipeline.functions["unit_of"]
        unit_of("worker-a", 1)
        unit_of("worker-b", 1)
        self.assertEqual(unit_of.cache.hits, 1)

    def test_types_are_part_of_the_key(self) -> None:
        pipeline = self._pipeline()
        unit_of = pipeline.functions["unit_of"]
        unit_of("ctx", 1)
        unit_of("ctx", True)
        unit_of("ctx", "1")
        # Python would treat 1 and True as one key; three calls are three
        # questions, and they get three answers.
        self.assertEqual(unit_of.cache.misses, 3)
        self.assertEqual(unit_of.cache.hits, 0)

    def test_an_unhashable_argument_falls_through_instead_of_raising(self) -> None:
        """A cache is an optimisation; it must never be why a pipeline fails."""
        from graetl.sdk import caching

        calls = []

        @caching.cached
        def load(spec):
            calls.append(spec)
            return dict(spec)

        class Opaque:
            __hash__ = None

        # A dict argument is frozen recursively, so it still caches.
        self.assertEqual(load({"a": [1, 2]}), {"a": [1, 2]})
        self.assertEqual(load({"a": [1, 2]}), {"a": [1, 2]})
        self.assertEqual(len(calls), 1)
        self.assertEqual(load.cache.hits, 1)

        # Something genuinely unhashable is simply not cached.
        @caching.cached
        def touch(value):
            calls.append(value)
            return 1

        touch(Opaque())
        touch(Opaque())
        self.assertEqual(touch.cache.skipped, 2)
        self.assertEqual(touch.cache.hits, 0)

    def test_the_cache_is_bounded_and_evicts_the_oldest(self) -> None:
        pipeline = self._pipeline()
        small = pipeline.functions["small"]
        for value in range(6):
            small(value)
        self.assertEqual(small.cache.limit, 3)
        self.assertEqual(small.cache.evictions, 3)
        small(5)                       # still there
        self.assertEqual(small.cache.hits, 1)
        small(0)                       # evicted, so recomputed
        self.assertEqual(small.cache.misses, 7)

    def test_a_function_graph_can_be_cached(self) -> None:
        from graetl.graph.build import build_pipeline_graphs

        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        path = write_graph(self.root, "graphs", "helpers.graphlib", library(
            simple_function("double"),
            simple_function("expensive", cache=True, cache_size=64),
        ))
        report = build_pipeline_graphs(self.settings.pipeline_dir("graphs"))
        self.assertTrue(report.ok, report.summary())
        code = path.with_name("helpers.graphlib.py").read_text(encoding="utf-8")
        self.assertIn("from graetl.sdk import cached, get_pipeline", code)
        self.assertIn("@cached(maxsize=64)", code)
        # Below @pipeline.function, so the registry holds the cached wrapper.
        self.assertLess(code.index('@pipeline.function("expensive"'),
                        code.index("@cached(maxsize=64)"))

        loaded = load_pipeline(self.settings.pipeline_dir("graphs"), pipeline_id="graphs")
        expensive = loaded.pipeline.functions["expensive"]
        self.assertTrue(getattr(expensive, "graetl_cached", False))
        expensive(None, 3)
        expensive(None, 3)
        self.assertEqual(expensive.cache.hits, 1)
        # Uncached neighbours in the same file are untouched.
        self.assertFalse(getattr(loaded.pipeline.functions["double"], "graetl_cached", False))

    def test_the_cache_size_comes_from_configuration(self) -> None:
        from graetl.sdk import caching

        try:
            caching.set_default_maxsize(7)

            @caching.cached
            def f(x):
                return x

            self.assertEqual(f.cache.limit, 7)
        finally:
            caching.set_default_maxsize(None)


class TestNodeLibraries(GraetlTestCase):
    """``*.nodes.py``: plain functions, no decorator, all of them nodes."""

    def _library(self, source: str = NODE_LIBRARY, name: str = "vitals.nodes.py") -> Path:
        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        target = self.root / "pipelines" / "graphs" / "modules" / "vitals"
        target.mkdir(parents=True, exist_ok=True)
        path = target / name
        path.write_text(source, encoding="utf-8")
        return path

    def test_every_public_function_becomes_a_node(self) -> None:
        from graetl.loader import load_pipeline

        self._library()
        pipeline = load_pipeline(self.settings.pipeline_dir("graphs"), pipeline_id="graphs").pipeline
        self.assertIn("bmi", pipeline.functions)
        self.assertIn("risk", pipeline.functions)
        self.assertIn("announce", pipeline.functions)
        # A private helper, an opted-out function and an import are not nodes.
        self.assertNotIn("_helper", pipeline.functions)
        self.assertNotIn("not_a_node", pipeline.functions)
        self.assertNotIn("log1p", pipeline.functions)
        self.assertNotIn("math", pipeline.functions)

        libraries = pipeline.assets["node_libraries"]
        self.assertEqual(libraries[0]["file"], "modules/vitals/vitals.nodes.py")
        self.assertEqual(libraries[0]["nodes"], ["announce", "bmi", "risk"])

    def test_purity_follows_the_signature(self) -> None:
        """Taking ctx means it can reach the run, so it is not an expression."""
        from graetl.loader import load_pipeline

        self._library()
        pipeline = load_pipeline(self.settings.pipeline_dir("graphs"), pipeline_id="graphs").pipeline
        self.assertTrue(pipeline.functions["bmi"].graetl_pure, "no ctx -> pure")
        self.assertFalse(pipeline.functions["announce"].graetl_pure, "ctx -> impure")
        # @node(...) wins over what would have been inferred.
        self.assertEqual(pipeline.functions["risk"].graetl_title, "Risk score")
        self.assertEqual(pipeline.functions["risk"].graetl_category, "Risk")
        # The file name is the default grouping.
        self.assertEqual(pipeline.functions["bmi"].graetl_category, "Vitals")

    def test_a_library_node_resolves_and_compiles(self) -> None:
        """The whole point: draw with it, and the Python calls it."""
        from graetl.graph.build import build_pipeline_graphs

        self._library()
        write_graph(self.root, "graphs", "shape.graph", {
            "graetl_graph": 1, "kind": "module", "name": "shape",
            "version": 1, "execution_layer": 5,
            "nodes": [
                {"id": "entry", "op": "core:entry", "pos": [0, 0]},
                {"id": "w", "op": "core:literal", "config": {"value": 70.0}, "pos": [100, 100]},
                {"id": "h", "op": "core:literal", "config": {"value": 1.8}, "pos": [100, 200]},
                {"id": "b", "op": "fn:bmi", "pos": [300, 150]},
                {"id": "say", "op": "fn:announce", "pos": [520, 40]},
                {"id": "text", "op": "core:format", "config": {"template": "bmi {value}"},
                 "pos": [420, 250]},
            ],
            "links": [
                {"from": ["entry", "then"], "to": ["say", "exec"]},
                {"from": ["w", "value"], "to": ["b", "weight_kg"]},
                {"from": ["h", "value"], "to": ["b", "height_m"]},
                {"from": ["b", "result"], "to": ["text", "value"]},
                {"from": ["text", "text"], "to": ["say", "message"]},
            ],
        }, folder="modules/vitals")

        report = build_pipeline_graphs(self.settings.pipeline_dir("graphs"))
        self.assertTrue(report.ok, report.summary())
        code = (
            self.settings.pipeline_dir("graphs") / "modules/vitals/shape.module.py"
        ).read_text(encoding="utf-8")
        # A pure library node is an expression; an impure one is a statement.
        self.assertIn('ctx.fn("bmi")(70.0, 1.8)', code)
        self.assertIn('ctx.fn("announce")(ctx,', code)

    def test_two_libraries_cannot_define_the_same_node(self) -> None:
        from graetl.loader import load_pipeline
        from graetl.sdk.errors import PipelineDefinitionError

        self._library()
        self._library(source="def bmi(x):\n    return x\n", name="other.nodes.py")
        with self.assertRaises(PipelineDefinitionError) as caught:
            load_pipeline(self.settings.pipeline_dir("graphs"), pipeline_id="graphs")
        # Both files are named, because the fix is to rename one of them.
        self.assertIn("bmi", str(caught.exception))
        self.assertIn("vitals.nodes.py", str(caught.exception))
        self.assertIn("other.nodes.py", str(caught.exception))

    def test_a_new_library_arrives_with_a_worked_example(self) -> None:
        write_pipeline(self.root, "graphs", GRAPH_PIPELINE)
        with self.client() as client:
            client.post("/api/pipelines/sync")
            created = client.post(
                "/api/pipelines/graphs/file/new",
                json={"path": "modules/text.nodes.py", "content": ""},
            )
            self.assertEqual(created.status_code, 201)
            body = (
                self.settings.pipeline_dir("graphs") / "modules/text.nodes.py"
            ).read_text(encoding="utf-8")
            self.assertIn("from graetl.sdk import node", body)
            self.assertIn("def text(", body)

            files = client.get("/api/pipelines/graphs/files").json()
            entry = next(f for f in files if f["path"] == "modules/text.nodes.py")
            self.assertEqual(entry["kind"], "nodes")
            self.assertTrue(entry["editable"])
            # The scaffold is valid Python that loads and registers its nodes.
            catalog = client.get("/api/pipelines/graphs/nodes").json()["nodes"]
            self.assertIn("fn:text", {e["op"] for e in catalog})


class TestFileTree(GraetlTestCase):
    """Move, rename, delete and upload, with GraETL's own rules enforced."""

    def _make(self) -> None:
        write_pipeline(self.root, "tree", GRAPH_PIPELINE.replace('id="graphs"', 'id="tree"'))
        write_module(self.root, "tree", "scoring", "scoring", TREE_MODULE)

    def test_the_listing_says_what_each_file_is(self) -> None:
        self._make()
        write_graph(self.root, "tree", "double.graphlib", {
            "graetl_graph": 1, "kind": "function", "name": "double",
            "inputs": [{"name": "n", "type": "float"}],
            "outputs": [{"name": "out", "type": "float"}],
            "nodes": [
                {"id": "entry", "op": "core:entry", "pos": [0, 0]},
                {"id": "ret", "op": "core:return", "pos": [200, 0]},
            ],
            "links": [
                {"from": ["entry", "then"], "to": ["ret", "exec"]},
                {"from": ["entry", "n"], "to": ["ret", "out"]},
            ],
        })
        with self.client() as client:
            client.post("/api/pipelines/sync")
            files = {f["path"]: f for f in client.get("/api/pipelines/tree/files").json()}
        self.assertEqual(files["pipeline.py"]["kind"], "python")
        self.assertEqual(files["double.graphlib"]["kind"], "graphlib")
        self.assertEqual(files["modules/scoring/scoring.module.py"]["kind"], "module")
        self.assertEqual(files["modules/scoring/scoring.module.py"]["module"], "scoring")
        generated = files["double.graphlib.py"]
        self.assertEqual(generated["kind"], "graph_function")
        self.assertEqual(generated["generated"], "double.graphlib")
        self.assertIsNone(files["pipeline.py"]["generated"])

    def test_renaming_a_module_carries_its_entity_state(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run = client.post("/api/pipelines/tree/runs", json={}).json()
            wait_for(
                lambda: client.get(f"/api/runs/{run['id']}").json()
                if client.get(f"/api/runs/{run['id']}").json()["status"] == "succeeded"
                else None,
                timeout=60,
            )
            before = client.get("/api/pipelines/tree/entities?limit=1").json()
            self.assertEqual({m["module"] for m in before["modules"]}, {"scoring"})

            moved = client.post("/api/pipelines/tree/file/move", json={
                "source": "modules/scoring/scoring.module.py",
                "target": "modules/scoring/rank.module.py",
            })
            self.assertEqual(moved.status_code, 200, moved.text)
            payload = moved.json()
            self.assertEqual(payload["renamed_modules"], [{"from": "scoring", "to": "rank"}])
            self.assertEqual(payload["state_rows_migrated"], 4)

            after = client.get("/api/pipelines/tree/entities?limit=1").json()
            self.assertEqual({m["module"] for m in after["modules"]}, {"rank"})
            self.assertEqual(after["statuses"], {"done": 4}, "nothing has to be reprocessed")

    def test_deleting_a_module_forgets_its_state(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run = client.post("/api/pipelines/tree/runs", json={}).json()
            wait_for(
                lambda: client.get(f"/api/runs/{run['id']}").json()
                if client.get(f"/api/runs/{run['id']}").json()["status"] == "succeeded"
                else None,
                timeout=60,
            )
            removed = client.request(
                "DELETE", "/api/pipelines/tree/file?path=modules/scoring/scoring.module.py"
            )
            self.assertEqual(removed.status_code, 200, removed.text)
            self.assertEqual(removed.json()["modules_dropped"], ["scoring"])
            self.assertEqual(removed.json()["state_rows_dropped"], 4)
            left = client.get("/api/pipelines/tree/entities?limit=1").json()
        self.assertEqual(left["statuses"], {}, "no orphaned module rows are left behind")
        self.assertEqual(left["total"], 4, "the entities themselves stay")

    def test_a_generated_file_cannot_be_moved_or_deleted_directly(self) -> None:
        self._make()
        write_graph(self.root, "tree", "cleanup.graph", {
            "graetl_graph": 1, "kind": "module", "name": "cleanup", "execution_layer": 90,
            "nodes": [{"id": "entry", "op": "core:entry", "pos": [0, 0]}], "links": [],
        }, folder="modules/cleanup")
        with self.client() as client:
            client.post("/api/pipelines/sync")
            moved = client.post("/api/pipelines/tree/file/move", json={
                "source": "modules/cleanup/cleanup.module.py",
                "target": "modules/cleanup/other.module.py",
            })
            self.assertEqual(moved.status_code, 400)
            self.assertIn("generated from cleanup.graph", moved.json()["detail"])

            deleted = client.request(
                "DELETE", "/api/pipelines/tree/file?path=modules/cleanup/cleanup.module.py"
            )
            self.assertEqual(deleted.status_code, 400)

    def test_renaming_a_graph_takes_its_module_and_its_python_with_it(self) -> None:
        self._make()
        write_graph(self.root, "tree", "cleanup.graph", {
            "graetl_graph": 1, "kind": "module", "name": "cleanup", "execution_layer": 90,
            "nodes": [{"id": "entry", "op": "core:entry", "pos": [0, 0]}], "links": [],
        }, folder="modules/cleanup")
        with self.client() as client:
            client.post("/api/pipelines/sync")
            moved = client.post("/api/pipelines/tree/file/move", json={
                "source": "modules/cleanup/cleanup.graph",
                "target": "modules/cleanup/tidy_up.graph",
            })
            self.assertEqual(moved.status_code, 200, moved.text)
            payload = moved.json()
            self.assertEqual(
                [m["to"] for m in payload["moved"]],
                ["modules/cleanup/tidy_up.graph", "modules/cleanup/tidy_up.module.py"],
            )
            self.assertEqual(payload["renamed_modules"], [{"from": "cleanup", "to": "tidy_up"}])
            self.assertTrue(payload["build"]["ok"], payload["build"])

            definition = client.get("/api/pipelines/tree").json()["definition"]
            self.assertIn("tidy_up", {m["name"] for m in definition["modules"]})

        folder = self.settings.pipeline_dir("tree") / "modules" / "cleanup"
        self.assertFalse((folder / "cleanup.module.py").exists())
        document = json.loads((folder / "tidy_up.graph").read_text(encoding="utf-8"))
        self.assertEqual(document["name"], "tidy_up", "the document name follows the file name")

    def test_pipeline_py_is_not_movable(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            refused = client.post("/api/pipelines/tree/file/move",
                                  json={"source": "pipeline.py", "target": "entry.py"})
        self.assertEqual(refused.status_code, 400)
        self.assertIn("entry file", refused.json()["detail"])

    def test_moving_a_folder_moves_everything_in_it(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            moved = client.post("/api/pipelines/tree/file/move",
                                json={"source": "modules/scoring", "target": "modules/rank"})
            self.assertEqual(moved.status_code, 200, moved.text)
            files = {f["path"] for f in client.get("/api/pipelines/tree/files").json()}
        self.assertIn("modules/rank/scoring.module.py", files)
        self.assertNotIn("modules/scoring/scoring.module.py", files)
        # A module is named by its file, not its folder, so nothing was renamed.
        self.assertEqual(moved.json()["renamed_modules"], [])

    def test_folders_uploads_and_collisions(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            created = client.post("/api/pipelines/tree/folder", json={"path": "data/raw"})
            self.assertEqual(created.status_code, 201)
            self.assertEqual(
                client.post("/api/pipelines/tree/folder", json={"path": "data/raw"}).status_code,
                409,
            )

            uploaded = client.put(
                "/api/pipelines/tree/upload?path=data/raw/readings.csv",
                content=b"id,value\n1,2\n",
            )
            self.assertEqual(uploaded.status_code, 201)
            self.assertEqual(uploaded.json()["size"], 13)
            self.assertEqual(
                client.put(
                    "/api/pipelines/tree/upload?path=data/raw/readings.csv", content=b"x"
                ).status_code,
                409,
                "an upload never silently replaces a file",
            )
            files = {f["path"] for f in client.get("/api/pipelines/tree/files").json()}
        self.assertIn("data/raw/readings.csv", files)

    def test_nothing_escapes_the_pipeline_folder(self) -> None:
        self._make()
        with self.client() as client:
            client.post("/api/pipelines/sync")
            for target in ("../escape.py", "../../etc/passwd"):
                refused = client.post("/api/pipelines/tree/file/move",
                                      json={"source": "modules/scoring", "target": target})
                self.assertEqual(refused.status_code, 400, target)
            self.assertEqual(
                client.put("/api/pipelines/tree/upload?path=../evil.py",
                           content=b"x").status_code,
                400,
            )

    def test_file_operations_are_refused_while_a_run_is_active(self) -> None:
        write_pipeline(self.root, "slow", SLOW_PIPELINE)
        with self.client() as client:
            client.post("/api/pipelines/sync")
            run = client.post("/api/pipelines/slow/runs", json={}).json()
            wait_for(
                lambda: client.get(f"/api/runs/{run['id']}").json()["status"] == "running" or None,
                timeout=30,
            )
            self.assertEqual(
                client.post("/api/pipelines/slow/file/move",
                            json={"source": "pipeline.toml", "target": "x.toml"}).status_code,
                409,
            )
            self.assertEqual(
                client.request("DELETE", "/api/pipelines/slow/file?path=pipeline.py").status_code,
                409,
            )
            client.post(f"/api/runs/{run['id']}/stop?force=true")


class TestConsole(GraetlTestCase):
    """One console, served from the package, with no build step anywhere."""

    def test_the_console_is_served_from_the_package(self) -> None:
        with self.client() as client:
            health = client.get("/api/health").json()
            self.assertEqual(health["ui"], "builtin")

            index = client.get("/")
            self.assertEqual(index.status_code, 200)
            self.assertIn("GraETL", index.text)
            # every module the page pulls in resolves
            for asset in (
                "/app.css", "/app.js", "/editor.js", "/fallback-editor.js",
                "/graph.js", "/filetree.js",
            ):
                response = client.get(asset)
                self.assertEqual(response.status_code, 200, asset)
                self.assertTrue(response.text.strip(), asset)

    def test_an_unknown_path_falls_through_to_the_router(self) -> None:
        with self.client() as client:
            deep = client.get("/p/whatever/files")
            self.assertEqual(deep.status_code, 200)
            self.assertIn("<div id=\"app\"", deep.text)

    def test_nothing_outside_the_static_folder_is_reachable(self) -> None:
        """A traversal attempt gets the index page, never a file from the package."""
        with self.client() as client:
            for attempt, giveaway in (
                ("/../config.py", "MONACO_CDN"),
                ("/../../pyproject.toml", "[build-system]"),
                ("/../app.py", "create_app"),
            ):
                response = client.get(attempt)
                self.assertNotIn(giveaway, response.text, attempt)
                self.assertIn("<div id=\"app\"", response.text, attempt)
