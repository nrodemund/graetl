"""Executes one run of one pipeline inside the runner process."""

from __future__ import annotations

import random
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import groupby
from pathlib import Path
from typing import Any

from graetl.config import Settings
from graetl.loader import LoadedPipeline
from graetl.runner.control import ControlWatcher
from graetl.runner.events import EventWriter
from graetl.runner.telemetry import ProcessSampler
from graetl.sdk.context import EntityContext
from graetl.sdk.errors import AbortRun, RetryEntity, SkipEntity, StopRequested
from graetl.sdk.pipeline import Entity, Module, Task
from graetl.store.state import STATUS_DONE, STATUS_SKIPPED, LockConflict, StateStore
from graetl.utils import elapsed_ms, now_iso, truncate


class RunResult:
    def __init__(self) -> None:
        self.status: str = "succeeded"
        self.error: str | None = None
        self.metrics: dict[str, Any] = {}


class Executor:
    def __init__(
        self,
        loaded: LoadedPipeline,
        settings: Settings,
        *,
        run_id: int,
        mode: str = "incremental",
        params: dict[str, Any] | None = None,
        writer: EventWriter,
        control: ControlWatcher | None = None,
    ) -> None:
        self.loaded = loaded
        self.pipeline = loaded.pipeline
        self.settings = settings
        self.run_id = run_id
        self.mode = mode
        self.params = params or {}
        self.writer = writer
        self.control = control

        self.state: StateStore | None = None
        self.result = RunResult()
        self._t0 = time.perf_counter()
        self._counters: dict[str, Any] = {
            "entities_total": 0,
            "entities_new": 0,
            "entities_changed": 0,
            "entities_seen": 0,
            "work_items": 0,
            "processed": 0,
            "skipped": 0,
            "failed": 0,
            "steps_run": 0,
        }
        self._step_stats: list[dict[str, Any]] = []
        self._counters_lock = threading.RLock()
        self._merged_metrics: dict[str, float] = {}
        #: "immediate" while modules run one at a time, "deferred" when parallel.
        self._tx_mode = "immediate"
        self.lock_retries = int(getattr(settings, "lock_retries", 5))
        self.batch_size = int(getattr(settings, "entity_batch_size", 500))
        #: ctx.debug() output is only produced on a deliberate debug run.
        self.debug = bool(self.params.get("debug"))
        self._sampler = ProcessSampler()
        self._stop_sampling = threading.Event()

    # ------------------------------------------------------------------ setup

    def _make_context(self) -> EntityContext:
        return EntityContext(
            pipeline_id=self.pipeline.id,
            pipeline_title=self.pipeline.title,
            run_id=self.run_id,
            mode=self.mode,
            dir=Path(self.loaded.folder),
            config=self.loaded.config,
            params=self.params,
            stateful=self.pipeline.stateful,
            _emit=self._emit_from_ctx,
            _control=self.control or _NoControl(),
            _state=None,
            _functions=self.pipeline.functions,
            _resource_factories=self.pipeline.resource_factories,
            debug_enabled=self.debug,
        )

    def _emit_from_ctx(self, kind: str, payload: dict[str, Any]) -> None:
        self.writer.emit(kind, payload)

    # -------------------------------------------------------------------- run

    def run(self) -> RunResult:
        pipeline = self.pipeline
        ctx = self._make_context()
        self.writer.emit(
            "run_start",
            {
                "run_id": self.run_id,
                "pipeline_id": pipeline.id,
                "mode": self.mode,
                "params": self.params,
                "definition": pipeline.to_dict(),
            },
        )

        sampler = threading.Thread(
            target=self._sample_resources, name="graetl-telemetry", daemon=True
        )
        sampler.start()
        try:
            if pipeline.stateful:
                self.state = StateStore(self.settings.state_db_path(pipeline.id))
                ctx._state = self.state
                healed = self.state.reset_stale_running()
                if healed:
                    ctx.warn(
                        f"Recovered {healed} entity/module state row(s) left running by an "
                        "interrupted run - they will be processed again."
                    )

            self._call_lifecycle("setup", pipeline.setup_fn, ctx)
            self._run_tasks(ctx, "pre")
            if pipeline.stateful:
                self._discover(ctx)
                self._run_modules(ctx)
            self._run_tasks(ctx, "post")

        except StopRequested:
            self.result.status = "stopped"
            self.writer.log("Run stopped by operator.", level="warning")
        except AbortRun as exc:
            self.result.status = "succeeded" if exc.ok else "failed"
            self.result.error = None if exc.ok else f"Run aborted: {exc.reason}"
            self.writer.log(
                f"Run aborted: {exc.reason}", level="info" if exc.ok else "error"
            )
        except BaseException as exc:  # noqa: BLE001
            self.result.status = "failed"
            self.result.error = f"{type(exc).__name__}: {exc}"
            self.writer.log(truncate(traceback.format_exc()), level="error")
        finally:
            self._stop_sampling.set()
            try:
                self._call_lifecycle("teardown", pipeline.teardown_fn, ctx, swallow=True)
                for name in ctx.close_resources():
                    self.writer.log(
                        f"resource {name!r} did not close cleanly", level="warning"
                    )
            finally:
                if self.state is not None:
                    try:
                        if self.result.status in ("stopped", "failed"):
                            self.state.reset_stale_running(self.run_id)
                        self._collect_state_metrics()
                    finally:
                        self.state.close()

        merged = dict(self._merged_metrics)
        for key, value in ctx.metrics.items():
            merged[key] = merged.get(key, 0) + value
        self._counters.update({"counters": merged})
        self._counters["duration_ms"] = elapsed_ms(self._t0, time.perf_counter())
        try:
            self._counters["resources"] = self._sampler.sample()
        except Exception:  # pragma: no cover
            pass
        self._counters["steps"] = self._step_stats
        # Only when something is actually cached: an empty list in every run's
        # metrics is noise.
        from graetl.sdk import caching

        cache_stats = [row for row in caching.stats() if row["hits"] or row["misses"] or row["skipped"]]
        if cache_stats:
            self._counters["caches"] = cache_stats
        self.result.metrics = self._counters
        if self.result.status == "succeeded" and self._counters["failed"] > 0:
            self.result.status = "failed"
            self.result.error = (
                f"{self._counters['failed']} entity/module execution(s) failed - "
                "see the console for details."
            )
        self.writer.emit(
            "run_end",
            {
                "status": self.result.status,
                "error": self.result.error,
                "metrics": self.result.metrics,
            },
        )
        return self.result

    def _sample_resources(self) -> None:
        """Stream RSS/CPU/throughput to the console while the run is alive."""
        interval = 2.0
        while not self._stop_sampling.wait(interval):
            try:
                stats = self._sampler.sample()
                with self._counters_lock:
                    done = (
                        self._counters["processed"]
                        + self._counters["skipped"]
                        + self._counters["failed"]
                    )
                elapsed = max(time.perf_counter() - self._t0, 1e-6)
                with self._counters_lock:
                    stats.update(
                        {
                            "processed": self._counters["processed"],
                            "skipped": self._counters["skipped"],
                            "failed": self._counters["failed"],
                            "entities_total": self._counters.get("entities_total", 0),
                        }
                    )
                stats.update(
                    {
                        "elapsed_s": round(elapsed, 1),
                        "entities_done": done,
                        "entities_per_s": round(done / elapsed, 2),
                        "parallel": self._parallelism(),
                    }
                )
                self.writer.emit("resource", stats)
            except Exception:  # pragma: no cover - telemetry never breaks a run
                pass

    # ------------------------------------------------------------- lifecycle

    def _call_lifecycle(self, name: str, fn: Any, ctx: EntityContext, *, swallow: bool = False) -> None:
        if fn is None:
            return
        ctx.step = name
        ctx.entity = None
        t0 = time.perf_counter()
        self.writer.emit("step_start", {"step": name, "step_kind": "lifecycle"})
        try:
            fn(ctx)
            self.writer.emit(
                "step_end",
                {
                    "step": name,
                    "step_kind": "lifecycle",
                    "status": "succeeded",
                    "duration_ms": elapsed_ms(t0, time.perf_counter()),
                },
            )
        except Exception as exc:  # noqa: BLE001
            self.writer.emit(
                "step_end",
                {
                    "step": name,
                    "step_kind": "lifecycle",
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                    "duration_ms": elapsed_ms(t0, time.perf_counter()),
                },
            )
            if swallow:
                self.writer.log(f"{name} failed: {exc}", level="error")
                return
            raise
        finally:
            ctx.step = None

    # ------------------------------------------------------------------ tasks

    def _run_tasks(self, ctx: EntityContext, phase: str) -> None:
        only = set(self.params.get("steps") or [])
        for task in self.pipeline.tasks_for(phase):
            if only and task.name not in only:
                continue
            self._guard()
            self._run_task(ctx, task)

    def _run_task(self, ctx: EntityContext, task: Task) -> None:
        ctx.step = task.name
        ctx.entity = None
        t0 = time.perf_counter()
        self.writer.emit(
            "step_start",
            {"step": task.name, "step_kind": "task", "version": task.version, "selected": 1,
             "phase": task.phase},
        )
        status, error = "succeeded", None
        try:
            task.fn(ctx)
            self._counters["processed"] += 1
        except SkipEntity as exc:
            status = "skipped"
            self._counters["skipped"] += 1
            ctx.info(f"Task {task.name} skipped: {exc.reason}")
        except (StopRequested, AbortRun):
            self.writer.emit(
                "step_end",
                {"step": task.name, "step_kind": "task", "status": "stopped",
                 "duration_ms": elapsed_ms(t0, time.perf_counter())},
            )
            raise
        except Exception as exc:  # noqa: BLE001
            status, error = "failed", f"{type(exc).__name__}: {exc}"
            self._counters["failed"] += 1
            ctx.error(f"Task {task.name} failed: {error}")
            self.writer.log(truncate(traceback.format_exc()), level="error", step=task.name)
            if self.params.get("fail_fast", True):
                self._record_step(task.name, "task", t0, status=status, error=error, processed=0,
                                  failed=1)
                raise AbortRun(f"task {task.name} failed") from exc
        finally:
            ctx.step = None
        self._counters["steps_run"] += 1
        self._record_step(
            task.name,
            "task",
            t0,
            status=status,
            error=error,
            processed=1 if status == "succeeded" else 0,
            failed=1 if status == "failed" else 0,
            skipped=1 if status == "skipped" else 0,
        )

    # -------------------------------------------------------------- discovery

    def _discover(self, ctx: EntityContext) -> None:
        pipeline = self.pipeline
        assert self.state is not None
        if pipeline.entities_fn is None:
            self._counters["entities_total"] = self.state.count_entities()
            return

        ctx.step = "discover"
        t0 = time.perf_counter()
        started_at = now_iso()
        self.writer.emit("step_start", {"step": "discover", "step_kind": "discovery"})
        batch: list[tuple[str, str | None, Any, dict]] = []
        seen = 0
        stats = {"new": 0, "changed": 0, "seen": 0}
        try:
            for item in pipeline.entities_fn(ctx) or []:
                if not isinstance(item, Entity):
                    raise TypeError(
                        "entity discovery must yield graetl.sdk.Entity objects, "
                        f"got {type(item).__name__}"
                    )
                batch.append(item.normalized())
                seen += 1
                if len(batch) >= 500:
                    self._flush_entities(batch, stats)
                    batch.clear()
                    self._guard()
                    ctx.progress(seen, None, message="discovering entities")
            if batch:
                self._flush_entities(batch, stats)
        except (StopRequested, AbortRun):
            raise
        except Exception as exc:  # noqa: BLE001
            self._record_step("discover", "discovery", t0, status="failed",
                              error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            ctx.step = None

        if pipeline.soft_delete_missing_entities:
            removed = self.state.soft_delete_unseen(started_at)
            if removed:
                ctx.warn(f"{removed} entity/entities disappeared from the source (soft-deleted).")
                self._counters["entities_removed"] = removed

        self._counters["entities_new"] = stats["new"]
        self._counters["entities_changed"] = stats["changed"]
        self._counters["entities_seen"] = stats["seen"]
        self._counters["entities_total"] = self.state.count_entities()
        self._record_step(
            "discover",
            "discovery",
            t0,
            status="succeeded",
            processed=stats["seen"],
            metrics={"new": stats["new"], "changed": stats["changed"],
                     "total": self._counters["entities_total"]},
        )
        ctx.info(
            f"Discovered {stats['seen']} entities "
            f"({stats['new']} new, {stats['changed']} changed, "
            f"{self._counters['entities_total']} known in total)."
        )

    def _flush_entities(self, batch: list, stats: dict) -> None:
        assert self.state is not None
        res = self.state.upsert_entities(batch)
        for key in ("new", "changed", "seen"):
            stats[key] += res[key]

    # -------------------------------------------------------------- modules

    def _parallelism(self) -> int:
        """How many modules of one layer may run at once."""
        value = self.params.get("parallel")
        if value is None:
            value = self.loaded.config.get("options", {}).get("parallel_modules")
        if value is None:
            value = self.pipeline.parallel_modules
        if value is None:
            value = getattr(self.settings, "parallel_modules", 1)
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return 1

    @staticmethod
    def _sub_levels(modules: list[Module]) -> list[list[Module]]:
        """Split one layer into batches that may run side by side.

        Modules in the same layer are normally independent, but ``depends_on``
        edges inside a layer are allowed - those modules land in later batches.
        """
        remaining = list(modules)
        names = {m.name for m in modules}
        done: set[str] = set()
        batches: list[list[Module]] = []
        while remaining:
            ready = [m for m in remaining if not (set(m.depends_on) & names - done)]
            if not ready:  # pragma: no cover - ordered_modules() rejects cycles
                ready = remaining
            batches.append(ready)
            done.update(m.name for m in ready)
            remaining = [m for m in remaining if m not in ready]
        return batches

    def _run_modules(self, ctx: EntityContext) -> None:
        assert self.state is not None
        modules = self.pipeline.ordered_modules()
        only = set(self.params.get("steps") or [])
        if only:
            modules = [m for m in modules if m.name in only]
        entity_filter = self.params.get("entity_ids") or None

        if only:
            self._check_upstream(ctx, modules, only)

        if self.pipeline.execution == "entity-major":
            if self._parallelism() > 1:
                ctx.warn(
                    "execution='entity-major' walks each entity through the whole chain, "
                    "so modules cannot run in parallel - continuing sequentially."
                )
            self._run_entity_major(ctx, modules, entity_filter)
            return

        parallel = self._parallelism()
        self._tx_mode = "deferred" if parallel > 1 else "immediate"

        for layer, group in groupby(modules, key=lambda m: m.execution_layer):
            layer_modules = list(group)
            for batch in self._sub_levels(layer_modules):
                self._guard()
                width = min(parallel, len(batch))
                if width > 1:
                    ctx.info(
                        f"Execution layer {layer}: running {len(batch)} module(s) "
                        f"{width} at a time - " + ", ".join(m.name for m in batch)
                    )
                self._run_batch(ctx, batch, entity_filter, width)

    def _run_batch(
        self,
        ctx: EntityContext,
        batch: list[Module],
        entity_filter: list[str] | None,
        width: int,
    ) -> None:
        """Run one set of independent modules, each in its own worker."""
        if width <= 1:
            for module in batch:
                self._guard()
                self._run_module_worker(ctx, module, entity_filter)
            return

        errors: list[BaseException] = []
        with ThreadPoolExecutor(max_workers=width, thread_name_prefix="graetl-module") as pool:
            futures = {
                pool.submit(self._run_module_worker, ctx, module, entity_filter): module
                for module in batch
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except BaseException as exc:  # noqa: BLE001 - re-raised below
                    errors.append(exc)
        # A stop or an abort wins over an ordinary module failure.
        for exc in errors:
            if isinstance(exc, (StopRequested, AbortRun)):
                raise exc
        if errors:
            raise errors[0]

    def _run_module_worker(
        self, base_ctx: EntityContext, module: Module, entity_filter: list[str] | None
    ) -> None:
        """One module, one worker: own connection, own context, exclusive lock."""
        t0 = time.perf_counter()
        state = StateStore(self.settings.state_db_path(self.pipeline.id))
        ctx = self._module_context(base_ctx, module, state)
        try:
            try:
                owner = state.acquire_module_lock(module.name, run_id=self.run_id)
            except LockConflict as exc:
                ctx.error(f"[{module.name}] {exc}")
                self._record_step(
                    module.name, "module", t0, status="failed",
                    error=str(exc), version=module.version,
                )
                with self._counters_lock:
                    self._counters["steps_run"] += 1
                    self._counters["lock_conflicts"] = (
                        self._counters.get("lock_conflicts", 0) + 1
                    )
                raise AbortRun(f"module {module.name} is locked by another worker") from exc
            try:
                self._run_module(ctx, module, entity_filter, state, owner)
            finally:
                state.release_module_lock(module.name, owner)
        finally:
            failed = ctx.close_resources()
            for name in failed:
                base_ctx.warn(f"[{module.name}] resource {name!r} did not close cleanly")
            with self._counters_lock:
                for key, value in ctx.metrics.items():
                    self._merged_metrics[key] = self._merged_metrics.get(key, 0) + value
            state.close()

    def _module_context(
        self, base: EntityContext, module: Module, state: StateStore
    ) -> EntityContext:
        """A context of this worker's own: own state connection, own metrics."""
        return EntityContext(
            pipeline_id=base.pipeline_id,
            pipeline_title=base.pipeline_title,
            run_id=base.run_id,
            mode=base.mode,
            dir=base.dir,
            config=base.config,
            params=base.params,
            resources=base.resources,  # shared on purpose: values put there by setup()
            stateful=base.stateful,
            _emit=base._emit,
            _control=base._control,
            _state=state,
            _functions=base._functions,
            _resource_factories=base._resource_factories,
            debug_enabled=base.debug_enabled,
            worker=module.name,
            step=module.name,
        )

    def _check_upstream(self, ctx: EntityContext, modules, only: set[str]) -> None:
        """Running a subset: report lower layers / dependencies that are behind.

        Executing a single module for debugging is deliberate, so GraETL does not
        silently pull its upstream in. It does check whether everything the module
        depends on is up to date, and says so when it is not.
        """
        assert self.state is not None
        by_name = {m.name: m for m in self.pipeline.modules}
        pending: dict[str, int] = {}
        for module in modules:
            for name, version in self.pipeline.requirements_for(module):
                if name in only or name in pending:
                    continue
                upstream = by_name[name]
                count = self.state.count_work(
                    name,
                    version,
                    mode="incremental",
                    requires=self.pipeline.requirements_for(upstream),
                    cascade=self.pipeline.cascade,
                )
                if count:
                    pending[name] = count
                    ctx.warn(
                        f"Upstream module {name!r} (execution_layer "
                        f"{upstream.execution_layer}) has {count} entity/entities pending - "
                        "this run does not execute it, so results may be based on stale data."
                    )
        if pending:
            with self._counters_lock:
                self._counters["waiting_for_upstream"] = sum(pending.values())
                self._counters["upstream_pending"] = pending

    def _select(
        self,
        module: Module,
        entity_filter: list[str] | None,
        state: StateStore | None = None,
        *,
        ctx=None,
        after: str | None = None,
        limit: int | None = None,
        order: str = "entity_id",
    ):
        store = state or self.state
        assert store is not None
        requires = self.pipeline.requirements_for(module)
        items = store.select_work(
            module.name,
            module.version,
            mode=self.mode,
            requires=requires,
            cascade=self.pipeline.cascade,
            entity_ids=entity_filter,
            after=after,
            limit=limit,
            order=order,
        )
        if requires and ctx is not None and after is None:
            # Entities this module is due for, but whose lower layers / dependencies
            # have not caught up. Worth saying out loud when a single module is run.
            waiting = store.count_work(
                module.name, module.version, mode=self.mode,
                requires=requires, cascade=self.pipeline.cascade,
                entity_ids=entity_filter, apply_requirements=False,
            ) - store.count_work(
                module.name, module.version, mode=self.mode,
                requires=requires, cascade=self.pipeline.cascade,
                entity_ids=entity_filter,
            )
            if waiting > 0:
                names = ", ".join(f"{n} v{v}" for n, v in requires)
                ctx.warn(
                    f"[{module.name}] {waiting} entity/entities are waiting for upstream "
                    f"work ({names}). Run the whole pipeline to bring them up to date."
                )
                with self._counters_lock:
                    self._counters["waiting_for_upstream"] = (
                        self._counters.get("waiting_for_upstream", 0) + waiting
                    )
        max_attempts = self.pipeline.max_attempts
        if self.mode == "incremental" and max_attempts > 0:
            blocked = {
                i.entity_id
                for i in items
                if i.previous_status == "failed" and i.attempts >= max_attempts
            }
            if blocked:
                items = [i for i in items if i.entity_id not in blocked]
                with self._counters_lock:
                    self._counters["blocked"] = self._counters.get("blocked", 0) + len(blocked)
        return items

    def _run_module(
        self,
        ctx: EntityContext,
        module: Module,
        entity_filter: list[str] | None,
        state: StateStore | None = None,
        lock_owner: str | None = None,
    ) -> None:
        store = state or self.state
        assert store is not None
        # A once-module has nothing to select: it runs when its layer comes up.
        if module.scope == "once":
            self._run_once_module(ctx, module, store)
            return
        t0 = time.perf_counter()
        batch_size = module.batch_size or self.batch_size

        # Entities are claimed in batches, paging forward by entity id, so a run
        # never materialises the whole backlog and always terminates: an entity
        # that fails stays behind the cursor instead of being picked up again.
        sample_limit = self.params.get("limit_entities")
        available = store.count_work(
            module.name,
            module.version,
            mode=self.mode,
            requires=self.pipeline.requirements_for(module),
            cascade=self.pipeline.cascade,
            entity_ids=entity_filter,
        )
        if sample_limit:
            total = min(int(sample_limit), available)
            ctx.info(
                f"sample run: {total} of {available} pending entity/entities "
                f"({self.params.get('sample', 'first')})"
            )
        else:
            total = available
        # the gated/ungated warning, once per module
        self._select(module, entity_filter, store, ctx=ctx, limit=1)
        with self._counters_lock:
            self._counters["work_items"] += total
        self.writer.emit(
            "step_start",
            {"step": module.name, "step_kind": "module", "version": module.version,
             "selected": total},
        )
        processed = skipped = failed = contended = 0
        seen = 0
        error: str | None = None
        status = "succeeded"
        cursor: str | None = None
        last_beat = time.monotonic()
        try:
            while True:
                self._guard()
                if sample_limit:
                    items = self._select(
                        module,
                        entity_filter,
                        store,
                        limit=int(sample_limit),
                        order="random" if self.params.get("sample") == "random" else "entity_id",
                    )
                else:
                    items = self._select(
                        module, entity_filter, store, after=cursor, limit=batch_size
                    )
                if not items:
                    break
                if module.scope == "batch":
                    outcome = self._run_entity_batch(ctx, module, items, store)
                    seen += len(items)
                    if outcome == STATUS_DONE:
                        processed += len(items)
                        key = "processed"
                    elif outcome == STATUS_SKIPPED:
                        skipped += len(items)
                        key = "skipped"
                    elif outcome == "contended":
                        contended += len(items)
                        key = None
                    else:
                        failed += len(items)
                        key = "failed"
                    if key:
                        with self._counters_lock:
                            self._counters[key] += len(items)
                    ctx.progress(seen, max(total, seen))
                    if lock_owner and time.monotonic() - last_beat > 15:
                        store.heartbeat_module_lock(module.name, lock_owner)
                        last_beat = time.monotonic()
                    if sample_limit:
                        break
                    cursor = items[-1].entity_id
                    continue
                for item in items:
                    self._guard()
                    outcome = self._run_entity(ctx, module, item, store)
                    seen += 1
                    key = {
                        STATUS_DONE: "processed",
                        STATUS_SKIPPED: "skipped",
                        "failed": "failed",
                    }.get(outcome)
                    if outcome == STATUS_DONE:
                        processed += 1
                    elif outcome == STATUS_SKIPPED:
                        skipped += 1
                    elif outcome == "failed":
                        failed += 1
                    elif outcome == "contended":
                        contended += 1
                    if key:
                        # Live counters: the UI shows progress while the module runs.
                        with self._counters_lock:
                            self._counters[key] += 1
                    if seen % 25 == 0 or seen == total:
                        ctx.progress(seen, max(total, seen))
                    if lock_owner and time.monotonic() - last_beat > 15:
                        store.heartbeat_module_lock(module.name, lock_owner)
                        last_beat = time.monotonic()
                if sample_limit:
                    break
                cursor = items[-1].entity_id
            if failed:
                status = "failed"
                error = f"{failed} of {seen} entities failed"
            if contended:
                ctx.warn(
                    f"[{module.name}] {contended} entity/entities hit write contention and "
                    "stayed pending - the next run picks them up."
                )
        except (StopRequested, AbortRun):
            status = "stopped"
            raise
        finally:
            ctx.entity = None
            with self._counters_lock:
                self._counters["steps_run"] += 1
                if contended:
                    self._counters["contended"] = (
                        self._counters.get("contended", 0) + contended
                    )
            self._record_step(
                module.name,
                "module",
                t0,
                status=status,
                error=error,
                selected=max(total, seen),
                processed=processed,
                skipped=skipped,
                failed=failed,
                version=module.version,
            )

    def _run_once_module(
        self, ctx: EntityContext, module: Module, store: StateStore
    ) -> None:
        """A once-module: ``fn(ctx)``, one call, no entity, no entity state.

        It sits in the layer order like any module, so everything below its
        layer is up to date by the time it runs - which is the point, because
        the work it does is usually about the table as a whole. Having no entity
        state, it runs on every run; nothing is left to resume.
        """
        t0 = time.perf_counter()
        status = "succeeded"
        error: str | None = None
        self.writer.emit(
            "step_start",
            {"step": module.name, "step_kind": "module", "version": module.version,
             "selected": 1, "scope": "once"},
        )
        ctx.entity = None
        try:
            # Its writes go through ctx.db like any module's, so they need a
            # transaction of their own to commit in.
            store.begin(self._tx_mode)
            try:
                module.fn(ctx)
                store.commit()
            except BaseException:
                store.rollback()
                raise
        except (StopRequested, AbortRun):
            status = "stopped"
            raise
        except SkipEntity as exc:
            status = "succeeded"
            ctx.info(f"[{module.name}] skipped: {exc.reason}")
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            error = f"{type(exc).__name__}: {exc}"
            with self._counters_lock:
                self._counters["failed"] += 1
            ctx.error(f"[{module.name}] {error}")
            self.writer.log(truncate(traceback.format_exc()), level="error", step=module.name)
            if self.params.get("fail_fast"):
                raise AbortRun(f"module {module.name} failed") from exc
        else:
            with self._counters_lock:
                self._counters["processed"] += 1
        finally:
            with self._counters_lock:
                self._counters["steps_run"] += 1
                self._counters["work_items"] += 1
            self._record_step(
                module.name, "module", t0, status=status, error=error,
                selected=1, processed=1 if status == "succeeded" else 0,
                skipped=0, failed=1 if status == "failed" else 0,
                version=module.version,
            )

    def _run_entity_batch(
        self, ctx: EntityContext, module: Module, items: list[Any], store: StateStore
    ) -> str:
        """A batch-module: ``fn(ctx, entities)``, one transaction for the lot.

        Every entity in the batch is marked done together or none is, so a
        failure leaves the whole batch pending for the next run. That is the
        price of one transaction, and it is what makes a batch safe.
        """
        t0 = time.perf_counter()
        entities = [
            Entity(
                id=item.entity_id,
                source_updated_at=item.source_updated_at,
                label=item.label,
                data=item.payload,
            )
            for item in items
        ]
        attempts = max(1, int(self.settings.lock_retries))
        for attempt in range(attempts):
            try:
                with store.batch_transaction(
                    items, module.name, module.version,
                    run_id=self.run_id, mode=self._tx_mode,
                ) as outcome:
                    try:
                        result = module.fn(ctx, entities)
                        outcome["status"] = STATUS_DONE
                        outcome["result"] = (
                            result if isinstance(result, (dict, list, str, int, float)) else None
                        )
                    except SkipEntity as exc:
                        outcome["status"] = STATUS_SKIPPED
                        outcome["result"] = {"reason": exc.reason}
                    finally:
                        outcome["duration_ms"] = elapsed_ms(t0, time.perf_counter())
                status = outcome["status"]
                self.writer.emit(
                    "entity",
                    {"entity_id": f"{len(items)} entities", "step": module.name,
                     "status": status, "duration_ms": outcome["duration_ms"]},
                )
                return status
            except LockConflict:
                if attempt == attempts - 1:
                    ctx.warn(
                        f"[{module.name}] batch of {len(items)} lost the write race after "
                        f"{attempts} attempts - left pending"
                    )
                    return "contended"
                time.sleep(min(0.05 * (2**attempt), 1.0) * (0.5 + random.random()))
            except (StopRequested, AbortRun):
                raise
            except Exception as exc:  # noqa: BLE001
                message = f"{type(exc).__name__}: {exc}"
                self.writer.emit(
                    "entity",
                    {"entity_id": f"{len(items)} entities", "step": module.name,
                     "status": "failed", "duration_ms": elapsed_ms(t0, time.perf_counter()),
                     "error": message},
                )
                ctx.error(f"[{module.name}] batch of {len(items)}: {message}")
                self.writer.log(
                    truncate(traceback.format_exc()), level="error", step=module.name
                )
                if self.params.get("fail_fast"):
                    raise AbortRun(f"module {module.name} failed on a batch") from exc
                return "failed"
        return "contended"  # pragma: no cover - loop always returns

    def _run_entity_major(self, ctx: EntityContext, modules, entity_filter) -> None:
        """Process each entity through the whole module chain before moving on."""
        assert self.state is not None
        self._tx_mode = "immediate"
        pending: dict[str, Any] = {}
        order: list[str] = []
        for module in modules:
            for item in self._select(module, entity_filter):
                if item.entity_id not in pending:
                    pending[item.entity_id] = item
                    order.append(item.entity_id)
        self._counters["work_items"] += len(order)
        totals = {m.name: {"processed": 0, "skipped": 0, "failed": 0, "t0": time.perf_counter()}
                  for m in modules}
        for module in modules:
            self.writer.emit(
                "step_start",
                {"step": module.name, "step_kind": "module", "version": module.version,
                 "selected": len(order)},
            )
        try:
            for index, entity_id in enumerate(order, start=1):
                self._guard()
                item = pending[entity_id]
                for module in modules:
                    selected = self.state.select_work(
                        module.name, module.version, mode=self.mode,
                        requires=self.pipeline.requirements_for(module),
                        cascade=self.pipeline.cascade,
                        entity_ids=[entity_id],
                    )
                    if not selected:
                        continue
                    ctx.step = module.name
                    outcome = self._run_entity(ctx, module, selected[0], self.state)
                    bucket = totals[module.name]
                    if outcome == STATUS_DONE:
                        bucket["processed"] += 1
                    elif outcome == STATUS_SKIPPED:
                        bucket["skipped"] += 1
                    elif outcome == "failed":
                        bucket["failed"] += 1
                        break  # downstream modules cannot run for this entity
                ctx.step = None
                ctx.progress(index, len(order), message="entities")
        finally:
            for module in modules:
                bucket = totals[module.name]
                self._counters["processed"] += bucket["processed"]
                self._counters["skipped"] += bucket["skipped"]
                self._counters["failed"] += bucket["failed"]
                self._counters["steps_run"] += 1
                self._record_step(
                    module.name,
                    "module",
                    bucket["t0"],
                    status="failed" if bucket["failed"] else "succeeded",
                    selected=len(order),
                    processed=bucket["processed"],
                    skipped=bucket["skipped"],
                    failed=bucket["failed"],
                    version=module.version,
                )

    def _run_entity(
        self, ctx: EntityContext, module: Module, item: Any, state: StateStore
    ) -> str:
        """Process one entity, retrying only on write contention.

        A retry re-executes the module body against a clean transaction: the
        previous attempt wrote nothing, and no state row was touched. Work done
        outside ``ctx.db`` is at-least-once, exactly as for an interrupted run.
        """
        entity = Entity(
            id=item.entity_id,
            source_updated_at=item.source_updated_at,
            label=item.label,
            data=item.payload,
        )
        ctx.step = module.name
        ctx.entity = entity
        t0 = time.perf_counter()
        state.mark_running(item.entity_id, module.name, module.version, self.run_id)
        attempts = self.lock_retries + 1 if self._tx_mode == "deferred" else 1
        try:
            for attempt in range(attempts):
                try:
                    return self._attempt_entity(ctx, module, item, entity, state, t0)
                except LockConflict:
                    if attempt + 1 >= attempts:
                        ctx.debug(
                            f"[{module.name}] {item.entity_id}: still contended after "
                            f"{attempts} attempts - left pending"
                        )
                        self.writer.emit(
                            "entity",
                            {"entity_id": item.entity_id, "step": module.name,
                             "status": "contended"},
                        )
                        return "contended"
                    time.sleep(min(0.05 * (2**attempt), 1.0) * (0.5 + random.random()))
            return "contended"  # pragma: no cover - loop always returns
        except (StopRequested, AbortRun):
            raise
        except Exception as exc:  # noqa: BLE001
            duration = elapsed_ms(t0, time.perf_counter())
            message = f"{type(exc).__name__}: {exc}"
            self.writer.emit(
                "entity",
                {
                    "entity_id": item.entity_id,
                    "step": module.name,
                    "status": "failed",
                    "duration_ms": duration,
                    "error": message,
                },
            )
            ctx.error(f"[{module.name}] {item.entity_id}: {message}")
            self.writer.log(truncate(traceback.format_exc()), level="error", step=module.name)
            if self.params.get("fail_fast"):
                raise AbortRun(f"module {module.name} failed on {item.entity_id}") from exc
            return "failed"
        finally:
            ctx.entity = None

    def _attempt_entity(
        self,
        ctx: EntityContext,
        module: Module,
        item: Any,
        entity: Entity,
        state: StateStore,
        t0: float,
    ) -> str:
        with state.entity_transaction(
            item.entity_id,
            module.name,
            module.version,
            run_id=self.run_id,
            source_updated_at=item.source_updated_at,
            mode=self._tx_mode,
        ) as outcome:
            try:
                result = module.fn(ctx, entity)
                outcome["status"] = STATUS_DONE
                outcome["result"] = (
                    result if isinstance(result, (dict, list, str, int, float)) else None
                )
            except SkipEntity as exc:
                outcome["status"] = STATUS_SKIPPED
                outcome["result"] = {"reason": exc.reason}
            except RetryEntity as exc:
                outcome["status"] = "pending"
                outcome["result"] = {"reason": exc.reason}
            finally:
                outcome["duration_ms"] = elapsed_ms(t0, time.perf_counter())
        status = outcome["status"]
        self.writer.emit(
            "entity",
            {
                "entity_id": item.entity_id,
                "step": module.name,
                "status": status,
                "duration_ms": outcome["duration_ms"],
            },
        )
        return status

    # ------------------------------------------------------------------ utils

    def _guard(self) -> None:
        if self.control is None:
            return
        self.control.wait_if_paused()
        if self.control.stop_requested:
            raise StopRequested()

    def _record_step(self, step: str, kind: str, t0: float, **fields: Any) -> None:
        payload = {
            "step": step,
            "step_kind": kind,
            "duration_ms": elapsed_ms(t0, time.perf_counter()),
            **fields,
        }
        self._step_stats.append(payload)
        self.writer.emit("step_end", payload)

    def _collect_state_metrics(self) -> None:
        if self.state is None:
            return
        try:
            self._counters["entity_status"] = self.state.status_counts()
            self._counters["module_state"] = self.state.module_summary()
            self._counters["entities_total"] = self.state.count_entities()
            remaining = 0
            for module in self.pipeline.ordered_modules():
                remaining += len(
                    self.state.select_work(
                        module.name, module.version, mode="incremental",
                        requires=self.pipeline.requirements_for(module),
                        cascade=self.pipeline.cascade,
                    )
                )
            self._counters["work_remaining"] = remaining
        except Exception:  # pragma: no cover - metrics must never break a run
            pass


class _NoControl:
    stop_requested = False

    def wait_if_paused(self) -> None:
        return None
