"""The context object handed to every pipeline callback.

``ctx`` is the single door into everything the runtime offers: configuration,
paths, the pipeline's own SQLite connection, structured logging that streams
into the GraETL console, metrics/profiling, and cooperative pause/stop.
"""

from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator

from graetl.sdk.errors import SkipEntity, StopRequested
from graetl.utils import elapsed_ms

Emitter = Callable[[str, dict], None]


class _NullControl:
    def wait_if_paused(self) -> None: ...

    @property
    def stop_requested(self) -> bool:
        return False


@dataclass
class Context:
    """Base context (available in setup/teardown and run-scoped tasks)."""

    pipeline_id: str
    pipeline_title: str
    run_id: int | None
    mode: str
    dir: Path
    config: dict[str, Any]
    params: dict[str, Any] = field(default_factory=dict)
    resources: dict[str, Any] = field(default_factory=dict)
    stateful: bool = False
    _emit: Emitter | None = None
    _control: Any = field(default_factory=_NullControl)
    _state: Any = None
    _functions: dict[str, Callable[..., Any]] = field(default_factory=dict)
    _resource_factories: dict[str, tuple[Callable[..., Any], Any]] = field(default_factory=dict)
    _resource_cache: dict[str, Any] = field(default_factory=dict)
    #: Which worker this context belongs to ("main", or the module name).
    worker: str = "main"
    #: ctx.debug() only reaches the console on a deliberate debug run.
    debug_enabled: bool = False
    _metrics: dict[str, float] = field(default_factory=dict)
    _timers: dict[str, float] = field(default_factory=dict)
    step: str | None = None

    # ------------------------------------------------------------------ paths

    @property
    def data_dir(self) -> Path:
        p = self.dir / "data"
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def cache_dir(self) -> Path:
        p = self.dir / ".cache"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def path(self, *parts: str) -> Path:
        """Resolve a path relative to the pipeline folder."""
        return self.dir.joinpath(*parts)

    # ------------------------------------------------------------------- data

    @property
    def state(self) -> Any:
        """The pipeline's :class:`~graetl.store.state.StateStore` (stateful pipelines)."""
        if self._state is None:
            raise RuntimeError(
                "This pipeline is stateless - no entity state database. "
                "Set Pipeline(stateful=True) to enable it."
            )
        return self._state

    @property
    def db(self) -> sqlite3.Connection:
        """The pipeline state database connection.

        Writes made here inside a module commit atomically together with the
        entity's state row.
        """
        return self.state.conn

    def resource(self, name: str) -> Any:
        """A per-worker resource built by its ``@pipeline.resource`` factory.

        The factory runs once per worker, so parallel modules never share a
        database connection or HTTP session. Compare ``ctx.resources``, a plain
        dict shared by every worker - fine for immutable values, unsafe for
        connections when ``parallel_modules > 1``.
        """
        if name in self._resource_cache:
            return self._resource_cache[name]
        try:
            factory, _close = self._resource_factories[name]
        except KeyError:
            known = ", ".join(sorted(self._resource_factories)) or "none registered"
            raise KeyError(
                f"unknown resource {name!r} - available: {known}. "
                "Register it with @pipeline.resource in pipeline.py."
            ) from None
        value = factory(self)
        self._resource_cache[name] = value
        return value

    def close_resources(self) -> list[str]:
        """Close everything this worker opened. Returns the names that failed."""
        failed: list[str] = []
        for name, value in list(self._resource_cache.items()):
            _factory, close = self._resource_factories.get(name, (None, None))
            try:
                if close is not None:
                    close(value)
                elif hasattr(value, "close"):
                    value.close()
            except Exception:  # noqa: BLE001 - teardown must not break a run
                failed.append(name)
            self._resource_cache.pop(name, None)
        return failed

    def fn(self, name: str) -> Callable[..., Any]:
        """A shared function registered with ``@pipeline.function`` in pipeline.py.

        The same registry backs node-flow graphs, so a helper written once is
        callable from module code and from a visual graph.
        """
        try:
            return self._functions[name]
        except KeyError:
            known = ", ".join(sorted(self._functions)) or "none registered"
            raise KeyError(
                f"unknown pipeline function {name!r} - available: {known}"
            ) from None

    def setting(self, path: str, default: Any = None) -> Any:
        """Read a dotted key out of ``pipeline.toml`` (``ctx.setting('source.dsn')``)."""
        cur: Any = self.config
        for part in path.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur[part]
        return cur

    # ---------------------------------------------------------------- logging

    def log(self, message: str, *, level: str = "info", **data: Any) -> None:
        if level == "debug" and not self.debug_enabled:
            return
        if self._emit:
            self._emit(
                "log",
                {"level": level, "message": str(message), "step": self.step, "data": data or None},
            )

    def debug(self, message: str, **data: Any) -> None:
        """Verbose output - emitted only on a debug run, dropped otherwise."""
        self.log(message, level="debug", **data)

    def info(self, message: str, **data: Any) -> None:
        self.log(message, level="info", **data)

    def warn(self, message: str, **data: Any) -> None:
        self.log(message, level="warning", **data)

    warning = warn

    def error(self, message: str, **data: Any) -> None:
        self.log(message, level="error", **data)

    def success(self, message: str, **data: Any) -> None:
        self.log(message, level="success", **data)

    # ---------------------------------------------------------------- metrics

    def metric(self, name: str, value: float = 1) -> None:
        """Add to a run counter (shown in the run metrics panel)."""
        self._metrics[name] = self._metrics.get(name, 0) + value

    def gauge(self, name: str, value: float) -> None:
        """Set an absolute value."""
        self._metrics[name] = value

    @property
    def metrics(self) -> dict[str, float]:
        return dict(self._metrics)

    @contextmanager
    def timeit(self, name: str) -> Iterator[None]:
        """Profile a block: ``with ctx.timeit('query'): ...``"""
        t0 = time.perf_counter()
        try:
            yield
        finally:
            ms = elapsed_ms(t0, time.perf_counter())
            self.metric(f"time.{name}_ms", ms)
            self.debug(f"{name} took {ms} ms")

    def progress(self, done: int, total: int | None = None, *, message: str | None = None) -> None:
        if self._emit:
            self._emit(
                "progress",
                {"step": self.step, "done": done, "total": total, "message": message},
            )

    # ------------------------------------------------------- pause / stop

    def checkpoint(self) -> None:
        """Cooperative yield point.

        Blocks while the run is paused and raises :class:`StopRequested` when a
        stop was requested. Long-running loops inside a module should call this.
        """
        self._control.wait_if_paused()
        if self._control.stop_requested:
            raise StopRequested("stop requested by operator")

    @property
    def stop_requested(self) -> bool:
        return bool(self._control.stop_requested)

    def skip(self, reason: str = "skipped") -> None:
        raise SkipEntity(reason)


@dataclass
class RunContext(Context):
    """Alias used for run-scoped tasks - same surface as :class:`Context`."""


@dataclass
class EntityContext(Context):
    """Context inside an entity module; adds the current entity."""

    entity: Any = None
