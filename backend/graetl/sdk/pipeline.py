"""Pipeline definition objects."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Iterator, Sequence

from graetl.sdk.errors import PipelineDefinitionError
from graetl.utils import to_iso


@dataclass(slots=True)
class Entity:
    """One unit of work a pipeline knows about.

    ``source_updated_at`` is the *source revision* - the moment the entity last
    changed in the source system. When it moves forward, every module has to
    process the entity again. Any comparable token works (timestamp, version
    number, ETag); timestamps are normalised to UTC ISO-8601.
    """

    id: str
    source_updated_at: Any = None
    label: str | None = None
    data: dict[str, Any] = field(default_factory=dict)

    def normalized(self) -> tuple[str, str | None, str | None, dict[str, Any]]:
        return (str(self.id), self.label, to_iso(self.source_updated_at), dict(self.data))


@dataclass(slots=True)
class Module:
    """An entity-scoped processing step.

    Defined in its own file, ``<name>.module.py``; ``file`` holds that path
    relative to the pipeline folder, ``folder`` its directory and ``meta`` the
    merged contents of ``<name>.module.toml`` and the folder's ``module.toml``.

    Ordering comes from two independent knobs:

    ``execution_layer``
        A coarse stage number. Everything in a lower layer must be up to date
        for an entity before a higher layer touches it - no wiring required.
    ``depends_on``
        Explicit module names, for fine-grained edges inside a layer.
    """

    name: str
    fn: Callable[..., Any]
    version: int = 1
    execution_layer: int = 0
    depends_on: tuple[str, ...] = ()
    description: str = ""
    batch_size: int | None = None
    seq: int = 0
    title: str = ""
    file: str | None = None
    folder: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    source: str = "python"  # 'python' or 'graph'
    #: How the module is called.
    #:
    #: ``entity``  ``fn(ctx, entity)`` - once per entity, one transaction each.
    #:             The default, and what "resumable at entity granularity" means.
    #: ``batch``   ``fn(ctx, entities)`` - a list at a time, in ONE transaction:
    #:             the whole batch is marked done together or not at all.
    #: ``once``    ``fn(ctx)`` - exactly once per run, when its layer comes up,
    #:             with no entity and no entity state. For global work: rebuild
    #:             a summary table, vacuum, export the lot.
    scope: str = "entity"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": "module",
            "source": self.source,
            "scope": self.scope,
            "version": self.version,
            "execution_layer": self.execution_layer,
            "depends_on": list(self.depends_on),
            "title": self.title or self.name,
            "description": self.description,
            "file": self.file,
            "folder": self.folder,
            "meta": self.meta,
            "seq": self.seq,
        }


@dataclass(slots=True)
class Task:
    """A run-scoped step (runs once per run, no entity state)."""

    name: str
    fn: Callable[..., Any]
    version: int = 1
    phase: str = "pre"  # 'pre' (before entity modules) or 'post' (after)
    description: str = ""
    seq: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": "task",
            "source": "python",
            "version": self.version,
            "phase": self.phase,
            "title": self.name,
            "description": self.description,
            "seq": self.seq,
        }


# --------------------------------------------------------------------- loading

_local = threading.local()


def get_pipeline() -> Pipeline:
    """The pipeline currently being loaded.

    This is how a module file reaches its pipeline::

        # modules/vitals/load_vital_signals.module.py
        from graetl.sdk import get_pipeline

        pipeline = get_pipeline()

        @pipeline.module(version=1, execution_layer=10)
        def load_vital_signals(ctx, entity):
            ...
    """
    pipeline = getattr(_local, "pipeline", None)
    if pipeline is None:
        raise PipelineDefinitionError(
            "get_pipeline() is only available while GraETL is loading a pipeline "
            "(inside pipeline.py or a *.module.py file)."
        )
    return pipeline


def current_module_file() -> tuple[str | None, str | None, str | None, dict[str, Any]]:
    """The module file being loaded, as ``(default_name, file, folder, metadata)``."""
    return (
        getattr(_local, "module_name", None),
        getattr(_local, "file", None),
        getattr(_local, "folder", None),
        getattr(_local, "module_meta", {}) or {},
    )


@contextmanager
def loading(
    pipeline: Pipeline,
    *,
    module_name: str | None = None,
    file: str | None = None,
    folder: str | None = None,
    meta: dict[str, Any] | None = None,
) -> Iterator[Pipeline]:
    """Used by the loader to make ``get_pipeline()`` work while executing files."""
    previous = (
        getattr(_local, "pipeline", None),
        getattr(_local, "module_name", None),
        getattr(_local, "file", None),
        getattr(_local, "folder", None),
        getattr(_local, "module_meta", None),
    )
    _local.pipeline = pipeline
    _local.module_name = module_name
    _local.file = file
    _local.folder = folder
    _local.module_meta = meta or {}
    try:
        yield pipeline
    finally:
        (
            _local.pipeline,
            _local.module_name,
            _local.file,
            _local.folder,
            _local.module_meta,
        ) = previous


class Pipeline:
    """A pipeline definition.

    A *stateless* pipeline (``stateful=False``) only runs tasks and records run
    metrics. A *stateful* pipeline additionally maintains an entity state
    database, which is what makes it resumable at entity granularity.
    """

    def __init__(
        self,
        id: str,
        *,
        title: str | None = None,
        description: str = "",
        stateful: bool = False,
        tags: Sequence[str] = (),
        execution: str = "module-major",
        soft_delete_missing_entities: bool = False,
        max_attempts: int = 1,
        cascade: bool = True,
        parallel_modules: int = 1,
    ) -> None:
        self.id = id
        self.title = title or id.replace("_", " ").replace("-", " ").title()
        self.description = description
        self.stateful = stateful
        self.tags = tuple(tags)
        if execution not in ("module-major", "entity-major"):
            raise PipelineDefinitionError("execution must be 'module-major' or 'entity-major'")
        self.execution = execution
        self.soft_delete_missing_entities = soft_delete_missing_entities
        self.max_attempts = max(1, int(max_attempts))
        #: Reprocess an entity when an upstream module processed it more recently -
        #: so bumping a module's version refreshes everything derived from it.
        self.cascade = cascade
        #: How many modules of the same execution layer may run at once. Each gets
        #: its own worker and its own database connection; a module is never run by
        #: more than one worker. 1 = strictly sequential.
        self.parallel_modules = max(1, int(parallel_modules))

        self.modules: list[Module] = []
        self.tasks: list[Task] = []
        self.functions: dict[str, Callable[..., Any]] = {}
        #: name -> (factory, close) - built once per worker, see Context.resource().
        self.resource_factories: dict[str, tuple[Callable[..., Any], Callable[[Any], None] | None]] = {}
        self.setup_fn: Callable[..., Any] | None = None
        self.teardown_fn: Callable[..., Any] | None = None
        self.entities_fn: Callable[..., Iterable[Entity]] | None = None
        #: Filled in by the loader: node-flow assets found on disk.
        self.assets: dict[str, Any] = {
            "graphlibs": [],
            "module_folders": [],
            "pending_modules": [],
        }
        self._seq = 0

    # ----------------------------------------------------------- registration

    def setup(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        """Open connections / load resources once per run, before anything else."""
        self.setup_fn = fn
        return fn

    def teardown(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        """Always called at the end of a run, even after failures."""
        self.teardown_fn = fn
        return fn

    def entities(self, fn: Callable[..., Iterable[Entity]]) -> Callable[..., Iterable[Entity]]:
        """Entity discovery: yield :class:`Entity` objects (stateful pipelines)."""
        self.entities_fn = fn
        self.stateful = True
        return fn

    # alias
    source = entities

    def function(
        self,
        name: str | None = None,
        *,
        description: str = "",
        category: str = "",
        title: str = "",
        pure: bool = False,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a helper that modules and node graphs can both call.

        Defined in ``pipeline.py``, reachable from any module as
        ``ctx.fn("name")(...)`` - and, because this registry is also the node
        registry, available as a node on any graph.

        ``pure=True`` marks the function as free of side effects. A pure node
        has no execution pins and compiles to an expression inlined where it is
        used, which is what keeps generated code readable; mark something pure
        only if calling it twice, or not at all, changes nothing.
        ``category`` groups it in the editor's node palette.
        """

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            key = name or fn.__name__
            if key in self.functions:
                raise PipelineDefinitionError(f"duplicate pipeline function: {key!r}")
            fn.graetl_description = description or (fn.__doc__ or "").strip().split("\n")[0]  # type: ignore[attr-defined]
            fn.graetl_category = category  # type: ignore[attr-defined]
            fn.graetl_title = title  # type: ignore[attr-defined]
            fn.graetl_pure = bool(pure)  # type: ignore[attr-defined]
            self.functions[key] = fn
            return fn

        # Allow bare @pipeline.function without parentheses.
        if callable(name):  # pragma: no cover - defensive convenience
            fn, name = name, None
            return decorator(fn)
        return decorator

    def register_function(self, fn: Callable[..., Any], *, name: str | None = None) -> str:
        """Register an already-annotated callable. Used by node libraries.

        The annotations (``graetl_pure`` and friends) are read off the function,
        which is what lets a ``*.nodes.py`` file register a plain ``def`` with no
        decorator at all.
        """
        key = name or getattr(fn, "graetl_name", None) or fn.__name__
        if key in self.functions:
            raise PipelineDefinitionError(f"duplicate pipeline function: {key!r}")
        self.functions[key] = fn
        return key

    def resource(
        self, name: str, *, close: Callable[[Any], None] | None = None
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a factory for something each worker needs its own copy of.

        Database connections, HTTP sessions and file handles are usually not
        safe to share between threads. A resource factory is called **once per
        worker**, lazily, and the result is reachable as ``ctx.resource("name")``::

            @pipeline.resource("source", close=lambda conn: conn.close())
            def source(ctx):
                return sqlite3.connect(ctx.setting("source.dsn"))

        With ``parallel_modules=1`` this behaves exactly like a value stashed in
        ``ctx.resources``; with more, every module gets its own instance.
        """

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            if name in self.resource_factories:
                raise PipelineDefinitionError(f"duplicate resource: {name!r}")
            self.resource_factories[name] = (fn, close)
            return fn

        return decorator

    def module(
        self,
        name: str | None = None,
        *,
        version: int = 1,
        execution_layer: int | None = None,
        depends_on: Sequence[str] | str = (),
        title: str = "",
        description: str = "",
        batch_size: int | None = None,
        scope: str = "entity",
        meta: dict[str, Any] | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a module. By default entity-scoped: ``fn(ctx, entity)``.

        Inside ``<name>.module.py`` the name may be omitted - the file name is
        used. Bump ``version`` whenever the module's logic changes: every entity
        is then reprocessed by this module (and only this module) on the next
        run.

        ``execution_layer`` is the simple ordering knob: before this module
        touches an entity, every module in a *lower* layer must be up to date
        for it. ``depends_on`` adds explicit edges on top of that.

        ``scope`` picks how the function is called::

            scope="entity"   def fn(ctx, entity)      once per entity (default)
            scope="batch"    def fn(ctx, entities)    a list at a time
            scope="once"     def fn(ctx)              once per run, no entity

        A **batch** is one transaction: every entity in it is marked done
        together, or none is and the batch is retried next run. ``batch_size``
        sets how many at a time (default: the configured ``entity_batch_size``).

        A **once** module runs exactly once when its layer comes up - after
        everything below that layer is up to date - and keeps no entity state,
        so it runs on every run. It is the right shape for work that is about
        the whole table rather than one row.
        """
        if scope not in ("entity", "batch", "once"):
            raise PipelineDefinitionError(
                f"module scope must be 'entity', 'batch' or 'once', not {scope!r}"
            )
        deps = (depends_on,) if isinstance(depends_on, str) else tuple(depends_on)

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            default_name, file, folder, file_meta = current_module_file()
            resolved = name or default_name or fn.__name__
            if any(m.name == resolved for m in self.modules):
                raise PipelineDefinitionError(f"duplicate module name: {resolved!r}")
            merged: dict[str, Any] = {**file_meta, **(meta or {})}
            layer = execution_layer
            if layer is None:
                layer = int(merged.get("execution_layer", 0) or 0)
            self._seq += 1
            self.modules.append(
                Module(
                    name=resolved,
                    fn=fn,
                    version=int(version),
                    execution_layer=int(layer),
                    depends_on=deps or tuple(merged.get("depends_on", ()) or ()),
                    title=title or str(merged.get("title", "")),
                    description=(
                        description
                        or str(merged.get("description", ""))
                        or (fn.__doc__ or "").strip().split("\n")[0]
                    ),
                    batch_size=batch_size,
                    scope=scope,
                    seq=self._seq,
                    file=file,
                    folder=folder,
                    meta=merged,
                )
            )
            self.stateful = True
            return fn

        return decorator

    def task(
        self,
        name: str | None = None,
        *,
        version: int = 1,
        phase: str = "pre",
        description: str = "",
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a run-scoped task: ``def fn(ctx) -> dict | None``."""
        if phase not in ("pre", "post"):
            raise PipelineDefinitionError("phase must be 'pre' or 'post'")

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            resolved = name or fn.__name__
            if any(t.name == resolved for t in self.tasks):
                raise PipelineDefinitionError(f"duplicate task name: {resolved!r}")
            self._seq += 1
            self.tasks.append(
                Task(
                    name=resolved,
                    fn=fn,
                    version=int(version),
                    phase=phase,
                    description=description or (fn.__doc__ or "").strip().split("\n")[0],
                    seq=self._seq,
                )
            )
            return fn

        return decorator

    # ------------------------------------------------------------- inspection

    def ordered_modules(self) -> list[Module]:
        """Modules in dependency order (stable, deterministic)."""
        by_name = {m.name: m for m in self.modules}
        for m in self.modules:
            for dep in m.depends_on:
                if dep not in by_name:
                    raise PipelineDefinitionError(
                        f"module {m.name!r} depends on unknown module {dep!r}"
                    )
        ordered: list[Module] = []
        placed: set[str] = set()
        visiting: set[str] = set()

        def visit(mod: Module) -> None:
            if mod.name in placed:
                return
            if mod.name in visiting:
                raise PipelineDefinitionError(f"dependency cycle around module {mod.name!r}")
            visiting.add(mod.name)
            for dep in mod.depends_on:
                visit(by_name[dep])
            visiting.discard(mod.name)
            placed.add(mod.name)
            ordered.append(mod)

        for mod in sorted(self.modules, key=lambda m: (m.execution_layer, m.seq)):
            visit(mod)
        # Stable sort keeps the dependency order inside each layer.
        return sorted(ordered, key=lambda m: m.execution_layer)

    def requirements_for(self, module: Module) -> list[tuple[str, int]]:
        """Modules that must be up to date for an entity before ``module`` runs.

        That is every module in a strictly lower execution layer, plus the
        explicit ``depends_on`` edges - each with the version it must have been
        processed at.

        A ``scope="once"`` module is never a requirement: it keeps no entity
        state, so "is it up to date for this entity" has no answer, and asking
        would gate every downstream entity forever. Ordering still holds - the
        layer barrier runs it before the layers above - but it is not a per-
        entity gate.
        """
        by_name = {m.name: m for m in self.modules}
        required: dict[str, int] = {}
        for other in self.modules:
            if other.name == module.name or other.scope == "once":
                continue
            if other.execution_layer < module.execution_layer:
                required[other.name] = other.version
        for dep in module.depends_on:
            target = by_name.get(dep)
            if target is not None and target.scope != "once":
                required[target.name] = target.version
        return sorted(required.items())

    def layers(self) -> list[int]:
        return sorted({m.execution_layer for m in self.modules})

    def tasks_for(self, phase: str) -> list[Task]:
        return [t for t in sorted(self.tasks, key=lambda t: t.seq) if t.phase == phase]

    def validate(self) -> None:
        self.ordered_modules()
        by_name = {m.name: m for m in self.modules}
        for module in self.modules:
            for dep in module.depends_on:
                target = by_name[dep]
                if target.execution_layer > module.execution_layer:
                    raise PipelineDefinitionError(
                        f"module {module.name!r} (execution_layer "
                        f"{module.execution_layer}) depends on {dep!r} in the higher "
                        f"layer {target.execution_layer} - lower layers always run first, "
                        "so this can never be satisfied."
                    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "stateful": self.stateful,
            "tags": list(self.tags),
            "execution": self.execution,
            "max_attempts": self.max_attempts,
            "cascade": self.cascade,
            "parallel_modules": self.parallel_modules,
            "resources": sorted(self.resource_factories),
            "has_entity_source": self.entities_fn is not None,
            "has_setup": self.setup_fn is not None,
            "has_teardown": self.teardown_fn is not None,
            "layers": self.layers(),
            "modules": [
                {**m.to_dict(), "requires": [n for n, _ in self.requirements_for(m)]}
                for m in self.ordered_modules()
            ],
            "tasks": [t.to_dict() for t in sorted(self.tasks, key=lambda t: t.seq)],
            "functions": [
                {
                    "name": key,
                    "description": getattr(fn, "graetl_description", ""),
                    "category": getattr(fn, "graetl_category", ""),
                    "pure": bool(getattr(fn, "graetl_pure", False)),
                }
                for key, fn in sorted(self.functions.items())
            ],
            "node_libraries": list(self.assets.get("node_libraries", [])),
            "graphlibs": list(self.assets.get("graphlibs", [])),
            "module_folders": list(self.assets.get("module_folders", [])),
            # .graph files: modules waiting for the node-flow runtime.
            "pending_modules": list(self.assets.get("pending_modules", [])),
        }

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<Pipeline {self.id!r} stateful={self.stateful} "
            f"modules={len(self.modules)} tasks={len(self.tasks)}>"
        )


# ------------------------------------------------------------- node libraries
#
# A file named ``<name>.nodes.py`` needs no decorator: every public top-level
# function in it becomes a node. ``@node`` exists only to override what the
# loader would otherwise infer, or to keep a helper out of the palette.


def node(
    name: str | None = None,
    *,
    title: str = "",
    description: str = "",
    category: str = "",
    pure: bool | None = None,
    cache: bool | int = False,
    skip: bool = False,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Annotate a function in a ``*.nodes.py`` file.

    Nothing is registered here - the loader sweeps the file afterwards and
    registers what it finds. This only records the decisions::

        @node(category="Vitals")
        def bmi(weight_kg, height_m):
            return weight_kg / (height_m ** 2)

        @node(pure=False)            # it writes somewhere
        def stamp(ctx, entity): ...

        @node(cache=True)            # the same arguments always mean the same
        def unit_of(ctx, code):      # answer - look it up once per run
            return ctx.db.execute(...).fetchone()[0]

        @node(skip=True)             # a helper, not a node
        def _internal(x): ...

    Purity is otherwise inferred: a function that takes ``ctx`` as its first
    parameter is impure (it can reach the database, the log and the run), and
    one that does not is pure. ``pure=`` overrides that either way.

    ``cache=True`` memoises the result per process (``cache=1000`` sets the
    number of entries). ``ctx`` is never part of the key. See
    :mod:`graetl.sdk.caching`.
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        fn.graetl_node = True  # type: ignore[attr-defined]
        fn.graetl_skip = bool(skip)  # type: ignore[attr-defined]
        if cache:
            fn.graetl_cache = True  # type: ignore[attr-defined]
            if cache is not True:
                fn.graetl_cache_size = int(cache)  # type: ignore[attr-defined]
        if name:
            fn.graetl_name = name  # type: ignore[attr-defined]
        if title:
            fn.graetl_title = title  # type: ignore[attr-defined]
        if description:
            fn.graetl_description = description  # type: ignore[attr-defined]
        if category:
            fn.graetl_category = category  # type: ignore[attr-defined]
        if pure is not None:
            fn.graetl_pure = bool(pure)  # type: ignore[attr-defined]
        return fn

    if callable(name):  # bare @node
        fn, name = name, None
        return decorator(fn)
    return decorator
