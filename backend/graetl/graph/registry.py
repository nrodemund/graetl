"""Where a node's shape comes from.

A node on the canvas is just an ``op`` string and a position. Everything else -
which pins it has, what they are called, what types they carry, whether it sits
in the execution flow or is a pure expression - is resolved here, from one of
four sources:

``fn:<name>``     a function registered with ``@pipeline.function`` in
                  ``pipeline.py``. Pins come from its Python signature; the
                  leading ``ctx`` parameter is implicit and never drawn.
``graph:<name>``  another graph, called like a function. Pins come from the
                  parameters that graph declares.
``py:<dotted>``   true reflection: ``py:math.floor`` imports ``math``, looks at
                  ``floor`` and reads its signature. Works for any importable
                  callable, including methods and classes.
``core:<name>``   a built-in - branch, loops, operators, literals. Declared in
                  core_nodes.py.

Resolution is per *node*, not per op, because some built-ins change shape with
their configuration: a sequence node has as many outputs as you asked for.

**Purity** decides how a node compiles. A pure node has no exec pins and
becomes an expression, inlined where it is used; an impure node sits in the
execution chain and becomes a statement. Reflected nodes are pure only when
they come from a module known to be side-effect free (configurable), because
inlining something that writes to a file would quietly reorder it.
"""

from __future__ import annotations

import builtins
import importlib
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Sequence

from graetl.graph.model import ANY, EXEC, GraphError, Node, Pin, exec_in, exec_out

#: Reflected callables from these modules are treated as pure by default.
#: Anything else gets exec pins unless the node sets ``config.pure``.
DEFAULT_PURE_MODULES: tuple[str, ...] = (
    "abc", "base64", "binascii", "bisect", "builtins", "calendar", "cmath",
    "collections", "colorsys", "datetime", "decimal", "difflib", "enum",
    "fractions", "functools", "hashlib", "heapq", "html", "itertools", "json",
    "math", "numbers", "operator", "pathlib", "re", "statistics", "string",
    "textwrap", "time", "types", "typing", "unicodedata", "urllib", "uuid",
)

#: Builtins reachable as ``py:len`` with no module prefix.
SAFE_BUILTINS: frozenset[str] = frozenset(
    {
        "abs", "all", "any", "ascii", "bin", "bool", "bytes", "chr", "dict",
        "divmod", "enumerate", "filter", "float", "format", "frozenset",
        "getattr", "hasattr", "hash", "hex", "int", "isinstance", "issubclass",
        "len", "list", "map", "max", "min", "oct", "ord", "pow", "range",
        "repr", "reversed", "round", "set", "slice", "sorted", "str", "sum",
        "tuple", "zip",
    }
)


@dataclass(slots=True)
class NodeDef:
    """The resolved shape of one node."""

    op: str
    kind: str
    title: str
    pins: list[Pin]
    pure: bool = False
    category: str = ""
    description: str = ""
    #: What the generated code calls, e.g. ``math.floor`` or ``compute_bmi``.
    target: str = ""
    #: Modules the generated file must import for ``target`` to resolve.
    imports: tuple[str, ...] = ()
    #: Pass ``ctx`` as the first positional argument at the call site.
    implicit_ctx: bool = False
    #: Built-ins the compiler emits itself rather than as a call.
    builtin: bool = False
    meta: dict[str, Any] = field(default_factory=dict)

    def pin(self, name: str, direction: str | None = None) -> Pin:
        for pin in self.pins:
            if pin.name == name and (direction is None or pin.direction == direction):
                return pin
        raise GraphError(
            f"{self.op} has no {direction or ''} pin {name!r} "
            f"(has: {', '.join(p.name for p in self.pins if direction in (None, p.direction))})"
        )

    def has_pin(self, name: str, direction: str | None = None) -> bool:
        try:
            self.pin(name, direction)
            return True
        except GraphError:
            return False

    def inputs(self, *, data_only: bool = False) -> list[Pin]:
        return [
            p for p in self.pins
            if p.direction == "in" and (not data_only or not p.is_exec)
        ]

    def outputs(self, *, data_only: bool = False) -> list[Pin]:
        return [
            p for p in self.pins
            if p.direction == "out" and (not data_only or not p.is_exec)
        ]

    def to_dict(self) -> dict[str, Any]:
        return {
            "op": self.op,
            "kind": self.kind,
            "title": self.title,
            "category": self.category,
            "description": self.description,
            "pure": self.pure,
            "pins": [p.to_dict() for p in self.pins],
            "meta": self.meta,
        }


# ------------------------------------------------------------------ helpers


def annotation_name(annotation: Any) -> str:
    """A readable type string for a pin, from whatever the annotation is."""
    if annotation is inspect.Parameter.empty or annotation is None:
        return ANY
    if isinstance(annotation, str):
        return annotation
    name = getattr(annotation, "__name__", None)
    if name:
        return name
    text = str(annotation).replace("typing.", "")
    return text or ANY


def signature_pins(
    func: Callable[..., Any],
    *,
    skip_first: bool = False,
    impure: bool = False,
    result_name: str = "result",
) -> tuple[list[Pin], bool]:
    """Pins for a callable. Returns ``(pins, took_ctx)``.

    ``skip_first`` drops a leading ``ctx`` parameter: the compiler passes it,
    so drawing it would be noise on every single node.
    """
    pins: list[Pin] = []
    took_ctx = False
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        # C functions without introspection data: one catch-all argument pin.
        signature = None

    if impure:
        pins.append(exec_in())
        pins.append(exec_out())

    if signature is None:
        pins.append(Pin(name="args", type=ANY, direction="in", variadic=True))
    else:
        parameters = list(signature.parameters.values())
        if skip_first and parameters and parameters[0].name in ("ctx", "context"):
            parameters = parameters[1:]
            took_ctx = True
        for parameter in parameters:
            if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
                pins.append(Pin(name=parameter.name, type=ANY, direction="in", variadic=True))
                continue
            if parameter.kind is inspect.Parameter.VAR_KEYWORD:
                continue  # **kwargs cannot be wired meaningfully on a canvas
            has_default = parameter.default is not inspect.Parameter.empty
            pins.append(
                Pin(
                    name=parameter.name,
                    type=annotation_name(parameter.annotation),
                    direction="in",
                    default=parameter.default if has_default else None,
                    has_default=has_default,
                    keyword_only=parameter.kind is inspect.Parameter.KEYWORD_ONLY,
                )
            )
        returns = annotation_name(signature.return_annotation)
        if returns != "None":
            pins.append(Pin(name=result_name, type=returns, direction="out"))

    return pins, took_ctx


def _humanise(name: str) -> str:
    return name.replace("_", " ").strip().capitalize()


# ----------------------------------------------------------------- registry


class NodeRegistry:
    """Resolves ``op`` strings to node definitions for one pipeline."""

    def __init__(
        self,
        *,
        functions: dict[str, Callable[..., Any]] | None = None,
        graphs: dict[str, Any] | None = None,
        pure_modules: Sequence[str] | None = None,
        reflect_allow: Sequence[str] | None = None,
    ) -> None:
        self.functions = dict(functions or {})
        #: name -> Graph, for ``graph:`` nodes. Filled by the compiler.
        self.graphs = dict(graphs or {})
        self.pure_modules = tuple(pure_modules if pure_modules is not None else DEFAULT_PURE_MODULES)
        #: When set, only these dotted roots may be reflected.
        self.reflect_allow = tuple(reflect_allow) if reflect_allow else ()
        self._cache: dict[tuple[str, str], NodeDef] = {}

    # -------------------------------------------------------------- resolve

    def resolve(self, node: Node) -> NodeDef:
        kind = node.kind
        if kind == "core":
            from graetl.graph import core_nodes

            return core_nodes.resolve(node)
        if kind == "fn":
            return self._resolve_function(node)
        if kind == "graph":
            return self._resolve_graph(node)
        if kind == "py":
            return self._resolve_reflected(node)
        raise GraphError(f"unknown node namespace {kind!r}", node=node.id)

    def _resolve_function(self, node: Node) -> NodeDef:
        name = node.ref
        func = self.functions.get(name)
        if func is None:
            known = ", ".join(sorted(self.functions)) or "none registered"
            raise GraphError(
                f"no pipeline function named {name!r} - available: {known}. "
                "Register it with @pipeline.function in pipeline.py.",
                node=node.id,
            )
        pure = bool(getattr(func, "graetl_pure", False))
        if "pure" in node.config:
            pure = bool(node.config["pure"])
        pins, took_ctx = signature_pins(func, skip_first=True, impure=not pure)
        return NodeDef(
            op=node.op,
            kind="fn",
            title=getattr(func, "graetl_title", "") or _humanise(name),
            pins=pins,
            pure=pure,
            category=getattr(func, "graetl_category", "") or "Pipeline functions",
            description=getattr(func, "graetl_description", "") or "",
            target=name,
            implicit_ctx=took_ctx,
        )

    def _resolve_graph(self, node: Node) -> NodeDef:
        name = node.ref
        graph = self.graphs.get(name)
        if graph is None:
            known = ", ".join(sorted(self.graphs)) or "none loaded"
            raise GraphError(
                f"no function graph named {name!r} - available: {known}. "
                "Function graphs are .graphlib files.",
                node=node.id,
            )
        if graph.kind != "function":
            raise GraphError(
                f"{name!r} is a {graph.kind} graph and cannot be called as a function",
                node=node.id,
            )
        pure = bool(graph.pure)
        if "pure" in node.config:
            pure = bool(node.config["pure"])
        pins: list[Pin] = []
        if not pure:
            pins.append(exec_in())
            pins.append(exec_out())
        for declared in graph.inputs:
            pins.append(
                Pin(
                    name=declared.name,
                    type=declared.type,
                    direction="in",
                    label=declared.label,
                    description=declared.description,
                    default=declared.default,
                    has_default=declared.has_default,
                )
            )
        for declared in graph.outputs:
            pins.append(
                Pin(
                    name=declared.name,
                    type=declared.type,
                    direction="out",
                    label=declared.label,
                    description=declared.description,
                )
            )
        return NodeDef(
            op=node.op,
            kind="graph",
            title=graph.title or _humanise(graph.name),
            pins=pins,
            pure=pure,
            category=graph.category or "Function graphs",
            description=graph.description,
            target=graph.function_name,
            meta={"graph": graph.name},
        )

    def _resolve_reflected(self, node: Node) -> NodeDef:
        dotted = node.ref
        if not dotted:
            raise GraphError("a reflected node needs a dotted path, e.g. py:math.floor",
                             node=node.id)
        cached = self._cache.get(("py", dotted))
        target, module_name = self._import_target(dotted, node)
        pure = module_name in self.pure_modules or (
            module_name == "" and dotted in SAFE_BUILTINS
        )
        if "pure" in node.config:
            pure = bool(node.config["pure"])
        if cached is not None and cached.pure == pure:
            return cached

        if not callable(target):
            raise GraphError(f"{dotted} is not callable", node=node.id)
        pins, _ = signature_pins(target, impure=not pure)
        definition = NodeDef(
            op=node.op,
            kind="py",
            title=dotted,
            pins=pins,
            pure=pure,
            category=f"Python · {module_name or 'builtins'}",
            description=(inspect.getdoc(target) or "").split("\n\n")[0].strip(),
            target=dotted,
            imports=(module_name,) if module_name else (),
        )
        self._cache[("py", dotted)] = definition
        return definition

    def _import_target(self, dotted: str, node: Node) -> tuple[Any, str]:
        """Resolve ``a.b.c`` to the object and the module that has to be imported."""
        root = dotted.split(".", 1)[0]
        if self.reflect_allow and root not in self.reflect_allow:
            raise GraphError(
                f"reflecting {dotted!r} is not allowed - [graphs] reflect_allow "
                f"lists {', '.join(self.reflect_allow)}",
                node=node.id,
            )
        if "." not in dotted:
            if dotted in SAFE_BUILTINS:
                return getattr(builtins, dotted), ""
            raise GraphError(
                f"{dotted!r} is not a known builtin - use a dotted path such as "
                "py:math.floor",
                node=node.id,
            )

        # Longest importable prefix wins: ``os.path.join`` imports ``os.path``.
        parts = dotted.split(".")
        for cut in range(len(parts) - 1, 0, -1):
            module_name = ".".join(parts[:cut])
            try:
                module = importlib.import_module(module_name)
            except ImportError:
                continue
            obj: Any = module
            try:
                for attribute in parts[cut:]:
                    obj = getattr(obj, attribute)
            except AttributeError:
                raise GraphError(
                    f"{module_name} has no attribute {'.'.join(parts[cut:])!r}",
                    node=node.id,
                ) from None
            return obj, module_name
        raise GraphError(f"cannot import anything from {dotted!r}", node=node.id)

    # -------------------------------------------------------------- catalog

    def catalog(self) -> list[dict[str, Any]]:
        """Every node the editor can offer, for the palette."""
        from graetl.graph import core_nodes

        out: list[dict[str, Any]] = [d.to_dict() for d in core_nodes.catalog()]
        for name in sorted(self.functions):
            try:
                out.append(self.resolve(Node(id="_", op=f"fn:{name}")).to_dict())
            except GraphError:
                continue
        for name, graph in sorted(self.graphs.items()):
            if getattr(graph, "kind", "") != "function":
                continue
            try:
                out.append(self.resolve(Node(id="_", op=f"graph:{name}")).to_dict())
            except GraphError:
                continue
        return out

    def describe(self, op: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
        """Resolve one op for the editor, without placing a node."""
        return self.resolve(Node(id="_preview", op=op, config=dict(config or {}))).to_dict()


def registry_for(pipeline: Any, graphs: Iterable[Any] = (), **kwargs: Any) -> NodeRegistry:
    """Build a registry from a loaded :class:`~graetl.sdk.pipeline.Pipeline`."""
    return NodeRegistry(
        functions=dict(getattr(pipeline, "functions", {}) or {}),
        graphs={g.name: g for g in graphs},
        **kwargs,
    )
