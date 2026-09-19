"""The built-in nodes - flow control, operators, and the graetl runtime calls.

These are the nodes that have no Python function behind them: the compiler
emits their code directly, so ``core:branch`` becomes a real ``if`` and
``core:for_each`` a real ``for``. That is the whole point of compiling rather
than interpreting - the generated file reads like code someone wrote.

Shapes are declared here; the code each one emits lives in compiler.py, next to
the rest of the code generation.

Some nodes change shape with their ``config``: a sequence has as many outputs
as you asked for, a dict node has one input per key, a format node has one per
placeholder. That is why definitions are built per node rather than looked up.
"""

from __future__ import annotations

from typing import Any, Callable

from graetl.graph.model import ANY, ENTRY_OPS, GraphError, Node, Pin, exec_in, exec_out
from graetl.graph.registry import NodeDef

FLOW = "Flow"
DATA = "Data"
MATH = "Operators"
RUNTIME = "GraETL"
VARIABLES = "Variables"


def _in(name: str, type_: str = ANY, **kw: Any) -> Pin:
    return Pin(name=name, type=type_, direction="in", **kw)


def _out(name: str, type_: str = ANY, **kw: Any) -> Pin:
    return Pin(name=name, type=type_, direction="out", **kw)


def _def(
    op: str,
    title: str,
    pins: list[Pin],
    *,
    pure: bool = False,
    category: str = FLOW,
    description: str = "",
    meta: dict[str, Any] | None = None,
) -> NodeDef:
    return NodeDef(
        op=op,
        kind="core",
        title=title,
        pins=pins,
        pure=pure,
        category=category,
        description=description,
        builtin=True,
        meta=meta or {},
    )


# --------------------------------------------------------------- entry / exit


def _entry(node: Node, graph: Any) -> NodeDef:
    """Where execution starts. Its output pins are the graph's own inputs.

    Entry always has an execution pin, including on a pure function graph:
    ``pure`` describes the *call site* - whether nodes calling this graph carry
    exec pins - not whether the function may use a branch or a loop internally.
    """
    pins: list[Pin] = [exec_out("then")]
    if graph is None or getattr(graph, "kind", "module") == "module":
        pins.append(_out("ctx", "Context", description="The module context"))
        pins.append(_out("entity", "Entity", description="The entity being processed"))
    else:
        pins.append(_out("ctx", "Context", description="The calling context"))
        for declared in getattr(graph, "inputs", []):
            pins.append(
                _out(
                    declared.name,
                    declared.type,
                    label=declared.label,
                    description=declared.description,
                )
            )
    return _def("core:entry", "Entry", pins, description="Start of the graph",
                meta={"scope": "entity"})


def _entry_batch(node: Node, graph: Any) -> NodeDef:
    """Entry for a module that is handed a *list* of entities at a time.

    One call, one transaction: every entity in the batch is marked done
    together, or none is. Use it when the work is cheaper in bulk - one INSERT
    of 500 rows rather than 500 of one.
    """
    size = int(node.config.get("size", 0) or 0)
    return _def(
        "core:entry_batch", "Entry · batch",
        [
            exec_out("then"),
            _out("ctx", "Context", description="The module context"),
            _out("entities", "list", description="The entities in this batch"),
        ],
        description=(
            f"Start of the graph - {size} entities at a time"
            if size else "Start of the graph - a batch of entities at a time"
        ),
        meta={"scope": "batch", "config": {"size": size} if size else {}},
    )


def _entry_once(node: Node, graph: Any) -> NodeDef:
    """Entry for a module that runs exactly once per run, with no entity.

    It still sits in the execution-layer order, so everything below its layer is
    up to date by the time it runs. For work about the whole table: rebuild a
    summary, export the lot, vacuum.
    """
    return _def(
        "core:entry_once", "Entry · once",
        [
            exec_out("then"),
            _out("ctx", "Context", description="The module context"),
        ],
        description="Start of the graph - runs once per run, no entity",
        meta={"scope": "once"},
    )


def _return(node: Node, graph: Any) -> NodeDef:
    """Hands values back. Its input pins are the graph's declared outputs."""
    pins: list[Pin] = [exec_in()]
    outputs = list(getattr(graph, "outputs", []) or [])
    if graph is None or getattr(graph, "kind", "module") == "module" or not outputs:
        pins.append(_in("value", ANY, description="Optional result recorded for the entity"))
    else:
        for declared in outputs:
            pins.append(
                _in(
                    declared.name,
                    declared.type,
                    label=declared.label,
                    description=declared.description,
                )
            )
    return _def("core:return", "Return", pins, description="Return from the graph")


# ------------------------------------------------------------------ control


def _sequence(node: Node, graph: Any) -> NodeDef:
    count = max(2, min(int(node.config.get("outputs", 2) or 2), 16))
    pins = [exec_in()]
    pins += [exec_out(f"then_{i}", label=f"Then {i}") for i in range(count)]
    return _def(
        "core:sequence", "Sequence", pins,
        description="Run each output in order",
        meta={"config": {"outputs": count}},
    )


def _branch(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:branch", "Branch",
        [exec_in(), _in("condition", "bool"), exec_out("true", label="True"),
         exec_out("false", label="False")],
        description="if / else",
    )


def _for_each(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:for_each", "For each",
        [
            exec_in(),
            _in("iterable", "Iterable"),
            exec_out("body", label="Loop body"),
            _out("item", ANY, description="Only valid inside the loop body"),
            _out("index", "int", description="Only valid inside the loop body"),
            exec_out("completed", label="Completed"),
        ],
        description="for item in iterable",
    )


def _for_range(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:for_range", "For range",
        [
            exec_in(),
            _in("start", "int", default=0, has_default=True),
            _in("stop", "int"),
            _in("step", "int", default=1, has_default=True),
            exec_out("body", label="Loop body"),
            _out("index", "int", description="Only valid inside the loop body"),
            exec_out("completed", label="Completed"),
        ],
        description="for i in range(start, stop, step)",
    )


def _while(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:while", "While",
        [
            exec_in(),
            _in("condition", "bool"),
            exec_out("body", label="Loop body"),
            exec_out("completed", label="Completed"),
        ],
        description="while condition",
    )


def _break(node: Node, graph: Any) -> NodeDef:
    return _def("core:break", "Break", [exec_in()], description="Leave the innermost loop")


def _continue(node: Node, graph: Any) -> NodeDef:
    return _def("core:continue", "Continue", [exec_in()],
                description="Next iteration of the innermost loop")


# ------------------------------------------------------------ graetl runtime

LOG_LEVELS = ("debug", "info", "success", "warning", "error")


def _log(node: Node, graph: Any) -> NodeDef:
    level = str(node.config.get("level", "info"))
    if level not in LOG_LEVELS:
        raise GraphError(
            f"log level {level!r} is not one of {', '.join(LOG_LEVELS)}", node=node.id
        )
    return _def(
        "core:log", f"Log · {level}",
        [exec_in(), _in("message", "str"), exec_out()],
        category=RUNTIME,
        description="ctx.info() / ctx.debug() / ... - debug output only reaches a debug run",
        meta={"config": {"level": level}},
    )


def _metric(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:metric", "Metric",
        [exec_in(), _in("name", "str"), _in("value", "float", default=1, has_default=True),
         exec_out()],
        category=RUNTIME,
        description="ctx.metric() - adds to a run counter",
    )


def _skip(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:skip", "Skip entity",
        [exec_in(), _in("reason", "str", default="skipped", has_default=True)],
        category=RUNTIME,
        description="ctx.skip() - records the entity as skipped and stops this module",
    )


def _checkpoint(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:checkpoint", "Checkpoint",
        [exec_in(), exec_out()],
        category=RUNTIME,
        description="ctx.checkpoint() - a safe point for pause and stop",
    )


def _db_execute(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:db_execute", "DB execute",
        [exec_in(), _in("sql", "str"), _in("params", "Sequence", default=None, has_default=True),
         exec_out(), _out("cursor", "Cursor")],
        category=RUNTIME,
        description="ctx.db.execute() - commits with the entity's state row",
    )


def _resource(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:resource", "Resource",
        [_in("name", "str"), _out("resource", ANY)],
        pure=True, category=RUNTIME,
        description="ctx.resource() - this worker's copy of a @pipeline.resource",
    )


def _setting(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:setting", "Setting",
        [_in("path", "str"), _in("default", ANY, default=None, has_default=True),
         _out("value", ANY)],
        pure=True, category=RUNTIME,
        description="ctx.setting() - a dotted key out of pipeline.toml",
    )


# ---------------------------------------------------------------- variables


def _var_name(node: Node, graph: Any) -> str:
    name = str(node.config.get("name", "")).strip()
    if not name:
        raise GraphError("this variable node has no 'name' in its config", node=node.id)
    known = {v.name for v in getattr(graph, "variables", [])} if graph else set()
    if graph is not None and name not in known:
        raise GraphError(
            f"variable {name!r} is not declared on this graph "
            f"(declared: {', '.join(sorted(known)) or 'none'})",
            node=node.id,
        )
    return name


def _get_var(node: Node, graph: Any) -> NodeDef:
    name = _var_name(node, graph)
    declared = next((v for v in getattr(graph, "variables", []) if v.name == name), None)
    return _def(
        "core:get_var", f"Get {name}",
        [_out("value", declared.type if declared else ANY)],
        pure=True, category=VARIABLES, meta={"config": {"name": name}},
    )


def _set_var(node: Node, graph: Any) -> NodeDef:
    name = _var_name(node, graph)
    declared = next((v for v in getattr(graph, "variables", []) if v.name == name), None)
    type_ = declared.type if declared else ANY
    return _def(
        "core:set_var", f"Set {name}",
        [exec_in(), _in("value", type_), exec_out(), _out("value", type_)],
        category=VARIABLES, meta={"config": {"name": name}},
    )


# ----------------------------------------------------------------- pure data

BINARY_OPS: dict[str, tuple[str, str]] = {
    "add": ("+", "int|float|str"), "sub": ("-", "float"), "mul": ("*", "float"),
    "truediv": ("/", "float"), "floordiv": ("//", "int"), "mod": ("%", "float"),
    "pow": ("**", "float"),
    "eq": ("==", "bool"), "ne": ("!=", "bool"), "lt": ("<", "bool"),
    "le": ("<=", "bool"), "gt": (">", "bool"), "ge": (">=", "bool"),
    "and": ("and", "bool"), "or": ("or", "bool"),
    "in": ("in", "bool"), "not_in": ("not in", "bool"),
    "is": ("is", "bool"), "is_not": ("is not", "bool"),
}

UNARY_OPS: dict[str, tuple[str, str]] = {
    "not": ("not ", "bool"), "neg": ("-", "float"), "pos": ("+", "float"),
}


def _binary_op(node: Node, graph: Any) -> NodeDef:
    key = str(node.config.get("op", "add"))
    if key not in BINARY_OPS:
        raise GraphError(
            f"unknown operator {key!r} - one of {', '.join(sorted(BINARY_OPS))}", node=node.id
        )
    symbol, result = BINARY_OPS[key]
    return _def(
        "core:binary_op", symbol,
        [_in("a"), _in("b"), _out("result", result)],
        pure=True, category=MATH, description=f"a {symbol} b",
        meta={"config": {"op": key}, "symbol": symbol},
    )


def _unary_op(node: Node, graph: Any) -> NodeDef:
    key = str(node.config.get("op", "not"))
    if key not in UNARY_OPS:
        raise GraphError(
            f"unknown operator {key!r} - one of {', '.join(sorted(UNARY_OPS))}", node=node.id
        )
    symbol, result = UNARY_OPS[key]
    return _def(
        "core:unary_op", symbol.strip() or symbol,
        [_in("a"), _out("result", result)],
        pure=True, category=MATH, meta={"config": {"op": key}, "symbol": symbol},
    )


def _literal(node: Node, graph: Any) -> NodeDef:
    type_ = str(node.config.get("type", ANY))
    return _def(
        "core:literal", "Literal",
        [_out("value", type_)],
        pure=True, category=DATA,
        description="A constant value",
        meta={"config": {"value": node.config.get("value"), "type": type_}},
    )


def _select(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:select", "Select",
        [_in("condition", "bool"), _in("if_true"), _in("if_false"), _out("result")],
        pure=True, category=DATA, description="if_true if condition else if_false",
    )


def _coalesce(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:coalesce", "Coalesce",
        [_in("value"), _in("fallback"), _out("result")],
        pure=True, category=DATA, description="value if value is not None else fallback",
    )


def _is_none(node: Node, graph: Any) -> NodeDef:
    return _def(
        "core:is_none", "Is none",
        [_in("value"), _out("result", "bool")],
        pure=True, category=DATA, description="value is None",
    )


def _get_item(node: Node, graph: Any) -> NodeDef:
    safe = bool(node.config.get("safe", False))
    pins = [_in("container"), _in("key")]
    if safe:
        pins.append(_in("default", ANY, default=None, has_default=True))
    pins.append(_out("value"))
    return _def(
        "core:get_item", "Get item" + (" (safe)" if safe else ""),
        pins, pure=True, category=DATA,
        description="container.get(key, default)" if safe else "container[key]",
        meta={"config": {"safe": safe}},
    )


def _get_attr(node: Node, graph: Any) -> NodeDef:
    name = str(node.config.get("name", "")).strip()
    if not name.isidentifier():
        raise GraphError(
            f"attribute name {name!r} is not a Python identifier", node=node.id
        )
    return _def(
        "core:get_attr", f".{name}",
        [_in("object"), _out("value")],
        pure=True, category=DATA, description=f"object.{name}",
        meta={"config": {"name": name}},
    )


def _method(node: Node, graph: Any) -> NodeDef:
    """``object.name(...)`` - calling a method on a value.

    Reflection reaches module-level functions; this reaches what a *value* can
    do. ``pandas.read_csv`` is a ``py:`` node, but the ``.to_csv`` on the frame
    it returns is this one. ``args`` is how many positional arguments the call
    takes, ``kwargs`` the keyword names.
    """
    name = str(node.config.get("name", "")).strip()
    if not name.isidentifier():
        raise GraphError(f"method name {name!r} is not a Python identifier", node=node.id)
    count = max(0, min(int(node.config.get("args", 0) or 0), 16))
    keywords = [str(k) for k in (node.config.get("kwargs") or []) if str(k).isidentifier()]
    pure = bool(node.config.get("pure", False))

    pins: list[Pin] = []
    if not pure:
        pins.append(exec_in())
        pins.append(exec_out())
    pins.append(_in("object", ANY, description="The value to call the method on"))
    pins += [_in(f"arg_{i}") for i in range(count)]
    pins += [_in(key, label=key) for key in keywords]
    pins.append(_out("result"))
    return _def(
        "core:method", f".{name}()", pins,
        pure=pure, category=DATA,
        description=f"object.{name}(…)",
        meta={"config": {"name": name, "args": count, "kwargs": keywords, "pure": pure}},
    )


def _make_list(node: Node, graph: Any) -> NodeDef:
    count = max(0, min(int(node.config.get("count", 2) or 0), 32))
    pins = [_in(f"item_{i}") for i in range(count)]
    pins.append(_out("list", "list"))
    return _def("core:make_list", "Make list", pins, pure=True, category=DATA,
                meta={"config": {"count": count}})


def _make_tuple(node: Node, graph: Any) -> NodeDef:
    count = max(0, min(int(node.config.get("count", 2) or 0), 32))
    pins = [_in(f"item_{i}") for i in range(count)]
    pins.append(_out("tuple", "tuple"))
    return _def("core:make_tuple", "Make tuple", pins, pure=True, category=DATA,
                meta={"config": {"count": count}})


def _make_dict(node: Node, graph: Any) -> NodeDef:
    keys = [str(k) for k in (node.config.get("keys") or [])]
    pins = [_in(f"value_{i}", label=key) for i, key in enumerate(keys)]
    pins.append(_out("dict", "dict"))
    return _def("core:make_dict", "Make dict", pins, pure=True, category=DATA,
                description="A dict literal, one pin per key",
                meta={"config": {"keys": keys}})


def _format(node: Node, graph: Any) -> NodeDef:
    """An f-string. Placeholders in the template become input pins."""
    template = str(node.config.get("template", ""))
    names = _placeholders(template)
    pins = [_in(name, "str") for name in names]
    pins.append(_out("text", "str"))
    return _def("core:format", "Format", pins, pure=True, category=DATA,
                description=template or "Text with {placeholders}",
                meta={"config": {"template": template}, "placeholders": names})


CASTS = {"int": "int", "float": "float", "str": "str", "bool": "bool", "list": "list"}


def _cast(node: Node, graph: Any) -> NodeDef:
    to = str(node.config.get("to", "str"))
    if to not in CASTS:
        raise GraphError(f"cannot cast to {to!r} - one of {', '.join(CASTS)}", node=node.id)
    return _def("core:cast", f"To {to}", [_in("value"), _out("result", to)],
                pure=True, category=DATA, meta={"config": {"to": to}})


def _placeholders(template: str) -> list[str]:
    """``"{a} and {b}"`` -> ``["a", "b"]``, in first-seen order."""
    names: list[str] = []
    index = 0
    while index < len(template):
        char = template[index]
        if char == "{":
            if index + 1 < len(template) and template[index + 1] == "{":
                index += 2
                continue
            close = template.find("}", index)
            if close == -1:
                break
            name = template[index + 1 : close].split("!")[0].split(":")[0].strip()
            if name.isidentifier() and name not in names:
                names.append(name)
            index = close + 1
            continue
        index += 1
    return names


# ------------------------------------------------------------------ registry

BUILDERS: dict[str, Callable[[Node, Any], NodeDef]] = {
    "core:entry": _entry,
    "core:return": _return,
    "core:sequence": _sequence,
    "core:branch": _branch,
    "core:for_each": _for_each,
    "core:for_range": _for_range,
    "core:while": _while,
    "core:break": _break,
    "core:continue": _continue,
    "core:log": _log,
    "core:metric": _metric,
    "core:skip": _skip,
    "core:checkpoint": _checkpoint,
    "core:db_execute": _db_execute,
    "core:resource": _resource,
    "core:setting": _setting,
    "core:get_var": _get_var,
    "core:set_var": _set_var,
    "core:binary_op": _binary_op,
    "core:unary_op": _unary_op,
    "core:literal": _literal,
    "core:select": _select,
    "core:coalesce": _coalesce,
    "core:is_none": _is_none,
    "core:get_item": _get_item,
    "core:entry_batch": _entry_batch,
    "core:entry_once": _entry_once,
    "core:get_attr": _get_attr,
    "core:method": _method,
    "core:make_list": _make_list,
    "core:make_tuple": _make_tuple,
    "core:make_dict": _make_dict,
    "core:format": _format,
    "core:cast": _cast,
}


def resolve(node: Node, graph: Any = None) -> NodeDef:
    builder = BUILDERS.get(node.op)
    if builder is None:
        raise GraphError(
            f"unknown built-in node {node.op!r} - one of {', '.join(sorted(BUILDERS))}",
            node=node.id,
        )
    return builder(node, graph)


def catalog() -> list[NodeDef]:
    """Every built-in, at its default configuration, for the editor palette."""
    out: list[NodeDef] = []
    defaults: dict[str, dict[str, Any]] = {
        "core:get_var": {"name": "_"}, "core:set_var": {"name": "_"},
        "core:get_attr": {"name": "value"},
        "core:method": {"name": "method", "args": 0},
        "core:make_dict": {"keys": ["key"]},
        "core:format": {"template": "{value}"},
    }
    for op in BUILDERS:
        # Entry is the graph's signature, not something you place: every graph
        # is created with exactly one, and a second one has no meaning. The
        # entry node's own menu switches between the three kinds.
        if op in ENTRY_OPS:
            continue
        node = Node(id="_", op=op, config=dict(defaults.get(op, {})))
        try:
            out.append(resolve(node, None))
        except GraphError:
            continue
    # Operators are one node with many faces; list each face in the palette.
    for key, (symbol, result) in BINARY_OPS.items():
        out.append(
            _def("core:binary_op", symbol,
                 [_in("a"), _in("b"), _out("result", result)],
                 pure=True, category=MATH, description=f"a {symbol} b",
                 meta={"config": {"op": key}, "symbol": symbol})
        )
    return out
