"""What to offer when a wire is dropped on empty canvas.

Blueprint's best habit: drag off a pin, let go, and the editor shows the things
you would plausibly do *with that value* rather than the whole palette. Dragging
``entity`` offers ``.id``, ``.data`` and ``.label``; dragging a number offers the
operators; dragging an execution pin offers the flow nodes.

Two sources feed it:

* **Members**, read off the real class - ``Entity`` is a dataclass, so its fields
  are its fields. Nothing here is a hand-maintained list of attribute names that
  could drift from the SDK.
* **A family table**, because "what do you do with a list" is a matter of taste
  and cannot be introspected.

Everything is returned as a fully resolved node definition, so the editor places
it exactly as it places a node picked from the palette.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Iterable

from graetl.graph import core_nodes
from graetl.graph.model import ANY, EXEC, GraphError, Node

#: Types whose members the editor can offer. Keyed by the name an annotation
#: produces, which is what the pin carries.
def _member_types() -> dict[str, Any]:
    from graetl.sdk.pipeline import Entity

    return {"Entity": Entity}


#: (op, config) pairs offered for a value of each type family, in order.
FAMILIES: dict[str, list[tuple[str, dict[str, Any]]]] = {
    "number": [
        ("core:binary_op", {"op": "add"}),
        ("core:binary_op", {"op": "sub"}),
        ("core:binary_op", {"op": "mul"}),
        ("core:binary_op", {"op": "truediv"}),
        ("core:binary_op", {"op": "gt"}),
        ("core:binary_op", {"op": "lt"}),
        ("core:cast", {"to": "int"}),
        ("core:cast", {"to": "str"}),
        ("core:format", {"template": "{value}"}),
    ],
    "str": [
        ("core:binary_op", {"op": "eq"}),
        ("core:format", {"template": "{value}"}),
        ("core:binary_op", {"op": "add"}),
        ("core:cast", {"to": "int"}),
        ("core:cast", {"to": "float"}),
    ],
    "bool": [
        ("core:branch", {}),
        ("core:select", {}),
        ("core:unary_op", {"op": "not"}),
        ("core:binary_op", {"op": "and"}),
        ("core:binary_op", {"op": "or"}),
    ],
    "mapping": [
        ("core:get_item", {"safe": True}),
        ("core:get_item", {"safe": False}),
        ("core:for_each", {}),
    ],
    "sequence": [
        ("core:for_each", {}),
        ("core:get_item", {"safe": False}),
        ("core:binary_op", {"op": "in"}),
    ],
    "exec": [
        ("core:branch", {}),
        ("core:for_each", {}),
        ("core:sequence", {}),
        ("core:log", {}),
        ("core:metric", {}),
        ("core:set_var", {"name": "_"}),
        ("core:db_execute", {}),
        ("core:skip", {}),
        ("core:return", {}),
    ],
    #: Anything at all: the operations that do not care what they are given.
    ANY: [
        ("core:is_none", {}),
        ("core:coalesce", {}),
        ("core:binary_op", {"op": "eq"}),
        ("core:format", {"template": "{value}"}),
        ("core:set_var", {"name": "_"}),
    ],
}

_FAMILY_OF = {
    "int": "number", "float": "number", "complex": "number",
    "str": "str", "bytes": "str",
    "bool": "bool",
    "dict": "mapping", "Mapping": "mapping",
    "list": "sequence", "tuple": "sequence", "set": "sequence",
    "Iterable": "sequence", "Sequence": "sequence", "Iterator": "sequence",
    EXEC: "exec",
}


def family_of(type_name: str) -> str:
    """The family a pin type belongs to, falling back to "anything"."""
    base = (type_name or ANY).split("[")[0].strip()
    return _FAMILY_OF.get(base, ANY)


def members_of(type_name: str) -> list[tuple[str, str]]:
    """``[(attribute, type)]`` for a type the SDK owns, else nothing."""
    target = _member_types().get((type_name or "").split("[")[0].strip())
    if target is None or not dataclasses.is_dataclass(target):
        return []
    out: list[tuple[str, str]] = []
    for field in dataclasses.fields(target):
        annotation = field.type
        name = getattr(annotation, "__name__", None) or str(annotation)
        out.append((field.name, name.replace("typing.", "")))
    return out


def for_pin(type_name: str, direction: str, registry: Any = None) -> list[dict[str, Any]]:
    """Nodes worth offering for a wire dragged off a pin of this type.

    ``direction`` is the *source* pin's direction: ``"out"`` means the editor is
    looking for something to consume the value, ``"in"`` for something to
    produce one.
    """
    pairs: list[tuple[str, dict[str, Any]]] = []
    if direction == "out":
        for attribute, _ in members_of(type_name):
            pairs.append(("core:get_attr", {"name": attribute}))
        pairs.extend(FAMILIES.get(family_of(type_name), []))
    else:
        # Looking for a producer. A literal and a graph variable always fit;
        # after that, whatever the family suggests is still the right shape.
        pairs.append(("core:literal", {"value": None, "type": type_name or ANY}))
        pairs.append(("core:get_var", {"name": "_"}))
        if type_name and type_name != EXEC:
            pairs.extend(
                pair for pair in FAMILIES.get(family_of(type_name), []) if pair[0] != "core:branch"
            )
        else:
            pairs.extend(FAMILIES["exec"])
    return _resolve(pairs, registry)


def _resolve(
    pairs: Iterable[tuple[str, dict[str, Any]]], registry: Any
) -> list[dict[str, Any]]:
    """Turn (op, config) pairs into real definitions, dropping any that fail."""
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for op, config in pairs:
        key = (op, repr(sorted(config.items())))
        if key in seen:
            continue
        seen.add(key)
        node = Node(id="_suggest", op=op, config=dict(config))
        try:
            definition = (
                core_nodes.resolve(node, None)
                if node.kind == "core"
                else registry.resolve(node)
            )
        except (GraphError, AttributeError):
            continue
        payload = definition.to_dict()
        payload.setdefault("meta", {})
        payload["meta"] = {**payload["meta"], "config": dict(config)}
        payload["suggested"] = True
        out.append(payload)
    return out
