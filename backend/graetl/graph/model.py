"""The node-graph document: what a ``.graph`` / ``.graphlib`` file contains.

The format is JSON, and it is deliberately small. A graph stores **what the
author decided** - which nodes exist, where they sit, what is wired to what,
and any literal typed into an unconnected pin. It does *not* store pin lists:
those come from the node's definition (a Python signature, another graph's
declared parameters, or a built-in). Storing them twice is how a visual editor
and its runtime drift apart.

A consequence worth knowing: rename a parameter of a decorated function and the
links to that pin stop resolving. The compiler reports that as an error naming
the node and the pin, rather than silently dropping the connection.

Two kinds of document, distinguished by ``kind``:

``module``    a ``.graph`` file - one entity-scoped module, the node-flow
              equivalent of a ``<name>.module.py``. Entry gives ``ctx`` and
              ``entity``; execution_layer / version / depends_on carry over.
``function``  a ``.graphlib`` file - declared inputs and outputs, callable from
              other graphs and from Python. At the pipeline root it registers as
              a pipeline function; inside a module folder it is a local helper.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Literal

SCHEMA_VERSION = 1

#: The pin type of a control-flow (white) pin. Everything else is a data pin.
EXEC = "exec"
#: Data pin type meaning "unknown / anything".
ANY = "Any"

#: The three shapes a module graph can start with. They differ only in what the
#: entry node hands you, and therefore in how the runner calls the module:
#:
#:   core:entry        fn(ctx, entity)     once per entity   (the default)
#:   core:entry_batch  fn(ctx, entities)   a list at a time, one transaction
#:   core:entry_once   fn(ctx)             once per run, no entity
ENTRY_OPS = ("core:entry", "core:entry_batch", "core:entry_once")
ENTRY_SCOPES = {"core:entry": "entity", "core:entry_batch": "batch",
                "core:entry_once": "once"}

Direction = Literal["in", "out"]


class GraphError(Exception):
    """A graph document is malformed, or references something that is not there."""

    def __init__(self, message: str, *, node: str | None = None, pin: str | None = None) -> None:
        where = ""
        if node:
            where = f" [node {node}{'.' + pin if pin else ''}]"
        super().__init__(f"{message}{where}")
        self.node = node
        self.pin = pin


# --------------------------------------------------------------------- pins


@dataclass(slots=True)
class Pin:
    """One connector on a node.

    ``type`` is a plain string: ``"exec"`` for control flow, otherwise whatever
    the annotation said (``"int"``, ``"str"``, ``"Entity"``, ``"Any"``). Python
    is dynamic, so types guide the editor and document intent; they never make
    the compiler refuse to emit code.
    """

    name: str
    type: str = ANY
    direction: Direction = "in"
    label: str = ""
    description: str = ""
    #: Default used when the pin is neither connected nor given a literal.
    default: Any = None
    has_default: bool = False
    #: ``*args`` / ``**kwargs`` on a reflected function.
    variadic: bool = False
    keyword_only: bool = False

    @property
    def is_exec(self) -> bool:
        return self.type == EXEC

    @property
    def display(self) -> str:
        return self.label or self.name

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"name": self.name, "type": self.type, "direction": self.direction}
        if self.label:
            out["label"] = self.label
        if self.description:
            out["description"] = self.description
        if self.has_default:
            out["default"] = self.default
        if self.variadic:
            out["variadic"] = True
        if self.keyword_only:
            out["keyword_only"] = True
        return out


def exec_in(name: str = "exec", label: str = "") -> Pin:
    return Pin(name=name, type=EXEC, direction="in", label=label)


def exec_out(name: str = "then", label: str = "") -> Pin:
    return Pin(name=name, type=EXEC, direction="out", label=label)


# -------------------------------------------------------------------- nodes


@dataclass(slots=True)
class Node:
    """One placed node.

    ``op`` names the definition, in one of four namespaces:

    ``fn:<name>``      a function registered with ``@pipeline.function``
    ``graph:<name>``   another graph, called like a function
    ``py:<dotted>``    a reflected Python callable, e.g. ``py:math.floor``
    ``core:<name>``    a built-in flow or data node, e.g. ``core:branch``

    ``values`` holds literals the author typed into unconnected input pins.
    ``config`` holds node-shape settings a definition asks for - the operator on
    ``core:binary_op``, the output count on ``core:sequence``.
    """

    id: str
    op: str
    title: str = ""
    pos: tuple[float, float] = (0.0, 0.0)
    values: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)
    comment: str = ""
    #: Collapse this node's expression into its use site even if used twice.
    inline: bool | None = None

    @property
    def kind(self) -> str:
        return self.op.split(":", 1)[0]

    @property
    def ref(self) -> str:
        """The part after the namespace: ``math.floor`` for ``py:math.floor``."""
        return self.op.split(":", 1)[1] if ":" in self.op else self.op

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"id": self.id, "op": self.op, "pos": [self.pos[0], self.pos[1]]}
        if self.title:
            out["title"] = self.title
        if self.values:
            out["values"] = self.values
        if self.config:
            out["config"] = self.config
        if self.comment:
            out["comment"] = self.comment
        if self.inline is not None:
            out["inline"] = self.inline
        return out

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Node:
        try:
            node_id = str(data["id"])
            op = str(data["op"])
        except KeyError as exc:
            raise GraphError(f"node is missing {exc.args[0]!r}") from None
        pos = data.get("pos") or [0, 0]
        return cls(
            id=node_id,
            op=op,
            title=str(data.get("title", "")),
            pos=(float(pos[0]), float(pos[1])),
            values=dict(data.get("values") or {}),
            config=dict(data.get("config") or {}),
            comment=str(data.get("comment", "")),
            inline=data.get("inline"),
        )


@dataclass(slots=True)
class Link:
    """A wire. Exec wires and data wires use the same record."""

    from_node: str
    from_pin: str
    to_node: str
    to_pin: str

    @property
    def source(self) -> tuple[str, str]:
        return (self.from_node, self.from_pin)

    @property
    def target(self) -> tuple[str, str]:
        return (self.to_node, self.to_pin)

    def to_dict(self) -> dict[str, Any]:
        return {"from": [self.from_node, self.from_pin], "to": [self.to_node, self.to_pin]}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Link:
        try:
            src, dst = data["from"], data["to"]
            return cls(str(src[0]), str(src[1]), str(dst[0]), str(dst[1]))
        except (KeyError, IndexError, TypeError) as exc:
            raise GraphError(f"malformed link: {data!r} ({exc})") from None


@dataclass(slots=True)
class Variable:
    """A graph-scoped variable, read by ``core:get_var`` and written by ``core:set_var``.

    Compiles to a plain local, initialised once at the top of the function.
    """

    name: str
    type: str = ANY
    default: Any = None
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"name": self.name, "type": self.type, "default": self.default}
        if self.description:
            out["description"] = self.description
        return out

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Variable:
        return cls(
            name=str(data["name"]),
            type=str(data.get("type", ANY)),
            default=data.get("default"),
            description=str(data.get("description", "")),
        )


@dataclass(slots=True)
class Comment:
    """A comment box. Text lands in the generated Python above the nodes it covers."""

    id: str
    text: str
    pos: tuple[float, float] = (0.0, 0.0)
    size: tuple[float, float] = (320.0, 180.0)
    color: str = ""
    #: Does dragging the box carry the nodes sitting on it? On by default,
    #: because that is what a comment box is for; off when you are only
    #: rearranging the boxes themselves.
    moves_nodes: bool = True

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "id": self.id,
            "text": self.text,
            "pos": [self.pos[0], self.pos[1]],
            "size": [self.size[0], self.size[1]],
        }
        if self.color:
            out["color"] = self.color
        if not self.moves_nodes:
            out["moves_nodes"] = False
        return out

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Comment:
        pos = data.get("pos") or [0, 0]
        size = data.get("size") or [320, 180]
        return cls(
            id=str(data.get("id", "")),
            text=str(data.get("text", "")),
            pos=(float(pos[0]), float(pos[1])),
            size=(float(size[0]), float(size[1])),
            color=str(data.get("color", "")),
            moves_nodes=bool(data.get("moves_nodes", True)),
        )


# -------------------------------------------------------------------- graph


@dataclass(slots=True)
class Graph:
    """A whole ``.graph`` / ``.graphlib`` document."""

    name: str
    kind: Literal["module", "function"] = "module"
    title: str = ""
    description: str = ""
    schema: int = SCHEMA_VERSION

    # module-only
    version: int = 1
    execution_layer: int = 0
    depends_on: list[str] = field(default_factory=list)

    # function-only: the declared signature
    inputs: list[Pin] = field(default_factory=list)
    outputs: list[Pin] = field(default_factory=list)
    #: A function graph with no side effects compiles to an expression-friendly
    #: helper and its call nodes need no exec pins.
    pure: bool = False
    category: str = ""
    #: Memoise the result per process. See :mod:`graetl.sdk.caching` - the run
    #: context is never part of the key, so this is for lookups whose answer
    #: depends only on the arguments.
    cache: bool = False
    #: Entries kept when cached; ``None`` uses the configured default.
    cache_size: int | None = None

    variables: list[Variable] = field(default_factory=list)
    nodes: list[Node] = field(default_factory=list)
    links: list[Link] = field(default_factory=list)
    comments: list[Comment] = field(default_factory=list)
    view: dict[str, Any] = field(default_factory=dict)
    #: Anything the editor wants to keep that the compiler does not read.
    meta: dict[str, Any] = field(default_factory=dict)

    #: Set when the graph was read from disk.
    path: Path | None = None

    # ------------------------------------------------------------- indexing

    def node(self, node_id: str) -> Node:
        for node in self.nodes:
            if node.id == node_id:
                return node
        raise GraphError(f"no such node: {node_id!r}")

    @property
    def nodes_by_id(self) -> dict[str, Node]:
        return {n.id: n for n in self.nodes}

    def links_from(self, node_id: str, pin: str | None = None) -> list[Link]:
        return [
            link
            for link in self.links
            if link.from_node == node_id and (pin is None or link.from_pin == pin)
        ]

    def links_into(self, node_id: str, pin: str | None = None) -> list[Link]:
        return [
            link
            for link in self.links
            if link.to_node == node_id and (pin is None or link.to_pin == pin)
        ]

    def iter_nodes(self, op: str) -> Iterator[Node]:
        for node in self.nodes:
            if node.op == op:
                yield node

    def entry(self) -> Node | None:
        return next((n for n in self.nodes if n.op in ENTRY_OPS), None)

    @property
    def scope(self) -> str:
        """How the runner calls this module: entity, batch or once."""
        node = self.entry()
        return ENTRY_SCOPES.get(node.op, "entity") if node else "entity"

    @property
    def batch_size(self) -> int | None:
        """Entities per call for a batch graph; ``None`` uses the configured size."""
        node = self.entry()
        if node is None or node.op != "core:entry_batch":
            return None
        size = int(node.config.get("size", 0) or 0)
        return size or None

    # --------------------------------------------------------------- codegen

    @property
    def function_name(self) -> str:
        """The Python identifier this graph compiles to."""
        return _identifier(self.name)

    # ------------------------------------------------------------ (de)serialise

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "graetl_graph": self.schema,
            "kind": self.kind,
            "name": self.name,
        }
        if self.title:
            out["title"] = self.title
        if self.description:
            out["description"] = self.description
        if self.kind == "module":
            out["version"] = self.version
            out["execution_layer"] = self.execution_layer
            if self.depends_on:
                out["depends_on"] = list(self.depends_on)
        else:
            out["inputs"] = [p.to_dict() for p in self.inputs]
            out["outputs"] = [p.to_dict() for p in self.outputs]
            if self.pure:
                out["pure"] = True
            if self.category:
                out["category"] = self.category
            if self.cache:
                out["cache"] = True
            if self.cache_size:
                out["cache_size"] = self.cache_size
        if self.variables:
            out["variables"] = [v.to_dict() for v in self.variables]
        out["nodes"] = [n.to_dict() for n in self.nodes]
        out["links"] = [link.to_dict() for link in self.links]
        if self.comments:
            out["comments"] = [c.to_dict() for c in self.comments]
        if self.view:
            out["view"] = self.view
        if self.meta:
            out["meta"] = self.meta
        return out

    def dumps(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False) + "\n"

    def save(self, path: Path | None = None) -> Path:
        target = Path(path or self.path or f"{self.name}.graph")
        target.write_text(self.dumps(), encoding="utf-8")
        self.path = target
        return target

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, path: Path | None = None) -> Graph:
        if not isinstance(data, dict):
            raise GraphError("a graph file must contain a JSON object")
        schema = int(data.get("graetl_graph", data.get("schema", 0)) or 0)
        if schema == 0:
            raise GraphError(
                "not a GraETL graph: the 'graetl_graph' schema marker is missing"
            )
        if schema > SCHEMA_VERSION:
            raise GraphError(
                f"graph schema {schema} is newer than this GraETL understands "
                f"({SCHEMA_VERSION}) - upgrade GraETL"
            )
        kind = str(data.get("kind", "module"))
        if kind not in ("module", "function"):
            raise GraphError(f"unknown graph kind {kind!r} (expected 'module' or 'function')")
        name = str(data.get("name") or (path.stem if path else "")).strip()
        if not name:
            raise GraphError("a graph needs a name")

        graph = cls(
            name=name,
            kind=kind,  # type: ignore[arg-type]
            title=str(data.get("title", "")),
            description=str(data.get("description", "")),
            schema=schema,
            version=int(data.get("version", 1) or 1),
            execution_layer=int(data.get("execution_layer", 0) or 0),
            depends_on=[str(d) for d in (data.get("depends_on") or [])],
            inputs=[_pin(p, "out") for p in (data.get("inputs") or [])],
            outputs=[_pin(p, "in") for p in (data.get("outputs") or [])],
            pure=bool(data.get("pure", False)),
            category=str(data.get("category", "")),
            cache=bool(data.get("cache", False)),
            cache_size=int(data["cache_size"]) if data.get("cache_size") else None,
            variables=[Variable.from_dict(v) for v in (data.get("variables") or [])],
            nodes=[Node.from_dict(n) for n in (data.get("nodes") or [])],
            links=[Link.from_dict(link) for link in (data.get("links") or [])],
            comments=[Comment.from_dict(c) for c in (data.get("comments") or [])],
            view=dict(data.get("view") or {}),
            meta=dict(data.get("meta") or {}),
            path=path,
        )
        graph.validate_structure()
        return graph

    @classmethod
    def load(cls, path: str | Path) -> Graph:
        path = Path(path)
        try:
            raw = json.loads(path.read_text(encoding="utf-8-sig"))
        except json.JSONDecodeError as exc:
            raise GraphError(f"{path.name}: invalid JSON - {exc}") from None
        except OSError as exc:
            raise GraphError(f"cannot read {path}: {exc}") from None
        return cls.from_dict(raw, path=path)

    # ------------------------------------------------------------ validation

    def validate_structure(self) -> None:
        """Checks that need no node definitions: ids, duplicates, dangling links."""
        seen: set[str] = set()
        for node in self.nodes:
            if node.id in seen:
                raise GraphError(f"duplicate node id: {node.id!r}")
            seen.add(node.id)
            if ":" not in node.op:
                raise GraphError(
                    f"node op {node.op!r} has no namespace - expected one of "
                    "fn: / graph: / py: / core:",
                    node=node.id,
                )
            if node.kind not in ("fn", "graph", "py", "core"):
                raise GraphError(f"unknown node namespace {node.kind!r}", node=node.id)
        for link in self.links:
            if link.from_node not in seen:
                raise GraphError(f"link starts at unknown node {link.from_node!r}")
            if link.to_node not in seen:
                raise GraphError(f"link ends at unknown node {link.to_node!r}")
        names = [v.name for v in self.variables]
        duplicate = {n for n in names if names.count(n) > 1}
        if duplicate:
            raise GraphError(f"duplicate variable name(s): {', '.join(sorted(duplicate))}")
        # Entry is the function's signature, so there is exactly one of it. Two
        # would mean two starting points and no way to say which one runs.
        entries = [n.id for n in self.nodes if n.op in ENTRY_OPS]
        if len(entries) > 1:
            raise GraphError(
                "a graph has exactly one entry node, but this one has "
                f"{len(entries)}: {', '.join(entries)}",
                node=entries[1],
            )


def _pin(data: dict[str, Any], direction: Direction) -> Pin:
    """A declared parameter of a function graph.

    Inputs are pins the *entry* node hands out, so they are outputs on the
    canvas; outputs are pins the *return* node accepts. The ``direction`` passed
    in is the canvas direction, which is the inverse of the reading direction.
    """
    return Pin(
        name=str(data["name"]),
        type=str(data.get("type", ANY)),
        direction=direction,
        label=str(data.get("label", "")),
        description=str(data.get("description", "")),
        default=data.get("default"),
        has_default="default" in data,
        variadic=bool(data.get("variadic", False)),
        keyword_only=bool(data.get("keyword_only", False)),
    )


def _identifier(value: str) -> str:
    """Turn a graph or pin name into a legal, readable Python identifier."""
    out = []
    for char in value.strip():
        if char.isalnum() or char == "_":
            out.append(char)
        elif char in " -.:/":
            out.append("_")
    text = "".join(out).strip("_") or "graph"
    while "__" in text:
        text = text.replace("__", "_")
    if text[0].isdigit():
        text = f"_{text}"
    return text


# ---------------------------------------------------------------- libraries


@dataclass(slots=True)
class Library:
    """A ``.graphlib`` file: one or more function graphs, compiled into one ``.py``.

    A library started life as a single function per file, and that shape still
    reads - it becomes a library of one, and saving writes the list form. The
    functions are independent graphs; grouping them in a file is organisation,
    not scope. Each still registers under its own name, so ``graph:<name>``
    keeps meaning exactly one function wherever it is drawn.
    """

    name: str
    functions: list[Graph] = field(default_factory=list)
    title: str = ""
    description: str = ""
    schema: int = SCHEMA_VERSION
    meta: dict[str, Any] = field(default_factory=dict)
    path: Path | None = None

    def function(self, name: str) -> Graph:
        for graph in self.functions:
            if graph.name == name:
                return graph
        raise GraphError(f"no function named {name!r} in {self.name}")

    @property
    def names(self) -> list[str]:
        return [graph.name for graph in self.functions]

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "graetl_graph": self.schema,
            "kind": "library",
            "name": self.name,
        }
        if self.title:
            out["title"] = self.title
        if self.description:
            out["description"] = self.description
        out["functions"] = [graph.to_dict() for graph in self.functions]
        if self.meta:
            out["meta"] = self.meta
        return out

    def dumps(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False) + "\n"

    def save(self, path: Path | None = None) -> Path:
        target = Path(path or self.path or f"{self.name}.graphlib")
        target.write_text(self.dumps(), encoding="utf-8")
        self.path = target
        return target

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, path: Path | None = None) -> Library:
        if not isinstance(data, dict):
            raise GraphError("a graph library must contain a JSON object")
        kind = str(data.get("kind", ""))
        stem = path.name[: -len(".graphlib")] if path and path.name.endswith(".graphlib") else None
        if kind != "library":
            # The original one-function-per-file shape.
            graph = Graph.from_dict(data, path=path)
            if graph.kind != "function":
                raise GraphError(f"{graph.name} is a module graph, not a function graph")
            return cls(
                name=stem or graph.name,
                functions=[graph],
                title=graph.title,
                description=graph.description,
                schema=graph.schema,
                path=path,
            )

        schema = int(data.get("graetl_graph", 0) or 0)
        if schema == 0:
            raise GraphError("not a GraETL graph: the 'graetl_graph' schema marker is missing")
        if schema > SCHEMA_VERSION:
            raise GraphError(
                f"graph schema {schema} is newer than this GraETL understands "
                f"({SCHEMA_VERSION}) - upgrade GraETL"
            )
        name = str(data.get("name") or stem or "").strip()
        if not name:
            raise GraphError("a graph library needs a name")

        functions: list[Graph] = []
        for entry in data.get("functions") or []:
            if not isinstance(entry, dict):
                raise GraphError(f"{name}: every entry under 'functions' must be an object")
            # The schema marker belongs to the file, not to each function in it.
            graph = Graph.from_dict(
                {"graetl_graph": schema, **entry, "kind": "function"}, path=path
            )
            functions.append(graph)
        library = cls(
            name=name,
            functions=functions,
            title=str(data.get("title", "")),
            description=str(data.get("description", "")),
            schema=schema,
            meta=dict(data.get("meta") or {}),
            path=path,
        )
        library.validate_structure()
        return library

    @classmethod
    def load(cls, path: str | Path) -> Library:
        path = Path(path)
        try:
            raw = json.loads(path.read_text(encoding="utf-8-sig"))
        except json.JSONDecodeError as exc:
            raise GraphError(f"{path.name}: invalid JSON - {exc}") from None
        except OSError as exc:
            raise GraphError(f"cannot read {path}: {exc}") from None
        return cls.from_dict(raw, path=path)

    def validate_structure(self) -> None:
        seen: set[str] = set()
        for graph in self.functions:
            if graph.name in seen:
                raise GraphError(f"duplicate function name in {self.name}: {graph.name!r}")
            seen.add(graph.name)
            graph.validate_structure()
