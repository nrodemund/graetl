"""Turn a node graph into Python a person would be content to have written.

The compiler is a straight walk of the execution wire, exactly as the author
drew it. ``core:branch`` becomes an ``if``, ``core:for_each`` becomes a ``for``,
and a pure node becomes an expression inlined where it is used. Nothing is
interpreted at run time: the output is an ordinary module file that the normal
loader runs, with no graph runtime underneath it.

Three decisions do most of the work for readability:

**Pure nodes are expressions.** A node with no exec pins is folded into the
place it is used, so ``a + b * c`` comes out as ``a + b * c`` rather than three
temporary variables. Precedence is tracked (see emit.py) so brackets appear
only where they change meaning.

**A shared expression becomes a named local.** If a pure node's output feeds two
places, duplicating it would be both noisy and, for anything non-trivial,
wasteful. It is assigned to a local named after the node - and the compiler
tracks which block that assignment landed in, so a value computed inside a loop
is never read after it.

**Scope is checked, not assumed.** The ``item`` of a for-each only exists inside
the loop body. Reading it afterwards is a compile error naming the node and the
pin, not a ``NameError`` at three in the morning halfway through 27,000
entities.
"""

from __future__ import annotations

import builtins as _builtins
import hashlib
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

from graetl import __version__
from graetl.graph import core_nodes
from graetl.graph.emit import (
    P_CALL,
    P_COMPARE,
    P_TERNARY,
    Emitter,
    Expr,
    Names,
    atom,
    binary,
    call,
    call_lines,
    literal,
    ternary,
    unary,
)
from graetl.graph.model import EXEC, Graph, GraphError, Library, Node
from graetl.graph.registry import NodeDef, NodeRegistry

Ref = tuple[str, str]  # (node id, pin name)


class CompileError(GraphError):
    """The graph cannot be turned into code."""


@dataclass
class CompiledFunction:
    """One graph, compiled to a function definition."""

    graph: Graph
    lines: list[str]
    imports: set[str] = field(default_factory=set)
    warnings: list[str] = field(default_factory=list)


@dataclass
class CompiledFile:
    """A whole generated ``.module.py`` or helper file."""

    graph: Graph
    source: str
    path_name: str
    imports: set[str] = field(default_factory=set)
    warnings: list[str] = field(default_factory=list)
    digest: str = ""


def graph_digest(graph: Graph) -> str:
    return hashlib.sha256(graph.dumps().encode("utf-8")).hexdigest()[:16]


def library_digest(library: Library) -> str:
    return hashlib.sha256(library.dumps().encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------


class GraphCompiler:
    """Compiles one graph into the body of one Python function."""

    def __init__(
        self,
        graph: Graph,
        registry: NodeRegistry,
        *,
        names: Names | None = None,
    ) -> None:
        self.graph = graph
        self.registry = registry
        self.names = names or Names({"ctx", "entity", "pipeline", "self"})
        self.imports: set[str] = set()
        self.warnings: list[str] = []

        self.defs: dict[str, NodeDef] = {}
        self.exec_edges: dict[Ref, str] = {}       # (node, out exec pin) -> node id
        self.data_source: dict[Ref, Ref] = {}      # (node, in pin) -> (node, out pin)
        self.use_count: dict[Ref, int] = {}
        #: Assigned outputs: ref -> (identifier, the block it was assigned in)
        self.values: dict[Ref, tuple[str, tuple[int, ...]]] = {}

        self._block_path: tuple[int, ...] = ()
        self._block_seq = 0
        self._loop_depth = 0
        self._visiting: set[str] = set()
        self._emitted_comments: set[str] = set()
        self._no_hoist = False
        #: region path -> the pure outputs that should be computed there
        self._hoist_plan: dict[tuple[int, ...], list[Ref]] = {}
        #: node -> every block it is emitted in (convergent exec wires
        #: duplicate a node into more than one branch, as Blueprint does)
        self._regions: dict[str, list[tuple[int, ...]]] = {}

    # ------------------------------------------------------------- preparing

    def prepare(self) -> None:
        for node in self.graph.nodes:
            try:
                if node.kind == "core":
                    self.defs[node.id] = core_nodes.resolve(node, self.graph)
                else:
                    self.defs[node.id] = self.registry.resolve(node)
            except GraphError as exc:
                raise CompileError(str(exc), node=node.id) from None

        for link in self.graph.links:
            source = self._checked_pin(link.from_node, link.from_pin, "out")
            target = self._checked_pin(link.to_node, link.to_pin, "in")
            if source.is_exec != target.is_exec:
                raise CompileError(
                    f"cannot wire {'execution' if source.is_exec else 'data'} pin "
                    f"{link.from_node}.{link.from_pin} to "
                    f"{'an execution' if target.is_exec else 'a data'} pin "
                    f"{link.to_node}.{link.to_pin}"
                )
            if source.is_exec:
                key = link.source
                if key in self.exec_edges:
                    raise CompileError(
                        f"execution pin {link.from_node}.{link.from_pin} drives two nodes - "
                        "use a Sequence node to run several things in order",
                        node=link.from_node, pin=link.from_pin,
                    )
                self.exec_edges[key] = link.to_node
            else:
                key = link.target
                if key in self.data_source:
                    raise CompileError(
                        f"input pin {link.to_node}.{link.to_pin} has two sources",
                        node=link.to_node, pin=link.to_pin,
                    )
                self.data_source[key] = link.source
                self.use_count[link.source] = self.use_count.get(link.source, 0) + 1

        self._warn_about_types()

    def _checked_pin(self, node_id: str, pin_name: str, direction: str):
        definition = self.defs.get(node_id)
        if definition is None:
            raise CompileError(f"link refers to unknown node {node_id!r}")
        try:
            return definition.pin(pin_name, direction)
        except GraphError as exc:
            raise CompileError(str(exc), node=node_id, pin=pin_name) from None

    def _warn_about_types(self) -> None:
        """Type mismatches are reported, never fatal - Python is dynamic."""
        for (to_node, to_pin), (from_node, from_pin) in self.data_source.items():
            source = self.defs[from_node].pin(from_pin, "out")
            target = self.defs[to_node].pin(to_pin, "in")
            if not _types_compatible(source.type, target.type):
                self.warnings.append(
                    f"{from_node}.{from_pin} is {source.type} but "
                    f"{to_node}.{to_pin} expects {target.type}"
                )

    # -------------------------------------------------------------- compiling

    def compile_body(self) -> list[str]:
        """The statements of the function, without the ``def`` line."""
        self.prepare()
        entry = self.graph.entry()
        if entry is None:
            raise CompileError("this graph has no core:entry node, so nothing can run")

        body = Emitter(indent=1)

        # Entry outputs are the function parameters: always in scope.
        entry_def = self.defs[entry.id]
        for pin in entry_def.outputs(data_only=True):
            self.values[(entry.id, pin.name)] = (self.names.reserve(pin.name), ())

        if self.graph.variables:
            body.comment("Graph variables")
            for variable in self.graph.variables:
                name = self.names.make(variable.name)
                if name != variable.name:
                    raise CompileError(
                        f"variable name {variable.name!r} collides with a parameter"
                    )
                body.line(f"{name} = {literal(variable.default).text}")
            body.blank()

        start = self.exec_edges.get((entry.id, "then"))
        if start is None:
            self.warnings.append("nothing is wired to the entry node's execution pin")
            body.line("pass")
            return body.lines

        self._plan_hoists(start)
        self.emit_chain(start, body, region=())
        if body.empty:
            body.line("pass")
        self._check_unreachable()
        return body.lines

    def _check_unreachable(self) -> None:
        reachable: set[str] = set()
        entry = self.graph.entry()
        if entry is None:  # pragma: no cover - checked earlier
            return
        stack = [entry.id]
        while stack:
            current = stack.pop()
            if current in reachable:
                continue
            reachable.add(current)
            for (node_id, _pin), target in self.exec_edges.items():
                if node_id == current:
                    stack.append(target)
        for node in self.graph.nodes:
            definition = self.defs[node.id]
            if definition.pure or node.id in reachable:
                continue
            if any(p.is_exec for p in definition.pins):
                self.warnings.append(
                    f"node {node.title or node.op} ({node.id}) is never reached - "
                    "its execution pin is not wired"
                )

    # -------------------------------------------------------------- planning

    def _plan_hoists(self, start: str) -> None:
        """Decide where each shared pure value should be computed.

        A pure node used twice becomes a named local - but *where* matters. Put
        it inside the first branch that needs it and the other branch has to
        recompute it; put it too early and it is computed on paths that never
        use it. The right place is the deepest block that encloses every use,
        which is what this works out, before a single line is emitted.
        """
        self._walk_regions(start, ())
        memo: dict[Ref, list[tuple[int, ...]]] = {}

        def regions_using(ref: Ref) -> list[tuple[int, ...]]:
            """Where this output is *really* consumed, seeing through pure nodes."""
            if ref in memo:
                return memo[ref]
            memo[ref] = []  # guards data cycles; reported separately
            found: list[tuple[int, ...]] = []
            for (to_node, _to_pin), source in self.data_source.items():
                if source != ref:
                    continue
                if self.defs[to_node].pure:
                    for pin in self.defs[to_node].outputs(data_only=True):
                        found.extend(regions_using((to_node, pin.name)))
                else:
                    found.extend(self._regions.get(to_node, []))
            memo[ref] = found
            return found

        # Two uses is not the only reason to name a value. A node that several
        # branches converge on is emitted once per branch, so even a single
        # wire can end up inlined in two places; counting the *places it is
        # emitted* catches both cases.
        for ref in list(self.use_count):
            node = self.graph.node(ref[0])
            definition = self.defs[ref[0]]
            if not definition.pure or node.op in ("core:get_var", "core:literal"):
                continue
            if node.inline is True:
                continue
            regions = regions_using(ref)
            if len(regions) < 2:
                continue
            common = _common_prefix(regions)
            self._hoist_plan.setdefault(common, []).append(ref)

    def _walk_regions(self, node_id: str | None, region: tuple[int, ...]) -> None:
        """Record which block(s) each node ends up in, without emitting anything.

        The block numbering here has to match emission exactly, so both use the
        same depth-first order: a branch numbers its true side before its false
        side, a loop numbers its body before continuing.
        """
        seq = [0]
        seen: set[tuple[str, tuple[int, ...]]] = set()

        def child(current: tuple[int, ...]) -> tuple[int, ...]:
            seq[0] += 1
            return current + (seq[0],)

        def walk(current_id: str | None, current: tuple[int, ...]) -> None:
            guard = 0
            while current_id is not None:
                guard += 1
                if guard > 10_000:  # pragma: no cover - defensive
                    return
                if (current_id, current) in seen:
                    return
                seen.add((current_id, current))
                self._regions.setdefault(current_id, []).append(current)
                node = self.graph.node(current_id)
                op = node.op
                if op == "core:branch":
                    for pin in ("true", "false"):
                        walk(self.exec_edges.get((current_id, pin)), child(current))
                    return
                if op == "core:sequence":
                    for pin in self.defs[current_id].outputs():
                        if pin.is_exec:
                            walk(self.exec_edges.get((current_id, pin.name)), current)
                    return
                if op in ("core:for_each", "core:for_range", "core:while"):
                    walk(self.exec_edges.get((current_id, "body")), child(current))
                    current_id = self.exec_edges.get((current_id, "completed"))
                    continue
                if op in ("core:return", "core:break", "core:continue"):
                    return
                current_id = self.exec_edges.get(
                    (current_id, self._exec_out_name(self.defs[current_id]))
                )

        walk(node_id, region)

    # ------------------------------------------------------------ exec chain

    def emit_chain(
        self, node_id: str | None, out: Emitter, *, region: tuple[int, ...] | None = None
    ) -> None:
        """Emit a run of statements, following the execution wire."""
        self._emit_hoists(region if region is not None else self._block_path, out)
        guard = 0
        while node_id is not None:
            guard += 1
            if guard > 10_000:  # pragma: no cover - defensive
                raise CompileError("execution chain does not terminate")
            node = self.graph.node(node_id)
            self._emit_comment_for(node, out)
            node_id = self.emit_node(node, out)

    def _emit_hoists(self, region: tuple[int, ...], out: Emitter) -> None:
        """Compute the shared pure values that belong at the top of this block."""
        planned = self._hoist_plan.pop(region, None)
        if not planned:
            return
        for ref in planned:
            if ref in self.values:
                continue
            node = self.graph.node(ref[0])
            expression = self.build_pure(node, self.defs[ref[0]], ref[1], out)
            name = self.names.make(self._name_for(node, ref[1]))
            out.line(f"{name} = {expression.text}")
            self.values[ref] = (name, self._block_path)

    def emit_node(self, node: Node, out: Emitter) -> str | None:
        """Emit one node. Returns the next node in the chain, if any."""
        definition = self.defs[node.id]
        op = node.op

        if op == "core:branch":
            return self._emit_branch(node, out)
        if op == "core:sequence":
            return self._emit_sequence(node, out)
        if op == "core:for_each":
            return self._emit_for_each(node, out)
        if op == "core:for_range":
            return self._emit_for_range(node, out)
        if op == "core:while":
            return self._emit_while(node, out)
        if op in ("core:break", "core:continue"):
            if self._loop_depth == 0:
                raise CompileError(
                    f"{op.split(':')[1]} is outside a loop", node=node.id
                )
            out.line(op.split(":")[1])
            return None
        if op == "core:return":
            self._emit_return(node, out)
            return None
        if op == "core:entry":
            return self.exec_edges.get((node.id, "then"))

        self._emit_statement(node, definition, out)
        return self.exec_edges.get((node.id, self._exec_out_name(definition)))

    @staticmethod
    def _exec_out_name(definition: NodeDef) -> str:
        for pin in definition.outputs():
            if pin.is_exec:
                return pin.name
        return "then"

    # ------------------------------------------------------------ statements

    def _emit_statement(self, node: Node, definition: NodeDef, out: Emitter) -> None:
        op = node.op
        if op == "core:log":
            level = str(node.config.get("level", "info"))
            message = self.value_of(node, "message", out, required=True)
            out.line(f"ctx.{level}({message.text})")
            return
        if op == "core:metric":
            name = self.value_of(node, "name", out, required=True)
            value = self.value_of(node, "value", out)
            args = name.text + (f", {value.text}" if value is not None else "")
            out.line(f"ctx.metric({args})")
            return
        if op == "core:skip":
            reason = self.value_of(node, "reason", out)
            out.line(f"ctx.skip({reason.text if reason is not None else ''})")
            if self.exec_edges.get((node.id, "then")):
                self.warnings.append(
                    f"nodes after {node.title or 'Skip entity'} ({node.id}) never run - "
                    "ctx.skip() raises"
                )
            return
        if op == "core:checkpoint":
            out.line("ctx.checkpoint()")
            return
        if op == "core:method":
            expression = self._build_method(node, self.defs[node.id], out)
            self._assign_or_call(node, "result", expression.text, out)
            return self.exec_edges.get((node.id, "then"))
        if op == "core:db_execute":
            sql = self.value_of(node, "sql", out, required=True)
            params = self.value_of(node, "params", out)
            args = sql.text + (f", {params.text}" if params is not None else "")
            statement = f"ctx.db.execute({args})"
            self._assign_or_call(node, "cursor", statement, out, desired="cursor")
            return
        if op == "core:set_var":
            name = str(node.config["name"])
            value = self.value_of(node, "value", out, required=True)
            out.line(f"{name} = {value.text}")
            self.values[(node.id, "value")] = (name, self._block_path)
            return

        # Everything else is a call: fn:, graph:, py:.
        expression = self.build_call(node, definition, out)
        result = next((p for p in definition.outputs(data_only=True)), None)
        if result is None:
            out.line(expression.text)
            return
        self._assign_or_call(node, result.name, expression.text, out)

    def _assign_or_call(
        self, node: Node, pin: str, statement: str, out: Emitter, desired: str | None = None
    ) -> None:
        """Assign the call's result only when something actually reads it."""
        if self.use_count.get((node.id, pin), 0) == 0:
            out.line(statement)
            return
        name = self.names.make(desired or self._name_for(node, pin))
        out.line(f"{name} = {statement}")
        self.values[(node.id, pin)] = (name, self._block_path)

    # ------------------------------------------------------------ flow nodes

    def _emit_branch(self, node: Node, out: Emitter) -> str | None:
        condition = self.value_of(node, "condition", out, required=True)
        on_true = self.exec_edges.get((node.id, "true"))
        on_false = self.exec_edges.get((node.id, "false"))
        if on_true is None and on_false is None:
            self.warnings.append(f"branch {node.id} has neither output wired")
            return None
        if on_true is None:
            out.line(f"if {unary('not', condition).wrapped(P_TERNARY)}:")
            with self._nested(out):
                self.emit_chain(on_false, out)
            return None
        out.line(f"if {condition.wrapped(P_TERNARY)}:")
        before = len(out.lines)
        with self._nested(out):
            self.emit_chain(on_true, out)
        if on_false is None:
            return None
        if _always_leaves(out.lines[before:], out.level + 1):
            # The true branch always returns or breaks, so an else would only
            # add a level of indentation for nothing.
            self.emit_chain(on_false, out)
        else:
            out.line("else:")
            with self._nested(out):
                self.emit_chain(on_false, out)
        return None

    def _emit_sequence(self, node: Node, out: Emitter) -> str | None:
        definition = self.defs[node.id]
        targets = [
            self.exec_edges.get((node.id, pin.name))
            for pin in definition.outputs()
            if pin.is_exec
        ]
        wired = [t for t in targets if t]
        for index, target in enumerate(wired):
            if index:
                out.blank()
            self.emit_chain(target, out)
        if not wired:
            self.warnings.append(f"sequence {node.id} has nothing wired")
        return None

    def _emit_for_each(self, node: Node, out: Emitter) -> str | None:
        iterable = self.value_of(node, "iterable", out, required=True)
        body_target = self.exec_edges.get((node.id, "body"))
        completed = self.exec_edges.get((node.id, "completed"))

        item_used = self.use_count.get((node.id, "item"), 0) > 0
        index_used = self.use_count.get((node.id, "index"), 0) > 0
        item_name = self.names.make(node.title or "item")
        index_name = self.names.make(f"{item_name}_index" if item_used else "index")

        if index_used:
            target = f"{index_name}, {item_name if item_used else '_'}"
            source = f"enumerate({iterable.text})"
        else:
            target = item_name if item_used else "_"
            source = iterable.wrapped(P_TERNARY)
        out.line(f"for {target} in {source}:")
        with self._nested(out, loop=True):
            if item_used:
                self.values[(node.id, "item")] = (item_name, self._block_path)
            if index_used:
                self.values[(node.id, "index")] = (index_name, self._block_path)
            if body_target is None:
                out.line("pass")
                self.warnings.append(f"for-each {node.id} has an empty body")
            else:
                self.emit_chain(body_target, out)
        return completed

    def _emit_for_range(self, node: Node, out: Emitter) -> str | None:
        start = self.value_of(node, "start", out)
        stop = self.value_of(node, "stop", out, required=True)
        step = self.value_of(node, "step", out)
        parts = [stop.text]
        if start is not None and start.text != "0":
            parts = [start.text, stop.text]
        if step is not None and step.text != "1":
            if len(parts) == 1:
                parts = ["0", stop.text]
            parts.append(step.text)
        index_name = self.names.make(node.title or "index")
        used = self.use_count.get((node.id, "index"), 0) > 0
        out.line(f"for {index_name if used else '_'} in range({', '.join(parts)}):")
        with self._nested(out, loop=True):
            if used:
                self.values[(node.id, "index")] = (index_name, self._block_path)
            body_target = self.exec_edges.get((node.id, "body"))
            if body_target is None:
                out.line("pass")
            else:
                self.emit_chain(body_target, out)
        return self.exec_edges.get((node.id, "completed"))

    def _emit_while(self, node: Node, out: Emitter) -> str | None:
        # The condition has to be re-read every iteration, so it is never
        # hoisted into a variable before the loop.
        previous, self._no_hoist = self._no_hoist, True
        try:
            condition = self.value_of(node, "condition", out, required=True)
        finally:
            self._no_hoist = previous
        source = self.data_source.get((node.id, "condition"))
        if source and not self.defs[source[0]].pure:
            self.warnings.append(
                f"the while condition on {node.id} comes from {source[0]}, which runs "
                "once before the loop - the loop may never end"
            )
        out.line(f"while {condition.wrapped(P_TERNARY)}:")
        with self._nested(out, loop=True):
            body_target = self.exec_edges.get((node.id, "body"))
            if body_target is None:
                out.line("break")
                self.warnings.append(f"while {node.id} has an empty body")
            else:
                self.emit_chain(body_target, out)
        return self.exec_edges.get((node.id, "completed"))

    def _emit_return(self, node: Node, out: Emitter) -> None:
        definition = self.defs[node.id]
        pins = definition.inputs(data_only=True)
        values = [self.value_of(node, pin.name, out) for pin in pins]
        given = [v.text if v is not None else "None" for v in values]
        if not pins or all(v is None for v in values):
            out.line("return")
            return
        out.line(f"return {', '.join(given)}")

    # ---------------------------------------------------------- expressions

    def value_of(
        self, node: Node, pin_name: str, out: Emitter, *, required: bool = False
    ) -> Expr | None:
        """What is on one input pin: a wire, a typed-in literal, or a default."""
        definition = self.defs[node.id]
        pin = definition.pin(pin_name, "in")
        source = self.data_source.get((node.id, pin_name))
        if source is not None:
            return self.expr_of(source, out)
        if pin_name in node.values:
            return literal(node.values[pin_name])
        if pin.has_default:
            return None if not required else literal(pin.default)
        if required:
            raise CompileError(
                f"input {pin_name!r} is not connected and has no value",
                node=node.id, pin=pin_name,
            )
        return None

    def expr_of(self, ref: Ref, out: Emitter) -> Expr:
        """The expression for one *output* pin."""
        node_id, pin_name = ref
        node = self.graph.node(node_id)
        definition = self.defs[node_id]

        if not definition.pure:
            assigned = self.values.get(ref)
            if assigned is None:
                raise CompileError(
                    f"{node.title or node.op}.{pin_name} is read before the node has run - "
                    "wire its execution pin earlier in the chain",
                    node=node_id, pin=pin_name,
                )
            name, block = assigned
            if not self._in_scope(block):
                raise CompileError(
                    f"{node.title or node.op}.{pin_name} only exists inside the block it is "
                    "produced in (a loop body, or one side of a branch)",
                    node=node_id, pin=pin_name,
                )
            return atom(name)

        cached = self.values.get(ref)
        if cached is not None and self._in_scope(cached[1]):
            return atom(cached[0])

        if node_id in self._visiting:
            raise CompileError(
                f"the data wires around {node.title or node.op} form a loop", node=node_id
            )
        self._visiting.add(node_id)
        try:
            expression = self.build_pure(node, definition, pin_name, out)
        finally:
            self._visiting.discard(node_id)

        if self._should_hoist(node, definition, ref):
            name = self.names.make(self._name_for(node, pin_name))
            out.line(f"{name} = {expression.text}")
            self.values[ref] = (name, self._block_path)
            return atom(name)
        return expression

    def _name_for(self, node: Node, pin: str) -> str:
        """The local name for one output, qualified when the short one would clash.

        ``entity.id`` would otherwise become ``id_``, which says nothing. When
        the attribute or key shadows a builtin, the source's own name is folded
        in: ``entity_id``.
        """
        if node.title:
            return node.title
        base = _node_label(node, pin)
        if node.op in ("core:get_attr", "core:get_item") and hasattr(_builtins, base):
            source_pin = "object" if node.op == "core:get_attr" else "container"
            source = self.data_source.get((node.id, source_pin))
            if source is not None:
                parent = self.graph.node(source[0])
                prefix = (
                    parent.title
                    or (self.values[source][0] if source in self.values else "")
                    or _node_label(parent, source[1])
                )
                if prefix:
                    return f"{prefix}_{base}"
        return base

    def _should_hoist(self, node: Node, definition: NodeDef, ref: Ref) -> bool:
        if self._no_hoist or node.inline is True:
            return False
        if node.inline is False:
            return True
        if node.op in ("core:get_var", "core:literal"):
            return False  # already an atom; a temporary would only add noise
        return self.use_count.get(ref, 0) > 1

    def _in_scope(self, block: tuple[int, ...]) -> bool:
        return self._block_path[: len(block)] == block

    # ------------------------------------------------------- pure node code

    def build_pure(self, node: Node, definition: NodeDef, pin: str, out: Emitter) -> Expr:
        op = node.op
        if op == "core:literal":
            return literal(node.config.get("value"))
        if op == "core:get_var":
            return atom(str(node.config["name"]))
        if op == "core:binary_op":
            symbol = core_nodes.BINARY_OPS[str(node.config.get("op", "add"))][0]
            return binary(
                symbol,
                self.value_of(node, "a", out, required=True),
                self.value_of(node, "b", out, required=True),
            )
        if op == "core:unary_op":
            symbol = core_nodes.UNARY_OPS[str(node.config.get("op", "not"))][0]
            return unary(symbol, self.value_of(node, "a", out, required=True))
        if op == "core:select":
            return ternary(
                self.value_of(node, "condition", out, required=True),
                self.value_of(node, "if_true", out, required=True),
                self.value_of(node, "if_false", out, required=True),
            )
        if op == "core:coalesce":
            value = self.value_of(node, "value", out, required=True)
            fallback = self.value_of(node, "fallback", out, required=True)
            return ternary(
                Expr(f"{value.wrapped(P_COMPARE)} is not None", P_COMPARE), value, fallback
            )
        if op == "core:is_none":
            value = self.value_of(node, "value", out, required=True)
            return Expr(f"{value.wrapped(P_COMPARE)} is None", P_COMPARE)
        if op == "core:get_item":
            container = self.value_of(node, "container", out, required=True)
            key = self.value_of(node, "key", out, required=True)
            if node.config.get("safe"):
                default = self.value_of(node, "default", out)
                args = key.text + (f", {default.text}" if default is not None else "")
                return call(f"{container.wrapped(P_CALL)}.get({args})")
            return Expr(f"{container.wrapped(P_CALL)}[{key.text}]", P_CALL)
        if op == "core:get_attr":
            obj = self.value_of(node, "object", out, required=True)
            return Expr(f"{obj.wrapped(P_CALL)}.{node.config['name']}", P_CALL)
        if op == "core:method":
            return self._build_method(node, definition, out)
        if op in ("core:make_list", "core:make_tuple"):
            count = len(definition.inputs(data_only=True))
            items = [
                self.value_of(node, f"item_{i}", out, required=True).text for i in range(count)
            ]
            if op == "core:make_list":
                return atom("[" + ", ".join(items) + "]")
            if len(items) == 1:
                return atom(f"({items[0]},)")
            return atom("(" + ", ".join(items) + ")")
        if op == "core:make_dict":
            keys = [str(k) for k in (node.config.get("keys") or [])]
            pairs = [
                f"{literal(key).text}: "
                f"{self.value_of(node, f'value_{i}', out, required=True).text}"
                for i, key in enumerate(keys)
            ]
            return atom("{" + ", ".join(pairs) + "}")
        if op == "core:format":
            return self._build_format(node, out)
        if op == "core:cast":
            value = self.value_of(node, "value", out, required=True)
            return call(f"{node.config.get('to', 'str')}({value.text})")
        if op == "core:resource":
            name = self.value_of(node, "name", out, required=True)
            return call(f"ctx.resource({name.text})")
        if op == "core:setting":
            path = self.value_of(node, "path", out, required=True)
            default = self.value_of(node, "default", out)
            args = path.text + (f", {default.text}" if default is not None else "")
            return call(f"ctx.setting({args})")
        if node.kind in ("fn", "graph", "py"):
            return self.build_call(node, definition, out)
        raise CompileError(f"{op} cannot be used as a value", node=node.id, pin=pin)

    def _build_format(self, node: Node, out: Emitter) -> Expr:
        """A format node becomes a real f-string.

        Only the literal parts of the template are escaped. A backslash inside
        an f-string *expression* is a syntax error before Python 3.12, so an
        expression that would need one - anything containing a quote - is
        hoisted to a local first. That reads better anyway: a nested
        ``ctx.fn("bmi")(...)`` inside an f-string is not what anyone would write.
        """
        template = str(node.config.get("template", ""))
        names = core_nodes._placeholders(template)
        if not names:
            return literal(template)

        pieces: list[str] = []
        resolved: dict[str, str] = {}      # a name used twice is emitted once
        for chunk, name, suffix in _split_template(template, names):
            pieces.append(_escape_fstring_text(chunk))
            if name is None:
                continue
            if name not in resolved:
                text = self.value_of(node, name, out, required=True).text
                if '"' in text or "\\" in text or "\n" in text:
                    local = self.names.make(self._name_for(node, name))
                    out.line(f"{local} = {text}")
                    text = local
                resolved[name] = text
            pieces.append("{" + resolved[name] + suffix + "}")
        return atom(f'f"{"".join(pieces)}"')

    def _build_method(self, node: Node, definition: NodeDef, out: Emitter) -> Expr:
        """``object.name(args…, key=value…)`` - a method call on a value."""
        obj = self.value_of(node, "object", out, required=True)
        name = str(node.config.get("name", ""))
        count = max(0, int(node.config.get("args", 0) or 0))
        keywords = [str(k) for k in (node.config.get("kwargs") or [])]
        arguments: list[str] = []
        for index in range(count):
            value = self.value_of(node, f"arg_{index}", out)
            if value is None:
                break     # a trailing argument left empty is simply not passed
            arguments.append(value.text)
        for key in keywords:
            value = self.value_of(node, key, out)
            if value is not None:
                arguments.append(f"{key}={value.text}")
        return call(f"{obj.wrapped(P_CALL)}.{name}({', '.join(arguments)})")

    def build_call(self, node: Node, definition: NodeDef, out: Emitter) -> Expr:
        """A call to a pipeline function, another graph, or a reflected callable."""
        arguments: list[str] = []
        skipped = False
        for pin in definition.inputs(data_only=True):
            value = self.value_of(node, pin.name, out)
            if value is None:
                skipped = True
                continue
            if pin.variadic:
                arguments.append(f"*{value.wrapped(P_CALL)}")
            elif skipped or pin.keyword_only:
                arguments.append(f"{pin.name}={value.text}")
            else:
                arguments.append(value.text)

        if definition.kind in ("fn", "graph"):
            # Both resolve to a function registered on the pipeline; a graph
            # gets there by compiling to its own .graphlib.py.
            name = definition.meta.get("graph") or definition.target
            target = f"ctx.fn({literal(name).text})"
            if definition.kind == "graph" or definition.implicit_ctx:
                arguments.insert(0, "ctx")
        else:
            target = definition.target
            self.imports.update(i for i in definition.imports if i)
        return call(f"{target}({', '.join(arguments)})")

    # ------------------------------------------------------------- comments

    def _emit_comment_for(self, node: Node, out: Emitter) -> None:
        """Comment boxes and per-node notes become real comments."""
        box = self._comment_for(node)
        if box is not None and box.id not in self._emitted_comments:
            self._emitted_comments.add(box.id)
            out.blank()
            out.comment(box.text)
        if node.comment:
            out.comment(node.comment)

    def _comment_for(self, node: Node):
        """The smallest comment box whose rectangle contains this node."""
        best = None
        best_area = None
        x, y = node.pos
        for box in self.graph.comments:
            left, top = box.pos
            width, height = box.size
            if left <= x <= left + width and top <= y <= top + height:
                area = width * height
                if best_area is None or area < best_area:
                    best, best_area = box, area
        return best

    # ---------------------------------------------------------------- blocks

    def _nested(self, out: Emitter, *, loop: bool = False):
        compiler = self

        class _Block:
            def __enter__(self) -> None:
                compiler._block_seq += 1
                compiler._block_path = compiler._block_path + (compiler._block_seq,)
                if loop:
                    compiler._loop_depth += 1
                out.level += 1

            def __exit__(self, *exc: Any) -> None:
                out.level -= 1
                if loop:
                    compiler._loop_depth -= 1
                compiler._block_path = compiler._block_path[:-1]

        return _Block()


TERMINATORS = ("return", "raise", "break", "continue", "ctx.skip(")


def _always_leaves(lines: list[str], level: int) -> bool:
    """Does this block always leave, so that an ``else`` would be redundant?"""
    from graetl.graph.emit import INDENT

    prefix = INDENT * level
    for line in reversed(lines):
        if not line.strip():
            continue
        if not line.startswith(prefix) or line[len(prefix):len(prefix) + 1] == " ":
            return False  # ended inside a nested block: cannot tell, be safe
        body = line[len(prefix):]
        return body.startswith(TERMINATORS)
    return False


def _common_prefix(paths: list[tuple[int, ...]]) -> tuple[int, ...]:
    if not paths:
        return ()
    shortest = min(len(p) for p in paths)
    out: list[int] = []
    for index in range(shortest):
        values = {p[index] for p in paths}
        if len(values) != 1:
            break
        out.append(paths[0][index])
    return tuple(out)


def _escape_fstring_text(text: str) -> str:
    """Escape the *literal* part of an f-string body. Braces double up."""
    return (
        text.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def _split_template(
    template: str, names: list[str]
) -> list[tuple[str, str | None, str]]:
    """``"a {x:>3} b"`` -> ``[("a ", "x", ":>3"), (" b", None, "")]``.

    Each item is (literal text before the placeholder, placeholder name or None,
    the conversion/format spec that followed the name). ``{{`` stays literal.
    """
    out: list[tuple[str, str | None, str]] = []
    buffer: list[str] = []
    index = 0
    while index < len(template):
        char = template[index]
        if char == "{" and template[index : index + 2] == "{{":
            buffer.append("{{")
            index += 2
            continue
        if char == "}" and template[index : index + 2] == "}}":
            buffer.append("}}")
            index += 2
            continue
        if char == "{":
            close = template.find("}", index)
            inner = template[index + 1 : close] if close != -1 else ""
            cut = min(
                (p for p in (inner.find("!"), inner.find(":")) if p != -1),
                default=len(inner),
            )
            name = inner[:cut].strip()
            if close != -1 and name in names:
                out.append(("".join(buffer), name, inner[cut:]))
                buffer = []
                index = close + 1
                continue
        buffer.append(char)
        index += 1
    out.append(("".join(buffer), None, ""))
    return out


def _node_label(node: Node, pin: str) -> str:
    """A readable variable name for one output pin.

    The author's title wins. Otherwise the name comes from what the node
    actually does - ``entity.data`` becomes ``data``, ``row.get("readings")``
    becomes ``readings`` - because ``get_attr_2`` tells a reader nothing.
    """
    if node.title:
        base = node.title
    elif node.op == "core:get_attr":
        base = str(node.config.get("name", "value"))
    elif node.op == "core:get_item":
        key = node.values.get("key")
        base = key if isinstance(key, str) and key else "item"
    elif node.op == "core:format":
        base = "text"
    elif node.op == "core:cast":
        base = f"as_{node.config.get('to', 'value')}"
    elif node.op == "core:make_dict":
        base = "record"
    elif node.op in ("core:make_list", "core:make_tuple"):
        base = "items"
    elif node.op == "core:select":
        base = "selected"
    elif node.op in ("core:coalesce", "core:literal", "core:binary_op", "core:unary_op"):
        base = "value"
    elif node.op == "core:is_none":
        base = "missing"
    elif node.op == "core:resource":
        base = str(node.values.get("name") or "resource")
    elif node.op == "core:method":
        base = str(node.config.get("name") or "result")
    elif node.op == "core:db_execute":
        base = "cursor"
    elif node.kind == "py":
        base = node.ref.split(".")[-1]
    elif node.kind in ("fn", "graph"):
        base = node.ref
    else:
        base = node.op.split(":")[-1]
    return base if pin in ("result", "value", "then", "cursor") else f"{base}_{pin}"


def _types_compatible(source: str, target: str) -> bool:
    if source in ("Any", "", target) or target in ("Any", ""):
        return True
    numeric = {"int", "float", "complex", "bool"}
    if source in numeric and target in numeric:
        return True
    if target in ("Iterable", "Sequence") and source in ("list", "tuple", "set", "dict", "str"):
        return True
    if "|" in target:
        return any(_types_compatible(source, part.strip()) for part in target.split("|"))
    if "|" in source:
        return any(_types_compatible(part.strip(), target) for part in source.split("|"))
    return False


# ------------------------------------------------------------------ assembly


def compile_function(
    graph: Graph, registry: NodeRegistry, *, names: Names | None = None
) -> CompiledFunction:
    """Compile one graph to a ``def`` block."""
    compiler = GraphCompiler(graph, registry, names=names)
    body = compiler.compile_body()

    out = Emitter()
    parameters = ["ctx"]
    if graph.kind == "module":
        # The entry node decides the shape: one entity, a batch of them, or
        # none at all. The runner calls the module to match.
        if graph.scope == "batch":
            parameters.append("entities")
        elif graph.scope != "once":
            parameters.append("entity")
    else:
        for pin in graph.inputs:
            if pin.has_default:
                parameters.append(f"{pin.name}={literal(pin.default).text}")
            else:
                parameters.append(pin.name)

    if graph.kind == "module":
        arguments = [f"version={graph.version}"]
        if graph.execution_layer:
            arguments.append(f"execution_layer={graph.execution_layer}")
        if graph.depends_on:
            arguments.append(f"depends_on={literal(list(graph.depends_on)).text}")
        if graph.scope != "entity":
            arguments.append(f"scope={literal(graph.scope).text}")
        if graph.scope == "batch" and graph.batch_size:
            arguments.append(f"batch_size={graph.batch_size}")
        for line in call_lines("@pipeline.module", arguments):
            out.line(line)
    else:
        arguments = [literal(graph.name).text]
        if graph.description:
            arguments.append(f"description={literal(graph.description).text}")
        if graph.category:
            arguments.append(f"category={literal(graph.category).text}")
        if graph.pure:
            arguments.append("pure=True")
        for line in call_lines("@pipeline.function", arguments):
            out.line(line)
        if graph.cache:
            # Below @pipeline.function, so the registry holds the cached
            # version - decorators apply bottom-up.
            size = f"maxsize={graph.cache_size}" if graph.cache_size else ""
            out.line(f"@cached({size})")

    out.line(f"def {graph.function_name}({', '.join(parameters)}):")
    with out.block():
        doc = graph.description or graph.title
        if doc:
            out.docstring(doc)
    out.lines.extend(body)
    return CompiledFunction(
        graph=graph, lines=out.lines, imports=compiler.imports, warnings=compiler.warnings
    )


def compile_module_file(
    graph: Graph,
    registry: NodeRegistry,
    *,
    helpers: Sequence[Graph] = (),
) -> CompiledFile:
    """Compile a ``.graph`` into the source of a complete ``<name>.module.py``."""
    if graph.kind != "module":
        raise CompileError(f"{graph.name} is a function graph, not a module graph")

    names = Names({"ctx", "entity", "pipeline", "get_pipeline"})
    compiled: list[CompiledFunction] = []
    imports: set[str] = set()
    warnings: list[str] = []

    for helper in helpers:
        helper.meta.setdefault("top_level", False)
        piece = compile_function(helper, registry, names=Names({"ctx", "pipeline"}))
        compiled.append(piece)
        imports |= piece.imports
        warnings += [f"{helper.name}: {w}" for w in piece.warnings]

    main = compile_function(graph, registry, names=names)
    imports |= main.imports
    warnings += main.warnings

    out = Emitter()
    title = graph.title or graph.name
    source_name = graph.path.name if graph.path else f"{graph.name}.graph"
    out.docstring(
        f"{title}\n\n"
        + (graph.description + "\n\n" if graph.description else "")
        + f"Generated from {source_name} by GraETL {__version__}.\n"
        "Do not edit this file: compiling the graph again overwrites it. Edit the "
        "graph, or delete it and keep this file as ordinary Python."
    )
    out.line(f"# graetl:generated-from {source_name}")
    out.line(f"# graetl:graph-digest {graph_digest(graph)}")
    out.blank()
    out.line("from __future__ import annotations")
    if imports:
        out.blank()
        for module in sorted(imports):
            out.line(f"import {module}")
    out.blank()
    out.line("from graetl.sdk import get_pipeline")
    out.blank(2)
    out.line("pipeline = get_pipeline()")

    for piece in compiled:
        out.blank(2)
        out.lines.extend(piece.lines)

    out.blank(2)
    out.lines.extend(main.lines)

    return CompiledFile(
        graph=graph,
        source=out.render(),
        path_name=f"{graph.name}.module.py",
        imports=imports,
        warnings=warnings,
        digest=graph_digest(graph),
    )


def compile_graphlib_file(library: Library | Graph, registry: NodeRegistry) -> CompiledFile:
    """Compile a function-graph library into one ``<name>.graphlib.py``.

    A library holds one or more functions; each registers itself with
    ``@pipeline.function``, so module graphs in any folder can call it and the
    same helper is reachable from hand-written Python as ``ctx.fn("name")``.
    Grouping several in one file is organisation, not scope.
    """
    if isinstance(library, Graph):
        if library.kind != "function":
            raise CompileError(f"{library.name} is a module graph, not a function graph")
        library = Library(name=library.name, functions=[library], path=library.path)
    if not library.functions:
        raise CompileError(f"{library.name} has no functions")

    # One name pool for the file, so two functions never collide on a local.
    names = Names({"ctx", "pipeline", "cached", "get_pipeline"})
    pieces = [compile_function(graph, registry, names=names) for graph in library.functions]
    imports: set[str] = set()
    warnings: list[str] = []
    for piece in pieces:
        imports |= set(piece.imports)
        warnings.extend(piece.warnings)

    source_name = library.path.name if library.path else f"{library.name}.graphlib"
    digest = library_digest(library)
    out = Emitter()
    listed = ", ".join(graph.name for graph in library.functions)
    out.docstring(
        f"{library.title or library.name}\n\n"
        + (library.description + "\n\n" if library.description else "")
        + f"Pipeline function(s): {listed}.\n\n"
        + f"Generated from {source_name} by GraETL {__version__}.\n"
        "Do not edit this file: compiling the graph again overwrites it."
    )
    out.line(f"# graetl:generated-from {source_name}")
    out.line(f"# graetl:graph-digest {digest}")
    out.blank()
    out.line("from __future__ import annotations")
    if imports:
        out.blank()
        for module in sorted(imports):
            out.line(f"import {module}")
    out.blank()
    if any(graph.cache for graph in library.functions):
        out.line("from graetl.sdk import cached, get_pipeline")
    else:
        out.line("from graetl.sdk import get_pipeline")
    out.blank(2)
    out.line("pipeline = get_pipeline()")
    for piece in pieces:
        out.blank(2)
        out.lines.extend(piece.lines)
    return CompiledFile(
        graph=library.functions[0],
        source=out.render(),
        path_name=f"{library.name}.graphlib.py",
        imports=imports,
        warnings=warnings,
        digest=digest,
    )


def load_graphs(paths: Iterable[Any]) -> list[Graph]:
    return [Graph.load(p) for p in paths]
