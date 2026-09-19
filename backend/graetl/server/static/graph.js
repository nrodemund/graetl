/* Node-graph canvas --------------------------------------------------------
 * Comment boxes behind, wires in an SVG layer, nodes as real DOM elements on
 * top (easier to style, and pin positions have to be measured anyway).
 *
 * Editing follows Unreal's Blueprint conventions, because that is what the
 * muscle memory expects: left-drag on empty space marquee-selects, right-drag
 * pans, drag a pin to wire it, drag a wired input away to pick the wire up,
 * Alt-click a pin to break its links. Nothing is saved until you ask - every
 * save recompiles the graph, and compiling half an edit is just noise.
 *
 * Geometry note: pin offsets are measured once per full render, relative to
 * their node. Dragging then moves nodes and redraws wires from
 * `node.pos + offset` without touching the DOM layout, which is what keeps a
 * hundred-node graph smooth.
 * ------------------------------------------------------------------------- */

const NS = "http://www.w3.org/2000/svg";

/* Pin colours follow the type, the way a Blueprint graph reads at a glance. */
const TYPE_COLORS = {
  exec: "#e8edf7",
  bool: "#ef5f6b",
  int: "#63d297", float: "#63d297", complex: "#63d297",
  str: "#ef86c8", bytes: "#ef86c8",
  list: "#5b8cff", tuple: "#5b8cff", Iterable: "#5b8cff", Sequence: "#5b8cff",
  dict: "#4dd0e1", set: "#4dd0e1",
  Context: "#ffcb6b", Entity: "#ffcb6b",
  Cursor: "#b794f6",
};

const GRID = 8;

/* Comment box colours. Named, not free-form: a palette keeps a graph legible,
   and the names survive a theme change in a way hex codes would not. */
const COMMENT_COLORS = {
  "": { label: "Default", tint: "#5b8cff" },
  blue: { label: "Blue", tint: "#5b8cff" },
  green: { label: "Green", tint: "#63d297" },
  amber: { label: "Amber", tint: "#ffcb6b" },
  red: { label: "Red", tint: "#ef5f6b" },
  purple: { label: "Purple", tint: "#b794f6" },
  teal: { label: "Teal", tint: "#4dd0e1" },
  pink: { label: "Pink", tint: "#ef86c8" },
  grey: { label: "Grey", tint: "#8b95a8" },
};

function pinColor(type) {
  return TYPE_COLORS[type] || "#8b95a8";
}

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

function headerClass(node, definition) {
  if (node.op.startsWith("core:entry") || node.op === "core:return") return "gv-n-entry";
  if (definition && definition.category === "Flow") return "gv-n-flow";
  return `gv-n-${node.op.split(":")[0]}`;
}

const clone = (value) => JSON.parse(JSON.stringify(value));

/** Numbers, booleans and null come back typed; anything else stays a string. */
function parseLiteral(text) {
  const trimmed = String(text).trim();
  if (!trimmed) return "";
  try {
    return JSON.parse(trimmed);
  } catch {
    return trimmed;
  }
}

/** Can these two pins be wired together? Returns null, or why not. */
function refuseLink(source, target) {
  if (!source || !target || !source.pin || !target.pin) return "unknown pin";
  if (source.node === target.node) return "a node cannot wire into itself";
  if (source.dir === target.dir) {
    return source.dir === "out" ? "both are outputs" : "both are inputs";
  }
  const a = source.dir === "out" ? source : target;
  const b = source.dir === "out" ? target : source;
  if (a.pin.type === "exec" && b.pin.type !== "exec") {
    return "an execution pin only connects to another execution pin";
  }
  if (a.pin.type !== "exec" && b.pin.type === "exec") {
    return "a data pin cannot drive execution";
  }
  return null;
}

/** Every list the editor mutates has to exist, whatever the document omitted. */
function normalise(document) {
  const g = document || {};
  g.nodes = g.nodes || [];
  g.links = g.links || [];
  g.comments = g.comments || [];
  return g;
}

/* What a module graph starts with, and so how the runner calls it. */
const ENTRY_KINDS = [
  { op: "core:entry", label: "Each entity",
    hint: "fn(ctx, entity) — one at a time, one transaction each" },
  { op: "core:entry_batch", label: "A batch of entities",
    hint: "fn(ctx, entities) — a list at a time, in one transaction" },
  { op: "core:entry_once", label: "Once per run",
    hint: "fn(ctx) — no entity, when this layer comes up" },
];
const ENTRY_OPS = ENTRY_KINDS.map((k) => k.op);

/** Looks like a dotted Python path, i.e. something reflection could resolve. */
const REFLECTABLE = /^[A-Za-z_][\w.]*\.[A-Za-z_]\w*$/;

export function createGraphView(container, options = {}) {
  const {
    editable = false, onSelect, onOpen, onChange, onSave, onConfigure, onRun,
    catalog = [], describe,
  } = options;

  let graph = normalise(options.graph);
  let definitions = options.definitions || {};
  let problems = options.problems || [];
  let dirty = false;

  const view = { x: 40, y: 40, zoom: 1 };
  const pinOffset = new Map();          // "node/dir/pin" -> {dx, dy} inside the node
  const selection = new Set();          // node ids
  const selectedComments = new Set();
  const undoStack = [];
  const redoStack = [];
  let clipboard = null;
  let pointer = null;                   // the active gesture
  let editing = null;                   // an open inline input

  container.innerHTML = `
    <div class="gv${editable ? " editing" : ""}">
      <div class="gv-toolbar">
        <span class="gv-title"></span>
        <span class="gv-dirty"></span>
        <div class="spacer"></div>
        <span class="gv-problems dim"></span>
        ${editable ? `
          <button class="btn sm gv-undo" title="Undo (Ctrl+Z)" disabled>↶</button>
          <button class="btn sm gv-redo" title="Redo (Ctrl+Shift+Z)" disabled>↷</button>
          <button class="btn sm gv-add" title="Add a node (right-click the canvas)">＋ Node</button>
          <button class="btn sm primary gv-save" title="Save and compile (Ctrl+S)" disabled>Save</button>` : ""}
        ${onRun ? `<button class="btn sm gv-run"
            title="Save, then run this module for one entity with debug output (right-click for options)"
            >▶ Test</button>` : ""}
        <button class="btn sm gv-fit" title="Fit the whole graph">⤢ Fit</button>
        <button class="btn sm gv-reset" title="Back to 100%">1:1</button>
      </div>
      <div class="gv-viewport" tabindex="0">
        <div class="gv-world">
          <div class="gv-comments"></div>
          <svg class="gv-wires" xmlns="${NS}"></svg>
          <div class="gv-nodes"></div>
        </div>
        <div class="gv-marquee" hidden></div>
        <div class="gv-hint dim"></div>
      </div>
    </div>`;

  const viewport = container.querySelector(".gv-viewport");
  const world = container.querySelector(".gv-world");
  const wires = container.querySelector(".gv-wires");
  const nodeLayer = container.querySelector(".gv-nodes");
  const commentLayer = container.querySelector(".gv-comments");
  const marquee = container.querySelector(".gv-marquee");
  const titleEl = container.querySelector(".gv-title");
  const dirtyEl = container.querySelector(".gv-dirty");
  const problemEl = container.querySelector(".gv-problems");
  const saveBtn = container.querySelector(".gv-save");
  const undoBtn = container.querySelector(".gv-undo");
  const redoBtn = container.querySelector(".gv-redo");

  container.querySelector(".gv-hint").textContent = editable
    ? "right-drag pans · left-drag selects · drag a pin to wire · Alt-click a pin to break · F2 renames"
    : "drag to pan · scroll to zoom";

  /* ------------------------------------------------------------ geometry */

  function worldPoint(event) {
    const rect = viewport.getBoundingClientRect();
    return {
      x: (event.clientX - rect.left - view.x) / view.zoom,
      y: (event.clientY - rect.top - view.y) / view.zoom,
    };
  }

  function nodeById(id) {
    return (graph.nodes || []).find((n) => n.id === id);
  }

  function pinPoint(nodeId, dir, pin) {
    const offset = pinOffset.get(`${nodeId}/${dir}/${pin}`);
    const node = nodeById(nodeId);
    if (!offset || !node) return null;
    return { x: (node.pos?.[0] || 0) + offset.dx, y: (node.pos?.[1] || 0) + offset.dy };
  }

  function pinSpec(nodeId, dir, name) {
    const definition = definitions[nodeId];
    if (!definition) return null;
    return definition.pins.find((p) => p.direction === dir && p.name === name) || null;
  }

  /* ------------------------------------------------------------- mutation */

  function snapshot() {
    return clone({ nodes: graph.nodes, links: graph.links, comments: graph.comments,
                   variables: graph.variables });
  }

  function mutate(fn, { rerender = true } = {}) {
    undoStack.push(snapshot());
    if (undoStack.length > 200) undoStack.shift();
    redoStack.length = 0;
    fn();
    markDirty();
    if (rerender) render();
    else { positionNodes(); drawWires(); }
  }

  function restore(state) {
    graph.nodes = state.nodes;
    graph.links = state.links;
    graph.comments = state.comments;
    graph.variables = state.variables;
    for (const id of [...selection]) if (!nodeById(id)) selection.delete(id);
    markDirty();
    render();
  }

  function undo() {
    if (!undoStack.length) return;
    redoStack.push(snapshot());
    restore(undoStack.pop());
  }

  function redo() {
    if (!redoStack.length) return;
    undoStack.push(snapshot());
    restore(redoStack.pop());
  }

  function markDirty(value = true) {
    dirty = value;
    if (dirtyEl) dirtyEl.textContent = dirty ? "● unsaved" : "";
    if (saveBtn) saveBtn.disabled = !dirty;
    if (undoBtn) undoBtn.disabled = !undoStack.length;
    if (redoBtn) redoBtn.disabled = !redoStack.length;
    if (dirty && onChange) onChange(graph);
  }

  function nextId(prefix = "n") {
    const taken = new Set((graph.nodes || []).map((n) => n.id));
    for (let i = 1; i < 100000; i += 1) {
      if (!taken.has(`${prefix}${i}`)) return `${prefix}${i}`;
    }
    return `${prefix}${Date.now()}`;
  }

  /* --------------------------------------------------------------- render */

  function render() {
    titleEl.textContent = `${graph.title || graph.name || "graph"} · ${
      (graph.nodes || []).length} node(s)`;
    problemEl.innerHTML = problems.length
      ? `<span class="gv-bad">${problems.length} unresolved node(s)</span>` : "";

    commentLayer.innerHTML = (graph.comments || []).map(renderComment).join("");
    nodeLayer.innerHTML = (graph.nodes || []).map(renderNode).join("");
    requestAnimationFrame(() => {
      measurePins();
      drawWires();
    });
    applyView();
    if (undoBtn) undoBtn.disabled = !undoStack.length;
    if (redoBtn) redoBtn.disabled = !redoStack.length;
  }

  function renderComment(box) {
    const [x, y] = box.pos || [0, 0];
    const [w, h] = box.size || [320, 180];
    const tint = (COMMENT_COLORS[box.color] || COMMENT_COLORS[""]).tint;
    return `<div class="gv-comment${selectedComments.has(box.id) ? " sel" : ""}${
        box.moves_nodes === false ? " loose" : ""}"
        data-comment="${esc(box.id)}"
        title="${box.moves_nodes === false
          ? "Moving this box leaves the nodes where they are"
          : "Moving this box moves the nodes on it (Alt-drag to skip)"}"
        style="left:${x}px;top:${y}px;width:${w}px;height:${h}px;--tint:${tint}">
        <div class="gv-comment-text" data-comment-text="${esc(box.id)}">${esc(box.text)}</div>
        ${editable ? `<div class="gv-comment-grip" data-comment-resize="${esc(box.id)}"></div>` : ""}
      </div>`;
  }

  function renderNode(node) {
    const definition = definitions[node.id];
    const bad = !definition;
    const title = node.title || (definition ? definition.title : node.op);
    const inputs = definition ? definition.pins.filter((p) => p.direction === "in") : [];
    const outputs = definition ? definition.pins.filter((p) => p.direction === "out") : [];
    const [x, y] = node.pos || [0, 0];

    const pinRow = (pin, side) => {
      const exec = pin.type === "exec";
      const wired = side === "in"
        ? (graph.links || []).some((l) => l.to[0] === node.id && l.to[1] === pin.name)
        : (graph.links || []).some((l) => l.from[0] === node.id && l.from[1] === pin.name);
      const literal = node.values?.[pin.name];
      const value = side === "in" && !exec && !wired
        ? `<span class="gv-lit${editable ? " editable" : ""}"
             data-literal="${esc(node.id)}/${esc(pin.name)}"
             >${literal === undefined ? "—" : esc(JSON.stringify(literal))}</span>`
        : "";
      const dot = `<span class="gv-pin${exec ? " exec" : ""}${wired ? " wired" : ""}"
          data-pin="${esc(node.id)}/${side}/${esc(pin.name)}"
          style="--pin:${pinColor(pin.type)}"
          title="${esc(pin.name)} : ${esc(pin.type)}"></span>`;
      const label = `<span class="gv-pin-name">${esc(pin.label || pin.name)}</span>`;
      return side === "in"
        ? `<div class="gv-row in">${dot}${label}${value}</div>`
        : `<div class="gv-row out">${label}${dot}</div>`;
    };

    const rows = [];
    for (let i = 0; i < Math.max(inputs.length, outputs.length); i += 1) {
      rows.push(
        `<div class="gv-pins">
           <div class="gv-col">${inputs[i] ? pinRow(inputs[i], "in") : ""}</div>
           <div class="gv-col">${outputs[i] ? pinRow(outputs[i], "out") : ""}</div>
         </div>`,
      );
    }

    return `<div class="gv-node${bad ? " bad" : ""}${selection.has(node.id) ? " sel" : ""}"
        data-node="${esc(node.id)}" style="left:${x}px;top:${y}px">
        <div class="gv-head ${headerClass(node, definition)}" data-head="${esc(node.id)}">
          <span class="gv-name" data-title="${esc(node.id)}">${esc(title)}</span>
          ${definition && definition.pure ? '<span class="gv-tag">pure</span>' : ""}
        </div>
        <div class="gv-op mono">${esc(node.op)}</div>
        <div class="gv-body">${rows.join("")}</div>
        ${bad ? `<div class="gv-err">${esc(problemFor(node.id))}</div>` : ""}
      </div>`;
  }

  function problemFor(nodeId) {
    const hit = problems.find((p) => p.node === nodeId);
    return hit ? hit.error : "this node could not be resolved";
  }

  /** Move node elements without rebuilding them. */
  function positionNodes() {
    for (const element of nodeLayer.children) {
      const node = nodeById(element.dataset.node);
      if (!node) continue;
      element.style.left = `${node.pos[0]}px`;
      element.style.top = `${node.pos[1]}px`;
      element.classList.toggle("sel", selection.has(node.id));
    }
    for (const element of commentLayer.children) {
      const box = (graph.comments || []).find((c) => c.id === element.dataset.comment);
      if (!box) continue;
      element.style.left = `${box.pos[0]}px`;
      element.style.top = `${box.pos[1]}px`;
      element.style.width = `${box.size[0]}px`;
      element.style.height = `${box.size[1]}px`;
      element.style.setProperty(
        "--tint", (COMMENT_COLORS[box.color] || COMMENT_COLORS[""]).tint);
      element.classList.toggle("sel", selectedComments.has(box.id));
    }
  }

  /* Document order is the stacking order, so the node you grab comes to the
     front - otherwise a node dropped on top of another buries it for good. */
  function raise(id) {
    const at = (graph.nodes || []).findIndex((n) => n.id === id);
    if (at === -1 || at === graph.nodes.length - 1) return;
    graph.nodes.push(graph.nodes.splice(at, 1)[0]);
    const element = nodeLayer.querySelector(`[data-node="${CSS.escape(id)}"]`);
    if (element) nodeLayer.appendChild(element);
  }

  /* Offsets are relative to the node, so a drag never needs a re-measure. */
  function measurePins() {
    pinOffset.clear();
    for (const element of nodeLayer.children) {
      const node = nodeById(element.dataset.node);
      if (!node) continue;
      const nodeRect = element.getBoundingClientRect();
      for (const pin of element.querySelectorAll("[data-pin]")) {
        const rect = pin.getBoundingClientRect();
        pinOffset.set(pin.dataset.pin, {
          dx: (rect.left + rect.width / 2 - nodeRect.left) / view.zoom,
          dy: (rect.top + rect.height / 2 - nodeRect.top) / view.zoom,
        });
      }
    }
  }

  function wirePath(from, to) {
    const reach = Math.max(40, Math.min(180, Math.abs(to.x - from.x) * 0.6));
    return `M ${from.x} ${from.y} C ${from.x + reach} ${from.y}, ${to.x - reach} ${to.y}, ${to.x} ${to.y}`;
  }

  function drawWires() {
    const parts = [];
    for (const [index, link] of (graph.links || []).entries()) {
      const from = pinPoint(link.from[0], "out", link.from[1]);
      const to = pinPoint(link.to[0], "in", link.to[1]);
      if (!from || !to) continue;
      const pin = pinSpec(link.from[0], "out", link.from[1]);
      const exec = pin && pin.type === "exec";
      const color = pin ? pinColor(pin.type) : "#8b95a8";
      // A fat transparent copy underneath makes the wire clickable.
      parts.push(
        `<path class="gv-wire-hit" d="${wirePath(from, to)}" data-link="${index}"
           fill="none" stroke="transparent" stroke-width="14" />
         <path class="gv-wire" d="${wirePath(from, to)}" data-link="${index}" fill="none"
           stroke="${color}" stroke-width="${exec ? 2.6 : 1.8}"
           stroke-opacity="${exec ? 0.95 : 0.75}" />`,
      );
    }
    if (pointer?.mode === "wire" && pointer.cursor) {
      const anchor = pinPoint(pointer.from.node, pointer.from.dir, pointer.from.pin.name);
      if (anchor) {
        const [a, b] = pointer.from.dir === "out"
          ? [anchor, pointer.cursor] : [pointer.cursor, anchor];
        parts.push(
          `<path class="gv-wire preview" d="${wirePath(a, b)}" fill="none"
             stroke="${pinColor(pointer.from.pin.type)}" stroke-width="2.4"
             stroke-dasharray="6 4" />`,
        );
      }
    }
    const bounds = worldBounds();
    wires.setAttribute("width", bounds.width);
    wires.setAttribute("height", bounds.height);
    wires.innerHTML = parts.join("");
  }

  function worldBounds() {
    let width = 1600;
    let height = 900;
    for (const node of graph.nodes || []) {
      width = Math.max(width, (node.pos?.[0] || 0) + 460);
      height = Math.max(height, (node.pos?.[1] || 0) + 420);
    }
    for (const box of graph.comments || []) {
      width = Math.max(width, (box.pos?.[0] || 0) + (box.size?.[0] || 0) + 80);
      height = Math.max(height, (box.pos?.[1] || 0) + (box.size?.[1] || 0) + 80);
    }
    return { width, height };
  }

  function applyView() {
    world.style.transform = `translate(${view.x}px, ${view.y}px) scale(${view.zoom})`;
  }

  /* ------------------------------------------------------------ selection */

  function setSelection(ids, comments = []) {
    selection.clear();
    selectedComments.clear();
    ids.forEach((id) => selection.add(id));
    comments.forEach((id) => selectedComments.add(id));
    positionNodes();
    if (onSelect) {
      const only = ids.length === 1 ? nodeById(ids[0]) : null;
      onSelect(only, only ? definitions[only.id] : null);
    }
  }

  /* ----------------------------------------------------------- the gestures */

  viewport.addEventListener("contextmenu", (event) => {
    // A right-drag pans; only a right *click* opens a menu.
    if (pointer?.moved) {
      event.preventDefault();
      return;
    }
    event.preventDefault();
    const pinEl = event.target.closest?.("[data-pin]");
    if (pinEl && editable) return pinMenu(event, parsePin(pinEl.dataset.pin));
    const wireEl = event.target.closest?.("[data-link]");
    if (wireEl && editable) return wireMenu(event, Number(wireEl.dataset.link));
    const nodeEl = event.target.closest?.("[data-node]");
    if (nodeEl) return nodeMenu(event, nodeById(nodeEl.dataset.node));
    const commentEl = event.target.closest?.("[data-comment]");
    if (commentEl && editable) return commentMenu(event, commentEl.dataset.comment);
    if (editable) canvasMenu(event);
  });

  function parsePin(key) {
    const [node, dir, ...rest] = key.split("/");
    const name = rest.join("/");
    return { node, dir, name, pin: pinSpec(node, dir, name) };
  }

  viewport.addEventListener("mousedown", (event) => {
    if (editing) commitEdit();
    viewport.focus({ preventScroll: true });
    const start = { x: event.clientX, y: event.clientY };

    // Right or middle button: pan.
    if (event.button === 1 || event.button === 2) {
      pointer = { mode: "pan", start, origin: { ...view }, moved: false };
      viewport.classList.add("panning");
      event.preventDefault();
      return;
    }
    if (event.button !== 0) return;

    const pinEl = editable && event.target.closest("[data-pin]");
    if (pinEl) {
      event.preventDefault();
      beginWire(parsePin(pinEl.dataset.pin), event);
      return;
    }

    const literalEl = editable && event.target.closest("[data-literal]");
    if (literalEl) {
      event.preventDefault();
      editLiteral(literalEl);
      return;
    }

    const grip = editable && event.target.closest("[data-comment-resize]");
    if (grip) {
      event.preventDefault();
      const box = (graph.comments || []).find((c) => c.id === grip.dataset.commentResize);
      undoStack.push(snapshot());
      pointer = { mode: "comment-resize", box, start, size: [...box.size], moved: false };
      return;
    }

    const nodeEl = event.target.closest("[data-node]");
    if (nodeEl) {
      const id = nodeEl.dataset.node;
      if (event.shiftKey || event.ctrlKey || event.metaKey) {
        if (selection.has(id)) selection.delete(id);
        else selection.add(id);
        positionNodes();
      } else if (!selection.has(id)) {
        setSelection([id]);
      } else if (onSelect) {
        onSelect(nodeById(id), definitions[id]);
      }
      if (!editable) return;
      event.preventDefault();
      raise(id);
      pointer = {
        mode: "nodes", start, moved: false,
        origins: [...selection].map((nid) => ({ id: nid, pos: [...nodeById(nid).pos] })),
        commentOrigins: [...selectedComments].map((cid) => {
          const box = graph.comments.find((c) => c.id === cid);
          return { id: cid, pos: [...box.pos] };
        }),
      };
      undoStack.push(snapshot());
      return;
    }

    const commentEl = editable && event.target.closest("[data-comment]");
    if (commentEl) {
      const id = commentEl.dataset.comment;
      const box = graph.comments.find((c) => c.id === id);
      if (!event.shiftKey) setSelection([], [id]);
      else selectedComments.add(id);
      event.preventDefault();
      undoStack.push(snapshot());
      // Moving a comment carries the nodes sitting on it, as Blueprint does -
      // unless this box says otherwise, or you hold Alt for one drag.
      const carry = box.moves_nodes !== false && !event.altKey;
      const inside = !carry ? [] : (graph.nodes || []).filter((n) =>
        n.pos[0] >= box.pos[0] && n.pos[0] <= box.pos[0] + box.size[0]
        && n.pos[1] >= box.pos[1] && n.pos[1] <= box.pos[1] + box.size[1]);
      pointer = {
        mode: "nodes", start, moved: false,
        origins: inside.map((n) => ({ id: n.id, pos: [...n.pos] })),
        commentOrigins: [{ id, pos: [...box.pos] }],
      };
      positionNodes();
      return;
    }

    // Empty canvas.
    if (editable) {
      if (!event.shiftKey) setSelection([]);
      pointer = { mode: "marquee", start, moved: false };
      marquee.hidden = false;
      marquee.style.cssText = `left:${start.x}px;top:${start.y}px;width:0;height:0`;
    } else {
      pointer = { mode: "pan", start, origin: { ...view }, moved: false };
      viewport.classList.add("panning");
    }
  });

  window.addEventListener("mousemove", (event) => {
    if (!pointer) return;
    const dx = event.clientX - pointer.start.x;
    const dy = event.clientY - pointer.start.y;
    if (Math.abs(dx) > 3 || Math.abs(dy) > 3) pointer.moved = true;

    if (pointer.mode === "pan") {
      view.x = pointer.origin.x + dx;
      view.y = pointer.origin.y + dy;
      applyView();
      return;
    }
    if (pointer.mode === "nodes") {
      const wx = dx / view.zoom;
      const wy = dy / view.zoom;
      for (const origin of pointer.origins) {
        const node = nodeById(origin.id);
        if (node) node.pos = [origin.pos[0] + wx, origin.pos[1] + wy];
      }
      for (const origin of pointer.commentOrigins || []) {
        const box = graph.comments.find((c) => c.id === origin.id);
        if (box) box.pos = [origin.pos[0] + wx, origin.pos[1] + wy];
      }
      positionNodes();
      drawWires();
      return;
    }
    if (pointer.mode === "comment-resize") {
      pointer.box.size = [
        Math.max(120, pointer.size[0] + dx / view.zoom),
        Math.max(80, pointer.size[1] + dy / view.zoom),
      ];
      positionNodes();
      return;
    }
    if (pointer.mode === "wire") {
      pointer.cursor = worldPoint(event);
      const over = event.target.closest?.("[data-pin]");
      highlightCompatible(over ? parsePin(over.dataset.pin) : null);
      drawWires();
      return;
    }
    if (pointer.mode === "marquee") {
      const rect = viewport.getBoundingClientRect();
      const x = Math.min(pointer.start.x, event.clientX);
      const y = Math.min(pointer.start.y, event.clientY);
      const w = Math.abs(dx);
      const h = Math.abs(dy);
      marquee.style.cssText =
        `left:${x - rect.left}px;top:${y - rect.top}px;width:${w}px;height:${h}px`;
      const box = {
        x0: (x - rect.left - view.x) / view.zoom,
        y0: (y - rect.top - view.y) / view.zoom,
        x1: (x - rect.left + w - view.x) / view.zoom,
        y1: (y - rect.top + h - view.y) / view.zoom,
      };
      selection.clear();
      for (const element of nodeLayer.children) {
        const node = nodeById(element.dataset.node);
        if (!node) continue;
        const nx = node.pos[0];
        const ny = node.pos[1];
        const nw = element.offsetWidth;
        const nh = element.offsetHeight;
        if (nx + nw > box.x0 && nx < box.x1 && ny + nh > box.y0 && ny < box.y1) {
          selection.add(node.id);
        }
      }
      positionNodes();
    }
  });

  window.addEventListener("mouseup", (event) => {
    if (!pointer) return;
    const finished = pointer;
    viewport.classList.remove("panning");
    marquee.hidden = true;

    if (finished.mode === "wire") {
      const over = event.target.closest?.("[data-pin]");
      finishWire(over ? parsePin(over.dataset.pin) : null, event);
    } else if (finished.mode === "nodes" && finished.moved) {
      // Snap to the grid so graphs stay tidy without any alignment fuss.
      for (const origin of finished.origins) {
        const node = nodeById(origin.id);
        if (node) node.pos = [Math.round(node.pos[0] / GRID) * GRID,
                              Math.round(node.pos[1] / GRID) * GRID];
      }
      for (const origin of finished.commentOrigins || []) {
        const box = graph.comments.find((c) => c.id === origin.id);
        if (box) box.pos = [Math.round(box.pos[0] / GRID) * GRID,
                            Math.round(box.pos[1] / GRID) * GRID];
      }
      positionNodes();
      drawWires();
      markDirty();
    } else if (finished.mode === "nodes" || finished.mode === "comment-resize") {
      if (finished.moved) markDirty();
      else undoStack.pop();           // a click, not a drag: no history entry
    }
    pointer = null;
    clearHighlights();
    // The gesture is over, so any preview wire has to go, refused or not.
    if (finished.mode === "wire") drawWires();
  });

  /* ---------------------------------------------------------------- wires */

  function beginWire(from, event) {
    // Dragging a wired input picks that wire up, exactly as Blueprint does.
    if (from.dir === "in") {
      const index = (graph.links || []).findIndex(
        (l) => l.to[0] === from.node && l.to[1] === from.name);
      if (index !== -1 && !event.altKey) {
        const link = graph.links[index];
        undoStack.push(snapshot());
        graph.links.splice(index, 1);
        markDirty();
        const source = parsePin(`${link.from[0]}/out/${link.from[1]}`);
        pointer = { mode: "wire", from: source, cursor: worldPoint(event), moved: true,
                    start: { x: event.clientX, y: event.clientY },
                    reconnect: true, picked: link };
        render();
        return;
      }
    }
    if (event.altKey) {
      breakLinks(from);
      return;
    }
    pointer = { mode: "wire", from, cursor: worldPoint(event), moved: false,
                start: { x: event.clientX, y: event.clientY } };
    drawWires();
  }

  function finishWire(target, event) {
    const from = pointer.from;
    if (!target) {
      // Dropped on empty canvas: offer what you would plausibly do with this
      // value, and wire it up for you. Blueprint's best habit. A click that
      // never moved is a mis-click, not a request for the palette.
      const dragged = pointer.moved;
      const at = worldPoint(event);
      if (pointer.reconnect) render();
      if (dragged) openPalette(event, at, from);
      return;
    }
    const refusal = refuseLink(from, target);
    if (refusal) {
      // Dropping a picked-up wire on empty space disconnects it, as Blueprint
      // does - but a *refused* target is a mistake, so put the wire back.
      if (pointer.picked) graph.links.push(pointer.picked);
      flash(refusal);
      if (pointer.reconnect) render();
      return;
    }
    const source = from.dir === "out" ? from : target;
    const sink = from.dir === "out" ? target : from;
    mutate(() => {
      graph.links = (graph.links || []).filter((l) => {
        // One source per input pin, one target per execution output.
        if (l.to[0] === sink.node && l.to[1] === sink.name) return false;
        if (source.pin.type === "exec"
            && l.from[0] === source.node && l.from[1] === source.name) return false;
        return true;
      });
      graph.links.push({ from: [source.node, source.name], to: [sink.node, sink.name] });
    });
  }

  function breakLinks(ref) {
    mutate(() => {
      graph.links = (graph.links || []).filter((l) =>
        !(ref.dir === "in" ? l.to[0] === ref.node && l.to[1] === ref.name
                           : l.from[0] === ref.node && l.from[1] === ref.name));
    });
  }

  function highlightCompatible(over) {
    clearHighlights();
    if (!pointer?.from) return;
    for (const element of nodeLayer.querySelectorAll("[data-pin]")) {
      const candidate = parsePin(element.dataset.pin);
      const ok = !refuseLink(pointer.from, candidate);
      element.classList.toggle("ok", ok);
      element.classList.toggle("no", !ok && candidate.node !== pointer.from.node);
    }
    if (over) {
      const element = nodeLayer.querySelector(`[data-pin="${CSS.escape(over.node + "/" + over.dir + "/" + over.name)}"]`);
      element?.classList.add("hot");
    }
  }

  function clearHighlights() {
    for (const element of nodeLayer.querySelectorAll("[data-pin]")) {
      element.classList.remove("ok", "no", "hot");
    }
  }

  function flash(message) {
    const el = container.querySelector(".gv-hint");
    const previous = el.textContent;
    el.textContent = message;
    el.classList.add("warn");
    setTimeout(() => {
      el.textContent = previous;
      el.classList.remove("warn");
    }, 2200);
  }

  /* ------------------------------------------------------- inline editing */

  function inlineInput(host, value, commit, { multiline = false } = {}) {
    const input = document.createElement(multiline ? "textarea" : "input");
    input.className = "gv-edit";
    input.value = value;
    host.replaceWith(input);
    input.focus();
    input.select();
    let done = false;
    const finish = (save) => {
      if (done) return;
      done = true;
      editing = null;
      if (save) commit(input.value);
      else render();
    };
    input.addEventListener("keydown", (event) => {
      event.stopPropagation();
      if (event.key === "Enter" && (!multiline || event.ctrlKey)) finish(true);
      else if (event.key === "Escape") finish(false);
    });
    input.addEventListener("blur", () => finish(true));
    input.addEventListener("mousedown", (event) => event.stopPropagation());
    editing = { commit: () => finish(true) };
  }

  function commitEdit() {
    editing?.commit();
  }

  function renameNode(id) {
    const element = nodeLayer.querySelector(`[data-title="${CSS.escape(id)}"]`);
    const node = nodeById(id);
    if (!element || !node) return;
    inlineInput(element, node.title || definitions[id]?.title || "", (value) => {
      mutate(() => {
        const text = value.trim();
        if (text) node.title = text;
        else delete node.title;
      });
    });
  }

  function editLiteral(element) {
    const [id, pin] = element.dataset.literal.split("/");
    const node = nodeById(id);
    const current = node.values?.[pin];
    inlineInput(element, current === undefined ? "" : JSON.stringify(current), (value) => {
      mutate(() => {
        node.values = node.values || {};
        if (!String(value).trim()) delete node.values[pin];
        else node.values[pin] = parseLiteral(value);
      });
    });
  }

  function editComment(id) {
    const element = commentLayer.querySelector(`[data-comment-text="${CSS.escape(id)}"]`);
    const box = (graph.comments || []).find((c) => c.id === id);
    if (!element || !box) return;
    inlineInput(element, box.text, (value) => {
      mutate(() => { box.text = value; });
    }, { multiline: true });
  }

  viewport.addEventListener("dblclick", (event) => {
    const titleEl2 = event.target.closest("[data-title]");
    if (titleEl2 && editable) return renameNode(titleEl2.dataset.title);
    const commentText = event.target.closest("[data-comment-text]");
    if (commentText && editable) return editComment(commentText.dataset.commentText);
    const nodeEl = event.target.closest("[data-node]");
    if (nodeEl) {
      if (!onOpen) return;
      const node = nodeById(nodeEl.dataset.node);
      if (node) onOpen(node, definitions[node.id]);
      return;
    }
    // Empty canvas: the palette, where Blueprint puts it.
    if (editable && !event.target.closest("[data-comment]")) openPalette(event, worldPoint(event));
  });

  /* ------------------------------------------------------------ structure */

  function addNode(entry, at) {
    const id = nextId();
    const config = clone(entry.meta?.config || {});
    mutate(() => {
      graph.nodes.push({
        id, op: entry.op, pos: [Math.round(at.x / GRID) * GRID, Math.round(at.y / GRID) * GRID],
        ...(Object.keys(config).length ? { config } : {}),
      });
      definitions[id] = clone(entry);
    });
    setSelection([id]);
    return id;
  }

  /** A reflected node: only the server knows what ``py:os.path.join`` looks like. */
  async function addReflected(op, at) {
    if (!describe) return flash("reflection needs a live server");
    let entry;
    try {
      entry = await describe(op, {});
    } catch (err) {
      return flash(err.message || `cannot reflect ${op}`);
    }
    return addNode(entry, at);
  }

  /** Change a node's config (which is what decides its pins) and re-resolve it. */
  async function configure(nodeId, config) {
    const node = nodeById(nodeId);
    if (!node) return;
    let entry = null;
    if (describe) {
      try {
        entry = await describe(node.op, config);
      } catch (err) {
        return flash(err.message || "that configuration does not resolve");
      }
    }
    mutate(() => {
      node.config = config;
      if (entry) definitions[nodeId] = entry;
      // Pins the new shape no longer has cannot stay wired.
      const names = new Set((entry?.pins || []).map((p) => `${p.direction}/${p.name}`));
      if (entry) {
        graph.links = graph.links.filter((l) =>
          (l.from[0] !== nodeId || names.has(`out/${l.from[1]}`))
          && (l.to[0] !== nodeId || names.has(`in/${l.to[1]}`)));
      }
    });
  }

  function deleteSelection() {
    // Entry is the function's signature: the graph needs one, and exactly one.
    // Keeping the last one is the rule - a stray duplicate must be removable,
    // or a mis-drop leaves a graph that cannot compile.
    const entries = (graph.nodes || []).filter((n) => ENTRY_OPS.includes(n.op));
    const doomed = entries.filter((n) => selection.has(n.id));
    if (entries.length && doomed.length === entries.length) {
      selection.delete(doomed[0].id);
      flash("a graph needs its entry node");
    }
    if (!selection.size && !selectedComments.size) return;
    mutate(() => {
      graph.nodes = graph.nodes.filter((n) => !selection.has(n.id));
      graph.links = (graph.links || []).filter(
        (l) => !selection.has(l.from[0]) && !selection.has(l.to[0]));
      graph.comments = (graph.comments || []).filter((c) => !selectedComments.has(c.id));
      selection.clear();
      selectedComments.clear();
    });
  }

  function copySelection() {
    if (!selection.size) return;
    // Copying entry would paste a second starting point.
    const nodes = graph.nodes.filter((n) => selection.has(n.id) && !ENTRY_OPS.includes(n.op));
    if (!nodes.length) return flash("the entry node cannot be copied");
    const copied = new Set(nodes.map((n) => n.id));
    clipboard = {
      nodes: clone(nodes),
      links: clone((graph.links || []).filter(
        (l) => copied.has(l.from[0]) && copied.has(l.to[0]))),
      definitions: Object.fromEntries(nodes.map((n) => [n.id, clone(definitions[n.id])])),
    };
    flash(`${nodes.length} node(s) copied`);
  }

  function pasteClipboard(at) {
    if (!clipboard?.nodes?.length) return;
    const remap = new Map();
    const originX = Math.min(...clipboard.nodes.map((n) => n.pos[0]));
    const originY = Math.min(...clipboard.nodes.map((n) => n.pos[1]));
    mutate(() => {
      for (const node of clipboard.nodes) {
        const id = nextId();
        remap.set(node.id, id);
        const copy = clone(node);
        copy.id = id;
        copy.pos = [at.x + (node.pos[0] - originX), at.y + (node.pos[1] - originY)];
        graph.nodes.push(copy);
        definitions[id] = clone(clipboard.definitions[node.id]);
      }
      for (const link of clipboard.links) {
        graph.links.push({
          from: [remap.get(link.from[0]), link.from[1]],
          to: [remap.get(link.to[0]), link.to[1]],
        });
      }
    });
    setSelection([...remap.values()]);
  }

  function addComment(at, wrapSelection = false) {
    const id = `c${Date.now().toString(36)}`;
    let pos = [Math.round(at.x / GRID) * GRID, Math.round(at.y / GRID) * GRID];
    let size = [360, 200];
    if (wrapSelection && selection.size) {
      const nodes = graph.nodes.filter((n) => selection.has(n.id));
      const xs = nodes.map((n) => n.pos[0]);
      const ys = nodes.map((n) => n.pos[1]);
      pos = [Math.min(...xs) - 24, Math.min(...ys) - 56];
      size = [Math.max(...xs) - Math.min(...xs) + 320, Math.max(...ys) - Math.min(...ys) + 220];
    }
    mutate(() => {
      graph.comments = graph.comments || [];
      graph.comments.push({ id, text: "Comment", pos, size });
    });
    editComment(id);
  }

  /* -------------------------------------------------------------- menus */

  /** Config is what decides the pins of the parametric built-ins. */
  function configurable(node) {
    return Object.keys(node.config || {}).length > 0
      || Object.keys(definitions[node.id]?.meta?.config || {}).length > 0;
  }

  function nodeMenu(event, node) {
    if (!node) return;
    if (!selection.has(node.id)) setSelection([node.id]);
    // The entry node is the module's shape, so its menu is about that.
    if (editable && ENTRY_OPS.includes(node.op) && graph.kind !== "function") {
      return options.onMenu?.(event, entryMenuItems(node));
    }
    const items = [
      ...(editable ? [
        { label: "✏ Rename", hint: "F2", onClick: () => renameNode(node.id) },
        ...(onConfigure && configurable(node)
          ? [{ label: "⚙ Configure…",
               onClick: () => onConfigure(node, definitions[node.id], configure) }]
          : []),
        { label: "⧉ Duplicate", hint: "Ctrl+D",
          onClick: () => { copySelection(); pasteClipboard({ x: node.pos[0] + 40, y: node.pos[1] + 40 }); } },
        { label: "⌫ Break all links",
          onClick: () => mutate(() => {
            graph.links = graph.links.filter(
              (l) => l.from[0] !== node.id && l.to[0] !== node.id);
          }) },
        null,
      ] : []),
      { label: "⧉ Copy node id", onClick: () => navigator.clipboard?.writeText(node.id) },
      ...(editable ? [
        null,
        { label: "🗑 Delete", hint: "Del", danger: true, onClick: deleteSelection },
      ] : []),
    ];
    options.onMenu?.(event, items);
  }

  /** Switching entry kind rewires nothing but the pins it no longer has. */
  function entryMenuItems(node) {
    const size = Number(node.config?.size || 0);
    return [
      { label: "Runs for", disabled: true, onClick: () => {} },
      ...ENTRY_KINDS.map((kind) => ({
        label: `${node.op === kind.op ? "●" : "○"} ${kind.label}`,
        hint: kind.hint,
        onClick: () => {
          if (node.op === kind.op) return;
          mutate(() => {
            node.op = kind.op;
            if (kind.op !== "core:entry_batch") delete node.config?.size;
            // Pins the new shape does not have cannot stay wired.
            graph.links = (graph.links || []).filter(
              (l) => l.from[0] !== node.id || l.from[1] === "then" || l.from[1] === "ctx");
          });
          reresolve(node.id);
        },
      })),
      ...(node.op === "core:entry_batch" ? [
        null,
        { label: `⚙ Entities per batch${size ? ` · ${size}` : " · default"}`,
          onClick: () => onConfigure?.(
            { ...node, config: { size: size || 500 } }, definitions[node.id], configure) },
      ] : []),
    ];
  }

  /** Ask the server for a node's shape again after its op or config changed. */
  async function reresolve(nodeId) {
    const node = nodeById(nodeId);
    if (!node || !describe) return;
    try {
      definitions[nodeId] = await describe(node.op, node.config || {});
      render();
    } catch { /* the save will report it */ }
  }

  function pinMenu(event, ref) {
    options.onMenu?.(event, [
      { label: `${ref.dir === "in" ? "Input" : "Output"} · ${ref.name} : ${ref.pin?.type ?? "?"}`,
        disabled: true, onClick: () => {} },
      null,
      { label: "⌫ Break links", hint: "Alt-click", onClick: () => breakLinks(ref) },
    ]);
  }

  function wireMenu(event, index) {
    options.onMenu?.(event, [
      { label: "⌫ Delete wire", danger: true,
        onClick: () => mutate(() => { graph.links.splice(index, 1); }) },
    ]);
  }

  function commentMenu(event, id) {
    const box = (graph.comments || []).find((c) => c.id === id);
    const carries = box?.moves_nodes !== false;
    options.onMenu?.(event, [
      { label: "✏ Edit text", onClick: () => editComment(id) },
      { label: `${carries ? "☑" : "☐"} Drag the nodes with it`,
        hint: "Alt-drag skips",
        title: carries
          ? "Moving this box moves the nodes sitting on it"
          : "Moving this box leaves the nodes where they are",
        onClick: () => mutate(() => { box.moves_nodes = !carries; }) },
      { label: "Colour",
        swatches: Object.entries(COMMENT_COLORS).map(([key, spec]) => ({
          key, title: spec.label, tint: spec.tint, on: (box?.color || "") === key,
        })),
        onPick: (key) => mutate(() => {
          if (key) box.color = key;
          else delete box.color;
        }),
        onClick: () => {} },
      null,
      { label: "🗑 Delete comment", danger: true,
        onClick: () => mutate(() => {
          graph.comments = graph.comments.filter((c) => c.id !== id);
        }) },
    ]);
  }

  function canvasMenu(event) {
    const at = worldPoint(event);
    options.onMenu?.(event, [
      { label: "＋ Add node…", hint: "double-click", onClick: () => openPalette(event, at) },
      { label: "▭ Add comment", hint: "C", onClick: () => addComment(at) },
      ...(clipboard ? [null, { label: "⎘ Paste", hint: "Ctrl+V",
                               onClick: () => pasteClipboard(at) }] : []),
    ]);
  }

  /* ------------------------------------------------------------- palette */

  /** Can this definition take (or feed) a wire from `from`? */
  function accepts(entry, from) {
    if (!from) return true;
    const want = from.dir === "out" ? "in" : "out";
    return (entry.pins || []).some((pin) => {
      if (pin.direction !== want) return false;
      const exec = from.pin.type === "exec";
      return exec ? pin.type === "exec" : pin.type !== "exec";
    });
  }

  /** The pin a new node should be wired to, given where the drag came from. */
  function landingPin(entry, from) {
    const want = from.dir === "out" ? "in" : "out";
    const candidates = (entry.pins || []).filter((p) =>
      p.direction === want
      && (from.pin.type === "exec" ? p.type === "exec" : p.type !== "exec"));
    // An exact type match beats "Any", which beats anything else.
    return candidates.find((p) => p.type === from.pin.type)
      || candidates.find((p) => p.type === "Any")
      || candidates[0]
      || null;
  }

  /** Add a node from a palette entry and, if a wire was being dragged, wire it.
   *  `entry` is either a resolved definition or `{op, config}` to resolve. */
  async function place(entry, at, from = null) {
    let id;
    if (entry.reflect) id = await addReflected(entry.op, at);
    else if (!entry.pins) id = await addResolved(entry.op, entry.config || {}, at);
    else id = addNode(entry, at);
    if (!id || !from) return id;
    // Wire the new node to the pin the drag came from - which is the point of
    // dragging off a pin rather than opening the palette.
    const landing = landingPin(definitions[id] || entry, from);
    if (!landing) {
      flash(`${entry.title || entry.op} has no pin for ${from.name}`);
      return id;
    }
    const target = { node: id, dir: from.dir === "out" ? "in" : "out",
                     name: landing.name, pin: landing };
    pointer = { from, mode: "wire" };
    finishWire(target);
    pointer = null;
    return id;
  }

  /** Place a node the server has to describe first (a reflected callable, a
   *  method picked out of a type). */
  async function addResolved(op, config, at) {
    if (!describe) return flash("that needs a live server");
    let definition;
    try {
      definition = await describe(op, config);
    } catch (err) {
      return flash(err.message || `cannot resolve ${op}`);
    }
    return addNode({ ...definition, meta: { ...(definition.meta || {}), config } }, at);
  }

  /** `from` (optional) is the pin a wire was dragged off. */
  function openPalette(event, at, from = null) {
    const host = document.createElement("div");
    host.className = "gv-palette";
    host.innerHTML = `
      ${from ? `<div class="gv-pal-from">
          from <span class="mono">${esc(from.name)}</span>
          <span class="dim mono">: ${esc(from.pin?.type ?? "?")}</span>
        </div>` : ""}
      <div class="gv-pal-head">
        <input type="search" class="gv-pal-search" placeholder="${
          from ? "Filter…" : "Search nodes…"}" />
        ${options.browse ? `<button class="gv-pal-import"
            title="Browse a Python module and place what you find">⤓ Import…</button>` : ""}
      </div>
      <div class="gv-pal-list"></div>`;
    document.body.appendChild(host);
    const rect = { x: event.clientX, y: event.clientY };
    host.style.left = `${Math.min(rect.x, window.innerWidth - 360)}px`;
    host.style.top = `${Math.min(rect.y, window.innerHeight - 420)}px`;

    const search = host.querySelector(".gv-pal-search");
    const list = host.querySelector(".gv-pal-list");
    let matches = [];
    let active = 0;
    //: Type-aware offers from the server; they arrive after the first paint.
    let suggested = [];
    if (from && options.suggest) {
      Promise.resolve(options.suggest(from.pin?.type || "Any", from.dir))
        .then((entries) => { suggested = entries || []; paint(); })
        .catch(() => {});
    }

    const paint = () => {
      const typed = search.value.trim();
      const needle = typed.toLowerCase();
      const hit = (entry) => !needle
        || `${entry.title} ${entry.op} ${entry.category} ${entry.description}`
            .toLowerCase().includes(needle);
      // With a source pin, only what can actually take the wire is offered,
      // and the type-aware suggestions come first.
      matches = [
        ...suggested.filter(hit),
        ...catalog.filter((entry) => hit(entry) && accepts(entry, from)),
      ].slice(0, 80);
      // Reflection has no list to search: a dotted name is the whole offer.
      const dotted = typed.replace(/^py:/, "");
      if (describe && REFLECTABLE.test(dotted)) {
        matches.unshift({
          op: `py:${dotted}`, title: dotted.split(".").pop(), category: "Reflection",
          description: `Reflect ${dotted}`, reflect: true,
        });
      }
      active = 0;
      let lastCategory = null;
      list.innerHTML = matches.map((entry, index) => {
        const category = entry.suggested ? "Suggested" : entry.category;
        const heading = category !== lastCategory
          ? `<div class="gv-pal-cat">${esc(category)}</div>` : "";
        lastCategory = category;
        return `${heading}<button class="gv-pal-item${index === 0 ? " on" : ""}"
            data-index="${index}" title="${esc(entry.description || "")}">
            <span class="gv-pal-name">${esc(entry.title)}</span>
            ${entry.pure ? '<span class="gv-tag">pure</span>' : ""}
            <span class="gv-pal-op mono">${esc(entry.op)}</span>
          </button>`;
      }).join("") || `<div class="gv-pal-empty dim">Nothing matches.</div>`;
      for (const button of list.querySelectorAll("[data-index]")) {
        button.addEventListener("mousedown", (e) => {
          e.preventDefault();
          choose(Number(button.dataset.index));
        });
      }
    };

    const choose = async (index) => {
      const entry = matches[index];
      close();
      if (!entry) return;
      // "Import a module…" is not a node: it opens the browser, which calls
      // back with whatever you pick there.
      await place(entry, at, from);
    };

    const close = () => {
      host.remove();
      window.removeEventListener("mousedown", onOutside, true);
    };
    const onOutside = (e) => {
      if (!host.contains(e.target)) close();
    };

    host.querySelector(".gv-pal-import")?.addEventListener("mousedown", (e) => {
      e.preventDefault();
      const typed = search.value.trim();
      close();
      options.browse(typed, (picked) => place(picked, at, from));
    });
    search.addEventListener("input", paint);
    search.addEventListener("keydown", (e) => {
      e.stopPropagation();
      if (e.key === "Escape") return close();
      if (e.key === "Enter") return choose(active);
      if (e.key === "ArrowDown" || e.key === "ArrowUp") {
        e.preventDefault();
        active = Math.max(0, Math.min(matches.length - 1, active + (e.key === "ArrowDown" ? 1 : -1)));
        list.querySelectorAll("[data-index]").forEach((b) =>
          b.classList.toggle("on", Number(b.dataset.index) === active));
        list.querySelector(".gv-pal-item.on")?.scrollIntoView({ block: "nearest" });
      }
    });
    paint();
    search.focus();
    setTimeout(() => window.addEventListener("mousedown", onOutside, true), 0);
  }

  /* ------------------------------------------------------------ keyboard */

  viewport.addEventListener("keydown", (event) => {
    if (editing) return;
    const meta = event.ctrlKey || event.metaKey;
    if (meta && event.key.toLowerCase() === "z") {
      event.preventDefault();
      return event.shiftKey ? redo() : undo();
    }
    if (meta && event.key.toLowerCase() === "y") {
      event.preventDefault();
      return redo();
    }
    if (meta && event.key.toLowerCase() === "s") {
      event.preventDefault();
      return save();
    }
    if (meta && event.key.toLowerCase() === "c") return copySelection();
    if (meta && event.key.toLowerCase() === "v") {
      return pasteClipboard(lastCursor || { x: 80, y: 80 });
    }
    if (meta && event.key.toLowerCase() === "d") {
      event.preventDefault();
      copySelection();
      return pasteClipboard({ x: (lastCursor?.x || 80) + 40, y: (lastCursor?.y || 80) + 40 });
    }
    if (meta && event.key.toLowerCase() === "a") {
      event.preventDefault();
      return setSelection((graph.nodes || []).map((n) => n.id));
    }
    if (!editable) return;
    if (event.key === "Delete" || event.key === "Backspace") {
      event.preventDefault();
      return deleteSelection();
    }
    if (event.key === "F2" && selection.size === 1) {
      event.preventDefault();
      return renameNode([...selection][0]);
    }
    if (event.key.toLowerCase() === "c" && !meta) {
      // Without this the "c" lands in the comment's own text box.
      event.preventDefault();
      return addComment(lastCursor || { x: 80, y: 80 }, true);
    }
  });

  let lastCursor = null;
  viewport.addEventListener("mousemove", (event) => {
    lastCursor = worldPoint(event);
  });

  viewport.addEventListener("wheel", (event) => {
    event.preventDefault();
    const rect = viewport.getBoundingClientRect();
    const px = event.clientX - rect.left;
    const py = event.clientY - rect.top;
    const next = Math.max(0.2, Math.min(2.5, view.zoom * (event.deltaY < 0 ? 1.1 : 1 / 1.1)));
    view.x = px - (px - view.x) * (next / view.zoom);
    view.y = py - (py - view.y) * (next / view.zoom);
    view.zoom = next;
    applyView();
    drawWires();
  }, { passive: false });

  /* ---------------------------------------------------------------- view */

  function fit() {
    const nodes = graph.nodes || [];
    if (!nodes.length) return;
    const xs = nodes.map((n) => n.pos?.[0] || 0);
    const ys = nodes.map((n) => n.pos?.[1] || 0);
    const minX = Math.min(...xs) - 60;
    const minY = Math.min(...ys) - 60;
    const maxX = Math.max(...xs) + 360;
    const maxY = Math.max(...ys) + 260;
    const rect = viewport.getBoundingClientRect();
    view.zoom = Math.max(0.25, Math.min(1.2,
      Math.min(rect.width / (maxX - minX), rect.height / (maxY - minY))));
    view.x = -minX * view.zoom + 20;
    view.y = -minY * view.zoom + 20;
    applyView();
    drawWires();
  }

  function save() {
    if (!dirty || !onSave) return;
    onSave(graph);
  }

  container.querySelector(".gv-fit").addEventListener("click", fit);
  container.querySelector(".gv-reset").addEventListener("click", () => {
    view.zoom = 1;
    view.x = 40;
    view.y = 40;
    applyView();
    drawWires();
  });
  saveBtn?.addEventListener("click", save);
  undoBtn?.addEventListener("click", undo);
  redoBtn?.addEventListener("click", redo);
  container.querySelector(".gv-add")?.addEventListener("click", (event) =>
    openPalette(event, lastCursor || { x: 120, y: 120 }));

  const runBtn = container.querySelector(".gv-run");
  runBtn?.addEventListener("click", () => onRun({}));
  runBtn?.addEventListener("contextmenu", (event) => {
    event.preventDefault();
    event.stopPropagation();
    onRun({ event, options: true });
  });

  render();
  requestAnimationFrame(fit);

  return {
    get graph() { return graph; },
    get dirty() { return dirty; },
    /** Show a different document. ``dirty`` carries an unsaved flag across a
     *  switch between the functions of one library - the file is what is
     *  saved, so its dirtiness does not belong to whichever tab is open. */
    load(next, { dirty: keep = false, fit: refit = true } = {}) {
      graph = normalise(next.graph || next);
      definitions = next.definitions || {};
      problems = next.problems || [];
      selection.clear();
      selectedComments.clear();
      undoStack.length = 0;
      redoStack.length = 0;
      markDirty(!!keep);
      render();
      if (refit) requestAnimationFrame(fit);
    },
    saved(next) {
      // The server may have normalised the document; take its version.
      if (next) {
        graph = normalise(next.graph || graph);
        definitions = next.definitions || definitions;
        problems = next.problems || [];
      }
      markDirty(false);
      render();
    },
    setProblems(list) {
      problems = list || [];
      render();
    },
    describePin: pinSpec,
    configure,
    /** Something outside the canvas changed the document - a library's own
     *  settings, say. Light the Save button without touching the history. */
    touch() { markDirty(true); },
    fit,
    undo,
    redo,
    save,
    refresh() { measurePins(); drawWires(); },
    dispose() { container.innerHTML = ""; },
  };
}
