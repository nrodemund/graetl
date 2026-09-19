/* The file tree ------------------------------------------------------------
 * A real tree, not a list of paths: collapsible folders, icons that say what a
 * file *is* to GraETL, inline rename, drag to move, and drop from the desktop.
 *
 * It owns presentation and gesture only. Every action that touches disk is
 * handed back through a callback, so the rules about generated files and module
 * state live in one place (the server) rather than being re-implemented here.
 * ------------------------------------------------------------------------- */

const GLYPH = {
  dir: "",
  module: "M",
  graph: "◆",
  graphlib: "ƒ",
  graph_function: "ƒ",
  nodes: "λ",
  python: "py",
  config: "⚙",
  data: "▤",
  database: "▦",
  file: "·",
};

/** Folders that hold runtime output; the server hides most, this is a belt. */
const HIDDEN = new Set(["__pycache__", "logs", "profiles"]);

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

function fmtSize(bytes) {
  if (bytes === null || bytes === undefined) return "";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} K`;
  return `${(bytes / 1024 / 1024).toFixed(1)} M`;
}

const dirname = (path) => (path.includes("/") ? path.slice(0, path.lastIndexOf("/")) : "");
const basename = (path) => path.slice(path.lastIndexOf("/") + 1);
const join = (dir, name) => (dir ? `${dir}/${name}` : name);

/** Everything before the last dot, so renaming selects "load" in "load.module.py". */
function stemOf(name) {
  for (const suffix of [".module.py", ".graphlib.py", ".module.toml"]) {
    if (name.endsWith(suffix)) return name.slice(0, -suffix.length);
  }
  const dot = name.indexOf(".");
  return dot > 0 ? name.slice(0, dot) : name;
}

/* Flat server listing -> nested tree. The server already emits folders. */
function buildTree(files) {
  const root = { path: "", name: "", type: "dir", children: [], depth: -1 };
  const byPath = new Map([["", root]]);

  const ensureDir = (path) => {
    if (byPath.has(path)) return byPath.get(path);
    const node = {
      path,
      name: basename(path),
      type: "dir",
      kind: "dir",
      children: [],
      depth: path.split("/").length - 1,
    };
    byPath.set(path, node);
    ensureDir(dirname(path)).children.push(node);
    return node;
  };

  for (const file of files) {
    if (file.path.split("/").some((part) => HIDDEN.has(part) || part.startsWith("."))) continue;
    if (file.type === "dir") {
      ensureDir(file.path);
      continue;
    }
    const node = { ...file, children: null, depth: file.path.split("/").length - 1 };
    byPath.set(file.path, node);
    ensureDir(dirname(file.path)).children.push(node);
  }

  const sort = (node) => {
    if (!node.children) return;
    node.children.sort((a, b) => {
      if ((a.type === "dir") !== (b.type === "dir")) return a.type === "dir" ? -1 : 1;
      return a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: "base" });
    });
    node.children.forEach(sort);
  };
  sort(root);
  return { root, byPath };
}

export function createFileTree(container, options = {}) {
  const {
    onOpen, onSelect, onMove, onDropFiles, onMenu, onRename, onDelete, onNew,
  } = options;

  let files = options.files || [];
  let current = options.current || null;
  let tree = buildTree(files);
  let selected = current;
  let renaming = null;
  const expanded = options.expanded instanceof Set ? options.expanded : new Set();
  let visible = [];              // flattened rows, for keyboard navigation

  container.classList.add("ft");
  container.tabIndex = 0;

  /* --------------------------------------------------------------- render */

  function expandAncestors(path) {
    let dir = dirname(path);
    while (dir) {
      expanded.add(dir);
      dir = dirname(dir);
    }
  }

  function flatten(node, out) {
    for (const child of node.children || []) {
      out.push(child);
      if (child.type === "dir" && expanded.has(child.path)) flatten(child, out);
    }
    return out;
  }

  function row(node) {
    const open = node.type === "dir" && expanded.has(node.path);
    const glyph = GLYPH[node.kind] ?? GLYPH.file;
    const generated = node.generated
      ? `<span class="ft-tag" title="generated from ${esc(node.generated)}">gen</span>`
      : "";
    const chevron =
      node.type === "dir"
        ? `<span class="ft-chev">${open ? "▾" : "▸"}</span>`
        : `<span class="ft-chev"></span>`;
    return `<div class="ft-row${node.path === selected ? " sel" : ""}${
      node.path === current ? " open" : ""}${node.generated ? " gen" : ""}"
        data-path="${esc(node.path)}" data-type="${node.type}"
        draggable="true" title="${esc(node.path)}"
        style="padding-left:${6 + node.depth * 14}px">
        ${chevron}
        <span class="ft-ico ft-k-${esc(node.kind || "file")}">${esc(glyph)}</span>
        <span class="ft-name">${esc(node.name)}</span>
        ${generated}
        <span class="ft-size dim">${node.type === "dir" ? "" : fmtSize(node.size)}</span>
      </div>`;
  }

  function render() {
    visible = flatten(tree.root, []);
    container.innerHTML = visible.length
      ? visible.map(row).join("")
      : `<div class="ft-empty dim">Nothing here yet — right-click to add a file.</div>`;
    wire();
  }

  /* ------------------------------------------------------------- gestures */

  function nodeAt(element) {
    const rowEl = element?.closest?.("[data-path]");
    return rowEl ? tree.byPath.get(rowEl.dataset.path) : null;
  }

  /** Where a drop lands: the folder itself, or the folder holding the file. */
  function dropDirFor(node) {
    if (!node) return "";
    return node.type === "dir" ? node.path : dirname(node.path);
  }

  function select(path, { open = false } = {}) {
    selected = path;
    for (const element of container.querySelectorAll(".ft-row")) {
      element.classList.toggle("sel", element.dataset.path === path);
    }
    if (onSelect) onSelect(path);
    if (open) {
      const node = tree.byPath.get(path);
      if (node && node.type !== "dir" && onOpen) onOpen(path);
    }
  }

  function toggle(path) {
    if (expanded.has(path)) expanded.delete(path);
    else expanded.add(path);
    render();
  }

  function wire() {
    for (const element of container.querySelectorAll(".ft-row")) {
      const path = element.dataset.path;
      const node = tree.byPath.get(path);

      element.addEventListener("click", () => {
        if (node.type === "dir") {
          select(path);
          toggle(path);
        } else {
          select(path, { open: true });
        }
      });

      element.addEventListener("contextmenu", (event) => {
        select(path);
        if (onMenu) onMenu(event, node);
      });

      element.addEventListener("dblclick", () => {
        if (node.type !== "dir") beginRename(path);
      });

      /* ------------------------------------------------- drag within tree */
      element.addEventListener("dragstart", (event) => {
        event.dataTransfer.setData("application/x-graetl-path", path);
        event.dataTransfer.setData("text/plain", path);
        event.dataTransfer.effectAllowed = "move";
        element.classList.add("dragging");
      });
      element.addEventListener("dragend", () => {
        element.classList.remove("dragging");
        clearDropTargets();
      });

      element.addEventListener("dragover", (event) => {
        const incoming = event.dataTransfer.types.includes("Files");
        const internal = event.dataTransfer.types.includes("application/x-graetl-path");
        if (!incoming && !internal) return;
        event.preventDefault();
        event.dataTransfer.dropEffect = incoming ? "copy" : "move";
        clearDropTargets();
        const dir = dropDirFor(node);
        const target = dir
          ? container.querySelector(`[data-path="${CSS.escape(dir)}"]`)
          : null;
        (target || container).classList.add("drop");
        if (node.type === "dir" && !expanded.has(path)) {
          // Hovering a closed folder opens it, as a file manager would.
          clearTimeout(element._hover);
          element._hover = setTimeout(() => {
            expanded.add(path);
            render();
          }, 600);
        }
      });
      element.addEventListener("dragleave", () => clearTimeout(element._hover));

      element.addEventListener("drop", (event) => {
        event.preventDefault();
        event.stopPropagation();
        clearTimeout(element._hover);
        handleDrop(event, dropDirFor(node));
      });
    }
  }

  function clearDropTargets() {
    container.classList.remove("drop");
    for (const element of container.querySelectorAll(".drop")) element.classList.remove("drop");
  }

  function handleDrop(event, dir) {
    clearDropTargets();
    const source = event.dataTransfer.getData("application/x-graetl-path");
    if (source) {
      const name = basename(source);
      const target = join(dir, name);
      if (target === source) return;
      if (dir === source || dir.startsWith(`${source}/`)) return;  // into itself
      if (onMove) onMove(source, target);
      return;
    }
    if (event.dataTransfer.files?.length || event.dataTransfer.items?.length) {
      collectDropped(event.dataTransfer).then((entries) => {
        if (entries.length && onDropFiles) onDropFiles(entries, dir);
      });
    }
  }

  /* The desktop can drop whole folders; walk them so a folder of CSVs lands
     with its structure intact rather than as a flat pile. */
  async function collectDropped(transfer) {
    const out = [];
    const items = [...(transfer.items || [])];
    const entries = items
      .map((item) => (item.webkitGetAsEntry ? item.webkitGetAsEntry() : null))
      .filter(Boolean);

    if (!entries.length) {
      for (const file of transfer.files || []) out.push({ file, relative: file.name });
      return out;
    }

    const walk = (entry, prefix) =>
      new Promise((resolve) => {
        if (entry.isFile) {
          entry.file((file) => {
            out.push({ file, relative: prefix ? `${prefix}/${entry.name}` : entry.name });
            resolve();
          }, resolve);
          return;
        }
        const reader = entry.createReader();
        const children = [];
        const readBatch = () =>
          reader.readEntries(async (batch) => {
            if (!batch.length) {
              await Promise.all(
                children.map((child) =>
                  walk(child, prefix ? `${prefix}/${entry.name}` : entry.name),
                ),
              );
              resolve();
              return;
            }
            children.push(...batch);
            readBatch();
          }, resolve);
        readBatch();
      });

    await Promise.all(entries.map((entry) => walk(entry, "")));
    return out;
  }

  /* Dropping on empty space below the rows means "the pipeline root". */
  container.addEventListener("dragover", (event) => {
    if (event.target !== container) return;
    if (!event.dataTransfer.types.includes("Files")
        && !event.dataTransfer.types.includes("application/x-graetl-path")) return;
    event.preventDefault();
    clearDropTargets();
    container.classList.add("drop");
  });
  container.addEventListener("dragleave", (event) => {
    if (event.target === container) clearDropTargets();
  });
  container.addEventListener("drop", (event) => {
    if (event.target !== container) return;
    event.preventDefault();
    handleDrop(event, "");
  });
  container.addEventListener("contextmenu", (event) => {
    if (event.target !== container) return;
    if (onMenu) onMenu(event, tree.root);
  });

  /* ------------------------------------------------------- inline rename */

  function beginRename(path) {
    const node = tree.byPath.get(path);
    if (!node || node.generated) return;
    const element = container.querySelector(`[data-path="${CSS.escape(path)}"]`);
    const nameEl = element?.querySelector(".ft-name");
    if (!nameEl) return;
    renaming = path;

    const input = document.createElement("input");
    input.className = "ft-edit";
    input.value = node.name;
    nameEl.replaceWith(input);
    input.focus();
    const stem = stemOf(node.name);
    input.setSelectionRange(0, stem.length);

    let done = false;
    const finish = (commit) => {
      if (done) return;
      done = true;
      renaming = null;
      const value = input.value.trim();
      if (commit && value && value !== node.name && onRename) onRename(path, value);
      else render();
      container.focus();
    };
    input.addEventListener("keydown", (event) => {
      event.stopPropagation();
      if (event.key === "Enter") finish(true);
      else if (event.key === "Escape") finish(false);
    });
    input.addEventListener("blur", () => finish(true));
  }

  /* ---------------------------------------------------------- keyboard */

  container.addEventListener("keydown", (event) => {
    if (renaming) return;
    const index = visible.findIndex((n) => n.path === selected);
    const node = index >= 0 ? visible[index] : null;

    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const next = visible[Math.max(0, Math.min(visible.length - 1,
        index + (event.key === "ArrowDown" ? 1 : -1)))];
      if (next) {
        select(next.path);
        container.querySelector(`[data-path="${CSS.escape(next.path)}"]`)
          ?.scrollIntoView({ block: "nearest" });
      }
      return;
    }
    if (event.key === "ArrowRight" && node?.type === "dir" && !expanded.has(node.path)) {
      event.preventDefault();
      toggle(node.path);
      return;
    }
    if (event.key === "ArrowLeft" && node) {
      event.preventDefault();
      if (node.type === "dir" && expanded.has(node.path)) toggle(node.path);
      else if (dirname(node.path)) select(dirname(node.path));
      return;
    }
    if (event.key === "Enter" && node) {
      event.preventDefault();
      if (node.type === "dir") toggle(node.path);
      else if (onOpen) onOpen(node.path);
      return;
    }
    if (event.key === "F2" && node) {
      event.preventDefault();
      beginRename(node.path);
      return;
    }
    if ((event.key === "Delete" || event.key === "Backspace") && node) {
      event.preventDefault();
      if (onDelete) onDelete(node);
    }
  });

  /* -------------------------------------------------------------- public */

  if (current) expandAncestors(current);
  render();

  return {
    expanded,
    get selected() {
      return selected;
    },
    setFiles(next, nextCurrent) {
      files = next;
      tree = buildTree(files);
      if (nextCurrent !== undefined) current = nextCurrent;
      if (current) expandAncestors(current);
      if (!tree.byPath.has(selected)) selected = current;
      render();
    },
    setCurrent(path) {
      current = path;
      selected = path;
      if (path) expandAncestors(path);
      render();
    },
    rename: beginRename,
    newIn(kind) {
      const node = tree.byPath.get(selected);
      if (onNew) onNew(kind, dropDirFor(node));
    },
    focus() {
      container.focus();
    },
  };
}

export { basename, dirname, join };
