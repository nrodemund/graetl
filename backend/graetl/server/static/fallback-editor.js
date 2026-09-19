/* Offline fallback editor ---------------------------------------------------
 * Used when Monaco cannot be loaded (no vendored copy and no network). No
 * dependencies: a transparent <textarea> over a highlighted <pre>, a synced
 * gutter, and the editing behaviour you expect from an IDE (indent,
 * auto-indent, block comment, find & replace, go to line). It implements the
 * same handle contract as the Monaco editor in editor.js.
 * ------------------------------------------------------------------------- */

const PY_KEYWORDS = new Set([
  "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class",
  "continue", "def", "del", "elif", "else", "except", "finally", "for", "from", "global",
  "if", "import", "in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return",
  "try", "while", "with", "yield", "match", "case",
]);

const PY_BUILTINS = new Set([
  "abs", "all", "any", "bool", "bytes", "dict", "enumerate", "filter", "float", "format",
  "frozenset", "getattr", "hasattr", "int", "isinstance", "len", "list", "map", "max", "min",
  "next", "object", "open", "print", "range", "repr", "reversed", "round", "set", "setattr",
  "sorted", "str", "sum", "super", "tuple", "type", "zip", "self", "cls", "ctx", "entity",
  "pipeline", "Exception", "ValueError", "TypeError", "KeyError", "RuntimeError",
]);

function escapeHtml(text) {
  return text.replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
}

/* Tokenizer: one pass, longest-match-first. Good enough for pipeline code and
   fast enough to re-run on every keystroke for files of a few thousand lines. */
const PY_RULES = [
  [/^("""|''')[\s\S]*?(\1|$)/, "str"],          // triple quoted / docstring
  [/^#[^\n]*/, "com"],
  [/^([rbfu]{0,2})("(?:\\.|[^"\\\n])*"|'(?:\\.|[^'\\\n])*')/, "str"],
  [/^@[A-Za-z_][\w.]*/, "dec"],
  [/^\b\d[\d_]*(\.\d+)?([eE][+-]?\d+)?\b/, "num"],
  [/^\b[A-Za-z_]\w*\b/, "word"],
  [/^[+\-*/%=<>!&|^~]+/, "op"],
  [/^\s+/, null],
  [/^[\s\S]/, null],
];

function highlightPython(source) {
  let out = "";
  let rest = source;
  let previousWord = "";
  while (rest) {
    let matched = false;
    for (const [rule, kind] of PY_RULES) {
      const hit = rule.exec(rest);
      if (!hit) continue;
      const text = hit[0];
      let cls = kind;
      if (kind === "word") {
        if (PY_KEYWORDS.has(text)) cls = "kw";
        else if (previousWord === "def" || previousWord === "class") cls = "fn";
        else if (PY_BUILTINS.has(text)) cls = "bi";
        else cls = null;
        previousWord = text;
      } else if (kind && kind !== "com") {
        previousWord = "";
      }
      out += cls ? `<span class="t-${cls}">${escapeHtml(text)}</span>` : escapeHtml(text);
      rest = rest.slice(text.length);
      matched = true;
      break;
    }
    if (!matched) {  // safety net, never hit in practice
      out += escapeHtml(rest[0]);
      rest = rest.slice(1);
    }
  }
  return out;
}

function highlightToml(source) {
  return source
    .split("\n")
    .map((line) => {
      if (/^\s*#/.test(line)) return `<span class="t-com">${escapeHtml(line)}</span>`;
      if (/^\s*\[/.test(line)) return `<span class="t-dec">${escapeHtml(line)}</span>`;
      const m = /^(\s*[\w.-]+\s*)(=)(.*)$/.exec(line);
      if (m) {
        return (
          `<span class="t-fn">${escapeHtml(m[1])}</span><span class="t-op">=</span>` +
          `<span class="t-str">${escapeHtml(m[3])}</span>`
        );
      }
      return escapeHtml(line);
    })
    .join("\n");
}

export function createFallbackEditor(container, options = {}) {
  const { value = "", language = "python", onSave, onChange } = options;

  container.innerHTML = `
    <div class="ed">
      <div class="ed-find" hidden>
        <input type="text" class="ed-find-input" placeholder="Find" />
        <input type="text" class="ed-replace-input" placeholder="Replace with" />
        <span class="ed-find-count dim"></span>
        <button class="btn sm ed-find-prev" title="Previous (Shift+Enter)">↑</button>
        <button class="btn sm ed-find-next" title="Next (Enter)">↓</button>
        <button class="btn sm ed-replace-one">Replace</button>
        <button class="btn sm ed-replace-all">All</button>
        <button class="btn sm ed-find-close" title="Close (Esc)">✕</button>
      </div>
      <div class="ed-body">
        <div class="ed-gutter"></div>
        <div class="ed-surface">
          <pre class="ed-highlight" aria-hidden="true"><code></code></pre>
          <textarea class="ed-input" spellcheck="false" autocapitalize="off"
                    autocorrect="off" autocomplete="off" wrap="off"></textarea>
        </div>
      </div>
      <div class="ed-status">
        <span class="ed-pos">1:1</span>
        <span class="ed-dirty"></span>
        <div class="spacer"></div>
        <span class="ed-problem"></span>
        <span class="ed-hint dim">Ctrl+S save · Ctrl+F find · Tab indent · Ctrl+/ comment</span>
      </div>
    </div>`;

  const input = container.querySelector(".ed-input");
  const code = container.querySelector(".ed-highlight code");
  const pre = container.querySelector(".ed-highlight");
  const gutter = container.querySelector(".ed-gutter");
  const posEl = container.querySelector(".ed-pos");
  const dirtyEl = container.querySelector(".ed-dirty");
  const problemEl = container.querySelector(".ed-problem");
  const findBar = container.querySelector(".ed-find");
  const findInput = container.querySelector(".ed-find-input");
  const replaceInput = container.querySelector(".ed-replace-input");
  const findCount = container.querySelector(".ed-find-count");

  let clean = value;
  let errorLine = null;
  let currentPath = options.path || null;

  const pickHighlighter = (lang) =>
    lang === "toml" ? highlightToml : lang === "python" ? highlightPython : escapeHtml;
  let highlighter = pickHighlighter(language);

  function paint() {
    const text = input.value;
    code.innerHTML = highlighter(text) + "\n";
    const lines = text.split("\n").length;
    let html = "";
    for (let i = 1; i <= lines; i++) {
      html += `<div class="ed-ln${i === errorLine ? " err" : ""}">${i}</div>`;
    }
    gutter.innerHTML = html;
    dirtyEl.textContent = input.value === clean ? "" : "● unsaved";
    syncScroll();
  }

  function syncScroll() {
    pre.scrollTop = input.scrollTop;
    pre.scrollLeft = input.scrollLeft;
    gutter.scrollTop = input.scrollTop;
  }

  function updatePosition() {
    const upto = input.value.slice(0, input.selectionStart);
    const line = upto.split("\n").length;
    const col = upto.length - upto.lastIndexOf("\n");
    posEl.textContent = `${line}:${col}`;
  }

  function setSelection(start, end) {
    input.focus();
    input.setSelectionRange(start, end);
    // keep the caret in view
    const before = input.value.slice(0, start).split("\n").length - 1;
    input.scrollTop = Math.max(0, before * lineHeight() - input.clientHeight / 2);
    syncScroll();
    updatePosition();
  }

  function lineHeight() {
    return parseFloat(getComputedStyle(input).lineHeight) || 20;
  }

  function lineBounds(position) {
    const start = input.value.lastIndexOf("\n", position - 1) + 1;
    let end = input.value.indexOf("\n", position);
    if (end === -1) end = input.value.length;
    return [start, end];
  }

  function replaceRange(start, end, text, caret) {
    input.setRangeText(text, start, end, "end");
    if (caret !== undefined) input.setSelectionRange(caret, caret);
    changed();
  }

  function changed() {
    paint();
    updatePosition();
    if (onChange) onChange(input.value);
  }

  /* ------------------------------------------------------------ key handling */

  function indentSelection(dedent) {
    const [start] = lineBounds(input.selectionStart);
    const [, end] = lineBounds(input.selectionEnd);
    const block = input.value.slice(start, end);
    const updated = block
      .split("\n")
      .map((line) =>
        dedent ? line.replace(/^ {1,4}|^\t/, "") : line.length || block.split("\n").length === 1 ? "    " + line : line,
      )
      .join("\n");
    input.setRangeText(updated, start, end, "select");
    changed();
  }

  function toggleComment() {
    const [start] = lineBounds(input.selectionStart);
    const [, end] = lineBounds(input.selectionEnd);
    const lines = input.value.slice(start, end).split("\n");
    const token = language === "python" || language === "toml" ? "#" : "//";
    const allCommented = lines.every((l) => !l.trim() || l.trimStart().startsWith(token));
    const updated = lines
      .map((line) => {
        if (!line.trim()) return line;
        if (allCommented) return line.replace(new RegExp(`^(\\s*)${token} ?`), "$1");
        const indent = line.match(/^\s*/)[0];
        return `${indent}${token} ${line.slice(indent.length)}`;
      })
      .join("\n");
    input.setRangeText(updated, start, end, "select");
    changed();
  }

  input.addEventListener("keydown", (event) => {
    const { selectionStart: s, selectionEnd: e, value: text } = input;

    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "s") {
      event.preventDefault();
      if (onSave) onSave(input.value);
      return;
    }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "f") {
      event.preventDefault();
      openFind();
      return;
    }
    if ((event.ctrlKey || event.metaKey) && event.key === "/") {
      event.preventDefault();
      toggleComment();
      return;
    }
    if (event.key === "Tab") {
      event.preventDefault();
      if (s !== e || event.shiftKey) indentSelection(event.shiftKey);
      else replaceRange(s, e, "    ");
      return;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      const [lineStart] = lineBounds(s);
      const current = text.slice(lineStart, s);
      let indent = (current.match(/^\s*/) || [""])[0];
      if (/:\s*$/.test(current.trimEnd())) indent += "    ";
      if (/^\s*(return|pass|raise|break|continue)\b/.test(current)) {
        indent = indent.slice(0, Math.max(0, indent.length - 4));
      }
      replaceRange(s, e, "\n" + indent);
      return;
    }
    if (event.key === "Backspace" && s === e) {
      const [lineStart] = lineBounds(s);
      const before = text.slice(lineStart, s);
      if (before.length && /^ +$/.test(before) && before.length % 4 === 0) {
        event.preventDefault();
        replaceRange(s - 4, s, "");
      }
    }
  });

  input.addEventListener("input", changed);
  input.addEventListener("scroll", syncScroll);
  ["click", "keyup", "select"].forEach((evt) =>
    input.addEventListener(evt, updatePosition),
  );

  /* ---------------------------------------------------------------- find bar */

  let matches = [];
  let matchIndex = 0;

  function refreshMatches() {
    const needle = findInput.value;
    matches = [];
    if (needle) {
      let from = 0;
      const haystack = input.value.toLowerCase();
      const lower = needle.toLowerCase();
      for (;;) {
        const at = haystack.indexOf(lower, from);
        if (at === -1) break;
        matches.push(at);
        from = at + Math.max(lower.length, 1);
      }
    }
    findCount.textContent = matches.length ? `${matchIndex + 1}/${matches.length}` : "no matches";
  }

  function gotoMatch(step) {
    if (!matches.length) return;
    matchIndex = (matchIndex + step + matches.length) % matches.length;
    const at = matches[matchIndex];
    setSelection(at, at + findInput.value.length);
    findCount.textContent = `${matchIndex + 1}/${matches.length}`;
  }

  function openFind() {
    findBar.hidden = false;
    const selected = input.value.slice(input.selectionStart, input.selectionEnd);
    if (selected && !selected.includes("\n")) findInput.value = selected;
    findInput.focus();
    findInput.select();
    matchIndex = -1;
    refreshMatches();
    gotoMatch(1);
  }

  findInput.addEventListener("input", () => {
    matchIndex = -1;
    refreshMatches();
    gotoMatch(1);
  });
  findInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      gotoMatch(event.shiftKey ? -1 : 1);
    } else if (event.key === "Escape") {
      findBar.hidden = true;
      input.focus();
    }
  });
  container.querySelector(".ed-find-next").addEventListener("click", () => gotoMatch(1));
  container.querySelector(".ed-find-prev").addEventListener("click", () => gotoMatch(-1));
  container.querySelector(".ed-find-close").addEventListener("click", () => {
    findBar.hidden = true;
    input.focus();
  });
  container.querySelector(".ed-replace-one").addEventListener("click", () => {
    if (!matches.length) return;
    const at = matches[matchIndex] ?? matches[0];
    replaceRange(at, at + findInput.value.length, replaceInput.value);
    matchIndex = -1;
    refreshMatches();
    gotoMatch(1);
  });
  container.querySelector(".ed-replace-all").addEventListener("click", () => {
    if (!findInput.value) return;
    const needle = findInput.value;
    const parts = input.value.split(needle);
    if (parts.length < 2) return;
    input.value = parts.join(replaceInput.value);
    changed();
    refreshMatches();
  });

  /* ------------------------------------------------------------------- api */

  input.value = value;
  paint();

  return {
    /** Always true: this editor is ready the moment it is created. */
    ready: Promise.resolve(false),
    flavour: "fallback",
    get path() {
      return currentPath;
    },
    /** Switch to another file. Undo history is not kept across files here. */
    open(nextPath, nextValue, nextLanguage) {
      currentPath = nextPath;
      highlighter = pickHighlighter(nextLanguage || "text");
      input.value = nextValue ?? "";
      clean = input.value;
      errorLine = null;
      problemEl.textContent = "";
      paint();
    },
    layout() {},
    dispose() {
      container.innerHTML = "";
    },
    get value() {
      return input.value;
    },
    set value(next) {
      input.value = next;
      clean = next;
      errorLine = null;
      problemEl.textContent = "";
      paint();
    },
    get dirty() {
      return input.value !== clean;
    },
    markSaved() {
      clean = input.value;
      errorLine = null;
      problemEl.textContent = "";
      paint();
    },
    showProblem(problem) {
      if (!problem) {
        errorLine = null;
        problemEl.textContent = "";
        paint();
        return;
      }
      errorLine = problem.line || null;
      problemEl.innerHTML = `<span class="ed-err">line ${problem.line}: ${escapeHtml(
        problem.message || "",
      )}</span>`;
      paint();
      if (problem.line) this.gotoLine(problem.line);
    },
    gotoLine(line) {
      const lines = input.value.split("\n");
      let at = 0;
      for (let i = 0; i < Math.min(line - 1, lines.length); i++) at += lines[i].length + 1;
      setSelection(at, at + (lines[line - 1] || "").length);
    },
    focus() {
      input.focus();
    },
  };
}
