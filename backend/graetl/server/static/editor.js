/* The GraETL code editor ----------------------------------------------------
 * Monaco, loaded from the first source that answers:
 *
 *   1. /vendor/vs          a copy vendored into the install (fully offline)
 *   2. [ui] monaco_url     whatever graetl.toml points at
 *   3. the jsDelivr CDN    convenient, but needs internet
 *
 * If none of them load - air-gapped machine, no vendored copy - the
 * hand-written editor in fallback-editor.js takes over, so the Files tab
 * always works. Both implement the same handle:
 *
 *   handle.value            get / set the text
 *   handle.dirty            unsaved changes?
 *   handle.markSaved()      the current text is now the saved text
 *   handle.showProblem(p)   {line, column, message} or null
 *   handle.gotoLine(n)      reveal and select a line
 *   handle.open(path, text, language)   switch file, keeping undo per file
 *   handle.focus() / layout() / dispose()
 *   handle.ready            resolves true once Monaco is live, false otherwise
 * ------------------------------------------------------------------------- */

import { createFallbackEditor } from "/fallback-editor.js";

const MONACO_VERSION = "0.52.2";
const CDN = `https://cdn.jsdelivr.net/npm/monaco-editor@${MONACO_VERSION}/min/vs`;
const VENDORED = "/vendor/vs";

/** Base URL from the server config, set by the app before first use.
 *  An empty string means "never go to the network": only a vendored copy is
 *  tried, and without one the offline fallback editor takes over. */
let configuredBase;
export function configureMonaco(url) {
  configuredBase = url;
  monacoPromise = null;
}

function monacoBases() {
  if (configuredBase === "") return [VENDORED];
  return [VENDORED, configuredBase || CDN];
}

let monacoPromise = null;
let monacoSource = null;

export function monacoStatus() {
  return monacoSource;
}

function loadScript(src) {
  return new Promise((resolve, reject) => {
    const el = document.createElement("script");
    el.src = src;
    el.async = false;
    el.onload = () => resolve(true);
    el.onerror = () => reject(new Error(`cannot load ${src}`));
    document.head.appendChild(el);
  });
}

/* Monaco's workers must come from the same base URL. When that base is another
   origin the browser refuses to start the worker directly, so we hand it a tiny
   same-origin blob that imports the real one. */
function workerEnvironment(base) {
  const absolute = new URL(base + "/", location.href).href;
  self.MonacoEnvironment = {
    baseUrl: absolute,
    getWorkerUrl() {
      const proxy = `self.MonacoEnvironment = { baseUrl: ${JSON.stringify(absolute)} };\n` +
        `importScripts(${JSON.stringify(absolute + "base/worker/workerMain.js")});`;
      return URL.createObjectURL(new Blob([proxy], { type: "text/javascript" }));
    },
  };
}

async function loadFrom(base) {
  if (!window.require || typeof window.require.config !== "function") {
    await loadScript(`${base}/loader.js`);
  }
  if (!window.require || typeof window.require.config !== "function") {
    throw new Error("AMD loader missing");
  }
  window.require.config({ paths: { vs: base } });
  workerEnvironment(base);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("monaco load timed out")), 20000);
    window.require(
      ["vs/editor/editor.main"],
      () => {
        clearTimeout(timer);
        resolve(window.monaco);
      },
      (err) => {
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}

function defineTheme(monaco) {
  monaco.editor.defineTheme("graetl-dark", {
    base: "vs-dark",
    inherit: true,
    rules: [
      { token: "comment", foreground: "6b7487", fontStyle: "italic" },
      { token: "keyword", foreground: "c792ea" },
      { token: "string", foreground: "8bd49c" },
      { token: "number", foreground: "f0b866" },
      { token: "type", foreground: "4dd0e1" },
      { token: "function", foreground: "5b8cff" },
      { token: "decorator", foreground: "f0b866" },
    ],
    colors: {
      "editor.background": "#11141b",
      "editor.foreground": "#d7deea",
      "editorLineNumber.foreground": "#4a5265",
      "editorLineNumber.activeForeground": "#8b95a8",
      "editorGutter.background": "#11141b",
      "editor.lineHighlightBackground": "#171b24",
      "editor.selectionBackground": "#2c3a4a",
      "editorCursor.foreground": "#5b8cff",
      "editorWidget.background": "#171b24",
      "editorWidget.border": "#252b38",
      "input.background": "#11141b",
      "dropdown.background": "#171b24",
    },
  });
}

async function loadMonaco() {
  if (monacoPromise) return monacoPromise;
  monacoPromise = (async () => {
    const bases = monacoBases();
    const tried = [];
    for (const base of bases) {
      try {
        const monaco = await loadFrom(base);
        if (!monaco) throw new Error("no monaco global");
        defineTheme(monaco);
        monacoSource = base;
        return monaco;
      } catch (err) {
        tried.push(`${base}: ${err.message}`);
      }
    }
    monacoSource = null;
    console.info("[graetl] Monaco unavailable, using the built-in editor.\n" + tried.join("\n"));
    return null;
  })();
  return monacoPromise;
}

/* ------------------------------------------------------------------ public */

const LANGUAGES = {
  py: "python",
  toml: "ini",
  json: "json",
  md: "markdown",
  txt: "plaintext",
  sql: "sql",
  csv: "plaintext",
  yaml: "yaml",
  yml: "yaml",
  ini: "ini",
  graph: "json",
  graphlib: "json",
};

export function languageFor(path) {
  const ext = String(path || "").split(".").pop().toLowerCase();
  return LANGUAGES[ext] || "plaintext";
}

export function createEditor(container, options = {}) {
  const handle = new Editor(container, options);
  return handle;
}

class Editor {
  constructor(container, options) {
    this.container = container;
    this.options = options;
    this.onSave = options.onSave;
    this.onChange = options.onChange;
    this.onDirty = options.onDirty;

    this.path = options.path || null;
    this.text = options.value ?? "";
    this.language = options.language || languageFor(this.path);
    this.clean = this.text;
    this.problem = null;

    this.monaco = null;
    this.editor = null;
    this.models = new Map();      // path -> {model, state}
    this.fallback = null;
    this.flavour = "loading";

    container.innerHTML = `<div class="mon">
        <div class="mon-host"></div>
        <div class="mon-status">
          <span class="mon-pos">1:1</span>
          <span class="mon-dirty"></span>
          <div class="spacer"></div>
          <span class="mon-problem"></span>
          <span class="mon-flavour dim">loading editor…</span>
        </div>
      </div>`;
    this.host = container.querySelector(".mon-host");
    this.posEl = container.querySelector(".mon-pos");
    this.dirtyEl = container.querySelector(".mon-dirty");
    this.problemEl = container.querySelector(".mon-problem");
    this.flavourEl = container.querySelector(".mon-flavour");

    this.ready = this._boot();
  }

  async _boot() {
    const monaco = await loadMonaco();
    if (this.disposed) return false;
    if (!monaco) {
      this._bootFallback();
      return false;
    }
    this.monaco = monaco;
    this.flavour = "monaco";
    this.container.querySelector(".mon-status").hidden = false;
    this.flavourEl.textContent = "Monaco · Ctrl+S save · Ctrl+F find · F1 commands";

    this.editor = monaco.editor.create(this.host, {
      model: this._model(this.path, this.text, this.language),
      theme: "graetl-dark",
      automaticLayout: true,
      fontSize: 13,
      fontFamily: "ui-monospace, SFMono-Regular, 'JetBrains Mono', Consolas, monospace",
      tabSize: 4,
      insertSpaces: true,
      renderWhitespace: "selection",
      rulers: [100],
      minimap: { enabled: true, renderCharacters: false, maxColumn: 90 },
      scrollBeyondLastLine: false,
      smoothScrolling: true,
      cursorBlinking: "smooth",
      bracketPairColorization: { enabled: true },
      guides: { bracketPairs: true, indentation: true },
      padding: { top: 10, bottom: 10 },
      suggestSelection: "first",
      fixedOverflowWidgets: true,
    });

    this.editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => {
      if (this.onSave) this.onSave(this.value);
    });
    this.editor.onDidChangeModelContent(() => {
      this._paintDirty();
      if (this.onChange) this.onChange(this.value);
    });
    this.editor.onDidChangeCursorPosition((e) => {
      this.posEl.textContent = `${e.position.lineNumber}:${e.position.column}`;
    });
    if (this.problem) this.showProblem(this.problem);
    this._paintDirty();
    return true;
  }

  _bootFallback() {
    this.flavour = "fallback";
    this.host.innerHTML = "";
    this.container.querySelector(".mon-status").hidden = true;
    this.fallback = createFallbackEditor(this.host, {
      value: this.text,
      language: this.options.language || (this.path?.endsWith(".toml") ? "toml" : "python"),
      path: this.path,
      onSave: (value) => this.onSave && this.onSave(value),
      onChange: (value) => this.onChange && this.onChange(value),
    });
    if (this.problem) this.fallback.showProblem(this.problem);
  }

  /* One model per file: switching files keeps undo history and cursor. */
  _model(path, text, language) {
    const key = path || "__scratch__";
    this.currentKey = key;
    const existing = this.models.get(key);
    if (existing) {
      if (text !== undefined && existing.model.getValue() !== text) {
        existing.model.setValue(text);
      }
      return existing.model;
    }
    const uri = this.monaco.Uri.parse(`inmemory://graetl/${encodeURI(key)}`);
    const model =
      this.monaco.editor.getModel(uri) ||
      this.monaco.editor.createModel(text ?? "", language || languageFor(path), uri);
    this.models.set(key, { model, state: null });
    return model;
  }

  _paintDirty() {
    const dirty = this.dirty;
    if (this.dirtyEl) this.dirtyEl.textContent = dirty ? "● unsaved" : "";
    if (this.onDirty) this.onDirty(dirty);
  }

  /* ------------------------------------------------------------- handle api */

  get value() {
    if (this.editor) return this.editor.getValue();
    if (this.fallback) return this.fallback.value;
    return this.text;
  }

  set value(next) {
    this.text = next;
    this.clean = next;
    if (this.editor) this.editor.setValue(next);
    else if (this.fallback) this.fallback.value = next;
    this._paintDirty();
  }

  get dirty() {
    return this.value !== this.clean;
  }

  open(path, text, language) {
    this.showProblem(null);
    this.path = path;
    this.text = text ?? "";
    this.clean = this.text;
    this.language = language || languageFor(path);
    if (this.editor) {
      const previous = this.models.get(this.currentKey);
      if (previous) previous.state = this.editor.saveViewState();
      const model = this._model(path, this.text, this.language);
      this.editor.setModel(model);
      const entry = this.models.get(path || "__scratch__");
      if (entry?.state) this.editor.restoreViewState(entry.state);
      this.editor.focus();
    } else if (this.fallback) {
      this.fallback.open(path, this.text, this.language === "ini" ? "toml" : this.language);
    }
    this._paintDirty();
  }

  markSaved() {
    this.clean = this.value;
    this.showProblem(null);
    if (this.fallback) this.fallback.markSaved();
    this._paintDirty();
  }

  showProblem(problem) {
    this.problem = problem || null;
    if (this.fallback) {
      this.fallback.showProblem(problem);
      return;
    }
    if (this.problemEl) {
      this.problemEl.innerHTML = problem
        ? `<span class="ed-err">line ${problem.line ?? "?"}: ${String(problem.message || "")
            .replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]))}</span>`
        : "";
    }
    if (!this.editor || !this.monaco) return;
    const model = this.editor.getModel();
    if (!model) return;
    if (!problem) {
      this.monaco.editor.setModelMarkers(model, "graetl", []);
      return;
    }
    const line = Math.min(Math.max(problem.line || 1, 1), model.getLineCount());
    const column = Math.max(problem.column || 1, 1);
    this.monaco.editor.setModelMarkers(model, "graetl", [
      {
        severity: this.monaco.MarkerSeverity.Error,
        message: problem.message || "syntax error",
        startLineNumber: line,
        startColumn: column,
        endLineNumber: line,
        endColumn: model.getLineMaxColumn(line),
      },
    ]);
    if (problem.line) this.gotoLine(problem.line);
  }

  gotoLine(line) {
    if (this.fallback) return this.fallback.gotoLine(line);
    if (!this.editor) return;
    this.editor.revealLineInCenter(line);
    this.editor.setPosition({ lineNumber: line, column: 1 });
    this.editor.focus();
  }

  focus() {
    if (this.editor) this.editor.focus();
    else if (this.fallback) this.fallback.focus();
  }

  layout() {
    if (this.editor) this.editor.layout();
  }

  dispose() {
    this.disposed = true;
    if (this.editor) this.editor.dispose();
    for (const { model } of this.models.values()) model.dispose();
    this.models.clear();
    if (this.fallback) this.fallback.dispose();
    this.editor = null;
    this.fallback = null;
  }
}
