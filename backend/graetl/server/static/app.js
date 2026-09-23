import { configureMonaco, createEditor, languageFor } from "/editor.js";
import { createGraphView } from "/graph.js";
import { basename, createFileTree, dirname, join } from "/filetree.js";

/* GraETL console ----------------------------------------------------------
 * The UI. Plain ES modules, no build step and no dependencies beyond Monaco,
 * which is loaded at runtime and has a hand-written fallback when it is not
 * reachable. Served straight from the package.
 * ------------------------------------------------------------------------ */

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

const TERMINAL = ["succeeded", "failed", "stopped", "crashed"];
const isActive = (s) => s && !TERMINAL.includes(s);

/* ---------------------------------------------------------------- utilities */

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

function fmtDuration(ms) {
  if (ms === null || ms === undefined) return "–";
  if (ms < 1000) return `${ms} ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)} s`;
  const m = Math.floor(s / 60);
  const rest = Math.round(s % 60);
  if (m < 60) return `${m}m ${rest}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

function fmtTime(iso) {
  if (!iso) return "–";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString(undefined, {
    month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

function fmtClock(iso) {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "" : d.toLocaleTimeString(undefined, { hour12: false });
}

function relTime(iso) {
  if (!iso) return "never";
  const diff = (Date.now() - new Date(iso).getTime()) / 1000;
  if (Number.isNaN(diff)) return "–";
  if (diff < 60) return "just now";
  if (diff < 3600) return `${Math.floor(diff / 60)} min ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)} h ago`;
  return `${Math.floor(diff / 86400)} d ago`;
}

function fmtNumber(n) {
  if (n === null || n === undefined) return "–";
  if (typeof n !== "number") return String(n);
  return Number.isInteger(n) ? n.toLocaleString() : n.toFixed(2);
}

/* --------------------------------------------------------- context menus */

function closeContextMenu() {
  document.querySelectorAll(".ctx-menu").forEach((el) => el.remove());
}

/** Right-click menu. `items` is a list of {label, hint, onClick, danger,
 *  disabled} entries; a `null` entry draws a separator. An entry carrying
 *  `swatches` renders a row of colour chips instead and calls `onPick(key)`. */
function contextMenu(ev, items) {
  ev.preventDefault();
  ev.stopPropagation();
  closeContextMenu();
  const visible = items.filter((i) => i !== null);
  if (!visible.length) return;

  const menu = document.createElement("div");
  menu.className = "ctx-menu";
  menu.innerHTML = items
    .map((item, i) => {
      if (item === null) return `<div class="ctx-sep"></div>`;
      if (item.swatches) {
        return `<div class="ctx-swatches" data-swatch-row="${i}">
            <span class="ctx-label dim">${item.label}</span>
            ${item.swatches.map((s) => `<button class="ctx-swatch${s.on ? " on" : ""}"
                 data-swatch="${esc(s.key)}" title="${esc(s.title)}"
                 style="background:${esc(s.tint)}"></button>`).join("")}
          </div>`;
      }
      return `<button class="ctx-item${item.danger ? " danger" : ""}" data-i="${i}" ${item.disabled ? "disabled" : ""}
             title="${esc(item.title || "")}">
             <span class="ctx-label">${item.label}</span>
             ${item.hint ? `<span class="ctx-hint">${esc(item.hint)}</span>` : ""}
           </button>`;
    })
    .join("");
  document.body.appendChild(menu);

  const rect = menu.getBoundingClientRect();
  menu.style.left = `${Math.max(6, Math.min(ev.clientX, window.innerWidth - rect.width - 8))}px`;
  menu.style.top = `${Math.max(6, Math.min(ev.clientY, window.innerHeight - rect.height - 8))}px`;

  menu.addEventListener("mousedown", (e) => e.stopPropagation());
  menu.addEventListener("click", (e) => {
    const chip = e.target.closest("[data-swatch]");
    if (chip) {
      const row = chip.closest("[data-swatch-row]");
      menu.remove();
      items[Number(row.dataset.swatchRow)].onPick(chip.dataset.swatch);
      return;
    }
    const button = e.target.closest("[data-i]");
    if (!button || button.disabled) return;
    menu.remove();
    items[Number(button.dataset.i)].onClick();
  });
  setTimeout(() => {
    document.addEventListener("mousedown", () => menu.remove(), { once: true });
    window.addEventListener("blur", () => menu.remove(), { once: true });
  }, 0);
}

function toast(message, kind = "") {
  const el = document.createElement("div");
  el.className = `toast ${kind}`;
  el.textContent = message;
  $("#toasts").appendChild(el);
  setTimeout(() => el.remove(), 4200);
}

/* --------------------------------------------------------------------- API */

const api = {
  async request(path, options = {}) {
    const res = await fetch(path, {
      headers: { "Content-Type": "application/json" },
      ...options,
      body: options.body ? JSON.stringify(options.body) : undefined,
    });
    const text = await res.text();
    let data = null;
    try { data = text ? JSON.parse(text) : null; } catch { data = text; }
    if (!res.ok) {
      let detail = res.statusText;
      let syntaxError = null;
      if (data && data.detail) {
        if (typeof data.detail === "string") detail = data.detail;
        else if (data.detail.detail) { detail = data.detail.detail; syntaxError = data.detail.syntax_error || null; }
        else detail = JSON.stringify(data.detail);
      }
      const error = new Error(detail);
      error.syntaxError = syntaxError;
      throw error;
    }
    return data;
  },
  get: (p) => api.request(p),
  post: (p, body) => api.request(p, { method: "POST", body: body || {} }),
  put: (p, body) => api.request(p, { method: "PUT", body }),
  patch: (p, body) => api.request(p, { method: "PATCH", body }),
  del: (p) => api.request(p, { method: "DELETE" }),
  upload: async (p, blob) => {
    const res = await fetch(p, { method: "PUT", body: blob });
    const text = await res.text();
    const data = text ? JSON.parse(text) : null;
    if (!res.ok) throw new Error(data?.detail || res.statusText);
    return data;
  },
};

/* ------------------------------------------------------------------- state */

const state = {
  pipelines: [],
  health: null,
  project: null,
  route: { view: "dashboard", id: null, tab: null },
  runSocket: null,
  runSocketId: null,
  console: { events: [], resources: [], follow: true, levels: new Set(["info", "success", "warning", "error", "event"]), filter: "" },
  detail: { pipeline: null, runs: [], entities: null, files: null, file: null, run: null },
  /** Files opened in the editor this session, shown as tabs. */
  openFiles: [],
  editor: null,
  editorPipeline: null,
};

/* The editor keeps its models, undo history and scroll position across file
   switches and tab changes, so it lives in a node we move around rather than
   one the renderer throws away. */
const editorHost = document.createElement("div");
editorHost.className = "editor-host";

/* -------------------------------------------------------------- navigation */

function parseHash() {
  const raw = location.hash.replace(/^#\/?/, "");
  const parts = raw.split("/").filter(Boolean);
  if (parts.length === 0) return { view: "dashboard" };
  if (parts[0] === "runs" && parts[1]) return { view: "run", id: Number(parts[1]) };
  if (parts[0] === "runs") return { view: "runs" };
  if (parts[0] === "p" && parts[1]) return { view: "pipeline", id: parts[1], tab: parts[2] || "overview" };
  return { view: "dashboard" };
}

function go(hash) { location.hash = hash; }

/* --------------------------------------------------------------- rendering */

function statusPill(status, label) {
  return `<span class="pill ${esc(status)}"><span class="dot ${esc(status)}"></span>${esc(label || status)}</span>`;
}

function renderSidebar() {
  const list = $("#pipeline-list");
  if (!state.pipelines.length) {
    list.innerHTML = `<div class="empty" style="padding:18px 10px">No pipelines yet.<br><br>
      <button class="btn sm primary" id="btn-new-empty">Create one</button></div>`;
    $("#btn-new-empty")?.addEventListener("click", openNewPipeline);
  } else {
    list.innerHTML = state.pipelines.map((p) => {
      const status = p.active_run ? p.active_run.status : (p.last_run ? p.last_run.status : "idle");
      const cls = p.enabled === false ? "disabled" : status;
      const meta = p.definition_error ? "error" : (p.stateful ? `${p.state_summary?.entities ?? 0}e` : "–");
      return `<div class="pl-item ${state.route.id === p.id ? "active" : ""}" data-pipeline="${esc(p.id)}">
        <span class="dot ${esc(cls)}"></span>
        <span class="pl-name" title="${esc(p.title)}">${esc(p.title)}</span>
        <span class="pl-meta">${esc(meta)}</span>
      </div>`;
    }).join("");
  }
  $$("[data-pipeline]", list).forEach((el) => {
    const pid = el.dataset.pipeline;
    el.addEventListener("click", () => go(`#/p/${pid}`));
    el.addEventListener("contextmenu", (ev) => {
      const p = state.pipelines.find((x) => x.id === pid);
      if (!p) return;
      const busy = !!p.active_run;
      contextMenu(ev, [
        { label: "▤ Open", onClick: () => go(`#/p/${pid}`) },
        { label: "✎ Code", hint: "files", onClick: () => go(`#/p/${pid}/files`) },
        { label: "≡ Entities", disabled: !p.stateful, onClick: () => go(`#/p/${pid}/entities`) },
        null,
        { label: "▶ Run", hint: "incremental", disabled: busy, onClick: () => startRun(pid, { mode: "incremental" }) },
        { label: "▶ Run full", disabled: busy, onClick: () => startRun(pid, { mode: "full" }) },
        { label: "↻ Retry failed", disabled: busy, onClick: () => startRun(pid, { mode: "retry-failed" }) },
        ...(busy
          ? [{ label: "■ Stop the active run", danger: true, onClick: () => pipelineAction(p, "stop") }]
          : []),
        null,
        {
          label: p.enabled === false ? "✓ Enable" : "⊘ Disable",
          onClick: () => pipelineAction(p, "toggle"),
        },
      ]);
    });
  });
  $$(".nav-item").forEach((el) => {
    el.classList.toggle("active", el.dataset.nav === state.route.view || (state.route.view === "run" && el.dataset.nav === "runs"));
  });
}

function renderHealth() {
  const h = state.health;
  $("#brand-sub").textContent = h ? `v${h.version} · ${h.active_runs} active` : "offline";
  $("#side-foot").innerHTML = h
    ? `${h.database ? `<div title="${esc(h.database)}">db · ${esc(String(h.database).split(/[\\/]/).slice(-2).join("/"))}</div>` : ""}
       <div>ui · ${esc(h.ui)}</div>`
    : "server unreachable";
}

/* ---------------------------------------------------------------- dashboard */

async function viewDashboard() {
  const data = await api.get("/api/overview");
  state.pipelines = data.pipelines;
  renderSidebar();
  const c = data.counts;
  $("#main").innerHTML = `
    <div class="page">
      <div class="page-head">
        <div>
          <h1>Dashboard</h1>
          <div class="page-sub">Orchestrate, run and observe your ETL pipelines.</div>
        </div>
        <div class="spacer"></div>
        <div class="row">
          <button class="btn" id="d-sync">Rescan folder</button>
          <button class="btn primary" id="d-new">New pipeline</button>
        </div>
      </div>
      <div class="cards">
        <div class="card"><div class="card-label">Pipelines</div><div class="card-value">${c.pipelines}</div></div>
        <div class="card"><div class="card-label">Stateful</div><div class="card-value">${c.stateful}</div></div>
        <div class="card"><div class="card-label">Active runs</div><div class="card-value" style="color:${c.running ? "var(--run)" : "inherit"}">${c.running}</div></div>
        <div class="card"><div class="card-label">Last run failed</div><div class="card-value" style="color:${c.failing ? "var(--err)" : "inherit"}">${c.failing}</div></div>
      </div>

      <div class="panel">
        <div class="panel-head">Pipelines</div>
        <div class="panel-body flush table-wrap">${pipelineTable(data.pipelines)}</div>
      </div>

      <div class="panel">
        <div class="panel-head">Recent runs</div>
        <div class="panel-body flush table-wrap">${runTable(data.recent_runs, true)}</div>
      </div>
    </div>`;
  $("#d-sync").addEventListener("click", syncPipelines);
  $("#d-new").addEventListener("click", openNewPipeline);
  wireTables();
}

function pipelineTable(pipelines) {
  if (!pipelines.length) return `<div class="empty">No pipelines found. Create one to get started.</div>`;
  return `<table><thead><tr>
      <th>Pipeline</th><th>Type</th><th>Steps</th><th>Entities</th><th>Status</th>
      <th>Last run</th><th class="num">Duration</th><th></th>
    </tr></thead><tbody>
    ${pipelines.map((p) => {
      const def = p.definition || {};
      const steps = (def.modules?.length || 0) + (def.tasks?.length || 0);
      const status = p.active_run ? p.active_run.status : (p.last_run ? p.last_run.status : "idle");
      const busy = !!p.active_run;
      return `<tr class="clickable" data-goto="#/p/${esc(p.id)}">
        <td><strong>${esc(p.title)}</strong><div class="dim mono" style="font-size:11px">${esc(p.id)}</div></td>
        <td>${p.definition_error ? '<span class="pill failed">broken</span>' : (p.stateful ? '<span class="pill tag">stateful</span>' : '<span class="pill tag">stateless</span>')}</td>
        <td class="num">${steps}</td>
        <td class="num">${p.stateful ? fmtNumber(p.state_summary?.entities ?? 0) : "–"}</td>
        <td>${status === "idle" ? '<span class="pill tag">idle</span>' : statusPill(status)}</td>
        <td class="dim">${esc(relTime(p.last_run?.finished_at))}</td>
        <td class="num dim">${esc(fmtDuration(p.last_run?.duration_ms))}</td>
        <td><button class="btn sm ${busy ? "" : "primary"}" data-run="${esc(p.id)}" ${busy || p.enabled === false ? "disabled" : ""}>Run</button></td>
      </tr>`;
    }).join("")}
    </tbody></table>`;
}

function runTable(runs, withPipeline) {
  if (!runs.length) return `<div class="empty">No runs yet.</div>`;
  return `<table><thead><tr>
      <th>Run</th>${withPipeline ? "<th>Pipeline</th>" : ""}<th>Status</th><th>Mode</th>
      <th>Progress</th><th class="num">Processed</th><th class="num">Failed</th>
      <th class="num">Duration</th><th>Finished</th>
    </tr></thead><tbody>
    ${runs.map((r) => {
      const m = r.metrics || {};
      const total = r.progress_total || 0;
      const pct = total ? Math.min(100, Math.round((r.progress_done / total) * 100)) : (isActive(r.status) ? 0 : 100);
      return `<tr class="clickable" data-goto="#/runs/${r.id}">
        <td class="mono">#${r.id}</td>
        ${withPipeline ? `<td>${esc(r.pipeline_id)}</td>` : ""}
        <td>${statusPill(r.status)}</td>
        <td class="dim">${esc(r.mode)}</td>
        <td><div class="bar ${r.status === "failed" ? "err" : (r.status === "succeeded" ? "ok" : "")}"><i style="width:${pct}%"></i></div></td>
        <td class="num">${fmtNumber(m.processed ?? 0)}</td>
        <td class="num" style="${m.failed ? "color:var(--err)" : ""}">${fmtNumber(m.failed ?? 0)}</td>
        <td class="num dim">${esc(fmtDuration(r.duration_ms))}</td>
        <td class="dim">${esc(r.finished_at ? relTime(r.finished_at) : (r.started_at ? "running" : "queued"))}</td>
      </tr>`;
    }).join("")}
    </tbody></table>`;
}

function wireTables() {
  $$("[data-goto]").forEach((el) => el.addEventListener("click", (ev) => {
    if (ev.target.closest("button")) return;
    go(el.dataset.goto);
  }));
  $$("[data-run]").forEach((el) => el.addEventListener("click", async (ev) => {
    ev.stopPropagation();
    await startRun(el.dataset.run, {});
  }));
}

/* --------------------------------------------------------------- runs view */

async function viewRuns() {
  const runs = await api.get("/api/runs?limit=100");
  $("#main").innerHTML = `
    <div class="page">
      <div class="page-head"><div><h1>Runs</h1><div class="page-sub">Every execution, newest first.</div></div></div>
      <div class="panel"><div class="panel-body flush table-wrap">${runTable(runs, true)}</div></div>
    </div>`;
  wireTables();
}

/* ----------------------------------------------------------- pipeline view */

async function viewPipeline(id, tab) {
  const p = await api.get(`/api/pipelines/${encodeURIComponent(id)}`);
  state.detail.pipeline = p;
  const def = p.definition || {};
  const active = p.active_run;
  const paused = active && ["paused", "pausing"].includes(active.status);

  $("#main").innerHTML = `
    <div class="page">
      <div class="page-head">
        <div>
          <h1>${esc(p.title)}</h1>
          <div class="page-sub">
            <span class="mono">${esc(p.id)}</span> ·
            ${p.stateful ? "stateful" : "stateless"} ·
            ${(def.modules?.length || 0)} module(s), ${(def.tasks?.length || 0)} task(s)
            ${(p.tags || []).map((t) => ` · <span class="pill tag">${esc(t)}</span>`).join("")}
          </div>
        </div>
        <div class="spacer"></div>
        <div class="row" id="actions">
          ${active ? `
            ${paused
              ? `<button class="btn primary" data-act="resume">▶ Resume</button>`
              : `<button class="btn" data-act="pause">⏸ Pause</button>`}
            <button class="btn danger" data-act="stop">■ Stop</button>
            <a class="btn" href="#/runs/${active.id}">Open console</a>`
            : `
            <button class="btn primary" data-act="run">▶ Run</button>
            <button class="btn" data-act="run-full">Run full</button>
            <button class="btn" data-act="retry">Retry failed</button>`}
          <button class="btn" data-act="toggle">${p.enabled ? "Disable" : "Enable"}</button>
        </div>
      </div>

      ${p.definition_error ? `<div class="banner">${esc(p.definition_error)}</div>` : ""}
      ${active ? `<div class="banner info">Run #${active.id} is ${esc(active.status)}${active.phase ? ` · ${esc(active.phase)}` : ""}${active.progress_total ? ` · ${active.progress_done}/${active.progress_total}` : ""} — <a href="#/runs/${active.id}" style="color:var(--accent)">open the live console</a></div>` : ""}

      <div class="tabs">
        ${["overview", "runs", "entities", "files"].map((t) => `
          <button class="tab ${tab === t ? "active" : ""}" data-tab="${t}"
            title="Shortcut: ${t[0]}"
            ${t === "entities" && !p.stateful ? "disabled style='opacity:.35;cursor:not-allowed'" : ""}>
            ${t[0].toUpperCase() + t.slice(1)}<span class="tab-key">${t[0]}</span>
          </button>`).join("")}
      </div>
      <div id="tab-body"><div class="empty">Loading…</div></div>
    </div>`;

  $$("[data-tab]").forEach((el) => el.addEventListener("click", () => go(`#/p/${id}/${el.dataset.tab}`)));
  $$("#actions [data-act]").forEach((el) => el.addEventListener("click", () => pipelineAction(p, el.dataset.act)));

  const body = $("#tab-body");
  if (tab === "runs") {
    const runs = await api.get(`/api/pipelines/${encodeURIComponent(id)}/runs?limit=100`);
    body.innerHTML = `<div class="panel"><div class="panel-body flush table-wrap">${runTable(runs, false)}</div></div>`;
    wireTables();
  } else if (tab === "entities") {
    await renderEntities(id, body);
  } else if (tab === "files") {
    await renderFiles(id, body);
  } else {
    renderPipelineOverview(p, body);
  }
}

/** Repaint the page header for the current pipeline without touching the tab.
 *  Used while the Files tab is open, because that tab holds state a re-render
 *  would destroy: the canvas, a test run in flight, unsaved editor text. */
async function refreshPipelineHeader(id) {
  let p;
  try {
    p = await api.get(`/api/pipelines/${encodeURIComponent(id)}`);
  } catch {
    return;
  }
  state.detail.pipeline = p;
  const active = p.active_run;
  const paused = active && ["paused", "pausing"].includes(active.status);

  const actions = $("#actions");
  if (actions) {
    actions.innerHTML = `
      ${active ? `
        ${paused
          ? `<button class="btn primary" data-act="resume">▶ Resume</button>`
          : `<button class="btn" data-act="pause">⏸ Pause</button>`}
        <button class="btn danger" data-act="stop">■ Stop</button>
        <a class="btn" href="#/runs/${active.id}">Open console</a>`
        : `
        <button class="btn primary" data-act="run">▶ Run</button>
        <button class="btn" data-act="run-full">Run full</button>
        <button class="btn" data-act="retry">Retry failed</button>`}
      <button class="btn" data-act="toggle">${p.enabled ? "Disable" : "Enable"}</button>`;
    $$("#actions [data-act]").forEach((el) =>
      el.addEventListener("click", () => pipelineAction(p, el.dataset.act)));
  }

  // Saving is refused while a run is active; keep the buttons honest without
  // rebuilding the pane they live in.
  if (state.files) state.files.active = !!active;
  const graph = isGraph(state.detail.file);
  const save = $("#file-save");
  if (save) save.disabled = !!active || (!graph && !state.detail.file);
  const check = $("#file-check");
  if (check) check.disabled = !state.detail.file;
}

function renderPipelineOverview(p, body) {
  const def = p.definition || {};
  const summary = p.state_summary || {};
  const byModule = Object.fromEntries((summary.modules || []).map((m) => [m.module, m]));
  const last = p.last_run;
  const metrics = last?.metrics?.counters || {};

  body.innerHTML = `
    <div class="cards">
      <div class="card"><div class="card-label">Last run</div>
        <div class="card-value small">${last ? statusPill(last.status) : '<span class="dim">never run</span>'}</div>
        <div class="dim" style="font-size:12px;margin-top:6px">${esc(last ? relTime(last.finished_at) : "")}</div></div>
      <div class="card"><div class="card-label">Success rate</div>
        <div class="card-value">${p.stats?.success_rate === null || p.stats?.success_rate === undefined ? "–" : Math.round(p.stats.success_rate * 100) + "%"}</div>
        <div class="dim" style="font-size:12px">last ${p.stats?.runs_considered ?? 0} run(s)</div></div>
      <div class="card"><div class="card-label">Avg duration</div>
        <div class="card-value small">${esc(fmtDuration(p.stats?.avg_duration_ms))}</div></div>
      ${p.stateful ? `<div class="card"><div class="card-label">Entities</div>
        <div class="card-value">${fmtNumber(summary.entities || 0)}</div>
        <div class="dim" style="font-size:12px">${esc(last?.metrics?.work_remaining !== undefined ? `${last.metrics.work_remaining} item(s) pending` : "")}</div></div>` : ""}
    </div>

    ${p.description ? `<div class="panel"><div class="panel-body">${esc(p.description)}</div></div>` : ""}

    <div class="panel">
      <div class="panel-head">Steps
        <span class="dim" style="font-weight:400">double-click a module to edit its code · right-click for actions</span>
      </div>
      <div class="panel-body flush table-wrap">
        ${(def.tasks?.length || def.modules?.length) ? `
        <table><thead><tr><th>Step</th><th>Kind</th><th class="num">Version</th>
          <th class="num">Layer</th><th>Requires</th>
          <th class="num">Done</th><th class="num">Pending</th><th class="num">Failed</th>
          <th>Description</th><th></th></tr></thead><tbody>
          ${(def.tasks || []).map((t) => `<tr>
            <td class="mono">${esc(t.name)}</td>
            <td><span class="pill tag">task · ${esc(t.phase)}</span></td>
            <td class="num">v${t.version}</td><td class="num dim">–</td><td class="dim">–</td>
            <td class="num dim">–</td><td class="num dim">–</td><td class="num dim">–</td>
            <td class="dim">${esc(t.description || "")}</td>
            <td><button class="btn sm" data-run-step="${esc(t.name)}" title="Run only this task">▶</button></td>
            </tr>`).join("")}
          ${(def.modules || []).map((m) => {
            const s = byModule[m.name] || {};
            const graphs = (m.meta && m.meta.graphs) || [];
            return `<tr class="row-module" data-module="${esc(m.name)}" data-file="${esc(m.file || "")}"
              title="Double-click to edit ${esc(m.file || m.name)} · right-click for actions">
            <td class="mono">${esc(m.name)}
              ${m.folder ? `<div class="dim" style="font-size:11px">${esc(m.folder)}/</div>` : ""}</td>
            <td><span class="pill tag">module</span>${graphs.length ? ` <span class="pill tag">${graphs.length} graph</span>` : ""}</td>
            <td class="num">v${m.version}</td>
            <td class="num">${m.execution_layer ?? 0}</td>
            <td class="dim mono">${esc((m.requires || m.depends_on || []).join(", ") || "–")}</td>
            <td class="num">${fmtNumber(s.done || 0)}</td>
            <td class="num">${fmtNumber((s.pending || 0) + (s.running || 0))}</td>
            <td class="num" style="${s.failed ? "color:var(--err)" : ""}">${fmtNumber(s.failed || 0)}</td>
            <td class="dim">${esc(m.description || "")}</td>
            <td><div class="btn-group">
              <button class="btn sm" data-run-step="${esc(m.name)}" title="Run this module for every pending entity">▶</button>
              <button class="btn sm" data-debug-step="${esc(m.name)}" title="Debug run: one entity, ctx.debug() on">🐞</button>
              <button class="btn sm" data-profile-step="${esc(m.name)}" title="Profile run over a sample">⏱</button>
            </div></td></tr>`;
          }).join("")}
          ${(def.pending_modules || []).map((g) => `<tr style="opacity:.6">
            <td class="mono">${esc(g.name)}
              <div class="dim" style="font-size:11px">${esc(g.file)}</div></td>
            <td><span class="pill stopped">graph</span></td>
            <td class="num dim">–</td><td class="num dim">–</td><td class="dim">–</td>
            <td class="num dim">–</td><td class="num dim">–</td><td class="num dim">–</td>
            <td class="dim">Node-flow module - not executable yet</td><td></td></tr>`).join("")}
        </tbody></table>` : `<div class="empty">This pipeline has no steps yet — a run will succeed immediately.</div>`}
      </div>
    </div>

    ${renderModuleFolders(def)}

    ${Object.keys(metrics).length ? `
    <div class="panel">
      <div class="panel-head">Metrics of run #${last.id}</div>
      <div class="panel-body">
        <div class="cards">
          ${Object.entries(metrics).map(([k, v]) => `
            <div class="card"><div class="card-label">${esc(k)}</div><div class="card-value small">${fmtNumber(v)}</div></div>`).join("")}
        </div>
      </div>
    </div>` : ""}

    ${last?.error ? `<div class="banner">${esc(last.error)}</div>` : ""}`;

  const busy = !!p.active_run;
  $$("[data-run-step]", body).forEach((el) => el.addEventListener("click", () => {
    if (busy) return toast("A run is already active", "error");
    startRun(p.id, { steps: [el.dataset.runStep] });
  }));
  $$("[data-debug-step]", body).forEach((el) => el.addEventListener("click", () => {
    if (busy) return toast("A run is already active", "error");
    openModuleRun(p.id, el.dataset.debugStep, "debug");
  }));
  $$("[data-profile-step]", body).forEach((el) => el.addEventListener("click", () => {
    if (busy) return toast("A run is already active", "error");
    openModuleRun(p.id, el.dataset.profileStep, "profile");
  }));

  // Direct manipulation: the row itself is the module.
  $$("tr.row-module", body).forEach((row) => {
    row.addEventListener("dblclick", (ev) => {
      if (ev.target.closest("button")) return;
      openSource(p.id, row.dataset.file, row.dataset.module);
    });
    row.addEventListener("contextmenu", (ev) =>
      moduleMenu(ev, p, row.dataset.module, row.dataset.file),
    );
  });
  $$("tr.row-folder", body).forEach((row) => {
    const files = JSON.parse(row.dataset.files || "[]");
    row.addEventListener("dblclick", () => openSource(p.id, files[0], row.dataset.folder));
    row.addEventListener("contextmenu", (ev) =>
      contextMenu(
        ev,
        files.length
          ? files.map((f) => ({
              label: `✎ Edit <span class="mono">${esc(f.split("/").pop())}</span>`,
              hint: f,
              onClick: () => openSource(p.id, f),
            }))
          : [{ label: "No editable file in this folder", disabled: true, onClick: () => {} }],
      ),
    );
  });
}

/* What you can do to a module, without hunting for a button. */
function moduleMenu(ev, p, name, file) {
  const busy = !!p.active_run;
  contextMenu(ev, [
    {
      label: "▶ Run this module",
      hint: "pending entities",
      disabled: busy,
      onClick: () => startRun(p.id, { steps: [name] }),
    },
    {
      label: "🐞 Debug run",
      hint: "one entity",
      disabled: busy,
      onClick: () => openModuleRun(p.id, name, "debug"),
    },
    {
      label: "⏱ Profile run",
      hint: "sample",
      disabled: busy,
      onClick: () => openModuleRun(p.id, name, "profile"),
    },
    null,
    {
      label: "✎ Edit source",
      hint: "double-click",
      disabled: !file,
      onClick: () => openSource(p.id, file, name),
    },
    {
      label: "⧉ Copy module name",
      onClick: () => copyText(name),
    },
    null,
    {
      label: "↺ Reset this module's state",
      hint: "reprocess all",
      danger: true,
      disabled: busy,
      onClick: () => confirmResetModule(p.id, name),
    },
  ]);
}

async function confirmResetModule(pipelineId, name) {
  if (!confirm(`Forget everything ${name} has processed?\n\nThe next run reprocesses every entity for this module. Data the module wrote is untouched.`)) return;
  try {
    const res = await api.post(`/api/pipelines/${encodeURIComponent(pipelineId)}/state/reset`, { module: name });
    toast(`Reset ${res.reset} state row(s) for ${name}`, "success");
    render();
  } catch (err) {
    toast(err.message, "error");
  }
}

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    toast(`Copied ${text}`);
  } catch {
    toast("Clipboard is not available here", "error");
  }
}

/** Jump straight to a file in the editor, switching tab if needed. */
function openSource(pipelineId, file, what) {
  if (!file) {
    toast(`${what || "This step"} has no editable source file`, "error");
    return;
  }
  state.detail.file = file;
  rememberOpenFile(file);
  const onFiles =
    state.route.view === "pipeline" && state.route.id === pipelineId && state.route.tab === "files";
  if (onFiles) selectFile(file);
  else go(`#/p/${pipelineId}/files`);
}

function renderModuleFolders(def) {
  const folders = def.module_folders || [];
  const functions = def.functions || [];
  const graphlibs = def.graphlibs || [];
  if (!folders.length && !functions.length && !graphlibs.length) return "";

  const byName = Object.fromEntries((def.modules || []).map((m) => [m.name, m]));
  const folderRows = folders.map((f) => {
    const files = (f.modules || []).map((name) => byName[name]?.file).filter(Boolean);
    return `<tr class="row-folder" data-folder="${esc(f.folder)}"
      data-files="${esc(JSON.stringify(files))}"
      title="${files.length ? "Double-click to edit " + esc(files[0]) : "No python module in this folder"}">
      <td class="mono">${esc(f.folder)}/</td>
      <td>${(f.modules || []).length ? `<span class="pill tag">${(f.modules || []).length} .module.py</span>` : ""}
          ${(f.graph_modules || []).length ? `<span class="pill stopped">${(f.graph_modules || []).length} .graph</span>` : ""}</td>
      <td class="mono dim">${esc([...(f.modules || []), ...((f.graph_modules || []).map((g) => g.name + " (graph)"))].join(", ") || "–")}</td>
      <td class="num">${(f.graphs || []).length}</td>
      <td class="num">${(f.graphlibs || []).length}</td>
      <td class="dim">${esc((f.meta && f.meta.owner) || "")}</td>
    </tr>`;
  }).join("");

  return `<div class="panel">
    <div class="panel-head">Module folders
      <span class="dim" style="font-weight:400">pipelines/${esc(def.id)}/modules/</span></div>
    <div class="panel-body flush table-wrap">
      ${folders.length ? `<table><thead><tr>
        <th>Folder</th><th>Contains</th><th>Defines</th>
        <th class="num">.graph</th><th class="num">.graphlib</th><th>Owner</th>
      </tr></thead><tbody>${folderRows}</tbody></table>`
      : `<div class="empty">No module folders yet.</div>`}
    </div>
    ${(functions.length || graphlibs.length) ? `<div class="panel-body" style="border-top:1px solid var(--border-soft)">
      ${functions.length ? `<div class="dim" style="font-size:12px;margin-bottom:6px">
        Shared functions (callable from modules via <code>ctx.fn()</code>, and from node graphs)</div>
        <div class="row">${functions.map((f) => `<span class="pill tag" title="${esc(f.description || "")}">${esc(f.name)}</span>`).join("")}</div>` : ""}
      ${graphlibs.length ? `<div class="dim" style="font-size:12px;margin:10px 0 6px">
        Pipeline-wide graph libraries</div>
        <div class="row">${graphlibs.map((g) => `<span class="pill tag mono">${esc(g)}</span>`).join("")}</div>` : ""}
    </div>` : ""}
  </div>`;
}

/* ---------------------------------------------------------- entity browser */

const ENTITY_PAGE_SIZES = [50, 100, 250, 500];

function entityState() {
  if (!state.entities) {
    state.entities = { search: "", status: "", module: "", order: "entity_id", limit: 100, offset: 0 };
  }
  return state.entities;
}

async function renderEntities(id, body) {
  const q = entityState();
  const params = new URLSearchParams({
    limit: String(q.limit),
    offset: String(q.offset),
    order: q.order,
  });
  if (q.search) params.set("search", q.search);
  if (q.status) params.set("status", q.status);
  if (q.module) params.set("module", q.module);

  const data = await api.get(`/api/pipelines/${encodeURIComponent(id)}/entities?${params}`);
  const modules = (state.detail.pipeline?.definition?.modules || []).map((m) => m.name);
  const statuses = Object.keys(data.statuses || {}).sort();
  const from = data.matching ? q.offset + 1 : 0;
  const to = Math.min(q.offset + q.limit, data.matching);
  const filtered = q.search || q.status || q.module;

  body.innerHTML = `
    <div class="panel">
      <div class="panel-head">
        <span>Entities</span>
        <div class="spacer"></div>
        <div class="toolbar">
          <input type="search" id="ent-search" placeholder="Search id or label…"
                 value="${esc(q.search)}" style="width:220px" />
          <select id="ent-module">
            <option value="">all modules</option>
            ${modules.map((m) => `<option value="${esc(m)}" ${q.module === m ? "selected" : ""}>${esc(m)}</option>`).join("")}
          </select>
          <select id="ent-status">
            <option value="">any state</option>
            ${statuses.map((s) => `<option value="${esc(s)}" ${q.status === s ? "selected" : ""}>${esc(s)} (${data.statuses[s]})</option>`).join("")}
          </select>
          <select id="ent-order">
            ${[["entity_id", "by id"], ["recent", "recently seen"], ["revision", "newest revision"], ["discovered", "newest first seen"]]
              .map(([v, label]) => `<option value="${v}" ${q.order === v ? "selected" : ""}>${label}</option>`).join("")}
          </select>
          <button class="btn sm danger" id="ent-reset">Reset state…</button>
        </div>
      </div>
      <div class="panel-body flush table-wrap">
        ${data.entities.length ? `
        <table><thead><tr>
          <th>Entity</th><th>Label</th><th>Source revision</th>
          ${modules.map((m) => `<th>${esc(m)}</th>`).join("")}
        </tr></thead><tbody>
        ${data.entities.map((e) => {
          const byMod = Object.fromEntries((e.modules || []).map((m) => [m.module, m]));
          return `<tr class="clickable" data-entity="${esc(e.entity_id)}">
            <td class="mono">${esc(e.entity_id)}</td>
            <td class="dim">${esc(e.label || "")}</td>
            <td class="mono dim">${esc(e.source_updated_at || "–")}</td>
            ${modules.map((name) => `<td>${entityCell(byMod[name], e)}</td>`).join("")}
          </tr>`;
        }).join("")}
        </tbody></table>` : `<div class="empty">${filtered ? "No entity matches these filters." : "No entities yet — run the pipeline to discover them."}</div>`}
      </div>
      <div class="panel-body" style="border-top:1px solid var(--border-soft)">
        <div class="toolbar">
          <span class="pager">
            ${fmtNumber(from)}–${fmtNumber(to)} of ${fmtNumber(data.matching)}
            ${filtered ? `matching · ${fmtNumber(data.total)} total` : "entities"}
          </span>
          <div class="spacer"></div>
          <select id="ent-size">
            ${ENTITY_PAGE_SIZES.map((n) => `<option value="${n}" ${q.limit === n ? "selected" : ""}>${n} per page</option>`).join("")}
          </select>
          <button class="btn sm" id="ent-first" ${q.offset ? "" : "disabled"}>« First</button>
          <button class="btn sm" id="ent-prev" ${q.offset ? "" : "disabled"}>‹ Prev</button>
          <button class="btn sm" id="ent-next" ${to < data.matching ? "" : "disabled"}>Next ›</button>
        </div>
      </div>
    </div>`;

  const rerender = () => renderEntities(id, body);
  const search = $("#ent-search");
  let timer = null;
  search.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      q.search = search.value.trim();
      q.offset = 0;
      rerender();
    }, 250);
  });
  $("#ent-module").addEventListener("change", (ev) => { q.module = ev.target.value; q.offset = 0; rerender(); });
  $("#ent-status").addEventListener("change", (ev) => { q.status = ev.target.value; q.offset = 0; rerender(); });
  $("#ent-order").addEventListener("change", (ev) => { q.order = ev.target.value; q.offset = 0; rerender(); });
  $("#ent-size").addEventListener("change", (ev) => { q.limit = Number(ev.target.value); q.offset = 0; rerender(); });
  $("#ent-first").addEventListener("click", () => { q.offset = 0; rerender(); });
  $("#ent-prev").addEventListener("click", () => { q.offset = Math.max(0, q.offset - q.limit); rerender(); });
  $("#ent-next").addEventListener("click", () => { q.offset += q.limit; rerender(); });
  $("#ent-reset").addEventListener("click", () => openResetState(id));
  $$("[data-entity]", body).forEach((el) => {
    const entityId = el.dataset.entity;
    el.addEventListener("click", () => openEntityDrawer(id, entityId, rerender));
    el.addEventListener("contextmenu", (ev) =>
      contextMenu(ev, [
        { label: "▤ Open details", onClick: () => openEntityDrawer(id, entityId, rerender) },
        { label: "⧉ Copy entity id", onClick: () => copyText(entityId) },
        {
          label: "⌕ Show only this entity",
          onClick: () => {
            q.search = entityId;
            q.offset = 0;
            rerender();
          },
        },
        null,
        {
          label: "↺ Reset all state for it",
          danger: true,
          onClick: async () => {
            await resetEntity(id, entityId, null);
            rerender();
          },
        },
      ]),
    );
  });
  if (state.focusEntitySearch) {
    state.focusEntitySearch = false;
    search.focus();
    search.select();
  }
}

function entityCell(state_, entity) {
  if (!state_) return `<span class="pill tag">–</span>`;
  const cls = { done: "succeeded", failed: "failed", running: "running", pending: "queued", skipped: "stopped" }[state_.status] || "tag";
  const stale =
    state_.processed_source_updated_at && entity.source_updated_at &&
    state_.processed_source_updated_at < entity.source_updated_at;
  const title = state_.error || `v${state_.module_version} · processed ${state_.processed_source_updated_at || "–"}`;
  return `<span title="${esc(title)}">${statusPill(cls, stale ? "stale" : state_.status)}</span>`;
}

/* --------------------------------------------------------- entity details */

async function openEntityDrawer(pipelineId, entityId, onChange) {
  const host = document.createElement("div");
  host.className = "drawer-backdrop";
  host.innerHTML = `<div class="drawer"><div class="drawer-head">
      <h2 class="mono">${esc(entityId)}</h2><div class="spacer"></div>
      <button class="btn sm" data-close>Close</button>
    </div><div class="drawer-body"><div class="empty">Loading…</div></div></div>`;
  document.body.appendChild(host);
  const close = () => host.remove();
  host.addEventListener("click", (ev) => {
    if (ev.target === host || ev.target.hasAttribute("data-close")) close();
  });

  const paint = async () => {
    const entity = await api.get(
      `/api/pipelines/${encodeURIComponent(pipelineId)}/entities/${encodeURIComponent(entityId)}`,
    );
    const filter = (state.entityFilter || "").toLowerCase();
    const defs = Object.fromEntries(
      (state.detail.pipeline?.definition?.modules || []).map((m) => [m.name, m]),
    );
    const rows = (entity.modules || []).filter(
      (m) => !filter || `${m.module} ${m.status} ${m.error || ""}`.toLowerCase().includes(filter),
    );

    $(".drawer-body", host).innerHTML = `
      <dl class="kv" style="margin-bottom:16px">
        <dt>Label</dt><dd>${esc(entity.label || "–")}</dd>
        <dt>Source revision</dt><dd>${esc(entity.source_updated_at || "–")}</dd>
        <dt>First seen</dt><dd>${esc(fmtTime(entity.discovered_at))}</dd>
        <dt>Last seen</dt><dd>${esc(fmtTime(entity.last_seen_at))}</dd>
        ${Object.keys(entity.payload || {}).length
          ? `<dt>Payload</dt><dd>${esc(JSON.stringify(entity.payload))}</dd>` : ""}
      </dl>

      <div class="row" style="margin-bottom:10px">
        <input type="search" id="ent-mod-filter" placeholder="Filter modules…"
               value="${esc(state.entityFilter || "")}" style="flex:1" />
        <button class="btn sm danger" id="ent-drawer-reset">Reset all state</button>
      </div>

      <div class="table-wrap">
        ${rows.length ? `<table><thead><tr>
          <th>Module</th><th></th><th class="num">Layer</th><th>State</th><th class="num">v</th>
          <th>Processed revision</th><th>Processed at</th><th class="num">Try</th>
          <th class="num">ms</th>
        </tr></thead><tbody>
        ${rows.map((m) => {
          const cls = { done: "succeeded", failed: "failed", running: "running", pending: "queued", skipped: "stopped" }[m.status] || "tag";
          const stale = m.processed_source_updated_at && entity.source_updated_at &&
            m.processed_source_updated_at < entity.source_updated_at;
          return `<tr class="row-entity-module" data-module="${esc(m.module)}"
              data-file="${esc(defs[m.module]?.file || "")}"
              title="Double-click to edit ${esc(defs[m.module]?.file || m.module)}">
            <td class="mono">${esc(m.module)}</td>
            <td><div class="btn-group">
              <button class="btn sm" data-debug="${esc(m.module)}" title="Debug run this module for this entity">🐞</button>
              <button class="btn sm" data-reset="${esc(m.module)}" title="Forget this module's state for this entity">↺</button>
            </div></td>
            <td class="num dim">${defs[m.module]?.execution_layer ?? "–"}</td>
            <td>${statusPill(cls, m.status)}${stale ? ' <span class="pill stopped">stale</span>' : ""}</td>
            <td class="num">${m.module_version}</td>
            <td class="mono dim">${esc(m.processed_source_updated_at || "–")}</td>
            <td class="dim">${esc(fmtTime(m.processed_at))}</td>
            <td class="num">${m.attempts}</td>
            <td class="num dim">${m.duration_ms ?? "–"}</td>
          </tr>`;
        }).join("")}
        </tbody></table>` : `<div class="empty">No module has touched this entity yet.</div>`}
      </div>

      ${rows.filter((m) => m.error).map((m) => `<div class="banner" style="margin-top:14px">
        <strong>${esc(m.module)}</strong>\n${esc(m.error)}</div>`).join("")}

      ${rows.filter((m) => m.result).map((m) => `<div class="panel" style="margin-top:14px">
        <div class="panel-head">${esc(m.module)} result</div>
        <div class="panel-body mono" style="font-size:12px;white-space:pre-wrap">${esc(JSON.stringify(m.result, null, 2))}</div>
      </div>`).join("")}`;

    const filterInput = $("#ent-mod-filter", host);
    filterInput.addEventListener("input", () => {
      state.entityFilter = filterInput.value;
      const at = filterInput.selectionStart;
      paint().then(() => {
        const next = $("#ent-mod-filter", host);
        if (next) { next.focus(); next.setSelectionRange(at, at); }
      });
    });
    $$("[data-debug]", host).forEach((el) =>
      el.addEventListener("click", () =>
        startRun(pipelineId, {
          mode: "full", steps: [el.dataset.debug], entity_ids: [entityId], debug: true,
        }),
      ),
    );
    $$("[data-reset]", host).forEach((el) =>
      el.addEventListener("click", async () => {
        await resetEntity(pipelineId, entityId, el.dataset.reset);
        await paint();
        if (onChange) onChange();
      }),
    );
    $$("tr.row-entity-module", host).forEach((row) => {
      const module = row.dataset.module;
      const open = () => {
        host.remove();
        openSource(pipelineId, row.dataset.file, module);
      };
      row.addEventListener("dblclick", (ev) => {
        if (ev.target.closest("button")) return;
        open();
      });
      row.addEventListener("contextmenu", (ev) =>
        contextMenu(ev, [
          {
            label: "🐞 Debug run for this entity",
            onClick: () =>
              startRun(pipelineId, {
                mode: "full", steps: [module], entity_ids: [entityId], debug: true,
              }),
          },
          { label: "✎ Edit source", hint: "double-click", disabled: !row.dataset.file, onClick: open },
          { label: "⧉ Copy module name", onClick: () => copyText(module) },
          null,
          {
            label: "↺ Forget this module's state here",
            danger: true,
            onClick: async () => {
              await resetEntity(pipelineId, entityId, module);
              await paint();
              if (onChange) onChange();
            },
          },
        ]),
      );
    });
    $("#ent-drawer-reset", host).addEventListener("click", async () => {
      await resetEntity(pipelineId, entityId, null);
      await paint();
      if (onChange) onChange();
    });
  };

  try {
    await paint();
  } catch (err) {
    $(".drawer-body", host).innerHTML = `<div class="banner">${esc(err.message)}</div>`;
  }
}

async function resetEntity(pipelineId, entityId, module) {
  try {
    const res = await api.post(
      `/api/pipelines/${encodeURIComponent(pipelineId)}/entities/${encodeURIComponent(entityId)}/reset`,
      { module },
    );
    toast(`Reset ${res.reset} state row(s)`, "success");
  } catch (err) {
    toast(err.message, "error");
  }
}

/* ------------------------------------------------------- per-module runs */

function openModuleRun(pipelineId, moduleName, kind) {
  const isProfile = kind === "profile";
  openModal(
    isProfile ? `Profile run — ${moduleName}` : `Debug run — ${moduleName}`,
    `<div class="dim">${isProfile
        ? "Runs a sample of entities with cProfile enabled. The profile is written to the pipeline's profiles/ folder and its top functions appear in the console."
        : "Runs this module for a single entity with ctx.debug() output switched on. Entities are reprocessed even when they are already up to date."}</div>
     <label class="field">Entity
       <input type="text" id="mr-entity" placeholder="leave empty to pick automatically" /></label>
     ${isProfile ? `<label class="field">How many entities
       <input type="number" id="mr-count" value="25" min="1" max="10000" /></label>` : ""}
     <label class="field">Selection
       <select id="mr-sample">
         <option value="first">first pending</option>
         <option value="random" selected>random</option>
       </select></label>`,
    async () => {
      const entity = $("#mr-entity").value.trim();
      const body = {
        mode: "full",
        steps: [moduleName],
        sample: $("#mr-sample").value,
        debug: !isProfile,
        profile: isProfile,
      };
      if (entity) body.entity_ids = [entity];
      else body.limit_entities = isProfile ? Number($("#mr-count").value || 25) : 1;
      await startRun(pipelineId, body);
    },
    isProfile ? "Profile" : "Debug run",
  );
}

/* ------------------------------------------------------- resource panel */

function sparkline(values, color, max) {
  if (!values.length) return "";
  const top = max || Math.max(...values, 1);
  const step = 100 / Math.max(values.length - 1, 1);
  const points = values
    .map((v, i) => `${(i * step).toFixed(2)},${(100 - (v / top) * 100).toFixed(2)}`)
    .join(" ");
  return `<svg class="spark" viewBox="0 0 100 100" preserveAspectRatio="none">
      <polyline points="${points}" fill="none" stroke="${color}" stroke-width="2"
                vector-effect="non-scaling-stroke" />
    </svg>`;
}

function renderResources() {
  const el = $("#run-resources");
  if (!el) return;
  const samples = state.console.resources || [];
  const last = samples[samples.length - 1] || state.detail.run?.stats || {};
  if (!samples.length && !Object.keys(last).length) {
    el.innerHTML = `<div class="empty">No telemetry for this run.</div>`;
    return;
  }
  const rss = samples.map((s) => s.rss_mb || 0);
  const cpu = samples.map((s) => s.cpu_percent || 0);
  el.innerHTML = `
    <div class="res-grid">
      <div class="res-item"><div class="res-label">Memory</div>
        <div class="res-value">${last.rss_mb ?? "–"}<span class="dim" style="font-size:11px"> MB</span></div>
        ${sparkline(rss, "var(--accent)")}</div>
      <div class="res-item"><div class="res-label">CPU</div>
        <div class="res-value">${last.cpu_percent ?? "–"}<span class="dim" style="font-size:11px"> %</span></div>
        ${sparkline(cpu, "var(--run)", 100)}</div>
      <div class="res-item"><div class="res-label">Peak memory</div>
        <div class="res-value">${last.peak_rss_mb ?? "–"}<span class="dim" style="font-size:11px"> MB</span></div></div>
      <div class="res-item"><div class="res-label">Threads</div>
        <div class="res-value">${last.threads ?? "–"}</div></div>
      <div class="res-item"><div class="res-label">Entities/s</div>
        <div class="res-value">${last.entities_per_s ?? "–"}</div></div>
      <div class="res-item"><div class="res-label">Parallel</div>
        <div class="res-value">${last.parallel ?? 1}</div></div>
      <div class="res-item"><div class="res-label">PID</div>
        <div class="res-value" style="font-size:14px">${last.pid ?? state.detail.run?.pid ?? "–"}</div></div>
      <div class="res-item"><div class="res-label">Source</div>
        <div class="res-value" style="font-size:14px">${esc(last.backend || "–")}</div></div>
    </div>`;
}

/* ------------------------------------------------------------------ files */

const isGraph = (path) => /\.(graph|graphlib)$/.test(path || "");

/* A graph is a document of its own: the canvas is the editor, and the Python
   beside it is the build output, shown read-only so you can check what the
   graph actually compiled to. */
async function renderGraphPanel(id, path, host) {
  host.innerHTML = `<div class="empty">Loading graph…</div>`;
  let data;
  try {
    data = await api.get(
      `/api/pipelines/${encodeURIComponent(id)}/graph?path=${encodeURIComponent(path)}`,
    );
  } catch (err) {
    host.innerHTML = `<div class="banner">${esc(err.message)}</div>`;
    return;
  }
  // The palette needs every node this pipeline can offer. A pipeline.py that
  // does not import is a real possibility; the canvas still has to open.
  let catalog = [];
  try {
    catalog = (await api.get(`/api/pipelines/${encodeURIComponent(id)}/nodes`)).nodes || [];
  } catch (err) {
    toast(`Node palette unavailable: ${err.message}`, "error");
  }

  // A .graph is one module graph; a .graphlib is a library of functions, and
  // the whole library is held here so switching between its functions is a
  // repaint rather than a round trip.
  const library = data.kind === "library" ? data.library : null;
  const ctx = state.graph = {
    id, path, host, library,
    resolved: data.resolved || {},
    dirty: false,
    current: library ? (library.functions[0]?.name || "") : null,
  };

  const editable = !state.detail.pipeline?.active_run;
  host.innerHTML = `
    <div class="tabs" style="margin-bottom:10px">
      <button class="tab active" data-gtab="canvas">Canvas</button>
      <button class="tab" data-gtab="python">Generated Python</button>
      <div class="spacer"></div>
      <span class="dim" style="font-size:12px;align-self:center">
        compiles to <span class="mono">${esc(data.output || "")}</span></span>
    </div>
    ${library ? `<div class="fn-tabs" id="fn-tabs"></div>` : ""}
    <div id="graph-canvas"></div>
    <div id="graph-python" hidden></div>
    <div class="panel gv-run-panel" id="graph-run" style="margin-top:12px" hidden>
      <div class="panel-head">Test run
        <span class="dim" id="graph-run-state"></span>
        <div class="spacer"></div>
        <button class="btn sm" id="graph-run-stop" hidden>Stop</button>
        <button class="icon-btn" id="graph-run-close" title="Close">✕</button>
      </div>
      <div class="console sm" id="graph-run-out"></div>
    </div>
    <div id="graph-inspect" class="panel" style="margin-top:12px" hidden></div>`;

  const selected = () => (library ? library.functions.find((f) => f.name === ctx.current) : data.graph);
  const resolvedFor = (graph) =>
    library ? (ctx.resolved[graph.name] || { definitions: {}, problems: [] })
            : { definitions: data.definitions, problems: data.problems };

  const first = selected();
  const view = createGraphView($("#graph-canvas", host), {
    editable,
    graph: first,
    definitions: resolvedFor(first).definitions,
    problems: resolvedFor(first).problems,
    catalog,
    describe: (op, config) => api.get(
      `/api/pipelines/${encodeURIComponent(id)}/nodes?op=${encodeURIComponent(op)}`
      + `&config=${encodeURIComponent(JSON.stringify(config || {}))}`,
    ),
    suggest: async (type, dir) => (await api.get(
      `/api/pipelines/${encodeURIComponent(id)}/nodes?suggest=${encodeURIComponent(type)}`
      + `&dir=${encodeURIComponent(dir)}`,
    )).nodes,
    browse: (initial, place) => importBrowser(id, initial, place),
    onMenu: (event, items) => contextMenu(event, items),
    onSelect: (node, definition) => showNodeDetails(host, node, definition),
    onOpen: (node) => openDefinition(id, node),
    onConfigure: (node, definition, apply) => configureNode(node, definition, apply),
    onChange: () => {
      ctx.dirty = true;
      $("#graph-python", host).dataset.loaded = "";
      paintDirty(true);
    },
    // The file is the unit that is saved, whichever function is on screen -
    // and `ctx.library` is re-read on every save, so it must be looked up now,
    // not captured.
    onSave: () => saveGraph(id, path, ctx.library || data.graph, host),
    onRun: !library
      ? (opts) => (opts.options
          ? testRunOptions(id, path, host, opts.event)
          : testRun(id, path, host, state.graphTest || {}))
      : null,
  });
  ctx.view = view;
  state.graphView = view;
  if (library) renderFunctionTabs();

  $("#graph-run-close", host).addEventListener("click", () => {
    $("#graph-run", host).hidden = true;
  });
  $("#graph-run-stop", host).addEventListener("click", async () => {
    if (state.graphRun) await api.post(`/api/runs/${state.graphRun}/stop`).catch(() => {});
  });
  if ((data.problems || []).length) {
    toast(`${data.problems.length} node(s) in this graph could not be resolved`, "error");
  }

  $$("[data-gtab]", host).forEach((el) => el.addEventListener("click", async () => {
    $$("[data-gtab]", host).forEach((o) => o.classList.toggle("active", o === el));
    const python = el.dataset.gtab === "python";
    $("#graph-canvas", host).hidden = python;
    $("#fn-tabs", host)?.toggleAttribute("hidden", python);
    $("#graph-python", host).hidden = !python;
    if (python && !$("#graph-python", host).dataset.loaded) {
      const target = $("#graph-python", host);
      target.dataset.loaded = "1";
      target.innerHTML = `<div class="empty">Compiling…</div>`;
      // Unsaved edits are not on disk yet, so preview what is in the editor.
      const pending = ctx.dirty || view.dirty;
      const res = pending
        ? await api.post(
            `/api/pipelines/${encodeURIComponent(id)}/graph/preview?path=${encodeURIComponent(path)}`,
            { graph: ctx.library || view.graph },
          )
        : await api.get(
            `/api/pipelines/${encodeURIComponent(id)}/graph/preview?path=${encodeURIComponent(path)}`,
          );
      target.innerHTML = res.ok
        ? `${pending ? `<div class="banner info">Preview of unsaved edits.</div>` : ""}
           ${(res.warnings || []).map((w) => `<div class="banner info">${esc(w)}</div>`).join("")}
           <pre class="gv-python mono">${esc(res.source)}</pre>`
        : `<div class="banner">${esc(res.error)}</div>`;
    } else if (!python && state.graphView) {
      state.graphView.refresh();
    }
  }));
}

/* ------------------------------------------- a library's function tabs
 *
 * A .graphlib holds one or more functions. They are independent graphs sharing
 * a file, so the tab strip switches which one the canvas shows; the file saves
 * as a whole, and the dirty flag belongs to the file, not the tab. */

function renderFunctionTabs() {
  const ctx = state.graph;
  const bar = $("#fn-tabs", ctx.host);
  if (!bar || !ctx.library) return;
  bar.innerHTML = `
    ${ctx.library.functions.map((fn) => `<button class="fn-tab${
        fn.name === ctx.current ? " active" : ""}" data-fn="${esc(fn.name)}"
        title="${esc(fn.description || fn.title || fn.name)}">
        ${esc(fn.title || fn.name)}
        ${fn.cache ? `<span class="fn-tag" title="cached">⚡</span>` : ""}
        ${fn.pure ? `<span class="fn-tag" title="pure">ƒ</span>` : ""}
      </button>`).join("")}
    <button class="fn-tab add" id="fn-add" title="Add a function to this library">＋</button>`;

  $$("[data-fn]", bar).forEach((el) => {
    el.addEventListener("click", () => selectFunction(el.dataset.fn));
    el.addEventListener("contextmenu", (ev) => functionMenu(ev, el.dataset.fn));
  });
  $("#fn-add", bar).addEventListener("click", () => addFunction());
}

function selectFunction(name) {
  const ctx = state.graph;
  if (!ctx?.library || name === ctx.current) return;
  ctx.current = name;
  const graph = ctx.library.functions.find((f) => f.name === name);
  const resolved = ctx.resolved[name] || { definitions: {}, problems: [] };
  // The view mutates the function object in place, so nothing is lost here -
  // but the dirty flag belongs to the file and has to survive the switch.
  ctx.view.load({ graph, definitions: resolved.definitions, problems: resolved.problems },
                { dirty: ctx.dirty });
  renderFunctionTabs();
}

function functionMenu(ev, name) {
  const ctx = state.graph;
  const fn = ctx.library.functions.find((f) => f.name === name);
  const only = ctx.library.functions.length === 1;
  contextMenu(ev, [
    { label: "✏ Rename…", onClick: () => renameFunction(name) },
    { label: "⚙ Signature…", hint: "inputs, outputs, cache",
      onClick: () => editFunction(name) },
    { label: `${fn.cache ? "☑" : "☐"} Cache results`,
      title: "Call it once per distinct set of arguments, per run",
      onClick: () => {
        if (fn.cache) { delete fn.cache; delete fn.cache_size; }
        else fn.cache = true;
        markLibraryDirty();
      } },
    { label: "⧉ Duplicate", onClick: () => duplicateFunction(name) },
    null,
    { label: "🗑 Delete", danger: true, disabled: only,
      title: only ? "A library needs at least one function" : "",
      onClick: () => deleteFunction(name) },
  ]);
}

function markLibraryDirty() {
  const ctx = state.graph;
  ctx.dirty = true;
  // The canvas owns the Save button, and this edit was not made on the canvas.
  ctx.view?.touch();
  paintDirty(true);
  $("#graph-python", ctx.host).dataset.loaded = "";
  renderFunctionTabs();
}

/** Function names are the pipeline's namespace, so they must be identifiers. */
function validFunctionName(raw, ctx, { except = null } = {}) {
  const name = String(raw || "").trim();
  if (!name) throw new Error("a name is required");
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new Error("a function name must be a Python identifier");
  }
  if (ctx.library.functions.some((f) => f.name === name && f.name !== except)) {
    throw new Error(`${name} is already in this library`);
  }
  return name;
}

function addFunction() {
  const ctx = state.graph;
  openModal("New function", `
    <div class="dim">A callable graph in <span class="mono">${esc(ctx.library.name)}</span>.
      It registers as a pipeline function, so every module and every other graph
      can use it.</div>
    <label class="field">Function name<input type="text" id="fn-name" placeholder="unit_of" /></label>
    <label class="field">Description<input type="text" id="fn-desc" /></label>`, async () => {
    const name = validFunctionName($("#fn-name").value, ctx);
    ctx.library.functions.push({
      kind: "function", name,
      title: name.replace(/_/g, " ").replace(/^./, (c) => c.toUpperCase()),
      description: $("#fn-desc").value.trim(),
      inputs: [], outputs: [], pure: true,
      nodes: [
        { id: "entry", op: "core:entry", pos: [80, 200] },
        { id: "return", op: "core:return", pos: [560, 200] },
      ],
      links: [],
    });
    ctx.resolved[name] = { definitions: {}, problems: [] };
    ctx.current = name;
    markLibraryDirty();
    selectFunction(name);
    // A new function has no resolved shapes until the server has seen it.
    await saveGraph(ctx.id, ctx.path, ctx.library, ctx.host);
  }, "Create");
}

function renameFunction(name) {
  const ctx = state.graph;
  const fn = ctx.library.functions.find((f) => f.name === name);
  openModal(`Rename ${name}`, `
    <div class="dim">Graphs calling this function by name will need rewiring.</div>
    <label class="field">Function name<input type="text" id="fn-name" value="${esc(name)}" /></label>`,
    async () => {
      const next = validFunctionName($("#fn-name").value, ctx, { except: name });
      fn.name = next;
      ctx.resolved[next] = ctx.resolved[name] || { definitions: {}, problems: [] };
      delete ctx.resolved[name];
      if (ctx.current === name) ctx.current = next;
      markLibraryDirty();
      await saveGraph(ctx.id, ctx.path, ctx.library, ctx.host);
    }, "Rename");
}

function duplicateFunction(name) {
  const ctx = state.graph;
  const fn = ctx.library.functions.find((f) => f.name === name);
  let copy = `${name}_copy`;
  let n = 2;
  while (ctx.library.functions.some((f) => f.name === copy)) copy = `${name}_copy${n++}`;
  const clone = JSON.parse(JSON.stringify(fn));
  clone.name = copy;
  clone.title = "";
  ctx.library.functions.push(clone);
  ctx.resolved[copy] = JSON.parse(JSON.stringify(ctx.resolved[name] || {}));
  ctx.current = copy;
  markLibraryDirty();
  selectFunction(copy);
}

async function deleteFunction(name) {
  const ctx = state.graph;
  if (ctx.library.functions.length === 1) return;
  if (!confirm(`Delete the function ${name}? Graphs that call it will stop compiling.`)) return;
  ctx.library.functions = ctx.library.functions.filter((f) => f.name !== name);
  delete ctx.resolved[name];
  if (ctx.current === name) ctx.current = ctx.library.functions[0].name;
  markLibraryDirty();
  selectFunction(ctx.current);
  await saveGraph(ctx.id, ctx.path, ctx.library, ctx.host);
}

/** A function graph's own signature: what it takes, what it returns, and
 *  whether its result is worth keeping. */
function editFunction(name) {
  const ctx = state.graph;
  const fn = ctx.library.functions.find((f) => f.name === name);
  const pins = (list) => (list || []).map((p) => p.name + (p.type && p.type !== "Any" ? `: ${p.type}` : "")).join(", ");
  openModal(`${name} — signature`, `
    <label class="field">Title<input type="text" id="fs-title" value="${esc(fn.title || "")}" /></label>
    <label class="field">Description<input type="text" id="fs-desc" value="${esc(fn.description || "")}" /></label>
    <label class="field">Inputs <span class="dim">name, or name: type — comma separated</span>
      <input type="text" id="fs-in" value="${esc(pins(fn.inputs))}" /></label>
    <label class="field">Outputs
      <input type="text" id="fs-out" value="${esc(pins(fn.outputs))}" /></label>
    <label class="field">Category<input type="text" id="fs-cat" value="${esc(fn.category || "")}" /></label>
    <label class="row" style="gap:7px"><input type="checkbox" id="fs-pure" ${fn.pure ? "checked" : ""} />
      <span>Pure — no side effects, so it needs no execution pins</span></label>
    <label class="row" style="gap:7px"><input type="checkbox" id="fs-cache" ${fn.cache ? "checked" : ""} />
      <span>Cache results — call it once per distinct set of arguments</span></label>
    <label class="field">Cache size <span class="dim">entries; blank uses the configured default</span>
      <input type="number" id="fs-size" min="1" value="${fn.cache_size || ""}" /></label>`,
    async () => {
      fn.title = $("#fs-title").value.trim();
      fn.description = $("#fs-desc").value.trim();
      fn.category = $("#fs-cat").value.trim();
      fn.inputs = parsePins($("#fs-in").value);
      fn.outputs = parsePins($("#fs-out").value);
      fn.pure = $("#fs-pure").checked;
      if ($("#fs-cache").checked) {
        fn.cache = true;
        const size = Number($("#fs-size").value);
        if (size > 0) fn.cache_size = size; else delete fn.cache_size;
      } else {
        delete fn.cache;
        delete fn.cache_size;
      }
      markLibraryDirty();
      await saveGraph(ctx.id, ctx.path, ctx.library, ctx.host);
    }, "Apply");
}

function parsePins(text) {
  return String(text || "").split(",").map((part) => part.trim()).filter(Boolean)
    .map((part) => {
      const [name, type] = part.split(":").map((s) => s.trim());
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
        throw new Error(`${name || "(blank)"} is not a valid parameter name`);
      }
      return type ? { name, type } : { name };
    });
}

/** Save the document, then report what the recompile made of it.
 *  For a library that is the whole file, whichever function is on screen. */
async function saveGraph(id, path, document, host) {
  const ctx = state.graph;
  try {
    const res = await api.put(
      `/api/pipelines/${encodeURIComponent(id)}/graph?path=${encodeURIComponent(path)}`,
      { graph: document, compile: true },
    );
    // Re-read: the server normalises the document and re-resolves every node,
    // so a pin that stopped existing shows up here rather than at run time.
    const fresh = await api.get(
      `/api/pipelines/${encodeURIComponent(id)}/graph?path=${encodeURIComponent(path)}`,
    );
    if (fresh.kind === "library" && ctx) {
      ctx.library = fresh.library;
      ctx.resolved = fresh.resolved || {};
      ctx.dirty = false;
      if (!ctx.library.functions.some((f) => f.name === ctx.current)) {
        ctx.current = ctx.library.functions[0]?.name || "";
      }
      const graph = ctx.library.functions.find((f) => f.name === ctx.current);
      const resolved = ctx.resolved[ctx.current] || { definitions: {}, problems: [] };
      ctx.view.saved({ graph, definitions: resolved.definitions, problems: resolved.problems });
      renderFunctionTabs();
    } else {
      state.graphView?.saved(fresh);
      if (ctx) ctx.dirty = false;
    }
    $("#graph-python", host).dataset.loaded = "";
    paintDirty(false);
    reportBuild(res.build);
    const clean = res.build ? res.build.ok !== false : true;
    if (clean) toast("Graph saved and compiled");
    if (res.pipeline) state.detail.pipeline = res.pipeline;
    return clean;
  } catch (err) {
    toast(err.message, "error");
    return false;
  }
}

/* ------------------------------------------------------- the import browser
 *
 * Reflection is powerful and unguessable: `py:pandas.read_csv` works, but only
 * once you know it exists. This dialog asks the server what is inside a module
 * and shows it - functions with their signatures, classes, submodules to step
 * into - so importing something is looking rather than remembering.
 *
 * Picking a class also offers its methods, because `df.to_csv(...)` is a call
 * on a value and no module-level name will ever reach it. */

function importBrowser(pipelineId, initial, place) {
  const root = document.createElement("div");
  root.className = "modal-backdrop";
  root.innerHTML = `
    <div class="modal wide">
      <div class="modal-head">Import a Python module</div>
      <div class="modal-body imp">
        <div class="imp-bar">
          <input type="text" id="imp-module" class="mono" placeholder="pandas"
                 value="${esc(initial || "")}" />
          <button class="btn sm primary" id="imp-go">Open</button>
        </div>
        <div class="imp-crumbs" id="imp-crumbs"></div>
        <input type="search" id="imp-filter" placeholder="Filter…" hidden />
        <div class="imp-list" id="imp-list">
          <div class="empty">Type a module name — <span class="mono">pandas</span>,
            <span class="mono">pathlib</span>, <span class="mono">datetime</span> — and press Open.</div>
        </div>
      </div>
      <div class="modal-foot">
        <span class="dim" id="imp-doc"></span>
        <div class="spacer"></div>
        <button class="btn ghost" data-close>Close</button>
      </div>
    </div>`;
  document.body.appendChild(root);
  const close = () => root.remove();
  root.addEventListener("click", (e) => {
    if (e.target === root || e.target.hasAttribute("data-close")) close();
  });
  root.addEventListener("keydown", (e) => { if (e.key === "Escape") close(); });

  const list = $("#imp-list", root);
  const filter = $("#imp-filter", root);
  const crumbs = $("#imp-crumbs", root);
  let entries = [];       // what is currently listed
  let trail = [];         // the modules/types stepped through

  const paint = () => {
    const needle = filter.value.trim().toLowerCase();
    // Rank by how well the *name* matches: a doc that happens to mention
    // read_csv must not outrank read_csv itself.
    const rank = (entry) => {
      const name = entry.name.toLowerCase();
      if (name === needle) return 0;
      if (name.startsWith(needle)) return 1;
      if (name.includes(needle)) return 2;
      return 3;
    };
    const shown = entries
      .filter((e) => !needle || `${e.name} ${e.doc || ""}`.toLowerCase().includes(needle))
      .map((e, i) => [needle ? rank(e) : 0, i, e])
      .sort((a, b) => a[0] - b[0] || a[1] - b[1])
      .map(([, , e]) => e);
    crumbs.innerHTML = trail
      .map((step, i) => `<button class="imp-crumb" data-crumb="${i}">${esc(step.label)}</button>`)
      .join('<span class="dim">›</span>');
    list.innerHTML = shown.length
      ? shown.map((entry, index) => `
          <button class="imp-row" data-index="${index}">
            <span class="imp-kind ${esc(entry.kind)}">${
              { function: "ƒ", class: "C", method: "·", module: "▸" }[entry.kind] || "·"}</span>
            <span class="imp-name mono">${esc(entry.name)}</span>
            <span class="imp-sig mono dim">${esc(entry.signature || "")}</span>
            <span class="imp-doc dim">${esc(entry.doc || "")}</span>
          </button>`).join("")
      : `<div class="empty">Nothing matches.</div>`;
    $$("[data-index]", list).forEach((el) =>
      el.addEventListener("click", () => pick(shown[Number(el.dataset.index)])));
    $$("[data-crumb]", crumbs).forEach((el) =>
      el.addEventListener("click", () => {
        const step = trail[Number(el.dataset.crumb)];
        trail = trail.slice(0, Number(el.dataset.crumb));
        open(step.target, step.methods);
      }));
  };

  const pick = async (entry) => {
    if (!entry) return;
    if (entry.kind === "module") return open(entry.target, false);
    // A class is worth stepping into: its methods are what you call on a value.
    if (entry.kind === "class") {
      return contextMenu(
        { preventDefault() {}, stopPropagation() {},
          clientX: window.innerWidth / 2, clientY: window.innerHeight / 2 },
        [
          { label: `＋ Place ${entry.name}()`, hint: "construct it",
            onClick: () => { close(); place({ op: entry.op }); } },
          { label: `▸ Browse ${entry.name} methods`,
            onClick: () => open(entry.op.replace(/^py:/, ""), true) },
        ]);
    }
    close();
    place(entry.config ? { op: entry.op, config: entry.config } : { op: entry.op });
  };

  const open = async (target, methods) => {
    list.innerHTML = `<div class="empty">Importing ${esc(target)}…</div>`;
    let data;
    try {
      data = await api.get(
        `/api/pipelines/${encodeURIComponent(pipelineId)}/nodes?module=${encodeURIComponent(target)}`
        + (methods ? "&methods=1" : ""),
      );
    } catch (err) {
      list.innerHTML = `<div class="banner">${esc(err.message)}</div>`;
      return;
    }
    trail.push({ label: methods ? `${target} methods` : target, target, methods });
    $("#imp-module", root).value = methods ? target : data.module;
    $("#imp-doc", root).textContent = data.doc || "";
    entries = methods
      ? (data.methods || []).map((m) => ({ ...m, kind: "method" }))
      : [
          ...(data.functions || []),
          ...(data.classes || []),
          ...(data.submodules || []).map((name) => ({
            name: name.split(".").pop(), target: name, kind: "module",
            doc: name, signature: "",
          })),
        ];
    filter.hidden = false;
    filter.value = "";
    paint();
    filter.focus();
  };

  filter.addEventListener("input", paint);
  $("#imp-go", root).addEventListener("click", () => {
    trail = [];
    open($("#imp-module", root).value.trim(), false);
  });
  $("#imp-module", root).addEventListener("keydown", (e) => {
    if (e.key === "Enter") { trail = []; open(e.target.value.trim(), false); }
  });
  if (initial && /^[A-Za-z_][\w.]*$/.test(initial)) open(initial, false);
  else $("#imp-module", root).focus();
}

/* ---------------------------------------------------- the graph test run
 *
 * Drawing a module and then hunting for the Runs tab to see whether it works is
 * two context switches too many. The button saves, runs just this module for
 * one entity with ctx.debug() on, and streams the result under the canvas. */

function testRunOptions(id, path, host, event) {
  const current = state.graphTest || {};
  const pick = (patch, label) => ({
    label, onClick: () => {
      state.graphTest = { ...current, ...patch };
      testRun(id, path, host, state.graphTest);
    },
  });
  contextMenu(event, [
    pick({ limit: 1, sample: "random", entity: null }, "\u25b6 One random entity"),
    pick({ limit: 10, sample: "random", entity: null }, "\u25b6 Ten random entities"),
    pick({ limit: null, sample: "first", entity: null }, "\u25b6 Every entity that needs it"),
    null,
    { label: "\u25b6 A specific entity\u2026", onClick: () => {
      openModal("Test run", `
        <div class="dim">Reprocesses this one entity, whatever its state.</div>
        <label class="field">Entity id<input type="text" id="gr-eid" /></label>`,
        async () => {
          const eid = $("#gr-eid").value.trim();
          if (!eid) throw new Error("an entity id is required");
          state.graphTest = { ...current, entity: eid, limit: null };
          await testRun(id, path, host, state.graphTest);
        }, "Run");
    } },
  ]);
}

async function testRun(id, path, host, opts = {}) {
  const view = state.graphView;
  const module = view?.graph?.name;
  if (!module) return toast("This graph has no module name yet", "error");

  const pane = $("#graph-run", host);
  const out = $("#graph-run-out", host);
  pane.hidden = false;
  out.innerHTML = `<div class="empty">Starting\u2026</div>`;
  setRunState(host, "", false);

  // The runner executes the compiled Python, so unsaved edits would not be in
  // the run. Saving first is the only honest thing to do.
  if (state.graph?.dirty || view?.dirty) {
    setRunState(host, "saving and compiling\u2026", false);
    const ok = await saveGraph(id, path, state.graph?.library || view.graph, host);
    if (!ok) {
      out.innerHTML = `<div class="banner">The graph did not compile; nothing was run.</div>`;
      setRunState(host, "", false);
      return;
    }
  }

  const body = {
    mode: "full",
    steps: [module],
    debug: true,
    sample: opts.sample || "random",
  };
  if (opts.entity) body.entity_ids = [opts.entity];
  else body.limit_entities = opts.limit === null ? null : (opts.limit || 1);
  if (body.limit_entities === null) delete body.limit_entities;

  let run;
  try {
    run = await api.post(`/api/pipelines/${encodeURIComponent(id)}/runs`, body);
  } catch (err) {
    out.innerHTML = `<div class="banner">${esc(err.message)}</div>`;
    return;
  }
  state.graphRun = run.id;
  state.graphRunLines = [];
  setRunState(host, `run #${run.id} \u00b7 ${module}`, true);
  watchTestRun(run.id, host);
}

function setRunState(host, text, running) {
  const label = $("#graph-run-state", host);
  if (label) label.textContent = text;
  const stop = $("#graph-run-stop", host);
  if (stop) stop.hidden = !running;
}

/** Its own socket: the Runs tab may be showing something else entirely. */
function watchTestRun(runId, host) {
  const out = $("#graph-run-out", host);
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const paint = () => {
    out.innerHTML = state.graphRunLines
      .map((l) => `<div class="line ${esc(l.level)}">
          <span class="tag">${esc(l.tag)}</span>
          <span class="msg">${esc(l.msg)}</span></div>`)
      .join("") || `<div class="empty">Waiting for output\u2026</div>`;
    out.scrollTop = out.scrollHeight;
  };

  let finished = false;
  const finish = async () => {
    if (finished) return;      // run_finished can arrive more than once
    finished = true;
    setRunState(host, "", false);
    try {
      const run = await api.get(`/api/runs/${runId}`);
      const m = run.metrics || {};
      state.graphRunLines.push({
        level: run.status === "succeeded" ? "success" : "error",
        tag: "run",
        msg: `${run.status} \u00b7 processed ${m.processed ?? 0}, skipped ${m.skipped ?? 0}, `
           + `failed ${m.failed ?? 0}${run.error ? ` \u2014 ${run.error}` : ""}`,
      });
      // A cache that did its job is worth seeing; one that did nothing is not.
      for (const cache of m.caches || []) {
        state.graphRunLines.push({
          level: "info", tag: "cache",
          msg: `${cache.function}: ${cache.hits} hit(s), ${cache.misses} miss(es)`
             + `${cache.skipped ? `, ${cache.skipped} uncacheable` : ""}`
             + `${cache.evictions ? `, ${cache.evictions} evicted` : ""}`,
        });
      }
      paint();
      await refreshPipelines();
    } catch { /* the run summary is a nicety, not the point */ }
  };

  let socket;
  try {
    socket = new WebSocket(`${proto}://${location.host}/api/ws/runs/${runId}`);
  } catch {
    return pollTestRun(runId, host, paint, finish);
  }
  let opened = false;
  socket.onopen = () => { opened = true; };
  socket.onerror = () => { if (!opened) pollTestRun(runId, host, paint, finish); };
  socket.onmessage = (message) => {
    const event = JSON.parse(message.data);
    if (event.kind === "snapshot") {
      state.graphRunLines = (event.events || []).map(eventToLine).filter(Boolean);
      return paint();
    }
    if (event.kind === "run_finished") {
      socket.close();
      return finish();
    }
    const line = eventToLine(event);
    if (!line) return;
    state.graphRunLines.push(line);
    if (state.graphRunLines.length > 800) state.graphRunLines.splice(0, 200);
    paint();
  };
  socket.onclose = () => { if (!opened) pollTestRun(runId, host, paint, finish); };
}

/** No WebSocket (a proxy, or uvicorn without the extra): poll instead. */
function pollTestRun(runId, host, paint, finish) {
  const timer = setInterval(async () => {
    try {
      const data = await api.get(`/api/runs/${runId}/console?limit=400`);
      state.graphRunLines = (data.events || []).map(eventToLine).filter(Boolean);
      paint();
      const run = await api.get(`/api/runs/${runId}`);
      if (TERMINAL.includes(run.status)) {
        clearInterval(timer);
        await finish();
      }
    } catch {
      clearInterval(timer);
    }
  }, 900);
}

/** Compile what the editor is holding, without writing anything. */
async function checkGraph(id, path) {
  const view = state.graphView;
  try {
    const res = await api.post(
      `/api/pipelines/${encodeURIComponent(id)}/graph/preview?path=${encodeURIComponent(path)}`,
      { graph: view ? view.graph : {} },
    );
    if (!res.ok) return toast(res.error, "error");
    const warnings = res.warnings || [];
    toast(warnings.length ? `Compiles, with ${warnings.length} warning(s)` : "Compiles cleanly",
          warnings.length ? "" : "success");
  } catch (err) {
    toast(err.message, "error");
  }
}

/** Double-clicking a node opens what it stands for. */
function openDefinition(id, node) {
  const op = node.op || "";
  if (op.startsWith("graph:")) {
    const name = op.slice(6);
    const hit = (state.files?.openable || []).find(
      (f) => f.path.endsWith(`/${name}.graphlib`) || f.path === `${name}.graphlib`);
    if (hit) return selectFile(hit.path);
  }
  toast(`${node.title || op} · ${op}`);
}

/** The parametric built-ins (get_attr, binary_op, format…) carry a small config
 *  object; editing it re-resolves the node, because config is what decides its
 *  pins. */
function configureNode(node, definition, apply) {
  const current = { ...(definition?.meta?.config || {}), ...(node.config || {}) };
  const keys = Object.keys(current);
  if (!keys.length) return toast("This node has nothing to configure");

  const body = keys
    .map((key) => `<label class="field">${esc(key)}
        <input type="text" data-key="${esc(key)}"
               value="${esc(typeof current[key] === "string" ? current[key] : JSON.stringify(current[key]))}" />
      </label>`)
    .join("");
  openModal(`Configure ${node.title || node.op}`, `
    <div class="dim mono" style="font-size:12px">${esc(node.op)}</div>
    ${body}`, async () => {
    const next = {};
    for (const input of $$("#modal-body [data-key]")) {
      const key = input.dataset.key;
      // A string stays a string; anything else came from JSON and goes back as
      // JSON, so a list of keys does not turn into the text "[a, b]".
      if (typeof current[key] === "string") next[key] = input.value;
      else {
        try { next[key] = JSON.parse(input.value); }
        catch { next[key] = input.value; }
      }
    }
    await apply(node.id, next);
  }, "Apply");
}

function showNodeDetails(host, node, definition) {
  const panel = $("#graph-inspect", host);
  if (!panel || !node) return;
  panel.hidden = false;
  const pins = (definition?.pins || [])
    .map((p) => `<tr><td class="mono">${esc(p.name)}</td>
        <td class="dim">${esc(p.direction)}</td>
        <td class="mono dim">${esc(p.type)}</td>
        <td class="dim">${esc(p.description || "")}</td></tr>`)
    .join("");
  panel.innerHTML = `
    <div class="panel-head">${esc(node.title || definition?.title || node.op)}
      <span class="dim mono" style="font-weight:400">${esc(node.op)}</span></div>
    <div class="panel-body">
      ${definition?.description ? `<div class="dim" style="margin-bottom:8px">${esc(definition.description)}</div>` : ""}
      ${Object.keys(node.config || {}).length
        ? `<div class="dim mono" style="font-size:12px;margin-bottom:8px">config ${esc(JSON.stringify(node.config))}</div>` : ""}
      <div class="table-wrap">${pins
        ? `<table><thead><tr><th>Pin</th><th>Dir</th><th>Type</th><th>Description</th></tr></thead>
           <tbody>${pins}</tbody></table>`
        : `<div class="empty">This node could not be resolved.</div>`}</div>
    </div>`;
}

function rememberOpenFile(path) {
  const tabs = state.openFiles;
  const at = tabs.indexOf(path);
  if (at !== -1) tabs.splice(at, 1);
  tabs.push(path);
  while (tabs.length > 8) tabs.shift();
}

function forgetOpenFile(path) {
  const at = state.openFiles.indexOf(path);
  if (at !== -1) state.openFiles.splice(at, 1);
}

async function renderFiles(id, body) {
  const files = await api.get(`/api/pipelines/${encodeURIComponent(id)}/files`);
  // Graphs are documents you open too, even though the server does not call
  // them editable text.
  const openable = files.filter((f) => f.editable || isGraph(f.path));
  const current = state.detail.file && openable.some((f) => f.path === state.detail.file)
    ? state.detail.file
    : (openable.find((f) => f.path === "pipeline.py")?.path || openable[0]?.path || null);
  state.detail.file = current;
  state.openFiles = state.openFiles.filter((p) => openable.some((f) => f.path === p));
  if (current) rememberOpenFile(current);
  const active = !!state.detail.pipeline?.active_run;
  state.files = { id, body, files, openable, active };

  body.innerHTML = `
    <div class="split" id="files-split" style="--tree-w:${treeWidth()}px">
      <div class="panel" style="margin:0">
        <div class="panel-head">Files
          <div class="spacer"></div>
          <button class="icon-btn" id="new-any" title="New file, folder, module or graph">＋</button>
          <button class="icon-btn" id="tree-refresh" title="Rescan the folder">⟲</button>
        </div>
        <div class="panel-body flush"><div class="file-list" id="file-list"></div></div>
      </div>
      <div class="splitter" id="files-splitter" title="Drag to resize"></div>
      <div class="panel" style="margin:0">
        <div class="file-tabs" id="file-tabs"></div>
        <div class="panel-head"><span class="mono" id="file-name">${esc(current || "no file")}</span>
          <span class="dim" id="file-dirty"></span>
          <div class="spacer"></div>
          <button class="btn sm" id="file-check" ${current ? "" : "disabled"}>Check</button>
          <button class="btn sm primary" id="file-save" ${current && !active ? "" : "disabled"}>Save</button>
        </div>
        <div class="panel-body">
          <div id="editor-slot"></div>
          ${active ? `<div class="dim" style="font-size:12px;margin-top:8px">
            A run is active — saving is disabled until it finishes.</div>` : ""}
        </div>
      </div>
    </div>`;

  mountFileTree(id, $("#file-list", body), files, current);
  wireSplitter($("#files-split", body), $("#files-splitter", body));
  $("#new-any").addEventListener("click", (ev) =>
    contextMenu(ev, [
      { label: "＋ New file", onClick: () => newEntry(id, "file", treeDir()) },
      { label: "＋ New folder", onClick: () => newEntry(id, "folder", treeDir()) },
      null,
      { label: "＋ New module", hint: ".module.py", onClick: () => newEntry(id, "module", treeDir()) },
      { label: "◆ New graph", hint: ".graph", onClick: () => newEntry(id, "graph", treeDir()) },
      { label: "ƒ New function graph", hint: ".graphlib",
        onClick: () => newEntry(id, "graphlib", treeDir()) },
      { label: "λ New node library", hint: ".nodes.py",
        onClick: () => newEntry(id, "nodes", treeDir()) },
    ]));
  $("#tree-refresh").addEventListener("click", async () => {
    await refreshTree(id);
    toast("Folder rescanned");
  });

  if (isGraph(current)) {
    // The canvas replaces the text editor; the generated Python is one tab away.
    // Save and Check keep working - they just act on the graph.
    $("#file-save").addEventListener("click", () => state.graphView?.save());
    $("#file-check").addEventListener("click", () => checkGraph(id, current));
    renderFileTabs();
    await renderGraphPanel(id, current, $("#editor-slot", body));
    return;
  }

  $("#editor-slot", body).appendChild(editorHost);
  $("#file-save").addEventListener("click", () => saveCurrentFile());
  $("#file-check").addEventListener("click", () => checkCurrentFile());

  // One editor per pipeline: switching pipelines starts with fresh models.
  if (state.editor && state.editorPipeline !== id) {
    state.editor.dispose();
    state.editor = null;
  }
  if (!current) {
    renderFileTabs();
    return;
  }

  const data = await api.get(
    `/api/pipelines/${encodeURIComponent(id)}/file?path=${encodeURIComponent(current)}`,
  );
  if (!state.editor) {
    state.editorPipeline = id;
    state.editor = createEditor(editorHost, {
      value: data.content,
      path: current,
      language: languageFor(current),
      onSave: () => saveCurrentFile(),
      onDirty: paintDirty,
    });
  } else {
    state.editor.open(current, data.content, languageFor(current));
    state.editor.layout();
  }
  renderFileTabs();
  paintDirty(false);
}

/* The file tree. It only reports gestures; every rule about generated files
   and module state lives on the server, so the two cannot disagree. */
function mountFileTree(id, host, files, current) {
  const expanded = (state.treeExpanded ||= {});
  const keep = (expanded[id] ||= new Set());

  state.tree = createFileTree(host, {
    files,
    current,
    expanded: keep,
    onOpen: (path) => selectFile(path),
    onMenu: (event, node) => fileMenu(event, id, node),
    onMove: (from, to) => moveEntry(id, from, to),
    onRename: (path, name) => moveEntry(id, path, join(dirname(path), name)),
    onDelete: (node) => deleteEntry(id, node),
    onDropFiles: (entries, dir) => uploadEntries(id, entries, dir),
    onNew: (kind, dir) => newEntry(id, kind, dir),
  });
  return state.tree;
}

function fileMenu(event, id, node) {
  const isRoot = !node.path;
  const dir = node.type === "dir" ? node.path : dirname(node.path);
  const generated = !!node.generated;
  const items = [
    ...(isRoot || node.type === "dir"
      ? []
      : [
          { label: "✎ Open", onClick: () => selectFile(node.path) },
          {
            label: "✏ Rename",
            hint: "F2",
            disabled: generated,
            onClick: () => state.tree.rename(node.path),
          },
          { label: "⧉ Duplicate", disabled: generated, onClick: () => duplicate(id, node) },
          null,
        ]),
    { label: "＋ New file", onClick: () => newEntry(id, "file", dir) },
    { label: "＋ New folder", onClick: () => newEntry(id, "folder", dir) },
    { label: "＋ New module", hint: ".module.py", onClick: () => newEntry(id, "module", dir) },
    { label: "◆ New graph", hint: ".graph", onClick: () => newEntry(id, "graph", dir) },
    { label: "ƒ New function graph", hint: ".graphlib", onClick: () => newEntry(id, "graphlib", dir) },
    { label: "λ New node library", hint: ".nodes.py", onClick: () => newEntry(id, "nodes", dir) },
    null,
    { label: "⧉ Copy path", disabled: isRoot, onClick: () => copyText(node.path) },
    ...(generated
      ? [{
          label: "◆ Open the graph it came from",
          onClick: () => selectFile(join(dirname(node.path), node.generated)),
        }]
      : []),
    ...(isRoot
      ? []
      : [
          null,
          {
            label: "🗑 Delete",
            hint: "Del",
            danger: true,
            disabled: generated,
            onClick: () => deleteEntry(id, node),
          },
        ]),
  ];
  contextMenu(event, items);
}

async function refreshTree(id, keepFile) {
  const files = await api.get(`/api/pipelines/${encodeURIComponent(id)}/files`);
  if (state.files) state.files.files = files;
  if (state.tree) state.tree.setFiles(files, keepFile ?? state.detail.file);
  return files;
}

function reportBuild(build) {
  if (!build) return;
  if (build.error) toast(build.error, "error");
  (build.graphs || [])
    .filter((g) => g.status === "failed")
    .forEach((g) => toast(`${g.source}: ${g.error}`, "error"));
  (build.warnings || []).slice(0, 4).forEach((w) => toast(w));
}

async function moveEntry(id, from, to) {
  if (from === to) return;
  try {
    const res = await api.post(`/api/pipelines/${encodeURIComponent(id)}/file/move`,
      { source: from, target: to });
    const renamed = res.renamed_modules || [];
    let message = `Moved ${basename(from)} → ${to}`;
    if (renamed.length) {
      message = `Renamed module ${renamed[0].from} → ${renamed[0].to}`;
      if (res.state_rows_migrated) {
        message += ` · ${fmtNumber(res.state_rows_migrated)} state row(s) carried over`;
      }
    }
    toast(message, "success");
    reportBuild(res.build);
    // Follow the file if it was the one being edited.
    if (state.detail.file === from) state.detail.file = to;
    else if (state.detail.file?.startsWith(`${from}/`)) {
      state.detail.file = to + state.detail.file.slice(from.length);
    }
    state.openFiles = state.openFiles.map((p) =>
      p === from ? to : p.startsWith(`${from}/`) ? to + p.slice(from.length) : p);
    await refreshPipelines();
    await renderFiles(id, state.files.body);
  } catch (err) {
    toast(err.message, "error");
    await refreshTree(id);
  }
}

async function deleteEntry(id, node) {
  if (!node?.path) return;
  const what = node.type === "dir" ? "folder" : "file";
  const extra = node.type === "dir" ? "\n\nEverything inside it goes too." : "";
  if (!confirm(`Delete the ${what} ${node.path}?${extra}\n\nThis cannot be undone here.`)) return;
  try {
    const res = await api.del(
      `/api/pipelines/${encodeURIComponent(id)}/file?path=${encodeURIComponent(node.path)}`);
    let message = `Deleted ${res.removed.join(", ")}`;
    if (res.state_rows_dropped) {
      message += ` · ${fmtNumber(res.state_rows_dropped)} state row(s) forgotten`;
    }
    toast(message, "success");
    reportBuild(res.build);
    state.openFiles = state.openFiles.filter(
      (p) => p !== node.path && !p.startsWith(`${node.path}/`));
    if (state.detail.file === node.path || state.detail.file?.startsWith(`${node.path}/`)) {
      state.detail.file = state.openFiles[state.openFiles.length - 1] || null;
    }
    await refreshPipelines();
    await renderFiles(id, state.files.body);
  } catch (err) {
    toast(err.message, "error");
  }
}

async function duplicate(id, node) {
  const name = basename(node.path);
  const stem = name.includes(".") ? name.slice(0, name.indexOf(".")) : name;
  const rest = name.slice(stem.length);
  const target = join(dirname(node.path), `${stem}_copy${rest}`);
  try {
    const data = await api.get(
      `/api/pipelines/${encodeURIComponent(id)}/file?path=${encodeURIComponent(node.path)}`);
    await api.post(`/api/pipelines/${encodeURIComponent(id)}/file/new`,
      { path: target, content: data.content });
    toast(`Created ${target}`, "success");
    state.detail.file = target;
    await refreshPipelines();
    await renderFiles(id, state.files.body);
  } catch (err) {
    toast(err.message, "error");
  }
}

async function uploadEntries(id, entries, dir) {
  const tooBig = entries.filter((e) => e.file.size > 64 * 1024 * 1024);
  if (tooBig.length) {
    toast(`${tooBig.length} file(s) are over 64 MB and were skipped`, "error");
  }
  const todo = entries.filter((e) => e.file.size <= 64 * 1024 * 1024);
  if (!todo.length) return;
  let done = 0;
  for (const entry of todo) {
    const target = join(dir, entry.relative);
    try {
      await api.upload(
        `/api/pipelines/${encodeURIComponent(id)}/upload?path=${encodeURIComponent(target)}`,
        entry.file);
      done += 1;
    } catch (err) {
      toast(`${entry.relative}: ${err.message}`, "error");
    }
  }
  if (done) toast(`Added ${done} file(s) to ${dir || "the pipeline folder"}`, "success");
  await refreshPipelines();
  await renderFiles(id, state.files.body);
}

/** Switch the editor to another file without rebuilding the tab. */
async function selectFile(path) {
  const ctx = state.files;
  if (!ctx || path === state.detail.file) return;
  const pending = isGraph(state.detail.file)
    ? (state.graph?.dirty || state.graphView?.dirty)
    : state.editor?.dirty;
  if (pending && !confirm(`${state.detail.file} has unsaved changes. Discard them?`)) return;
  if (isGraph(path) || isGraph(state.detail.file)) {
    // A graph swaps the whole right-hand pane, so rebuild the tab.
    state.detail.file = path;
    rememberOpenFile(path);
    await renderFiles(ctx.id, ctx.body);
    return;
  }
  try {
    const data = await api.get(
      `/api/pipelines/${encodeURIComponent(ctx.id)}/file?path=${encodeURIComponent(path)}`,
    );
    state.detail.file = path;
    rememberOpenFile(path);
    if (state.editor) {
      state.editor.open(path, data.content, languageFor(path));
      state.editor.focus();
    }
    $("#file-name").textContent = path;
    $$("#file-list .file-item").forEach((el) =>
      el.classList.toggle("active", el.dataset.file === path),
    );
    renderFileTabs();
    paintDirty(false);
  } catch (err) {
    toast(err.message, "error");
  }
}

function renderFileTabs() {
  const bar = $("#file-tabs");
  if (!bar) return;
  const tabs = state.openFiles;
  bar.innerHTML = tabs.length
    ? tabs
        .map(
          (path) => `<div class="file-tab ${path === state.detail.file ? "active" : ""}"
            data-tab-file="${esc(path)}" title="${esc(path)}">
            <span>${esc(path.split("/").pop())}</span>
            <button class="file-tab-close" data-close-file="${esc(path)}" title="Close">✕</button>
          </div>`,
        )
        .join("")
    : "";
  $$("[data-tab-file]", bar).forEach((el) =>
    el.addEventListener("click", (ev) => {
      if (ev.target.closest("[data-close-file]")) return;
      selectFile(el.dataset.tabFile);
    }),
  );
  $$("[data-close-file]", bar).forEach((el) =>
    el.addEventListener("click", (ev) => {
      ev.stopPropagation();
      const path = el.dataset.closeFile;
      forgetOpenFile(path);
      if (path === state.detail.file && state.openFiles.length) {
        selectFile(state.openFiles[state.openFiles.length - 1]);
      } else {
        renderFileTabs();
      }
    }),
  );
}

/* The Files tab is a two-pane layout; let people size it like an editor would. */
function treeWidth() {
  if (state.treeWidth) return state.treeWidth;
  try {
    state.treeWidth = Number(localStorage.getItem("graetl.treeWidth")) || 260;
  } catch {
    state.treeWidth = 260;
  }
  return state.treeWidth;
}

function wireSplitter(split, handle) {
  if (!split || !handle) return;
  handle.addEventListener("mousedown", (start) => {
    start.preventDefault();
    const origin = start.clientX;
    const from = treeWidth();
    const move = (event) => {
      const width = Math.max(180, Math.min(560, from + event.clientX - origin));
      state.treeWidth = width;
      split.style.setProperty("--tree-w", `${width}px`);
    };
    const stop = () => {
      window.removeEventListener("mousemove", move);
      window.removeEventListener("mouseup", stop);
      document.body.classList.remove("resizing");
      try { localStorage.setItem("graetl.treeWidth", String(state.treeWidth)); } catch { /* fine */ }
    };
    document.body.classList.add("resizing");
    window.addEventListener("mousemove", move);
    window.addEventListener("mouseup", stop);
  });
}

/** The folder the tree's selection implies, for "new" actions. */
function treeDir() {
  const selected = state.tree?.selected;
  if (!selected) return "";
  const entry = (state.files?.files || []).find((f) => f.path === selected);
  return entry?.type === "dir" ? selected : dirname(selected);
}

function paintDirty(dirty) {
  const el = $("#file-dirty");
  if (el) el.textContent = dirty ? "● unsaved" : "";
  const tab = $(`[data-tab-file="${CSS.escape(state.detail.file || "")}"]`);
  if (tab) tab.classList.toggle("dirty", !!dirty);
}

async function saveCurrentFile() {
  const ctx = state.files;
  if (!ctx || !state.detail.file || !state.editor) return;
  if (ctx.active) {
    toast("Cannot save while a run is active", "error");
    return;
  }
  const path = state.detail.file;
  try {
    await api.put(
      `/api/pipelines/${encodeURIComponent(ctx.id)}/file?path=${encodeURIComponent(path)}`,
      { content: state.editor.value },
    );
    state.editor.markSaved();
    paintDirty(false);
    toast(`Saved ${path}`, "success");
    await refreshPipelines();
  } catch (err) {
    state.editor.showProblem(err.syntaxError || { line: null, message: err.message });
    toast(err.message, "error");
  }
}

async function checkCurrentFile() {
  const ctx = state.files;
  if (!ctx || !state.detail.file || !state.editor) return;
  try {
    const res = await api.post(
      `/api/pipelines/${encodeURIComponent(ctx.id)}/file/check?path=${encodeURIComponent(state.detail.file)}`,
      { content: state.editor.value },
    );
    state.editor.showProblem(res.error);
    toast(
      res.ok ? "No syntax errors" : `${res.error.message} (line ${res.error.line})`,
      res.ok ? "success" : "error",
    );
  } catch (err) {
    toast(err.message, "error");
  }
}

const NEW_KINDS = {
  file: {
    title: "New file", label: "Path", placeholder: "parsing.py", ok: "Create",
    hint: "Helper code lives next to the module that imports it. A file named "
        + "<span class=\"mono\">&lt;name&gt;.module.py</span> becomes a module of its own.",
  },
  folder: {
    title: "New folder", label: "Folder", placeholder: "vitals", ok: "Create",
    hint: "Folders group a module with its helpers, graphs and metadata.",
  },
  module: {
    title: "New module", label: "Module name", placeholder: "load_vital_signals",
    ok: "Create module",
    hint: "Creates <span class=\"mono\">&lt;name&gt;.module.py</span> and its "
        + "<span class=\"mono\">.module.toml</span>.",
  },
  graph: {
    title: "New graph", label: "Module name", placeholder: "load_vital_signals",
    ok: "Create graph",
    hint: "A node graph that compiles to <span class=\"mono\">&lt;name&gt;.module.py</span>. "
        + "Draw it on the canvas; the Python is the build output.",
  },
  graphlib: {
    title: "New function graph", label: "Function name", placeholder: "compute_bmi",
    ok: "Create function",
    hint: "A callable graph. It registers as a pipeline function, so every module "
        + "and every other graph can use it.",
  },
  nodes: {
    title: "New node library", label: "Library name", placeholder: "vitals",
    ok: "Create library",
    hint: "Creates <span class=\"mono\">&lt;name&gt;.nodes.py</span>. Every function you "
        + "write in it is a node on any graph &mdash; no decorator, pins from the "
        + "signature, and the file name groups them in the palette.",
  },
};

function newEntry(pipelineId, kind, dir = "") {
  const spec = NEW_KINDS[kind] || NEW_KINDS.file;
  const layerField = (kind === "graph" || kind === "module")
    ? `<label class="field">Execution layer
         <input type="number" id="ne-layer" value="${nextLayer()}" step="10" /></label>`
    : "";
  // A module graph starts with one of three entry nodes, and that decides how
  // the runner calls it. Asking here is cheaper than explaining it later.
  const entryField = kind === "graph"
    ? `<div class="field"><span>Runs for</span>
         <div class="choices" id="ne-entry">
           ${[["entity", "Each entity", "fn(ctx, entity) — one at a time, one transaction each. The usual choice."],
              ["batch", "A batch of entities", "fn(ctx, entities) — a list at a time in ONE transaction. Cheaper in bulk; the whole batch succeeds or is retried."],
              ["once", "Once per run", "fn(ctx) — no entity at all, when its layer comes up. For work about the whole table."]]
             .map(([value, label, hint], i) => `
               <label class="choice">
                 <input type="radio" name="ne-entry" value="${value}" ${i === 0 ? "checked" : ""} />
                 <span><b>${label}</b><br><span class="dim">${hint}</span></span>
               </label>`).join("")}
         </div>
       </div>
       <label class="field" id="ne-size-row" hidden>Entities per batch
         <input type="number" id="ne-size" min="1" placeholder="the configured default (500)" /></label>`
    : "";
  openModal(spec.title, `
    <div class="dim">${spec.hint}</div>
    ${dir ? `<div class="dim mono" style="font-size:12px">in ${esc(dir)}/</div>` : ""}
    <label class="field">${spec.label}
      <input type="text" id="ne-name" placeholder="${esc(spec.placeholder)}" /></label>
    ${layerField}
    ${entryField}`, async () => {
    const raw = $("#ne-name").value.trim();
    if (!raw) throw new Error(`${spec.label.toLowerCase()} is required`);
    const layer = Number($("#ne-layer")?.value || 0);
    const base = `/api/pipelines/${encodeURIComponent(pipelineId)}`;

    if (kind === "folder") {
      await api.post(`${base}/folder`, { path: join(dir, raw) });
      state.tree?.expanded.add(join(dir, raw));
      toast(`Created ${join(dir, raw)}/`, "success");
    } else if (kind === "file") {
      const path = raw.includes("/") ? raw : join(dir, raw);
      await api.post(`${base}/file/new`, { path, content: "" });
      state.detail.file = path;
      toast(`Created ${path}`, "success");
    } else if (kind === "nodes") {
      const path = join(dir, raw.endsWith(".nodes.py") ? raw : `${raw}.nodes.py`);
      // Empty content asks the server for the worked example.
      await api.post(`${base}/file/new`, { path, content: "" });
      state.detail.file = path;
      toast(`Node library ${path} created`, "success");
    } else if (kind === "module") {
      // Scaffolding decides the folder; honour the one that was right-clicked.
      const folder = dir.startsWith("modules/") ? dir.slice("modules/".length) : raw;
      await api.post(`${base}/modules`,
        { name: raw, folder: folder || null, execution_layer: layer });
      state.detail.file = `modules/${folder || raw}/${raw}.module.py`;
      toast(`Module ${raw} created`, "success");
    } else {
      const suffix = kind === "graph" ? ".graph" : ".graphlib";
      const target = dir || (kind === "graph" ? `modules/${raw}` : "");
      const path = join(target, `${raw}${suffix}`);
      const entry = $('#ne-entry input:checked')?.value || "entity";
      await api.post(`${base}/graphs`, {
        path,
        kind: kind === "graph" ? "module" : "function",
        execution_layer: layer,
        entry,
        batch_size: Number($("#ne-size")?.value || 0),
      });
      state.detail.file = path;
      toast(`Created ${path}`, "success");
    }
    await refreshPipelines();
    await renderFiles(pipelineId, state.files.body);
  }, spec.ok);
  $$('#ne-entry input[name="ne-entry"]').forEach((el) =>
    el.addEventListener("change", () => {
      $("#ne-size-row").hidden = el.value !== "batch" || !el.checked;
    }));
}

function nextLayer() {
  const layers = state.detail.pipeline?.definition?.layers || [];
  return layers.length ? Math.max(...layers) + 10 : 0;
}

/* ---------------------------------------------------------------- run view */

async function viewRun(runId) {
  const run = await api.get(`/api/runs/${runId}`);
  state.detail.run = run;
  state.console.events = [];
  if (run.params && run.params.debug) state.console.levels.add("debug");

  $("#main").innerHTML = `
    <div class="page">
      <div class="page-head">
        <div>
          <h1>Run #${run.id} <span id="run-status">${statusPill(run.status)}</span></h1>
          <div class="page-sub">
            <a href="#/p/${esc(run.pipeline_id)}" style="color:var(--accent)">${esc(run.pipeline_id)}</a>
            · mode ${esc(run.mode)} · started ${esc(fmtTime(run.started_at))}
            ${run.parent_run_id ? ` · resumed from <a href="#/runs/${run.parent_run_id}" style="color:var(--accent)">#${run.parent_run_id}</a>` : ""}
          </div>
        </div>
        <div class="spacer"></div>
        <div class="row" id="run-actions"></div>
      </div>
      ${run.error ? `<div class="banner">${esc(run.error)}</div>` : ""}
      <div class="cards" id="run-cards"></div>
      <div class="panel">
        <div class="panel-head">Process &amp; resources
          <span class="dim" style="font-weight:400">sampled every 2s while the run is alive</span></div>
        <div class="panel-body" id="run-resources"><div class="empty">Waiting for telemetry…</div></div>
      </div>
      <div class="panel">
        <div class="panel-head">Steps</div>
        <div class="panel-body flush table-wrap" id="run-steps"></div>
      </div>
      <div class="panel">
        <div class="panel-head">Console</div>
        <div class="panel-body flush">
          <div class="console-wrap">
            <div class="console-bar">
              ${["debug", "info", "success", "warning", "error", "event"].map((lvl) =>
                `<span class="chip ${state.console.levels.has(lvl) ? "on" : ""}" data-level="${lvl}">${lvl}</span>`).join("")}
              <input type="search" id="console-filter" placeholder="filter…" style="width:160px" />
              <div class="spacer"></div>
              <label class="row" style="font-size:12px;color:var(--text-dim);gap:6px">
                <input type="checkbox" id="console-follow" ${state.console.follow ? "checked" : ""} /> follow
              </label>
              <button class="btn sm" id="console-clear">Clear</button>
              <a class="btn sm" href="/api/runs/${run.id}/log/download">Download</a>
            </div>
            <div class="console" id="console"></div>
          </div>
        </div>
      </div>
    </div>`;

  state.console.resources = run.stats && Object.keys(run.stats).length ? [run.stats] : [];
  renderRunHeader(run);
  renderResources();
  $$("[data-level]").forEach((el) => el.addEventListener("click", () => {
    const lvl = el.dataset.level;
    if (state.console.levels.has(lvl)) state.console.levels.delete(lvl);
    else state.console.levels.add(lvl);
    el.classList.toggle("on");
    renderConsole();
  }));
  $("#console-filter").addEventListener("input", (ev) => {
    state.console.filter = ev.target.value.toLowerCase();
    renderConsole();
  });
  $("#console-follow").addEventListener("change", (ev) => { state.console.follow = ev.target.checked; });
  $("#console-clear").addEventListener("click", () => { state.console.events = []; renderConsole(); });

  connectRunSocket(run.id);
}

function renderRunHeader(run) {
  // While a run is alive the authoritative counters arrive with the telemetry
  // samples; the metrics row is only written when the run finishes.
  const live = (state.console.resources || []).slice(-1)[0] || {};
  const m = { ...(run.metrics || {}) };
  if (isActive(run.status)) {
    for (const key of ["processed", "skipped", "failed", "entities_total"]) {
      if (live[key] !== undefined) m[key] = live[key];
    }
  }
  const statusEl = $("#run-status");
  if (statusEl) statusEl.innerHTML = statusPill(run.status);

  const actions = $("#run-actions");
  if (actions) {
    const paused = ["paused", "pausing"].includes(run.status);
    actions.innerHTML = isActive(run.status)
      ? `${paused ? `<button class="btn primary" data-ract="resume">▶ Resume</button>`
                  : `<button class="btn" data-ract="pause">⏸ Pause</button>`}
         <button class="btn danger" data-ract="stop">■ Stop</button>`
      : `<button class="btn primary" data-ract="resume">▶ Resume in a new run</button>`;
    $$("[data-ract]", actions).forEach((el) => el.addEventListener("click", async () => {
      try {
        const res = await api.post(`/api/runs/${run.id}/${el.dataset.ract}`);
        if (el.dataset.ract === "resume" && res.id !== run.id) go(`#/runs/${res.id}`);
        else toast(`Run ${el.dataset.ract} requested`);
      } catch (err) { toast(err.message, "error"); }
    }));
  }

  const cards = $("#run-cards");
  if (cards) {
    const pct = run.progress_total ? Math.round((run.progress_done / run.progress_total) * 100) : null;
    cards.innerHTML = `
      <div class="card"><div class="card-label">Duration</div><div class="card-value small">${esc(fmtDuration(run.duration_ms ?? (run.started_at ? Date.now() - new Date(run.started_at).getTime() : null)))}</div></div>
      <div class="card"><div class="card-label">Processed</div><div class="card-value">${fmtNumber(m.processed ?? 0)}</div></div>
      <div class="card"><div class="card-label">Skipped</div><div class="card-value">${fmtNumber(m.skipped ?? 0)}</div></div>
      <div class="card"><div class="card-label">Failed</div><div class="card-value" style="${m.failed ? "color:var(--err)" : ""}">${fmtNumber(m.failed ?? 0)}</div></div>
      <div class="card"><div class="card-label">Entities</div><div class="card-value">${fmtNumber(m.entities_total ?? 0)}</div></div>
      ${pct !== null ? `<div class="card"><div class="card-label">Progress ${run.phase ? "· " + esc(run.phase) : ""}</div>
        <div class="card-value small">${run.progress_done}/${run.progress_total}</div>
        <div class="bar" style="margin-top:8px"><i style="width:${pct}%"></i></div></div>` : ""}`;
  }
  renderRunSteps(run.steps || []);
}

function renderRunSteps(steps) {
  const el = $("#run-steps");
  if (!el) return;
  if (!steps.length) { el.innerHTML = `<div class="empty">No steps recorded yet.</div>`; return; }
  el.innerHTML = `<table><thead><tr>
      <th>Step</th><th>Kind</th><th>Status</th><th class="num">Selected</th><th class="num">Processed</th>
      <th class="num">Skipped</th><th class="num">Failed</th><th class="num">Duration</th>
    </tr></thead><tbody>
    ${steps.map((s) => `<tr>
      <td class="mono">${esc(s.step)}</td>
      <td class="dim">${esc(s.kind)}${s.version > 1 ? ` v${s.version}` : ""}</td>
      <td>${statusPill({ succeeded: "succeeded", failed: "failed", running: "running", stopped: "stopped", pending: "queued", skipped: "stopped" }[s.status] || "tag", s.status)}</td>
      <td class="num">${fmtNumber(s.selected)}</td>
      <td class="num">${fmtNumber(s.processed)}</td>
      <td class="num">${fmtNumber(s.skipped)}</td>
      <td class="num" style="${s.failed ? "color:var(--err)" : ""}">${fmtNumber(s.failed)}</td>
      <td class="num dim">${esc(fmtDuration(s.duration_ms))}</td>
    </tr>`).join("")}</tbody></table>`;
}

/* --------------------------------------------------------------- console */

function eventToLine(ev) {
  const kind = ev.kind;
  if (kind === "log") {
    return { level: ev.level || "info", tag: ev.step || ev.source || "", msg: ev.message || "", ts: ev.ts };
  }
  if (kind === "entity") {
    const level = ev.status === "failed" ? "error" : "debug";
    return { level, tag: ev.step || "", ts: ev.ts,
      msg: `${ev.entity_id}: ${ev.status}${ev.duration_ms !== undefined ? ` (${ev.duration_ms} ms)` : ""}${ev.error ? ` — ${ev.error}` : ""}` };
  }
  if (kind === "step_start") {
    return { level: "event", tag: "step", ts: ev.ts,
      msg: `▶ ${ev.step}${ev.selected ? ` — ${ev.selected} item(s)` : ""}` };
  }
  if (kind === "step_end") {
    return { level: ev.status === "failed" ? "error" : "event", tag: "step", ts: ev.ts,
      msg: `■ ${ev.step}: ${ev.status} · processed ${ev.processed ?? 0}, skipped ${ev.skipped ?? 0}, failed ${ev.failed ?? 0} · ${fmtDuration(ev.duration_ms)}` };
  }
  if (kind === "run_start") return { level: "event", tag: "run", msg: `▶ run started (mode ${ev.mode})`, ts: ev.ts };
  if (kind === "run_end") {
    return { level: ev.status === "succeeded" ? "success" : "error", tag: "run", ts: ev.ts,
      msg: `■ run ${ev.status}${ev.error ? ` — ${ev.error}` : ""}` };
  }
  if (kind === "status") return { level: "event", tag: "run", msg: `run ${ev.status}`, ts: ev.ts };
  if (kind === "profile") return { level: "info", tag: "profile", msg: `profile written to ${ev.path}\n${ev.top || ""}`, ts: ev.ts };
  if (kind === "resource") return null;
  return null;
}

function renderConsole() {
  const el = $("#console");
  if (!el) return;
  const filter = state.console.filter;
  const html = state.console.events.map(eventToLine).filter(Boolean)
    .filter((l) => state.console.levels.has(l.level))
    .filter((l) => !filter || (l.msg + l.tag).toLowerCase().includes(filter))
    .map((l) => `<div class="line ${esc(l.level)}"><span class="ts">${esc(fmtClock(l.ts))}</span><span class="tag">${esc(l.tag)}</span><span class="msg">${esc(l.msg)}</span></div>`)
    .join("");
  el.innerHTML = html || `<div class="empty">Waiting for output…</div>`;
  if (state.console.follow) el.scrollTop = el.scrollHeight;
}

function connectRunSocket(runId) {
  closeRunSocket();
  const proto = location.protocol === "https:" ? "wss" : "ws";
  let opened = false;
  let ws;
  try {
    ws = new WebSocket(`${proto}://${location.host}/api/ws/runs/${runId}`);
  } catch {
    startConsolePolling(runId);
    return;
  }
  state.runSocket = ws;
  state.runSocketId = runId;
  ws.onopen = () => { opened = true; };
  ws.onerror = () => { if (!opened) startConsolePolling(runId); };

  ws.onmessage = (msg) => {
    const data = JSON.parse(msg.data);
    if (data.kind === "snapshot") {
      state.console.events = data.events || [];
      if (data.run) { state.detail.run = { ...data.run, steps: state.detail.run?.steps || [] }; renderRunHeader(state.detail.run); }
      renderConsole();
      return;
    }
    if (data.kind === "run_finished") {
      api.get(`/api/runs/${runId}`).then((run) => { state.detail.run = run; renderRunHeader(run); });
      refreshPipelines();
      return;
    }
    if (data.kind === "resource") {
      state.console.resources.push(data);
      if (state.console.resources.length > 120) state.console.resources.shift();
      renderResources();
      if (state.detail.run) renderRunHeader(state.detail.run);
      return;
    }
    state.console.events.push(data);
    if (state.console.events.length > 5000) state.console.events.splice(0, 1000);

    if (["step_start", "step_end"].includes(data.kind)) {
      api.get(`/api/runs/${runId}/steps`).then(renderRunSteps).catch(() => {});
    }
    if (data.kind === "progress" && state.detail.run) {
      state.detail.run.progress_done = data.done || 0;
      state.detail.run.progress_total = data.total || 0;
      state.detail.run.phase = data.step;
      renderRunHeader(state.detail.run);
    }
    if (data.kind === "status" && state.detail.run) {
      state.detail.run.status = { paused: "paused", running: "running", stopping: "stopping" }[data.status] || state.detail.run.status;
      renderRunHeader(state.detail.run);
    }
    renderConsole();
  };
  ws.onclose = () => {
    if (state.runSocketId === runId) state.runSocket = null;
    if (!opened) startConsolePolling(runId);
  };
}

/* Fallback for environments where WebSockets are unavailable (proxies, or a
   uvicorn install without the websockets extra): poll the console endpoint. */
function startConsolePolling(runId) {
  if (state.consolePoll && state.consolePollId === runId) return;
  stopConsolePolling();
  state.consolePollId = runId;
  const tick = async () => {
    if (state.route.view !== "run" || state.route.id !== runId) return stopConsolePolling();
    try {
      const data = await api.get(`/api/runs/${runId}/console?limit=3000`);
      const all = data.events || [];
      state.console.events = all.filter((e) => e.kind !== "resource");
      state.console.resources = all.filter((e) => e.kind === "resource").slice(-120);
      renderConsole();
      renderResources();
      const run = await api.get(`/api/runs/${runId}`);
      state.detail.run = run;
      renderRunHeader(run);
      if (!isActive(run.status)) stopConsolePolling();
    } catch { /* keep trying */ }
  };
  tick();
  state.consolePoll = setInterval(tick, 1500);
}

function stopConsolePolling() {
  if (state.consolePoll) clearInterval(state.consolePoll);
  state.consolePoll = null;
  state.consolePollId = null;
}

function closeRunSocket() {
  stopConsolePolling();
  if (state.runSocket) { try { state.runSocket.close(); } catch { /* ignore */ } }
  state.runSocket = null;
  state.runSocketId = null;
}

/* ---------------------------------------------------------------- actions */

async function startRun(pipelineId, body) {
  try {
    const run = await api.post(`/api/pipelines/${encodeURIComponent(pipelineId)}/runs`, body);
    toast(`Run #${run.id} started`, "success");
    document.querySelectorAll(".drawer-backdrop").forEach((el) => el.remove());
    go(`#/runs/${run.id}`);
    return run;
  } catch (err) { toast(err.message, "error"); }
}

async function pipelineAction(pipeline, action) {
  const id = pipeline.id;
  const active = pipeline.active_run;
  try {
    if (action === "run") return await startRun(id, { mode: "incremental" });
    if (action === "run-full") return await startRun(id, { mode: "full" });
    if (action === "retry") return await startRun(id, { mode: "retry-failed" });
    if (action === "toggle") {
      await api.patch(`/api/pipelines/${encodeURIComponent(id)}`, { enabled: !pipeline.enabled });
      toast(pipeline.enabled ? "Pipeline disabled" : "Pipeline enabled");
      return render();
    }
    if (active && ["pause", "resume", "stop"].includes(action)) {
      await api.post(`/api/runs/${active.id}/${action}`);
      toast(`Run ${action} requested`);
      setTimeout(render, 400);
    }
  } catch (err) { toast(err.message, "error"); }
}

async function syncPipelines() {
  try {
    state.pipelines = await api.post("/api/pipelines/sync");
    renderSidebar();
    toast(`${state.pipelines.length} pipeline(s) found`, "success");
    render();
  } catch (err) { toast(err.message, "error"); }
}

async function refreshPipelines() {
  try {
    state.pipelines = await api.get("/api/pipelines");
    renderSidebar();
  } catch { /* ignore */ }
}

/* ----------------------------------------------------------------- modals */

function openModal(title, bodyHtml, onOk, okLabel = "Create") {
  $("#modal-title").textContent = title;
  $("#modal-body").innerHTML = bodyHtml;
  $("#modal-ok").textContent = okLabel;
  $("#modal").hidden = false;
  const ok = $("#modal-ok");
  const clone = ok.cloneNode(true);
  ok.replaceWith(clone);
  clone.addEventListener("click", async () => {
    try { await onOk(); $("#modal").hidden = true; }
    catch (err) { toast(err.message, "error"); }
  });
}

function openNewPipeline() {
  openModal("New pipeline", `
    <label class="field">Pipeline id (folder name)
      <input type="text" id="np-id" placeholder="icu_admissions" /></label>
    <label class="field">Title
      <input type="text" id="np-title" placeholder="ICU Admissions" /></label>
    <label class="field">Description
      <input type="text" id="np-desc" /></label>
    <label class="field">Template
      <select id="np-template">
        <option value="stateful">Stateful — entity state, resumable</option>
        <option value="stateless">Stateless — ordered tasks only</option>
        <option value="empty">Empty — nothing yet</option>
      </select></label>`, async () => {
    const id = $("#np-id").value.trim();
    if (!id) throw new Error("an id is required");
    await api.post("/api/pipelines", {
      id,
      title: $("#np-title").value.trim() || null,
      description: $("#np-desc").value.trim(),
      template: $("#np-template").value,
    });
    await refreshPipelines();
    toast(`Pipeline ${id} created`, "success");
    go(`#/p/${id}/files`);
  });
}

function openResetState(id) {
  openModal("Reset entity state", `
    <div class="dim">This clears processing state so the next run reprocesses the entities.
      Data your modules wrote is not touched.</div>
    <label class="field">Scope
      <select id="rs-scope">
        <option value="all">All modules (keep entities)</option>
        <option value="drop">All modules and forget entities</option>
      </select></label>`, async () => {
    const scope = $("#rs-scope").value;
    await api.post(`/api/pipelines/${encodeURIComponent(id)}/state/reset`,
      { drop_entities: scope === "drop" });
    toast("State reset", "success");
    render();
  }, "Reset");
}

/* ------------------------------------------------------------------ router */

async function render() {
  state.route = parseHash();
  if (state.route.view !== "run") closeRunSocket();
  renderSidebar();
  try {
    if (state.route.view === "dashboard") await viewDashboard();
    else if (state.route.view === "runs") await viewRuns();
    else if (state.route.view === "run") await viewRun(state.route.id);
    else if (state.route.view === "pipeline") await viewPipeline(state.route.id, state.route.tab);
  } catch (err) {
    $("#main").innerHTML = `<div class="page"><div class="banner">${esc(err.message)}</div>
      <button class="btn" onclick="location.hash='#/'">Back to dashboard</button></div>`;
  }
  renderSidebar();
}

function connectSystemSocket() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/api/ws/system`);
  ws.onmessage = async (msg) => {
    const data = JSON.parse(msg.data);
    if (data.kind === "pipelines_changed" || data.kind === "pipelines_removed") {
      await refreshPipelines();
      if (state.route.view === "dashboard") await viewDashboard();
    } else if (data.kind === "run_changed") {
      await refreshPipelines();
      if (state.route.view === "dashboard") await viewDashboard();
      else if (state.route.view === "pipeline" && data.run?.pipeline_id === state.route.id) {
        // The Files tab holds live state - a canvas, a test-run pane, an editor
        // with unsaved text. Re-rendering the view would throw all of it away
        // every time a run ticks, so the header is refreshed in place instead.
        if (state.route.tab === "files") await refreshPipelineHeader(state.route.id);
        else await viewPipeline(state.route.id, state.route.tab);
      }
    }
  };
  ws.onclose = () => setTimeout(connectSystemSocket, 3000);
}

/* ------------------------------------------------------------- shortcuts */

function closeTopOverlay() {
  if (document.querySelector(".ctx-menu")) return closeContextMenu();
  const drawer = $(".drawer-backdrop");
  if (drawer) return drawer.remove();
  if (!$("#modal").hidden) $("#modal").hidden = true;
}

function wireShortcuts() {
  document.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape") {
      closeTopOverlay();
      return;
    }
    const el = ev.target;
    const typing =
      /^(input|textarea|select)$/i.test(el.tagName) ||
      el.isContentEditable ||
      el.closest?.(".monaco-editor, .ed");

    // Ctrl+S saves the open file from anywhere on the Files tab. Monaco and the
    // fallback editor handle it themselves when the caret is inside them.
    if ((ev.ctrlKey || ev.metaKey) && ev.key.toLowerCase() === "s") {
      if (state.route.view === "pipeline" && state.route.tab === "files" && !el.closest?.(".monaco-editor, .ed")) {
        ev.preventDefault();
        if (isGraph(state.detail.file)) state.graphView?.save();
        else saveCurrentFile();
      }
      return;
    }
    if (typing) return;

    if (ev.key === "/") {
      const search = $("#ent-search");
      if (search) {
        ev.preventDefault();
        search.focus();
        search.select();
      }
      return;
    }
    // Single-key tab switching inside a pipeline.
    if (state.route.view === "pipeline" && !ev.ctrlKey && !ev.metaKey && !ev.altKey) {
      const tab = { o: "overview", r: "runs", e: "entities", f: "files" }[ev.key.toLowerCase()];
      if (tab) go(`#/p/${state.route.id}/${tab}`);
    }
  });

  // Enter confirms a modal.
  $("#modal").addEventListener("keydown", (ev) => {
    if (ev.key === "Enter" && ev.target.tagName === "INPUT") {
      ev.preventDefault();
      $("#modal-ok").click();
    }
  });
}

/* ----------------------------------------------------------------- project */

/* A GraETL instance opens exactly one project - one data warehouse. Started
   without one, the server serves the console anyway and this picker completes
   the startup; switching warehouses means restarting, so there is no
   "close project" anywhere in the UI. */

const picker = { path: null, mode: "recent", busy: false, error: "" };

async function renderProject() {
  const info = await api.get("/api/project");
  state.project = info;
  if (!info.open) {
    picker.mode = (info.recent || []).length ? "recent" : "new";
    await showPicker(info.recent || []);
    return false;
  }
  $("#picker").hidden = true;
  $(".shell").hidden = false;
  const el = $("#project");
  const target = info.target || {};
  const bad = target.reachable === false;
  el.hidden = false;
  el.className = `project${bad ? " bad" : ""}`;
  el.innerHTML = `
    ${info.logo ? `<img class="project-logo" src="/api/project/logo" alt="" />` : ""}
    <div class="project-text">
      <div class="project-title" title="${esc(info.root)}">${esc(info.title)}</div>
      <button class="project-target" id="project-target" title="${esc(target.describe || "")}">
        ${bad ? "⚠ " : ""}${esc(target.describe || "no target")}
      </button>
    </div>`;
  const button = $("#project-target");
  if (button) {
    button.addEventListener("click", (ev) => {
      const items = [
        { label: `Project  ${info.root}` },
        { label: `Pipelines  ${info.pipelines_dir}` },
        { label: `Files  ${info.files_root}` },
        { label: `Target  ${target.describe || "-"}` },
      ];
      if (target.schema) items.push({ label: `Schema  ${target.schema}` });
      if (bad) items.push({ label: `Unreachable: ${target.error || "unknown error"}` });
      contextMenu(ev, items.map((i) => ({ ...i, onClick: () => {} })));
    });
  }
  return true;
}

async function showPicker(recent) {
  $(".shell").hidden = true;
  const el = $("#picker");
  el.hidden = false;
  el.innerHTML = `
    <div class="picker-card">
      <div class="picker-head">
        <div class="brand-mark">G</div>
        <div>
          <div class="picker-title">Open a project</div>
          <div class="picker-sub">A project is one data warehouse and the pipelines that fill it.</div>
        </div>
      </div>
      <div class="picker-tabs">
        <button class="picker-tab" data-mode="recent">Recent</button>
        <button class="picker-tab" data-mode="browse">Browse</button>
        <button class="picker-tab" data-mode="new">New project</button>
      </div>
      ${picker.error ? `<div class="banner error">${esc(picker.error)}</div>` : ""}
      <div class="picker-body" id="picker-body"></div>
    </div>`;
  el.querySelectorAll(".picker-tab").forEach((tab) => {
    tab.classList.toggle("on", tab.dataset.mode === picker.mode);
    tab.addEventListener("click", async () => {
      picker.mode = tab.dataset.mode;
      picker.error = "";
      await showPicker(recent);
    });
  });
  if (picker.mode === "recent") renderPickerRecent(recent);
  else if (picker.mode === "browse") await renderPickerBrowse();
  else renderPickerNew();
}

function renderPickerRecent(recent) {
  const body = $("#picker-body");
  if (!recent.length) {
    body.innerHTML = `<div class="picker-empty">No projects opened yet.</div>`;
    return;
  }
  body.innerHTML = `<div class="picker-list">${recent
    .map(
      (r, i) => `<div class="picker-row" data-i="${i}">
        <div class="picker-row-main">
          <div class="picker-row-title">${esc(r.title || r.name)}</div>
          <div class="picker-row-path">${esc(r.root)}</div>
        </div>
        <span class="pill">${esc(r.target || "?")}</span>
        <button class="icon-btn picker-forget" data-i="${i}" title="Remove from this list">×</button>
      </div>`
    )
    .join("")}</div>`;
  body.querySelectorAll(".picker-row").forEach((row) => {
    row.addEventListener("click", () => openProject(recent[Number(row.dataset.i)].root));
  });
  body.querySelectorAll(".picker-forget").forEach((button) => {
    button.addEventListener("click", async (ev) => {
      ev.stopPropagation(); // forgetting must never also open it
      const entry = recent[Number(button.dataset.i)];
      const data = await api.post("/api/project/forget", { path: entry.root });
      await showPicker(data.recent || []);
    });
  });
}

async function renderPickerBrowse() {
  const body = $("#picker-body");
  let data;
  try {
    data = await api.get(`/api/project/browse${picker.path ? `?path=${encodeURIComponent(picker.path)}` : ""}`);
  } catch (err) {
    body.innerHTML = `<div class="banner error">${esc(err.message)}</div>`;
    return;
  }
  picker.path = data.path;
  body.innerHTML = `
    <div class="picker-path">${esc(data.path)}</div>
    <div class="picker-list">
      ${data.parent ? `<div class="picker-row up" data-up="1"><div class="picker-row-main">..</div></div>` : ""}
      ${data.entries
        .map(
          (e, i) => `<div class="picker-row" data-i="${i}">
            <div class="picker-row-main">${esc(e.name)}</div>
            ${e.project ? `<span class="pill ok">project</span>` : ""}
          </div>`
        )
        .join("")}
    </div>
    <div class="picker-actions">
      <button class="btn" id="pick-here">Use this folder for a new project</button>
    </div>`;
  const up = body.querySelector(".picker-row.up");
  if (up) up.addEventListener("click", async () => { picker.path = data.parent; await renderPickerBrowse(); });
  body.querySelectorAll(".picker-row:not(.up)").forEach((row) => {
    row.addEventListener("click", async () => {
      const entry = data.entries[Number(row.dataset.i)];
      if (entry.project) return openProject(entry.path);
      picker.path = entry.path;
      await renderPickerBrowse();
    });
  });
  $("#pick-here").addEventListener("click", async () => {
    picker.mode = "new";
    await showPicker([]);
  });
}

function renderPickerNew() {
  const body = $("#picker-body");
  const suggestion = picker.path ? `${picker.path}/warehouse` : "";
  body.innerHTML = `
    <div class="form">
      <label>Folder<input id="np-path" value="${esc(suggestion)}" placeholder="/path/to/warehouse" /></label>
      <label>Title<input id="np-title" placeholder="My Warehouse" /></label>
      <label>Target database
        <select id="np-system">
          <option value="sqlite">SQLite (a file in the project)</option>
          <option value="postgres">PostgreSQL</option>
        </select>
      </label>
      <div id="np-pg" hidden>
        <label>DSN<input id="np-dsn" placeholder="postgresql://user:\${PGPASSWORD}@host:5432/warehouse" /></label>
        <label>Schema for GraETL's own tables<input id="np-schema" value="graetl" /></label>
        <div class="hint">A \${VAR} in the DSN is read from the environment or a git-ignored .env, so no password is committed.</div>
      </div>
      <div class="picker-actions">
        <button class="btn primary" id="np-create">Create project</button>
      </div>
    </div>`;
  const system = $("#np-system");
  system.addEventListener("change", () => { $("#np-pg").hidden = system.value !== "postgres"; });
  $("#np-create").addEventListener("click", async () => {
    if (picker.busy) return;
    picker.busy = true;
    try {
      const payload = {
        path: $("#np-path").value.trim(),
        title: $("#np-title").value.trim(),
        system: system.value,
        dsn: system.value === "postgres" ? $("#np-dsn").value.trim() : "",
        schema: system.value === "postgres" ? $("#np-schema").value.trim() || "graetl" : "graetl",
      };
      await api.post("/api/project/create", payload);
      await enterConsole();
    } catch (err) {
      picker.error = err.message;
      picker.busy = false;
      await showPicker([]);
      return;
    }
    picker.busy = false;
  });
}

async function openProject(path) {
  if (picker.busy) return;
  picker.busy = true;
  try {
    await api.post("/api/project/open", { path });
    await enterConsole();
  } catch (err) {
    picker.error = err.message;
    const info = await api.get("/api/project").catch(() => ({ recent: [] }));
    await showPicker(info.recent || []);
  } finally {
    picker.busy = false;
  }
}

/** Finish startup once a project exists: load what boot() skipped. */
async function enterConsole() {
  picker.error = "";
  state.health = await api.get("/api/health");
  state.pipelines = await api.get("/api/pipelines");
  await renderProject();
  renderHealth();
  renderSidebar();
  await render();
  connectSystemSocket();
}

async function boot() {
  $("#btn-sync").addEventListener("click", syncPipelines);
  $("#btn-new").addEventListener("click", openNewPipeline);
  $("#modal").addEventListener("click", (ev) => {
    if (ev.target.id === "modal" || ev.target.hasAttribute("data-close")) $("#modal").hidden = true;
  });
  window.addEventListener("hashchange", render);
  wireShortcuts();

  let opened = false;
  try {
    state.health = await api.get("/api/health");
    // Nothing else is worth loading until a warehouse is open - every other
    // endpoint answers 503 until then.
    opened = await renderProject();
    if (opened) state.pipelines = await api.get("/api/pipelines");
  } catch (err) {
    toast(`Cannot reach the GraETL server: ${err.message}`, "error");
  }
  if (!opened) {
    configureMonaco(state.health?.monaco_vendored ? "" : state.health?.monaco_url ?? "");
    return;
  }
  // A vendored copy makes the editor fully offline; otherwise follow the
  // configured URL (an empty one means "stay offline, use the small editor").
  configureMonaco(state.health?.monaco_vendored ? "" : state.health?.monaco_url ?? "");
  renderHealth();
  await render();
  connectSystemSocket();
  setInterval(async () => {
    try { state.health = await api.get("/api/health"); renderHealth(); } catch { /* offline */ }
  }, 15000);
}

boot();
