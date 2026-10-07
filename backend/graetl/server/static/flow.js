/* Pipeline flow view ---------------------------------------------------------
 * The pipeline as one picture, left to right in execution order:
 *
 *   discover -> layer 0 -> layer 1 -> ... -> post tasks -> tables
 *
 * Each layer is a lane; a lane boundary is a barrier (everything to its left is
 * up to date for an entity before anything to its right touches it). Explicit
 * depends_on edges are drawn, the implied "all lower layers" edges are not -
 * they would turn the picture into a hairball and the barrier already says it.
 * Every module card carries a health bar measured against its *current*
 * version and code, and the tables it writes; hovering a table on the right
 * lights up every module writing it.
 * ------------------------------------------------------------------------- */

const SVG = "http://www.w3.org/2000/svg";

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[c]);
}

const fmt = (n) => (n === null || n === undefined ? "–" : Number(n).toLocaleString());

/** Segments of a module's health bar, in reading order. */
function segments(h) {
  if (!h) return [];
  return [
    { key: "current", n: h.current - (h.drift || 0), cls: "ok", label: "current" },
    { key: "drift", n: h.drift || 0, cls: "drift", label: "current version, but older code" },
    { key: "outdated", n: (h.outdated_version || 0) + (h.outdated_source || 0) + (h.upstream_changed || 0),
      cls: "warn", label: "outdated (other version, source or upstream changed)" },
    { key: "failed", n: h.failed || 0, cls: "err", label: "failed" },
    { key: "pending", n: h.pending || 0, cls: "pend", label: "pending" },
    { key: "never", n: Math.max(0, h.never || 0), cls: "never", label: "not processed yet" },
  ].filter((s) => s.n > 0);
}

/** Order a lane so a module sits below what it depends on inside that lane. */
function laneOrder(mods) {
  const names = new Set(mods.map((m) => m.name));
  const placed = [];
  const done = new Set();
  let rest = [...mods];
  while (rest.length) {
    const ready = rest.filter((m) => m.depends_on.every((d) => !names.has(d) || done.has(d)));
    const take = ready.length ? ready : rest;
    take.forEach((m) => { placed.push(m); done.add(m.name); });
    rest = rest.filter((m) => !take.includes(m));
  }
  return placed;
}

function moduleCard(m, entities) {
  const h = m.health;
  const segs = segments(h);
  const total = Math.max(1, entities || 0);
  const once = m.scope === "once";
  const work = m.work;
  const upToDate = h && h.current === entities && !h.drift && !(work && work.due);
  const bar = once
    ? `<div class="fc-bar once" title="scope=once: runs on every run, keeps no entity state"></div>`
    : `<div class="fc-bar">${segs.map((s) => `<span class="seg ${s.cls}" data-seg="${s.key}"
          style="flex:${s.n / total}" title="${esc(fmt(s.n))} ${esc(s.label)}"></span>`).join("")}</div>`;
  const status = once
    ? `<div class="fc-nums dim">runs once per run</div>`
    : h
      ? `<div class="fc-nums"><b>${fmt(h.current)}</b><span class="dim"> / ${fmt(entities)} current</span>
          ${upToDate ? '<span class="fc-ok" title="every entity processed by this version and code">✓</span>' : ""}</div>`
      : `<div class="fc-nums dim">no state yet</div>`;
  const workLine = work && (work.due || 0) > 0
    ? `<div class="fc-work"><span title="a run processes these now">${fmt(work.ready)} to run</span>${work.waiting
        ? ` · <span class="dim" title="waiting for a lower layer or dependency">${fmt(work.waiting)} waiting</span>` : ""}</div>`
    : "";
  const drift = h && h.drift
    ? `<div class="fc-warn" title="These entities were processed at v${esc(m.version)} by different code. Bump the version so they are reprocessed - otherwise results at this version depend on when an entity ran.">⚠ code changed at v${esc(m.version)} · ${fmt(h.drift)}</div>`
    : "";
  const versions = h && Object.keys(h.versions || {}).length > 1
    ? `<div class="fc-versions dim">${Object.entries(h.versions)
        .sort((a, b) => Number(b[0]) - Number(a[0]))
        .map(([v, n]) => `<span class="${Number(v) === Number(m.version) ? "" : "old"}">v${esc(v)}: ${fmt(n)}</span>`)
        .join(" ")}</div>`
    : "";
  return `<div class="fc ${once ? "is-once" : ""}" data-module="${esc(m.name)}" data-file="${esc(m.file || "")}"
      title="${esc(m.description || m.name)}\nDouble-click to edit · right-click for actions">
    <div class="fc-top">
      <span class="fc-name mono">${esc(m.name)}</span>
      <span class="fc-ver mono" title="module version${m.code_hash ? ` · code ${esc(m.code_hash)}` : ""}">v${esc(m.version)}</span>
    </div>
    <div class="fc-meta dim">${esc(m.scope)}${m.source === "graph" ? " · graph" : ""}${m.code_hash ? ` · <span class="mono">${esc(m.code_hash.slice(0, 7))}</span>` : ""}</div>
    ${bar}${status}${workLine}${drift}${versions}
    ${(m.outputs || []).length ? `<div class="fc-outs">${m.outputs.map((o) => `<span class="fc-out ${esc(o.mode)}"
        title="${o.mode === "owned" ? "owns its rows here (ctx.write): reprocessing replaces them" : "upserts by key (ctx.upsert): shared, never deleted"}">${o.mode === "owned" ? "→" : "⇢"} ${esc(o.table)}</span>`).join("")}</div>` : ""}
  </div>`;
}

/**
 * Render the flow into ``host``.
 * handlers: { onOpen(module), onMenu(event, module), onSegment(module, key), onTable(table) }
 */
export function renderFlow(host, flow, handlers = {}) {
  const mods = flow.modules || [];
  const layers = flow.layers || [];
  const pre = (flow.tasks || []).filter((t) => t.phase === "pre");
  const post = (flow.tasks || []).filter((t) => t.phase === "post");
  const tables = flow.tables || [];

  const lanes = layers.map((layer) => ({
    layer,
    mods: laneOrder(mods.filter((m) => m.layer === layer)),
  }));

  const behind = mods.filter((m) => m.work && m.work.due > 0).length;
  const failing = mods.filter((m) => m.health && m.health.failed > 0).length;
  const drifting = mods.filter((m) => m.health && m.health.drift > 0).length;
  const summary = !mods.length
    ? "No modules yet"
    : [
        behind ? `${behind} module(s) have work` : "Every module is up to date",
        failing ? `${failing} with failures` : "",
        drifting ? `${drifting} changed without a version bump` : "",
      ].filter(Boolean).join(" · ");

  host.innerHTML = `
    <div class="flow-head">
      <span class="flow-summary ${behind || failing || drifting ? "" : "good"}">${esc(summary)}</span>
      <div class="spacer"></div>
      <div class="flow-legend">
        <span><i class="seg ok"></i>current</span>
        <span><i class="seg drift"></i>code drift</span>
        <span><i class="seg warn"></i>outdated</span>
        <span><i class="seg err"></i>failed</span>
        <span><i class="seg pend"></i>pending</span>
        <span><i class="seg never"></i>not yet</span>
      </div>
    </div>
    ${flow.error ? `<div class="banner">${esc(flow.error)}</div>` : ""}
    <div class="flow-scroll"><div class="flow">
      <svg class="flow-edges"></svg>
      <div class="flow-lane source">
        <div class="flow-lane-head">Source</div>
        ${pre.map((t) => `<div class="fc task"><div class="fc-top"><span class="fc-name mono">${esc(t.name)}</span>
          <span class="fc-ver mono">v${esc(t.version)}</span></div><div class="fc-meta dim">pre task</div></div>`).join("")}
        <div class="fc source-card" data-source>
          <div class="fc-top"><span class="fc-name">Entities</span></div>
          <div class="fc-big">${fmt(flow.entities)}</div>
          <div class="fc-meta dim">${flow.stateful ? "discovered from the source" : "stateless pipeline"}</div>
        </div>
      </div>
      ${lanes.map((lane) => `
        <div class="flow-lane" data-layer="${lane.layer}">
          <div class="flow-lane-head" title="Everything to the left is up to date for an entity before layer ${lane.layer} touches it">Layer ${esc(lane.layer)}</div>
          ${lane.mods.map((m) => moduleCard(m, flow.entities)).join("")}
        </div>`).join("")}
      ${post.length ? `<div class="flow-lane">
        <div class="flow-lane-head">Post</div>
        ${post.map((t) => `<div class="fc task"><div class="fc-top"><span class="fc-name mono">${esc(t.name)}</span>
          <span class="fc-ver mono">v${esc(t.version)}</span></div><div class="fc-meta dim">post task</div></div>`).join("")}
      </div>` : ""}
      ${tables.length ? `<div class="flow-lane tables">
        <div class="flow-lane-head" title="Warehouse tables modules write with ctx.write (owned) or ctx.upsert (shared)">Tables</div>
        ${tables.map((t) => `<div class="ft" data-table="${esc(t.table)}">
          <span class="mono">${esc(t.table)}</span>
          <span class="dim">${t.writers.map((w) => esc(w.module)).join(", ")}</span></div>`).join("")}
      </div>` : ""}
    </div></div>`;

  const flowEl = host.querySelector(".flow");
  const svg = host.querySelector(".flow-edges");

  const draw = () => {
    if (!flowEl.isConnected) return;
    const box = flowEl.getBoundingClientRect();
    svg.setAttribute("width", flowEl.scrollWidth);
    svg.setAttribute("height", flowEl.scrollHeight);
    svg.innerHTML = "";
    const anchor = (el, side) => {
      const r = el.getBoundingClientRect();
      return {
        x: (side === "right" ? r.right : r.left) - box.left,
        y: r.top + Math.min(r.height / 2, 22) - box.top,
      };
    };
    const curve = (a, b, cls, title) => {
      const path = document.createElementNS(SVG, "path");
      const dx = Math.max(30, (b.x - a.x) / 2);
      path.setAttribute("d", `M${a.x},${a.y} C${a.x + dx},${a.y} ${b.x - dx},${b.y} ${b.x},${b.y}`);
      path.setAttribute("class", cls);
      if (title) {
        const t = document.createElementNS(SVG, "title");
        t.textContent = title;
        path.appendChild(t);
      }
      svg.appendChild(path);
      return path;
    };
    const card = (name) => flowEl.querySelector(`.fc[data-module="${CSS.escape(name)}"]`);
    for (const m of mods) {
      const to = card(m.name);
      if (!to) continue;
      for (const dep of m.depends_on || []) {
        const from = card(dep);
        if (!from) continue;
        const same = from.closest(".flow-lane") === to.closest(".flow-lane");
        if (same) {
          // Inside a lane: a bracket on the left edge, from dependency down to dependant.
          const a = anchor(from, "left");
          const b = anchor(to, "left");
          const path = document.createElementNS(SVG, "path");
          path.setAttribute("d", `M${a.x},${a.y} C${a.x - 26},${a.y} ${b.x - 26},${b.y} ${b.x},${b.y}`);
          path.setAttribute("class", "edge dep");
          path.dataset.from = dep; path.dataset.to = m.name;
          svg.appendChild(path);
        } else {
          const p = curve(anchor(from, "right"), anchor(to, "left"), "edge dep", `${dep} → ${m.name}`);
          p.dataset.from = dep; p.dataset.to = m.name;
        }
      }
    }
  };

  // Hover: light up what a module touches, dim the rest.
  const focus = (name) => {
    flowEl.classList.toggle("focusing", !!name);
    if (!name) {
      flowEl.querySelectorAll(".hot").forEach((el) => el.classList.remove("hot"));
      return;
    }
    const related = new Set([name]);
    const m = mods.find((x) => x.name === name);
    (m?.depends_on || []).forEach((d) => related.add(d));
    mods.filter((x) => (x.depends_on || []).includes(name)).forEach((x) => related.add(x.name));
    flowEl.querySelectorAll(".fc[data-module]").forEach((el) =>
      el.classList.toggle("hot", related.has(el.dataset.module)));
    flowEl.querySelectorAll(".ft").forEach((el) =>
      el.classList.toggle("hot", (m?.outputs || []).some((o) => o.table === el.dataset.table)));
    svg.querySelectorAll(".edge").forEach((el) =>
      el.classList.toggle("hot", el.dataset.from === name || el.dataset.to === name));
  };

  flowEl.querySelectorAll(".fc[data-module]").forEach((el) => {
    const name = el.dataset.module;
    el.addEventListener("mouseenter", () => focus(name));
    el.addEventListener("mouseleave", () => focus(null));
    el.addEventListener("dblclick", () => handlers.onOpen && handlers.onOpen(name, el.dataset.file));
    el.addEventListener("contextmenu", (ev) => handlers.onMenu && handlers.onMenu(ev, name, el.dataset.file));
    el.querySelectorAll("[data-seg]").forEach((seg) => seg.addEventListener("click", (ev) => {
      ev.stopPropagation();
      handlers.onSegment && handlers.onSegment(name, seg.dataset.seg);
    }));
  });
  flowEl.querySelectorAll(".ft").forEach((el) => {
    el.addEventListener("mouseenter", () => {
      flowEl.classList.add("focusing");
      const writers = new Set(tables.find((t) => t.table === el.dataset.table)?.writers.map((w) => w.module));
      el.classList.add("hot");
      flowEl.querySelectorAll(".fc[data-module]").forEach((c) => c.classList.toggle("hot", writers.has(c.dataset.module)));
      svg.querySelectorAll(".edge").forEach((e) => e.classList.remove("hot"));
    });
    el.addEventListener("mouseleave", () => focus(null));
  });

  requestAnimationFrame(draw);
  const ro = new ResizeObserver(() => draw());
  ro.observe(flowEl);
  return { redraw: draw, dispose: () => ro.disconnect() };
}
