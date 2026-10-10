"use strict";

(() => {
  const REFRESH_MS = 2000;
  const ACTION_HEADERS = { "X-PgQueue-Action": "1" };
  const state = { paused: false, problemStatus: "dead", timer: null, lastOverview: null };
  const $ = (id) => document.getElementById(id);
  const fmt = new Intl.NumberFormat();

  // -- helpers ------------------------------------------------------------------------------

  function el(tag, attrs = {}, ...children) {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(attrs)) {
      if (key === "class") node.className = value;
      else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
      else if (value !== null && value !== undefined) node.setAttribute(key, value);
    }
    for (const child of children.flat()) {
      if (child === null || child === undefined) continue;
      node.append(child instanceof Node ? child : document.createTextNode(String(child)));
    }
    return node;
  }

  function num(value, cls = "") {
    return el("td", { class: `num ${value === 0 ? "zero" : cls}` }, fmt.format(value));
  }

  function ago(iso, now) {
    const seconds = Math.max(0, (now - new Date(iso)) / 1000);
    if (seconds < 60) return `${Math.round(seconds)}s ago`;
    if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.round(seconds / 3600)}h ago`;
    return `${Math.round(seconds / 86400)}d ago`;
  }

  function until(iso, now) {
    const seconds = (new Date(iso) - now) / 1000;
    if (seconds <= 0) return "due";
    if (seconds < 60) return `in ${Math.round(seconds)}s`;
    if (seconds < 3600) return `in ${Math.round(seconds / 60)}m`;
    return `in ${Math.round(seconds / 3600)}h`;
  }

  function duration(seconds) {
    if (seconds < 1) return "0s";
    if (seconds < 60) return `${seconds.toFixed(0)}s`;
    if (seconds < 3600) return `${(seconds / 60).toFixed(1)}m`;
    return `${(seconds / 3600).toFixed(1)}h`;
  }

  function firstLine(error) {
    if (!error) return "";
    const lines = error.trim().split("\n");
    return lines[lines.length - 1];
  }

  function replaceRows(tbody, rows, colspan, emptyText) {
    tbody.replaceChildren(
      ...(rows.length ? rows : [el("tr", {}, el("td", { colspan, class: "empty" }, emptyText))]),
    );
  }

  function toast(message) {
    const node = $("toast");
    node.textContent = message;
    node.hidden = false;
    clearTimeout(toast.timer);
    toast.timer = setTimeout(() => { node.hidden = true; }, 2600);
  }

  async function api(path, options = {}) {
    const response = await fetch(path, { credentials: "same-origin", ...options });
    if (!response.ok) {
      let detail = response.statusText;
      try { detail = (await response.json()).detail || detail; } catch { /* not JSON */ }
      throw new Error(`${response.status}: ${detail}`);
    }
    return response.json();
  }

  // -- rendering ------------------------------------------------------------------------------

  function renderKpis(data) {
    const t = data.totals;
    const last = data.throughput.slice(-2, -1)[0] || { succeeded: 0 };
    const alive = data.workers.filter((w) => w.alive).length;
    $("kpi-ready").textContent = fmt.format(t.queued);
    $("kpi-running").textContent = fmt.format(t.running);
    $("kpi-retrying").textContent = fmt.format(t.retrying);
    $("kpi-failed").textContent = fmt.format(t.failed);
    $("kpi-dead").textContent = fmt.format(t.dead);
    $("kpi-rate").textContent = fmt.format(last.succeeded);
    $("kpi-workers").textContent = `${alive}`;
  }

  function niceMax(value) {
    if (value <= 5) return 5;
    const magnitude = 10 ** Math.floor(Math.log10(value));
    for (const step of [1, 2, 2.5, 5, 10]) {
      if (value <= step * magnitude) return step * magnitude;
    }
    return 10 * magnitude;
  }

  function renderChart(points) {
    const host = $("chart");
    const width = Math.max(host.clientWidth, 320);
    const height = host.clientHeight || 190;
    const pad = { top: 8, right: 4, bottom: 22, left: 44 };
    const plotW = width - pad.left - pad.right;
    const plotH = height - pad.top - pad.bottom;
    const max = niceMax(Math.max(1, ...points.map((p) => p.succeeded + p.failed)));
    const slot = plotW / points.length;
    const barW = Math.max(1, slot * 0.72);
    const y = (v) => pad.top + plotH - (v / max) * plotH;

    const ns = "http://www.w3.org/2000/svg";
    const svg = document.createElementNS(ns, "svg");
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    const add = (tag, attrs, text) => {
      const node = document.createElementNS(ns, tag);
      for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
      if (text !== undefined) node.textContent = text;
      svg.append(node);
      return node;
    };

    for (let i = 0; i <= 4; i++) {
      const value = (max / 4) * i;
      add("line", { class: "gridline", x1: pad.left, x2: width - pad.right, y1: y(value), y2: y(value) });
      add("text", { class: "axis", x: pad.left - 8, y: y(value) + 4, "text-anchor": "end" }, fmt.format(value));
    }

    points.forEach((p, i) => {
      const x = pad.left + i * slot + (slot - barW) / 2;
      const okTop = y(p.succeeded);
      if (p.succeeded) add("rect", { class: "bar-ok", x, y: okTop, width: barW, height: pad.top + plotH - okTop, rx: 1 });
      if (p.failed) {
        const failTop = y(p.succeeded + p.failed);
        add("rect", { class: "bar-fail", x, y: failTop, width: barW, height: okTop - failTop, rx: 1 });
      }
      const minute = new Date(p.minute);
      if (minute.getMinutes() % 10 === 0) {
        const label = minute.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
        add("text", { class: "axis", x: x + barW / 2, y: height - 6, "text-anchor": "middle" }, label);
      }
      const hit = add("rect", { class: "hit", x: pad.left + i * slot, y: pad.top, width: slot, height: plotH });
      hit.addEventListener("mouseenter", () => showTip(host, pad.left + i * slot + slot / 2, okTop, p));
      hit.addEventListener("mouseleave", () => hideTip(host));
    });

    host.replaceChildren(svg);
  }

  function showTip(host, x, top, point) {
    hideTip(host);
    const time = new Date(point.minute).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    const tip = el("div", { class: "tooltip" }, `${time}  ✓ ${point.succeeded}  ✗ ${point.failed}`);
    tip.style.left = `${(x / host.clientWidth) * 100}%`;
    tip.style.top = `${Math.max(top, 40)}px`;
    host.append(tip);
  }

  function hideTip(host) {
    host.querySelectorAll(".tooltip").forEach((n) => n.remove());
  }

  function renderQueues(data) {
    const rows = data.queues.map((q) =>
      el("tr", {},
        el("td", { class: "mono" }, q.queue),
        num(q.counts.queued),
        num(q.counts.running, "running"),
        num(q.counts.retrying, "retrying"),
        num(q.counts.succeeded, "succeeded"),
        num(q.counts.failed, "failed"),
        num(q.counts.dead, "failed"),
        el("td", { class: `num ${q.lag_seconds > 60 ? "failed" : q.lag_seconds < 1 ? "zero" : ""}` }, duration(q.lag_seconds)),
      ));
    replaceRows($("queues"), rows, 8, "No jobs yet");
  }

  function renderWorkers(data) {
    const now = new Date(data.now);
    const rows = data.workers.map((w) => {
      const pct = Math.min(100, Math.round((w.in_flight / w.concurrency) * 100));
      return el("tr", {},
        el("td", { class: "mono", title: `${w.hostname} pid ${w.pid}` }, w.id),
        el("td", { class: "queues-cell", title: w.queues.join(", ") }, w.queues.join(", ")),
        el("td", {},
          el("div", { class: "busy" },
            el("div", { class: "track" }, el("div", { class: "fill", style: `width:${pct}%` })),
            el("span", { class: "label" }, `${w.in_flight}/${w.concurrency}`))),
        num(w.processed, "succeeded"),
        num(w.failed, "failed"),
        el("td", { class: `num ${w.alive ? "" : "stale"}` }, w.alive ? ago(w.last_seen_at, now) : `lost · ${ago(w.last_seen_at, now)}`),
      );
    });
    replaceRows($("workers"), rows, 6, "No workers running");
  }

  function renderPeriodic(data) {
    const now = new Date(data.now);
    const rows = data.periodic.map((p) =>
      el("tr", {},
        el("td", { class: "mono" }, p.name),
        el("td", { class: "mono" }, p.cron),
        el("td", { class: "mono" }, p.task),
        el("td", {}, el("span", { class: "chip" }, p.queue)),
        el("td", { class: "num" }, until(p.next_run_at, now)),
        el("td", { class: "num" }, p.last_run_at ? ago(p.last_run_at, now) : "never"),
      ));
    replaceRows($("periodic"), rows, 6, "No schedules");
  }

  function renderTabs(data) {
    document.querySelectorAll(".tabs button").forEach((button) => {
      const status = button.dataset.status;
      const count = data.totals[status] || 0;
      let badge = button.querySelector(".count");
      if (!badge) { badge = el("span", { class: "count" }); button.append(badge); }
      badge.textContent = fmt.format(count);
    });
    const bulk = $("bulk");
    const status = state.problemStatus;
    const buttons = data.queues
      .filter((q) => q.counts[status] > 0)
      .map((q) => el("button", { type: "button", onclick: (e) => requeueQueue(e.currentTarget, q.queue, status) },
        `Requeue all ${status} in `, el("span", { class: "mono" }, q.queue), ` (${fmt.format(q.counts[status])})`));
    bulk.replaceChildren(...buttons);
  }

  async function renderProblems(now) {
    const page = await api(`/api/jobs?status=${state.problemStatus}&limit=25`);
    const rows = page.jobs.map((job) =>
      el("tr", {},
        el("td", { class: "num" }, el("button", { type: "button", class: "link", onclick: () => openJob(job.id) }, `#${job.id}`)),
        el("td", { class: "mono" }, job.task),
        el("td", {}, el("span", { class: "chip" }, job.queue)),
        el("td", { class: "num" }, `${job.attempts}/${job.max_attempts}`),
        el("td", {}, el("div", { class: "error-line", title: firstLine(job.last_error) }, firstLine(job.last_error))),
        el("td", { class: "num" }, ago(job.finished_at || job.run_at, now)),
        el("td", { class: "num" }, el("button", { type: "button", onclick: (e) => requeueJob(e.currentTarget, job.id) },
          job.status === "retrying" ? "Run now" : "Requeue")),
      ));
    replaceRows($("problems"), rows, 7, "Nothing here");
  }

  // -- actions ----------------------------------------------------------------------------------

  async function requeueJob(button, id) {
    button.disabled = true;
    try {
      await api(`/api/jobs/${id}/requeue`, { method: "POST", headers: ACTION_HEADERS });
      toast(`Job #${id} requeued`);
      await refresh();
    } catch (error) {
      toast(`Could not requeue #${id}: ${error.message}`);
      button.disabled = false;
    }
  }

  async function requeueQueue(button, queue, status) {
    button.disabled = true;
    try {
      const result = await api(`/api/queues/${encodeURIComponent(queue)}/requeue?status=${status}`, {
        method: "POST", headers: ACTION_HEADERS,
      });
      toast(`Requeued ${fmt.format(result.requeued)} ${status} job(s) in ${queue}`);
      await refresh();
    } catch (error) {
      toast(`Bulk requeue failed: ${error.message}`);
      button.disabled = false;
    }
  }

  async function openJob(id) {
    const job = await api(`/api/jobs/${id}`);
    $("drawer-title").replaceChildren(`Job #${job.id} `, el("span", { class: `pill ${job.status}` }, job.status));
    const meta = [
      ["task", job.task], ["queue", job.queue], ["priority", job.priority],
      ["attempts", `${job.attempts} / ${job.max_attempts}`], ["created", job.created_at],
      ["run at", job.run_at], ["started", job.started_at || "–"], ["finished", job.finished_at || "–"],
      ["worker", job.locked_by || "–"], ["idempotency key", job.idempotency_key || "–"],
    ];
    $("drawer-meta").replaceChildren(...meta.flatMap(([k, v]) => [el("dt", {}, k), el("dd", {}, String(v))]));
    $("drawer-payload").textContent = JSON.stringify(job.payload, null, 2);
    $("drawer-result").textContent = job.result === null ? "–" : JSON.stringify(job.result, null, 2);
    $("drawer-error").textContent = job.last_error || "–";
    $("drawer").hidden = false;
  }

  // -- refresh loop ---------------------------------------------------------------------------

  async function refresh() {
    try {
      const data = await api("/api/overview?minutes=60");
      state.lastOverview = data;
      renderKpis(data);
      renderChart(data.throughput);
      renderQueues(data);
      renderWorkers(data);
      renderPeriodic(data);
      renderTabs(data);
      await renderProblems(new Date(data.now));
      $("live-dot").className = "dot live";
      $("live-text").textContent = `live · ${new Date().toLocaleTimeString()}`;
    } catch (error) {
      $("live-dot").className = "dot error";
      $("live-text").textContent = `disconnected (${error.message})`;
    }
  }

  function schedule() {
    clearTimeout(state.timer);
    if (!state.paused) state.timer = setTimeout(async () => { await refresh(); schedule(); }, REFRESH_MS);
  }

  $("pause").addEventListener("click", (event) => {
    state.paused = !state.paused;
    event.currentTarget.textContent = state.paused ? "Resume" : "Pause";
    event.currentTarget.setAttribute("aria-pressed", String(state.paused));
    $("live-text").textContent = state.paused ? "paused" : "resuming…";
    if (!state.paused) refresh().then(schedule);
    else clearTimeout(state.timer);
  });

  document.querySelectorAll(".tabs button").forEach((button) => {
    button.addEventListener("click", () => {
      state.problemStatus = button.dataset.status;
      document.querySelectorAll(".tabs button").forEach((b) => b.setAttribute("aria-selected", String(b === button)));
      if (state.lastOverview) renderTabs(state.lastOverview);
      renderProblems(new Date());
    });
  });

  $("drawer-close").addEventListener("click", () => { $("drawer").hidden = true; });
  document.addEventListener("keydown", (event) => { if (event.key === "Escape") $("drawer").hidden = true; });
  window.addEventListener("resize", () => { if (state.lastOverview) renderChart(state.lastOverview.throughput); });

  refresh().then(schedule);
})();
