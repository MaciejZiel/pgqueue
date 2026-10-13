"use strict";

(() => {
  const POLL_MS = 2000;
  const HISTORY = 120;
  const PROBLEMS_EVERY = 4; // refresh the problems table every Nth snapshot
  const READ_ONLY = document.body.dataset.readOnly === "true";
  const ACTION_HEADERS = { "X-PgQueue-Action": "1" };
  const state = {
    paused: false,
    problemStatus: "retrying",
    source: null,
    pollTimer: null,
    last: null,
    ticks: 0,
    depth: new Map(), // queue -> recent depth samples, kept in the browser
    seenFeed: new Set(),
    firstFeed: true,
  };
  const $ = (id) => document.getElementById(id);
  const fmt = new Intl.NumberFormat();
  const NS = "http://www.w3.org/2000/svg";

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

  function svg(tag, attrs = {}, text) {
    const node = document.createElementNS(NS, tag);
    for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
    if (text !== undefined) node.textContent = text;
    return node;
  }

  const num = (value, cls = "") => el("td", { class: `num ${value === 0 ? "zero" : cls}` }, fmt.format(value));

  function ago(iso, now) {
    const seconds = Math.max(0, (now - new Date(iso)) / 1000);
    if (seconds < 1.5) return "just now";
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

  function seconds(value) {
    if (value === null || value === undefined) return "–";
    if (value < 0.0095) return `${(value * 1000).toFixed(1)} ms`;
    if (value < 0.995) return `${Math.round(value * 1000)} ms`;
    if (value < 60) return `${value.toFixed(value < 10 ? 2 : 1)} s`;
    if (value < 3600) return `${(value / 60).toFixed(1)} min`;
    return `${(value / 3600).toFixed(1)} h`;
  }

  const pair = (stats) => (stats.samples ? `${seconds(stats.p50)} / ${seconds(stats.p95)}` : "–");

  function lastLine(error) {
    if (!error) return "";
    const lines = error.trim().split("\n");
    return lines[lines.length - 1];
  }

  function shortId(id) {
    // hostname:pid:random -> pid:random keeps rows readable
    const parts = id.split(":");
    return parts.length >= 3 ? parts.slice(-2).join(":") : id;
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

  // -- headline numbers -----------------------------------------------------------------------

  function renderKpis(data) {
    const t = data.totals;
    const s = data.stats;
    $("kpi-rate").textContent = s.jobs_per_second.toFixed(1);
    $("kpi-pickup").textContent = pair(s.pickup);
    $("kpi-run").textContent = pair(s.run);
    $("kpi-ready").textContent = fmt.format(t.queued + t.retrying);
    $("kpi-running").textContent = fmt.format(t.running);
    $("kpi-retries").textContent = fmt.format(s.retries);
    $("kpi-failed").textContent = fmt.format(s.failures);
    const alive = data.workers.filter((w) => w.alive).length;
    $("workers-count").textContent = `· ${alive} alive`;
  }

  // -- throughput chart -----------------------------------------------------------------------

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
    const width = Math.max(host.clientWidth, 280);
    const height = host.clientHeight || 190;
    const pad = { top: 8, right: 4, bottom: 22, left: 36 };
    const plotW = width - pad.left - pad.right;
    const plotH = height - pad.top - pad.bottom;
    const max = niceMax(Math.max(1, ...points.map((p) => p.succeeded + p.failed)));
    const slot = plotW / points.length;
    const barW = Math.max(1, slot * 0.78);
    const y = (v) => pad.top + plotH - (v / max) * plotH;
    const root = svg("svg", { viewBox: `0 0 ${width} ${height}` });

    for (let i = 0; i <= 4; i++) {
      const value = (max / 4) * i;
      root.append(svg("line", { class: "gridline", x1: pad.left, x2: width - pad.right, y1: y(value), y2: y(value) }));
      root.append(svg("text", { class: "axis", x: pad.left - 6, y: y(value) + 4, "text-anchor": "end" }, fmt.format(Math.round(value * 10) / 10)));
    }
    points.forEach((p, i) => {
      const x = pad.left + i * slot + (slot - barW) / 2;
      const okTop = y(p.succeeded);
      if (p.succeeded) root.append(svg("rect", { class: "bar-ok", x, y: okTop, width: barW, height: pad.top + plotH - okTop }));
      if (p.failed) {
        const failTop = y(p.succeeded + p.failed);
        root.append(svg("rect", { class: "bar-fail", x, y: failTop, width: barW, height: okTop - failTop }));
      }
      const behind = points.length - 1 - i;
      if (behind % 30 === 0) {
        root.append(svg("text", { class: "axis", x: x + barW / 2, y: height - 6, "text-anchor": i === 0 ? "start" : behind === 0 ? "end" : "middle" }, behind === 0 ? "now" : `-${behind}s`));
      }
      const hit = svg("rect", { class: "hit", x: pad.left + i * slot, y: pad.top, width: slot, height: plotH });
      hit.addEventListener("mouseenter", () => showTip(host, pad.left + i * slot + slot / 2, okTop, p, behind));
      hit.addEventListener("mouseleave", () => hideTip(host));
      root.append(hit);
    });
    host.replaceChildren(root);
    const total = points.reduce((n, p) => n + p.succeeded + p.failed, 0);
    host.setAttribute("aria-label", `Throughput over the last two minutes: ${total} jobs finished, peak ${Math.max(...points.map((p) => p.succeeded + p.failed))} per second`);
  }

  function showTip(host, x, top, point, behind) {
    hideTip(host);
    const tip = el("div", { class: "tooltip" }, `${behind === 0 ? "now" : `-${behind}s`}  ok ${point.succeeded}  failed ${point.failed}`);
    tip.style.left = `${(x / host.clientWidth) * 100}%`;
    tip.style.top = `${Math.max(top, 40)}px`;
    host.append(tip);
  }

  function hideTip(host) {
    host.querySelectorAll(".tooltip").forEach((n) => n.remove());
  }

  // -- queues ---------------------------------------------------------------------------------

  function sparkline(samples) {
    const w = 200;
    const h = 44;
    const max = Math.max(5, ...samples);
    const step = w / (HISTORY - 1);
    const offset = (HISTORY - samples.length) * step;
    const pts = samples.map((v, i) => [offset + i * step, h - 2 - (v / max) * (h - 6)]);
    const line = pts.map(([px, py], i) => `${i ? "L" : "M"}${px.toFixed(1)} ${py.toFixed(1)}`).join(" ");
    const area = `${line} L${pts[pts.length - 1][0].toFixed(1)} ${h} L${pts[0][0].toFixed(1)} ${h} Z`;
    const root = svg("svg", { class: "spark", viewBox: `0 0 ${w} ${h}`, preserveAspectRatio: "none", "aria-hidden": "true" });
    root.append(svg("path", { class: "area", d: area }), svg("path", { class: "line", d: line }));
    return root;
  }

  function renderQueues(data) {
    const cards = data.queues.map((q) => {
      const depth = q.counts.queued + q.counts.retrying;
      const history = state.depth.get(q.queue) || [];
      history.push(depth);
      if (history.length > HISTORY) history.shift();
      state.depth.set(q.queue, history);
      return el("article", { class: "queue-card" },
        el("header", {},
          el("span", { class: "qname" }, q.queue),
          el("span", { class: "depth", title: "jobs waiting" }, fmt.format(depth))),
        sparkline(history),
        el("div", { class: "counts" },
          el("span", {}, "running ", el("b", {}, fmt.format(q.counts.running))),
          el("span", { class: "retrying" }, "retrying ", el("b", {}, fmt.format(q.counts.retrying))),
          el("span", { class: "failed" }, "dead ", el("b", {}, fmt.format(q.counts.dead + q.counts.failed))),
          el("span", {}, "lag ", el("b", {}, seconds(q.lag_seconds)))));
    });
    $("queues").replaceChildren(...(cards.length ? cards : [el("p", { class: "empty" }, "No jobs yet")]));
  }

  // -- workers and leases ---------------------------------------------------------------------

  function renderWorkers(data) {
    const now = new Date(data.now);
    const cards = data.workers.map((w) => {
      const pct = Math.min(100, Math.round((w.in_flight / w.concurrency) * 100));
      const age = (now - new Date(w.last_seen_at)) / 1000;
      return el("article", { class: `worker ${w.alive ? "" : "lost"}` },
        el("div", { class: "top" },
          el("span", { class: "wid", title: w.hostname ? `${w.id} · ${w.hostname} pid ${w.pid}` : w.id }, shortId(w.id)),
          el("span", { class: "state" }, w.alive ? "alive" : "lost")),
        el("div", { class: "meta-line" },
          el("span", {}, `heartbeat ${age < 1.5 ? "just now" : `${age.toFixed(0)}s ago`}`),
          el("span", {}, `lease ${w.lease_seconds}s`),
          el("span", {}, `done ${fmt.format(w.processed)}`),
          el("span", {}, `failed ${fmt.format(w.failed)}`),
          el("span", {}, w.queues.join(", "))),
        el("div", { class: "busy" },
          el("div", { class: "track" }, el("div", { class: "fill", style: `width:${pct}%` })),
          el("span", { class: "label" }, `${w.in_flight}/${w.concurrency} slots`)));
    });
    $("workers").replaceChildren(...(cards.length ? cards : [el("p", { class: "empty" }, "No workers running")]));
  }

  function renderLeases(data) {
    const now = new Date(data.now);
    const workerLease = new Map(data.workers.map((w) => [w.id, w.lease_seconds]));
    const rows = data.running.map((job) => {
      const left = (new Date(job.locked_until) - now) / 1000;
      const total = workerLease.get(job.locked_by) || 30;
      const pct = Math.max(0, Math.min(100, (left / total) * 100));
      return el("tr", {},
        el("td", { class: "num" }, el("button", { type: "button", class: "link", onclick: () => openJob(job.id) }, `#${job.id}`)),
        el("td", { class: "mono" }, job.task),
        el("td", {}, el("span", { class: "chip" }, job.queue)),
        el("td", { class: "mono" }, shortId(job.locked_by)),
        el("td", { class: "num" }, `${job.attempts}/${job.max_attempts}`),
        el("td", { class: "num" }, seconds((now - new Date(job.started_at)) / 1000)),
        el("td", {},
          el("div", { class: "busy" },
            el("div", { class: "track" }, el("div", { class: `fill lease ${pct < 35 ? "low" : ""}`, style: `width:${pct}%` })),
            el("span", { class: "label" }, left > 0 ? `${left.toFixed(0)}s` : "expired"))));
    });
    replaceRows($("leases"), rows, 7, "Nothing is running right now");
  }

  // -- feed, problems, schedules ---------------------------------------------------------------

  function renderFeed(data) {
    const now = new Date(data.now);
    const items = data.feed.map((item) => {
      const key = `${item.id}:${item.status}:${item.attempts}`;
      const fresh = !state.firstFeed && !state.seenFeed.has(key);
      state.seenFeed.add(key);
      const detail = item.status === "succeeded" && item.duration !== null
        ? `${seconds(item.duration)}`
        : `attempt ${item.attempts}/${item.max_attempts}`;
      return el("li", { class: fresh ? "fresh" : "" },
        el("span", { class: `pill ${item.status}` }, item.status),
        el("span", { class: "what" },
          `#${item.id} ${item.task} `, el("span", { class: "q" }, `· ${item.queue} · ${detail}`),
          item.error ? el("span", { class: "err", title: item.error }, item.error) : null),
        el("span", { class: "when" }, ago(item.at, now)));
    });
    state.firstFeed = false;
    if (state.seenFeed.size > 2000) state.seenFeed = new Set([...state.seenFeed].slice(-500));
    $("feed").replaceChildren(...(items.length ? items : [el("li", { class: "empty" }, "Waiting for jobs…")]));
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
      const count = data.totals[button.dataset.status] || 0;
      let badge = button.querySelector(".count");
      if (!badge) { badge = el("span", { class: "count" }); button.append(badge); }
      badge.textContent = fmt.format(count);
    });
    if (READ_ONLY) return;
    const status = state.problemStatus;
    const buttons = data.queues
      .filter((q) => q.counts[status] > 0)
      .map((q) => el("button", { type: "button", onclick: (e) => requeueQueue(e.currentTarget, q.queue, status) },
        `Requeue all ${status} in `, el("span", { class: "mono" }, q.queue), ` (${fmt.format(q.counts[status])})`));
    $("bulk").replaceChildren(...buttons);
  }

  async function renderProblems(now) {
    const page = await api(`api/jobs?status=${state.problemStatus}&limit=15`);
    const rows = page.jobs.map((job) =>
      el("tr", {},
        el("td", { class: "num" }, el("button", { type: "button", class: "link", onclick: () => openJob(job.id) }, `#${job.id}`)),
        el("td", { class: "mono" }, job.task),
        el("td", {}, el("span", { class: "chip" }, job.queue)),
        el("td", { class: "num" }, `${job.attempts}/${job.max_attempts}`),
        el("td", {}, el("div", { class: "error-line", title: lastLine(job.last_error) }, lastLine(job.last_error))),
        el("td", { class: "num" }, job.status === "retrying" ? until(job.run_at, now) : ago(job.finished_at || job.run_at, now)),
        READ_ONLY ? null : el("td", { class: "num act" }, el("button", { type: "button", onclick: (e) => requeueJob(e.currentTarget, job.id) },
          job.status === "retrying" ? "Run now" : "Requeue")),
      ));
    replaceRows($("problems"), rows, READ_ONLY ? 6 : 7, "Nothing here");
  }

  // -- actions (only in the full admin, never in a read-only deployment) -------------------------

  async function requeueJob(button, id) {
    button.disabled = true;
    try {
      await api(`api/jobs/${id}/requeue`, { method: "POST", headers: ACTION_HEADERS });
      toast(`Job #${id} requeued`);
      await renderProblems(new Date());
    } catch (error) {
      toast(`Could not requeue #${id}: ${error.message}`);
      button.disabled = false;
    }
  }

  async function requeueQueue(button, queue, status) {
    button.disabled = true;
    try {
      const result = await api(`api/queues/${encodeURIComponent(queue)}/requeue?status=${status}`, {
        method: "POST", headers: ACTION_HEADERS,
      });
      toast(`Requeued ${fmt.format(result.requeued)} ${status} job(s) in ${queue}`);
      await renderProblems(new Date());
    } catch (error) {
      toast(`Bulk requeue failed: ${error.message}`);
      button.disabled = false;
    }
  }

  async function openJob(id) {
    let job;
    try { job = await api(`api/jobs/${id}`); } catch (error) { toast(error.message); return; }
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
    $("drawer-close").focus();
  }

  // -- live updates: server-sent events, with polling as the fallback ---------------------------

  function render(data) {
    state.last = data;
    renderKpis(data);
    renderChart(data.stats.throughput);
    renderQueues(data);
    renderWorkers(data);
    renderLeases(data);
    renderFeed(data);
    renderPeriodic(data);
    renderTabs(data);
    if (state.ticks++ % PROBLEMS_EVERY === 0) renderProblems(new Date(data.now)).catch(() => {});
  }

  function setStatus(kind, text) {
    $("live-dot").className = `dot ${kind}`;
    $("live-text").textContent = text;
  }

  function receive(data, via) {
    render(data);
    setStatus("live", `live · ${via}`);
  }

  async function poll() {
    try {
      receive(await api("api/live"), "polling");
    } catch (error) {
      setStatus("error", `disconnected (${error.message})`);
    }
    if (!state.paused && !state.source) state.pollTimer = setTimeout(poll, POLL_MS);
  }

  function connect() {
    stop();
    if (typeof EventSource === "undefined") { poll(); return; }
    const source = new EventSource("api/stream");
    state.source = source;
    source.onmessage = (event) => receive(JSON.parse(event.data), "streaming");
    source.onerror = () => {
      if (source.readyState === EventSource.CLOSED) {
        // the server refused the stream (e.g. too many viewers): fall back to polling
        stop();
        poll();
      } else {
        setStatus("error", "reconnecting…");
      }
    };
  }

  function stop() {
    if (state.source) { state.source.close(); state.source = null; }
    clearTimeout(state.pollTimer);
  }

  $("pause").addEventListener("click", (event) => {
    state.paused = !state.paused;
    event.currentTarget.textContent = state.paused ? "Resume" : "Pause";
    event.currentTarget.setAttribute("aria-pressed", String(state.paused));
    if (state.paused) { stop(); setStatus("", "paused"); } else { setStatus("", "resuming…"); connect(); }
  });

  document.querySelectorAll(".tabs button").forEach((button) => {
    button.addEventListener("click", () => {
      state.problemStatus = button.dataset.status;
      document.querySelectorAll(".tabs button").forEach((b) => b.setAttribute("aria-selected", String(b === button)));
      if (state.last) renderTabs(state.last);
      renderProblems(state.last ? new Date(state.last.now) : new Date()).catch(() => {});
    });
  });

  $("drawer-close").addEventListener("click", () => { $("drawer").hidden = true; });
  document.addEventListener("keydown", (event) => { if (event.key === "Escape") $("drawer").hidden = true; });
  let resizeTimer;
  window.addEventListener("resize", () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => { if (state.last) renderChart(state.last.stats.throughput); }, 120);
  });
  document.addEventListener("visibilitychange", () => {
    if (state.paused) return;
    if (document.hidden) stop(); else connect();
  });

  connect();
})();
