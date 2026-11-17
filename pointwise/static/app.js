/* Pointwise console logic: vanilla JS, polls /api/state every 2s. */
const $ = (id) => document.getElementById(id);

async function api(path, opts = {}) {
  const resp = await fetch(path, {
    method: opts.method || "GET",
    headers: { "Content-Type": "application/json" },
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) flash(`错误 ${resp.status}: ${JSON.stringify(data)}`, true);
  return { ok: resp.ok, data };
}

function flash(msg, bad) {
  const el = $("flash");
  el.textContent = msg;
  el.style.borderColor = bad ? "var(--red)" : "var(--accent)";
  el.classList.add("show");
  clearTimeout(flash._t);
  flash._t = setTimeout(() => el.classList.remove("show"), 3500);
}

function esc(v) {
  return String(v ?? "").replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

function pill(text, cls) {
  return `<span class="pill ${cls}">${esc(text)}</span>`;
}

function rangeText(s) {
  const hi = s.hi === null || s.hi === undefined ? "open" : s.hi;
  return `<span class="mono">[${s.lo}, ${hi})</span>`;
}

// ------------------------------------------------------------- actions
async function initDemo() {
  const r = await api("/api/init-demo", { method: "POST", body: {} });
  if (r.ok) flash("演示初始化完成：2 个分片 + 采集器已启动");
}
async function inject() {
  const n = parseInt($("inject-n").value, 10) || 0;
  const r = await api("/api/source/inject", { method: "POST", body: { count: n } });
  if (r.ok) flash(`已注入 ${r.data.injected} 条，新 max_id=${r.data.max_id}`);
}
async function refuse() {
  const seconds = parseFloat($("refuse-s").value) || 10;
  const retry_after = parseInt($("refuse-ra").value, 10) || 2;
  const r = await api("/api/source/refuse", { method: "POST", body: { seconds, retry_after } });
  if (r.ok) flash(`来源将在 ${seconds}s 内拒绝 /records（采集器自动退避，不丢进度）`);
}
async function clearRefuse() {
  await api("/api/source/clear-refuse", { method: "POST", body: {} });
  flash("已解除拒绝窗口");
}
async function splitShard(id) {
  const r = await api(`/api/shards/${id}/split`, { method: "POST", body: {} });
  if (r.ok) flash(`拆分完成 → ${r.data.left.id} + ${r.data.right.id}（父 epoch 已作废旧令牌）`);
}
async function mergeShards(id) {
  const r = await api("/api/state");
  const actives = r.data.shards.filter((s) => ["active", "done"].includes(s.state) && s.id !== id);
  if (!actives.length) return flash("没有可合并的相邻分片", true);
  const other = actives[0].id;
  const m = await api("/api/shards/merge", { method: "POST", body: { shard_a: id, shard_b: other } });
  if (m.ok) flash(`合并完成 → ${m.data.merged.id}（双方 epoch 已作废）`);
}
async function startCollector(id) {
  const r = await api("/api/collectors/start", { method: "POST", body: { shard_id: id } });
  if (r.ok) flash(r.data.ok ? `采集器 ${r.data.collector_id} 已启动` : r.data.error);
}
async function handoff(id, mode) {
  const r = await api("/api/handoff", { method: "POST", body: { shard_id: id, mode } });
  if (r.ok) flash(`交接(${mode})：${r.data.old_collector || "无"} → ${r.data.new_collector} — ${r.data.note}`);
}
async function lifecycle(id, action) {
  await api(`/api/collectors/${id}/${action}`, { method: "POST", body: {} });
}

// ------------------------------------------------------------- rendering
async function loadEvidence() {
  const r = await api("/api/evidence");
  $("evidence").textContent = JSON.stringify(r.data, null, 2);
}
async function loadRequests() {
  const r = await api("/api/source/requests");
  const summary = (r.data.summary || [])
    .map((x) => `${x.method} ${x.path} → ${x.status} × ${x.n}`).join("\n");
  const recent = (r.data.requests || []).slice(0, 25)
    .map((x) => `${x.ts}  ${x.status}  ${x.path}`).join("\n");
  $("requests").textContent = `按路径汇总:\n${summary || "（暂无）"}\n\n最近 25 条:\n${recent || "（暂无）"}`;
}

function renderShards(shards, collectors) {
  const ownerByShard = {};
  collectors.forEach((c) => {
    if (c.alive && c.runtime) (ownerByShard[c.shard_id] ||= []).push(c);
  });
  $("shards").innerHTML = shards.map((s) => {
    const owners = (ownerByShard[s.id] || [])
      .map((c) => `${esc(c.collector_id.slice(0, 10))} ${pill(c.runtime.status, c.runtime.status)}`)
      .join("<br>") || '<span class="muted">（无租约）</span>';
    const lease = s.lease_held
      ? `${esc((s.lease_owner || "").slice(0, 10))} <span class="muted">${s.lease_remaining.toFixed(0)}s</span>`
      : owners;
    const actions = ["active", "done"].includes(s.state)
      ? `<button onclick="splitShard('${s.id}')">拆分</button>
         <button onclick="mergeShards('${s.id}')">合并</button>
         <button onclick="startCollector('${s.id}')">启动采集器</button>
         <button onclick="handoff('${s.id}','overlap')">交接·重叠</button>
         <button onclick="handoff('${s.id}','pause')">交接·暂停</button>`
      : '<span class="muted">已退役，令牌全部失效</span>';
    return `<tr>
      <td class="mono">${esc(s.id)}</td>
      <td>${rangeText(s)}</td>
      <td>${pill(s.state, s.state)}</td>
      <td class="mono">${s.epoch}</td>
      <td class="mono">${s.resume_cursor}</td>
      <td>${lease}</td>
      <td id="cov-${s.id}" class="muted">见证据面板</td>
      <td>${actions}</td>
    </tr>`;
  }).join("");
}

function renderCollectors(collectors) {
  $("collectors").innerHTML = collectors.map((c) => {
    const rt = c.runtime || {};
    const st = rt.status || "unknown";
    const stats = rt.stats || {};
    const controls = c.alive
      ? `<button onclick="lifecycle('${c.collector_id}','pause')">暂停</button>
         <button onclick="lifecycle('${c.collector_id}','resume')">恢复</button>
         <button class="danger" onclick="lifecycle('${c.collector_id}','stop')">停止</button>`
      : '<span class="muted">进程已退出</span>';
    return `<tr>
      <td class="mono">${esc(c.collector_id)}</td>
      <td class="mono">${esc(c.shard_id.slice(0, 12))}</td>
      <td>${pill(st, st)}<div class="muted" style="max-width:260px">${esc(rt.detail || "")}</div></td>
      <td class="mono">${rt.epoch ?? "–"}</td>
      <td class="mono">${rt.cursor ?? "–"}</td>
      <td class="mono">${stats.pages_fetched ?? 0} / ${stats.inserts ?? 0} / ${stats.duplicates ?? 0} / ${stats.refusals ?? 0} / ${stats.fenced_events ?? 0}</td>
      <td>${controls}</td>
    </tr>`;
  }).join("") || '<tr><td colspan="7" class="muted">暂无采集器进程</td></tr>';
}

function renderEvents(events) {
  $("events").innerHTML = events.map((e) =>
    `<tr><td class="ev-ts mono">${esc(e.ts)}</td>
     <td class="ev-kind ${esc(e.kind)}">${esc(e.kind)}</td>
     <td class="mono">${esc((e.shard_id || "").slice(0, 12))}</td>
     <td class="mono" style="max-width:520px">${esc(e.detail)}</td></tr>`).join("");
}

async function refresh() {
  const r = await api("/api/state");
  if (!r.ok) return;
  const d = r.data;
  $("clock").textContent = d.ts;
  $("src-total").textContent = d.source.total_records ?? "–";
  $("src-max").textContent = "max_id=" + (d.source.max_id ?? "–");
  $("src-refuse").textContent = d.source.refusing_until
    ? `拒绝中，至 ${new Date(d.source.refusing_until * 1000).toLocaleTimeString()}` : "来源正常";
  const b = d.receiver.batches || {};
  $("rcv-distinct").textContent = d.receiver.distinct_records ?? "–";
  $("rcv-batches").textContent =
    `新增 ${b.inserted || 0} · 重复 ${b.duplicates || 0} · 拒收 ${b.rejected || 0}`;
  const ev = d.evidence || {};
  const reconciled = ev.reconciled;
  $("reconcile").innerHTML = reconciled === undefined ? "–"
    : reconciled ? '<span class="ok">一致 ✓</span>' : '<span class="bad">未对账</span>';
  $("reconcile-detail").textContent = ev.source
    ? `接收 ${ev.receiver_distinct_records} vs 来源 ${ev.source.count} · 校验和 ${ev.receiver_checksum}/${ev.source.checksum}`
    : "";
  const alive = d.collectors.filter((c) => c.alive);
  $("col-count").textContent = alive.length;
  const fenced = alive.filter((c) => c.runtime && c.runtime.status === "fenced").length;
  $("col-fenced").textContent = fenced ? `${fenced} 个已被 fence（待退出）` : "无过期任务";
  renderShards(d.shards, d.collectors);
  renderCollectors(d.collectors);
  renderEvents((await (await fetch("/api/events")).json()).events || []);
  loadEvidence();
  loadRequests();
}

refresh();
setInterval(refresh, 2000);
