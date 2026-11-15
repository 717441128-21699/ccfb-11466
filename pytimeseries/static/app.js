/* PyTimeSeries 控制台前端逻辑(原生 JS, 无依赖) */
"use strict";

const $ = (id) => document.getElementById(id);
const COLORS = ["#4f8cff", "#35d0ba", "#ffb454", "#ff6b6b", "#b48cff",
                "#3ddc97", "#ff8fab", "#7cc7ff", "#f6d55c", "#8ce99a"];

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || res.statusText);
  return data;
}

function fmtBytes(n) {
  if (n == null) return "-";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return n.toFixed(n >= 100 || i === 0 ? 0 : 2) + " " + units[i];
}
function fmtNum(n) {
  return (n ?? 0).toLocaleString("zh-CN");
}

/* ---------------- Tabs ---------------- */
document.querySelectorAll(".tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
    btn.classList.add("active");
    $("tab-" + btn.dataset.tab).classList.add("active");
    if (btn.dataset.tab === "storage") loadStats();
    if (btn.dataset.tab === "downsample") loadReport();
  });
});

/* ---------------- Canvas 折线图 ---------------- */
function drawChart(canvas, series) {
  const ctx = canvas.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.clientWidth, H = canvas.clientHeight;
  canvas.width = W * dpr; canvas.height = H * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W, H);
  const pad = { l: 64, r: 18, t: 18, b: 34 };
  const cw = W - pad.l - pad.r, ch = H - pad.t - pad.b;

  let allPts = series.flatMap((s) => s.points);
  if (!allPts.length) {
    ctx.fillStyle = "#8b97b0";
    ctx.font = "13px sans-serif";
    ctx.fillText("暂无数据", W / 2 - 30, H / 2);
    return;
  }
  const tMin = Math.min(...allPts.map((p) => p[0]));
  const tMax = Math.max(...allPts.map((p) => p[0]));
  const vMin = Math.min(...allPts.map((p) => p[1]));
  const vMax = Math.max(...allPts.map((p) => p[1]));
  const tSpan = Math.max(1, tMax - tMin);
  const vSpan = vMax - vMin || Math.abs(vMax) || 1;

  // 网格 + Y 轴刻度
  ctx.strokeStyle = "#243049"; ctx.fillStyle = "#8b97b0";
  ctx.font = "11px Consolas, monospace"; ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = pad.t + (ch / 4) * i;
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(W - pad.r, y); ctx.stroke();
    const val = vMax - (vSpan / 4) * i;
    ctx.fillText(val >= 1e6 ? (val / 1e6).toFixed(1) + "M"
                 : val >= 1e3 ? (val / 1e3).toFixed(1) + "K"
                 : val.toFixed(1), 6, y + 4);
  }
  // X 轴时间刻度
  for (let i = 0; i <= 5; i++) {
    const t = tMin + (tSpan / 5) * i;
    const x = pad.l + (cw / 5) * i;
    const d = new Date(t * 1000);
    const label = d.getMonth() + 1 + "/" + d.getDate() + " " +
                  String(d.getHours()).padStart(2, "0") + ":" +
                  String(d.getMinutes()).padStart(2, "0");
    ctx.fillText(label, x - 22, H - 10);
  }

  const xOf = (t) => pad.l + ((t - tMin) / tSpan) * cw;
  const yOf = (v) => pad.t + ch - ((v - vMin) / vSpan) * ch;

  series.forEach((s, si) => {
    const color = COLORS[si % COLORS.length];
    s.color = color;
    if (!s.points.length) return;
    ctx.strokeStyle = color; ctx.lineWidth = 1.8; ctx.beginPath();
    s.points.forEach(([t, v], i) => {
      const x = xOf(t), y = yOf(v);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.stroke();
    ctx.fillStyle = color;
    s.points.forEach(([t, v]) => {
      ctx.beginPath(); ctx.arc(xOf(t), yOf(v), 2.2, 0, Math.PI * 2); ctx.fill();
    });
  });
}

function renderLegend(el, series) {
  el.innerHTML = "";
  series.forEach((s) => {
    const item = document.createElement("div");
    item.className = "item";
    item.innerHTML = `<span class="swatch" style="background:${s.color || "#4f8cff"}"></span>` +
      `${Object.entries(s.labels).map(([k, v]) => `${k}=${v}`).join(", ") || "(全部)"}` +
      ` · ${s.points.length} 点`;
    el.appendChild(item);
  });
}

/* ---------------- 指标浏览 ---------------- */
async function loadMetrics() {
  const data = await api("/api/metrics");
  const metricSel = $("b-metric");
  metricSel.innerHTML = data.metrics.map((m) => `<option>${m}</option>`).join("");
  const fill = (id, vals) => {
    $(id).innerHTML = '<option value="">(全部)</option>' +
      vals.map((v) => `<option>${v}</option>`).join("");
  };
  fill("b-idc", data.labels.idc || []);
  fill("b-device", data.labels.device || []);
  fill("b-port", data.labels.port || []);
}

$("b-run").addEventListener("click", async () => {
  const filters = [];
  for (const [id, key] of [["b-idc", "idc"], ["b-device", "device"], ["b-port", "port"]]) {
    const v = $(id).value;
    if (v) filters.push(`${key}="${v}"`);
  }
  let q = $("b-metric").value;
  if (filters.length) q += "{" + filters.join(",") + "}";
  if ($("b-rate").checked) q = "rate(" + q + ")";
  const end = Math.floor(Date.now() / 1000);
  const start = end - parseInt($("b-range").value, 10);
  const data = await api("/api/query", {
    method: "POST",
    body: { text: q, start, end, step: parseInt($("b-step").value, 10) },
  });
  drawChart($("b-chart"), data.series);
  renderLegend($("b-legend"), data.series);
});

/* ---------------- 查询编辑 ---------------- */
function setQuickRange() {
  const end = new Date();
  const start = new Date(end.getTime() - 3600 * 1000);
  const toLocal = (d) => {
    const p = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T` +
           `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
  };
  $("q-start").value = toLocal(start);
  $("q-end").value = toLocal(end);
}
$("q-quick").addEventListener("click", setQuickRange);
document.querySelectorAll(".chip").forEach((c) =>
  c.addEventListener("click", () => { $("q-text").value = c.dataset.q; }));

$("q-run").addEventListener("click", async () => {
  const start = Math.floor(new Date($("q-start").value).getTime() / 1000);
  const end = Math.floor(new Date($("q-end").value).getTime() / 1000);
  const data = await api("/api/query", {
    method: "POST",
    body: {
      text: $("q-text").value, start, end,
      step: parseInt($("q-step").value, 10),
    },
  });
  drawChart($("q-chart"), data.series);
  renderLegend($("q-legend"), data.series);
  $("q-json").textContent = JSON.stringify(data, null, 2);
});

/* ---------------- 降采样 ---------------- */
$("ds-run").addEventListener("click", async () => {
  $("ds-status").textContent = "执行中...";
  try {
    const r = await api("/api/downsample/run", { method: "POST", body: {} });
    $("ds-status").textContent = "完成 ✔";
    $("ds-report").textContent = JSON.stringify(r.report, null, 2);
  } catch (e) {
    $("ds-status").textContent = "失败: " + e.message;
  }
});
async function loadReport() {
  const r = await api("/api/downsample/report");
  $("ds-report").textContent = JSON.stringify(r, null, 2);
}

/* ---------------- 存储状态 ---------------- */
async function loadStats() {
  const s = await api("/api/stats");
  const ratioPct = (s.compression_ratio * 100).toFixed(2);
  const ratioOk = s.compression_ratio > 0 && s.compression_ratio < 0.1;
  $("st-kpis").innerHTML = `
    <div class="kpi"><div class="k">序列总数</div><div class="v">${fmtNum(s.index.series)}</div></div>
    <div class="kpi"><div class="k">累计写入点</div><div class="v">${fmtNum(s.total_written)}</div></div>
    <div class="kpi"><div class="k">内存表点数</div><div class="v">${fmtNum(s.memtable_points)}</div></div>
    <div class="kpi"><div class="k">原始大小</div><div class="v">${fmtBytes(s.total_raw_bytes)}</div></div>
    <div class="kpi"><div class="k">压缩后</div><div class="v">${fmtBytes(s.total_compressed_bytes)}</div></div>
    <div class="kpi"><div class="k">磁盘占用</div><div class="v">${fmtBytes(s.disk_bytes)}</div></div>
    <div class="kpi"><div class="k">压缩率(压后/原始)</div>
      <div class="v ${ratioOk ? "good" : "warn"}">${ratioPct}%</div></div>
    <div class="kpi"><div class="k">压缩倍数</div><div class="v">${s.compression_x.toFixed(1)}x</div></div>`;
  const tb = $("st-table").querySelector("tbody");
  tb.innerHTML = Object.entries(s.tiers).map(([tier, t]) => `
    <tr><td>${tier}</td><td>${fmtNum(t.blocks)}</td><td>${fmtNum(t.points)}</td>
        <td>${t.segment_files}</td><td>${fmtNum(t.retention_seconds)}</td></tr>`).join("");
}
$("st-refresh").addEventListener("click", loadStats);
setInterval(() => {
  if ($("st-auto").checked && $("tab-storage").classList.contains("active")) loadStats();
}, 5000);

/* ---------------- 写入压测 ---------------- */
function benchBody(mode) {
  const base = {
    devices: parseInt($("bm-devices").value, 10),
    ports: parseInt($("bm-ports").value, 10),
    batch_size: parseInt($("bm-batch").value, 10),
    wrap_enabled: $("bm-wrap").checked,
    reset_rate: parseFloat($("bm-reset").value),
  };
  if (mode === "load") {
    return { ...base, points: parseInt($("bm-points").value, 10),
             out_of_order_ratio: parseFloat($("bm-ooo").value) };
  }
  return { ...base, days: parseFloat($("bm-days").value) };
}
async function startBench(mode) {
  $("bm-msg").textContent = "任务已提交, 正在后台运行...";
  $("bm-result").textContent = "运行中";
  await api("/api/benchmark/" + mode, { method: "POST", body: benchBody(mode) });
  pollBench();
}
$("bm-load").addEventListener("click", () => startBench("load"));
$("bm-gen").addEventListener("click", () => startBench("generate"));

async function pollBench() {
  const timer = setInterval(async () => {
    const s = await api("/api/stats");
    $("bm-result").textContent =
      `运行中... 累计写入 ${fmtNum(s.total_written)} 点, ` +
      `内存表 ${fmtNum(s.memtable_points)} 点, 磁盘 ${fmtBytes(s.disk_bytes)}`;
    const st = await api("/api/benchmark/status");
    if (!st.running) {
      clearInterval(timer);
      if (st.result && st.result.error) {
        $("bm-msg").textContent = "失败: " + st.result.error;
      } else {
        $("bm-msg").textContent = "完成 ✔";
      }
      $("bm-result").textContent = JSON.stringify(st.result, null, 2);
      loadStats();
    }
  }, 2000);
}

/* ---------------- 初始化 ---------------- */
setQuickRange();
loadMetrics().catch((e) => $("bm-msg").textContent = "指标加载失败: " + e.message);
loadStats().catch(() => {});
