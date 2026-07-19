"""
log_stream.py - Live logging: HTTP requests + system logs, streamed via SSE.

Two independent live feeds, same as Railway's dashboard:
  1. HTTP request logs   -> stored in SQLite, tailed and streamed live
  2. System/app logs     -> captured from Python's logging module automatically

Mount points (added to main.py):
  GET  /admin/logs                    -> HTML dashboard (2 tabs: Requests / System)
  GET  /admin/logs/http/stream         -> SSE stream of HTTP requests
  GET  /admin/logs/system/stream       -> SSE stream of system/app logs
  GET  /admin/logs/http/data           -> JSON snapshot (for initial page load)
  DELETE /admin/logs                   -> clear old logs (HTTP + system)

All endpoints require ?key=<ADMIN_STATUS_KEY> to match your existing admin key.

IMPORTANT: for the /stream (SSE) endpoints to actually be live rather than
buffered, nginx needs `proxy_buffering off;` on those specific routes - see
the nginx config block documented in project notes. Without it, nginx
buffers the whole response before forwarding, defeating the purpose of SSE.
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware

DB_PATH = os.environ.get("REQUEST_LOG_DB_PATH", "/app/data/logs.db")
ADMIN_KEY = os.environ.get("ADMIN_STATUS_KEY", "")
SYSTEM_LOG_BUFFER_SIZE = 2000

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

router = APIRouter()


# ============================================================
# 1. HTTP REQUEST LOGGING (SQLite-backed)
# ============================================================

def _init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS request_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                duration_ms REAL NOT NULL,
                client_ip TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON request_logs(timestamp)")
        conn.commit()


_init_db()


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _get_real_client_ip(request: Request) -> str:
    """
    request.client.host is the IP of whoever connects DIRECTLY to this app.
    Since nginx sits in front as a reverse proxy, that's always nginx itself
    (127.0.0.1 or the Docker bridge IP) - NOT the real visitor. nginx
    forwards the real visitor IP via the X-Forwarded-For header (set in the
    nginx config's proxy_set_header directive) - read that first, falling
    back to request.client.host only if it's missing (e.g. hitting the app
    directly on :8000, bypassing nginx entirely).
    """
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.client.host if request.client else "-"


class RequestLoggerMiddleware(BaseHTTPMiddleware):
    """Logs every HTTP request to SQLite. Add via app.add_middleware(RequestLoggerMiddleware)."""

    async def dispatch(self, request: Request, call_next):
        start = time.time()
        response = await call_next(request)
        duration_ms = (time.time() - start) * 1000

        if not request.url.path.startswith("/admin/logs"):
            try:
                client_ip = _get_real_client_ip(request)
                with get_db() as conn:
                    conn.execute(
                        "INSERT INTO request_logs (timestamp, method, path, status_code, duration_ms, client_ip) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            datetime.utcnow().isoformat(),
                            request.method,
                            request.url.path,
                            response.status_code,
                            round(duration_ms, 2),
                            client_ip,
                        ),
                    )
                    conn.commit()
            except Exception:
                pass

        return response


def _check_admin(key: str):
    if not ADMIN_KEY or key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


@router.get("/admin/logs/http/data")
def get_http_logs(key: str = Query(...), limit: int = Query(200, le=2000)):
    _check_admin(key)
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM request_logs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) as c FROM request_logs").fetchone()["c"]
        success = conn.execute(
            "SELECT COUNT(*) as c FROM request_logs WHERE status_code < 400"
        ).fetchone()["c"]
    return JSONResponse(
        {"total": total, "success": success, "failed": total - success, "logs": [dict(r) for r in rows]}
    )


async def _http_log_event_generator():
    last_id = 0
    with get_db() as conn:
        row = conn.execute("SELECT MAX(id) as m FROM request_logs").fetchone()
        last_id = row["m"] or 0

    while True:
        await asyncio.sleep(1)
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM request_logs WHERE id > ? ORDER BY id ASC", (last_id,)
            ).fetchall()
        for r in rows:
            last_id = r["id"]
            yield f"data: {json.dumps(dict(r))}\n\n"


@router.get("/admin/logs/http/stream")
async def stream_http_logs(key: str = Query(...)):
    _check_admin(key)
    return StreamingResponse(_http_log_event_generator(), media_type="text/event-stream")


# ============================================================
# 2. SYSTEM / APP LOGGING (hooks into Python's `logging` module)
# ============================================================

_system_log_buffer = deque(maxlen=SYSTEM_LOG_BUFFER_SIZE)


class BufferLogHandler(logging.Handler):
    def emit(self, record):
        try:
            _system_log_buffer.append(
                {
                    "timestamp": datetime.utcfromtimestamp(record.created).isoformat(),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                }
            )
        except Exception:
            pass


def attach_system_log_capture():
    handler = BufferLogHandler()
    handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)


@router.get("/admin/logs/system/data")
def get_system_logs(key: str = Query(...), limit: int = Query(200, le=2000)):
    _check_admin(key)
    logs = list(_system_log_buffer)[-limit:]
    return JSONResponse({"total": len(_system_log_buffer), "logs": logs})


async def _system_log_event_generator():
    last_len = len(_system_log_buffer)
    while True:
        await asyncio.sleep(1)
        current = list(_system_log_buffer)
        if len(current) > last_len:
            for entry in current[last_len:]:
                yield f"data: {json.dumps(entry)}\n\n"
            last_len = len(current)
        elif len(current) < last_len:
            last_len = len(current)


@router.get("/admin/logs/system/stream")
async def stream_system_logs(key: str = Query(...)):
    _check_admin(key)
    return StreamingResponse(_system_log_event_generator(), media_type="text/event-stream")


# ============================================================
# 3. LOG CLEANUP
# ============================================================

@router.delete("/admin/logs")
def delete_logs(key: str = Query(...), older_than_days: int = Query(None)):
    _check_admin(key)
    with get_db() as conn:
        if older_than_days is not None:
            cutoff = (datetime.utcnow() - timedelta(days=older_than_days)).isoformat()
            cur = conn.execute("DELETE FROM request_logs WHERE timestamp < ?", (cutoff,))
        else:
            cur = conn.execute("DELETE FROM request_logs")
        conn.commit()
        deleted_http = cur.rowcount

    if older_than_days is None:
        _system_log_buffer.clear()

    return {"deleted_http_logs": deleted_http, "system_buffer_cleared": older_than_days is None}


# ============================================================
# 4. DASHBOARD UI
# ============================================================

@router.get("/admin/logs", response_class=HTMLResponse)
def logs_dashboard(key: str = Query(...)):
    _check_admin(key)
    html = """
<!DOCTYPE html>
<html>
<head>
<title>AudioForges - Live Logs</title>
<meta charset="utf-8">
<style>
  body { font-family: -apple-system, system-ui, sans-serif; background: #0f1117; color: #e2e2e2; margin: 0; padding: 20px; }
  h1 { font-size: 20px; margin-bottom: 12px; }
  .tabs { display: flex; gap: 8px; margin-bottom: 16px; }
  .tab { background: #1a1d29; padding: 8px 18px; border-radius: 6px; cursor: pointer; color: #888; }
  .tab.active { background: #262a38; color: #fff; }
  .stats { display: flex; gap: 20px; margin-bottom: 14px; }
  .stat-box { background: #1a1d29; padding: 10px 18px; border-radius: 8px; }
  .stat-box .label { font-size: 12px; color: #888; }
  .stat-box .value { font-size: 20px; font-weight: 600; }
  .success { color: #4ade80; }
  .failed { color: #f87171; }
  .controls { margin-bottom: 10px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
  .controls-label { font-size: 12px; color: #666; margin-right: 4px; }
  select, input[type=text], input[type=date] {
    background: #262a38; color: #e2e2e2; border: 1px solid #333748;
    border-radius: 6px; padding: 7px 10px; font-size: 13px;
    font-family: -apple-system, system-ui, sans-serif;
  }
  select:focus, input[type=text]:focus, input[type=date]:focus { outline: none; border-color: #4ade80; }
  button, .date-btn {
    background: #262a38; color: #e2e2e2; border: 1px solid #333748;
    padding: 7px 14px; border-radius: 6px; cursor: pointer; font-size: 13px;
  }
  button:hover, .date-btn:hover { background: #333748; }
  .date-btn.active { background: #4ade80; color: #0f1117; border-color: #4ade80; font-weight: 600; }
  .reset-btn { background: #3a1d24; border-color: #5a2530; color: #f87171; }
  .reset-btn:hover { background: #4a232b; }
  .live-dot { display: inline-block; width: 8px; height: 8px; background: #4ade80; border-radius: 50%; margin-right: 6px; animation: pulse 1.5s infinite; }
  @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid #262a38; }
  th { color: #888; font-weight: 500; position: sticky; top: 0; background: #0f1117; }
  .status-2xx { color: #4ade80; }
  .status-4xx { color: #fbbf24; }
  .status-5xx { color: #f87171; }
  .level-INFO { color: #60a5fa; }
  .level-WARNING { color: #fbbf24; }
  .level-ERROR { color: #f87171; }
  .level-CRITICAL { color: #f87171; font-weight: bold; }
  #system-panel { background: #0a0c11; border-radius: 8px; padding: 10px; height: 500px; overflow-y: auto; font-family: monospace; font-size: 12.5px; }
  #system-panel div { padding: 2px 0; white-space: pre-wrap; }
  .panel { display: none; }
  .panel.active { display: block; }
  .empty-state { color: #555; text-align: center; padding: 30px; font-size: 13px; }
  .new-row { animation: highlight 1.5s ease-out; }
  @keyframes highlight { 0% { background: rgba(74, 222, 128, 0.25); } 100% { background: transparent; } }
</style>
</head>
<body>
  <h1><span class="live-dot"></span>AudioForges — Live Logs</h1>

  <div class="tabs">
    <div class="tab active" onclick="switchTab('http')">HTTP Requests</div>
    <div class="tab" onclick="switchTab('system')">System Logs</div>
  </div>

  <div id="http-panel" class="panel active">
    <div class="stats">
      <div class="stat-box"><div class="label">Total</div><div class="value" id="total">-</div></div>
      <div class="stat-box"><div class="label">Success</div><div class="value success" id="success">-</div></div>
      <div class="stat-box"><div class="label">Failed</div><div class="value failed" id="failed">-</div></div>
    </div>

    <div class="controls">
      <span class="controls-label">Filter:</span>
      <select id="methodFilter" onchange="applyFilter()">
        <option value="">All methods</option>
        <option value="GET">GET</option>
        <option value="POST">POST</option>
        <option value="DELETE">DELETE</option>
      </select>
      <input type="text" id="pathFilter" placeholder="Search path, e.g. /download" oninput="applyFilter()" style="width:220px;">
      <label style="font-size:13px;color:#888;">
        <input type="checkbox" id="hideNoise" onchange="applyFilter()"> Hide bot/scanner noise
      </label>
      <button class="reset-btn" onclick="resetFilters()">Reset filters</button>
    </div>

    <div class="controls">
      <span class="controls-label">Date:</span>
      <button class="date-btn active" id="dateBtnAll" onclick="setDateFilter('all')">All time</button>
      <button class="date-btn" id="dateBtnToday" onclick="setDateFilter('today')">Today</button>
      <button class="date-btn" id="dateBtnYesterday" onclick="setDateFilter('yesterday')">Yesterday</button>
      <span class="controls-label">or pick a date:</span>
      <input type="date" id="customDate" onchange="setDateFilter('custom')">
    </div>

    <div class="controls">
      <button onclick="deleteLogs(1)">Delete logs older than 1 day</button>
      <button onclick="deleteLogs(7)">Delete logs older than 7 days</button>
      <button onclick="deleteLogs(null)">Delete ALL logs</button>
    </div>

    <div class="controls">
      <button id="pauseBtn" onclick="togglePause()">⏸ Pause live updates</button>
      <span id="pendingBadge" style="display:none; color:#fbbf24; font-size:13px;"></span>
    </div>

    <table>
      <thead>
        <tr><th>Time (NPT)</th><th>Method</th><th>Path</th><th>Status</th><th>Duration</th><th>IP</th></tr>
      </thead>
      <tbody id="http-rows"></tbody>
    </table>
    <div id="http-empty" class="empty-state" style="display:none;">No requests match the current filters.</div>
    <div id="http-loading" class="empty-state">Loading...</div>
  </div>

  <div id="system-panel-wrap" class="panel">
    <div class="controls">
      <button onclick="deleteLogs(null)">Clear system log buffer</button>
    </div>
    <div id="system-panel"></div>
  </div>

<script>
const KEY = encodeURIComponent("PLACEHOLDER_ADMIN_KEY");

function switchTab(tab) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  if (tab === 'http') {
    document.querySelectorAll('.tab')[0].classList.add('active');
    document.getElementById('http-panel').classList.add('active');
  } else {
    document.querySelectorAll('.tab')[1].classList.add('active');
    document.getElementById('system-panel-wrap').classList.add('active');
  }
}

let allHttpLogs = [];
let currentDateFilter = "all";
const MAX_LOGS_IN_MEMORY = 1000; // caps client-side memory growth for long-running tabs
let isPaused = false;
let pendingCount = 0;

const NOISE_PATTERNS = [
  "/robots.txt", "/favicon.ico", "/.env", "/wp-", "/.git",
  "/SDK/", "/phpmyadmin", "/.well-known", "/xmlrpc.php"
];
function isNoise(path) {
  return NOISE_PATTERNS.some(p => path.includes(p));
}

function statusClass(code) {
  if (code >= 500) return "status-5xx";
  if (code >= 400) return "status-4xx";
  return "status-2xx";
}

function formatDuration(ms) {
  if (ms >= 1000) return (ms / 1000).toFixed(2) + "s";
  return ms.toFixed(0) + "ms";
}

function toNepalTime(isoString) {
  const date = new Date(isoString + "Z");
  return date.toLocaleString("en-US", {
    timeZone: "Asia/Kathmandu",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: true
  });
}

function getNepalYMD(isoString) {
  const date = new Date(isoString + "Z");
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Kathmandu", year: "numeric", month: "2-digit", day: "2-digit"
  }).formatToParts(date);
  return {
    y: parseInt(parts.find(p => p.type === "year").value),
    m: parseInt(parts.find(p => p.type === "month").value),
    d: parseInt(parts.find(p => p.type === "day").value)
  };
}

function ymdToKey(ymd) {
  return ymd.y + "-" + String(ymd.m).padStart(2, "0") + "-" + String(ymd.d).padStart(2, "0");
}

function shiftYMD(ymd, deltaDays) {
  const ms = Date.UTC(ymd.y, ymd.m - 1, ymd.d) + deltaDays * 86400000;
  const d = new Date(ms);
  return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate() };
}

function nepalTodayYMD() {
  return getNepalYMD(new Date().toISOString());
}

// ---------------- FILTER PERSISTENCE (localStorage) ----------------
// This is a real production webpage (not a Claude artifact sandbox), so
// localStorage works normally here - saves your filter preferences (e.g.
// "always hide bot noise") across page reloads/tab closes.
function saveFilterPrefs() {
  const prefs = {
    method: document.getElementById("methodFilter").value,
    path: document.getElementById("pathFilter").value,
    hideNoise: document.getElementById("hideNoise").checked
  };
  localStorage.setItem("audioforges_log_filters", JSON.stringify(prefs));
}

function loadFilterPrefs() {
  try {
    const saved = JSON.parse(localStorage.getItem("audioforges_log_filters") || "{}");
    if (saved.method) document.getElementById("methodFilter").value = saved.method;
    if (saved.path) document.getElementById("pathFilter").value = saved.path;
    if (saved.hideNoise) document.getElementById("hideNoise").checked = saved.hideNoise;
  } catch (e) { /* ignore corrupt/missing prefs */ }
}

function setDateFilter(which) {
  currentDateFilter = which;
  document.querySelectorAll(".date-btn").forEach(b => b.classList.remove("active"));
  if (which === "all") document.getElementById("dateBtnAll").classList.add("active");
  if (which === "today") document.getElementById("dateBtnToday").classList.add("active");
  if (which === "yesterday") document.getElementById("dateBtnYesterday").classList.add("active");
  applyFilter();
}

function passesDateFilter(log) {
  if (currentDateFilter === "all") return true;
  const logKey = ymdToKey(getNepalYMD(log.timestamp));
  if (currentDateFilter === "today") {
    return logKey === ymdToKey(nepalTodayYMD());
  }
  if (currentDateFilter === "yesterday") {
    return logKey === ymdToKey(shiftYMD(nepalTodayYMD(), -1));
  }
  if (currentDateFilter === "custom") {
    const picked = document.getElementById("customDate").value;
    return picked && logKey === picked;
  }
  return true;
}

function applyFilter() {
  saveFilterPrefs();
  const methodVal = document.getElementById("methodFilter").value;
  const pathVal = document.getElementById("pathFilter").value.toLowerCase();
  const hideNoise = document.getElementById("hideNoise").checked;

  const filtered = allHttpLogs.filter(log => {
    if (methodVal && log.method !== methodVal) return false;
    if (pathVal && !log.path.toLowerCase().includes(pathVal)) return false;
    if (hideNoise && isNoise(log.path)) return false;
    if (!passesDateFilter(log)) return false;
    return true;
  });

  // Newest first - matches how most log viewers (including Railway) show
  // activity, so the most recent request is always visible at the top
  // without needing to scroll down.
  document.getElementById("http-rows").innerHTML = filtered.slice().reverse().map(renderHttpRow).join("");
  document.getElementById("http-empty").style.display = filtered.length === 0 ? "block" : "none";
}

function resetFilters() {
  document.getElementById("methodFilter").value = "";
  document.getElementById("pathFilter").value = "";
  document.getElementById("hideNoise").checked = false;
  document.getElementById("customDate").value = "";
  setDateFilter("all");
}

function renderHttpRow(log, isNew) {
  return `<tr class="${isNew ? 'new-row' : ''}">
    <td>${toNepalTime(log.timestamp)}</td>
    <td>${log.method}</td>
    <td>${log.path}</td>
    <td class="${statusClass(log.status_code)}">${log.status_code}</td>
    <td>${formatDuration(log.duration_ms)}</td>
    <td>${log.client_ip}</td>
  </tr>`;
}

function togglePause() {
  isPaused = !isPaused;
  const btn = document.getElementById("pauseBtn");
  if (isPaused) {
    btn.textContent = "▶ Resume live updates";
    btn.style.color = "#fbbf24";
  } else {
    btn.textContent = "⏸ Pause live updates";
    btn.style.color = "";
    pendingCount = 0;
    document.getElementById("pendingBadge").style.display = "none";
    applyFilter(); // flush anything that arrived while paused
  }
}

async function loadInitialHttp() {
  document.getElementById("http-loading").style.display = "block";
  try {
    const res = await fetch(`/admin/logs/http/data?key=${KEY}&limit=${MAX_LOGS_IN_MEMORY}`);
    const data = await res.json();
    document.getElementById("total").innerText = data.total;
    document.getElementById("success").innerText = data.success;
    document.getElementById("failed").innerText = data.failed;
    allHttpLogs = data.logs.reverse(); // stored oldest->newest internally; display order handled in applyFilter
    applyFilter();
  } finally {
    document.getElementById("http-loading").style.display = "none";
  }
}

const httpSource = new EventSource(`/admin/logs/http/stream?key=${KEY}`);
httpSource.onmessage = (event) => {
  const log = JSON.parse(event.data);
  allHttpLogs.push(log);
  if (allHttpLogs.length > MAX_LOGS_IN_MEMORY) {
    allHttpLogs.shift(); // cap memory growth for long-running tabs
  }
  document.getElementById("total").innerText = parseInt(document.getElementById("total").innerText || 0) + 1;
  if (log.status_code < 400) {
    document.getElementById("success").innerText = parseInt(document.getElementById("success").innerText || 0) + 1;
  } else {
    document.getElementById("failed").innerText = parseInt(document.getElementById("failed").innerText || 0) + 1;
  }

  if (isPaused) {
    pendingCount++;
    const badge = document.getElementById("pendingBadge");
    badge.style.display = "inline";
    badge.textContent = `${pendingCount} new request(s) waiting - resume to view`;
    return; // don't touch the DOM while paused, avoids the table jumping under the user
  }

  applyFilter();
};

function renderSystemLine(entry) {
  return `<div><span class="level-${entry.level}">[${entry.level}]</span> ${toNepalTime(entry.timestamp)} ${entry.logger} — ${entry.message}</div>`;
}

async function loadInitialSystem() {
  const res = await fetch(`/admin/logs/system/data?key=${KEY}&limit=200`);
  const data = await res.json();
  const panel = document.getElementById("system-panel");
  panel.innerHTML = data.logs.map(renderSystemLine).join("");
  panel.scrollTop = panel.scrollHeight;
}

const systemSource = new EventSource(`/admin/logs/system/stream?key=${KEY}`);
systemSource.onmessage = (event) => {
  const entry = JSON.parse(event.data);
  const panel = document.getElementById("system-panel");
  panel.insertAdjacentHTML("beforeend", renderSystemLine(entry));
  panel.scrollTop = panel.scrollHeight;
};

async function deleteLogs(days) {
  const label = days ? `older than ${days} day(s)` : "ALL";
  if (!confirm(`Delete logs ${label}? This can't be undone.`)) return;
  const url = days ? `/admin/logs?key=${KEY}&older_than_days=${days}` : `/admin/logs?key=${KEY}`;
  const res = await fetch(url, { method: "DELETE" });
  const data = await res.json();
  alert(`Deleted ${data.deleted_http_logs} HTTP logs.` + (data.system_buffer_cleared ? " System logs cleared too." : ""));
  loadInitialHttp();
  loadInitialSystem();
}

document.getElementById("customDate").max = ymdToKey(nepalTodayYMD());
loadFilterPrefs();

loadInitialHttp();
loadInitialSystem();
</script>
</body>
</html>
    """
    html = html.replace("PLACEHOLDER_ADMIN_KEY", ADMIN_KEY)
    return HTMLResponse(content=html)