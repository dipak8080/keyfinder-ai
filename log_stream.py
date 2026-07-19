"""
log_stream.py - Live logging: HTTP requests + system logs, streamed via SSE.

Two independent live feeds, same as Railway's dashboard:
  1. HTTP request logs   -> stored in SQLite, tailed and streamed live
  2. System/app logs     -> captured from Python's logging module automatically
                            (no changes needed to youtube.py, utils.py, etc.
                             any existing logger.info()/logger.error() call
                             anywhere in the app is captured here for free)

Mount points (added to main.py):
  GET  /admin/logs                    -> HTML dashboard (2 tabs: Requests / System)
  GET  /admin/logs/http/stream         -> SSE stream of HTTP requests
  GET  /admin/logs/system/stream       -> SSE stream of system/app logs
  GET  /admin/logs/http/data           -> JSON snapshot (for initial page load)
  DELETE /admin/logs                   -> clear old logs (HTTP + system)

All endpoints require ?key=<ADMIN_STATUS_KEY> to match your existing admin key.
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
SYSTEM_LOG_BUFFER_SIZE = 2000  # how many recent system log lines to keep in memory

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


class RequestLoggerMiddleware(BaseHTTPMiddleware):
    """Logs every HTTP request to SQLite. Add via app.add_middleware(RequestLoggerMiddleware)."""

    async def dispatch(self, request: Request, call_next):
        start = time.time()
        response = await call_next(request)
        duration_ms = (time.time() - start) * 1000

        if not request.url.path.startswith("/admin/logs"):
            try:
                client_ip = request.client.host if request.client else "-"
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
                pass  # never let logging break the actual request

        return response


def _check_admin(key: str):
    if not ADMIN_KEY or key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


@router.get("/admin/logs/http/data")
def get_http_logs(key: str = Query(...), limit: int = Query(100, le=1000)):
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
    """Tails the SQLite table once per second, yields only new rows as SSE events."""
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
    """Custom logging.Handler that captures every log record app-wide into an in-memory buffer.
    Attach this once at startup and every existing logger.info()/error() call anywhere
    in the codebase (youtube.py, utils.py, monitoring.py, rate_limit.py, etc.) is captured
    automatically - no need to touch those files.
    """

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
    """Call once at startup (in main.py's lifespan) to start capturing all logs."""
    handler = BufferLogHandler()
    handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)  # attaches to the ROOT logger -> catches everything


@router.get("/admin/logs/system/data")
def get_system_logs(key: str = Query(...), limit: int = Query(200, le=2000)):
    _check_admin(key)
    logs = list(_system_log_buffer)[-limit:]
    return JSONResponse({"total": len(_system_log_buffer), "logs": logs})


async def _system_log_event_generator():
    """Polls the in-memory buffer once per second, yields only new entries as SSE events."""
    last_len = len(_system_log_buffer)
    while True:
        await asyncio.sleep(1)
        current = list(_system_log_buffer)
        if len(current) > last_len:
            new_entries = current[last_len:]
            for entry in new_entries:
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
# 4. DASHBOARD UI (two live tabs, using EventSource / SSE)
# ============================================================

@router.get("/admin/logs", response_class=HTMLResponse)
def logs_dashboard(key: str = Query(...)):
    _check_admin(key)
    html = f"""
<!DOCTYPE html>
<html>
<head>
<title>AudioForges - Live Logs</title>
<meta charset="utf-8">
<style>
  body {{ font-family: -apple-system, system-ui, sans-serif; background: #0f1117; color: #e2e2e2; margin: 0; padding: 20px; }}
  h1 {{ font-size: 20px; margin-bottom: 12px; }}
  .tabs {{ display: flex; gap: 8px; margin-bottom: 16px; }}
  .tab {{ background: #1a1d29; padding: 8px 18px; border-radius: 6px; cursor: pointer; color: #888; }}
  .tab.active {{ background: #262a38; color: #fff; }}
  .stats {{ display: flex; gap: 20px; margin-bottom: 16px; }}
  .stat-box {{ background: #1a1d29; padding: 10px 18px; border-radius: 8px; }}
  .stat-box .label {{ font-size: 12px; color: #888; }}
  .stat-box .value {{ font-size: 20px; font-weight: 600; }}
  .success {{ color: #4ade80; }}
  .failed {{ color: #f87171; }}
  .controls {{ margin-bottom: 12px; }}
  button {{ background: #262a38; color: #e2e2e2; border: none; padding: 7px 14px; border-radius: 6px; cursor: pointer; margin-right: 8px; font-size: 13px; }}
  button:hover {{ background: #333748; }}
  .live-dot {{ display: inline-block; width: 8px; height: 8px; background: #4ade80; border-radius: 50%; margin-right: 6px; animation: pulse 1.5s infinite; }}
  @keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.3; }} }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th, td {{ text-align: left; padding: 6px 10px; border-bottom: 1px solid #262a38; }}
  th {{ color: #888; font-weight: 500; position: sticky; top: 0; background: #0f1117; }}
  .status-2xx {{ color: #4ade80; }}
  .status-4xx {{ color: #fbbf24; }}
  .status-5xx {{ color: #f87171; }}
  .level-INFO {{ color: #60a5fa; }}
  .level-WARNING {{ color: #fbbf24; }}
  .level-ERROR {{ color: #f87171; }}
  .level-CRITICAL {{ color: #f87171; font-weight: bold; }}
  #system-panel {{ background: #0a0c11; border-radius: 8px; padding: 10px; height: 500px; overflow-y: auto; font-family: monospace; font-size: 12.5px; }}
  #system-panel div {{ padding: 2px 0; white-space: pre-wrap; }}
  .panel {{ display: none; }}
  .panel.active {{ display: block; }}
</style>
</head>
<body>
  <h1><span class="live-dot"></span>AudioForges — Live Logs</h1>

  <div class="tabs">
    <div class="tab active" onclick="switchTab('http')">HTTP Requests</div>
    <div class="tab" onclick="switchTab('system')">System Logs</div>
  </div>

  <div class="controls">
    <button onclick="deleteLogs(1)">Delete logs older than 1 day</button>
    <button onclick="deleteLogs(7)">Delete logs older than 7 days</button>
    <button onclick="deleteLogs(null)">Delete ALL logs</button>
  </div>

  <div id="http-panel" class="panel active">
    <div class="stats">
      <div class="stat-box"><div class="label">Total</div><div class="value" id="total">-</div></div>
      <div class="stat-box"><div class="label">Success</div><div class="value success" id="success">-</div></div>
      <div class="stat-box"><div class="label">Failed</div><div class="value failed" id="failed">-</div></div>
    </div>
    <table>
      <thead>
        <tr><th>Time (UTC)</th><th>Method</th><th>Path</th><th>Status</th><th>Duration (ms)</th><th>IP</th></tr>
      </thead>
      <tbody id="http-rows"></tbody>
    </table>
  </div>

  <div id="system-panel-wrap" class="panel">
    <div id="system-panel"></div>
  </div>

<script>
const KEY = "{ADMIN_KEY}";

function switchTab(tab) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  if (tab === 'http') {{
    document.querySelectorAll('.tab')[0].classList.add('active');
    document.getElementById('http-panel').classList.add('active');
  }} else {{
    document.querySelectorAll('.tab')[1].classList.add('active');
    document.getElementById('system-panel-wrap').classList.add('active');
  }}
}}

function statusClass(code) {{
  if (code >= 500) return "status-5xx";
  if (code >= 400) return "status-4xx";
  return "status-2xx";
}}

async function loadInitialHttp() {{
  const res = await fetch(`/admin/logs/http/data?key=${{KEY}}&limit=100`);
  const data = await res.json();
  document.getElementById("total").innerText = data.total;
  document.getElementById("success").innerText = data.success;
  document.getElementById("failed").innerText = data.failed;
  const rows = data.logs.reverse().map(renderHttpRow).join("");
  document.getElementById("http-rows").innerHTML = rows;
}}

function renderHttpRow(log) {{
  return `<tr>
    <td>${{log.timestamp.replace("T", " ").split(".")[0]}}</td>
    <td>${{log.method}}</td>
    <td>${{log.path}}</td>
    <td class="${{statusClass(log.status_code)}}">${{log.status_code}}</td>
    <td>${{log.duration_ms}}</td>
    <td>${{log.client_ip}}</td>
  </tr>`;
}}

const httpSource = new EventSource(`/admin/logs/http/stream?key=${{KEY}}`);
httpSource.onmessage = (event) => {{
  const log = JSON.parse(event.data);
  const tbody = document.getElementById("http-rows");
  tbody.insertAdjacentHTML("beforeend", renderHttpRow(log));
  tbody.scrollTop = tbody.scrollHeight;
  document.getElementById("total").innerText = parseInt(document.getElementById("total").innerText || 0) + 1;
  if (log.status_code < 400) {{
    document.getElementById("success").innerText = parseInt(document.getElementById("success").innerText || 0) + 1;
  }} else {{
    document.getElementById("failed").innerText = parseInt(document.getElementById("failed").innerText || 0) + 1;
  }}
}};

async function loadInitialSystem() {{
  const res = await fetch(`/admin/logs/system/data?key=${{KEY}}&limit=200`);
  const data = await res.json();
  const panel = document.getElementById("system-panel");
  panel.innerHTML = data.logs.map(renderSystemLine).join("");
  panel.scrollTop = panel.scrollHeight;
}}

function renderSystemLine(entry) {{
  return `<div><span class="level-${{entry.level}}">[${{entry.level}}]</span> ${{entry.timestamp.replace("T"," ").split(".")[0]}} ${{entry.logger}} — ${{entry.message}}</div>`;
}}

const systemSource = new EventSource(`/admin/logs/system/stream?key=${{KEY}}`);
systemSource.onmessage = (event) => {{
  const entry = JSON.parse(event.data);
  const panel = document.getElementById("system-panel");
  panel.insertAdjacentHTML("beforeend", renderSystemLine(entry));
  panel.scrollTop = panel.scrollHeight;
}};

async function deleteLogs(days) {{
  const label = days ? `older than ${{days}} day(s)` : "ALL";
  if (!confirm(`Delete logs ${{label}}? This can't be undone.`)) return;
  const url = days ? `/admin/logs?key=${{KEY}}&older_than_days=${{days}}` : `/admin/logs?key=${{KEY}}`;
  const res = await fetch(url, {{ method: "DELETE" }});
  const data = await res.json();
  alert(`Deleted ${{data.deleted_http_logs}} HTTP logs.` + (data.system_buffer_cleared ? " System logs cleared too." : ""));
  loadInitialHttp();
  loadInitialSystem();
}}

loadInitialHttp();
loadInitialSystem();
</script>
</body>
</html>
    """
    return HTMLResponse(content=html)