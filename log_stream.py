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

--------------------------------------------------------------------------
FIXES APPLIED (2026-07-19):
  1. getNepalYMD() crashed on already-Z-suffixed ISO strings (it blindly
     appended a second "Z"), which threw an uncaught RangeError on page
     load and halted the whole script before loadInitialHttp() /
     loadInitialSystem() ever ran. This is why the dashboard looked "empty
     after refresh" even though the backend/DB had all the data the whole
     time. Fixed to detect an existing timezone suffix before appending.
  2. Total/Success/Failed counters could permanently become "NaN" if an
     SSE event arrived before the initial fetch populated real numbers
     (parseInt("-") -> NaN, and NaN + 1 stays NaN forever). Fixed by
     tracking counts as real JS numbers instead of re-parsing DOM text.
  3. Page-init sequence wrapped in try/catch so one bad date/edge case
     can no longer block the rest of the dashboard from loading.
  4. passesDateFilter() threw on any row with an unparseable timestamp
     (e.g. a stray manual-test row), which aborted the whole Array.filter
     mid-pass in applyFilter(). Because the DOM update line runs after the
     filter, the table just kept showing whatever was on screen before -
     making "Today"/"Yesterday" look like they were showing the wrong
     data, when really they'd just silently failed to update at all.
     Fixed to catch per-row and exclude bad rows instead of aborting.
  5. HTTP request table now renders oldest-first / newest-at-the-bottom
     (like a terminal tail), auto-scrolling the table to the bottom when
     new live rows arrive, instead of newest-at-top.
  6. System logs were only ever kept in an in-memory deque, which is
     wiped every time the container restarts/redeploys (deploy.yml does
     stop+rm+run on every push) - so only logs since the last deploy were
     ever visible, looking like "older system logs are missing." System
     logs are now persisted to the same SQLite DB as HTTP logs, so they
     survive restarts and redeploys exactly like HTTP logs already do.

FIXES APPLIED (2026-07-24):
  7. System log lines had no way to tell which HTTP request produced
     them, so a busy period of overlapping requests just looked like one
     undifferentiated wall of log lines - impossible to tell where one
     request's logs ended and the next one's began. Added a per-request
     ID (via contextvars, set once in RequestLoggerMiddleware) that gets
     attached to every log line emitted while that request is in flight,
     including lines emitted from a background task spawned by that
     request (asyncio.create_task() captures a snapshot of the current
     context automatically, so a long-running job like /separate or
     /speech-to-text still tags its log lines with the request that
     started it, even after the original HTTP response has already been
     sent). The frontend uses this to draw a divider only when the
     request ID actually changes between consecutive log lines, instead
     of between every single line.

FIXES APPLIED (2026-08-02):
  8. "Failed" used to mean "not a 2xx/3xx," which counted a LOT of
     completely normal traffic as failure: a bot probing for
     /api/auth/validate-sso (404 - the route correctly doesn't exist), a
     visitor who hit a rate limit (429 - working as designed), a
     separation request rejected because the queue was full (503 - also
     working as designed). All of that is CLIENT behavior, not a server
     problem, and lumping it in with "failed" made the dashboard's top
     numbers actively misleading - a spike of bot noise looked identical
     to the app being broken.

     Split into three buckets instead of two:
       - success: status_code < 400
       - client:  400-499  (bad/rejected requests - expected, not a bug)
       - server:  500+     (the backend actually broke - THIS is what to
                            chase)

     The JSON responses from get_http_logs() now return
     {total, success, client, server, logs} instead of
     {total, success, failed, logs}. Per-row coloring in the table was
     already correct (amber for 4xx, red for 5xx) - only the SUMMARY
     counters were conflating the two.
--------------------------------------------------------------------------
"""

import asyncio
import contextvars
import json
import logging
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware

DB_PATH = os.environ.get("REQUEST_LOG_DB_PATH", "/app/data/logs.db")
ADMIN_KEY = os.environ.get("ADMIN_STATUS_KEY", "")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

router = APIRouter()

# Per-request ID, readable from anywhere in the call stack (including a
# background task spawned via asyncio.create_task() from inside a request,
# since asyncio automatically copies the current contextvars context into
# new tasks). Used to group system log lines by the request that produced
# them - see BufferLogHandler.emit() below.
_request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-"
)


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
        # System logs now share this same SQLite file (and volume mount) as
        # request_logs, so they survive container restarts/redeploys instead
        # of vanishing every time deploy.yml recreates the container.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                level TEXT NOT NULL,
                logger TEXT NOT NULL,
                message TEXT NOT NULL,
                request_id TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_system_timestamp ON system_logs(timestamp)")

        # Migration for databases created before request_id existed -
        # CREATE TABLE IF NOT EXISTS above is a no-op on an existing table,
        # so an older deployment's system_logs table won't have this column
        # yet. ALTER TABLE ADD COLUMN is safe to attempt unconditionally;
        # SQLite errors only if the column is already there, which we
        # swallow since that just means this is a fresh table that already
        # has it from the CREATE TABLE above.
        try:
            conn.execute("ALTER TABLE system_logs ADD COLUMN request_id TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists

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
    """Logs every HTTP request to SQLite. Add via app.add_middleware(RequestLoggerMiddleware).

    Also assigns each request a short request_id and sets it on
    _request_id_ctx for the duration of the request, so any log line
    emitted anywhere in the call stack while handling this request -
    including from a background task the request spawns - can be tagged
    with which request it came from (see BufferLogHandler.emit()).
    """

    async def dispatch(self, request: Request, call_next):
        request_id = uuid.uuid4().hex[:8]
        # Deliberately never reset this contextvar. Each request runs in
        # its own asyncio task, so the value can safely linger for the
        # task's remaining lifetime - and that lingering is exactly what
        # we want: cleanup/completion lines logged AFTER the response has
        # been returned (temp-file deletion, "Download complete", cache
        # writes, etc.) still carry this request's ID and stay grouped
        # with the request that caused them, instead of falling back to
        # "-" and splitting into a separate orphan group in the dashboard.
        _request_id_ctx.set(request_id)

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
                logging.getLogger(__name__).exception("Failed to write request log")

        return response


def _check_admin(key: str):
    if not ADMIN_KEY or key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")



# Same patterns the frontend's "Hide noise" checkbox already filters out
# of the TABLE rows - this list is what applies that same exclusion to
# the SUMMARY counts too. Kept as one shared list here (not duplicated
# per query) so an addition to it can't accidentally cover the table view
# without covering the numbers at the top, or vice versa.
#
# Every entry here is automated internet-wide vulnerability scanning -
# bots sweeping IP ranges for exposed control panels, PHP/Laravel/Drupal
# exploits, leaked .env files, exposed Docker sockets, MCP/JSON-RPC
# probes, and known RCE payloads (e.g. the PHPUnit eval-stdin exploit).
# This traffic hits every public server on the internet, this one
# included, and none of it reflects a real visitor or a real problem
# with this app - it inflated "Client Errors" without meaning anything,
# which is exactly the confusion the success/client/server split above
# was meant to remove.
_NOISE_PATTERNS = (
    "/robots.txt", "/favicon.ico", "/.env", "/wp-", "/.git",
    "/SDK/", "/phpmyadmin", "/.well-known", "/xmlrpc.php",
    "/mcp", "/jsonrpc", "/sse", "/containers/json",
    "eval-stdin.php", "/_ignition/", "/actuator/",
    "/+CSCOE+/", "/+webvpn+/", "phpunit",
)


def _noise_exclusion_sql() -> str:
    """
    Builds the `path NOT LIKE ... AND path NOT LIKE ...` clause shared by
    every count query below. A single f-string fragment rather than
    parameterized placeholders is safe here specifically because
    _NOISE_PATTERNS is a fixed, hardcoded tuple in this file - never
    user input, never request data - so there is no injection surface;
    building it as literal SQL just keeps the call sites below readable.
    """
    return " AND ".join(f"path NOT LIKE '%{p}%'" for p in _NOISE_PATTERNS)


def _status_counts(conn) -> dict:
    """
    Shared by the full-window and delta code paths in get_http_logs()
    below, so the two can never define "success"/"client"/"server"
    differently by accident.

    Three buckets, not two:
      - success: < 400
      - client:  400-499, EXCLUDING known bot/scanner noise - what's left
                 is a real caller's request being rejected for a normal
                 reason (bad upload, rate limit, queue full). Expected
                 traffic, not a bug, but at least now it's actually
                 traffic from someone using the site.
      - server:  >= 500 - the backend itself broke. This is the number
                 worth watching; a spike here means something is actually
                 wrong.

    NOTE: "total" and "success" deliberately still count EVERYTHING,
    noise included - the Total box should reflect true traffic volume
    (useful context: "833 total, most of it noise" is itself information
    worth having), and success is unaffected by noise almost by
    definition (a 404 or 405 scanner hit is never a 2xx). Only "client"
    gets the noise filter, since that is the one number noise was
    actually distorting.
    """
    noise_filter = _noise_exclusion_sql()

    total = conn.execute("SELECT COUNT(*) as c FROM request_logs").fetchone()["c"]
    success = conn.execute(
        "SELECT COUNT(*) as c FROM request_logs WHERE status_code < 400"
    ).fetchone()["c"]
    client_errors = conn.execute(
        f"SELECT COUNT(*) as c FROM request_logs "
        f"WHERE status_code >= 400 AND status_code < 500 AND {noise_filter}"
    ).fetchone()["c"]
    server_errors = conn.execute(
        "SELECT COUNT(*) as c FROM request_logs WHERE status_code >= 500"
    ).fetchone()["c"]
    return {
        "total": total,
        "success": success,
        "client": client_errors,
        "server": server_errors,
    }


@router.get("/admin/logs/http/data")
def get_http_logs(
    key: str = Query(...),
    limit: int = Query(200, le=2000),
    after_id: int = Query(None, description="If set, return only rows with id > after_id (delta/poll mode), newest-last, ignoring `limit`."),
):
    _check_admin(key)
    with get_db() as conn:
        if after_id is not None:
            # Delta mode: used by the dashboard's poll loop. Returns only
            # genuinely new rows since the client's last known id - on a
            # quiet server this is an empty list instead of re-sending the
            # whole window every 3 seconds.
            rows = conn.execute(
                "SELECT * FROM request_logs WHERE id > ? ORDER BY id ASC", (after_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM request_logs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        counts = _status_counts(conn)
    return JSONResponse({**counts, "logs": [dict(r) for r in rows]})


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
# Persisted to the same SQLite DB/volume as request_logs, rather than an
# in-memory deque - a deque gets wiped every time the container restarts
# or redeploys, which made older system logs disappear after every push.

class BufferLogHandler(logging.Handler):
    def emit(self, record):
        try:
            with get_db() as conn:
                conn.execute(
                    "INSERT INTO system_logs (timestamp, level, logger, message, request_id) VALUES (?, ?, ?, ?, ?)",
                    (
                        datetime.utcfromtimestamp(record.created).isoformat(),
                        record.levelname,
                        record.name,
                        record.getMessage(),
                        _request_id_ctx.get(),
                    ),
                )
                conn.commit()
        except Exception:
            pass


def attach_system_log_capture():
    handler = BufferLogHandler()
    handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)


@router.get("/admin/logs/system/data")
def get_system_logs(
    key: str = Query(...),
    limit: int = Query(200, le=2000),
    after_id: int = Query(None, description="If set, return only rows with id > after_id (delta/poll mode), newest-last, ignoring `limit`."),
):
    _check_admin(key)
    with get_db() as conn:
        if after_id is not None:
            # Delta mode - see get_http_logs() for the reasoning. Already
            # ASC (oldest -> newest), so no reversal needed here.
            rows = conn.execute(
                "SELECT * FROM system_logs WHERE id > ? ORDER BY id ASC", (after_id,)
            ).fetchall()
            logs = [dict(r) for r in rows]
        else:
            rows = conn.execute(
                "SELECT * FROM system_logs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            logs = [dict(r) for r in rows][::-1]  # oldest -> newest, for chronological display
        total = conn.execute("SELECT COUNT(*) as c FROM system_logs").fetchone()["c"]
    return JSONResponse({"total": total, "logs": logs})


async def _system_log_event_generator():
    with get_db() as conn:
        row = conn.execute("SELECT MAX(id) as m FROM system_logs").fetchone()
        last_id = row["m"] or 0

    while True:
        await asyncio.sleep(1)
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM system_logs WHERE id > ? ORDER BY id ASC", (last_id,)
            ).fetchall()
        for r in rows:
            last_id = r["id"]
            yield f"data: {json.dumps(dict(r))}\n\n"


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
            cur_sys = conn.execute("DELETE FROM system_logs WHERE timestamp < ?", (cutoff,))
        else:
            cur = conn.execute("DELETE FROM request_logs")
            cur_sys = conn.execute("DELETE FROM system_logs")
        conn.commit()
        deleted_http = cur.rowcount
        deleted_system = cur_sys.rowcount

    return {
        "deleted_http_logs": deleted_http,
        "deleted_system_logs": deleted_system,
        "system_buffer_cleared": older_than_days is None,
    }


# ============================================================
# 4. DASHBOARD UI
#
# This inline HTML dashboard is a separate, self-contained fallback UI
# from the Next.js /admin/logs page - the Next.js page is what you
# actually use day to day, but this one hits the exact same
# get_http_logs()/get_system_logs() endpoints and must stay consistent
# with the same {success, client, server} shape.
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
  .client-error { color: #fbbf24; }
  .server-error { color: #f87171; }
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
  #http-table-wrap { max-height: 500px; overflow-y: auto; border-radius: 8px; }
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
  .manage-section { margin-bottom: 14px; border: 1px solid #262a38; border-radius: 8px; overflow: hidden; }
  .manage-toggle { padding: 8px 12px; font-size: 13px; color: #888; cursor: pointer; background: #14161f; user-select: none; }
  .manage-toggle:hover { color: #e2e2e2; }
  .manage-body { padding: 10px 12px; display: flex; gap: 8px; background: #0f1117; }
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
      <div class="stat-box"><div class="label">Client Errors</div><div class="value client-error" id="clientErrors">-</div></div>
      <div class="stat-box"><div class="label">Server Errors</div><div class="value server-error" id="serverErrors">-</div></div>
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
      <button class="reset-btn" onclick="resetFilters()">Reset</button>
    </div>

    <div class="controls">
      <span class="controls-label">Date:</span>
      <button class="date-btn active" id="dateBtnAll" onclick="setDateFilter('all')">All time</button>
      <button class="date-btn" id="dateBtnToday" onclick="setDateFilter('today')">Today</button>
      <button class="date-btn" id="dateBtnYesterday" onclick="setDateFilter('yesterday')">Yesterday</button>
      <input type="date" id="customDate" onchange="setDateFilter('custom')">
      <span style="flex-grow:1;"></span>
      <button id="pauseBtn" onclick="togglePause()">⏸ Pause</button>
      <span id="pendingBadge" style="display:none; color:#fbbf24; font-size:13px;"></span>
    </div>

    <div class="manage-section">
      <div class="manage-toggle" onclick="toggleManage()">⚙ Manage logs (delete) <span id="manageArrow">▸</span></div>
      <div class="manage-body" id="manageBody" style="display:none;">
        <button onclick="deleteLogs(1)">Delete logs older than 1 day</button>
        <button onclick="deleteLogs(7)">Delete logs older than 7 days</button>
        <button class="reset-btn" onclick="deleteLogs(null)">Delete ALL logs</button>
      </div>
    </div>

    <div id="http-table-wrap">
      <table>
        <thead>
          <tr><th>Time (NPT)</th><th>Method</th><th>Path</th><th>Status</th><th>Duration</th><th>IP</th></tr>
        </thead>
        <tbody id="http-rows"></tbody>
      </table>
    </div>
    <div id="http-empty" class="empty-state" style="display:none;">No requests match the current filters.</div>
    <div id="http-loading" class="empty-state" style="display:none;">Loading...</div>
    <div id="http-error" class="empty-state" style="display:none; color:#f87171;"></div>
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

// Track counts as real numbers, never re-parse DOM text (which starts as
// "-" and turns any increment into NaN forever). Three buckets now, not
// two - see the FIXES APPLIED note at the top of this file for why
// "failed" was replaced with separate client (4xx) and server (5xx)
// counts: a 404 from a bot or a 429 rate-limit is normal client
// behavior, not evidence the app is broken.
let totalCount = 0;
let successCount = 0;
let clientErrorCount = 0;
let serverErrorCount = 0;

function renderCounters() {
  document.getElementById("total").innerText = totalCount;
  document.getElementById("success").innerText = successCount;
  document.getElementById("clientErrors").innerText = clientErrorCount;
  document.getElementById("serverErrors").innerText = serverErrorCount;
}

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
  const hasZone = /Z$|[+-]\d{2}:\d{2}$/.test(isoString);
  const date = new Date(hasZone ? isoString : isoString + "Z");
  return date.toLocaleString("en-US", {
    timeZone: "Asia/Kathmandu",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: true
  });
}

// isoString may already end in "Z" (e.g. new Date().toISOString() from
// nepalTodayYMD()) or may be a bare SQLite timestamp with no zone (e.g.
// "2026-07-19T15:15:16.963554" from the DB). Blindly appending "Z" broke
// the first case ("...000ZZ" -> Invalid Date -> uncaught RangeError in
// formatToParts). Only append "Z" when there's no zone suffix already.
function getNepalYMD(isoString) {
  const hasZone = /Z$|[+-]\d{2}:\d{2}$/.test(isoString);
  const date = new Date(hasZone ? isoString : isoString + "Z");
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
  let logKey;
  try {
    logKey = ymdToKey(getNepalYMD(log.timestamp));
  } catch (e) {
    return false; // unparseable timestamp - exclude from date-filtered views rather than silently breaking the whole filter
  }
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

  // Oldest first, newest at the bottom - matches a typical terminal/tail
  // view (and the System Logs panel, which already appends+autoscrolls).
  const tableWrap = document.getElementById("http-table-wrap");
  const wasNearBottom = tableWrap
    ? tableWrap.scrollHeight - tableWrap.scrollTop - tableWrap.clientHeight < 60
    : true;
  document.getElementById("http-rows").innerHTML = filtered.map(renderHttpRow).join("");
  document.getElementById("http-empty").style.display = filtered.length === 0 ? "block" : "none";
  if (tableWrap && wasNearBottom) {
    tableWrap.scrollTop = tableWrap.scrollHeight;
  }
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

function toggleManage() {
  const body = document.getElementById("manageBody");
  const arrow = document.getElementById("manageArrow");
  const isOpen = body.style.display !== "none";
  body.style.display = isOpen ? "none" : "flex";
  arrow.textContent = isOpen ? "▸" : "▾";
}

async function loadInitialHttp() {
  const loadingEl = document.getElementById("http-loading");
  const errorEl = document.getElementById("http-error");
  loadingEl.style.display = "block";
  errorEl.style.display = "none";
  try {
    const res = await fetch(`/admin/logs/http/data?key=${KEY}&limit=${MAX_LOGS_IN_MEMORY}`);
    if (!res.ok) {
      throw new Error(`Server returned ${res.status}: ${res.statusText}`);
    }
    const data = await res.json();
    totalCount = data.total;
    successCount = data.success;
    clientErrorCount = data.client;
    serverErrorCount = data.server;
    renderCounters();
    allHttpLogs = data.logs.reverse();
    applyFilter();
  } catch (err) {
    errorEl.textContent = `Failed to load logs: ${err.message}. Try reloading the page.`;
    errorEl.style.display = "block";
    console.error("loadInitialHttp failed:", err);
  } finally {
    loadingEl.style.display = "none";
  }
}

const httpSource = new EventSource(`/admin/logs/http/stream?key=${KEY}`);
httpSource.onmessage = (event) => {
  const log = JSON.parse(event.data);
  allHttpLogs.push(log);
  if (allHttpLogs.length > MAX_LOGS_IN_MEMORY) {
    allHttpLogs.shift(); // cap memory growth for long-running tabs
  }

  totalCount++;
  if (log.status_code < 400) {
    successCount++;
  } else if (log.status_code < 500) {
    clientErrorCount++;
  } else {
    serverErrorCount++;
  }
  renderCounters();

  if (isPaused) {
    pendingCount++;
    const badge = document.getElementById("pendingBadge");
    badge.style.display = "inline";
    badge.textContent = `${pendingCount} new request(s) waiting - resume to view`;
    return; // don't touch the DOM while paused, avoids the table jumping under the user
  }

  applyFilter();
};
httpSource.onerror = () => {
  console.warn("HTTP log stream disconnected - browser will auto-retry.");
};

function renderSystemLine(entry) {
  return `<div><span class="level-${entry.level}">[${entry.level}]</span> ${toNepalTime(entry.timestamp)} ${entry.logger} — ${entry.message}</div>`;
}

async function loadInitialSystem() {
  try {
    const res = await fetch(`/admin/logs/system/data?key=${KEY}&limit=200`);
    if (!res.ok) throw new Error(`Server returned ${res.status}`);
    const data = await res.json();
    const panel = document.getElementById("system-panel");
    panel.innerHTML = data.logs.map(renderSystemLine).join("");
    panel.scrollTop = panel.scrollHeight;
  } catch (err) {
    document.getElementById("system-panel").innerHTML =
      `<div style="color:#f87171;">Failed to load system logs: ${err.message}</div>`;
    console.error("loadInitialSystem failed:", err);
  }
}

const systemSource = new EventSource(`/admin/logs/system/stream?key=${KEY}`);
systemSource.onmessage = (event) => {
  const entry = JSON.parse(event.data);
  const panel = document.getElementById("system-panel");
  panel.insertAdjacentHTML("beforeend", renderSystemLine(entry));
  panel.scrollTop = panel.scrollHeight;
};
systemSource.onerror = () => {
  console.warn("System log stream disconnected - browser will auto-retry.");
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

// Wrapped in try/catch so one bad date/edge case can never again block
// the rest of the dashboard from loading.
try {
  document.getElementById("customDate").max = ymdToKey(nepalTodayYMD());
} catch (e) {
  console.error("Failed to set date picker max:", e);
}

try {
  loadFilterPrefs();
} catch (e) {
  console.error("Failed to load filter prefs:", e);
}

loadInitialHttp();
loadInitialSystem();
</script>
</body>
</html>
    """
    html = html.replace("PLACEHOLDER_ADMIN_KEY", ADMIN_KEY)
    return HTMLResponse(content=html)