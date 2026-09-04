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

All endpoints requires ?key=<ADMIN_STATUS_KEY> to match your existing admin key.

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

PERFORMANCE PASS (2026-08-07):
  9. Every logged HTTP request opened a brand-new SQLite connection,
     wrote one row, committed (an fsync), and closed the connection -
     INSIDE the request/response path, before the response was returned
     to the visitor. Every single system log line did the same thing,
     synchronously, from inside whatever code called logger.info(). On a
     $7 VPS with no swap that's the most expensive thing in the whole
     hot path, and it scales with traffic in exactly the wrong
     direction: the busier the site gets, the more each request pays.

     Writes now go onto an in-process queue and are flushed by a single
     background thread in batches (executemany + one commit per batch,
     up to ~500 rows or 250ms, whichever comes first). Logging is now a
     queue append - microseconds - and never touches the disk on the
     request path. The queue is bounded and drops on overflow rather
     than ever blocking a request: losing a log line under extreme load
     is strictly better than adding latency to a real user's response.

 10. Enabled WAL mode (journal_mode=WAL, synchronous=NORMAL). The
     default rollback journal takes an exclusive lock for every write,
     so the dashboard's read queries and the writer thread were
     serialising against each other and periodically throwing
     "database is locked." WAL lets readers and the single writer run
     concurrently, which is the whole reason it exists.

 11. Connections are now reused per-thread instead of opened and closed
     on every single query. sqlite3.connect() is not free - it parses
     the path, opens the file, and re-runs PRAGMA setup each time.

 12. _status_counts() ran FOUR separate COUNT(*) queries - four full
     scans of request_logs - on every poll, every 3 seconds, forever,
     including the one with a 17-clause LIKE chain. Collapsed into a
     single scan using conditional SUM(), and cached for a couple of
     seconds so a burst of polls (or several open dashboard tabs)
     shares one scan instead of each paying for their own.

 13. Added before_id cursor pagination to both data endpoints. "Load
     older entries" previously worked by re-requesting an ever-larger
     `limit`, but `limit` is capped at 2000 server-side, so entry 2001
     of 14,482 was permanently unreachable no matter how many times the
     button was clicked. Paging backwards from a cursor has no ceiling
     and, unlike a growing limit, each page costs the same regardless
     of how far back you have scrolled.

SCHEMA CHANGE (2026-08-10): TOOL / TIER IDENTITY
 14. Path-based filtering (the "family" picker) breaks down whenever one
     variant of a tool reuses another's routes after the initial submit -
     HQ separation is the concrete case: /youtube/stems-hq is hit ONCE
     (the POST), then every one of that job's ~40 status polls, its
     preview and its download all go through the exact same SHARED
     routes the standard tier uses. Filtering the picker to "YouTube
     Stems HQ" therefore showed exactly one row (the submit) - the other
     40+ lines belonging to that same job were indistinguishable, by
     path alone, from a standard-tier job's polls sitting right next to
     them. The only place the real tier existed was as free text inside
     a system log message ("[YOUTUBE_STEMS_HQ] job=... COMPLETE").

     request_logs and system_logs both gain two new columns: `tool` and
     `tier`. Neither is derived from the path. Both are set ONCE by the
     route handler, as soon as it knows what kind of job this actually
     is (see set_job_context() below), and from that point on every log
     line the job produces - regardless of which shared route it hits -
     carries the same tag. This is the exact same mechanism request_id
     already uses (a contextvar, inherited automatically by any
     background task the request spawns), just carrying two more fields.

     Callers (routes.py) are the next step, not this file - this file
     only adds the mechanism and the places that read/write/filter by
     it. Until routes.py calls set_job_context(), tool/tier will simply
     be "-" on every row, same as request_id defaults to "-" for lines
     with no request in flight.

RELIABILITY PASS (2026-08-15): DELTA CURSOR SEEDING
 15. The frontend's delta pollers (fetchHttpDelta / fetchSystemDelta)
     refuse to fire while their cursor (httpLastIdRef / sysLastIdRef) is
     0, and previously that cursor was ONLY set from the newest row of a
     full/paged fetch - i.e. only when that fetch actually matched at
     least one row. Any filter combination that legitimately matched
     zero rows at the moment it was applied (a tool/tier filter before
     that tool's first job of the day, a search term, a fresh empty
     table right after "Delete all logs") left the cursor permanently at
     0. Delta polling for that filter was then dead forever - matching
     rows that arrived afterwards never appeared - until something else
     (manual Refresh, a tab switch, changing the filter again) forced a
     full refetch and re-seeded it by accident.

     Both data endpoints now also return `max_id`: the newest id in the
     table, computed WITHOUT any of the request's filters applied. The
     frontend seeds its cursor from max(newest-matching-row, max_id), so
     even a zero-match response gives it a valid, meaningful cursor -
     "everything up to here has already been considered and didn't
     match" - and delta polling keeps working correctly the instant
     something new does match.

     This also means the after_id (delta) branch can now legitimately be
     asked to resume from id 0 on a fresh table, which previously wasn't
     a state the frontend could produce. That branch ignores `limit` by
     design (a 3s poll should never be capped mid-page), which is fine
     for the normal few-rows-since-last-tick case but would return the
     ENTIRE table if ever asked from 0. Capped at _DELTA_MAX with a
     `truncated` flag so the frontend can detect it and fall back to a
     normal paged fetch instead of silently splicing a truncated slice
     onto its in-memory list.

     Also added a small TTL cache around the filtered_total/total
     COUNT(*) queries (_cached_count), the same fix #12 already applied
     to the status counters - these two counts were added after that
     pass and had quietly reintroduced the same per-poll full-scan cost,
     including on LIKE-based filters that can't use an index.

CORRECTNESS PASS (2026-08-15b): SEARCH, ESCAPING, COUNTS
 16. Searching "500" returned three 500s and two unrelated 200s. The
     rows were real and the filter was working exactly as written - `q`
     ORs across request_id, and request_id is 8 hex characters, so "500"
     is a perfectly valid substring of one. The match was a coincidence
     of encoding, not intent: nobody typing a number into a log search
     means "find this digit sequence inside a hex id." A numeric search
     term now skips request_id and tool entirely (tool tags are
     alphabetic and can never match digits anyway), leaving path,
     client_ip, method and the exact status_code match. Same change on
     the System tab's `q`, where the hex collision is the only effect
     since there's no status_code column there.

 17. LIKE wildcards in user input were taken literally as wildcards.
     Searching for "_" matched every row, and any path segment
     containing "_" in a `family` or `job_id` filter silently
     over-matched, because "_" is LIKE's single-character wildcard.
     Every user-supplied LIKE pattern is now escaped and every LIKE
     carries ESCAPE '\\'. Same treatment for NOISE_PATH_MARKERS, which
     contains literal underscores.

 18. `DELETE FROM request_logs` with no WHERE triggers SQLite's truncate
     optimization, and cursor.rowcount is not reliable under it - "Delete
     all logs" could report "Removed 0 entries" after deleting 815. Both
     branches now COUNT(*) first and report that number, which is true
     regardless of which delete path SQLite takes.

 19. Deleting logs left the WAL file at whatever size it had grown to,
     which on a VPS already at ~69% disk is the opposite of what someone
     pressing Delete is trying to achieve. A full delete now checkpoints
     the WAL and VACUUMs so the space is actually returned.

 20. The SSE generators polled forever with no disconnect check, so every
     closed dashboard tab left a task querying the database once a second
     for the life of the container. Both now stop when the client goes
     away.

 21. datetime.utcnow() is deprecated from Python 3.12. Replaced with an
     explicit UTC-now-as-naive helper that produces the byte-identical
     string format, because `since`/`until` filtering compares timestamps
     as TEXT - a mix of "…T10:00:00" and "…T10:00:00+00:00" rows would
     break those range queries in a way that only shows up on old data.

 22. Composite (level, id) and (status_code, id) indexes. The common
     query on both tabs is "filter by one indexed column, ORDER BY id
     DESC LIMIT n"; with only the single-column index SQLite could use
     the index for the filter or for the ordering, not both, and fell
     back to sorting the matched set.

 23. Rows dropped by the bounded write queue were counted and then never
     surfaced anywhere. The count is now returned by the HTTP data
     endpoint, so silent log loss under load is visible rather than
     needing to be inferred from gaps.

FEATURE + HARDENING (2026-09-04): SILENT ERROR VISIBILITY
 24. The dashboard bucketed everything by HTTP status, so a handler that
     caught its own exception, logged it at ERROR, and still returned a
     200 (with an {"error": ...} body for the UI) was completely
     invisible on the HTTP tab. Two real cases: a midi-worker that was
     unreachable (DNS failure), and rubberband failing on an unsupported
     input file - both logged ERROR + WARNING, both returned 200, both
     showed up in Success. The only way to notice was to already suspect
     something and go read the System tab.

     request_logs gains `error_logged` (0/1) and `error_count`. These
     are set the SAME way tool/tier is - via the per-request mutable dict
     in _job_ctx - so no route has to opt in: BufferLogHandler.emit()
     flips the flag automatically the instant ANY code logs at >= ERROR
     while a request is in flight, from the handler itself, an API
     client, a subprocess wrapper, anything. RequestLoggerMiddleware
     reads it after call_next and stamps it onto the row.

     Surfaced three ways:
       - a "silent" count: rows that returned < 400 but logged an error
         (the true hidden-failure bucket - deliberately non-overlapping
         with client/server so it can't double-count a 5xx).
       - an `errored` filter param that matches EVERY error_logged row
         regardless of status, so a 4xx-with-error is still findable.
       - the row itself carries error_logged/error_count out to the
         frontend for a per-row marker.

     LIMIT: this captures SYNCHRONOUS failures - anything that logs the
     error before the response returns (both examples above). A failure
     inside a background task that outlives the response (an HQ /separate
     job that dies minutes later) is logged after the HTTP row is already
     written, so it won't retro-flag that row; that path is what job
     outcome tracking is for. For a failure that only ever logs at
     WARNING and never ERROR, call mark_request_errored() explicitly at
     the point you log "... FAILED".

 25. HARDENING: the writer thread flushed http_rows and sys_rows under a
     SINGLE try + single commit, so one malformed row in either list
     (wrong tuple arity, a bad type) rolled back and dropped the ENTIRE
     batch - up to ~500 good rows from BOTH tables - on one poison row.
     The two inserts are now independent: a bad HTTP batch can't take the
     system batch down with it, and vice versa, and rows lost to a failed
     insert are added to the same dropped-row counter (#23) so DB-side
     loss is as visible as queue-overflow loss instead of vanishing.

 26. HARDENING: _dropped_rows was a bare `global x += 1` incremented from
     every request/logging thread - a read-modify-write that can lose
     increments under contention, undercounting the one number that
     exists to make loss visible. Now guarded by a lock (cheap - only
     touched on the drop path, which by definition is not the hot path).
--------------------------------------------------------------------------
"""

import asyncio
import atexit
import contextvars
import json
import logging
import os
import queue
import re
import sqlite3
import threading
import time
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware

from admin_auth import guard_admin_request, verify_admin_key
from config import NOISE_PATH_MARKERS

DB_PATH = os.environ.get("REQUEST_LOG_DB_PATH", "/app/data/logs.db")
ADMIN_KEY = os.environ.get("ADMIN_STATUS_KEY", "")

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

router = APIRouter()


def _utcnow_iso() -> str:
    """
    UTC now, formatted exactly as datetime.utcnow().isoformat() always
    was - naive, no zone suffix.

    datetime.utcnow() is deprecated from 3.12, but the obvious
    replacement (datetime.now(timezone.utc)) appends "+00:00", and these
    timestamps are compared as TEXT by the since/until filters. A table
    holding both "2026-08-15T10:00:00" and "2026-08-15T10:00:00+00:00"
    would sort and range-filter incorrectly across the boundary, and the
    breakage would only appear on rows written before the change. Strip
    the tzinfo back off so the stored format is byte-identical.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


# Per-request ID, readable from anywhere in the call stack (including a
# background task spawned via asyncio.create_task() from inside a request,
# since asyncio automatically copies the current contextvars context into
# new tasks). Used to group system log lines by the request that produced
# them - see BufferLogHandler.emit() below.
_request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-"
)

# Tool/tier identity + error state for THIS request - deliberately
# structured differently from _request_id_ctx above, because it becomes
# known at a DIFFERENT TIME and therefore has to cross a task boundary the
# other direction.
#
# THE PROBLEM THIS SOLVES (subtle, and it silently produces wrong data if
# you get it wrong):
#
#   RequestLoggerMiddleware extends Starlette's BaseHTTPMiddleware, whose
#   call_next() runs the downstream app - including the route handler -
#   in a SEPARATE anyio task. contextvars propagate parent -> child (the
#   child task gets a copy of the context at spawn time), but NEVER child
#   -> parent. request_id works fine because it's .set() BEFORE
#   call_next, so the handler inherits it. Tool/tier (and the error flag)
#   is the opposite case: only the handler knows it, and the middleware
#   needs to read it AFTER call_next returns, to stamp the HTTP row. A
#   plain ContextVar.set() inside the handler would be invisible up there.
#
# So the contextvar holds a MUTABLE DICT rather than a string. The
# middleware installs a fresh one per request before call_next; the
# handler's set_job_context() / the log handler's mark_request_errored()
# mutate that same dict in place. Both tasks hold a reference to the
# identical object, so the mutation is visible from both sides - no
# propagation required, because nothing needs to propagate.
#
# Background tasks spawned by the handler (asyncio.create_task) inherit
# the reference too, which is exactly right: a job's later log lines
# belong to the same job and should carry the same tag. Each request gets
# its own dict, so two concurrent requests can never see each other's.
_job_ctx: contextvars.ContextVar = contextvars.ContextVar("log_job_tags", default=None)

# Used when nothing has been tagged: a plain page hit, an admin call, a
# log line emitted at import/startup with no request in flight, or a
# handler that raised before reaching its set_job_context() call. "-"
# matches request_id's own default so an untagged value reads
# unambiguously as "not applicable" rather than as a blank/broken field.
_UNTAGGED = ("-", "-")


def _current_tags() -> tuple:
    """(tool, tier) for whatever request/job owns the current context."""
    tags = _job_ctx.get()
    if not tags:
        return _UNTAGGED
    return (tags.get("tool") or "-", tags.get("tier") or "-")


def get_current_request_id() -> str:
    """Read-only accessor for the current request's id, for callers
    (routes.py) that need to hand it to a subprocess - the subprocess
    can't read this process's contextvar directly, so the value has to
    be captured here and passed explicitly."""
    return _request_id_ctx.get()


def write_system_log_direct(
    level: str,
    logger_name: str,
    message: str,
    request_id: str = "-",
    tool: str = "-",
    tier: str = "-",
) -> None:
    """
    Direct, synchronous system_logs insert for callers OUTSIDE this
    process's own writer-queue machinery - specifically download_worker.py,
    which runs as a separate OS process (see utils.run_in_killable_subprocess)
    and therefore cannot touch this module's in-memory _write_queue or
    per-thread connections; those are Python objects living only in THIS
    process's address space and are invisible across a process boundary,
    same as BufferLogHandler's registration on the root logger.

    Opens and closes a short-lived connection rather than reusing get_db()'s
    per-thread-kept-forever pattern - that pattern exists to amortize
    connection setup across a long-running server process, which a
    downloader subprocess is not. Call volume here is bounded by
    download_progress.py's own throttling (every ~10% or ~3s), so the
    per-call overhead is negligible.

    WAL mode (set inside _configure(), which _new_conn() always calls) is
    what makes this safe to call concurrently with the main process's
    writer thread - that's the whole reason WAL was enabled in the first
    place (see fix #10 at the top of this file).

    NOTE: a subprocess logging an error here cannot flip the parent
    request's error_logged flag (separate address space, and the HTTP row
    was already written before the subprocess even started) - that's the
    same background-task limitation documented under fix #24.
    """
    try:
        conn = _new_conn()
        try:
            conn.execute(
                "INSERT INTO system_logs "
                "(timestamp, level, logger, message, request_id, tool, tier) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    _utcnow_iso(),
                    level,
                    logger_name,
                    message,
                    request_id,
                    tool,
                    tier,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        # A logging failure must never break the download itself.
        pass


def new_job_context() -> dict:
    """
    Installs a fresh, empty tag holder for this request. Called by
    RequestLoggerMiddleware at the very top of dispatch(), BEFORE
    call_next - see the long comment above for why the object has to
    exist before the handler task is spawned rather than being created
    lazily by set_job_context().

    Carries the error state (fix #24) alongside tool/tier so the same
    single per-request object serves both, and so a request that only
    ever errors (never calls set_job_context) still has somewhere to
    record it.
    """
    tags = {"tool": "-", "tier": "-", "errored": False, "error_count": 0}
    _job_ctx.set(tags)
    return tags


def set_job_context(tool: str, tier: str = "standard") -> None:
    """
    The ONE sanctioned way to tag the current request/job with what it
    actually is. Call this once, as early as possible in the route
    handler, as soon as tool/tier is known - e.g.:

        set_job_context("STEMS", "hq" if hq else "standard")

    From that point on, every log line emitted for the rest of this
    request - AND from any background task it spawns via
    asyncio.create_task(), since asyncio copies the current contextvars
    context into new tasks automatically - carries this tag. That's what
    lets a job's ~40 status polls, which all hit a route SHARED with
    every other tier of the same tool, still report which tool and tier
    actually produced them: the tag travels with the JOB, not with
    whichever path a given line happened to hit.

    Mutates in place rather than calling .set() - that's load-bearing,
    not stylistic. See the _job_ctx comment above: .set() here would be
    invisible to RequestLoggerMiddleware, which reads these values from a
    DIFFERENT task after call_next returns.

    Falls back to installing a holder if none exists, so this is safe to
    call from a context with no middleware above it (a test, a startup
    task, a worker script) instead of silently doing nothing. Any error
    state already recorded on an existing holder is preserved.
    """
    tags = _job_ctx.get()
    if tags is None:
        tags = new_job_context()
    tags["tool"] = tool
    tags["tier"] = tier


def mark_request_errored() -> None:
    """
    Flags the CURRENT request/job as having produced an error, and bumps
    its error count. Called automatically by BufferLogHandler.emit() for
    every record at >= ERROR (fix #24), so no route has to remember to
    call it - but exposed publicly for the one case the automatic hook
    can't see: a failure that you only ever log at WARNING/INFO (e.g. a
    "... FAILED: <friendly message>" line). Call it there explicitly.

    Mutates the shared per-request dict in place, same contract as
    set_job_context(), so the flag is visible to the middleware after
    call_next. Falls back to installing a holder if none exists so it's
    safe from anywhere.
    """
    tags = _job_ctx.get()
    if tags is None:
        tags = new_job_context()
    tags["errored"] = True
    tags["error_count"] = (tags.get("error_count") or 0) + 1


# ---------- JOB TAG REGISTRY ----------
# The problem this solves, which the contextvar alone cannot:
#
#   A job's lifecycle spans MANY separate HTTP requests. The submit POST
#   knows what it is and tags itself. But each of the ~40 status polls,
#   plus preview and download, is its OWN request with its OWN fresh
#   context - none of them inherit anything from the POST, because they
#   are not descendants of it in any task sense. They're separate
#   connections, often minutes later. So without this, exactly the rows
#   the feature exists to explain (the shared /youtube/stems/status/<id>
#   polls that look identical between HQ and standard) stayed untagged.
#
# The only thing those requests carry that ties them back is the job id
# in their path. So: when a job is created, remember what its creator
# tagged it as; when a later request handles that same job id, look it
# back up and re-apply the same tag.
#
# In-process dict rather than a DB column on the jobs table: this is
# logging metadata, not job state, and it must never be able to fail or
# slow down a real request. Bounded and FIFO-evicted - a job that fell
# out of the window just logs "-" again, which is the same as the old
# behaviour and strictly better than unbounded growth. Lost on restart
# for the same reason and with the same consequence.
_JOB_TAGS_MAX = 5000
_job_tags: "OrderedDict[str, tuple]" = OrderedDict()
_job_tags_lock = threading.Lock()


def remember_job_tags(job_id: str) -> None:
    """
    Records whatever the CURRENT context is tagged as against this job
    id. Called from routes.py right after create_job(), where
    set_job_context() has already run - so it takes no tool/tier
    arguments, deliberately: re-passing them would create a second place
    that decides what a job is, and the two could disagree.
    """
    if not job_id:
        return
    tool, tier = _current_tags()
    if tool == "-":
        return  # nothing worth remembering
    with _job_tags_lock:
        _job_tags[job_id] = (tool, tier)
        _job_tags.move_to_end(job_id)
        while len(_job_tags) > _JOB_TAGS_MAX:
            _job_tags.popitem(last=False)  # evict oldest


def tag_from_job(job_id: str) -> bool:
    """
    Re-applies a known job's tag to the CURRENT request. Called at the
    top of every status/preview/download handler, which is what finally
    makes a job's whole request lifecycle carry one consistent tool/tier
    instead of only its submit POST.

    Returns whether a tag was found, for callers that want to know;
    everything in routes.py currently ignores it, since "this job
    predates the registry" is a perfectly normal outcome that needs no
    handling - the row just logs "-" like it always used to.
    """
    if not job_id:
        return False
    with _job_tags_lock:
        hit = _job_tags.get(job_id)
        if hit is not None:
            _job_tags.move_to_end(job_id)  # keep active jobs from aging out mid-run
    if hit is None:
        return False
    set_job_context(hit[0], hit[1])
    return True


# ============================================================
# 0. LIKE ESCAPING
# ============================================================
# SQLite's LIKE treats % and _ as wildcards, and there is no way to turn
# that off - the only escape hatch is an explicit ESCAPE clause. Without
# one, a user typing "_" into the search box matched EVERY row, and any
# filter value containing an underscore (which real paths and job ids do)
# silently over-matched. Every LIKE below that takes user or config input
# now goes through _like() and carries ESCAPE '\'.

_LIKE_ESCAPE = "\\"


def _like_escape(value: str) -> str:
    """Escape LIKE's wildcards so a search term means itself."""
    return (
        value.replace(_LIKE_ESCAPE, _LIKE_ESCAPE * 2)
        .replace("%", _LIKE_ESCAPE + "%")
        .replace("_", _LIKE_ESCAPE + "_")
    )


def _like(value: str) -> str:
    """A `%contains%` pattern with wildcards in the value neutralised."""
    return f"%{_like_escape(value)}%"


# SQL fragment appended to every LIKE that uses the above. Kept as a
# constant so no call site can forget it - a LIKE with an escaped pattern
# but no ESCAPE clause matches the backslashes literally and finds
# nothing, which is a worse failure than the bug it was fixing.
_ESC = f" ESCAPE '{_LIKE_ESCAPE}'"


# ============================================================
# 0b. CONNECTION HANDLING
# ============================================================
# One connection per thread, created once and kept. Previously every
# query - including the one fired by the middleware on every single HTTP
# request - paid for a fresh sqlite3.connect() plus PRAGMA setup plus a
# close(). Under a poll loop plus live traffic that adds up to thousands
# of pointless open/close cycles a minute.

_local = threading.local()


def _configure(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.row_factory = sqlite3.Row
    # WAL: readers never block the writer and the writer never blocks
    # readers. Without this the dashboard's polling reads and the log
    # writer contend for an exclusive lock on the same file.
    conn.execute("PRAGMA journal_mode=WAL")
    # NORMAL is the standard companion to WAL: durable across process
    # crashes, only at risk in an OS-level crash/power loss. For a log
    # table that tradeoff is obviously correct, and it removes an fsync
    # from every commit.
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA temp_store=MEMORY")
    # ~8MB page cache (negative = KiB). Keeps the hot end of the log
    # table resident so the common "last N rows" query stays in memory.
    conn.execute("PRAGMA cache_size=-8000")
    # Cap WAL growth. Without this the WAL only checkpoints when SQLite
    # feels like it, and on a box already tight on disk a busy day can
    # leave a large -wal sitting next to the database indefinitely.
    conn.execute("PRAGMA journal_size_limit=16777216")  # 16MB
    return conn


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10.0, check_same_thread=False)
    return _configure(conn)


@contextmanager
def get_db():
    """
    Yields this thread's long-lived connection. Kept as a contextmanager
    so every existing `with get_db() as conn:` call site is unchanged -
    the only difference is that exiting the block no longer closes the
    connection.
    """
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = _new_conn()
        _local.conn = conn
    yield conn


def _init_db():
    conn = _new_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS request_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                duration_ms REAL NOT NULL,
                client_ip TEXT,
                request_id TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON request_logs(timestamp)")
        # Supports get_endpoint_counts()'s GROUP BY path. Without it that
        # becomes a full table scan every cache miss, which gets worse as
        # request_logs grows.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_request_path ON request_logs(path)")
        # Supports the server-side status-class filter (4xx/5xx chips).
        conn.execute("CREATE INDEX IF NOT EXISTS idx_request_status ON request_logs(status_code)")

        # Migration for databases created before request_id was stored on
        # the HTTP side. system_logs has carried a request_id since
        # 2026-07-24, but request_logs never did - which meant the two
        # tables held the two halves of the same story with no way to join
        # them. Seeing a 500 in the HTTP tab told you a request broke;
        # finding the traceback meant eyeballing timestamps in the system
        # tab and hoping nothing else was in flight. Storing the id on
        # both sides turns that into one click.
        try:
            conn.execute("ALTER TABLE request_logs ADD COLUMN request_id TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists

        # Migration for tool/tier identity (see the SCHEMA CHANGE note at
        # the top of this file). Same ALTER-and-swallow pattern as
        # request_id just above - safe to run unconditionally, since
        # SQLite only errors when the column already exists.
        try:
            conn.execute("ALTER TABLE request_logs ADD COLUMN tool TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE request_logs ADD COLUMN tier TEXT")
        except sqlite3.OperationalError:
            pass

        # Migration for silent-error visibility (fix #24). DEFAULT 0 is
        # deliberate: SQLite backfills existing rows with the default on
        # read, so pre-migration rows count as "no error logged" without a
        # NULL to guard against in the SUM() aggregations below.
        try:
            conn.execute("ALTER TABLE request_logs ADD COLUMN error_logged INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE request_logs ADD COLUMN error_count INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass

        # Supports the new tool/tier filters in get_http_logs(). Without
        # this, "show me every HQ job" is a full scan of request_logs.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_request_tool ON request_logs(tool)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_request_tier ON request_logs(tier)")

        # Composite (filter column, id) indexes - fix #22. Every query the
        # dashboard issues is "filter on one indexed column, ORDER BY id
        # DESC LIMIT n". With only the single-column index SQLite can use
        # it for the filter OR satisfy the ordering from the rowid, not
        # both, so it materialises and sorts the whole matched set - fine
        # at 800 rows, not at 100k. Including id in the index gives it an
        # ordered scan it can stop early.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_request_status_id "
            "ON request_logs(status_code, id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_request_tool_id ON request_logs(tool, id)"
        )
        # Same composite reasoning for the new `errored` filter: "rows that
        # logged an error, newest first".
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_request_error_id "
            "ON request_logs(error_logged, id)"
        )

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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_system_request_id ON system_logs(request_id)")
        # Supports the server-side level filter on the System tab.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_system_level ON system_logs(level)")

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

        # Same tool/tier migration as request_logs above - see the SCHEMA
        # CHANGE note at the top of this file.
        try:
            conn.execute("ALTER TABLE system_logs ADD COLUMN tool TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE system_logs ADD COLUMN tier TEXT")
        except sqlite3.OperationalError:
            pass
        conn.execute("CREATE INDEX IF NOT EXISTS idx_system_tool ON system_logs(tool)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_system_tier ON system_logs(tier)")
        # Same composite reasoning as request_logs above. "level=ERROR,
        # newest first" is the single most-used query on the System tab.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_system_level_id ON system_logs(level, id)"
        )

        conn.commit()
    finally:
        conn.close()


_init_db()


# ============================================================
# 0c. BATCHED BACKGROUND WRITER
# ============================================================
# Nothing on the request path writes to SQLite directly any more. Both
# the request-logging middleware and the logging handler just append to
# this queue; a single daemon thread owns the only write connection and
# flushes in batches.
#
# One writer thread (not a pool) is deliberate: SQLite allows exactly one
# writer at a time regardless, so extra threads would only contend. One
# thread with executemany also means one commit amortised across up to
# 500 rows instead of one commit per row.

_HTTP = 0
_SYS = 1

_MAX_QUEUE = 20000      # ~20k pending rows before we start dropping
_BATCH_MAX = 500        # rows per flush
_BATCH_WINDOW = 0.25    # seconds to wait accumulating a batch

_write_queue: "queue.Queue[tuple]" = queue.Queue(maxsize=_MAX_QUEUE)
_dropped_rows = 0
_dropped_lock = threading.Lock()


def _bump_dropped(n: int = 1) -> None:
    """Thread-safe increment of the dropped-row counter (fix #26). A bare
    `global += n` from many threads can lose increments to a read-modify-
    write race, undercounting the exact signal this counter exists for."""
    global _dropped_rows
    with _dropped_lock:
        _dropped_rows += n


def get_dropped_row_count() -> int:
    """How many log rows have been discarded since start - both from the
    bounded queue overflowing (fix #23) and from a batch insert failing
    in the writer (fix #25).

    Surfaced by the HTTP data endpoint. This used to be incremented and
    then never read anywhere, which meant the one condition it exists to
    signal - logging having gone lossy - was invisible. A gap in the log
    is exactly the kind of thing you'd otherwise spend an hour chasing as
    a bug in the code that failed to log.
    """
    return _dropped_rows


def _enqueue(kind: int, row: tuple) -> None:
    """Never blocks, never raises. Dropping a log line is always better
    than adding latency to a real request or deadlocking the logger."""
    try:
        _write_queue.put_nowait((kind, row))
    except queue.Full:
        _bump_dropped()


def _writer_loop() -> None:
    conn = _new_conn()
    while True:
        try:
            first = _write_queue.get()  # blocks until there's work
            batch = [first]
            deadline = time.monotonic() + _BATCH_WINDOW
            while len(batch) < _BATCH_MAX:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    batch.append(_write_queue.get(timeout=remaining))
                except queue.Empty:
                    break

            http_rows = [r for k, r in batch if k == _HTTP]
            sys_rows = [r for k, r in batch if k == _SYS]

            # Each table's insert is isolated (fix #25). Previously both
            # ran under one try + one commit, so a single malformed row in
            # EITHER list rolled back and dropped the whole batch from BOTH
            # tables. Now a poison HTTP batch can't take the system batch
            # down with it, and rows lost to a failed insert are counted as
            # dropped so the loss stays visible instead of vanishing.
            if http_rows:
                try:
                    conn.execute("BEGIN")
                    conn.executemany(
                        "INSERT INTO request_logs "
                        "(timestamp, method, path, status_code, duration_ms, client_ip, "
                        "request_id, tool, tier, error_logged, error_count) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        http_rows,
                    )
                    conn.commit()
                except Exception:
                    _safe_rollback(conn)
                    _bump_dropped(len(http_rows))
            if sys_rows:
                try:
                    conn.execute("BEGIN")
                    conn.executemany(
                        "INSERT INTO system_logs "
                        "(timestamp, level, logger, message, request_id, tool, tier) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        sys_rows,
                    )
                    conn.commit()
                except Exception:
                    _safe_rollback(conn)
                    _bump_dropped(len(sys_rows))
        except Exception:
            # Never let the writer thread die - a dead writer would
            # silently stop all logging for the life of the container.
            # Deliberately not using logging here: a failure inside the
            # log writer must not re-enter the log writer.
            try:
                conn.rollback()
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = _new_conn()
            time.sleep(0.5)


def _safe_rollback(conn: sqlite3.Connection) -> None:
    try:
        conn.rollback()
    except Exception:
        pass


_writer_thread = threading.Thread(target=_writer_loop, name="log-writer", daemon=True)
_writer_thread.start()


def _flush_pending(timeout: float = 3.0) -> None:
    """
    Give the writer a moment to drain before the process exits.

    The writer is a daemon thread, so a normal shutdown kills it wherever
    it happens to be - up to a full batch window's worth of queued rows
    vanish. deploy.yml does stop+rm+run on every push, so that's every
    deploy, and the rows lost are the ones written immediately before a
    restart, which are disproportionately the interesting ones. Bounded
    wait: a slow flush must never hold up a container stop.
    """
    deadline = time.monotonic() + timeout
    while not _write_queue.empty() and time.monotonic() < deadline:
        time.sleep(0.05)


atexit.register(_flush_pending)


# ============================================================
# 1. HTTP REQUEST LOGGING (SQLite-backed)
# ============================================================

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

    The row is handed to the background writer rather than inserted
    inline, so the visitor's response is never waiting on a disk commit.
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

        # Install the tool/tier/error holder BEFORE call_next. This
        # ordering is required, not incidental: call_next runs the route
        # handler in a separate anyio task that inherits a COPY of the
        # context as it exists right now. The handler's set_job_context()
        # and the log handler's mark_request_errored() then mutate this
        # same dict object, which is how their values get back here despite
        # child->parent context propagation not existing. See the _job_ctx
        # comment near the top of this file for the full explanation.
        tags = new_job_context()

        start = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - start) * 1000

        # Read AFTER the handler has run, so a handler that called
        # set_job_context() - or any ERROR logged synchronously during the
        # request - is reflected here. Reading `tags` directly rather than
        # a helper because this task's own contextvar holds the identical
        # object either way.
        tool = tags.get("tool") or "-"
        tier = tags.get("tier") or "-"
        error_logged = 1 if tags.get("errored") else 0
        error_count = int(tags.get("error_count") or 0)

        # Was "/admin/logs" only - which meant every OTHER admin call
        # (/admin/endpoints, /admin/status, /admin/clear-cache,
        # /admin/cookies/status...) still got logged as ordinary traffic.
        # That's the actual reason admin routes were showing up as fake
        # "tools" in the endpoint picker: the picker's traffic-derived
        # entries were built from real logged rows, and those rows should
        # never have existed. Broadened to all of /admin - operator
        # tooling isn't a product "tool" and was never meant to appear in
        # a dashboard that's specifically ABOUT that traffic.
        if not request.url.path.startswith("/admin"):
            try:
                _enqueue(
                    _HTTP,
                    (
                        _utcnow_iso(),
                        request.method,
                        request.url.path,
                        response.status_code,
                        round(duration_ms, 2),
                        _get_real_client_ip(request),
                        request_id,
                        tool,
                        tier,
                        error_logged,
                        error_count,
                    ),
                )
            except Exception:
                pass  # logging must never break a real response

        return response


def _check_admin(request: Request, key: str):
    """
    Rate-limited + lockout-protected admin check, shared by every
    /admin/logs* route below. Replaces the previous bare equality check
    (`if not ADMIN_KEY or key != ADMIN_KEY: raise 401`) - see
    admin_auth.py for why a plain comparison alone isn't enough: a
    Strix pentest run demonstrated an automated agent firing thousands
    of key guesses in quick succession with nothing to slow it down.

    NOTE: kept as 401 on failure (not 403 like routes.py/cookie_upload.py)
    to preserve this file's existing status-code contract for its
    callers - only the PROTECTION got stricter here, the response shape
    for "you got the key wrong" is unchanged.
    """
    if not ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    client_ip = guard_admin_request(request)
    try:
        verify_admin_key(key, client_ip)
    except HTTPException as e:
        # verify_admin_key raises 403 on a wrong key; this file's own
        # contract is 401 for "unauthorized" - translate so callers of
        # this function see the same status code they always have.
        if e.status_code == 403:
            raise HTTPException(status_code=401, detail="Unauthorized")
        raise


# Same patterns the frontend's "Hide noise" checkbox filters out of the
# TABLE rows - this applies that same exclusion to the SUMMARY counts.
# Sourced from config.NOISE_PATH_MARKERS, the one canonical list every
# consumer (this SQL exclusion, the fallback dashboard's embedded JS
# below, and the Next.js dashboard via admin_endpoints()) reads from -
# see that constant's docstring for why three independently-maintained
# copies used to exist here and had already drifted out of sync.
def _noise_exclusion_sql() -> str:
    """
    Builds the `path NOT LIKE ... AND path NOT LIKE ...` clause shared by
    every count query below. A single f-string fragment rather than
    parameterized placeholders is safe here specifically because
    NOISE_PATH_MARKERS is a fixed, hardcoded tuple in config.py - never
    user input, never request data - so there is no injection surface;
    building it as literal SQL just keeps the call sites below readable.

    The markers ARE escaped for LIKE wildcards though (fix #17): several
    of them contain underscores, and an unescaped "_" is a
    single-character wildcard, so "/wp_admin" would also have excluded
    "/wpXadmin". Quietly wrong rather than obviously wrong, which is
    worse.
    """
    return " AND ".join(
        f"path NOT LIKE '%{_like_escape(p)}%'{_ESC}" for p in NOISE_PATH_MARKERS
    )


# Built once at import instead of re-joining 17 strings on every request.
_NOISE_SQL = _noise_exclusion_sql()

# `silent` (fix #24): returned < 400 (looked fully successful) but logged
# an error. Deliberately scoped to < 400 so it never overlaps the client
# (4xx) or server (5xx) buckets - the headline number stays clean and
# can't double-count a 5xx. The `errored` FILTER below is broader (any
# error_logged row, any status), so a 4xx-with-error is still findable
# even though it isn't in this count.
_COUNTS_SQL = f"""
    SELECT
        COUNT(*)                                                       AS total,
        COALESCE(SUM(status_code < 400), 0)                            AS success,
        COALESCE(SUM(status_code >= 400 AND status_code < 500
                     AND {_NOISE_SQL}), 0)                             AS client,
        COALESCE(SUM(status_code >= 500), 0)                           AS server,
        COALESCE(SUM(status_code < 400 AND error_logged = 1), 0)       AS silent
    FROM request_logs
"""

_COUNTS_TTL = 2.0  # seconds
_counts_lock = threading.Lock()
_counts_cache: dict = {"at": 0.0, "val": None}

# ---------- FILTERED-COUNT CACHE (fix #15) ----------
# get_http_logs()/get_system_logs() each run a filtered_total COUNT(*)
# (and get_system_logs also runs an unfiltered `total` COUNT) on every
# single request - including every 3s poll tick, per open tab. Some of
# those clauses are LIKE-based (`q` search) and can't use an index, so
# under a handful of open dashboard tabs this was a real, avoidable scan
# cost repeating every few seconds for a number that moves by single
# digits between ticks. Same shape as fix #12's _status_counts cache,
# generalised to arbitrary WHERE clauses via a (table, clause, params)
# key.
_FILTERED_COUNT_TTL = 2.0
_filtered_count_lock = threading.Lock()
_filtered_count_cache: dict = {}


def _cached_count(conn, table: str, clause: str, params: tuple) -> int:
    """COUNT(*) under the current filter set, memoised for a couple of
    seconds. `table` is interpolated rather than parameterised because
    SQLite can't parameterise identifiers and every call site passes a
    literal ("request_logs" or "system_logs"), never request data."""
    key = (table, clause, params)
    now = time.monotonic()
    with _filtered_count_lock:
        hit = _filtered_count_cache.get(key)
        if hit is not None and (now - hit[0]) < _FILTERED_COUNT_TTL:
            return hit[1]
    val = conn.execute(
        f"SELECT COUNT(*) AS c FROM {table}{clause}", params
    ).fetchone()["c"]
    with _filtered_count_lock:
        # Bounded: each distinct filter combination is its own key, and a
        # user fiddling with filters generates new ones faster than old
        # ones expire. Reset rather than let this grow unbounded.
        if len(_filtered_count_cache) > 200:
            _filtered_count_cache.clear()
        _filtered_count_cache[key] = (now, val)
    return val


def _invalidate_counts() -> None:
    with _counts_lock:
        _counts_cache["at"] = 0.0
        _counts_cache["val"] = None
    # Endpoint totals describe the same table, so a deletion invalidates
    # both - otherwise the tool picker would keep showing pre-delete
    # numbers for up to _ENDPOINT_COUNTS_TTL after logs were cleared.
    with _endpoint_counts_lock:
        _endpoint_counts_cache["at"] = 0.0
        _endpoint_counts_cache["val"] = None
    # Same reasoning, same table, for the tool/tier counts.
    with _tool_counts_lock:
        _tool_counts_cache["at"] = 0.0
        _tool_counts_cache["val"] = None
    # Same reasoning, for the filtered_total/total caches used by the
    # data endpoints - a deletion invalidates every filter combination
    # currently cached, since the underlying table just changed.
    with _filtered_count_lock:
        _filtered_count_cache.clear()


# ---------- PER-ENDPOINT TOTALS ----------
# The dashboard's tool picker used to count rows in whatever the browser
# had LOADED, which meant the numbers visibly shrank as the in-memory
# window trimmed older rows - "/download 967" quietly becoming "/download
# 233" looked like requests had disappeared. These are the real totals
# from the database, so the picker shows a number that doesn't move
# depending on how far someone has scrolled.
#
# Grouping happens in Python, not SQL, deliberately: the family rule
# (strip trailing action segments and job ids, keep namespaced tools
# intact) has no clean SQL expression, and re-implementing it in SQL
# would make a THIRD copy of that rule alongside routes.py's
# admin_endpoints() and page.tsx's toolFamily(). Two already have to be
# kept in agreement; a third in a different language would drift.
#
# One GROUP BY over an indexed column, then a pass over distinct paths -
# not one query per endpoint. Cached on the same TTL as the status
# counters for the same reason: several dashboard tabs polling shouldn't
# each pay for their own scan.
#
# NOTE: this stays PATH-based (family), same as before. It is a separate
# question from tool/tier - "which URL shape got hit" vs "which tool/tier
# actually ran" - and conflating them is exactly the bug tool/tier exists
# to fix. A parallel tool/tier aggregate (for a future picker) is a
# follow-up, not part of this change.

_ACTION_SEGMENTS = {"status", "preview", "download", "result"}
_ID_LIKE = re.compile(r"^[0-9a-f]{6,}(-[0-9a-f]{4,}){0,4}$", re.IGNORECASE)


def _tool_family(path: str) -> str:
    """Mirrors toolFamily() in page.tsx and the family loop in
    admin_endpoints(). The i > 0 guard is what keeps "/download" - a real
    tool whose name collides with an action segment - from collapsing to
    nothing."""
    parts = []
    for i, seg in enumerate([s for s in path.split("/") if s]):
        if i > 0 and (seg in _ACTION_SEGMENTS or _ID_LIKE.match(seg)):
            break
        if _ID_LIKE.match(seg):
            break
        parts.append(seg)
    return "/" + "/".join(parts) if parts else path


_ENDPOINT_COUNTS_TTL = 10.0  # longer than status counts - this moves slowly
_endpoint_counts_lock = threading.Lock()
_endpoint_counts_cache: dict = {"at": 0.0, "val": None}


def get_endpoint_counts() -> dict:
    """
    Returns {family_path: total_request_count} across the WHOLE table.

    Noise is excluded using the same shared NOISE_PATH_MARKERS the
    Client Errors count uses, so a scanner probing hundreds of junk
    paths can't flood the picker with meaningless families.
    """
    now = time.monotonic()
    cached = _endpoint_counts_cache["val"]
    if cached is not None and (now - _endpoint_counts_cache["at"]) < _ENDPOINT_COUNTS_TTL:
        return cached

    counts: dict = {}
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT path, COUNT(*) AS c FROM request_logs WHERE {_NOISE_SQL} GROUP BY path"
        ).fetchall()
    for row in rows:
        family = _tool_family(row["path"])
        counts[family] = counts.get(family, 0) + row["c"]

    with _endpoint_counts_lock:
        _endpoint_counts_cache["at"] = now
        _endpoint_counts_cache["val"] = counts
    return counts


_TOOL_COUNTS_TTL = 10.0  # same cadence as endpoint counts - this moves slowly too
_tool_counts_lock = threading.Lock()
_tool_counts_cache: dict = {"at": 0.0, "val": None}


def get_tool_counts() -> dict:
    """
    Returns {tool_tag: {"standard": n, "hq": n, "total": n}, ...} across
    the WHOLE table - the tool/tier equivalent of get_endpoint_counts()
    above, except grouped by the tags set_job_context() actually WROTE
    (see routes.py) instead of derived by guessing from the path.

    This is what makes the frontend's tool/tier filter dropdown dynamic:
    it lists only tags that have genuinely appeared in the data, with
    real counts, and needs no hand-maintained list on either side that
    could drift out of sync with what routes.py actually tags things as.

    Rows where tool is NULL, "-", or "" are excluded - those are rows
    from before this migration, or from a request whose handler never
    called set_job_context() (a stray internal call, a request that
    errored before reaching that line) - there's nothing meaningful to
    offer as a filter option for "unknown".

    NOTE: unlike get_endpoint_counts()/admin_endpoints(), which can show
    a registered tool with zero traffic (sourced from FastAPI's own route
    table), a tool tag only appears here once at least one real request
    has carried it. There is no equivalent "table of every tag that
    could ever be set" to merge in zero-traffic entries against - tags
    are just contextvar values, not a registered structure - so a
    brand-new tool genuinely won't show up in the filter until it has
    been used at least once. Acceptable: the filter's whole job is
    describing what's IN the data, and an unused tool has nothing to
    filter for yet anyway.
    """
    now = time.monotonic()
    cached = _tool_counts_cache["val"]
    if cached is not None and (now - _tool_counts_cache["at"]) < _TOOL_COUNTS_TTL:
        return cached

    with get_db() as conn:
        rows = conn.execute(
            "SELECT tool, tier, COUNT(*) AS c FROM request_logs "
            "WHERE tool IS NOT NULL AND tool != '-' AND tool != '' "
            "GROUP BY tool, tier"
        ).fetchall()

    counts: dict = {}
    for r in rows:
        tool = r["tool"]
        # Anything that isn't literally "hq" buckets as "standard" -
        # defensive against a future tier value nobody anticipated
        # rather than silently dropping the count.
        tier = r["tier"] if r["tier"] == "hq" else "standard"
        entry = counts.setdefault(tool, {"standard": 0, "hq": 0, "total": 0})
        entry[tier] += r["c"]
        entry["total"] += r["c"]

    with _tool_counts_lock:
        _tool_counts_cache["at"] = now
        _tool_counts_cache["val"] = counts
    return counts


def _status_counts(conn) -> dict:
    """
    Shared by the full-window and delta code paths in get_http_logs()
    below, so the two can never define the buckets differently by accident.

    Four buckets:
      - success: < 400
      - client:  400-499, EXCLUDING known bot/scanner noise - what's left
                 is a real caller's request being rejected for a normal
                 reason (bad upload, rate limit, queue full). Expected
                 traffic, not a bug.
      - server:  >= 500 - the backend itself broke. A spike here means
                 something is actually wrong.
      - silent:  < 400 but an error was logged during the request (fix
                 #24). This is the bucket that catches a handler that
                 caught its own exception, logged it, and still returned
                 200 - a real failure the status code hides.

    NOTE: "total" and "success" deliberately still count EVERYTHING,
    noise included - the Total box should reflect true traffic volume,
    and success is unaffected by noise almost by definition. Only "client"
    gets the noise filter, since that is the one number noise distorts.

    NOTE 2: "silent" is a SUBSET of "success" (both require < 400), not a
    fifth disjoint slice - a silent-error row is counted in success too.
    That's intentional: it stayed in Success historically, and the point
    of the silent count is to say "N of your successes weren't."

    PERF: one scan, not five, and memoised for _COUNTS_TTL seconds.
    """
    now = time.monotonic()
    cached = _counts_cache["val"]
    if cached is not None and (now - _counts_cache["at"]) < _COUNTS_TTL:
        return cached

    row = conn.execute(_COUNTS_SQL).fetchone()
    val = {
        "total": row["total"],
        "success": row["success"],
        "client": row["client"],
        "server": row["server"],
        "silent": row["silent"],
    }
    with _counts_lock:
        _counts_cache["at"] = now
        _counts_cache["val"] = val
    return val


# Hard cap on the after_id (delta) branch of both data endpoints.
# `after_id` deliberately ignores `limit` - correct for a 3-second poll
# returning a handful of rows, catastrophic if a client ever asks from a
# low/zero cursor, which is the entire table in one response. The
# frontend can now legitimately hold a cursor of 0 on a fresh table or a
# zero-match filter (see fix #15 above), so this went from theoretical to
# reachable. Capped, and the response says so via `truncated` so the
# client re-seeds from a normal page instead of splicing a truncated
# middle onto its in-memory list.
_DELTA_MAX = 2000


def _build_search_clause(
    q: str,
    text_columns: list,
    *,
    id_columns: list = (),
    status_column: str = None,
) -> tuple:
    """
    Builds the OR clause for the global `q` search box, shared by both
    data endpoints so they can never disagree about what a search term
    means.

    THE BUG THIS EXISTS TO FIX (fix #16): searching "500" on a table with
    five 5xx rows returned three 500s and two unrelated 200s. Both 200s
    were real and the SQL was doing exactly what it said - `q` ORed
    across request_id, and request_id is 8 hex characters, so "500" is a
    perfectly ordinary substring of one. The match was an artifact of the
    id's encoding, not of anything the operator meant.

    So `id_columns` (request_id, tool) are searched ONLY for non-numeric
    terms:
      - request_id is hex, so digits collide with it by coincidence
      - tool tags are alphabetic and can never match a digit string at
        all, so including them for a numeric term is pure cost

    A bare 3-digit term additionally matches status_code exactly, IN
    ADDITION to the text columns rather than instead of them, because
    "500" can legitimately appear in a path.

    Returns (clause, params). Every LIKE carries the ESCAPE clause so
    wildcards in the search term mean themselves (fix #17).
    """
    stripped = q.strip()
    if not stripped:
        return None, []

    pattern = _like(stripped)
    numeric = stripped.isdigit()

    parts = [f"{col} LIKE ?{_ESC}" for col in text_columns]
    params = [pattern] * len(text_columns)

    if not numeric:
        parts += [f"{col} LIKE ?{_ESC}" for col in id_columns]
        params += [pattern] * len(id_columns)

    if numeric and status_column and len(stripped) == 3:
        parts.append(f"{status_column} = ?")
        params.append(int(stripped))

    return "(" + " OR ".join(parts) + ")", params


@router.get("/admin/logs/http/data")
def get_http_logs(
    request: Request,
    key: str = Query(...),
    limit: int = Query(200, le=2000),
    after_id: int = Query(None, description="If set, return only rows with id > after_id (delta/poll mode), ignoring `limit`."),
    before_id: int = Query(None, description="If set, return one page of OLDER rows with id < before_id, capped at `limit`."),
    family: str = Query(None, description="Tool family, e.g. '/volume' also matches '/volume/status/<id>'."),
    method: str = Query(None, description="HTTP method filter, e.g. 'POST'."),
    q: str = Query(None, description="Global substring search across path, client_ip and method; plus request_id and tool for non-numeric terms. A bare 3-digit value also matches status_code exactly."),
    status_class: str = Query(None, description="'4xx' or '5xx'."),
    errored: bool = Query(False, description="Only rows where an ERROR-level log fired during the request (any status). Catches silent failures that returned 2xx."),
    hide_noise: bool = Query(False, description="Exclude known bot/scanner paths."),
    since: str = Query(None, description="ISO UTC timestamp, inclusive lower bound."),
    until: str = Query(None, description="ISO UTC timestamp, exclusive upper bound."),
    job_id: str = Query(None, description="Every row whose path contains this job id, across all routes and methods."),
    tool: str = Query(None, description="Exact tool tag set via set_job_context(), e.g. 'STEMS'. Independent of path/family - see set_job_context()'s docstring."),
    tier: str = Query(None, description="'standard' or 'hq', set alongside tool via set_job_context()."),
):
    """
    ALL filtering happens here, in SQL, deliberately.

    It used to happen in the browser over whatever rows were already
    loaded, which meant every filter silently under-reported the moment
    the real result set was bigger than the loaded window: a tool with 6
    requests older than the window showed "No requests match", and the
    4xx/5xx chips, method dropdown, date filter and path search all had
    the same flaw. The stat boxes at the top were computed over the whole
    table, so they disagreed with the list below them - two answers to
    the same question, which is exactly what makes a dashboard
    untrustworthy.

    Filtering in SQL means the answer doesn't depend on how far someone
    has scrolled. `filtered_total` is returned alongside so the UI can
    say how many rows actually match, rather than how many happen to be
    in memory. `max_id` is returned so the client's delta-poll cursor can
    always be seeded, even when this particular query matches nothing -
    see fix #15 at the top of this file.

    Date filtering takes explicit `since`/`until` UTC timestamps rather
    than a "today"/"yesterday" keyword: the dashboard displays Nepal
    time (UTC+5:45), and computing that boundary here would duplicate
    timezone logic that the frontend already has to own for rendering.
    The client sends the exact window it means.

    `tool`/`tier` are a SEPARATE axis from `family`: family groups by URL
    shape (useful for "how much /convert traffic"), tool/tier reports
    what the handler actually decided this request/job IS, which stays
    correct even when several tiers share the same polling routes. See
    set_job_context()'s docstring for the full reasoning.

    `errored` (fix #24) is a THIRD axis: "did any code log an error while
    handling this request", regardless of what status it returned. It's
    what surfaces the 200-that-actually-failed case. Unlike the `silent`
    COUNT (which is < 400 only, to stay non-overlapping with the other
    buckets), this filter matches every error_logged row so a 4xx that
    also broke internally is still findable.

    ROW ORDER: the default and before_id branches return NEWEST-FIRST and
    the client reverses them; the after_id branch returns oldest-first.
    That asymmetry is deliberate (LIMIT has to take the newest N, which
    requires DESC) and the frontend depends on it - note that
    get_system_logs() reverses server-side instead, so the two endpoints
    do NOT have the same contract here.
    """
    _check_admin(request, key)

    where: list = []
    params: list = []

    if family:
        # Matches the family itself plus anything nested under it. The
        # trailing slash in the LIKE is what stops '/separate' from also
        # matching '/separate-hq'. Escaped because real families contain
        # underscores, which LIKE would otherwise treat as wildcards.
        where.append(f"(path = ? OR path LIKE ?{_ESC})")
        params.extend([family, _like_escape(family) + "/%"])
    if method:
        where.append("method = ?")
        params.append(method)
    if q:
        # See _build_search_clause for the full reasoning. Short version:
        # this used to be `path LIKE ?` and nothing else, which meant the
        # one text box on the dashboard could answer exactly one question
        # and silently returned nothing for an IP, a job id, a tool tag or
        # a status code. Widening it introduced a second problem - numeric
        # terms colliding with hex request ids - which is why id columns
        # are conditional now.
        #
        # PERF: this is a full scan, deliberately. An OR across columns
        # can't use any index regardless, and search is a debounced,
        # human-triggered action (250ms in page.tsx), not part of the 3s
        # poll loop. One scan per typed phrase is the right trade for a
        # box that actually finds things.
        clause, search_params = _build_search_clause(
            q,
            text_columns=["path", "client_ip", "method"],
            id_columns=["request_id", "tool"],
            status_column="status_code",
        )
        if clause:
            where.append(clause)
            params.extend(search_params)
    if status_class == "4xx":
        where.append("status_code >= 400 AND status_code < 500")
    elif status_class == "5xx":
        where.append("status_code >= 500")
    if errored:
        # Every row that logged an error, any status. Broader than the
        # `silent` count on purpose - see the docstring.
        where.append("error_logged = 1")
    if hide_noise:
        where.append(f"({_NOISE_SQL})")
    if since:
        where.append("timestamp >= ?")
        params.append(since)
    if until:
        where.append("timestamp < ?")
        params.append(until)
    if job_id:
        # A job's lifecycle spans several DIFFERENT routes and methods:
        #   POST /youtube/stems-hq              (submit - the only row
        #                                        that touches the -hq path)
        #   GET  /youtube/stems/status/<id>     (~40 polls)
        #   GET  /youtube/stems/preview/<id>
        #   GET  /youtube/stems/download/<id>
        #
        # Filtering by tool family splits those apart - an HQ job shows
        # exactly ONE row under "YouTube Stems HQ" because every
        # subsequent request legitimately goes to the shared standard
        # route (HQ deliberately reuses those endpoints, see
        # youtube_separate_hq_route in routes.py). The job id is the only
        # thing common to all of them, so it's what actually answers
        # "show me everything this one job did".
        where.append(f"path LIKE ?{_ESC}")
        params.append(_like(job_id))
    if tool:
        # Exact match, not LIKE - tool is a fixed vocabulary set by
        # set_job_context() (e.g. "STEMS", "CONVERT"), not free text.
        where.append("tool = ?")
        params.append(tool)
    if tier:
        where.append("tier = ?")
        params.append(tier)

    def _clause(extra: str = "") -> str:
        parts = list(where)
        if extra:
            parts.append(extra)
        return (" WHERE " + " AND ".join(parts)) if parts else ""

    truncated = False
    with get_db() as conn:
        if after_id is not None:
            # LIMIT + 1 so we can distinguish "exactly at the cap" from
            # "more behind it" without a second query.
            rows = conn.execute(
                f"SELECT * FROM request_logs{_clause('id > ?')} ORDER BY id ASC LIMIT ?",
                (*params, after_id, _DELTA_MAX + 1),
            ).fetchall()
            if len(rows) > _DELTA_MAX:
                rows = rows[:_DELTA_MAX]
                truncated = True
        elif before_id is not None:
            rows = conn.execute(
                f"SELECT * FROM request_logs{_clause('id < ?')} ORDER BY id DESC LIMIT ?",
                (*params, before_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT * FROM request_logs{_clause()} ORDER BY id DESC LIMIT ?",
                (*params, limit),
            ).fetchall()

        # How many rows match the CURRENT filters across the whole table.
        # Without this the UI can only report "showing N of what's
        # loaded", which is the number that was misleading in the first
        # place.
        filtered_total = _cached_count(conn, "request_logs", _clause(), tuple(params))

        # Newest id in the table, IGNORING every filter. This exists for
        # one specific reason: the client seeds its delta-poll cursor
        # from the newest row it received, and a filter matching nothing
        # gives it nothing to seed from - leaving the cursor at 0, which
        # its delta poller treats as "not ready" and refuses to send.
        # Delta polling then stayed permanently dead for that filter and
        # later matching rows never arrived. max_id is the correct
        # starting point in that case: every row up to it has already
        # been considered by this query and didn't match, so there's
        # nothing before it worth re-asking for. See fix #15.
        max_id = conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM request_logs"
        ).fetchone()["m"]

        counts = _status_counts(conn)

    return JSONResponse({
        **counts,
        "filtered_total": filtered_total,
        "max_id": max_id,
        "truncated": truncated,
        # Non-zero means logging went lossy - either the write queue
        # overflowed or a batch insert failed (fixes #23, #25). Surfaced
        # so a gap in the log is attributable rather than mysterious.
        "dropped_rows": get_dropped_row_count(),
        # error_logged / error_count ride out on each row via SELECT * -
        # the frontend reads them for the per-row failure marker.
        "logs": [dict(r) for r in rows],
    })


async def _http_log_event_generator(request: Request):
    last_id = 0
    with get_db() as conn:
        row = conn.execute("SELECT MAX(id) as m FROM request_logs").fetchone()
        last_id = row["m"] or 0

    while True:
        await asyncio.sleep(1)
        # Without this check a closed tab left this loop querying the
        # database once a second for the life of the container - the
        # generator has no other way to learn its reader is gone, since
        # nothing downstream raises until a write is attempted.
        if await request.is_disconnected():
            break
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM request_logs WHERE id > ? ORDER BY id ASC", (last_id,)
            ).fetchall()
        for r in rows:
            last_id = r["id"]
            yield f"data: {json.dumps(dict(r))}\n\n"


@router.get("/admin/logs/http/stream")
async def stream_http_logs(request: Request, key: str = Query(...)):
    _check_admin(request, key)
    return StreamingResponse(
        _http_log_event_generator(request), media_type="text/event-stream"
    )


# ============================================================
# 2. SYSTEM / APP LOGGING (hooks into Python's `logging` module)
# ============================================================
# Persisted to the same SQLite DB/volume as request_logs, rather than an
# in-memory deque - a deque gets wiped every time the container restarts
# or redeploys, which made older system logs disappear after every push.

class BufferLogHandler(logging.Handler):
    """
    emit() must be cheap and must never block: it runs inline inside
    whatever code called logger.info(), including code holding locks or
    running inside a request. It used to open a connection, insert, and
    commit - a full fsync - on every single log line. Now it formats the
    record and appends to the writer queue.

    It is ALSO where silent-error detection happens (fix #24): any record
    at >= ERROR flips the current request's errored flag via
    mark_request_errored(), before enqueueing the line. That's what makes
    the feature automatic - no route has to opt in, because every path to
    an error ends at a logger.error()/exception() call, and they all pass
    through here.
    """

    def emit(self, record):
        try:
            if record.levelno >= logging.ERROR:
                # Flip the current request/job's error flag. Cheap dict
                # mutation on the shared per-request object; no-op (just
                # installs a throwaway holder) if there's no request in
                # flight, e.g. a startup error.
                mark_request_errored()
            tool, tier = _current_tags()
            _enqueue(
                _SYS,
                (
                    datetime.fromtimestamp(record.created, timezone.utc)
                    .replace(tzinfo=None)
                    .isoformat(),
                    record.levelname,
                    record.name,
                    record.getMessage(),
                    _request_id_ctx.get(),
                    tool,
                    tier,
                ),
            )
        except Exception:
            pass


def attach_system_log_capture():
    handler = BufferLogHandler()
    handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)


@router.get("/admin/logs/system/data")
def get_system_logs(
    request: Request,
    key: str = Query(...),
    limit: int = Query(200, le=2000),
    after_id: int = Query(None, description="If set, return only rows with id > after_id (delta/poll mode), ignoring `limit`."),
    before_id: int = Query(None, description="If set, return one page of OLDER rows with id < before_id, capped at `limit`."),
    request_id: str = Query(None, description="If set, return EVERY system_logs row carrying this request_id, ignoring limit/before_id/after_id entirely."),
    job_id: str = Query(None, description="If set, return every system_logs row mentioning this job id, ignoring pagination. Use when a request produced no logs of its own (e.g. a status poll) but its JOB did."),
    level: str = Query(None, description="Log level filter, e.g. 'ERROR'."),
    q: str = Query(None, description="Global substring search across message and logger; plus request_id and tool for non-numeric terms."),
    tool: str = Query(None, description="Exact tool tag set via set_job_context(), e.g. 'STEMS'."),
    tier: str = Query(None, description="'standard' or 'hq'."),
):
    """
    ROW ORDER: every branch here returns OLDEST-FIRST, reversed
    server-side where the query had to run DESC to take the newest N.
    That differs from get_http_logs(), which returns newest-first and
    leaves the reversal to the client. Both contracts are relied on by
    page.tsx as written - don't "harmonise" one without the other.
    """
    _check_admin(request, key)
    with get_db() as conn:
        if job_id is not None:
            # Job-scoped lookup. Deliberately separate from request_id
            # correlation because they answer different questions:
            #
            #   request_id -> "what did THIS ONE http request log?"
            #   job_id     -> "what did this whole JOB do, start to finish?"
            #
            # The distinction matters because a job's ~40 status-poll
            # GETs each have their own request_id but log NOTHING (the
            # handler is a dict lookup - nothing worth logging every 20
            # seconds per active job). So clicking a status-poll row and
            # correlating by request_id correctly returns an empty list,
            # which reads as broken even though it's accurate. Matching
            # on the job id in the message text surfaces the real story
            # instead: queued -> downloaded -> Demucs started -> complete.
            rows = conn.execute(
                f"SELECT * FROM system_logs WHERE message LIKE ?{_ESC} ORDER BY id ASC",
                (_like(job_id),),
            ).fetchall()
            logs = [dict(r) for r in rows]
            return JSONResponse({"total": len(logs), "filtered_total": len(logs), "logs": logs})

        if request_id is not None:
            # Correlation lookup, not pagination. A request's log lines
            # can span a background task that keeps running well after
            # the HTTP row was written (see RequestLoggerMiddleware's
            # comment on why the contextvar is never reset), so this
            # deliberately ignores every other filter and returns the
            # full set - there is no sane page size for "however many
            # lines one request happened to produce", and capping it
            # would silently hide the exact lines someone clicked through
            # to find. No total needed either; the count IS the result.
            rows = conn.execute(
                "SELECT * FROM system_logs WHERE request_id = ? ORDER BY id ASC",
                (request_id,),
            ).fetchall()
            logs = [dict(r) for r in rows]
            return JSONResponse({"total": len(logs), "filtered_total": len(logs), "logs": logs})

        # Filtering in SQL for the same reason as get_http_logs: doing it
        # in the browser could only ever search the rows already loaded,
        # so searching for an error that scrolled out of the window
        # returned nothing even though it was sitting in the database.
        where: list = []
        params: list = []
        if level:
            where.append("level = ?")
            params.append(level)
        if q:
            # Same widening as get_http_logs' q, scoped to the columns
            # this table has, and with the same numeric-term rule (fix
            # #16). There's no status_code here, so for a numeric term
            # this narrows to message and logger only - which is correct:
            # a digit string matching a hex request id was never what
            # anyone meant. request_id and tool were the notable gaps
            # before this existed at all - pasting a request id from the
            # HTTP tab into this box found nothing, even though
            # correlating on that exact id is the single most common
            # thing anyone wants to do here.
            clause, search_params = _build_search_clause(
                q,
                text_columns=["message", "logger"],
                id_columns=["request_id", "tool"],
            )
            if clause:
                where.append(clause)
                params.extend(search_params)
        if tool:
            where.append("tool = ?")
            params.append(tool)
        if tier:
            where.append("tier = ?")
            params.append(tier)

        def _clause(extra: str = "") -> str:
            parts = list(where)
            if extra:
                parts.append(extra)
            return (" WHERE " + " AND ".join(parts)) if parts else ""

        truncated = False
        if after_id is not None:
            rows = conn.execute(
                f"SELECT * FROM system_logs{_clause('id > ?')} ORDER BY id ASC LIMIT ?",
                (*params, after_id, _DELTA_MAX + 1),
            ).fetchall()
            if len(rows) > _DELTA_MAX:
                rows = rows[:_DELTA_MAX]
                truncated = True
            logs = [dict(r) for r in rows]
        elif before_id is not None:
            rows = conn.execute(
                f"SELECT * FROM system_logs{_clause('id < ?')} ORDER BY id DESC LIMIT ?",
                (*params, before_id, limit),
            ).fetchall()
            logs = [dict(r) for r in rows][::-1]  # oldest -> newest, same shape as default
        else:
            rows = conn.execute(
                f"SELECT * FROM system_logs{_clause()} ORDER BY id DESC LIMIT ?",
                (*params, limit),
            ).fetchall()
            logs = [dict(r) for r in rows][::-1]  # oldest -> newest, for chronological display

        total = _cached_count(conn, "system_logs", "", ())
        filtered_total = _cached_count(conn, "system_logs", _clause(), tuple(params))
        # See get_http_logs for why this is unfiltered and why it
        # matters - it's what lets the client seed a valid delta cursor
        # even when this particular query matched zero rows.
        max_id = conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM system_logs"
        ).fetchone()["m"]

    return JSONResponse({
        "total": total,
        "filtered_total": filtered_total,
        "max_id": max_id,
        "truncated": truncated,
        "logs": logs,
    })


async def _system_log_event_generator(request: Request):
    with get_db() as conn:
        row = conn.execute("SELECT MAX(id) as m FROM system_logs").fetchone()
        last_id = row["m"] or 0

    while True:
        await asyncio.sleep(1)
        if await request.is_disconnected():
            break
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM system_logs WHERE id > ? ORDER BY id ASC", (last_id,)
            ).fetchall()
        for r in rows:
            last_id = r["id"]
            yield f"data: {json.dumps(dict(r))}\n\n"


@router.get("/admin/logs/system/stream")
async def stream_system_logs(request: Request, key: str = Query(...)):
    _check_admin(request, key)
    return StreamingResponse(
        _system_log_event_generator(request), media_type="text/event-stream"
    )


# ============================================================
# 3. LOG CLEANUP
# ============================================================

@router.delete("/admin/logs")
def delete_logs(request: Request, key: str = Query(...), older_than_days: int = Query(None)):
    _check_admin(request, key)
    with get_db() as conn:
        if older_than_days is not None:
            cutoff = (
                datetime.now(timezone.utc).replace(tzinfo=None)
                - timedelta(days=older_than_days)
            ).isoformat()
            # COUNT first rather than trusting cursor.rowcount - see the
            # note in the else branch. Cheap here (indexed on timestamp)
            # and keeps both branches reporting the same way.
            deleted_http = conn.execute(
                "SELECT COUNT(*) AS c FROM request_logs WHERE timestamp < ?", (cutoff,)
            ).fetchone()["c"]
            deleted_system = conn.execute(
                "SELECT COUNT(*) AS c FROM system_logs WHERE timestamp < ?", (cutoff,)
            ).fetchone()["c"]
            conn.execute("DELETE FROM request_logs WHERE timestamp < ?", (cutoff,))
            conn.execute("DELETE FROM system_logs WHERE timestamp < ?", (cutoff,))
        else:
            # `DELETE FROM table` with no WHERE triggers SQLite's truncate
            # optimization, which drops the whole b-tree in one step
            # instead of walking rows - and under it, changes() (which is
            # what cursor.rowcount reports) is not a reliable count of
            # what was removed. "Deleted all 815 logs" could come back as
            # "Deleted 0 logs", which reads as the button being broken.
            # Counting first is correct regardless of which path SQLite
            # takes internally.
            deleted_http = conn.execute(
                "SELECT COUNT(*) AS c FROM request_logs"
            ).fetchone()["c"]
            deleted_system = conn.execute(
                "SELECT COUNT(*) AS c FROM system_logs"
            ).fetchone()["c"]
            conn.execute("DELETE FROM request_logs")
            conn.execute("DELETE FROM system_logs")
        conn.commit()

        # Deleting rows returns their pages to SQLite's freelist, not to
        # the filesystem, and leaves the WAL at whatever size it grew to.
        # Someone pressing "Delete all logs" on a VPS at ~69% disk is
        # trying to reclaim space, so actually reclaim it. Both are
        # best-effort: a checkpoint can be blocked by a concurrent reader
        # and VACUUM needs the file lock, and neither failing is worth
        # turning a successful delete into an error response.
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error:
            pass
        if older_than_days is None:
            try:
                conn.execute("VACUUM")
            except sqlite3.Error:
                pass

    # The cached counters describe a table that no longer looks like
    # that. Without this the dashboard would show pre-delete numbers for
    # up to _COUNTS_TTL seconds after a deletion.
    _invalidate_counts()

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
# with the same {success, client, server, silent} shape.
# ============================================================

@router.get("/admin/logs", response_class=HTMLResponse)
def logs_dashboard(request: Request, key: str = Query(...)):
    _check_admin(request, key)
    # Raw string: the embedded JS contains /\d{2}/ regexes, and in a
    # normal string Python treats "\d" as an unknown escape - it survives
    # at runtime but emits a SyntaxWarning on every import (and becomes a
    # hard error in a future version). No intentional Python escapes in
    # here, so r"""...""" is a straight upgrade.
    html = r"""
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
  .silent-error { color: #f87171; }
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
  .row-silent { background: rgba(248, 113, 113, 0.07); }
  .silent-badge { display: inline-block; margin-left: 6px; font-size: 10px; font-weight: 700; text-transform: uppercase; color: #f87171; border: 1px solid rgba(248,113,113,.4); background: rgba(248,113,113,.12); border-radius: 4px; padding: 0 4px; }
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
      <div class="stat-box" title="Returned <400 but logged an ERROR — a failure the status code hides."><div class="label">Silent Errors</div><div class="value silent-error" id="silentErrors">-</div></div>
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
      <label style="font-size:13px;color:#f87171;">
        <input type="checkbox" id="erroredOnly" onchange="applyFilter()"> Only errored
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
// "-" and turns any increment into NaN forever). Five buckets now - see
// the FIXES APPLIED note at the top of this file for why "failed" became
// separate client/server counts, and #24 for the silent-error bucket.
let totalCount = 0;
let successCount = 0;
let clientErrorCount = 0;
let serverErrorCount = 0;
let silentErrorCount = 0;

function renderCounters() {
  document.getElementById("total").innerText = totalCount;
  document.getElementById("success").innerText = successCount;
  document.getElementById("clientErrors").innerText = clientErrorCount;
  document.getElementById("serverErrors").innerText = serverErrorCount;
  document.getElementById("silentErrors").innerText = silentErrorCount;
}

// Injected from config.NOISE_PATH_MARKERS by logs_dashboard() below -
// see that constant's docstring for why this is no longer a separately
// maintained list. Same source the Client Errors SQL exclusion and the
// Next.js dashboard both read.
const NOISE_PATTERNS = PLACEHOLDER_NOISE_PATTERNS;
function isNoise(path) {
  const lower = path.toLowerCase();
  return NOISE_PATTERNS.some(p => lower.includes(p.toLowerCase()));
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
    hideNoise: document.getElementById("hideNoise").checked,
    erroredOnly: document.getElementById("erroredOnly").checked
  };
  localStorage.setItem("audioforges_log_filters", JSON.stringify(prefs));
}

function loadFilterPrefs() {
  try {
    const saved = JSON.parse(localStorage.getItem("audioforges_log_filters") || "{}");
    if (saved.method) document.getElementById("methodFilter").value = saved.method;
    if (saved.path) document.getElementById("pathFilter").value = saved.path;
    if (saved.hideNoise) document.getElementById("hideNoise").checked = saved.hideNoise;
    if (saved.erroredOnly) document.getElementById("erroredOnly").checked = saved.erroredOnly;
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
  const erroredOnly = document.getElementById("erroredOnly").checked;

  const filtered = allHttpLogs.filter(log => {
    if (methodVal && log.method !== methodVal) return false;
    if (pathVal && !log.path.toLowerCase().includes(pathVal)) return false;
    if (hideNoise && isNoise(log.path)) return false;
    if (erroredOnly && !log.error_logged) return false;
    if (!passesDateFilter(log)) return false;
    return true;
  });

  // Oldest first, newest at the bottom - matches a typical terminal/tail
  // view (and the System Logs panel, which already appends+autoscrolls).
  const tableWrap = document.getElementById("http-table-wrap");
  const wasNearBottom = tableWrap
    ? tableWrap.scrollHeight - tableWrap.scrollTop - tableWrap.clientHeight < 60
    : true;
  document.getElementById("http-rows").innerHTML = filtered.map(l => renderHttpRow(l)).join("");
  document.getElementById("http-empty").style.display = filtered.length === 0 ? "block" : "none";
  if (tableWrap && wasNearBottom) {
    tableWrap.scrollTop = tableWrap.scrollHeight;
  }
}

function resetFilters() {
  document.getElementById("methodFilter").value = "";
  document.getElementById("pathFilter").value = "";
  document.getElementById("hideNoise").checked = false;
  document.getElementById("erroredOnly").checked = false;
  document.getElementById("customDate").value = "";
  setDateFilter("all");
}

function renderHttpRow(log, isNew) {
  const silent = log.error_logged && log.status_code < 400;
  const badge = log.error_logged
    ? `<span class="silent-badge" title="${log.error_count || 1} error(s) logged during this request">failed</span>`
    : "";
  return `<tr class="${isNew ? 'new-row' : ''} ${silent ? 'row-silent' : ''}">
    <td>${toNepalTime(log.timestamp)}</td>
    <td>${log.method}</td>
    <td>${log.path}${badge}</td>
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
    silentErrorCount = data.silent || 0;
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
    if (log.error_logged) silentErrorCount++;
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
    html = html.replace("PLACEHOLDER_NOISE_PATTERNS", json.dumps(list(NOISE_PATH_MARKERS)))
    return HTMLResponse(content=html)