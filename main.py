"""
main.py - App entrypoint. This file should stay tiny: create the app,
wire middleware/lifespan, mount routes. All logic lives elsewhere.
"""
import socket

# ---------- FORCE AF_INET-ONLY DNS RESOLUTION, PROCESS-WIDE ----------
# Placed as the very first thing this file does, before any other import,
# so every module imported afterward (yt_dlp, urllib3, requests, aiohttp,
# etc.) picks up the patched version - some of these libraries hold a
# direct reference to socket.getaddrinfo at import time rather than
# looking it up fresh on every call, so patching late would miss them.
#
# WHY THIS EXISTS: this container has no working IPv6 route (confirmed -
# see IPV6_UNROUTABLE_MARKERS in youtube.py and the VPSDime support
# ticket), even though the VPS HOST now does. yt-dlp's own
# 'source_address': '0.0.0.0' option (the documented --force-ipv4
# equivalent) filters candidates AFTER resolution, on the code path yt-dlp
# controls directly - but some googlevideo.com edges are dual-stack, and
# the underlying urllib3/socket connection logic calls getaddrinfo()
# UNRESTRICTED (family=0), gets back a mixed list of IPv4 and IPv6
# candidates, and can end up trying to bind our IPv4 source_address
# against an IPv6 candidate in that mixed set. That mismatch is exactly
# what throws "Address family for hostname not supported" - confirmed via
# `docker exec ... python3 -c "socket.getaddrinfo(host, 443, AF_INET)"`
# returning a perfectly valid IPv4 address instantly, proving the address
# exists and DNS is fine; the bug is in which candidates get PAIRED with
# the IPv4 bind, not in resolution itself.
#
# Restricting resolution to AF_INET at the SOURCE - before any library
# sees a mixed result - closes this for every connection this process
# ever makes, not just the two yt-dlp call sites that separately still
# also set source_address as defense in depth (now largely redundant
# with this in place, but harmless to leave).
#
# SCOPE: this is process-wide and affects every outbound connection this
# API makes, not just yt-dlp - acceptable here since this container has
# no usable IPv6 path at all right now (see docstring above), so nothing
# is lost by refusing to attempt it anywhere in this process.
#
# REMOVE THIS when Docker's IPv6 networking is properly configured
# (tracked as a separate future task - see the Incus/LXD nested-container
# networking notes from that investigation before attempting it, it's
# genuinely fragile in this hosting setup and not a quick fix).
_orig_getaddrinfo = socket.getaddrinfo


def _getaddrinfo_ipv4_only(host, port, family=0, type=0, proto=0, flags=0):
    if family in (0, socket.AF_UNSPEC, socket.AF_INET6):
        family = socket.AF_INET
    return _orig_getaddrinfo(host, port, family, type, proto, flags)


socket.getaddrinfo = _getaddrinfo_ipv4_only
# ---------- END AF_INET-ONLY PATCH ----------

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from config import (
    logger,
    ALLOWED_ORIGINS,
)
from utils import ensure_cookies_file
from routes import router
from jobs import cleanup_expired_jobs, get_job_stats
from log_stream import RequestLoggerMiddleware, router as logs_router, attach_system_log_capture
from cookie_upload import router as cookie_upload_router
from gpu_internal_routes import router as gpu_internal_router


# How often the background sweep runs. Jobs are removed based on their
# own ttl_seconds, so this only controls how promptly an already-expired
# job's files are reclaimed - not how long they live. A minute is
# frequent enough that disk never drifts far, and cheap enough to be
# invisible: the sweep is a dict scan plus however many os.remove() calls
# the expired jobs earned.
JOB_CLEANUP_INTERVAL_SECONDS = int(os.environ.get("JOB_CLEANUP_INTERVAL_SECONDS", "60"))


async def _job_cleanup_loop():
    """
    Periodic TTL sweep for the job table.

    WHY THIS EXISTS AS A BACKGROUND TASK: cleanup_expired_jobs() used to
    be called at the top of ~20 route handlers ("opportunistic sweep"),
    which had three problems on a single-worker deployment:

      1. It ran blocking os.remove() calls directly on the event loop.
         Expiring one stems job means deleting four full-length WAVs, so
         a request that happened to trigger the sweep stalled every other
         connection - including the cheap status polls the frontend needs
         to stay responsive - for the duration.
      2. The cost was paid by whoever happened to submit next, making
         one unlucky user's request arbitrarily slower than the rest for
         reasons entirely unrelated to their own file.
      3. It only ran when traffic arrived. An idle server never swept, so
         files sat on a 30GB disk until the next visitor showed up.

    Running it here fixes all three: off the request path, on a fixed
    schedule, and dispatched to a worker thread so even the deletion
    itself never blocks the loop.
    """
    while True:
        try:
            await asyncio.sleep(JOB_CLEANUP_INTERVAL_SECONDS)
            # run_in_executor, not a direct call: cleanup does real
            # blocking disk I/O, and the whole point of moving it here
            # was to keep that off the event loop.
            await asyncio.get_running_loop().run_in_executor(None, cleanup_expired_jobs)
        except asyncio.CancelledError:
            # Normal shutdown - re-raise so the task actually stops
            # rather than looping forever through a swallowed cancel.
            raise
        except Exception as e:
            # A failed sweep must never kill the loop; the next tick
            # retries and the log line makes a persistent failure
            # (e.g. a permissions problem on the output dir) visible
            # instead of silently letting disk fill up.
            logger.error(f"[JOBS] Cleanup sweep failed: {e}", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    attach_system_log_capture()  # starts capturing all logger.info()/error() calls app-wide
    ensure_cookies_file()
    logger.info(f"[CORS] Allowed origins: {ALLOWED_ORIGINS}")

    # One sweep immediately at boot: a redeploy recreates the container
    # while output files persist on the mounted volume, so anything
    # already past its TTL should go now rather than after the first
    # interval.
    try:
        cleanup_expired_jobs()
    except Exception as e:
        logger.error(f"[JOBS] Startup cleanup failed: {e}", exc_info=True)

    cleanup_task = asyncio.create_task(_job_cleanup_loop())
    logger.info(
        f"[JOBS] Background cleanup running every {JOB_CLEANUP_INTERVAL_SECONDS}s"
    )

    yield

    # Shutdown - stop the sweep and wait for it to actually unwind, so a
    # deletion in progress isn't torn down mid-write.
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass

    stats = get_job_stats()
    logger.info(f"[JOBS] Shutting down with jobs in table: {stats}")


# ============================================================
# INTERACTIVE DOCS DISABLED (2026-08-17)
#
# docs_url / redoc_url / openapi_url are all None, so this app serves no
# Swagger UI, no ReDoc, and no machine-readable schema at any path.
#
# WHY: /openapi.json is a complete, structured map of every route this
# service exposes - paths, methods, and the exact shape of every request
# body - published to anyone who asks. That is precisely the reconnaissance
# step that precedes targeted probing, and it costs an attacker one GET.
# There is a Cloudflare rule (block-recon-paths) blocking /docs, /redoc
# and /openapi.json at the edge, but a WAF rule is a config entry that can
# be deleted, reordered, or bypassed if the origin is ever reachable
# directly; not generating the schema at all removes the thing being
# protected instead of guarding it. Defense in depth, cheapest layer first.
#
# NOTHING ELSE CHANGES. These three parameters only control the docs
# endpoints - route registration, validation, and every response body are
# completely unaffected. The frontend never touched /docs.
#
# TO READ THE SCHEMA DURING DEVELOPMENT: temporarily set openapi_url back
# to "/openapi.json" locally, or introspect the live app without exposing
# anything:
#
#   docker exec audioforges-api python3 -c \
#     "from main import app; import json; print(json.dumps(app.openapi()))"
#
# The /admin/endpoints route (auth-gated) also already lists every
# registered route, which covers most of what /docs was actually used for.
# ============================================================
app = FastAPI(
    title="Audio Analysis API - ESSENTIA FIXED",
    version="12.7.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

# Logs every HTTP request (timestamp, method, path, status, duration, IP) to SQLite
app.add_middleware(RequestLoggerMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# VALIDATION ERROR LOGGING (added 2026-08-16)
#
# WHY THIS EXISTS: FastAPI's validation layer runs BEFORE the route
# handler. Every logger.info() call in this codebase lives inside a
# handler, so a request rejected at validation produces a row in
# request_logs and ZERO system log lines - which is exactly what the
# admin dashboard showed for "POST /convert -> 422 - 0 lines". The
# status code said "malformed request" and nothing anywhere said WHICH
# FIELD, or whether the caller was a real user or a scanner.
#
# That ambiguity is the whole problem. A scanner probing /convert with
# an empty body and a genuine frontend/backend contract mismatch produce
# an identical log signature. One is noise to ignore forever; the other
# is a bug silently breaking a tool for real users. Without the field
# names there is no way to tell them apart, so either you investigate
# every scanner hit or you learn to ignore the category entirely -
# and the second is what actually happens.
#
# Confirmed before writing this: no route in this app raises
# HTTPException(422) on the /convert path. _shared.py uses 400/404/409/
# 500/503, save_upload uses 413. So a 422 here can ONLY be a
# RequestValidationError, i.e. a genuinely malformed request body.
# ============================================================
@app.exception_handler(RequestValidationError)
async def log_validation_errors(request: Request, exc: RequestValidationError):
    """
    Logs WHY a request was rejected with 422, then returns FastAPI's own
    default response shape completely unchanged.

    FIELD NAMES ONLY, NEVER VALUES. A rejected request's form fields can
    carry anything the caller chose to send, and log_stream.py writes
    these lines into a SQLite table that renders in the admin dashboard.
    The field NAME is what identifies a contract mismatch; the value adds
    nothing diagnostic while creating a path for arbitrary
    caller-controlled data to land in the log store and be displayed
    back. Not worth it for zero extra signal.

    WARNING, not ERROR, deliberately. A 4xx is this server correctly
    refusing bad input - not a server fault. Logging routine scanner
    traffic at ERROR would put it in the same bucket as real failures,
    and the predictable result is that the bucket stops being read.

    The response body is byte-for-byte FastAPI's default. Anything
    already parsing 422s - including humanizeError() in the frontend's
    JobToolForm - keeps working exactly as before. This is purely
    additive observability, with no behaviour change on any code path.
    """
    try:
        problems = []
        for err in exc.errors():
            # loc looks like ("body", "target_format"). The leading
            # "body"/"query" bucket is noise once the path is known, so
            # it's dropped - what's left is the field name that matters.
            location = ".".join(str(p) for p in err.get("loc", ()) if p != "body")
            problems.append(f"{location or '?'}: {err.get('msg', 'invalid')}")
        logger.warning(
            f"[VALIDATION] {request.method} {request.url.path} rejected (422) - "
            f"{'; '.join(problems) or 'no detail'}"
        )
    except Exception as e:
        # A logging helper must never be the reason a request fails. If
        # pydantic ever changes the shape of errors(), swallow it and
        # still return the correct 422 below rather than turning a clean
        # client error into a 500.
        logger.warning(f"[VALIDATION] Could not summarise validation error: {e}")

    return JSONResponse(status_code=422, content={"detail": exc.errors()})


# ============================================================
# HTTP EXCEPTION LOGGING (added 2026-08-16, same day as the 422 fix)
#
# WHY THIS EXISTS: the 422 handler above only covers FastAPI's own
# RequestValidationError - it never fires for a manually raised
# HTTPException(400)/(404)/(409)/(413)/(503), which is most of this
# app's actual rejections (see midi.py's onset_threshold check,
# _shared.py's 400/404/409/500/503, save_upload's 413). Those all hit
# the same blind spot the 422s used to: a row in request_logs, zero
# lines in system_logs, and the only place the reason ever existed was
# the HTTP response body sent back to the caller - never visible from
# the dashboard.
#
# This closes that gap the same way: log what actually happened, then
# return the exact response Starlette's own default handler would have
# produced, so no existing caller (frontend included) sees any
# behaviour change.
#
# WHY NOT JUST `raise exc`: FastAPI does not re-run its own default
# exception handling on a re-raised exception from inside a registered
# handler - doing that turns a clean 400 into an unhandled 500. The
# response has to be built here explicitly, mirroring what Starlette's
# default HTTPException handler already returns.
#
# WHY 404 AND 429 ARE EXCLUDED: this VPS gets scanned constantly (see
# NOISE_PATH_MARKERS in config.py - the same list the dashboard's "Hide
# noise" filter uses). Every one of those hits is a 404. Logging all of
# them at WARNING would flood system_logs with entries that carry zero
# diagnostic value and bury the rejections that are actually worth
# reading. 429 (rate limit) is excluded for the same reason from a
# different cause - it's the rate limiter working exactly as designed,
# not a signal of anything wrong.
#
# WHY < 500 ONLY: a 5xx raised as an HTTPException would mean something
# on this server's own side broke, which is a different class of
# problem than "this server correctly rejected bad input" - that
# belongs at ERROR with a traceback, not folded into this WARNING-level
# rejection log. Nothing in this codebase currently raises
# HTTPException with a 5xx status, so this is a guard for the future,
# not a case seen today.
# ============================================================
@app.exception_handler(StarletteHTTPException)
async def log_http_exceptions(request: Request, exc: StarletteHTTPException):
    """
    Logs manually-raised HTTPExceptions - 400/404/409/413/503 today,
    whatever routes.py or midi.py raise next tomorrow - the same way
    log_validation_errors logs 422s. Rebuilds Starlette's own default
    response shape exactly, so this is purely additive observability
    with no change to what any caller receives.
    """
    if exc.status_code < 500 and exc.status_code not in (404, 429):
        try:
            logger.warning(
                f"[HTTP_EXCEPTION] {request.method} {request.url.path} "
                f"rejected ({exc.status_code}) - {exc.detail}"
            )
        except Exception as e:
            # Same rule as the 422 handler above: a logging failure must
            # never be the reason a request fails.
            logger.warning(f"[HTTP_EXCEPTION] Could not log exception detail: {e}")

    headers = getattr(exc, "headers", None)
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=headers,
    )


app.include_router(router)
app.include_router(logs_router)  # /admin/logs live dashboard (HTTP + system logs)
app.include_router(cookie_upload_router)  # /admin/upload-cookies - upload cookies.txt directly, no base64
app.include_router(gpu_internal_router)  # /internal/gpu/* - GPU worker file transfer, shared-secret auth