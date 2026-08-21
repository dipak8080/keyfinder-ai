"""
routes/admin.py - operator tooling and public metadata: /admin/*,
/limits, /health, and / (root).

Split out of the old monolithic routes.py (2026-08-14 restructure). Pure
move: every docstring, comment, and line of logic here is unchanged from
its original location, with exactly ONE deliberate wiring change - see
the "DELIBERATE WRINKLE" note below on admin_endpoints(). Everything else
is unchanged behaviour.

--------------------------------------------------------------------------
WHAT CHANGED (2026-08-15): SPLIT-TUNNEL OBSERVABILITY + AN EVENT-LOOP FIX

1. /admin/status now reports a "split_tunnel" block (see
   split_breaker_status() in youtube.py). Without it the split tunnel's
   three new path counters - proxy_extract, direct_media, proxy_media -
   arrive in "paths" with no context: no way to tell whether the feature
   is even enabled, whether a sticky session is pinned, or whether the
   health breaker has silently reverted to full-proxy downloads. That
   last case is the dangerous one, because a silent revert is
   indistinguishable from "the savings just stopped" unless something
   states it explicitly.

2. POST /admin/reset-split-breaker, mirroring the existing
   /admin/reset-proxy-botcheck and /admin/reset-cdn-breaker.

3. BUG FIX, unrelated to the above and older than it: the midi-worker
   health probe called requests.get(..., timeout=3) DIRECTLY inside this
   async handler. requests is synchronous, so that call blocked the
   event loop - the entire server, every concurrent connection - for up
   to 3 seconds whenever the sidecar was slow or unreachable. This is
   precisely the failure class the codebase already guards against
   everywhere else via utils.run_blocking() (see its docstring, and the
   "must always be called via run_blocking" warnings throughout
   youtube.py and audio_analysis.py). The probe was the one place it was
   missed - and it is a probe that runs exactly when the sidecar is
   already unhealthy, i.e. exactly when the server can least afford to
   stall. Now dispatched through run_blocking like every other blocking
   call in the app.
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-21): PER-TOOL YOUTUBE CHAIN LIMITS IN /limits

The two YOUTUBE_CHAIN_RATE_LIMIT_MAX_REQUESTS / _HQ_ constants this file
imported no longer exist - config.py now defines one pair per tool
(YOUTUBE_ANALYZE_*, YOUTUBE_SEPARATE_*, YOUTUBE_STEMS_* and the two HQ
variants). See that block's comment for why: the three chained tools
already had independent per-IP buckets (rate_limit.py keys on
(ip, path)), but were forced to share a single NUMBER despite
/youtube/analyze costing ~30s on a 4-slot semaphore while
/youtube/separate and /youtube/stems each hold the single separation
slot for 3-5 minutes.

/limits gains one key per tool as a result. `youtube_chain` and
`youtube_chain_hq` are KEPT in the response, both pointing at the
separation numbers, because this endpoint is a public contract that the
frontend reads - removing a key it might still be indexing would turn a
config change into a runtime undefined. They are marked for removal
below once lib/data/rate-limits.ts is confirmed to be off them.
--------------------------------------------------------------------------
"""
import requests
from fastapi import APIRouter, HTTPException, Query, Request

from config import (
    logger,
    NOISE_PATH_MARKERS,
    MAX_UPLOAD_BYTES,
    MAX_VIDEO_UPLOAD_BYTES,
    JOIN_MAX_FILES,
    JOIN_MAX_TOTAL_BYTES,
    ALLOWED_AUDIO_INPUT_FORMATS,
    SEPARATION_RATE_LIMIT_MAX_REQUESTS,
    SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
    STEMS_RATE_LIMIT_MAX_REQUESTS,
    STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_ANALYZE_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_SEPARATE_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_SEPARATE_HQ_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_STEMS_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
    SEPARATION_RATE_LIMIT_WINDOW_SECONDS,
    SEPARATION_HQ_ENABLED,
    MAX_QUEUED_SEPARATIONS,
    MAX_CONCURRENT_SEPARATIONS,
    MIDI_WORKER_URL,
)
from utils import run_blocking
from youtube import (
    proxy_available,
    reset_proxy_circuit_breaker,
    cdn_breaker_status,
    reset_cdn_breaker,
    proxy_botcheck_degraded,
    reset_proxy_botcheck_breaker,
    get_account_health,
    get_path_stats,
    get_cookie_accounts,
    split_breaker_status,
    reset_split_breaker,
)
from cache import clear_cache, set_cache_max_gb, get_cache_stats
from monitoring import get_status_snapshot
from jobs import get_job_stats
from admin_auth import guard_admin_request, verify_admin_key
from log_stream import get_endpoint_counts, get_tool_counts

router = APIRouter()


@router.post("/admin/clear-cache")
async def admin_clear_cache(request: Request, key: str = Query(...)):
    client_ip = guard_admin_request(request)
    verify_admin_key(key, client_ip)
    result = clear_cache()
    logger.info(f"[CACHE] Admin manually cleared cache: {result}")
    return {"status": "cache cleared", **result}


@router.post("/admin/cache/limit")
async def admin_set_cache_limit(request: Request, key: str = Query(...), gb: float = Query(..., gt=0, le=1000)):
    client_ip = guard_admin_request(request)
    verify_admin_key(key, client_ip)
    try:
        stats = set_cache_max_gb(gb)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "updated", **stats}


@router.post("/admin/reset-proxy")
async def admin_reset_proxy(request: Request, key: str = Query(...)):
    client_ip = guard_admin_request(request)
    verify_admin_key(key, client_ip)
    reset_proxy_circuit_breaker()
    return {"status": "proxy circuit breaker reset"}


@router.post("/admin/reset-cdn-breaker")
async def admin_reset_cdn_breaker(request: Request, key: str = Query(...)):
    """
    Forces the direct path back to healthy immediately instead of waiting
    out CDN_DEGRADED_COOLDOWN_SECONDS. Useful when you know the network
    situation changed (proxy topped up, host routing fixed, edges came
    back) and don't want to keep paying for proxy traffic in the meantime.
    """
    client_ip = guard_admin_request(request)
    verify_admin_key(key, client_ip)
    reset_cdn_breaker()
    return {"status": "CDN degradation breaker reset - direct path re-enabled"}


def _iter_tool_routes():
    """
    Yields every route object registered across the routes/ package.

    WHY THIS EXISTS RATHER THAN `from routes import router; router.routes`
    - and this is a real bug that was caught in verification before the
    restructure shipped, not a hypothetical:

    In the old monolithic routes.py there was ONE flat APIRouter, and
    every route reached it directly through an @router.get/@router.post
    decorator. Iterating `router.routes` therefore always yielded real
    APIRoute objects with usable .path and .methods attributes.

    routes/__init__.py builds its combined router with include_router()
    instead. How include_router() stores what it was given is a FastAPI
    IMPLEMENTATION DETAIL that has changed across versions: some
    versions copy the child's APIRoute objects onto the parent eagerly
    at include time (so `.routes` looks flat), while newer ones append
    a lazy internal wrapper object per include and only flatten later.
    On a lazy version, `parent.routes` yields ~7 wrapper objects with NO
    .path and NO .methods - so `getattr(route, "path", None)` returns
    None for every one of them, every route gets skipped by the guard in
    admin_endpoints(), `families` comes out empty, and /admin/endpoints
    returns an empty tool list. No exception, no error log, no failed
    healthcheck - the admin dashboard's tool picker would simply be
    blank, and CI/CD would have deployed it happily.

    Walking the sub-routers directly sidesteps the question entirely.
    Each of the seven modules below owns a plain APIRouter whose routes
    were all registered by decorator, exactly like the old monolith's
    single router - so `.routes` on each one is guaranteed to be real
    APIRoute objects on every FastAPI version, past or future.

    admin.py's own router is included too: admin_endpoints() filters
    /admin/* out by path anyway (see its docstring), but leaving it in
    keeps this function honestly "every route in the package" rather
    than quietly depending on that filter to hide an omission.
    """
    # Imported here, not at module level - see admin_endpoints()'s
    # docstring for the import-order reasoning.
    from . import youtube, separation, audio_tools, midi, transcribe, media

    sub_routers = [
        youtube.router,
        separation.router,
        audio_tools.router,
        midi.router,
        transcribe.router,
        media.router,
        router,  # this module's own
    ]

    for sub in sub_routers:
        for route in getattr(sub, "routes", []):
            yield route


@router.get("/admin/endpoints")
async def admin_endpoints(request: Request, key: str = Query(...)):
    """
    Returns the list of TOOLS this API exposes - one entry per tool, with
    a human-readable label - read from FastAPI's own route table rather
    than a hand-maintained list that goes stale every time a tool is
    added.

    Collapsing is the whole point. Every tool registers four routes:

        POST /convert
        GET  /convert/status/{job_id}
        GET  /convert/preview/{job_id}
        GET  /convert/download/{job_id}

    Returning those raw gives ~100 entries for ~25 tools, and a filter
    dropdown that long is worse than no dropdown at all. The sub-routes
    are implementation detail: nobody wants to filter logs by "preview"
    specifically, they want to see everything /convert did. So the
    trailing action segment and its id are stripped, leaving one clean
    "/convert" family. Method (GET/POST/DELETE) is already its own filter
    in the dashboard, so families deliberately do not fork by method
    either.

    /admin/* is excluded: operator tooling, not a product tool, and
    listing it would put this very endpoint in the same picker as
    /convert and /pitch.

    Returns only static route metadata - identical for every caller,
    every request. No job ids, no IPs, nothing per-request.

    NOTE: this is still the PATH/family picker, unrelated to the new
    tool/tier columns. A parallel "which tool/tier tags actually exist in
    the data" endpoint - for populating a tool/tier dropdown the same way
    this populates the family dropdown - is a natural follow-up once the
    frontend is ready to consume it, not part of this change.

    --------------------------------------------------------------------
    DELIBERATE WRINKLE (2026-08-14 routes/ package restructure):

    This function needs to walk EVERY route in the app, not just the
    handful registered on this module's own `router` object above.
    Before the restructure, routes.py had exactly one flat router for
    the whole app, so `for route in router.routes` already meant "every
    route." Now that routes are split across seven files each with their
    own local `router = APIRouter()`, this module's own `router` only
    carries admin.py's own routes.

    It walks the SEVEN SUB-ROUTERS directly (see _iter_tool_routes
    below) rather than the assembled router from routes/__init__.py.
    That is deliberate and load-bearing, not a stylistic choice - see
    _iter_tool_routes' docstring for the FastAPI-version behaviour that
    forces it. The short version: a router built with include_router()
    does not reliably expose flattened APIRoute objects on `.routes`
    across FastAPI versions, whereas a router whose routes were
    registered directly by @router.get/@router.post decorators always
    does. Every sub-router here is the latter.

    The imports are inside the function body rather than at module
    level, because a module-level `from . import youtube, separation...`
    would run while routes/__init__.py is still midway through importing
    THIS file - import order would decide whether it worked, which is
    exactly the kind of fragility worth avoiding. By the time any
    request reaches this handler, every sibling module is long since
    fully imported.
    --------------------------------------------------------------------
    """
    client_ip = guard_admin_request(request)
    verify_admin_key(key, client_ip)

    # Trailing segments that mark an ACTION on a job rather than a
    # distinct tool. Anything at/after one of these is stripped.
    action_segments = {"status", "preview", "download", "result"}

    families: dict = {}
    for route in _iter_tool_routes():
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", None)
        if not path or not methods:
            continue
        if path.startswith("/admin") or path in ("/", "/health", "/limits"):
            continue

        segments = [s for s in path.split("/") if s]
        # Walk from the left, stopping at the first action segment or
        # path parameter - what remains is the tool itself. Left-to-right
        # (not trimming from the right) keeps namespaced tools intact:
        # /youtube/analyze/result/{job_id} correctly yields
        # /youtube/analyze, not /youtube.
        #
        # The i > 0 guard matters and was missing initially: "/download"
        # is itself a real tool (the YouTube downloader, the busiest
        # endpoint on this API), but "download" is ALSO an action segment
        # for every job tool's /<tool>/download/{job_id} route. Without
        # this guard the loop broke on segment zero, family_parts came
        # out empty, and the route was skipped entirely - so /download
        # never appeared in the dashboard's tool picker and all of its
        # traffic silently fell into the "Other" bucket. An action word
        # only ends a family when something already precedes it.
        family_parts = []
        for i, seg in enumerate(segments):
            if i > 0 and (seg in action_segments or seg.startswith("{")):
                break
            if seg.startswith("{"):
                break  # a parameter as the FIRST segment is never a tool
            family_parts.append(seg)

        if not family_parts:
            continue

        family = "/" + "/".join(family_parts)
        entry = families.setdefault(
            family,
            {"path": family, "label": _humanize_endpoint(family_parts), "methods": set()},
        )
        for method in methods:
            if method != "HEAD":  # implied by GET, not a distinct action
                entry["methods"].add(method)

    # Real totals from the database, not from whatever the browser
    # happens to have loaded. The picker previously counted rows in the
    # client's in-memory window, so its numbers visibly shrank as older
    # rows were trimmed - "/download 967" becoming "/download 233" looked
    # like requests had vanished. Cached server-side (see
    # get_endpoint_counts), so this adds no per-request query cost.
    counts = get_endpoint_counts()

    endpoints = [
        {
            "path": f["path"],
            "label": f["label"],
            "methods": sorted(f["methods"]),
            "total_requests": counts.get(f["path"], 0),
        }
        for f in families.values()
    ]
    endpoints.sort(key=lambda e: e["label"].lower())

    # Tool/tier tags - a SEPARATE list from `endpoints` above. `endpoints`
    # describes URL shapes (path families); `tools` describes what
    # set_job_context() actually tagged requests as, which is what the
    # frontend's Tool/Tier filter dropdown needs (see page.tsx's
    # TOOL_OPTIONS-turned-toolOptions and log_stream.get_tool_counts()'s
    # docstring for the full "why these are different axes" reasoning).
    # This is what makes that dropdown dynamic: no hardcoded list on
    # either side, just whatever tags genuinely exist in the data right
    # now, with real counts.
    tool_counts = get_tool_counts()
    tools = [
        {
            "tool": tag,
            "label": _humanize_tool(tag),
            "standard_count": counts["standard"],
            "hq_count": counts["hq"],
            "total": counts["total"],
        }
        for tag, counts in tool_counts.items()
    ]
    tools.sort(key=lambda t: t["label"].lower())

    # Served alongside the tool list so the Next.js dashboard's "Hide
    # noise" checkbox reads the exact same definition the backend uses
    # to exclude noise from the Client Errors count - see
    # config.NOISE_PATH_MARKERS for why these two were previously
    # separate, silently-drifted copies.
    return {
        "endpoints": endpoints,
        "tools": tools,
        "noise_patterns": list(NOISE_PATH_MARKERS),
    }


def _humanize_tool(tag: str) -> str:
    """
    "SPEECH_TO_TEXT" -> "Speech To Text", "YOUTUBE_STEMS" -> "YouTube
    Stems", "STEMS" -> "Stems".

    Mirrors _humanize_endpoint() below almost exactly - same special-case
    map, so a tool's label reads consistently with a path's label even
    though tool tags are UNDERSCORE-separated (the strings routes.py
    passes to set_job_context()) where paths are hyphen-separated. Kept
    as a separate function rather than reusing _humanize_endpoint()
    directly because that one expects a list of path segments, not a
    single underscore-joined tag - forcing one shape into the other would
    be more confusing than two small functions with the same spirit.
    """
    special = {"hq": "HQ", "youtube": "YouTube", "url": "URL", "api": "API"}
    words = []
    for part in tag.split("_"):
        if not part:
            continue
        words.append(special.get(part.lower(), part.capitalize()))
    return " ".join(words)


def _humanize_endpoint(segments: list) -> str:
    """
    "/youtube/analyze" -> "YouTube Analyze", "/speech-to-text" ->
    "Speech To Text", "/stems-hq" -> "Stems HQ".

    Exists so the dashboard picker reads like a list of tools rather
    than a list of URLs. Kept here rather than in the frontend because
    the backend is the thing that actually knows what these routes are,
    and duplicating the mapping client-side is exactly the kind of
    quietly-drifting second source of truth this endpoint exists to
    avoid.
    """
    special = {"hq": "HQ", "youtube": "YouTube", "url": "URL", "api": "API"}
    words = []
    for seg in segments:
        for part in seg.replace("_", "-").split("-"):
            if not part:
                continue
            words.append(special.get(part.lower(), part.capitalize()))
    return " ".join(words)


@router.post("/admin/reset-proxy-botcheck")
async def admin_reset_proxy_botcheck(request: Request, key: str = Query(...)):
    """
    Clears the proxy bot-check breaker immediately instead of waiting out
    PROXY_BOTCHECK_COOLDOWN_SECONDS. Use after changing YT_PROXY_URL
    (e.g. pinning a sticky session or a fixed exit country) so the new
    configuration gets tried right away rather than sitting behind a
    cooldown earned by the old one.
    """
    client_ip = guard_admin_request(request)
    verify_admin_key(key, client_ip)
    reset_proxy_botcheck_breaker()
    return {"status": "proxy bot-check breaker reset - escalation re-enabled"}


@router.post("/admin/reset-split-breaker")
async def admin_reset_split_breaker(request: Request, key: str = Query(...)):
    """
    Clears the split-tunnel health breaker immediately instead of waiting
    out SPLIT_TUNNEL_COOLDOWN_SECONDS.

    Use after changing whatever the breaker was reacting to - pinning a
    sticky session in YT_PROXY_URL, switching proxy provider, raising
    YT_MEDIA_SOCKET_TIMEOUT - so the new setup gets measured on its own
    merits rather than serving out a cooldown earned by the old one.
    Exactly the same purpose and shape as /admin/reset-proxy-botcheck
    above; the breaker it resets just happens to govern bandwidth spend
    rather than escalation.
    """
    client_ip = guard_admin_request(request)
    verify_admin_key(key, client_ip)
    reset_split_breaker()
    return {"status": "split-tunnel health breaker reset - direct media re-enabled"}


def _probe_midi_worker() -> dict:
    """
    Blocking midi-worker health probe, dispatched via run_blocking from
    admin_status().

    MUST NOT be called directly from async code. `requests` is fully
    synchronous, so calling it inline in an async handler blocks the
    event loop - and therefore every other in-flight connection - for
    the full timeout. That is not theoretical here: this probe fires
    precisely when the sidecar is slow or down, which is exactly the
    moment the rest of the server can least afford a 3-second stall.
    Same threading rule as every other blocking call in this codebase
    (see utils.run_blocking's docstring).

    Errors are swallowed and returned as data rather than raised: a
    health probe must never be able to take down the status endpoint
    that reports on everything else.
    """
    try:
        health = requests.get(f"{MIDI_WORKER_URL}/health", timeout=3).json()
        return {"reachable": True, **health}
    except Exception as e:
        return {"reachable": False, "error": str(e)[:200]}


@router.get("/admin/status")
async def admin_status(request: Request, key: str = Query(...)):
    client_ip = guard_admin_request(request)
    verify_admin_key(key, client_ip)
    snapshot = get_status_snapshot()
    snapshot["proxy"] = {
        "circuit_breaker": "OPEN (proxy disabled)" if not proxy_available() else "CLOSED (proxy available)",
        # Separate from the quota breaker above: this one means the proxy
        # works but YouTube is challenging its exits, so escalations are
        # being skipped to avoid paying for known-bad requests.
        "botcheck_breaker": "ACTIVE (escalation paused)" if proxy_botcheck_degraded() else "clear",
    }
    # Direct-path health. When this shows DEGRADED, YouTube is routing
    # this server's IP to unreachable googlevideo edges and downloads are
    # being sent straight to the proxy - which means proxy spend is
    # temporarily higher, and is the number to watch if the bill moves.
    #
    # As of 2026-08-15 this breaker is fed ONLY by genuine connect
    # timeouts. It used to be fed by read timeouts too, which meant a
    # merely SLOW transfer counted as an unreachable edge - three of them
    # in five minutes forced every subsequent download onto the proxy,
    # so slow transfers were manufacturing proxy spend. See
    # is_cdn_read_timeout_error() in youtube.py for the full writeup.
    snapshot["cdn"] = cdn_breaker_status()
    # Per-path success rates. Answers "is the proxy actually working?"
    # and "did that proxy config change help?" - both previously
    # unanswerable without reading raw logs.
    #
    # With YT_SPLIT_TUNNEL=1 this grows three buckets beyond
    # direct/proxy: proxy_extract (phase 1 through the proxy),
    # direct_media (phase 2 direct - the bytes no longer being paid for),
    # and proxy_media (phase 2 fallback). direct_media.success_rate is
    # the single number that says whether the split tunnel is working.
    snapshot["paths"] = get_path_stats()
    # The context those raw counters can't carry: whether the feature is
    # enabled at all, whether a sticky session is pinned (without one the
    # media fallback can land on a different exit IP and 403 on a signed
    # URL), and whether the health breaker has reverted to full-proxy
    # downloads. That last one is why this block exists - a silent revert
    # is otherwise indistinguishable from "the savings stopped".
    snapshot["split_tunnel"] = split_breaker_status()
    snapshot["cookies"] = {
        "accounts_available": len(get_cookie_accounts()),
        # Per-account detail, including WHICH phase each account last
        # failed in. A media-phase failure means extraction succeeded,
        # which means the cookie was ACCEPTED - so a high failure count
        # with last_failure_phase="media" is a network problem, not a
        # cookie problem. That distinction is the whole reason this
        # exists; without it a healthy account looks identical to a dead
        # one.
        "accounts": get_account_health(),
    }
    # GPU spend is no longer tracked as a separate in-app counter - the
    # ceiling is RunPod's own account balance, checked directly at
    # https://runpod.io (Billing), not estimated here. See
    # separation.py's insufficient-balance handling for what happens
    # when that balance actually runs out mid-request.
    # Sidecar health. Without this, a dead midi-worker is invisible from
    # the admin panel and only shows up as a run of failed jobs with a
    # generic message. Short timeout, fully swallowed errors - a health
    # probe must never be able to take down the status endpoint itself.
    # Dispatched via run_blocking: see _probe_midi_worker's docstring for
    # why calling requests.get() inline here was freezing the event loop.
    snapshot["midi_worker"] = await run_blocking(_probe_midi_worker)

    snapshot["cache"] = {
        "enabled": True,
        "backend": "local-disk",
        **get_cache_stats(),
    }
    # Job-table state, including the separation queue depth the bounded
    # queue keys on. Worth having here rather than only in the logs: when
    # someone reports "it's stuck", this answers whether anything is
    # actually running, and how long the oldest in-flight job has been
    # going.
    snapshot["jobs"] = {
        **get_job_stats(),
        "separation_queue_limit": MAX_QUEUED_SEPARATIONS,
        "separation_concurrency": MAX_CONCURRENT_SEPARATIONS,
    }
    return snapshot


@router.get("/limits")
async def limits():
    """
    The single source of truth for every limit the frontend needs to
    enforce or display.

    Before this existed, the same numbers were hardcoded in ~20 page
    files, in the client-side validator, AND in config.py - and they
    drifted, which is how a 50MB per-file check ended up silently
    blocking uploads on a tool whose UI advertised a 150MB total. The
    frontend should read these at build time and render from them
    instead of repeating them.
    """
    return {
        "max_upload_bytes": MAX_UPLOAD_BYTES,
        "max_upload_mb": MAX_UPLOAD_BYTES // (1024 * 1024),
        "max_video_upload_bytes": MAX_VIDEO_UPLOAD_BYTES,
        "max_video_upload_mb": MAX_VIDEO_UPLOAD_BYTES // (1024 * 1024),
        "join": {
            "max_files": JOIN_MAX_FILES,
            "max_total_bytes": JOIN_MAX_TOTAL_BYTES,
            "max_total_mb": JOIN_MAX_TOTAL_BYTES // (1024 * 1024),
            # Stated explicitly because it is NOT implied by the total,
            # and the frontend enforces it separately.
            "max_per_file_mb": MAX_UPLOAD_BYTES // (1024 * 1024),
        },
        "allowed_audio_formats": sorted(ALLOWED_AUDIO_INPUT_FORMATS),
        "rate_limits": {
            "separate": SEPARATION_RATE_LIMIT_MAX_REQUESTS,
            "separate_hq": SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
            "stems": STEMS_RATE_LIMIT_MAX_REQUESTS,
            "stems_hq": STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
            # One key per chained YouTube tool (2026-08-21). These used
            # to be a single youtube_chain / youtube_chain_hq pair, back
            # when all three standard chained routes shared one constant.
            "youtube_analyze": YOUTUBE_ANALYZE_RATE_LIMIT_MAX_REQUESTS,
            "youtube_separate": YOUTUBE_SEPARATE_RATE_LIMIT_MAX_REQUESTS,
            "youtube_separate_hq": YOUTUBE_SEPARATE_HQ_RATE_LIMIT_MAX_REQUESTS,
            "youtube_stems": YOUTUBE_STEMS_RATE_LIMIT_MAX_REQUESTS,
            "youtube_stems_hq": YOUTUBE_STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
            # LEGACY, kept deliberately. /limits is a public contract the
            # frontend reads, and dropping a key it may still index would
            # turn a backend config change into `undefined` rendered in a
            # UI string. Both point at the SEPARATION numbers, since that
            # is what a caller reading "youtube_chain" while sizing a
            # separation job would want - not the looser analyze figure.
            #
            # Remove once lib/data/rate-limits.ts is confirmed to be on
            # the per-tool keys above and nothing else reads these.
            "youtube_chain": YOUTUBE_SEPARATE_RATE_LIMIT_MAX_REQUESTS,
            "youtube_chain_hq": YOUTUBE_SEPARATE_HQ_RATE_LIMIT_MAX_REQUESTS,
            "window_seconds": SEPARATION_RATE_LIMIT_WINDOW_SECONDS,
        },
        "features": {
            "separation_hq_enabled": SEPARATION_HQ_ENABLED,
        },
    }


@router.get("/health")
async def health():
    return {"status": "healthy"}


@router.get("/")
async def root():
    """
    Public service description. Kept deliberately thin - the exhaustive
    changelog that used to live here has moved to each module's own
    docstring, where it stays accurate because it sits next to the code
    it describes.

    `features` is read by the frontend's server-side getFeatureFlags() to
    decide whether to render the Studio Quality toggle at all. Only a
    boolean is exposed - not the model name, timeout, or any other
    internal detail - so there is nothing here for a client to learn
    about the feature beyond "on or off".
    """
    return {
        "status": "AudioForges API",
        "engine": (
            "Essentia (key/BPM) + Demucs (separation) + ffmpeg (conversion, trim, "
            "volume, reverse, fade, channels, resample, denoise, echo, silence) + "
            "rubberband (pitch, tempo) + faster-whisper (transcription)"
        ),
        "features": {
            "separation_hq_enabled": SEPARATION_HQ_ENABLED,
        },
        "limits": "/limits",
    }