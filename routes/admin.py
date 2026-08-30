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

WHAT CHANGED (2026-08-25): PAYWALL STATE ON THE PUBLIC METADATA ROUTES

Two additive changes, both driven by the same principle this file
already follows - the backend is the thing that actually knows, so the
frontend should read rather than repeat.

1. `/` root now reports `paywall_enabled` and `paywall_tools` inside the
   existing `features` block. See root()'s docstring for why this is
   worth a route change rather than letting the browser ask
   /credits/me: it is the difference between zero client requests and
   one per page load across ~90 static pages, on a site that has
   already had a Vercel Edge Request incident from navbar prefetching.

2. `/limits` now reports `separation_hq_max_duration_seconds`. The HQ
   cap (6 min) is TIGHTER than the standard cap (10 min), which is
   counterintuitive enough that a user will discover it by being
   rejected unless the UI states it. Exposing it here lets the Studio
   Quality toggle be greyed out from static data before a file is even
   picked.

Both fail closed. A credits-package problem must never break the root
endpoint, which doubles as the health signal for the whole deploy.
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-27): TRANSCRIPTION IS METERED - THREE CONSEQUENCES

All three are the same mistake in different places: a hand-maintained
list that did not move when a tool did.

1. root() no longer filters "transcribe" out of paywall_tools. That
   filter was correct while the rule was a placeholder and became a
   lie the moment PAYWALL_TOOL_TRANSCRIBE_ENABLED went true - the API
   was charging a credit while telling the frontend it wasn't, which
   is the one combination guaranteed to produce a 402 the UI never
   warned about. Every rule is now reported, enabled or not.

2. _iter_tool_routes() gained video_transcribe, youtube_transcribe and
   tiktok. Those three modules register real product routes and none of
   them were being walked, so /video-to-text, /youtube/transcribe and
   /tiktok-to-mp3 never appeared in the dashboard's tool picker and
   their traffic fell into "Other". Precisely the silent-omission
   failure that function's own docstring exists to prevent - which is
   what makes it worth stating rather than quietly fixing.

3. /limits now reports the transcription duration cap and the three
   transcription rate limits, for the same reason it already reports
   separation_hq_max_duration_seconds: a cap the user meets by being
   rejected is a cap the UI should have shown first.
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-28): /audio-to-midi-hq

midi_hq added to _iter_tool_routes(), and /limits gained
midi_hq_enabled plus both MIDI duration caps and both MIDI rate limits.

Note the list in _iter_tool_routes() has now been corrected twice in two
days, and routes/__init__.py was found to have drifted in the opposite
direction over the same period - separation_upgrade registered four
routes that were never mounted, while this file's list happily reported
them to the dashboard. Two hand-maintained lists of the same thing,
failing in opposite directions and each disguising the other. If either
drifts a third time, replace both with pkgutil enumeration rather than
another fix.
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-30): PER-TOOL RATE-LIMIT WINDOWS IN /limits

/limits published fifteen per-tool MAXIMA under a single, flat
`window_seconds` - SEPARATION's 3600 - on the unstated assumption that
every tool shares one window.

Fourteen of them do. /audio-to-midi does not:
MIDI_RATE_LIMIT_WINDOW_SECONDS is 300, so its real allowance is 5 per
FIVE MINUTES while this endpoint advertised 5 per hour. Twelvefold
looser than what rate_limit.py actually enforces, and the only symptom
for a user is a 429 the UI said could not happen yet.

This is the same failure the endpoint exists to prevent, one layer down.
/limits was built because ~20 page files, the client-side validator and
config.py each restated the same numbers and drifted. Then /limits
restated one itself - a window, hardcoded from the wrong constant, in
the file whose whole job is to stop that.

THE FIX IS ADDITIVE. A `windows` map is added alongside the existing
`window_seconds`, which stays exactly as it was. Same reasoning as the
youtube_chain keys retained above: /limits is a public contract the
frontend reads, and a key it may still index must not vanish because the
backend tidied up. `window_seconds` remains correct for every key except
audio_to_midi, and can be removed once nothing reads it - the same
condition already written against the legacy chain keys.

WHAT THIS DOES NOT FIX, and should not: /limits still carries one
hand-listed entry per route. That is the fifth such list in this
codebase, after routes/__init__.py, _iter_tool_routes(), the Cloudflare
WAF allowlist and the frontend's rate-limits.ts - and three of those
have already drifted. This change makes the existing list correct; it
does not make it self-maintaining. If it goes stale, the answer is the
one already written into _iter_tool_routes(): stop maintaining it by
hand.
--------------------------------------------------------------------------
"""
import requests
from fastapi import APIRouter, HTTPException, Query, Request

from config import (
    logger,
    NOISE_PATH_MARKERS,
    MAX_UPLOAD_BYTES,
    MAX_VIDEO_UPLOAD_BYTES,
    MAX_VIDEO_TRANSCRIBE_BYTES,
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
    AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
    VIDEO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
    MAX_TRANSCRIPTION_DURATION_SECONDS,
    MIDI_RATE_LIMIT_MAX_REQUESTS,
    MIDI_HQ_RATE_LIMIT_MAX_REQUESTS,
    MIDI_HQ_ENABLED,
    MAX_MIDI_DURATION_SECONDS,
    MAX_MIDI_HQ_DURATION_SECONDS,
    SEPARATION_RATE_LIMIT_WINDOW_SECONDS,
    # The other thirteen windows (added 2026-08-30). Imported so the
    # `windows` map below reads each tool's OWN constant rather than
    # assuming they all match SEPARATION's - which is exactly the
    # assumption that made /audio-to-midi's published limit wrong by
    # a factor of twelve.
    SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS,
    STEMS_RATE_LIMIT_WINDOW_SECONDS,
    STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_ANALYZE_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_SEPARATE_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_SEPARATE_HQ_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_STEMS_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
    AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
    VIDEO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
    MIDI_RATE_LIMIT_WINDOW_SECONDS,
    MIDI_HQ_RATE_LIMIT_WINDOW_SECONDS,
    SEPARATION_HQ_ENABLED,
    MAX_SEPARATION_DURATION_SECONDS_HQ,
    MAX_QUEUED_SEPARATIONS,
    MAX_CONCURRENT_SEPARATIONS,
    # Retention (added 2026-08-30). These became user-facing privacy
    # claims the moment /vocal-remover's FAQ started stating them, so
    # they are published rather than retyped - see the `retention` block
    # in limits() for the wrong-claim incident that prompted it.
    SEPARATION_JOB_TTL_SECONDS,
    AUDIO_TOOL_JOB_TTL_SECONDS,
    TRANSCRIPTION_JOB_TTL_SECONDS,
    # Durations and the two format sets the audio list does not cover
    # (added 2026-08-30). See the `durations` block in limits() for why
    # ~18 pages stating no length limit at all was the more urgent half
    # of this.
    MAX_AUDIO_TOOL_DURATION_SECONDS,
    AUDIO_TOOL_MAX_DURATION_SECONDS,
    VIDEO_EXTRACT_MAX_DURATION_SECONDS,
    JOIN_MAX_TOTAL_DURATION_SECONDS,
    MIN_MIDI_DURATION_SECONDS,
    MIN_MIDI_HQ_DURATION_SECONDS,
    ALLOWED_VIDEO_INPUT_FORMATS,
    MIDI_INPUT_FORMATS,
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
    Each of the modules below owns a plain APIRouter whose routes were
    all registered by decorator, exactly like the old monolith's single
    router - so `.routes` on each one is guaranteed to be real APIRoute
    objects on every FastAPI version, past or future.

    admin.py's own router is included too: admin_endpoints() filters
    /admin/* out by path anyway (see its docstring), but leaving it in
    keeps this function honestly "every route in the package" rather
    than quietly depending on that filter to hide an omission.

    separation_upgrade is included as of 2026-08-25. It registers four
    routes (/separate/upgrade, /stems/upgrade and their upgrade-info
    partners) and would otherwise be invisible to the dashboard's tool
    picker - which is exactly the silent-omission failure this function
    exists to prevent.

    THE SAME OMISSION HAD HAPPENED THREE MORE TIMES, found 2026-08-27:
    video_transcribe, youtube_transcribe and tiktok were all missing.
    So /video-to-text, /youtube/transcribe and /tiktok-to-mp3 - three
    live product routes, two of them now METERED - never reached the
    dashboard's picker, and every request they served was bucketed into
    "Other" with no way to filter it back out.

    That this list has now silently gone stale twice is the real finding.
    The failure is invisible by construction: no error, no empty result,
    just a slightly shorter dropdown that nobody counts. Anything added
    under routes/ that registers a product route MUST be added here in
    the same commit - and if this list goes stale a third time, the fix
    is to stop maintaining it by hand and enumerate the package with
    pkgutil instead.
    """
    # Imported here, not at module level - see admin_endpoints()'s
    # docstring for the import-order reasoning.
    from . import (
        youtube,
        separation,
        separation_upgrade,
        audio_tools,
        midi,
        midi_hq,
        transcribe,
        video_transcribe,
        youtube_transcribe,
        tiktok,
        media,
    )

    sub_routers = [
        youtube.router,
        separation.router,
        separation_upgrade.router,
        audio_tools.router,
        midi.router,
        midi_hq.router,
        transcribe.router,
        video_transcribe.router,
        youtube_transcribe.router,
        tiktok.router,
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
    route." Now that routes are split across several files each with
    their own local `router = APIRouter()`, this module's own `router`
    only carries admin.py's own routes.

    It walks the SUB-ROUTERS directly (see _iter_tool_routes below)
    rather than the assembled router from routes/__init__.py. That is
    deliberate and load-bearing, not a stylistic choice - see
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
    #
    # "upgrade" and "upgrade-info" are here as of 2026-08-25 for exactly
    # the same reason as "status"/"preview"/"download": they are actions
    # ON a separation job, not tools of their own. Without them,
    # /separate/upgrade/{id} would appear in the picker as a separate
    # "Separate Upgrade" family, splitting one tool's traffic across two
    # rows for no reader's benefit.
    action_segments = {"status", "preview", "download", "result", "upgrade", "upgrade-info"}

    families: dict = {}
    for route in _iter_tool_routes():
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", None)
        if not path or not methods:
            continue
        if path.startswith("/admin") or path in ("/", "/health", "/limits"):
            continue
        # The credits package mounts its own routers directly on the app
        # (see main.py), so its paths never reach this loop - but guard
        # anyway, since a future include_router() here would otherwise
        # put /credits/me in the product-tool picker.
        if path.startswith("/credits") or path.startswith("/auth"):
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


def _credits_snapshot() -> dict:
    """
    Credits/paywall state for /admin/status.

    Deliberately a SUMMARY, not the full picture. /admin/credits/* is the
    real operator surface for money - it has its own token, its own
    lockout, and its own endpoints for cost, user lookup and webhook
    triage. What belongs HERE is only what someone glancing at the
    general status page needs in order to know whether to go look there:
    is the paywall on, is anything stuck, is anyone owed credits.

    webhooks_unprocessed is the one number worth the space. A paid order
    whose webhook never processed is the failure mode that costs a real
    customer real money, and it is otherwise invisible until they email.

    Swallows everything: this is a health page, and a credits DB problem
    must not take down the reporting that covers everything else.
    """
    try:
        from credits.config import get_settings as _credits_settings
        from credits.db import connect as _credits_connect

        settings = _credits_settings()
        with _credits_connect() as conn:
            outstanding = conn.execute(
                "SELECT COALESCE(SUM(delta),0) AS n FROM credit_ledger"
            ).fetchone()["n"]
            held = conn.execute(
                "SELECT COUNT(*) AS n FROM job_charges WHERE status='held'"
            ).fetchone()["n"]
            stuck = conn.execute(
                "SELECT COUNT(*) AS n FROM webhook_events WHERE processed_at IS NULL"
            ).fetchone()["n"]

        return {
            "paywall_enabled": settings.paywall_enabled,
            "provider": settings.payments_provider,
            "metered_routes": [r.tool for r in settings.tool_rules.values() if r.enabled],
            "credits_outstanding": outstanding,
            "holds_open": held,
            "webhooks_unprocessed": stuck,
            "detail_at": "/admin/credits/overview",
        }
    except Exception as e:  # noqa: BLE001
        return {"available": False, "error": str(e)[:200]}


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
    #
    # What IS tracked, as of 2026-08-25, is per-job GPU seconds and an
    # estimated cost - recorded, never enforced. That lives in
    # /admin/credits/costs rather than here; see credits/metering.py for
    # why the old self-tracked spend breaker was removed and why nothing
    # reads those numbers back to make a decision.
    #
    # Sidecar health. Without this, a dead midi-worker is invisible from
    # the admin panel and only shows up as a run of failed jobs with a
    # generic message. Short timeout, fully swallowed errors - a health
    # probe must never be able to take down the status endpoint itself.
    # Dispatched via run_blocking: see _probe_midi_worker's docstring for
    # why calling requests.get() inline here was freezing the event loop.
    snapshot["midi_worker"] = await run_blocking(_probe_midi_worker)

    # Credits summary. Read-only, swallows its own errors, and points at
    # /admin/credits/overview for anything more than a glance.
    snapshot["credits"] = _credits_snapshot()

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

    NOTE on rate_limits (2026-08-25): these are the FREE-tier numbers.
    Callers holding credits get a looser per-account limit on the
    metered routes - see credits/limits.py. The applicable limit for a
    specific visitor comes from GET /credits/me's `rate_limit` block,
    which resolves it through the same code the limiter uses. This
    endpoint stays static and cacheable precisely because it does NOT
    know who is asking; a per-visitor number does not belong here.

    WINDOWS (2026-08-30): every max above now has a matching entry in
    `windows`, because they are NOT all the same. See the module
    docstring for the /audio-to-midi case that made this necessary - a
    limit published as twelve times looser than the one enforced.
    """
    return {
        "max_upload_bytes": MAX_UPLOAD_BYTES,
        "max_upload_mb": MAX_UPLOAD_BYTES // (1024 * 1024),
        "max_video_upload_bytes": MAX_VIDEO_UPLOAD_BYTES,
        "max_video_upload_mb": MAX_VIDEO_UPLOAD_BYTES // (1024 * 1024),
        # /video-to-text has its OWN byte cap, lower than /video-to-audio's
        # 200MB. config.py's reasoning: a 200MB video is almost certainly
        # longer than the transcription duration cap, so accepting the
        # upload only to reject it on duration wastes the entire transfer.
        # The frontend needs the number that will actually be enforced on
        # the route being used, not the larger one for a different tool.
        "max_video_transcribe_bytes": MAX_VIDEO_TRANSCRIBE_BYTES,
        "max_video_transcribe_mb": MAX_VIDEO_TRANSCRIBE_BYTES // (1024 * 1024),
        "join": {
            "max_files": JOIN_MAX_FILES,
            "max_total_bytes": JOIN_MAX_TOTAL_BYTES,
            "max_total_mb": JOIN_MAX_TOTAL_BYTES // (1024 * 1024),
            # Stated explicitly because it is NOT implied by the total,
            # and the frontend enforces it separately.
            "max_per_file_mb": MAX_UPLOAD_BYTES // (1024 * 1024),
        },
        "allowed_audio_formats": sorted(ALLOWED_AUDIO_INPUT_FORMATS),
        # The two sets the audio list does NOT cover (added 2026-08-30).
        # Both were already enforced and neither was published, so a page
        # wanting either had to hand-write it - which is precisely how
        # /stems came to omit AIFF while the tool accepted it.
        #
        # Video is deliberately its own set rather than folded into the
        # audio one: /video-to-audio and /video-to-text accept these, no
        # other endpoint should, and no endpoint anywhere outputs one.
        # Merging them would make every audio tool advertise mp4.
        "allowed_video_formats": sorted(ALLOWED_VIDEO_INPUT_FORMATS),
        # /audio-to-midi and /audio-to-midi-hq accept the audio set PLUS
        # opus and webm - basic-pitch decodes via librosa, which falls
        # back to ffmpeg for containers soundfile cannot read. Published
        # as its own complete list rather than as "audio plus two" so the
        # frontend renders it directly instead of reconstructing it.
        "allowed_midi_input_formats": sorted(MIDI_INPUT_FORMATS),
        # ---------- DURATION CAPS (added 2026-08-30) ----------
        # /limits published four duration caps - midi, midi_hq,
        # separation_hq, transcription - and nothing for the ~18 ordinary
        # job tools, which left every one of those pages silent about a
        # limit that will reject an upload. Silence is better than a
        # wrong number and worse than the right one.
        #
        # A FALLBACK PLUS OVERRIDES, not a flat per-tool map, because
        # that is the actual shape in config.py: _validate_duration_or_reject
        # looks the tool up in AUDIO_TOOL_MAX_DURATION_SECONDS and falls
        # through to MAX_AUDIO_TOOL_DURATION_SECONDS when it is absent.
        # Publishing a flattened map would mean re-listing every tool
        # here - a sixth hand-maintained enumeration, and three of the
        # existing five have already drifted. Read
        # per_tool_seconds[tool] ?? default_seconds; a tool missing from
        # the map is not an omission, it is the fallback.
        #
        # WORTH KNOWING WHY THE OVERRIDES SUDDENLY MATTER: that map sat
        # in config.py for weeks with ZERO readers - a grep for its name
        # across the container returned two hits, both inside config.py
        # itself. Every tool silently took the 3600 fallback, so pitch
        # and tempo's 900s entries were decoration. Wired up 2026-08-30,
        # which means those two tools dropped from 1 hour to 15 minutes
        # in a single deploy. Any page still advertising an hour for
        # pitch or tempo has been wrong since that deploy - which is the
        # strongest argument available for reading this block rather
        # than typing the numbers.
        "durations": {
            "audio_tools_default_seconds": MAX_AUDIO_TOOL_DURATION_SECONDS,
            "audio_tools_per_tool_seconds": dict(AUDIO_TOOL_MAX_DURATION_SECONDS),
            # Keyed by the same job_type string create_job() uses, which
            # is what _validate_duration_or_reject looks up - so the keys
            # here are the enforced keys, not a parallel naming scheme.
            #
            # /convert is the ONE tool with no duration cap at all, and
            # it is absent from both values above rather than carrying a
            # large number - so it needs its own key or the frontend
            # would apply the 3600 fallback to it and reject uploads the
            # server would have accepted.
            #
            # VERIFIED AT THE ROUTE, not inferred from config.py's
            # comment: convert_audio_route is the only caller anywhere
            # passing check_duration=False. Worth stating how it was
            # checked, because the per-tool map immediately above spent
            # weeks being described accurately by a comment and read by
            # nothing - a config comment is evidence of intent, not of
            # behaviour.
            "exempt_tools": ["convert"],
            "video_extract_max_seconds": VIDEO_EXTRACT_MAX_DURATION_SECONDS,
            # A TOTAL across every file in one /join request, not per
            # file - ten four-minute tracks is a forty-minute re-encode
            # however modest each one looks alone.
            "join_max_total_seconds": JOIN_MAX_TOTAL_DURATION_SECONDS,
            # LOWER bounds, the only two on the site. Below these there
            # is not enough signal for the model to find anything and the
            # result is a guaranteed empty MIDI, so submitting is
            # rejected rather than spending a worker round trip on it.
            "midi_min_seconds": MIN_MIDI_DURATION_SECONDS,
            "midi_hq_min_seconds": MIN_MIDI_HQ_DURATION_SECONDS,
        },
        # ---------- RETENTION (added 2026-08-30) ----------
        # Published because these are PRIVACY CLAIMS now, not internal
        # details. /vocal-remover's FAQ said uploads are "deleted once
        # processing finishes" - true for every other tool on the site
        # and false for that one, because separation deliberately retains
        # the source for the Studio Quality upgrade path. A user read a
        # sentence about their own file that was not true of their own
        # file.
        #
        # The fix is not a corrected sentence, it is removing the
        # opportunity to type one: ~20 pages state a retention number,
        # and any number typed on a page is a number that can drift from
        # the code. Same argument as every other value in this endpoint.
        #
        # THREE SHAPES, not one pair of numbers, because the tools
        # genuinely differ and flattening them is how the wrong claim
        # happened in the first place:
        #
        #   separation    input AND output both live to the job TTL. The
        #                 input is retained ON PURPOSE (routes/separation.py
        #                 calls set_job_input() and passes an empty
        #                 cleanup_paths) so /separate/upgrade can re-run a
        #                 finished standard job at HQ without a second
        #                 upload. The only tool family where the upload
        #                 outlives the job.
        #
        #   audio_tools   input deleted the moment the job ends - win,
        #                 lose, or killed by a redeploy - via
        #                 _run_tool_job's `finally`. Only the OUTPUT
        #                 waits for the TTL. Verified across all 18: the
        #                 14 sharing _submit_audio_tool and the 4 with
        #                 their own submit paths (/trim, /join,
        #                 /video-to-audio, /silence-split), every one of
        #                 which passes its input to cleanup_paths.
        #
        #   transcription input deleted at job end like the audio tools,
        #                 but the OUTPUT is inline text in the job record
        #                 rather than a file - so nothing sits on disk
        #                 after processing at all. Worth its own shape
        #                 because "your file is available for an hour" is
        #                 the wrong sentence for a transcript.
        #
        # input_seconds is null where the input does not survive the job,
        # rather than 0 - null reads as "not applicable", 0 invites
        # "deleted after zero seconds", and the frontend should render a
        # different sentence, not the same one with a different number.
        "retention": {
            "separation": {
                "input_deleted_when": "ttl",
                "input_seconds": SEPARATION_JOB_TTL_SECONDS,
                "output_seconds": SEPARATION_JOB_TTL_SECONDS,
                "output_kind": "files",
            },
            "audio_tools": {
                "input_deleted_when": "job_end",
                "input_seconds": None,
                "output_seconds": AUDIO_TOOL_JOB_TTL_SECONDS,
                "output_kind": "file",
            },
            "transcription": {
                "input_deleted_when": "job_end",
                "input_seconds": None,
                "output_seconds": TRANSCRIPTION_JOB_TTL_SECONDS,
                "output_kind": "text",
            },
        },
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
            # ADDED 2026-08-27, now that all three transcription routes
            # are metered. They share one credits rule and one GPU
            # endpoint but have SEPARATE per-IP rate-limit buckets, since
            # rate_limit.py keys on (ip, path) - so three keys, not one.
            "speech_to_text": AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
            "video_to_text": VIDEO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
            "youtube_transcribe": YOUTUBE_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
            # ADDED 2026-08-28. Both MIDI tools, because they are two
            # products rather than two qualities of one - see
            # credits/config.py's rule note. The frontend needs both
            # numbers on one page if it ever offers the upgrade inline.
            #
            # audio_to_midi's WINDOW is 300s, not 3600 - the only tool in
            # this block that differs. Until 2026-08-30 that fact had
            # nowhere to live in this response, so this number was read
            # as hourly and advertised twelvefold too loose. See
            # `windows` below.
            "audio_to_midi": MIDI_RATE_LIMIT_MAX_REQUESTS,
            "audio_to_midi_hq": MIDI_HQ_RATE_LIMIT_MAX_REQUESTS,
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
            # PER-TOOL WINDOWS (added 2026-08-30). Every key above has an
            # entry here, read from that tool's OWN window constant.
            #
            # Fourteen of the fifteen are 3600, which is exactly why the
            # single flat window_seconds below survived this long without
            # anyone noticing: it was right almost everywhere.
            # /audio-to-midi is 300, so its published allowance was 5 per
            # hour against 5 per five minutes actually enforced - and the
            # only way a user learns that is a 429 the UI promised could
            # not happen.
            #
            # This is the same drift /limits was built to end, one layer
            # down: the endpoint that stops the frontend restating
            # config.py's numbers had restated one of them itself, from
            # the wrong constant.
            #
            # ADDITIVE. window_seconds below is untouched, for the same
            # reason the youtube_chain keys above are untouched - a
            # public contract does not lose a key because the backend
            # tidied up. New consumers read windows[tool]; the flat value
            # can be dropped once nothing reads it, which is the same
            # condition already written against those legacy keys.
            "windows": {
                "separate": SEPARATION_RATE_LIMIT_WINDOW_SECONDS,
                "separate_hq": SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS,
                "stems": STEMS_RATE_LIMIT_WINDOW_SECONDS,
                "stems_hq": STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
                "youtube_analyze": YOUTUBE_ANALYZE_RATE_LIMIT_WINDOW_SECONDS,
                "youtube_separate": YOUTUBE_SEPARATE_RATE_LIMIT_WINDOW_SECONDS,
                "youtube_separate_hq": YOUTUBE_SEPARATE_HQ_RATE_LIMIT_WINDOW_SECONDS,
                "youtube_stems": YOUTUBE_STEMS_RATE_LIMIT_WINDOW_SECONDS,
                "youtube_stems_hq": YOUTUBE_STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
                "speech_to_text": AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
                "video_to_text": VIDEO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
                "youtube_transcribe": YOUTUBE_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
                "audio_to_midi": MIDI_RATE_LIMIT_WINDOW_SECONDS,
                "audio_to_midi_hq": MIDI_HQ_RATE_LIMIT_WINDOW_SECONDS,
                "youtube_chain": YOUTUBE_SEPARATE_RATE_LIMIT_WINDOW_SECONDS,
                "youtube_chain_hq": YOUTUBE_SEPARATE_HQ_RATE_LIMIT_WINDOW_SECONDS,
            },
            # LEGACY as of 2026-08-30, same status as the two chain keys
            # above: correct for every tool except audio_to_midi, kept
            # because the frontend may still index it. Read `windows`
            # instead.
            "window_seconds": SEPARATION_RATE_LIMIT_WINDOW_SECONDS,
        },
        "features": {
            "separation_hq_enabled": SEPARATION_HQ_ENABLED,
            # ADDED 2026-08-25. The HQ input cap is TIGHTER than the
            # standard one (6 min vs 10) - counterintuitive, and
            # deliberately so per config.py: at ~5x the per-minute cost a
            # 10 min track would eat the entire 30 min timeout budget.
            #
            # Exposed here so the frontend can grey out the Studio
            # Quality toggle from STATIC data before a file is even
            # picked, rather than accepting an upload and rejecting it at
            # submit. Same reasoning as every other number in this
            # endpoint: the backend knows, so the frontend should read
            # rather than repeat.
            "separation_hq_max_duration_seconds": MAX_SEPARATION_DURATION_SECONDS_HQ,
            # ADDED 2026-08-27, same argument one tool over. All three
            # transcription routes enforce this cap and reject past it
            # with a 400 - after the upload has already been sent, which
            # for a 100MB video is a genuinely expensive way to learn a
            # limit. The frontend can now check duration client-side
            # (HTMLMediaElement.duration) and say so before the transfer.
            #
            # Applies to EVERY caller, free or paid - it is a hard
            # rejection line, not a tier. The tiering is entirely in the
            # credits: 2 free ops a month, then 1 credit per job.
            "transcription_max_duration_seconds": MAX_TRANSCRIPTION_DURATION_SECONDS,
            # ADDED 2026-08-28. midi_hq_enabled mirrors
            # separation_hq_enabled exactly: a kill switch the frontend
            # reads so it can hide the HQ option rather than letting
            # someone submit into a guaranteed 503.
            #
            # Both duration caps are exposed because the two MIDI tools
            # accept the same length TODAY (600s each) and there is no
            # guarantee they always will - a frontend that reads one and
            # assumes the other would break silently the first time they
            # diverge, which is exactly the drift this endpoint exists to
            # prevent.
            "midi_hq_enabled": MIDI_HQ_ENABLED,
            "midi_max_duration_seconds": MAX_MIDI_DURATION_SECONDS,
            "midi_hq_max_duration_seconds": MAX_MIDI_HQ_DURATION_SECONDS,
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

    --------------------------------------------------------------------
    PAYWALL FLAGS (added 2026-08-25) follow the same rule, and exist for
    a specific reason worth stating.

    Without them, the frontend's CreditProvider has to call /credits/me
    from the BROWSER on every page load just to discover whether the
    paywall is on at all. That is one client request on ~90 static pages
    for a feature that is currently disabled - and this site has already
    had a Vercel Edge Request consumption incident caused by navbar
    prefetching, so that shape of problem is not hypothetical here.

    Read from here instead, it costs nothing: getFeatureFlags() already
    fetches this route server-side, cached by Next's fetch cache, and
    already fails closed. While the paywall is off the provider makes
    ZERO requests.

    Values are EFFECTIVE state - global AND per-tool - so the frontend
    never has to combine two flags and cannot get that combination
    wrong.

    EVERY RULE IS REPORTED, including disabled ones (changed
    2026-08-27). A tool that is off reports false rather than being
    absent, which is more useful to the frontend than having to tell
    "not metered" apart from "key missing".

    "transcribe" used to be filtered out of this dict on the grounds
    that it was a placeholder rule nothing should render. That was true
    until PAYWALL_TOOL_TRANSCRIBE_ENABLED went true, at which point the
    API was charging a credit for /speech-to-text, /video-to-text and
    /youtube/transcribe while telling the frontend those tools were
    free. The user-visible result of that combination is the worst one
    available: no credit balance shown, no "1 credit" label, both free
    ops spent invisibly, and then a 402 out of nowhere.

    The filter is DELETED rather than inverted, deliberately. An
    exclusion list keyed on tool name has to be edited every time a tool
    changes state, by someone who remembers it exists - and it had
    already gone stale once by the time anyone looked.

    FAILS CLOSED, and that matters more than it looks: this route is also
    the deploy's health signal. A credits-package problem must degrade to
    "paywall off" rather than take down the endpoint that tells you
    whether the API is alive at all.
    --------------------------------------------------------------------
    """
    try:
        from credits.config import get_settings as _credits_settings

        _c = _credits_settings()
        paywall_enabled = _c.paywall_enabled
        paywall_tools = {
            key: bool(_c.paywall_enabled and rule.enabled)
            for key, rule in _c.tool_rules.items()
        }
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[CREDITS] Could not read paywall state for / - reporting off: {e}")
        paywall_enabled = False
        paywall_tools = {}

    return {
        "status": "AudioForges API",
        "engine": (
            "Essentia (key/BPM) + Demucs (separation) + ffmpeg (conversion, trim, "
            "volume, reverse, fade, channels, resample, denoise, echo, silence) + "
            "rubberband (pitch, tempo) + faster-whisper (transcription)"
        ),
        "features": {
            "separation_hq_enabled": SEPARATION_HQ_ENABLED,
            # ADDED 2026-08-29. It was in /limits and NOT here, and the
            # difference matters because getFeatureFlags() reads THIS
            # route, not /limits.
            #
            # The consequence was specific and bad: with no availability
            # flag to read, the frontend tied the Multi-track engine
            # picker to paywall_tools["audio-to-midi-hq"] instead. Those
            # are different questions. Turning OFF charging - the exact
            # state the integration spec tells you to test in - made the
            # tool vanish rather than become free, so the free-flow test
            # could not be run at all.
            #
            # Sits beside separation_hq_enabled because it is the same
            # kind of flag: "can this tool run", independent of "does it
            # cost anything". Both belong here; neither belongs in
            # paywall_tools.
            "midi_hq_enabled": MIDI_HQ_ENABLED,
            "paywall_enabled": paywall_enabled,
            "paywall_tools": paywall_tools,
        },
        "limits": "/limits",
    }