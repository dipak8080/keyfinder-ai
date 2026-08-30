"""
routes/youtube.py - /download (the busiest endpoint on this API) plus
every /youtube/* chained route: paste a URL, get the processed result,
skipping the manual download-then-reupload step.

Split out of the old monolithic routes.py (2026-08-14 restructure). Pure
move: every docstring, comment, and line of logic here is unchanged from
its original location. Nothing in this file changes behaviour.

/download lives here rather than in its own file: it's a YouTube tool
structurally, and grouping it with the /youtube/* chained tools means
every route that touches yt-dlp, cookie accounts, the proxy circuit
breaker, or the CDN degradation breaker is in ONE file - which matters
most on exactly the days this file gets touched under pressure, since
none of that shared context (breaker state, cookie health, proxy
escalation) has to be chased across two files.

--------------------------------------------------------------------------
WHAT CHANGED (2026-08-02) AND WHY

Five problems, each of which had produced real user-visible failures that
left no useful trace in the logs:

1. UPLOADS WERE BUFFERED WHOLE IN MEMORY, THEN SIZE-CHECKED.
   Twenty routes did `content = await file.read()`, checked len(), then
   wrote the buffer to disk - all three steps synchronous, on the event
   loop, on a box with NO SWAP (Incus container VPS; swapon is not
   permitted). The size check ran AFTER the whole body was resident, so
   the limit bounded nothing: an oversized upload was fully buffered
   before being rejected. All twenty now call save_upload() from
   upload.py, which streams in 1MB chunks, enforces the cap mid-stream,
   deletes the partial file on rejection, and returns 413 (not a generic
   400) so the frontend can tell "too big" from "wrong format". (Not
   directly relevant to this file - /download and /youtube/* take no
   uploads - but kept here since the surrounding history explains why
   the rest of the app looks the way it does.)

2. TTL CLEANUP RAN ON THE REQUEST PATH.
   cleanup_expired_jobs() was called at the top of ~20 handlers. It now
   runs on a 60s background timer in main.py; every call here is gone.

3. BACKGROUND JOBS COULD STICK ON "processing" FOREVER.
   Each _run_*_background() marked its job failed inside `except`, but an
   exception raised outside those handlers skipped all of them - most
   realistically acquire_slot_or_503() raising HTTPException inside a
   background task, where no HTTP layer exists to catch it. Every
   background task now calls jobs.fail_if_unfinished() from a `finally`.
   See _chain_download() below for the /youtube/* specific version of
   this fix.

4. THE SEPARATION QUEUE WAS UNBOUNDED.
   See _shared.py's _reject_if_separation_queue_full() docstring for the
   full story - every /youtube/separate*, /youtube/stems* route below
   calls it before creating a job.

5. LOGS COULDN'T ANSWER "WHAT HAPPENED TO THIS REQUEST?"
   Every job now logs a start line (file, size) and an end line
   (COMPLETE/FAILED plus elapsed seconds), both carrying job=<id>.
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-05):

/download's error-classification chain now has a dedicated branch for
CDN connect-timeouts (is_cdn_connect_timeout_error, from youtube.py) -
production logs showed repeated ~20s connect timeouts to the SAME
googlevideo media edge across otherwise-unrelated requests, taking a
full 73s (3 attempts x ~23s) to fail and then returning a generic 500.
Two things changed:
  - ydl_opts now sets socket_timeout=10, so a doomed connect-timeout
    fails faster per attempt.
  - The error chain now recognizes this failure shape explicitly and
    returns 503 ("try again shortly") instead of falling through to the
    generic 500 - this is transient infra flakiness on YouTube's/this
    server's networking, not a bug in this app, and the two deserve
    different status codes for the same reason every other branch in
    this chain already does.
should_use_proxy() in youtube.py was also updated to escalate this
failure shape to the proxy tier, since a different exit IP frequently
resolves to a different, reachable CDN edge - that change lives entirely
in youtube.py (the top-level module, not this routes file) and needs no
further changes here.
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-07):

/download had no branch for a YouTube Music Premium restriction ("This
video is only available to Music Premium members"). This is the same
shape as the existing age-restricted / members-only handling: an
account-privilege problem, not an IP-reputation problem. A different
cookie account that happens to have Music Premium active could succeed
where the current one can't. is_music_premium_error() (added in
youtube.py) is now wired into the same three places its age-restricted /
members-only siblings already were, and gets its own 403 branch here.

should_use_proxy() in youtube.py briefly stopped escalating CDN
connect-timeouts to the proxy tier the same day, then was reverted a few
hours later once the proxy provider's own usage log showed 190/190
googlevideo fetches succeeding through it. CDN connect-timeouts DO
escalate to proxy again, same as geo-restriction and bot-check (see
should_use_proxy()'s docstring in youtube.py for the full history).

On top of that, a direct-path degradation breaker was added
(CDN_DEGRADED_THRESHOLD/_WINDOW_SECONDS/_COOLDOWN_SECONDS in config.py,
record_cdn_timeout()/direct_path_degraded() in youtube.py): once enough
direct-path CDN timeouts cluster together, further downloads skip the
doomed ~10s direct attempt entirely and go straight to proxy for a
cooldown window, surfaced via GET /admin/status's "cdn" block (see
admin.py) and resettable via POST /admin/reset-cdn-breaker (also
admin.py).
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-10): TOOL / TIER TAGGING

Every route that creates a job now calls log_stream.set_job_context(tool,
tier) before kicking off any background task - see log_stream.py's own
"SCHEMA CHANGE" note for the write-side reasoning.

Call sites in THIS file, and why each is placed where it is:

  - youtube_analyze_route, youtube_separate_route,
    youtube_separate_hq_route, youtube_stems_route, youtube_stems_hq_route
    - set BEFORE asyncio.create_task(), not inside the spawned
    _run_youtube_*() function. asyncio.create_task() copies whatever
    context exists AT THE MOMENT it's called into the new task - the same
    mechanism request_id already relies on - so calling it earlier here
    is what makes the tag visible on the initial POST's own HTTP log row,
    not just on the background job's later lines.
  - download_audio() - the one synchronous tool with no job/background
    task at all. Tagged for consistency, so "which tool" is answerable
    the same way for every row in request_logs, not just the
    tiered/backgrounded ones.

Nothing here changes behaviour, status codes, or response shapes - every
added line is a single set_job_context(...) call with no side effects
beyond what gets written to the log tables.
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-21): PER-TOOL RATE LIMITS

The five /youtube/* POST routes below used to share exactly two pairs of
constants between them - YOUTUBE_CHAIN_RATE_LIMIT_* across analyze,
separate and stems, and YOUTUBE_CHAIN_HQ_RATE_LIMIT_* across the two HQ
routes. Each now names its own pair.

What this does NOT change, and is worth stating so nobody re-derives it
under pressure later: the per-IP BUCKETS were already separate. See
rate_limit.py's `_requests`, keyed on (ip, path) - one IP burning its
allowance on /youtube/analyze never consumed /youtube/separate's. The
shared constants only meant the three tools were forced to agree on a
number, not that they drew from a common pool.

The number was the problem. /youtube/analyze holds _analysis_semaphore
(4 slots) for ~30s of Essentia on a 3-minute trim; /youtube/separate and
/youtube/stems each hold the SINGLE _separation_semaphore slot for 3-5
minutes of Demucs. 15/hour is fine for the first and far too loose for
the other two - 15 accepted separation jobs from one IP is over an hour
of the only separation slot on the box, which every /separate, /stems
and other /youtube/* job then waits behind. The loosest tool's needs
were setting the limit for the most expensive ones.

Config knobs are now per tool (see the YOUTUBE CHAINED TOOLS block in
config.py). Nothing else in this file changed - same handlers, same
status codes, same response shapes.
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-25): CREDITS ON THE TWO HQ ROUTES

This file was MISSED when the paywall shipped. routes/separation.py got
the guard on /separate-hq and /stems-hq; /youtube/separate-hq and
/youtube/stems-hq were listed in credits/config.py's tool rules and had
their env flags turned on - but no guard, no tiered rate limit, and no
metering. So flipping the flag made them *report* as metered while
serving Studio Quality separation free to anyone at ~$0.018 of GPU per
run. Caught in production testing; the flags were turned off within
minutes and this is the real fix.

TWO STRUCTURAL DIFFERENCES from routes/separation.py, both of which
change where things can go wrong:

1. THE DOWNLOAD HAPPENS INSIDE THE BACKGROUND TASK. There is no file at
   submit time, so there is no duration to probe, so the pre-charge
   duration check that /separate-hq performs is IMPOSSIBLE here. The
   charge is therefore taken at submit with input_seconds=None, which
   paywall.decide() treats as billable - unknown duration on a metered
   tool must never fail open.

   The safety net is the refund, not a pre-check: if the track turns out
   to exceed MAX_SEPARATION_DURATION_SECONDS_HQ, _run_demucs_on_gpu
   raises SeparationError, _run_tool_job catches it, and its `finally`
   calls settle_or_refund() - the credit is back immediately, not in 90
   minutes. That is a worse experience than /separate-hq's clean 400
   (the user waits for a download before learning), but it is honest and
   it costs them nothing.

2. _chain_download RETURNS EARLY ON FAILURE, before _run_tool_job is
   ever reached - so the `finally` that normally settles the charge does
   not run on that path. A failed YouTube download would have held the
   credit for the full sweeper window. _run_youtube_separation now calls
   settle_or_refund() explicitly on that branch. This is exactly the
   kind of hole a shared runner hides: the refund looked "handled
   everywhere" because it was handled in the one place most code goes
   through.
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-30): /download CAN RETURN A URL INSTEAD OF BASE64

/download returned the entire audio file as a base64 string inside the
JSON body. That shape predates the job system - it is not a decision
anyone made about large files, it is simply older than the routes that
do this properly - and at MAX_VIDEO_DURATION_SECONDS it does not work on
either end:

  BROWSER: a ~420 MB WAV becomes ~560 MB of base64 that the frontend
  holds as a string and then converts to a Blob. That is an OOM crash on
  most phones - and /youtube-to-wav, the highest-traffic page on the
  site (~10K/month), defaults to WAV, so the worst case sits on the
  busiest route.

  SERVER: the same request allocates ~420 MB reading the file, ~560 MB
  encoding it, and ~560 MB again when JSONResponse serializes the body -
  roughly 1.5 GB resident for ONE request, on a VPS with NO SWAP, behind
  a semaphore that allows concurrent downloads. The cache-HIT path did
  exactly the same thing. Two overlapping long WAVs is an OOM kill with
  nothing to absorb it.

The fix is the shape the job-based tools already use: return a URL and
let the browser stream it to disk. `response=url` on the POST returns
{"title", "format", "url", "expires_at"}, and the new
GET /download/file/{video_id}.{fmt} serves the bytes via FileResponse -
which streams in chunks and supports Range requests, so a dropped mobile
connection resumes instead of restarting a 400 MB transfer.

DEFAULT IS STILL "base64", DELIBERATELY. This route serves ~10K requests
a month; flipping its response shape in the same deploy that introduces
the new path would mean any mistake takes the busiest page down with no
way to tell which of the two changes caused it. The frontend opts in
when it is ready, and the base64 branch gets deleted in a later, boring
deploy.

WHY THE URL IS SIGNED: /download/file/{video_id}.{fmt} with no token
would be trivially enumerable - every cached track on the box fetchable
by anyone who can guess a YouTube ID, bypassing the rate limiter, with
bandwidth being the actual bill. An HMAC over (video_id, format, expiry)
closes that while staying stateless: no tokens table, no cleanup job,
and links that expire on their own.

TWO FOLLOW-UPS, same day, both raised by the frontend during migration:

  size_bytes IS NOW IN THE PAYLOAD. url mode never touches the bytes, so
  the result card lost the file size it used to derive from the base64
  string's length. The server already has the number; sending it saves a
  HEAD request made purely to print a figure.

  THE FILE ROUTE TAKES ?disposition=inline. FileResponse(filename=...)
  sets Content-Disposition: attachment, which is REQUIRED for the
  download button - <a download> is ignored cross-origin, and
  api.audioforges.com is cross-origin from the site, so the header is
  the only thing that makes a click save rather than navigate. But that
  same header stops the preview player from streaming the file, and a
  working download button would never have revealed it. attachment stays
  the default; inline is opt-in per request.
--------------------------------------------------------------------------
"""
import os
import time
import uuid
import hmac
import base64
import hashlib
import asyncio
from typing import Optional
from functools import partial

from fastapi import APIRouter, HTTPException, Depends, Query, Form
from fastapi.responses import JSONResponse, FileResponse

from config import (
    logger,
    UPLOAD_DIR,
    ANALYSIS_MAX_SECONDS,
    DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS,
    DOWNLOAD_RATE_LIMIT_MAX_REQUESTS,
    DOWNLOAD_RATE_LIMIT_WINDOW_SECONDS,
    SEPARATION_MODEL,
    SEPARATION_OVERLAP,
    DEMUCS_TIMEOUT_SECONDS,
    MAX_SEPARATION_DURATION_SECONDS,
    SEPARATION_MODEL_HQ,
    SEPARATION_OVERLAP_HQ,
    DEMUCS_TIMEOUT_SECONDS_HQ,
    MAX_SEPARATION_DURATION_SECONDS_HQ,
    SEPARATION_HQ_ENABLED,
    YOUTUBE_ANALYZE_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_ANALYZE_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_SEPARATE_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_SEPARATE_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_SEPARATE_HQ_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_SEPARATE_HQ_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_STEMS_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_STEMS_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_ANALYZE_JOB_TTL_SECONDS,
)
from utils import (
    cleanup_file,
    release_memory_to_os,
    run_blocking,
    run_in_killable_subprocess,
    acquire_slot_or_503,
    get_camelot,
    _analysis_semaphore,
    _download_semaphore,
    _separation_semaphore,
)
from youtube import (
    is_bot_check_error,
    is_geo_restricted_error,
    is_age_restricted_error,
    is_members_only_error,
    is_music_premium_error,
    is_not_yet_live_error,
    is_permanent_error,
    is_cdn_connect_timeout_error,
    is_cdn_read_timeout_error,
    is_page_reload_error,
    is_media_forbidden_error,
    is_format_unavailable_error,
    is_valid_youtube_url,
    extract_video_id,
    proxy_available,
    get_cookie_accounts,
    ytdlp_alert_logger,
)
from audio_analysis import detect_key_bpm_essentia, cross_check_with_librosa, trim_audio_for_analysis
from rate_limit import check_rate_limit
from cache import get_cached_audio, put_cached_audio, get_cached_path, put_cached_file
from monitoring import record_result
from download_progress import make_progress_hook
from jobs import (
    create_job,
    mark_complete,
    mark_stems_complete,
    mark_data_complete,
    mark_failed,
    fail_if_unfinished,
    get_job,
)
from separation import run_separation, run_stem_separation
from youtube_chain import download_audio_to_file, ChainDownloadError
from log_stream import (
    get_current_request_id,
    set_job_context,
    remember_job_tags,
    tag_from_job,
)

# Credits. Inert while PAYWALL_ENABLED is unset; see the 2026-08-25 note
# in this module's docstring for why these two HQ routes were missed the
# first time and what that cost.
from credits import metering, paywall
from credits.identity import Identity
from credits.ledger import settle_or_refund
from credits.limits import tiered_rate_limit

from ._shared import spawn_background_task, _mb, _reject_if_separation_queue_full, _tool_status, _run_tool_job

router = APIRouter()


# ============================================================
# SIGNED DOWNLOAD URLS (added 2026-08-30)
#
# See this module's docstring for why /download can now hand back a URL
# instead of a base64 body, and why that URL has to be signed.
#
# Stateless on purpose. A tokens table would need a schema migration, a
# write on every download, and a sweeper to delete expired rows - three
# new things that can break, replacing an HMAC that cannot get out of
# sync with itself. The expiry travels inside the token and is covered
# by the signature, so a forged expiry fails verification.
# ============================================================
DOWNLOAD_URL_TTL_SECONDS = int(os.environ.get("DOWNLOAD_URL_TTL_SECONDS", "3600"))

# SET DOWNLOAD_URL_SECRET IN .env. The ADMIN_KEY fallback exists so this
# works on first deploy without a config change, but it couples two
# unrelated things: rotating the admin key would silently invalidate
# every outstanding download link. The uuid4 fallback below that is
# process-local, so links simply stop working after a restart - annoying,
# never insecure, which is the right direction for a missing secret.
_DOWNLOAD_URL_SECRET = (
    os.environ.get("DOWNLOAD_URL_SECRET")
    or os.environ.get("ADMIN_KEY")
    or uuid.uuid4().hex
).encode()

if not os.environ.get("DOWNLOAD_URL_SECRET"):
    logger.warning(
        "[DOWNLOAD] DOWNLOAD_URL_SECRET is not set - falling back to ADMIN_KEY "
        "(or a per-process random value if that is unset too). Set it in .env so "
        "signed download links survive an admin-key rotation and a container restart."
    )

_MEDIA_TYPES = {"mp3": "audio/mpeg", "wav": "audio/wav"}


def _sign_download_token(video_id: str, fmt: str, expires_at: int) -> str:
    """Token format is '<expires_at>.<signature>' - the expiry is
    readable so verification can reject a stale token without any lookup,
    and signed so it cannot be edited to extend itself."""
    payload = f"{video_id}:{fmt}:{expires_at}"
    sig = hmac.new(_DOWNLOAD_URL_SECRET, payload.encode(), hashlib.sha256).hexdigest()[:32]
    return f"{expires_at}.{sig}"


def _verify_download_token(video_id: str, fmt: str, token: str) -> bool:
    try:
        expires_str, _ = token.split(".", 1)
        expires_at = int(expires_str)
    except (ValueError, AttributeError):
        return False

    if time.time() > expires_at:
        return False

    # compare_digest, NOT ==. A plain string comparison short-circuits on
    # the first differing byte, and that timing difference is enough to
    # recover a valid signature one byte at a time given enough requests.
    return hmac.compare_digest(token, _sign_download_token(video_id, fmt, expires_at))


def _safe_filename(title: str, fmt: str) -> str:
    """Content-Disposition filename built from a YouTube title.

    Strips path separators and the Windows-reserved set, drops
    non-printables, and caps the length - a title is attacker-influenced
    text (anyone can name a video anything) heading into a response
    header. Starlette handles RFC 5987 encoding of whatever survives, so
    this only has to remove what should never be in a filename at all.
    """
    cleaned = "".join(
        c for c in (title or "audio")
        if c.isprintable() and c not in '/\\:*?"<>|'
    )
    cleaned = cleaned.strip()[:120] or "audio"
    return f"{cleaned}.{fmt}"


def _url_payload(
    video_id: str,
    fmt: str,
    title: Optional[str],
    size_bytes: Optional[int] = None,
) -> dict:
    """The `response=url` body. expires_at is returned so the frontend
    can decide whether to reuse a link or ask for a fresh one, rather
    than discovering expiry as a 403 partway through a download.

    size_bytes added 2026-08-30. url mode never touches the bytes, so the
    frontend lost the file size it used to derive from the base64 length,
    and its result card lost the number. The server already knows it -
    either from the file it just wrote or from a stat on the cache entry -
    so sending it saves the client a HEAD request purely to print a
    figure. Omitted rather than sent as null when it genuinely could not
    be read, so `"size_bytes" in payload` is a meaningful check.
    """
    expires_at = int(time.time()) + DOWNLOAD_URL_TTL_SECONDS
    token = _sign_download_token(video_id, fmt, expires_at)
    payload = {
        "title": title or "Unknown",
        "format": fmt,
        "url": f"/download/file/{video_id}.{fmt}?token={token}",
        "expires_at": expires_at,
    }
    if size_bytes is not None:
        payload["size_bytes"] = size_bytes
    return payload


# ============================================================
# /download - YouTube URL to MP3/WAV (synchronous, cached)
#
# The only tool that takes no upload, which is why it was the ONLY tool
# still working during the incident that prompted this rewrite: nothing
# to buffer, nothing to stall the loop with, and its result is cached.
# ============================================================

@router.post(
    "/download",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=DOWNLOAD_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=DOWNLOAD_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def download_audio(
    url: str = Form(...),
    format: str = Form("mp3"),
    response: str = Form("base64"),
):
    """
    `response` selects the body shape:

      "base64" (default) - {"title", "audio", "format"}, the original
          shape. Every existing client keeps working untouched.

      "url" - {"title", "format", "url", "expires_at"}. The bytes are
          fetched separately from GET /download/file/{video_id}.{fmt},
          which streams them. Use this for anything that might be large;
          see the 2026-08-30 note in this module's docstring for the
          memory numbers that make it necessary on WAV.

    URL mode needs a video_id - it is both the cache key and the resource
    identifier. Every URL that passes is_valid_youtube_url() yields one,
    so in practice this only fails if a YouTube URL shape changes under
    us, and it degrades to base64 rather than erroring: a working large
    response beats a clean failure.
    """
    # Synchronous tool, no job - tagged anyway so its row in request_logs
    # reports "DOWNLOAD" the same consistent way every other tool's rows
    # do, instead of being the one row type where "which tool" has to be
    # inferred from the path.
    set_job_context(tool="DOWNLOAD", tier="standard")

    if format not in ["mp3", "wav"]:
        raise HTTPException(400, "Format must be 'mp3' or 'wav'")

    if response not in ("base64", "url"):
        raise HTTPException(400, "response must be 'base64' or 'url'")

    if not is_valid_youtube_url(url):
        logger.warning(f"[DOWNLOAD] Rejected - not a recognizable YouTube URL: {url}")
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    started = time.monotonic()
    video_id = extract_video_id(url)

    want_url = response == "url" and bool(video_id)
    if response == "url" and not video_id:
        logger.warning(
            f"[DOWNLOAD] response=url requested but no video_id could be extracted "
            f"from {url} - falling back to base64."
        )

    if video_id:
        if want_url:
            # Path-based lookup: no bytes read, nothing to encode. This is
            # the single biggest win in the change - a cache HIT on a
            # 420 MB WAV used to allocate ~1.5 GB just to answer.
            cached_path, cached_title = await run_blocking(get_cached_path, video_id, format)
            if cached_path:
                try:
                    cached_size = os.path.getsize(cached_path)
                except OSError:
                    # Evicted between the lookup and this stat. The link
                    # is still worth returning - the GET does its own
                    # lookup and will 404 honestly if it is really gone -
                    # so don't fail a request over a display number.
                    cached_size = None
                logger.info(
                    f"[CACHE] HIT '{cached_title}' ({format}) url-mode "
                    f"in {time.monotonic() - started:.2f}s"
                )
                record_result("/download", True)
                return JSONResponse(_url_payload(video_id, format, cached_title, cached_size))
        else:
            try:
                cached_audio, cached_title = await run_blocking(get_cached_audio, video_id, format)
            except Exception as cache_err:
                logger.warning(f"[CACHE] Lookup failed (non-fatal, downloading fresh): {cache_err}")
                cached_audio, cached_title = None, None

            if cached_audio:
                cached_b64 = base64.b64encode(cached_audio).decode('utf-8')
                logger.info(
                    f"[CACHE] HIT '{cached_title}' ({format}) {_mb(len(cached_audio))} "
                    f"in {time.monotonic() - started:.2f}s"
                )
                record_result("/download", True)
                return JSONResponse({"title": cached_title or "Unknown", "audio": cached_b64, "format": format})

    temp_id = str(uuid.uuid4())
    temp_path = os.path.join(UPLOAD_DIR, f"{temp_id}.%(ext)s")
    output_file = os.path.join(UPLOAD_DIR, f"{temp_id}.{format}")

    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': temp_path,
        'quiet': False,
        'verbose': True,
        'noplaylist': True,
        'ffmpeg_location': '/usr/bin/ffmpeg',
        # Equivalent of yt-dlp's --force-ipv4 CLI flag. VPSDime assigned
        # this VPS a real, working global IPv6 address on 2026-08-03 (see
        # ticket - `curl -6` on the HOST now succeeds), but that
        # connectivity does NOT reach this Docker container: Docker's
        # default bridge networking does not forward IPv6 into containers
        # unless explicitly configured (fixed-cidr-v6 in
        # /etc/docker/daemon.json). Confirmed via
        # `docker exec audioforges-api curl -6 https://ipv6.google.com`
        # failing with "Network is unreachable" on every resolved address,
        # the same failure the HOST used to show before VPSDime's fix.
        # Until Docker's IPv6 networking is separately configured (a
        # bigger infra change, tracked separately - not done here),
        # yt-dlp inside this container still has no usable IPv6 path, so
        # googlevideo.com edges that are IPv6-only remain unreachable.
        # Pinning source_address to 0.0.0.0 keeps every connection this
        # YoutubeDL instance opens on IPv4, avoiding that dead path.
        'source_address': '0.0.0.0',
        # Default connect timeout is 20s (yt-dlp's own default). A
        # connect-timeout to a dead/unreachable googlevideo edge is
        # guaranteed to fail identically on every retry against the SAME
        # IP (see is_cdn_connect_timeout_error in youtube.py) - lowering
        # this means a doomed attempt fails in ~10s instead of ~20s,
        # cutting the worst-case all-attempts-failed wall time roughly in
        # half before the proxy tier (or the 503 branch below) takes
        # over. 10s is still generous for a genuinely slow-but-working
        # connection; it is not so low that it risks false-failing normal
        # requests under typical latency.
        'socket_timeout': 20,
        'extractor_args': {
            'youtubepot-bgutilscript': {
                'script_path': ['/root/bgutil-ytdlp-pot-provider/server/build/generate_once.js']
            },
            'youtube': {
                'player_client': ['android_vr', 'android', 'web'],
            },
        },
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': format,
            'preferredquality': '192',
        }],
        'remote_components': ['ejs:github'],
        'logger': ytdlp_alert_logger,
        'progress_hooks': [make_progress_hook(video_id or url)],
    }

    proxy_url = os.environ.get('YT_PROXY_URL')
    available_accounts = get_cookie_accounts()
    logger.info(
        f"[COOKIES] accounts_available={len(available_accounts)} "
        f"[PROXY] configured={bool(proxy_url)} circuit_breaker={'OPEN' if not proxy_available() else 'CLOSED'} "
        f"url={url}"
    )

    # download_worker.py runs in a separate process and reconstructs its
    # own logger/hooks internally - neither survives a JSON boundary, so
    # strip them here rather than pass them across.
    serializable_ydl_opts = {
        k: v for k, v in ydl_opts.items()
        if k not in ("logger", "progress_hooks")
    }

    await acquire_slot_or_503(_download_semaphore, "download")

    audio_data = None
    succeeded = False
    # put_cached_file() MOVES the download into the cache, so on that path
    # there is nothing left at output_file for the `finally` to clean up.
    # Tracking the path in its own variable and clearing it after a
    # successful move keeps cleanup correct without depending on how
    # cleanup_file() happens to handle a path that no longer exists.
    pending_cleanup = output_file
    try:
        result = await run_in_killable_subprocess(
            serializable_ydl_opts, url, proxy_url,
            DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS, temp_id,
            progress_label=video_id or url,
            request_id=get_current_request_id(),
        )

        if result["ok"]:
            title = result["title"]
        elif result["kind"] == "too_long":
            logger.warning(f"[DOWNLOAD] Rejected - video too long: {result['error']}")
            raise HTTPException(400, result["error"])
        elif result["kind"] == "timeout":
            logger.warning(
                f"[DOWNLOAD] Wall-clock timeout ({DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS}s) - "
                f"process group killed, slot freed: {url}"
            )
            raise HTTPException(503, "This download is taking too long. Please try again.")
        elif result["kind"] == "crashed":
            logger.error(f"[DOWNLOAD] Worker process crashed: {result['error']}")
            raise HTTPException(
                500,
                "Something went wrong while downloading this video. Please try again."
            )
        else:
            error_text = result["error"]

            # Each branch below maps a yt-dlp failure onto the status code
            # that actually describes it. This matters more than it looks:
            # a 404 tells the frontend "this video is gone, don't retry",
            # while a 503 means "try again shortly" - collapsing them all
            # into 500 (as the generic fallback does) is what turns a
            # clear problem into an unexplained one.
            if is_permanent_error(error_text):
                logger.warning(f"[DOWNLOAD] Permanent error for {url}: {error_text}")
                raise HTTPException(
                    404,
                    "This video is unavailable - it may have been deleted, made private, "
                    "or removed for copyright reasons. Please try a different video."
                )

            if is_geo_restricted_error(error_text):
                logger.warning(f"[DOWNLOAD] Geo-restricted: {url}")
                raise HTTPException(
                    451,
                    "This video is restricted by the uploader to specific countries and "
                    "isn't available from our server's location. This isn't something we "
                    "can fix on our end for this particular video - try a different one."
                )

            if is_age_restricted_error(error_text):
                logger.warning(f"[DOWNLOAD] Age-restricted: {url}")
                raise HTTPException(
                    403,
                    "This video is age-restricted by YouTube and requires a verified "
                    "account to view. We're not able to download age-restricted content "
                    "at this time - try a different video."
                )

            if is_members_only_error(error_text):
                logger.warning(f"[DOWNLOAD] Members-only: {url}")
                raise HTTPException(
                    403,
                    "This video is exclusive to that channel's paid members and isn't "
                    "publicly downloadable - try a different video."
                )

            if is_music_premium_error(error_text):
                logger.warning(f"[DOWNLOAD] Music Premium required: {url}")
                raise HTTPException(
                    403,
                    "This track is exclusive to YouTube Music Premium subscribers and "
                    "isn't publicly downloadable - try a different video."
                )

            if is_not_yet_live_error(error_text):
                logger.warning(f"[DOWNLOAD] Not yet live: {url}")
                raise HTTPException(
                    409,
                    "This video is a scheduled premiere or live stream that hasn't "
                    "started yet - try again once it's live, or try a different video."
                )

            if is_bot_check_error(error_text):
                logger.error(f"[DOWNLOAD] Bot verification / format restriction: {url}")
                raise HTTPException(
                    503,
                    "This video is temporarily unavailable for download because YouTube is "
                    "requiring bot verification or is restricting available formats for this client. "
                    "Please try again in a few minutes."
                )

            if is_cdn_connect_timeout_error(error_text) or is_cdn_read_timeout_error(error_text):
                # A connect-timeout to a specific googlevideo media edge.
                # should_use_proxy() DOES escalate this to the proxy tier
                # (see its docstring in youtube.py for the back-and-forth
                # on that decision and the evidence that settled it) - so
                # reaching this branch means direct failed AND the proxy
                # attempt either wasn't available or also failed. The
                # direct-path degradation breaker (cdn_breaker_status(),
                # in admin.py's /admin/status; config.py's CDN_DEGRADED_*
                # knobs) separately tracks repeated direct-path timeouts
                # and, once enough cluster together, skips the doomed
                # ~10s direct attempt entirely for a cooldown window
                # rather than paying for it on every request. Either way
                # this is transient network flakiness, not a bug in this
                # app, so it gets a 503 ("try again") rather than a
                # generic 500.
                logger.warning(f"[DOWNLOAD] CDN edge timeout: {url}: {error_text}")
                raise HTTPException(
                    503,
                    "Couldn't reach YouTube's servers for this video. Please try again in a moment."
                )

            if (
                is_page_reload_error(error_text)
                or is_media_forbidden_error(error_text)
                or is_format_unavailable_error(error_text)
            ):
                # Every client set on the ladder failed. These are all
                # YouTube-side extraction changes (SABR stripping format
                # URLs, a player JS challenge the current yt-dlp can't
                # solve), not bugs here and not anything the user did.
                # 503 rather than 500: it is genuinely transient - the
                # same video usually works again once YouTube rotates its
                # experiment or yt-dlp ships a fix.
                logger.error(
                    f"[DOWNLOAD] All client sets exhausted for {url}: {error_text}"
                )
                raise HTTPException(
                    503,
                    "YouTube is currently blocking downloads for this video. This is "
                    "usually temporary - please try again later, or try a different video."
                )

            # Raw yt-dlp text is logged, never returned. It leaks internals,
            # means nothing to the user, and makes a working system look
            # broken - the failure above is the same either way.
            logger.error(f"[DOWNLOAD] Failed after all attempts: {error_text}")
            raise HTTPException(
                500,
                "Something went wrong while downloading this video. Please try again, "
                "or try a different video."
            )

        if not os.path.exists(output_file):
            logger.error(f"[DOWNLOAD] Expected output missing after download: {output_file}")
            raise HTTPException(500, "Failed: audio file was not produced by the downloader")

        if want_url:
            # Move (not copy) into the cache: the downloaded file BECOMES
            # the cache entry, which is also what the new GET route
            # serves. Nothing is read into memory anywhere on this path.
            raw_size = os.path.getsize(output_file)
            cached_path = await run_blocking(put_cached_file, video_id, format, output_file, title)

            if not cached_path:
                # put_cached_file never raises, so None means the move
                # failed - and after a failed cross-filesystem move the
                # source may or may not still exist. Serving a URL would
                # 404 and falling back to base64 would need a file we can
                # no longer trust, so fail cleanly and let the user retry
                # (that retry is a fresh download, not this half-state).
                logger.error(
                    f"[DOWNLOAD] Could not move '{title}' ({format}) into the cache - "
                    f"cannot serve a URL for it."
                )
                raise HTTPException(
                    500,
                    "Something went wrong while preparing this download. Please try again."
                )

            # The move consumed output_file; nothing left to clean up.
            pending_cleanup = None

            logger.info(
                f"[DOWNLOAD] COMPLETE '{title}' ({format}) {_mb(raw_size)} url-mode "
                f"in {time.monotonic() - started:.1f}s"
            )
            succeeded = True
            # raw_size was read off the file above, before the move.
            return JSONResponse(_url_payload(video_id, format, title, raw_size))

        audio_bytes = await run_blocking(_read_file_bytes, output_file)
        audio_data = base64.b64encode(audio_bytes).decode('utf-8')

        if video_id:
            try:
                await run_blocking(put_cached_audio, video_id, format, audio_bytes, title)
            except Exception as cache_err:
                logger.warning(f"[CACHE] Save failed (non-fatal): {cache_err}")

        raw_size = len(audio_bytes)
        del audio_bytes

        logger.info(
            f"[DOWNLOAD] COMPLETE '{title}' ({format}) {_mb(raw_size)} "
            f"in {time.monotonic() - started:.1f}s"
        )

        succeeded = True
        return JSONResponse({"title": title, "audio": audio_data, "format": format})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[DOWNLOAD] Unexpected error: {e}", exc_info=True)
        raise HTTPException(
            500,
            "Something went wrong while downloading this video. Please try again."
        )
    finally:
        if pending_cleanup:
            cleanup_file(pending_cleanup)
        if audio_data is not None:
            del audio_data
        release_memory_to_os()
        _download_semaphore.release()
        record_result("/download", succeeded)


@router.get("/download/file/{video_id}.{fmt}")
async def download_audio_file(
    video_id: str,
    fmt: str,
    token: str = Query(...),
    disposition: str = Query("attachment"),
):
    """
    Streams a cached download straight to the browser.

    FileResponse streams the file in chunks and handles Range requests
    natively, so the server never holds it in memory and a dropped mobile
    connection resumes instead of restarting a 400 MB transfer. That is
    the entire point of this route - see the 2026-08-30 note in this
    module's docstring.

    NOT rate-limited by check_rate_limit. The POST that produced the
    token already passed the limiter, and a resumed Range request would
    otherwise count as a second call and could 429 a download that is
    already half finished. The signature and the TTL are what bound abuse
    here.
    """
    if fmt not in ("mp3", "wav"):
        raise HTTPException(400, "Format must be 'mp3' or 'wav'")

    if disposition not in ("attachment", "inline"):
        raise HTTPException(400, "disposition must be 'attachment' or 'inline'")

    if not _verify_download_token(video_id, fmt, token):
        # Deliberately identical wording for a bad signature and an
        # expired one, and deliberately checked BEFORE the cache lookup:
        # a different response for "valid token, no such entry" would
        # turn this route into an oracle for which videos are cached,
        # which is exactly the enumeration the signature exists to stop.
        logger.warning(
            f"[DOWNLOAD] Rejected file request with an invalid/expired token: {video_id}.{fmt}"
        )
        raise HTTPException(
            403,
            "This download link is invalid or has expired. Please request the file again."
        )

    path, title = await run_blocking(get_cached_path, video_id, fmt)
    if not path:
        # 404 rather than 410: from the client's side there is nothing to
        # distinguish "your link outlived the entry" from "LRU eviction
        # reclaimed the space", and the fix is identical either way -
        # POST /download again.
        logger.info(f"[DOWNLOAD] File no longer cached: {video_id}.{fmt}")
        raise HTTPException(
            404,
            "This file is no longer available. Please request the download again."
        )

    logger.info(
        f"[DOWNLOAD] Serving '{title}' ({fmt}) from cache: {video_id} ({disposition})"
    )
    return FileResponse(
        path,
        media_type=_MEDIA_TYPES[fmt],
        filename=_safe_filename(title, fmt),
        # ATTACHMENT IS THE DEFAULT AND HAS TO BE. <a download> is IGNORED
        # on cross-origin URLs, and api.audioforges.com is cross-origin
        # from the site - so this header is the only thing that makes a
        # click save the file rather than navigate to it. Dropping it
        # would break every download button.
        #
        # inline is opt-in for the preview player, which wants the same
        # bytes from the same route without triggering a save. Left OUT
        # of the signed payload deliberately: it changes presentation,
        # not access, so a user flipping it gains nothing they did not
        # already have with a valid token.
        content_disposition_type=disposition,
    )


def _read_file_bytes(path: str) -> bytes:
    """Blocking read, dispatched via run_blocking from /download.

    Reading a finished download can mean pulling tens of megabytes off
    disk; doing that inline in the async handler blocks every other
    connection for its duration, which on a single worker is the whole
    server.

    Only the legacy base64 path uses this now - url mode never reads the
    file at all."""
    with open(path, "rb") as f:
        return f.read()


# ============================================================
# /youtube/* - Paste a URL, get the processed result, skipping the
# manual download-then-reupload step.
#
# Each of these chains TWO of the app's heaviest operations in one
# background job: a YouTube download, then either analysis or Demucs
# separation. The download slot is RELEASED before the processing slot
# is acquired - the two are never held at once, so a slow separation
# doesn't also tie up a download slot for its whole duration.
#
# Each POST route below carries its OWN rate-limit constants (2026-08-21)
# rather than the single shared YOUTUBE_CHAIN_RATE_LIMIT_* pair they used
# to share - see the WHAT CHANGED note at the top of this file for why
# the shared number was wrong even though the per-IP buckets were already
# separate.
#
# The two HQ routes additionally carry a credit charge (2026-08-25) - see
# the CREDITS note at the top of this file for the two structural
# differences from routes/separation.py that make this harder here.
# ============================================================

async def _chain_download(job_id: str, url: str, tool: str, metric: str) -> Optional[tuple]:
    """
    Shared first half of every /youtube/* job: acquire a download slot,
    fetch the audio, release the slot. Returns (file_path, title), or
    None if it failed (in which case the job is already marked and the
    metric already recorded).

    The acquire is INSIDE the try. acquire_slot_or_503() raises
    HTTPException when the queue wait times out - and in a background
    task there is no HTTP layer to catch that, so before this change it
    propagated straight out of the task, skipping every mark_failed()
    below it and leaving the job stuck on "processing" forever with no
    log line explaining why. That single detail accounted for a whole
    class of "it just spun forever" reports.

    The release lives in `finally` guarded by a flag, so the slot is
    returned exactly once whether the download succeeded, failed, or the
    acquire itself blew up.

    tool/tier are NOT set here - by the time this runs, the calling route
    (youtube_separate_route, youtube_analyze_route, etc.) has already
    called set_job_context() before spawning this task, so the tag is
    already inherited. Setting it again here would just be a second call
    site for the same information.

    NOTE ON CREDITS: this function does NOT settle or refund. It returns
    None on failure and the CALLER decides - see
    _run_youtube_separation, which refunds on that branch. Putting it
    here would refund for /youtube/analyze too, which is never charged.
    """
    acquired = False
    try:
        await acquire_slot_or_503(_download_semaphore, f"{tool.lower()}-download")
        acquired = True
        started = time.monotonic()
        # download_audio_to_file is now `async def` and handles its own
        # wall-clock timeout internally via run_in_killable_subprocess -
        # the outer asyncio.wait_for/run_blocking wrapping is gone since
        # a killed process group needs no further guarding here.
        file_path, title = await download_audio_to_file(url, job_id)
        logger.info(
            f"[{tool}] job={job_id} downloaded '{title}' in {time.monotonic() - started:.1f}s"
        )
        return file_path, title

    except ChainDownloadError as e:
        mark_failed(job_id, str(e))
        logger.warning(f"[{tool}] job={job_id} download FAILED: {e}")
        record_result(metric, False)
        return None

    except HTTPException as e:
        # Almost always the queue-wait 503 from acquire_slot_or_503.
        detail = e.detail if isinstance(e.detail, str) else "The server was too busy."
        mark_failed(job_id, detail)
        logger.warning(f"[{tool}] job={job_id} download rejected: {detail}")
        record_result(metric, False)
        return None

    except Exception as e:
        mark_failed(job_id, "Download failed unexpectedly.")
        logger.error(f"[{tool}] job={job_id} download FAILED (unexpected): {e}", exc_info=True)
        record_result(metric, False)
        return None

    finally:
        if acquired:
            _download_semaphore.release()


async def _run_youtube_analyze(job_id: str, url: str):
    """Download, then key/BPM analysis. Two different semaphores, held
    one at a time."""
    downloaded = await _chain_download(job_id, url, "YOUTUBE_ANALYZE", "/youtube/analyze")
    if downloaded is None:
        fail_if_unfinished(job_id, "Download failed.")
        return

    file_path, title = downloaded
    analysis_path = file_path
    succeeded = False
    acquired = False
    started = time.monotonic()

    try:
        await acquire_slot_or_503(_analysis_semaphore, "youtube-analyze")
        acquired = True

        if ANALYSIS_MAX_SECONDS is not None:
            analysis_path = await run_blocking(trim_audio_for_analysis, file_path, ANALYSIS_MAX_SECONDS)

        key, scale, key_conf, bpm, bpm_conf, audio_array, essentia_sr = await run_blocking(
            detect_key_bpm_essentia, analysis_path
        )
        key, scale, key_conf, bpm, bpm_conf, agreement = await run_blocking(
            cross_check_with_librosa, audio_array, essentia_sr, key, scale, key_conf, bpm, bpm_conf
        )
        del audio_array

        result = {
            "key": f"{key} {scale}",
            "camelot": get_camelot(key, scale),
            "bpm": bpm,
            "confidence": int(min(0.99, key_conf) * 100),
            "bpm_confidence": min(99, bpm_conf),
            "cross_check": agreement,
        }
        mark_data_complete(job_id, title, result)
        succeeded = True
        logger.info(
            f"[YOUTUBE_ANALYZE] job={job_id} COMPLETE in {time.monotonic() - started:.1f}s: "
            f"{result['key']} / {result['camelot']} / {result['bpm']} BPM"
        )

    except HTTPException as e:
        detail = e.detail if isinstance(e.detail, str) else "The server was too busy."
        mark_failed(job_id, detail)
        logger.warning(f"[YOUTUBE_ANALYZE] job={job_id} rejected: {detail}")

    except asyncio.CancelledError:
        mark_failed(job_id, "The server restarted while this job was running.")
        logger.warning(f"[YOUTUBE_ANALYZE] job={job_id} CANCELLED (shutdown)")
        raise

    except Exception as e:
        mark_failed(job_id, "Analysis failed unexpectedly.")
        logger.error(f"[YOUTUBE_ANALYZE] job={job_id} FAILED (unexpected): {e}", exc_info=True)

    finally:
        fail_if_unfinished(job_id, "Analysis failed unexpectedly.")
        cleanup_file(file_path)
        if analysis_path != file_path:
            cleanup_file(analysis_path)
        if acquired:
            _analysis_semaphore.release()
        release_memory_to_os()
        record_result("/youtube/analyze", succeeded)


async def _run_youtube_separation(
    job_id: str,
    url: str,
    *,
    stems: bool,
    model: str,
    overlap: float,
    timeout_seconds: int,
    max_duration_seconds: int,
    hq: bool = False,
):
    """Download, then Demucs. One function for all four YouTube
    separation routes (/youtube/separate, /youtube/separate-hq,
    /youtube/stems, /youtube/stems-hq) - they differ only in which worker
    runs, how the result is stored, and which quality knobs are used.

    The knobs are passed in rather than read from config here, matching
    _queue_separation() in separation.py: they're resolved by the caller
    at SUBMISSION time, so a config change (or the HQ kill switch being
    flipped off) can never retroactively alter a job that's already
    queued. It runs with the settings it was accepted under.

    tool/tier: not set here either, same reasoning as _chain_download's
    docstring above - the calling route already set it before
    spawn_background_task() spawned this function, so it's already
    inherited by the time this runs.
    """
    suffix = "_HQ" if hq else ""
    tool = ("YOUTUBE_STEMS" if stems else "YOUTUBE_SEPARATE") + suffix
    metric = ("/youtube/stems" if stems else "/youtube/separate") + ("-hq" if hq else "")

    downloaded = await _chain_download(job_id, url, tool, metric)
    if downloaded is None:
        # THE REFUND HOLE THIS CLOSES: _chain_download returns early here,
        # so _run_tool_job below is never reached - and _run_tool_job's
        # `finally` is where every other tool's charge gets settled or
        # refunded. Without this line, a paid YouTube job whose DOWNLOAD
        # failed (a private video, a bot check, a queue-wait 503) would
        # hold the credit until the 90-minute sweeper found it.
        #
        # Unconditional and a no-op for the two standard routes, which
        # have no charge row - same property that lets the call in
        # _run_tool_job stay unconditional.
        settle_or_refund(job_id, False, reason=f"{tool.lower()}_download_failed")
        fail_if_unfinished(job_id, "Download failed.")
        return

    file_path, title = downloaded

    # Now that the file exists, record what we're actually about to
    # process. The submit path could not do this - there was no file yet.
    if hq:
        metering.record_input_duration_safe(job_id, file_path)

    if stems:
        # No run_blocking() - run_stem_separation()/run_separation() are
        # async (they await an HTTP call to the RunPod GPU worker), not
        # blocking local subprocess calls. Same reasoning as
        # separation.py's own _queue_separation().
        work = lambda: run_stem_separation(
            file_path, job_id, model, overlap, timeout_seconds, max_duration_seconds,
        )
        on_success = lambda result: mark_stems_complete(job_id, title, result)
        success_detail = lambda result: f"{len(result)} stems"
        generic_error = "Stem separation failed unexpectedly."
    else:
        work = lambda: run_separation(
            file_path, job_id, model, overlap, timeout_seconds, max_duration_seconds,
        )
        on_success = lambda paths: mark_complete(job_id, title, paths[0], paths[1])
        success_detail = None
        generic_error = "Separation failed unexpectedly."

    # _run_tool_job's `finally` calls settle_or_refund - so a track that
    # turns out to exceed MAX_SEPARATION_DURATION_SECONDS_HQ, or any
    # other GPU-side failure, returns the credit immediately rather than
    # via the sweeper.
    await _run_tool_job(
        tool=tool,
        metric=metric,
        job_id=job_id,
        semaphore=_separation_semaphore,
        work=work,
        on_success=on_success,
        generic_error=generic_error,
        cleanup_paths=[file_path],
        success_detail=success_detail,
        # False: see separation.py's _queue_separation equivalent comment
        # - the real billed figure is recorded inside separation.py.
        gpu_billed=False,
    )


@router.post(
    "/youtube/analyze",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=YOUTUBE_ANALYZE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=YOUTUBE_ANALYZE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_analyze_route(url: str = Form(...)):
    """Poll GET /youtube/analyze/status/{job_id}, then .../result.

    The loosest of the five /youtube/* limits, and the only one that
    should be: this is the one chained tool that never touches the
    single separation slot. After the download it holds a slot on
    _analysis_semaphore (4 of them) for roughly 30 seconds of Essentia
    work on an ANALYSIS_MAX_SECONDS trim, not minutes of Demucs.
    """
    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    # Set BEFORE create_task(), not inside _run_youtube_analyze - see the
    # WHAT CHANGED note at the top of this file for why the timing here
    # matters: create_task() copies the context at the moment it's
    # called, and this is also what tags the POST's own HTTP log row.
    set_job_context(tool="YOUTUBE_ANALYZE", tier="standard")

    job_id = create_job(job_type="youtube_analyze", ttl_seconds=YOUTUBE_ANALYZE_JOB_TTL_SECONDS)

    remember_job_tags(job_id)
    spawn_background_task(_run_youtube_analyze(job_id, url))

    logger.info(f"[YOUTUBE_ANALYZE] job={job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/youtube/analyze/status/{job_id}")
async def youtube_analyze_status(job_id: str):
    return _tool_status(job_id, "youtube_analyze")


@router.get("/youtube/analyze/result/{job_id}")
async def youtube_analyze_result(job_id: str):
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_analyze":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    result = job.get("result_data")
    if not result:
        raise HTTPException(404, "Result not found (it may have expired).")
    return JSONResponse(result)


@router.post(
    "/youtube/separate",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=YOUTUBE_SEPARATE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=YOUTUBE_SEPARATE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_separate_route(url: str = Form(...)):
    """Downloads then runs standard-tier vocal/instrumental separation.
    Stem paths are stored the same way /separate stores them.

    Its own limit (not shared with /youtube/analyze any more) because
    each accepted job here holds the SINGLE _separation_semaphore slot
    for 3-5 minutes, which every other separation job on the box then
    waits behind.

    FREE FOREVER. No credits identity, no guard - like /separate, this
    route has no code path that reaches the ledger regardless of config.
    """
    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    set_job_context(tool="YOUTUBE_SEPARATE", tier="standard")

    _reject_if_separation_queue_full()

    job_id = create_job(job_type="youtube_separate")

    remember_job_tags(job_id)
    spawn_background_task(_run_youtube_separation(
        job_id, url,
        stems=False,
        model=SEPARATION_MODEL,
        overlap=SEPARATION_OVERLAP,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS,
    ))

    logger.info(f"[YOUTUBE_SEPARATE] job={job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.post(
    "/youtube/separate-hq",
    dependencies=[Depends(tiered_rate_limit(
        "youtube/separate-hq",
        free_max=YOUTUBE_SEPARATE_HQ_RATE_LIMIT_MAX_REQUESTS,
        free_window=YOUTUBE_SEPARATE_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_separate_hq_route(
    url: str = Form(...),
    identity: Identity = Depends(paywall.get_identity),
):
    """
    High-quality YouTube vocal/instrumental separation - htdemucs_ft at
    raised overlap, same knobs as /separate-hq, with a download bolted
    on the front.

    Deliberately uses job_type="youtube_separate" (not a separate type):
    /separate and /separate-hq already share job_type="separation" for
    the same reason, so every existing status/preview/download route
    works for HQ jobs without a single change. The tier affects HOW the
    job runs, not what shape the result is - and now that the DB has a
    real `tier` column, that same distinction is queryable directly
    instead of needing to be reconstructed from job_type.

    A separate route rather than a `quality` form field because
    rate-limit dependencies are evaluated before the request body is
    read - a Depends() cannot see a Form value, so per-tier limits need
    per-tier routes.

    COSTS ONE CREDIT when PAYWALL_TOOL_YOUTUBE_SEPARATE_HQ_ENABLED is on.

    input_seconds=None is not an oversight: there is no file yet, so
    there is no duration to probe. paywall.decide() treats unknown
    duration on a metered tool as billable, which is the only safe
    direction - failing open would give away the expensive tier to
    anyone whose duration happened to be unreadable. If the track later
    turns out to exceed the 6-minute HQ cap, _run_tool_job's `finally`
    refunds the credit immediately.
    """
    if not SEPARATION_HQ_ENABLED:
        raise HTTPException(
            503,
            "High quality separation is temporarily unavailable due to server load. "
            "Please use standard separation."
        )

    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    set_job_context(tool="YOUTUBE_SEPARATE", tier="hq")

    # Capacity before payment: a 503 must never cost a credit.
    _reject_if_separation_queue_full()

    job_id = create_job(job_type="youtube_separate")
    remember_job_tags(job_id)

    metering.record_job_created(
        job_id=job_id, tool="youtube/separate-hq",
        subject_id=identity.subject_id, account_id=identity.account_id,
        ip_hash=identity.ip_hash,
    )

    async with paywall.guard(
        identity, job_id=job_id, tool="youtube/separate-hq", input_seconds=None
    ) as charge:
        spawn_background_task(_run_youtube_separation(
            job_id, url,
            stems=False,
            model=SEPARATION_MODEL_HQ,
            overlap=SEPARATION_OVERLAP_HQ,
            timeout_seconds=DEMUCS_TIMEOUT_SECONDS_HQ,
            max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS_HQ,
            hq=True,
        ))

    metering.record_job_created(
        job_id=job_id, tool="youtube/separate-hq",
        subject_id=identity.subject_id, account_id=identity.account_id,
        ip_hash=identity.ip_hash, charge_type=charge.charge_type,
    )

    logger.info(
        f"[YOUTUBE_SEPARATE_HQ] job={job_id} queued for {url} charged={charge.charge_type}"
    )
    payload = {"job_id": job_id, "status": "processing"}
    if charge.charge_type != "none":
        payload["billing"] = {
            "charged": charge.charge_type,
            "balance": charge.balance_after,
            "free_remaining": charge.free_remaining_after,
        }
    return JSONResponse(payload)


@router.get("/youtube/separate/status/{job_id}")
async def youtube_separate_status(job_id: str):
    return _tool_status(job_id, "youtube_separate")


def _resolve_youtube_separate_path(job_id: str, stem: str) -> str:
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_separate":
        raise HTTPException(404, "Job not found (it may have expired).")
    if stem not in ("vocals", "instrumental"):
        raise HTTPException(400, "stem must be 'vocals' or 'instrumental'")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["vocals_path"] if stem == "vocals" else job["instrumental_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Stem file not found (it may have expired).")
    return path


@router.get("/youtube/separate/preview/{job_id}")
async def youtube_separate_preview(job_id: str, stem: str = Query(...)):
    path = _resolve_youtube_separate_path(job_id, stem)
    return FileResponse(path, media_type="audio/wav")


@router.get("/youtube/separate/download/{job_id}")
async def youtube_separate_download(job_id: str, stem: str = Query(...)):
    path = _resolve_youtube_separate_path(job_id, stem)
    return FileResponse(path, media_type="audio/wav", filename=f"{stem}.wav")


@router.post(
    "/youtube/stems",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=YOUTUBE_STEMS_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=YOUTUBE_STEMS_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_stems_route(url: str = Form(...)):
    """Downloads then runs standard-tier full 4-stem separation.

    Same Demucs cost as /youtube/separate (same model, same run - only
    the output files differ), so it gets the same numbers. Kept as its
    own constant rather than importing /youtube/separate's so that stays
    a decision, not an assumption baked into a shared name.

    FREE FOREVER, same as /youtube/separate.
    """
    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    set_job_context(tool="YOUTUBE_STEMS", tier="standard")

    _reject_if_separation_queue_full()

    job_id = create_job(job_type="youtube_stems")

    remember_job_tags(job_id)
    spawn_background_task(_run_youtube_separation(
        job_id, url,
        stems=True,
        model=SEPARATION_MODEL,
        overlap=SEPARATION_OVERLAP,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS,
    ))

    logger.info(f"[YOUTUBE_STEMS] job={job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.post(
    "/youtube/stems-hq",
    dependencies=[Depends(tiered_rate_limit(
        "youtube/stems-hq",
        free_max=YOUTUBE_STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
        free_window=YOUTUBE_STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_stems_hq_route(
    url: str = Form(...),
    identity: Identity = Depends(paywall.get_identity),
):
    """
    High-quality YouTube 4-stem separation - same knobs and kill switch
    as /stems-hq. Shares job_type="youtube_stems" with the standard
    tier so the existing status/preview/download routes need no changes;
    see youtube_separate_hq_route() for the full reasoning.

    COSTS ONE CREDIT when PAYWALL_TOOL_YOUTUBE_STEMS_HQ_ENABLED is on.
    """
    if not SEPARATION_HQ_ENABLED:
        raise HTTPException(
            503,
            "High quality separation is temporarily unavailable due to server load. "
            "Please use standard stem separation."
        )

    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    set_job_context(tool="YOUTUBE_STEMS", tier="hq")

    _reject_if_separation_queue_full()

    job_id = create_job(job_type="youtube_stems")
    remember_job_tags(job_id)

    metering.record_job_created(
        job_id=job_id, tool="youtube/stems-hq",
        subject_id=identity.subject_id, account_id=identity.account_id,
        ip_hash=identity.ip_hash,
    )

    async with paywall.guard(
        identity, job_id=job_id, tool="youtube/stems-hq", input_seconds=None
    ) as charge:
        spawn_background_task(_run_youtube_separation(
            job_id, url,
            stems=True,
            model=SEPARATION_MODEL_HQ,
            overlap=SEPARATION_OVERLAP_HQ,
            timeout_seconds=DEMUCS_TIMEOUT_SECONDS_HQ,
            max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS_HQ,
            hq=True,
        ))

    metering.record_job_created(
        job_id=job_id, tool="youtube/stems-hq",
        subject_id=identity.subject_id, account_id=identity.account_id,
        ip_hash=identity.ip_hash, charge_type=charge.charge_type,
    )

    logger.info(
        f"[YOUTUBE_STEMS_HQ] job={job_id} queued for {url} charged={charge.charge_type}"
    )
    payload = {"job_id": job_id, "status": "processing"}
    if charge.charge_type != "none":
        payload["billing"] = {
            "charged": charge.charge_type,
            "balance": charge.balance_after,
            "free_remaining": charge.free_remaining_after,
        }
    return JSONResponse(payload)


@router.get("/youtube/stems/status/{job_id}")
async def youtube_stems_status(job_id: str):
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_stems":
        raise HTTPException(404, "Job not found (it may have expired).")
    stems = job.get("stems") or {}
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
        "stems": sorted(stems.keys()),
        "elapsed_seconds": round(time.time() - job["created_at"], 1),
    }


def _resolve_youtube_stems_file(job_id: str, stem: str) -> str:
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_stems":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    stems = job.get("stems") or {}
    if stem not in stems:
        raise HTTPException(400, f"stem must be one of: {', '.join(sorted(stems.keys()))}")
    path = stems[stem]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Stem file not found (it may have expired).")
    return path


@router.get("/youtube/stems/preview/{job_id}")
async def youtube_stems_preview(job_id: str, stem: str = Query(...)):
    path = _resolve_youtube_stems_file(job_id, stem)
    return FileResponse(path, media_type="audio/wav")


@router.get("/youtube/stems/download/{job_id}")
async def youtube_stems_download(job_id: str, stem: str = Query(...)):
    path = _resolve_youtube_stems_file(job_id, stem)
    return FileResponse(path, media_type="audio/wav", filename=f"{stem}.wav")