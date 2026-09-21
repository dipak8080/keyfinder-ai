"""
tiktok/core.py - Everything the /tiktok routes need for talking to yt_dlp.

WHY THIS IS SO MUCH SIMPLER THAN youtube.py: TikTok does not fight back
the way YouTube does. Every finding below was confirmed by direct CLI
test on 2026-08-18, on this VPS, against real TikTok URLs:

  - No bot-check. Extraction succeeds from the datacenter IP.
  - No PO token, no JS challenge, no player-client selection.
  - No cookie accounts needed for public posts.
  - THE PROXY DOES NOT HELP. The one IP-shaped error TikTok returns
    ("Your IP address is blocked from accessing this post") reproduces
    IDENTICALLY through the residential proxy. A different exit IP
    producing an identical failure is proof the exit IP was never the
    variable - the same reasoning youtube.py's is_format_unavailable_error
    docstring uses. So there is NO proxy tier here at all, and TikTok
    downloads cost nothing beyond bandwidth.

So: no client ladder, no cookie rotation, no circuit breakers, no split
tunnel. Resisting the urge to copy that machinery over is the single
most important design decision in this file. Every one of those exists
in youtube.py to solve a problem TikTok does not have, and each one is
a thing that can break at 3am.

IMPERSONATION IS NOW REQUIRED (2026-09). TikTok returns a non-media
response to un-impersonated requests; yt-dlp downloads it and ffprobe
then fails in postprocessing with "unable to obtain file audio codec".
The fix has TWO parts and needs BOTH: curl_cffi installed (the
[curl-cffi] extra in requirements.txt) AND an explicit impersonate
target set in _base_opts. Installing curl_cffi alone does NOT make
yt-dlp use it - the target must be passed in opts. The value lives in
config.py as TIKTOK_IMPERSONATE_TARGET; change it there if a given
fingerprint stops working.

(Historical note: an earlier revision of this file claimed curl_cffi
BROKE TikTok. That was observed with curl_cffi present but NO explicit
target, where yt-dlp's auto-selection misbehaved. Setting the target
explicitly is the controlled path and is what this file now does.)
"""
import os
import re
import time
import subprocess
from typing import Optional, Tuple

import yt_dlp
from yt_dlp.networking.impersonate import ImpersonateTarget

from config import (
    logger,
    MAX_TIKTOK_DURATION_SECONDS,
    TIKTOK_MP3_BITRATE as MP3_BITRATE,
    TIKTOK_MAX_ATTEMPTS as MAX_ATTEMPTS,
    TIKTOK_BASE_BACKOFF_SECONDS as BASE_BACKOFF_SECONDS,
    TIKTOK_IMPERSONATE_TARGET,
    FFMPEG_PATH as FFMPEG,
    FFPROBE_PATH as FFPROBE,
)


# ============================================================
# LIMITS
#
# Every tunable lives in config.py, per that module's own docstring
# ("Central place for every constant"). Deliberately NOT redefined here:
# NOISE_PATH_MARKERS is the cautionary tale in this codebase - it
# existed as three separately maintained copies that silently drifted
# until two of them disagreed about what counted as noise. One
# definition, every consumer importing it, is how that stays fixed.
#
# The reasoning behind each VALUE is documented at its definition in
# config.py - particularly TIKTOK_MP3_BITRATE, which encodes the
# measured ~64 kbps AAC source finding and why the frontend must not
# advertise a bitrate number.
# ============================================================


class TikTokError(Exception):
    """Base for every failure this module raises deliberately.

    Carries `kind` (a stable machine-readable string the route layer
    maps to a status code) and `message` (already user-facing - the
    route layer never has to translate, and raw yt-dlp text never
    reaches a user)."""

    kind = "unknown"

    def __init__(self, message: str, kind: Optional[str] = None, debug: str = ""):
        self.message = message
        self.debug = debug
        if kind:
            self.kind = kind
        super().__init__(message)


class TikTokTooLongError(TikTokError):
    kind = "too_long"


# ============================================================
# URL HANDLING
# ============================================================

# The share formats TikTok actually hands out. Kept as a SHAPE check
# only - it exists to reject obvious junk before spending a yt-dlp call,
# a semaphore slot, or three retries on something that was never a
# TikTok URL. Real validation happens downstream when yt-dlp tries it.
_TIKTOK_URL_PATTERN = re.compile(
    r"^https?://"
    r"(?:"
    r"(?:www\.|m\.)?tiktok\.com/(?:@[\w.\-]+/(?:video|photo)/\d+|t/[\w]+/?|v/\d+)"
    r"|(?:vt|vm)\.tiktok\.com/[\w]+/?"
    r")",
    re.IGNORECASE,
)

# Photo/slideshow posts. yt-dlp CANNOT extract these - it falls back to
# the generic extractor and errors "Unsupported URL". This is a
# long-standing upstream gap, still open as of 2026-08 and reproduced
# on this box. Detecting it by URL is worth doing because it turns a
# ~3s failed extraction into a 1ms rejection with an accurate message.
_PHOTO_URL_PATTERN = re.compile(r"tiktok\.com/@[\w.\-]+/photo/\d+", re.IGNORECASE)

# Short links hide what they point at. vt.tiktok.com/ABC123 can resolve
# to EITHER a /video/ or a /photo/ URL, and there is no way to know
# which without following it - confirmed in production, where a
# vt. link resolved to a /photo/ post and failed. So photo detection
# has to happen TWICE: once on the URL here (cheap, catches direct
# links) and once on yt-dlp's error text (catches short links). Only
# doing the first would leave the most common share format unhandled.
_SHORT_URL_PATTERN = re.compile(
    r"^https?://(?:(?:vt|vm)\.tiktok\.com/|(?:www\.)?tiktok\.com/t/)", re.IGNORECASE
)

# Sound, effect, hashtag and profile pages. Short links can resolve to
# these, and yt-dlp's extractors for them need TikTok app credentials.
_NOT_VIDEO_URL_PATTERN = re.compile(
    r"tiktok\.com/(?:music/|sticker/|tag/|@[\w.\-]+/collection/|@[\w.\-]+/?(?:[?#]|$))",
    re.IGNORECASE,
)

_VIDEO_ID_PATTERN = re.compile(r"/(?:video|photo|v)/(\d+)")


# Nothing legitimate comes close to this. It exists so a multi-megabyte
# string cannot be fed to the regex engine (catastrophic backtracking is
# not possible with these patterns, but the memory copy in .strip() is
# real) and so absurd input is rejected before a semaphore slot or a
# subprocess is spent on it.
MAX_URL_LENGTH = 2048


def is_valid_tiktok_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    if len(url) > MAX_URL_LENGTH:
        return False
    stripped = url.strip()
    # Control characters have no business in a URL and are a classic
    # way to smuggle a newline into something downstream that logs or
    # shells out. yt-dlp is passed this value as an argv element, so
    # there is no shell to inject into - this is defence in depth, and
    # it also keeps the log lines readable.
    if any(ord(c) < 32 or ord(c) == 127 for c in stripped):
        return False
    return bool(_TIKTOK_URL_PATTERN.match(stripped))


def is_photo_url(url: str) -> bool:
    """True for a DIRECT photo-post URL. Short links resolving to a
    photo post are caught later by is_photo_error() instead."""
    return bool(_PHOTO_URL_PATTERN.search(url or ""))


def is_not_video_url(url: str) -> bool:
    return bool(_NOT_VIDEO_URL_PATTERN.search(url or ""))


def is_short_url(url: str) -> bool:
    return bool(_SHORT_URL_PATTERN.match((url or "").strip()))


def extract_tiktok_id(url: str) -> Optional[str]:
    """The numeric post ID, used as the cache key.

    Returns None for short links - their ID is only knowable after
    resolution. Callers must treat None as "not cacheable yet" rather
    than an error; the real ID comes back in the extracted info dict
    and can be cached then."""
    if not url:
        return None
    m = _VIDEO_ID_PATTERN.search(url)
    return m.group(1) if m else None


# ============================================================
# ERROR CLASSIFICATION
#
# Every marker below is a string OBSERVED in production on 2026-08-18,
# not one inferred from documentation. Keep it that way: a marker that
# matches text TikTok never actually emits is dead code that reads like
# a guarantee.
# ============================================================

def _norm(text: str) -> str:
    """Lowercase + normalise typographic punctuation before matching.

    Same defence as youtube.py's _normalize_error_text: a curly
    apostrophe (U+2019) in a provider's message never matches a marker
    written with a straight one, and the two are visually identical, so
    the bug is invisible on inspection."""
    lowered = (text or "").lower()
    return (
        lowered
        .replace("\u2019", "'").replace("\u2018", "'")
        .replace("\u201c", '"').replace("\u201d", '"')
    )


# yt-dlp falling back to the generic extractor on a /photo/ URL.
PHOTO_MARKERS = (
    "unsupported url",
    "falling back on generic information extractor",
)

# OBSERVED 2026-09-15: a /t/ short link resolving to a sound or hashtag
# page. Those extractors call TikTok's app API, which has no credentials.
NOT_VIDEO_MARKERS = (
    "no working app info is available",
)

# OBSERVED: "This post may not be comfortable for some audiences.
# Log in for access."
AGE_GATE_MARKERS = (
    "may not be comfortable for some audiences",
    "log in for access",
)

# OBSERVED: "Your IP address is blocked from accessing this post".
# Despite the wording this is NOT fixable by a different exit IP -
# tested through the residential proxy, identical failure. Treated as a
# post-level restriction, never escalated anywhere.
BLOCKED_MARKERS = (
    "ip address is blocked",
)

# Genuinely gone. UNVERIFIED - not yet reproduced on this box, since
# every "deleted video" test so far was contaminated by the curl_cffi
# incident. Confirm the real text before relying on these; until then
# an unmatched deleted post falls through to the generic branch, which
# is the safe direction to be wrong in.
UNAVAILABLE_MARKERS = (
    "video not available",
    "content is not available",
    "this post is not available",
    "video is private",
    "removed",
)

# OBSERVED 2026-09-09: FFmpegExtractAudio's ffprobe found no audio
# stream in the downloaded file. Not retryable - the same format will
# be silent again.
NO_AUDIO_MARKERS = (
    "unable to obtain file audio codec",
)

# OBSERVED 2026-09-19: extraction succeeds and lists formats, but the
# signed play URL for a given format 404s on the byte fetch. Per-video,
# not IP-related (reproduced 6/6 on the VPS for one video while a
# neighbouring lower-bitrate format served fine). check_formats in
# _base_opts probes candidates and falls through; if EVERY candidate is
# dead yt-dlp raises "Requested format is not available". The raw 404
# text is kept as a marker for the window between probe and download.
MEDIA_DEAD_MARKERS = (
    "unable to download video data",
    "requested format is not available",
)

# yt-dlp's own "I don't recognise this response" message. Deliberately
# treated as RETRYABLE rather than given a specific user message: it
# means the page shape changed under the extractor, which is sometimes
# transient and sometimes an upstream break. It is NOT a private-video
# signal, despite appearing during private-video testing - that was
# curl_cffi noise (see module docstring).
TRANSIENT_MARKERS = (
    "unexpected response from webpage request",
    "unable to download webpage",
    "read timed out",
    "connection reset",
    "reset by server",
    "protocol_error",
    "temporary failure",
    "timed out",
)


def is_photo_error(text: str) -> bool:
    n = _norm(text)
    return any(m in n for m in PHOTO_MARKERS)


def is_not_video_error(text: str) -> bool:
    n = _norm(text)
    return any(m in n for m in NOT_VIDEO_MARKERS)


def is_age_gated_error(text: str) -> bool:
    n = _norm(text)
    return any(m in n for m in AGE_GATE_MARKERS)


def is_blocked_error(text: str) -> bool:
    n = _norm(text)
    return any(m in n for m in BLOCKED_MARKERS)


def is_unavailable_error(text: str) -> bool:
    n = _norm(text)
    return any(m in n for m in UNAVAILABLE_MARKERS)


def is_no_audio_error(text: str) -> bool:
    n = _norm(text)
    return any(m in n for m in NO_AUDIO_MARKERS)


def is_media_dead_error(text: str) -> bool:
    n = _norm(text)
    return any(m in n for m in MEDIA_DEAD_MARKERS)


def is_retryable_error(text: str) -> bool:
    """The ONLY errors that get a retry.

    Ordered check: anything already classified is definitively not
    retryable, so those are excluded first. Being wrong in the
    retryable direction is cheap (a few wasted seconds); being wrong in
    the other direction loses a request that would have succeeded."""
    if is_media_dead_error(text):
        # A fresh extraction re-signs the play URLs; a second pass with
        # check_formats usually lands on a live one.
        return True
    if (is_photo_error(text) or is_not_video_error(text) or is_age_gated_error(text)
            or is_blocked_error(text) or is_unavailable_error(text)
            or is_no_audio_error(text)):
        return False
    n = _norm(text)
    return any(m in n for m in TRANSIENT_MARKERS)


def classify(text: str) -> Tuple[str, str]:
    """Maps raw yt-dlp text to (kind, user-facing message).

    Raw text is LOGGED by the caller, never returned. It leaks
    internals, means nothing to a user, and makes a working system look
    broken."""
    if is_photo_error(text):
        return ("photo_post", (
            "This looks like a TikTok photo/slideshow post. Only videos "
            "with audio can be converted - try a video post instead."
        ))
    if is_not_video_error(text):
        return ("not_a_video", (
            "This link opens a TikTok sound, hashtag or profile page, not a "
            "video. Open a video on TikTok, tap Share, copy that link and "
            "paste it here."
        ))
    if is_age_gated_error(text):
        return ("age_gated", (
            "This TikTok requires you to be logged in to view it, so it "
            "can't be downloaded here. Try a different video."
        ))
    if is_blocked_error(text):
        return ("blocked", (
            "TikTok has restricted this post, so it isn't available to "
            "download. Try a different video."
        ))
    if is_media_dead_error(text):
        return ("media_dead", (
            "TikTok isn't serving the media for this video right now. "
            "Please try again in a moment."
        ))
    if is_unavailable_error(text):
        return ("unavailable", (
            "This TikTok isn't available - it may have been deleted, made "
            "private, or restricted. Try a different video."
        ))
    if is_no_audio_error(text):
        return ("no_audio", (
            "This TikTok doesn't have an audio track that can be extracted, "
            "so there's nothing to convert. Try a different video."
        ))
    return ("unknown", (
        "Something went wrong while downloading this TikTok. Please try "
        "again, or try a different video."
    ))


# ============================================================
# EXTRACTION
# ============================================================

def _base_opts(outtmpl: str) -> dict:
    return {
        # TikTok labels EVERY format acodec=aac, but its HEVC (bytevc1)
        # web streams are frequently video-only in reality, so
        # bestaudio/best picks the highest-bitrate HEVC file and gets
        # silence. h264 streams carry audio. yt-dlp #15642, closed as
        # not-a-bug; the selector is the fix.
        "format": "best[vcodec^=h264]/best[vcodec^=avc]/best",
        # TikTok serves some signed play URLs dead per-video (404 on the
        # byte fetch, extraction fine). The "/" alternatives above only
        # fall through on a no-match, never on a download failure, so
        # without this a dead top-bitrate format fails the whole job
        # while a working lower-bitrate one sits right below it.
        "check_formats": "selected",
        "outtmpl": outtmpl,
        "quiet": True,
        "noplaylist": True,
        "ffmpeg_location": FFMPEG,
        # Without this TikTok serves a non-media page and ffprobe fails
        # in postprocessing. curl_cffi must be installed (requirements.txt);
        # if the target is unavailable yt-dlp raises at construction.
        "impersonate": ImpersonateTarget.from_str(TIKTOK_IMPERSONATE_TARGET),
        # Same reasoning as the YouTube path: this container has no
        # usable IPv6 route (Docker does not forward it in by default),
        # so pinning to IPv4 avoids a dead path on any IPv6-only host.
        "source_address": "0.0.0.0",
        "socket_timeout": 20,
        # Retry a dropped/reset transfer (HTTP/2 stream reset, curl 92)
        # instead of failing on the first one; mirrors youtube_chain.py.
        "retries": 3,
        "fragment_retries": 5,
        # No proxy key at all, by design - see module docstring.
        # No cookiefile - public posts need none, and the one case that
        # would need it (age-gated) is deliberately unsupported.
    }


def _resolve_short_url(ydl, url: str) -> str:
    """Follows a vt./vm./t/ link without extracting, so non-video
    targets are rejected before yt-dlp hits the app API."""
    if not is_short_url(url):
        return url
    result = ydl.extract_info(url, download=False, process=False) or {}
    resolved = result.get("url") or result.get("webpage_url") or url
    if is_photo_url(resolved):
        raise TikTokError(
            "This is a TikTok photo/slideshow post. Only videos with "
            "audio can be converted - try a video post instead.",
            kind="photo_post",
        )
    if is_not_video_url(resolved):
        raise TikTokError(
            "This link opens a TikTok sound, hashtag or profile page, not a "
            "video. Open a video on TikTok, tap Share, copy that link and "
            "paste it here.",
            kind="not_a_video",
        )
    return resolved


def _probe_has_audio(path: str) -> bool:
    """True if the downloaded file actually contains an audio stream.

    WHY THIS EXISTS: a TikTok posted with no sound extracts, downloads,
    and converts without any error - producing a silent or zero-byte
    MP3. That is the one failure mode in this whole module that is
    SILENT: every other problem raises something. Without this check a
    user gets a file that appears to work and does not.

    Fails OPEN (returns True) if ffprobe itself errors: a probe failure
    is not evidence of missing audio, and rejecting a good download over
    a broken probe is the more expensive way to be wrong."""
    try:
        out = subprocess.run(
            [FFPROBE, "-v", "error", "-select_streams", "a:0",
             "-show_entries", "stream=codec_name", "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=30,
        )
        return bool(out.stdout.strip())
    except Exception as e:
        logger.warning(f"[TIKTOK] ffprobe check failed (assuming audio present): {e}")
        return True


def extract_and_download(url: str, out_dir: str, job_id: str) -> Tuple[str, str, dict]:
    """
    Downloads a TikTok post and converts it to MP3.

    Returns (mp3_path, title, info). Raises TikTokError with an
    already-user-facing message on every failure.

    Fully synchronous/blocking - MUST be dispatched via
    utils.run_blocking() or a killable subprocess from async code, or it
    freezes the event loop for the whole server.
    """
    # ---- pre-flight: reject photo posts before spending anything ----
    # Only catches DIRECT photo URLs. Short links are caught after
    # extraction fails - see the _PHOTO_URL_PATTERN comment.
    if is_photo_url(url):
        raise TikTokError(
            "This is a TikTok photo/slideshow post. Only videos with "
            "audio can be converted - try a video post instead.",
            kind="photo_post",
        )

    outtmpl = os.path.join(out_dir, f"{job_id}_tiktok.%(ext)s")
    opts = _base_opts(outtmpl)
    opts["postprocessors"] = [{
        "key": "FFmpegExtractAudio",
        "preferredcodec": "mp3",
        "preferredquality": MP3_BITRATE.rstrip("k"),
    }]

    last_error_text = ""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                target = _resolve_short_url(ydl, url)
                if target != url:
                    logger.info(f"[TIKTOK] job={job_id} short link resolved to {target[:120]}")
                info = ydl.extract_info(target, download=False)

                duration = (info or {}).get("duration")
                if duration and duration > MAX_TIKTOK_DURATION_SECONDS:
                    raise TikTokTooLongError(
                        f"This video is {int(duration) // 60} min long, which "
                        f"exceeds the {MAX_TIKTOK_DURATION_SECONDS // 60} min limit."
                    )

                # Checked BEFORE downloading: if no format reports an
                # audio codec there is nothing to convert, and pulling
                # several MB of video first would be wasted bandwidth.
                formats = (info or {}).get("formats") or []
                if formats and not any(
                    f.get("acodec") not in (None, "none") for f in formats
                ):
                    raise TikTokError(
                        "This TikTok doesn't have an audio track, so there's "
                        "nothing to convert.",
                        kind="no_audio",
                    )

                info = ydl.process_ie_result(info, download=True)

            title = (info or {}).get("title") or "TikTok audio"
            mp3_path = os.path.join(out_dir, f"{job_id}_tiktok.mp3")

            if not os.path.exists(mp3_path):
                raise TikTokError(
                    "The audio file was not produced by the converter. "
                    "Please try again.",
                    kind="no_output",
                )

            # Belt and braces: a file can exist and still be silent, so
            # the metadata check above is not sufficient on its own.
            if os.path.getsize(mp3_path) < 1024 or not _probe_has_audio(mp3_path):
                try:
                    os.remove(mp3_path)
                except OSError:
                    pass
                raise TikTokError(
                    "This TikTok doesn't have usable audio, so there's "
                    "nothing to convert.",
                    kind="no_audio",
                )

            logger.info(
                f"[TIKTOK] job={job_id} COMPLETE '{title[:50]}' "
                f"({os.path.getsize(mp3_path) // 1024} KB)"
            )
            return mp3_path, title, (info or {})

        except TikTokError:
            # Already classified and already user-facing. Retrying would
            # produce the identical result.
            raise

        except Exception as e:
            last_error_text = str(e)

            if not is_retryable_error(last_error_text):
                kind, message = classify(last_error_text)
                logger.warning(
                    f"[TIKTOK] job={job_id} {kind} (not retrying): {last_error_text[:200]}"
                )
                raise TikTokError(message, kind=kind, debug=last_error_text[:500])

            if attempt < MAX_ATTEMPTS:
                backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    f"[TIKTOK] job={job_id} attempt {attempt}/{MAX_ATTEMPTS} "
                    f"failed with a transient error, retrying in {backoff:.1f}s: "
                    f"{last_error_text[:160]}"
                )
                time.sleep(backoff)
            else:
                logger.error(
                    f"[TIKTOK] job={job_id} all {MAX_ATTEMPTS} attempts failed: "
                    f"{last_error_text}"
                )

    kind, message = classify(last_error_text)
    raise TikTokError(message, kind=kind, debug=last_error_text[:500])