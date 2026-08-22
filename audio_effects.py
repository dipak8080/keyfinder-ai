"""
audio_effects.py - Four small, single-filter ffmpeg operations, grouped
into one module since each is a handful of lines: fade in/out, mono<->
stereo channel conversion, sample-rate/bit-depth resampling, and a
ringtone maker (trim + M4A-as-M4R).

Each gets its OWN route/status/preview/download in routes.py (not a
combined endpoint) so every tool still has its own landing page to rank
for its own query - "fade audio", "convert to mono", "make a ringtone"
are different searches with different intent, and collapsing them into
one endpoint would mean one page trying to rank for all of them at once.
Only the ffmpeg-calling CODE is shared here, because each function really
is just one filter.

VALIDATION PHILOSOPHY: every public function here re-validates its own
inputs rather than trusting that routes.py already checked them. This
is deliberate defense-in-depth, not duplication for its own sake - these
functions are the actual business logic, and a route-only check means
this module silently trusts a caller it can't see. Two concrete bugs
this caught in practice:

  1. resample_audio() previously trusted RESAMPLE_ALLOWED_RATES as a
     single flat list, but libmp3lame (the MP3 encoder) only accepts 9
     specific sample rates and rejects 96000 outright - the route's
     "is this rate in the allowed list" check passed, then ffmpeg failed
     with a raw, unhelpful encoder error instead of a clean rejection.
  2. Every function indexed _ENCODE_ARGS[source_format] with no existence
     check first - a format that slipped through routes.py's validation
     for any reason would raise a bare KeyError here instead of a clean
     AudioToolError, surfacing as an opaque 500 rather than a 400 with an
     actionable message.

Same subprocess-per-call, blocking pattern as the rest of this codebase -
every public function here MUST be dispatched via utils.run_blocking()
from the async route.

--------------------------------------------------------------------------
WHAT CHANGED (2026-08-22): EMBEDDED ARTWORK BROKE ALL FOUR TOOLS

_run_ffmpeg() builds and runs its own subprocess rather than going
through audio_common.run_subprocess(), so when -vn was added there the
same day, all four tools in this file silently missed the fix.

The failure, first seen on /echo-remove: an audio file carrying its
cover image as a second stream (normal for anything saved from
Instagram or TikTok, anything tagged in iTunes, most purchased music)
makes ffmpeg try to transcode that JPEG to H.264 and mux it into the
audio output. The strict muxers refuse:

    [ipod] Could not find tag for codec h264 in stream #0, codec not
    currently supported in container

/ringtone is the worst hit, because it ALWAYS writes m4a - so every
ringtone made from an artwork-bearing track failed outright, which is
most tracks anyone would want as a ringtone. fade/channels/resample
fail the same way whenever the source format is m4a or aac.

Every command here now goes through as_audio_only_ffmpeg(). Nothing else
changed.
--------------------------------------------------------------------------
"""
import math
import os
import subprocess

from config import (
    logger,
    FFMPEG_PATH,
    FADE_MAX_SECONDS,
    RESAMPLE_ALLOWED_RATES,
    RESAMPLE_ALLOWED_BIT_DEPTHS,
    RINGTONE_MAX_DURATION_SECONDS,
    AUDIO_TOOL_SUBPROCESS_TIMEOUT_SECONDS,
)
from audio_common import AudioToolError, validate_duration, as_audio_only_ffmpeg

_ENCODE_ARGS = {
    "mp3": ["-c:a", "libmp3lame", "-q:a", "2"],
    "wav": ["-c:a", "pcm_s16le"],
    "flac": ["-c:a", "flac"],
    "m4a": ["-c:a", "aac", "-b:a", "192k"],
    "aac": ["-c:a", "aac", "-b:a", "192k"],
    "ogg": ["-c:a", "libvorbis", "-q:a", "5"],
    "aiff": ["-c:a", "pcm_s16be"],
}

# ffmpeg's pcm bit-depth codec names, keyed by the depth users actually
# think in. Only used by resample - the other three effects keep
# whatever bit depth the container already implies.
_PCM_CODECS_BY_DEPTH = {
    16: "pcm_s16le",
    24: "pcm_s24le",
    32: "pcm_s32le",
}

# libmp3lame (the MP3 encoder) only accepts these EXACT sample rates -
# anything else fails at the encoder with a raw ffmpeg error
# ("Specified sample rate 96000 is not supported by the libmp3lame
# encoder") rather than a clean rejection. This is the one real
# per-format constraint among RESAMPLE_ALLOWED_RATES' four values -
# WAV/AIFF/FLAC (uncompressed/lossless) accept arbitrary rates, and
# ffmpeg's native AAC encoder (used for both m4a/aac) and libvorbis
# (used for ogg) both comfortably support the full 22050-96000 range
# this app offers, so only MP3 needs a narrower allow-list here.
_MP3_SUPPORTED_SAMPLE_RATES = {8000, 11025, 12000, 16000, 22050, 24000, 32000, 44100, 48000}


def _require_known_format(source_format: str) -> str:
    """
    Every effect function below eventually indexes _ENCODE_ARGS by
    source_format. Without this check, a format that somehow slipped past
    routes.py's validation (a bug there, a future format added to one
    list but not the other, etc.) would raise a bare KeyError - which
    surfaces as an opaque 500 with a Python traceback instead of a clean,
    actionable error. Centralized here since all four public functions
    need the identical check.
    """
    normalized = (source_format or "").strip().lower()
    if normalized not in _ENCODE_ARGS:
        raise AudioToolError(
            f"'{source_format}' isn't a supported audio format. "
            f"Supported: {', '.join(sorted(_ENCODE_ARGS.keys()))}."
        )
    return normalized


def _require_finite(value: float, field_name: str) -> float:
    """Rejects NaN/inf before it reaches a subprocess arg list - a
    non-finite value here would otherwise get stringified into the
    ffmpeg command as literally "nan" or "inf" and fail with a confusing
    encoder error rather than a clear validation message."""
    if not math.isfinite(value):
        raise AudioToolError(f"{field_name} must be a real, finite number.")
    return value


def _run_ffmpeg(cmd: list, error_message: str, output_path: str):
    """Shared subprocess-run-and-verify tail end for all four effects
    below - same timeout, same failure/empty-output handling, avoids
    repeating this six times for what is otherwise a one-line filter.

    as_audio_only_ffmpeg() is applied HERE rather than at each of the
    four call sites, for exactly the reason this function exists at all:
    one place to change, and a fifth effect added later inherits it
    without anyone having to remember. See its docstring in
    audio_common.py for the artwork bug that made it necessary."""
    cmd = as_audio_only_ffmpeg(cmd)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=AUDIO_TOOL_SUBPROCESS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise AudioToolError(f"{error_message} timed out. Try a shorter file.")

    if result.returncode != 0:
        logger.error(f"[AUDIO_EFFECTS] ffmpeg failed: {result.stderr[-1500:]}")
        raise AudioToolError(f"{error_message}.")

    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        raise AudioToolError(f"{error_message} produced an empty file.")


def apply_fade(input_path: str, output_path: str, source_format: str,
               fade_in_seconds: float, fade_out_seconds: float):
    """
    Fades the start and/or end of the track in/out via ffmpeg's afade
    filter.

    Bounds and "at least one fade requested" are validated HERE, not
    just in routes.py - this function must be safe to call correctly on
    its own, since trusting the route to have already checked everything
    is exactly the kind of implicit coupling that breaks silently when
    either side changes independently.

    fade_out is anchored to the END of the track (start time = duration
    minus fade_out_seconds), which requires knowing the file's actual
    duration first - unlike fade_in, which always starts at t=0.
    """
    source_format = _require_known_format(source_format)
    fade_in_seconds = _require_finite(fade_in_seconds, "fade_in_seconds")
    fade_out_seconds = _require_finite(fade_out_seconds, "fade_out_seconds")

    if fade_in_seconds < 0 or fade_in_seconds > FADE_MAX_SECONDS:
        raise AudioToolError(f"fade_in_seconds must be between 0 and {FADE_MAX_SECONDS}.")
    if fade_out_seconds < 0 or fade_out_seconds > FADE_MAX_SECONDS:
        raise AudioToolError(f"fade_out_seconds must be between 0 and {FADE_MAX_SECONDS}.")
    if fade_in_seconds <= 0 and fade_out_seconds <= 0:
        raise AudioToolError("At least one of fade_in_seconds or fade_out_seconds must be greater than 0.")

    duration = validate_duration(input_path)

    if fade_out_seconds > 0 and fade_out_seconds > duration:
        raise AudioToolError(
            f"fade_out_seconds ({fade_out_seconds}s) exceeds the track's "
            f"duration ({duration:.1f}s)."
        )
    if fade_in_seconds > 0 and fade_in_seconds > duration:
        raise AudioToolError(
            f"fade_in_seconds ({fade_in_seconds}s) exceeds the track's "
            f"duration ({duration:.1f}s)."
        )

    filters = []
    if fade_in_seconds > 0:
        filters.append(f"afade=t=in:st=0:d={fade_in_seconds}")
    if fade_out_seconds > 0:
        fade_out_start = max(0.0, duration - fade_out_seconds)
        filters.append(f"afade=t=out:st={fade_out_start}:d={fade_out_seconds}")

    cmd = [FFMPEG_PATH, "-y", "-i", input_path, "-af", ",".join(filters)]
    cmd += _ENCODE_ARGS[source_format]
    cmd += [output_path]

    logger.info(f"[FADE] {input_path}: in={fade_in_seconds}s out={fade_out_seconds}s (duration={duration:.1f}s)")
    _run_ffmpeg(cmd, "Fade failed", output_path)


def convert_channels(input_path: str, output_path: str, source_format: str, target: str):
    """
    Converts to mono (ac=1, downmixes stereo by averaging channels) or
    stereo (ac=2, duplicates a mono source to both channels).
    """
    source_format = _require_known_format(source_format)

    target = (target or "").strip().lower()
    if target not in ("mono", "stereo"):
        raise AudioToolError("target must be 'mono' or 'stereo'.")

    channels = 1 if target == "mono" else 2
    cmd = [FFMPEG_PATH, "-y", "-i", input_path, "-ac", str(channels)]
    cmd += _ENCODE_ARGS[source_format]
    cmd += [output_path]

    logger.info(f"[CHANNELS] {input_path}: -> {target}")
    _run_ffmpeg(cmd, "Channel conversion failed", output_path)


def resample_audio(input_path: str, output_path: str, source_format: str,
                    sample_rate: int, bit_depth: int = None):
    """
    Changes sample rate (-ar) and, for WAV/AIFF only, bit depth (via the
    matching pcm_sXXle/be codec). bit_depth is silently ignored (but
    LOGGED - see below) for compressed formats (mp3/aac/ogg/flac) - those
    don't expose a user-facing PCM bit depth the way an uncompressed
    container does, so there's nothing meaningful to change there.

    Raises AudioToolError for an out-of-range rate/depth, an unknown
    format, OR a rate the specific source format's encoder can't
    actually produce (currently: MP3 rejects several of the rates this
    app otherwise allows) - this last check is what a flat "is it in
    RESAMPLE_ALLOWED_RATES" test misses, since that list is a UNION of
    what's valid across all formats, not what's valid for any one of
    them.
    """
    source_format = _require_known_format(source_format)

    if sample_rate not in RESAMPLE_ALLOWED_RATES:
        raise AudioToolError(f"sample_rate must be one of: {', '.join(str(r) for r in RESAMPLE_ALLOWED_RATES)}")

    if source_format == "mp3" and sample_rate not in _MP3_SUPPORTED_SAMPLE_RATES:
        raise AudioToolError(
            f"MP3 doesn't support {sample_rate}Hz. Supported MP3 rates: "
            f"{', '.join(str(r) for r in sorted(_MP3_SUPPORTED_SAMPLE_RATES))}. "
            f"For {sample_rate}Hz, convert to WAV, FLAC, or AAC/M4A first."
        )

    if bit_depth is not None and bit_depth not in RESAMPLE_ALLOWED_BIT_DEPTHS:
        raise AudioToolError(f"bit_depth must be one of: {', '.join(str(b) for b in RESAMPLE_ALLOWED_BIT_DEPTHS)}")

    cmd = [FFMPEG_PATH, "-y", "-i", input_path, "-ar", str(sample_rate)]

    if bit_depth is not None and source_format == "wav":
        cmd += ["-c:a", _PCM_CODECS_BY_DEPTH[bit_depth]]
    elif bit_depth is not None and source_format == "aiff":
        # AIFF is big-endian PCM - reusing the little-endian codec names
        # from _PCM_CODECS_BY_DEPTH would produce a file that plays as
        # noise, so the "be" variant is substituted here specifically.
        cmd += ["-c:a", _PCM_CODECS_BY_DEPTH[bit_depth].replace("le", "be")]
    else:
        if bit_depth is not None:
            # Not silent from an OPS perspective even though it's silent
            # to the end user (per the docstring above, this is intended
            # product behavior) - if this ever shows up in a support
            # ticket ("I asked for 24-bit and got X"), this line is what
            # explains why without needing to reproduce the request.
            logger.info(
                f"[RESAMPLE] bit_depth={bit_depth} requested but ignored - "
                f"'{source_format}' has no user-facing PCM bit depth to set."
            )
        cmd += _ENCODE_ARGS[source_format]

    cmd += [output_path]

    logger.info(f"[RESAMPLE] {input_path}: -> {sample_rate}Hz" + (f", {bit_depth}-bit" if bit_depth else ""))
    _run_ffmpeg(cmd, "Resampling failed", output_path)


def make_ringtone(input_path: str, output_path: str, start_seconds: float, duration_seconds: float):
    """
    Trims to a short clip and encodes as AAC-in-M4A, then the CALLER
    renames/serves it with a .m4r extension - .m4r is not a distinct
    codec or container, it's exactly an M4A file that iOS's Ringtones
    app recognizes by extension alone. This function writes standard
    .m4a bytes; routes.py is responsible for the .m4r filename on
    download.

    duration_seconds is capped at RINGTONE_MAX_DURATION_SECONDS
    (iPhone's own ringtone length limit) rather than the general
    MAX_AUDIO_TOOL_DURATION_SECONDS - a "ringtone" longer than that
    isn't a ringtone, so the cap is a real constraint of the format,
    not just a server-load guard.

    THE ARTWORK BUG HIT THIS TOOL HARDEST. It always writes m4a, whose
    muxer is the strictest of the lot, so before _run_ffmpeg() started
    applying -vn every ringtone made from a track with cover art failed -
    and a track someone wants as a ringtone is overwhelmingly likely to
    have cover art. See this module's WHAT CHANGED note.

    Note both -ss and -to sit BEFORE -i, making them INPUT options, so
    -to is on the source file's own timeline. If either were moved after
    -i they would become output options, the timestamps would restart at
    zero after the seek, and -to would cut a clip of length `end` rather
    than `end - start`. Worth knowing before rearranging this list.
    """
    start_seconds = _require_finite(start_seconds, "start_seconds")
    duration_seconds = _require_finite(duration_seconds, "duration_seconds")

    if duration_seconds <= 0 or duration_seconds > RINGTONE_MAX_DURATION_SECONDS:
        raise AudioToolError(f"duration_seconds must be between 0 and {RINGTONE_MAX_DURATION_SECONDS}.")

    total_duration = validate_duration(input_path)
    if start_seconds < 0 or start_seconds >= total_duration:
        raise AudioToolError(f"start_seconds must be within the track's {total_duration:.1f}s duration.")

    end_seconds = min(start_seconds + duration_seconds, total_duration)

    cmd = [
        FFMPEG_PATH, "-y",
        "-ss", str(start_seconds), "-to", str(end_seconds),
        "-i", input_path,
        "-c:a", "aac", "-b:a", "192k",
        output_path,
    ]

    logger.info(f"[RINGTONE] {input_path}: [{start_seconds}s -> {end_seconds}s]")
    _run_ffmpeg(cmd, "Ringtone creation failed", output_path)