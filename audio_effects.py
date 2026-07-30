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

Same subprocess-per-call, blocking pattern as the rest of this codebase -
every public function here MUST be dispatched via utils.run_blocking()
from the async route.
"""
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
from audio_common import AudioToolError, validate_duration

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


def _run_ffmpeg(cmd: list, error_message: str, output_path: str):
    """Shared subprocess-run-and-verify tail end for all four effects
    below - same timeout, same failure/empty-output handling, avoids
    repeating this six times for what is otherwise a one-line filter."""
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
    filter. At least one of fade_in_seconds/fade_out_seconds must be > 0
    - validated by the route before this is called, since a request for
    a fade with both at 0 is a no-op the user almost certainly didn't
    intend.

    fade_out is anchored to the END of the track (start time = duration
    minus fade_out_seconds), which requires knowing the file's actual
    duration first - unlike fade_in, which always starts at t=0.
    """
    duration = validate_duration(input_path)

    if fade_out_seconds > 0 and fade_out_seconds > duration:
        raise AudioToolError(
            f"fade_out_seconds ({fade_out_seconds}s) exceeds the track's "
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
    stereo (ac=2, duplicates a mono source to both channels). target
    must be "mono" or "stereo" - validated by the route.
    """
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
    matching pcm_sXXle/be codec). bit_depth is silently ignored for
    compressed formats (mp3/aac/ogg/flac) - those don't expose a
    user-facing PCM bit depth the way an uncompressed container does, so
    there's nothing meaningful to change there.
    """
    if sample_rate not in RESAMPLE_ALLOWED_RATES:
        raise AudioToolError(f"sample_rate must be one of: {', '.join(str(r) for r in RESAMPLE_ALLOWED_RATES)}")

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
    """
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