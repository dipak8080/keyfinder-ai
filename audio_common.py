"""
audio_common.py - Shared low-level helpers for every audio-tool module
(audio_converter.py, audio_cutter.py, pitch_changer.py, tempo_changer.py,
volume_booster.py, reverse_audio.py).

Same role for the audio-tools family that separation.py plays alone for
Demucs: subprocess execution, timeout handling, and error normalization.
Centralizing this here means each tool module only has to hold its own
ffmpeg/rubberband argument list, not its own copy of "how do I safely run
a subprocess and turn its failure into a clean error".
"""
import os
import uuid
import subprocess

from config import (
    logger,
    FFMPEG_PATH,
    FFPROBE_PATH,
    AUDIO_TOOLS_DIR,
    UPLOAD_DIR,
    ALLOWED_AUDIO_INPUT_FORMATS,
    MAX_AUDIO_TOOL_DURATION_SECONDS,
    AUDIO_TOOL_SUBPROCESS_TIMEOUT_SECONDS,
    FFPROBE_TIMEOUT_SECONDS,
)
from utils import safe_extension


class AudioToolError(Exception):
    """
    Raised for any expected/user-facing failure in an audio-tool module
    (bad format, file too long, subprocess failure, etc.). Routes catch
    this specifically and turn it into a clean error message/job status,
    the same way routes.py already does for separation.SeparationError -
    anything that ISN'T an AudioToolError is a genuine bug and should
    surface as an unexpected 500 / job failure with a generic message.
    """
    pass


# ========== STARTUP INVARIANT ==========
# Uploads and outputs MUST live in different directories. If they don't,
# every tool that preserves the file extension (volume, trim, pitch,
# tempo, reverse, noise-remove, voice-clean, echo-remove,
# silence-remove) silently breaks: input and output resolve to the same
# path, ffmpeg refuses to edit in place, and the cleanup step deletes
# the output. That exact failure shipped on 2026-08-09.
#
# Checked at import, not at request time, so a bad config kills startup
# with a clear message instead of producing a confusing per-request
# failure that looks like corrupt user uploads.
if os.path.abspath(UPLOAD_DIR) == os.path.abspath(AUDIO_TOOLS_DIR):
    raise RuntimeError(
        f"UPLOAD_DIR and AUDIO_TOOLS_DIR must be different directories "
        f"(both are '{UPLOAD_DIR}'). Job inputs and outputs share the same "
        f"<job_id>.<ext> naming, so pointing them at the same directory makes "
        f"input and output the same file for every format-preserving tool."
    )


# ========== FORMAT VALIDATION ==========

def get_extension(filename: str) -> str:
    """Returns the lowercase extension without the leading dot, e.g.
    'song.WAV' -> 'wav'. Empty string if there's no extension."""
    _, ext = os.path.splitext(filename or "")
    return ext.lstrip(".").lower()


def validate_input_format(filename: str) -> str:
    """
    Validates the uploaded file's extension against the whitelist and
    returns it (lowercased) if valid. Raises AudioToolError otherwise.

    NOTE: this checks the filename extension only, as a fast first-pass
    rejection. probe_duration_seconds() below (which shells out to
    ffprobe) is what actually confirms the file is valid, playable audio
    - a malicious/corrupt file with a spoofed extension will fail there,
    not here.

    Worth being clear about what this does NOT check, because it has been
    misread as a completeness guarantee: it says nothing about what is
    INSIDE the container. An .m4a carrying an embedded cover image is a
    perfectly valid audio file and passes here correctly - handling that
    is as_audio_only_ffmpeg()'s job further down, not this function's.
    Tightening validation would have rejected a file that works.
    """
    ext = get_extension(filename)
    if ext not in ALLOWED_AUDIO_INPUT_FORMATS:
        raise AudioToolError(
            f"Unsupported file type '.{ext}'. Supported formats: "
            f"{', '.join(sorted(ALLOWED_AUDIO_INPUT_FORMATS))}."
        )
    return ext


def validate_conversion_pair(source_format: str, target_format: str, conversion_matrix: dict) -> None:
    """
    Confirms (source_format -> target_format) is an explicitly allowed
    pair in the given matrix (AUDIO_CONVERSION_MATRIX from config.py).
    Raises AudioToolError otherwise. This is the actual security
    boundary - target_format never reaches a subprocess command line
    unless it passed this check.
    """
    allowed_targets = conversion_matrix.get(source_format)
    if not allowed_targets or target_format not in allowed_targets:
        raise AudioToolError(
            f"Conversion from '.{source_format}' to '.{target_format}' is not supported."
        )


# ========== FFPROBE: DURATION CHECK ==========

def probe_duration_seconds(file_path: str) -> float:
    """
    Runs ffprobe to get the audio duration in seconds. Also serves as a
    cheap validity check - a corrupt/non-audio file will fail here with
    a clean AudioToolError rather than wasting a full ffmpeg pass first.

    Uses FFPROBE_TIMEOUT_SECONDS rather than the ffmpeg-sized one this
    previously shared. ffprobe reads a header; it does not decode. A
    probe still running after 30 seconds is stuck on a pathological file,
    not working hard - and the old 600s ceiling meant such a file could
    hold a thread-pool worker for ten minutes before anyone found out.
    """
    cmd = [
        FFPROBE_PATH,
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        file_path,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=FFPROBE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise AudioToolError("Timed out while inspecting the audio file.")

    if result.returncode != 0 or not result.stdout.strip():
        logger.warning(f"[AUDIO_TOOLS] ffprobe failed on {file_path}: {result.stderr.strip()}")
        raise AudioToolError("Could not read this file as valid audio. It may be corrupt or an unsupported format.")

    try:
        return float(result.stdout.strip())
    except ValueError:
        raise AudioToolError("Could not determine audio duration.")


def validate_duration(file_path: str, max_seconds: int = MAX_AUDIO_TOOL_DURATION_SECONDS) -> float:
    """Probes duration and rejects anything over max_seconds. Returns the
    duration (callers that need it, e.g. audio_cutter, can reuse it
    instead of probing twice)."""
    duration = probe_duration_seconds(file_path)
    if duration > max_seconds:
        raise AudioToolError(
            f"Audio is too long ({duration / 60:.1f} min). "
            f"Maximum allowed is {max_seconds / 60:.0f} min."
        )
    return duration


# ========== OUTPUT PATH HELPERS ==========

def build_output_path(job_id: str, output_format: str) -> str:
    """Deterministic output path for a job, e.g.
    audio_tools_output/<job_id>.mp3 - kept separate from the input
    upload path so cleanup of one never risks touching the other.

    Safe by construction already: output_format is never raw user input,
    it comes from the validated conversion matrix or the source file's
    validated extension.
    """
    return os.path.join(AUDIO_TOOLS_DIR, f"{job_id}.{output_format}")


def build_temp_input_path(job_id: str, original_filename: str) -> str:
    """
    Deterministic input path for a job: <UPLOAD_DIR>/<job_id>.<ext>

    THIS FUNCTION CAUSED A PRODUCTION OUTAGE (2026-08-08), and a SECOND
    ONE FROM ITS OWN FIX (2026-08-09) - documenting both so the next
    change doesn't reintroduce either.

    Bug #1 (2026-08-08): built f"{job_id}_{original_filename}", pasting
    the raw user-supplied filename straight into a path. Linux caps a
    filename at 255 BYTES - not characters - and UTF-8 encodes Hebrew at
    2 bytes/char, emoji at 4. A ~180-character Hebrew filename came to
    390 bytes with the job-id prefix, open() failed with [Errno 36] File
    name too long, and every job tool 500'd for that upload.

    Bug #2 (2026-08-09), introduced BY the fix for bug #1: the corrected
    version built the path as AUDIO_TOOLS_DIR/<job_id>.<ext> - the exact
    same directory build_output_path() uses for the OUTPUT file, with the
    exact same naming shape. For any tool that doesn't change the file's
    extension (volume, trim, pitch, tempo, reverse, noise-remove, voice-
    clean, echo-remove, silence-remove - i.e. most of them; only /convert
    and a handful of others change format), input_path and output_path
    became IDENTICAL. ffmpeg correctly refuses to edit a file in place:
    "Output ... same as Input #0 - exiting FFmpeg cannot edit existing
    files in-place." Every /volume, /trim, /pitch etc. call started
    failing with exit 234.

    The actual fix, addressing both at once: input goes in UPLOAD_DIR,
    output stays in AUDIO_TOOLS_DIR - two physically separate
    directories, so job_id-based filenames can never collide regardless
    of whether the tool changes the extension. This was the ORIGINAL
    design before bug #1's fix accidentally merged them into one
    directory. safe_extension() (utils.py) still keeps the filename
    itself bounded and ASCII-only, so bug #1 stays fixed.
    """
    return os.path.join(UPLOAD_DIR, f"{job_id}.{safe_extension(original_filename)}")


def assert_distinct_paths(input_path: str, output_path: str) -> None:
    """
    Guards the single bug class that has now caused two production
    incidents in two days - both from path construction, both silent
    until a user hit them.

    On 2026-08-09 build_temp_input_path() and build_output_path() briefly
    produced IDENTICAL paths for every tool that doesn't change the file
    extension (volume, trim, pitch, tempo, reverse, noise-remove,
    voice-clean, echo-remove, silence-remove - i.e. most of them). The
    symptoms were awful to diagnose from the outside: ffmpeg exited 234
    with "Output ... same as Input #0", the user saw a generic "file may
    be corrupt" message, and the cleanup step then DELETED the output
    because input_path and output_path were the same file.

    An audit proves the path builders are correct today. This proves it
    at every invocation, forever - including for any tool added later by
    someone who hasn't read this file. Failing loudly here, at submit
    time, with the actual paths in the message, is worth far more than
    the microsecond it costs: the alternative is failing opaquely deep
    inside a subprocess ten seconds later.

    Raises AudioToolError (not a bare assert) so it can't be compiled
    out with python -O, and so it lands in the same handling path as
    every other expected failure rather than surfacing as a 500.
    """
    if os.path.abspath(input_path) == os.path.abspath(output_path):
        logger.error(
            f"[AUDIO_TOOLS] PATH COLLISION - input and output are the same file: "
            f"{input_path}. This is a bug in path construction, not a bad upload. "
            f"See assert_distinct_paths() in audio_common.py."
        )
        raise AudioToolError("Internal path configuration error. Please report this.")


# ========== SUBPROCESS EXECUTION ==========

# ffmpeg's "-vn" - discard any video stream. Injected into every ffmpeg
# command this module runs; see as_audio_only_ffmpeg() below.
_FFMPEG_NO_VIDEO_FLAG = "-vn"


def as_audio_only_ffmpeg(cmd: list) -> list:
    """
    Adds -vn to an ffmpeg command so embedded artwork can't break the mux.

    PUBLIC, not private, because run_subprocess() below is NOT the only
    place ffmpeg gets invoked. audio_loudnorm.py builds and runs its own
    two subprocess calls directly - it needs pass 1's stderr to parse the
    measurement JSON, which run_subprocess() discards - so it would have
    silently missed this fix entirely. Any module that calls
    subprocess.run() on an ffmpeg command itself must pass the command
    through here first.

    THE BUG THIS FIXES (production, 2026-08-22). A user submitted an m4a
    pulled from an Instagram reel to /echo-remove. The file is perfectly
    valid audio - and it carries its cover image as a SECOND stream:

        Stream #0:0: Audio: aac (HE-AAC), 44100 Hz, stereo
        Stream #0:1: Video: mjpeg (Progressive), 640x1136 (attached pic)

    ffmpeg maps every stream it finds unless told otherwise, so it tried
    to transcode that JPEG to H.264 and write it into the .m4a output:

        [ipod] Could not find tag for codec h264 in stream #0, codec not
        currently supported in container
        [out#0/ipod] Could not write header (incorrect codec parameters ?)
        Nothing was written into output file

    The audio side was fine and never got written, because the video
    stream killed the muxer first. The user was told "the file may be
    corrupt or in an unsupported format" about a file that is neither.

    WHY THIS IS NOT A VALIDATION PROBLEM, which is the tempting reading:
    rejecting the upload would be the WRONG fix. Embedded artwork is
    normal and extremely common - anything saved from Instagram or
    TikTok, anything tagged in iTunes, most purchased music. Those files
    should work, and they do. ffmpeg just needed telling that this is an
    audio job.

    WHY -vn RATHER THAN PRESERVING THE ARTWORK: "-map 0:a -c:v copy"
    would keep the cover image, at the cost of reopening
    container-compatibility questions for every format pair this app
    supports (mp3 art into flac, m4a art into ogg, and so on). Dropping
    it is the honest, predictable behaviour for a processed audio file,
    and it cannot fail.

    SAFE FOR EVERY CALLER: every ffmpeg command in this codebase produces
    AUDIO. Nothing here outputs video - /video-to-audio least of all,
    since it exists to extract an audio track. There is no invocation for
    which -vn is wrong.

    NOT APPLIED TO NON-FFMPEG COMMANDS. rubberband (pitch, tempo) takes
    no such flag and would reject it as an unknown argument, so the guard
    on cmd[0] below is load-bearing, not defensive decoration.

    POSITION: -vn is an OUTPUT option, so it must sit before the output
    file - which is the last element of every command this app builds.
    Inserting at index -1 is therefore correct for all of them.
    Idempotent as well: a command that already carries -vn is returned
    untouched, so a tool module adding its own later can never produce a
    duplicate.
    """
    if not cmd or cmd[0] != FFMPEG_PATH:
        return cmd
    if _FFMPEG_NO_VIDEO_FLAG in cmd:
        return cmd
    return cmd[:-1] + [_FFMPEG_NO_VIDEO_FLAG, cmd[-1]]


def run_subprocess(cmd: list, timeout: int = AUDIO_TOOL_SUBPROCESS_TIMEOUT_SECONDS) -> None:
    """
    Runs a subprocess command (ffmpeg, rubberband, etc.), raising a clean
    AudioToolError on non-zero exit or timeout instead of letting a raw
    CalledProcessError/TimeoutExpired propagate. Every tool module routes
    its ffmpeg/rubberband invocation through this single function so
    error handling/logging behavior stays identical across all six.

    cmd must be a list (never a shell string) - this is a deliberate
    security boundary against shell injection, same principle as the
    conversion-matrix whitelist above.

    ffmpeg commands get -vn added here rather than in each of the sixteen
    tool modules - see as_audio_only_ffmpeg() for the incident that
    motivated it. Doing it at the single choke point is the entire reason
    this function exists: one edit covers every tool, including any added
    later by someone who has never heard of the bug.
    """
    cmd = as_audio_only_ffmpeg(cmd)

    logger.info(f"[AUDIO_TOOLS] Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        logger.error(f"[AUDIO_TOOLS] Subprocess timed out after {timeout}s: {' '.join(cmd)}")
        raise AudioToolError("Processing took too long and was stopped. Try a shorter file.")

    if result.returncode != 0:
        logger.error(f"[AUDIO_TOOLS] Subprocess failed (exit {result.returncode}): {result.stderr.strip()}")
        raise AudioToolError("Audio processing failed. The file may be corrupt or in an unsupported format.")


def new_job_id() -> str:
    """Plain uuid4 hex, same style as jobs.create_job()'s internal id
    generation - used where a module needs the id before calling
    create_job() (e.g. to build file paths ahead of time)."""
    return uuid.uuid4().hex


# ========== MIME TYPE MAPPING (for inline preview) ==========

_AUDIO_MIME_TYPES = {
    "mp3": "audio/mpeg",
    "wav": "audio/wav",
    "flac": "audio/flac",
    "m4a": "audio/mp4",
    "aac": "audio/aac",
    "ogg": "audio/ogg",
    "aiff": "audio/aiff",
    "mid": "audio/midi",
}


def get_audio_mime_type(extension: str) -> str:
    """Returns the correct Content-Type for inline <audio> playback of
    the given extension. Falls back to a generic type if unrecognized
    (shouldn't happen given ALLOWED_AUDIO_INPUT_FORMATS validation, but
    fail safe rather than raise)."""
    return _AUDIO_MIME_TYPES.get(extension.lower(), "application/octet-stream")