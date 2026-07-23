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
    ALLOWED_AUDIO_INPUT_FORMATS,
    MAX_AUDIO_TOOL_DURATION_SECONDS,
    AUDIO_TOOL_SUBPROCESS_TIMEOUT_SECONDS,
)


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
            timeout=AUDIO_TOOL_SUBPROCESS_TIMEOUT_SECONDS,
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
    upload path so cleanup of one never risks touching the other."""
    return os.path.join(AUDIO_TOOLS_DIR, f"{job_id}.{output_format}")


def build_temp_input_path(job_id: str, original_filename: str) -> str:
    """Deterministic input path for a job, mirrors the
    f'{job_id}_{filename}' pattern already used in routes.py for
    /analyze and /separate."""
    return os.path.join(AUDIO_TOOLS_DIR, f"{job_id}_{original_filename}")


# ========== SUBPROCESS EXECUTION ==========

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
    """
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
}


def get_audio_mime_type(extension: str) -> str:
    """Returns the correct Content-Type for inline <audio> playback of
    the given extension. Falls back to a generic type if unrecognized
    (shouldn't happen given ALLOWED_AUDIO_INPUT_FORMATS validation, but
    fail safe rather than raise)."""
    return _AUDIO_MIME_TYPES.get(extension.lower(), "application/octet-stream")