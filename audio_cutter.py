"""
audio_cutter.py - Trim/cut audio to a start/end range via ffmpeg.

Business logic only - no HTTP/FastAPI here, same pattern as
audio_converter.py. Output format always matches input format (trimming
and converting are separate concerns, kept in separate modules by
design - a user who wants both calls /trim then /convert, or the
frontend chains them).
"""
from config import logger, FFMPEG_PATH
from audio_common import AudioToolError, run_subprocess


def trim_audio(input_path: str, output_path: str, start_seconds: float, end_seconds: float, duration: float) -> None:
    """
    Trims input_path to [start_seconds, end_seconds] and writes the
    result to output_path (same container/format as input).

    duration is the input's total duration (already probed by the
    caller via audio_common.validate_duration, passed in here rather
    than re-probed to avoid a redundant ffprobe call).

    Uses -ss BEFORE -i for fast seeking plus a re-encode (not `-c copy`)
    so cut points land exactly where requested - stream-copy trimming
    can only cut on keyframe boundaries and would silently produce
    inaccurate results for many source files. Accuracy matters more
    here than the small speed cost.

    Raises AudioToolError on invalid range or ffmpeg failure.
    """
    if start_seconds < 0:
        raise AudioToolError("start_seconds cannot be negative.")
    if end_seconds <= start_seconds:
        raise AudioToolError("end_seconds must be greater than start_seconds.")
    if end_seconds > duration:
        raise AudioToolError(
            f"end_seconds ({end_seconds}s) exceeds the audio's actual duration ({duration:.1f}s)."
        )

    clip_length = end_seconds - start_seconds

    cmd = [
        FFMPEG_PATH, "-y",
        "-i", input_path,
        "-ss", str(start_seconds),
        "-t", str(clip_length),
        output_path,
    ]

    run_subprocess(cmd)

    logger.info(f"[TRIM] {input_path} [{start_seconds}s -> {end_seconds}s] -> {output_path}")