"""
reverse_audio.py - Reverse audio playback via ffmpeg's `areverse`
filter.

Simplest of the six audio-tool modules - no parameters, no rubberband
dependency, just a single ffmpeg filter pass. Output format matches
input format (chain with /convert for format changes).
"""
from config import logger, FFMPEG_PATH
from audio_common import run_subprocess


def reverse_audio(input_path: str, output_path: str) -> None:
    """
    Reverses input_path front-to-back, writing the result to
    output_path (same format as input). No parameters to validate -
    the operation is unconditional given valid input audio (which
    routes.py has already confirmed via validate_input_format +
    validate_duration before this is ever called).

    Raises AudioToolError (via run_subprocess) on ffmpeg failure.
    """
    cmd = [
        FFMPEG_PATH, "-y",
        "-i", input_path,
        "-af", "areverse",
        output_path,
    ]

    run_subprocess(cmd)

    logger.info(f"[REVERSE] {input_path} -> {output_path}")