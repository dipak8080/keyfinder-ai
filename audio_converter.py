"""
audio_converter.py - Format conversion (MP3<->WAV, FLAC->WAV, M4A->MP3,
AAC->WAV, OGG->MP3, AIFF->WAV) via ffmpeg.

Business logic only - no HTTP/FastAPI here, same separation of concerns
as youtube.py / audio_analysis.py / separation.py. routes.py wires this
into the actual /convert endpoint.
"""
from config import logger, FFMPEG_PATH, AUDIO_CONVERSION_MATRIX
from audio_common import (
    AudioToolError,
    validate_input_format,
    validate_conversion_pair,
    run_subprocess,
)


def convert_audio(input_path: str, output_path: str, source_format: str, target_format: str) -> None:
    """
    Converts input_path (source_format) to output_path (target_format)
    via ffmpeg. Caller is responsible for validating the pair against
    AUDIO_CONVERSION_MATRIX before calling this (routes.py does this at
    request-validation time, before the job is even created) - this
    function re-validates anyway as defense in depth, since it's cheap
    and this function could in principle be called from elsewhere later.

    Raises AudioToolError on any failure (bad pair, ffmpeg failure).
    """
    validate_conversion_pair(source_format, target_format, AUDIO_CONVERSION_MATRIX)

    cmd = [FFMPEG_PATH, "-y", "-i", input_path]

    # Lossless-target formats: no bitrate flag needed, ffmpeg picks a
    # sensible default codec for the container.
    if target_format in ("wav", "flac", "aiff"):
        pass
    # Lossy-target formats: pin a reasonable default bitrate for
    # consistent, predictable output size/quality across requests.
    # m4a shares this branch with aac since it's just AAC audio in an
    # MP4 container - same encoder, same bitrate flag applies.
    elif target_format in ("mp3", "aac", "ogg", "m4a"):
        cmd += ["-b:a", "192k"]
    else:
        # Should be unreachable given validate_conversion_pair() above,
        # but fail loudly rather than silently falling through to a
        # format ffmpeg might guess wrong on.
        raise AudioToolError(f"No encoder configuration for target format '.{target_format}'.")

    cmd.append(output_path)

    run_subprocess(cmd)

    logger.info(f"[CONVERT] {input_path} ({source_format}) -> {output_path} ({target_format})")