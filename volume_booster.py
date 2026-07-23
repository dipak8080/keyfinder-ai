"""
volume_booster.py - Adjust audio gain (boost or reduce volume) via
ffmpeg's `volume` filter.

Business logic only - no HTTP/FastAPI here, same pattern as
audio_converter.py / audio_cutter.py. Output format matches input
format (chain with /convert if a format change is also needed).
"""
from config import logger, FFMPEG_PATH, VOLUME_GAIN_MIN_DB, VOLUME_GAIN_MAX_DB
from audio_common import AudioToolError, run_subprocess


def apply_volume_gain(input_path: str, output_path: str, gain_db: float) -> None:
    """
    Applies a gain adjustment of gain_db decibels to input_path, writing
    the result to output_path (same format as input).

    Positive gain_db boosts volume, negative reduces it. Range is
    enforced against VOLUME_GAIN_MIN_DB/MAX_DB (config.py) - this is a
    sanity/abuse guard, not a technical ffmpeg limitation: extreme boosts
    (e.g. +80dB) just produce unusable, heavily clipped/distorted audio,
    so there's no legitimate use case for allowing values outside this
    range.

    Raises AudioToolError on out-of-range gain or ffmpeg failure.
    """
    if gain_db < VOLUME_GAIN_MIN_DB or gain_db > VOLUME_GAIN_MAX_DB:
        raise AudioToolError(
            f"gain_db must be between {VOLUME_GAIN_MIN_DB} and {VOLUME_GAIN_MAX_DB}."
        )

    cmd = [
        FFMPEG_PATH, "-y",
        "-i", input_path,
        "-af", f"volume={gain_db}dB",
        output_path,
    ]

    run_subprocess(cmd)

    logger.info(f"[VOLUME] {input_path} ({gain_db:+.1f}dB) -> {output_path}")