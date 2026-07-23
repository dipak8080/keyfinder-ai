"""
silence_remover.py - Strip silent gaps throughout the audio (not just
leading/trailing) via ffmpeg's silenceremove filter.

Useful for podcast/voice-memo editing where dead air needs trimming
out. Two params expose the actual tuning knobs users would want:
threshold_db (how quiet counts as "silence") and min_duration_seconds
(how long a gap has to be before it's cut).

Output format matches input format (chain with /convert for format
changes).
"""
from config import (
    logger,
    FFMPEG_PATH,
    SILENCE_THRESHOLD_MIN_DB,
    SILENCE_THRESHOLD_MAX_DB,
    SILENCE_MIN_DURATION_SECONDS,
    SILENCE_MAX_DURATION_SECONDS,
)
from audio_common import AudioToolError, run_subprocess


def remove_silence(input_path: str, output_path: str, threshold_db: float, min_duration_seconds: float) -> None:
    """
    Removes silent gaps of at least min_duration_seconds, where
    "silence" is anything at or below threshold_db, throughout
    input_path, writing the result to output_path (same format as
    input).

    stop_periods=-1 means EVERY qualifying silent gap is removed
    (not just leading/trailing) - start_periods=1 handles a silent
    lead-in, stop_periods=-1 handles all interior + trailing gaps.

    Raises AudioToolError on out-of-range params or ffmpeg failure.
    """
    if threshold_db < SILENCE_THRESHOLD_MIN_DB or threshold_db > SILENCE_THRESHOLD_MAX_DB:
        raise AudioToolError(
            f"threshold_db must be between {SILENCE_THRESHOLD_MIN_DB} and {SILENCE_THRESHOLD_MAX_DB}."
        )
    if min_duration_seconds < SILENCE_MIN_DURATION_SECONDS or min_duration_seconds > SILENCE_MAX_DURATION_SECONDS:
        raise AudioToolError(
            f"min_duration_seconds must be between {SILENCE_MIN_DURATION_SECONDS} and {SILENCE_MAX_DURATION_SECONDS}."
        )

    silence_filter = (
        f"silenceremove="
        f"start_periods=1:start_silence={min_duration_seconds}:start_threshold={threshold_db}dB:"
        f"stop_periods=-1:stop_silence={min_duration_seconds}:stop_threshold={threshold_db}dB:"
        f"detection=peak"
    )

    cmd = [
        FFMPEG_PATH, "-y",
        "-i", input_path,
        "-af", silence_filter,
        output_path,
    ]

    run_subprocess(cmd)

    logger.info(
        f"[SILENCE_REMOVE] {input_path} (threshold={threshold_db}dB, "
        f"min_duration={min_duration_seconds}s) -> {output_path}"
    )