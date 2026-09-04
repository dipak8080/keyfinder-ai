"""
pitch_changer.py - Pitch shift audio (independent of tempo) via the
rubberband CLI tool.

Deliberately NOT implemented via ffmpeg's asetrate/atempo tricks - those
change pitch by changing playback speed, which also alters tempo. This
module needs pitch changed WITHOUT tempo changing, which is exactly what
rubberband's time-stretch/pitch-shift algorithm is built for (and is why
tempo_changer.py, next step, will also use rubberband rather than a
naive ffmpeg speed change for its higher-quality path).

Output format matches input format (chain with /convert for format
changes).
"""
from config import logger, PITCH_SHIFT_MIN_SEMITONES, PITCH_SHIFT_MAX_SEMITONES
from audio_common import AudioToolError, run_rubberband


def shift_pitch(input_path: str, output_path: str, semitones: float) -> None:
    """
    Shifts input_path's pitch by `semitones` (positive = higher,
    negative = lower) without changing tempo/duration, writing the
    result to output_path (same format as input).

    Raises AudioToolError on out-of-range semitones or subprocess
    failure.
    """
    if semitones < PITCH_SHIFT_MIN_SEMITONES or semitones > PITCH_SHIFT_MAX_SEMITONES:
        raise AudioToolError(
            f"semitones must be between {PITCH_SHIFT_MIN_SEMITONES} and {PITCH_SHIFT_MAX_SEMITONES}."
        )

    # rubberband's --pitch flag takes a semitone value directly.
    # -c 5 (crispness) is rubberband's default general-purpose setting,
    # left explicit here rather than relying on its own default so
    # behavior can't silently change on a future rubberband version bump.
    run_rubberband(input_path, output_path, ["--pitch", str(semitones), "-c", "5"])

    logger.info(f"[PITCH] {input_path} ({semitones:+.1f} semitones) -> {output_path}")