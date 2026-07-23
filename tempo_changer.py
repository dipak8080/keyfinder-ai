"""
tempo_changer.py - Change audio tempo/speed (independent of pitch) via
the rubberband CLI tool.

Deliberately NOT implemented via ffmpeg's atempo filter alone - atempo
changes speed cleanly for small ranges but is chained/limited for larger
factors, and more importantly this app wants tempo and pitch changes to
share one consistent, high-quality engine (rubberband) rather than two
different algorithms with different quality characteristics. Same
reasoning as pitch_changer.py's choice, in reverse: rubberband's
--tempo mode changes speed without touching pitch.

Output format matches input format (chain with /convert for format
changes).
"""
from config import logger, RUBBERBAND_PATH, TEMPO_MIN_FACTOR, TEMPO_MAX_FACTOR
from audio_common import AudioToolError, run_subprocess


def change_tempo(input_path: str, output_path: str, tempo_factor: float) -> None:
    """
    Changes input_path's tempo by `tempo_factor` (1.0 = unchanged,
    2.0 = twice as fast, 0.5 = half speed) without changing pitch,
    writing the result to output_path (same format as input).

    Raises AudioToolError on out-of-range factor or subprocess failure.
    """
    if tempo_factor < TEMPO_MIN_FACTOR or tempo_factor > TEMPO_MAX_FACTOR:
        raise AudioToolError(
            f"tempo_factor must be between {TEMPO_MIN_FACTOR} and {TEMPO_MAX_FACTOR}."
        )

    # rubberband's --tempo flag takes a speed multiplier directly
    # (matches this function's tempo_factor semantics 1:1, no
    # conversion needed). -c 5 same as pitch_changer.py, kept explicit
    # for the same future-proofing reason.
    cmd = [
        RUBBERBAND_PATH,
        "--tempo", str(tempo_factor),
        "-c", "5",
        input_path,
        output_path,
    ]

    run_subprocess(cmd)

    logger.info(f"[TEMPO] {input_path} (x{tempo_factor:.2f}) -> {output_path}")