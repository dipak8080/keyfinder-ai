"""
noise_remover.py - Reduce background noise (hiss, hum, static) via
ffmpeg's afftdn (FFT denoiser) filter.

Uses afftdn rather than the higher-quality arnndn (RNNoise) filter -
arnndn needs an external trained model file shipped with the app,
afftdn is built into ffmpeg with no extra dependency. Good enough for
general hiss/hum reduction; if noticeably better quality is needed
later, arnndn is a drop-in filter swap once a model file is added to
the Dockerfile.

Output format matches input format (chain with /convert for format
changes).
"""
from config import logger, FFMPEG_PATH, NOISE_REDUCTION_MIN_STRENGTH, NOISE_REDUCTION_MAX_STRENGTH
from audio_common import AudioToolError, run_subprocess


def remove_noise(input_path: str, output_path: str, strength: float) -> None:
    """
    Reduces background noise in input_path, writing the result to
    output_path (same format as input). strength maps directly to
    afftdn's nr (noise reduction amount) parameter.

    Raises AudioToolError on out-of-range strength or ffmpeg failure.
    """
    if strength < NOISE_REDUCTION_MIN_STRENGTH or strength > NOISE_REDUCTION_MAX_STRENGTH:
        raise AudioToolError(
            f"strength must be between {NOISE_REDUCTION_MIN_STRENGTH} and {NOISE_REDUCTION_MAX_STRENGTH}."
        )

    cmd = [
        FFMPEG_PATH, "-y",
        "-i", input_path,
        "-af", f"afftdn=nr={strength}",
        output_path,
    ]

    run_subprocess(cmd)

    logger.info(f"[NOISE] {input_path} (strength={strength}) -> {output_path}")