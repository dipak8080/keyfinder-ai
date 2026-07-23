"""
voice_cleaner.py - Speech-optimized cleanup preset: cuts low-frequency
rumble, reduces background noise with settings tuned for voice
(rather than noise_remover.py's general-purpose default), and
normalizes loudness for consistent playback level.

Unlike noise_remover.py (single filter, user-tunable strength), this is
a fixed three-stage chain - the point of a dedicated "voice cleaner" is
a good-by-default one-click result for speech, not another knob to
tune. Power users who want fine control should use /noise-remove
directly instead.

Output format matches input format (chain with /convert for format
changes).
"""
from config import logger, FFMPEG_PATH
from audio_common import run_subprocess


def clean_voice(input_path: str, output_path: str) -> None:
    """
    Runs a fixed speech-cleanup filter chain on input_path, writing the
    result to output_path (same format as input):
      1. highpass=f=100   - cuts rumble/handling noise below typical
                             speech fundamental frequency
      2. afftdn=nr=20:nf=-25 - moderate denoise, tuned stronger than
                             noise_remover's default since speech
                             recordings (phone/laptop mic) typically
                             have more consistent background noise to
                             remove than music
      3. dynaudnorm       - smooths out volume swings so quiet/loud
                             passages land at a more consistent level,
                             which plain afftdn/highpass alone don't do

    No parameters - this is a fixed, good-by-default preset (see
    module docstring). Raises AudioToolError (via run_subprocess) on
    ffmpeg failure.
    """
    cmd = [
        FFMPEG_PATH, "-y",
        "-i", input_path,
        "-af", "highpass=f=100,afftdn=nr=20:nf=-25,dynaudnorm",
        output_path,
    ]

    run_subprocess(cmd)

    logger.info(f"[VOICE_CLEAN] {input_path} -> {output_path}")