"""
echo_remover.py - Suppress echo/reverb tails via a noise-gate-based
approach.

IMPORTANT SCOPE NOTE: this is NOT a full acoustic dereverberation
(that requires a trained model, e.g. DeepFilterNet, which is out of
scope for this ffmpeg-only build). What this DOES do: gate out
low-level trailing reflections/echo using afftdn (light denoise pass)
followed by agate (suppresses audio below a threshold, which cuts off
quiet echo tails between/after spoken words). Effective for mild room
echo and repeated/slap echo on speech; won't fully remove heavy
reverb from a large room. Document this limitation to end users.

Output format matches input format (chain with /convert for format
changes).
"""
from config import logger, FFMPEG_PATH
from audio_common import run_subprocess


def remove_echo(input_path: str, output_path: str) -> None:
    """
    Runs a fixed echo-suppression filter chain on input_path, writing
    the result to output_path (same format as input):
      1. afftdn=nr=10:nf=-25   - light denoise pass, cleans up before
                                  gating so the gate isn't fooled by
                                  background hiss
      2. agate=threshold=0.03:ratio=9:attack=5:release=150
                                - gates out low-level audio (the quiet
                                  trailing echo/reflections between and
                                  after speech), while passing the main
                                  signal through untouched

    No parameters - fixed preset, same design reasoning as
    voice_cleaner.py. Raises AudioToolError (via run_subprocess) on
    ffmpeg failure.
    """
    cmd = [
        FFMPEG_PATH, "-y",
        "-i", input_path,
        "-af", "afftdn=nr=10:nf=-25,agate=threshold=0.03:ratio=9:attack=5:release=150",
        output_path,
    ]

    run_subprocess(cmd)

    logger.info(f"[ECHO_REMOVE] {input_path} -> {output_path}")