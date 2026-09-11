"""
voice_cleaner.py - One-click speech cleanup: highpass for rumble,
RNNoise (ffmpeg arnndn) for background noise, dynaudnorm for level.
Output keeps the input's format and sample rate.

Model: rnnoise_bd.rnnn at the repo root (GregorR/rnnoise-models
"beguiling-drafter": voice signal, recording noise). Falls back to the
old afftdn chain with a warning if the file is missing.
"""
import os
import subprocess

from config import logger, FFMPEG_PATH, FFPROBE_PATH, FFPROBE_TIMEOUT_SECONDS
from audio_common import run_subprocess

RNNOISE_MODEL_PATH = os.environ.get("RNNOISE_MODEL_PATH", "/app/rnnoise_bd.rnnn").strip()

_RNNOISE_CHAIN = f"highpass=f=100,aresample=48000,arnndn=m={RNNOISE_MODEL_PATH},dynaudnorm"
_FALLBACK_CHAIN = "highpass=f=100,afftdn=nr=20:nf=-25,dynaudnorm"


def _probe_sample_rate(path: str):
    try:
        result = subprocess.run(
            [
                FFPROBE_PATH, "-v", "error",
                "-select_streams", "a:0",
                "-show_entries", "stream=sample_rate",
                "-of", "csv=p=0",
                path,
            ],
            capture_output=True,
            text=True,
            timeout=FFPROBE_TIMEOUT_SECONDS,
        )
        return int(result.stdout.strip().split(",")[0])
    except (subprocess.TimeoutExpired, ValueError, IndexError):
        return None


def clean_voice(input_path: str, output_path: str) -> None:
    cmd = [FFMPEG_PATH, "-y", "-i", input_path]

    if os.path.exists(RNNOISE_MODEL_PATH):
        engine = "rnnoise"
        cmd += ["-af", _RNNOISE_CHAIN]
        # arnndn only runs at 48 kHz, so restore the input's rate on the way out
        sample_rate = _probe_sample_rate(input_path)
        if sample_rate:
            cmd += ["-ar", str(sample_rate)]
    else:
        engine = "afftdn"
        logger.warning(f"[VOICE_CLEAN] RNNoise model missing at {RNNOISE_MODEL_PATH}, using afftdn fallback")
        cmd += ["-af", _FALLBACK_CHAIN]

    cmd.append(output_path)
    run_subprocess(cmd)

    logger.info(f"[VOICE_CLEAN] {input_path} ({engine}) -> {output_path}")