"""
separation.py - Runs Demucs (Meta/Facebook Research's open-source music
source separation model) to split an uploaded track into vocals and
instrumental ("no_vocals") WAV files.

Uses Demucs' built-in --two-stems=vocals mode rather than full 4-stem
separation (vocals/drums/bass/other) - roughly 2x faster on CPU and
matches exactly what the product needs (vocals vs. instrumental only).

STORAGE: finished stems are written to local disk (SEPARATION_DIR), NOT
R2 - see config.py's SEPARATION section for why. jobs.py handles TTL
cleanup of these files.

Demucs is invoked as a subprocess (its own CLI), not imported as a Python
library - this keeps this module simple and isolates a Demucs crash/hang
to a subprocess we can timeout and kill, rather than risking it taking
down the whole FastAPI worker thread it runs in.
"""
import os
import shutil
import subprocess
import uuid
from typing import Tuple

from config import (
    logger,
    SEPARATION_MODEL,
    SEPARATION_DIR,
    DEMUCS_TIMEOUT_SECONDS,
    MAX_SEPARATION_DURATION_SECONDS,
    FFMPEG_PATH,
)


class SeparationError(Exception):
    """Raised for any separation failure that should surface as a clean
    error to the caller (routes.py) - covers Demucs subprocess failures,
    timeouts, and duration-limit rejections alike."""
    pass


def get_audio_duration_seconds(file_path: str) -> float:
    """
    Uses ffprobe (bundled with ffmpeg) to read a file's duration WITHOUT
    decoding the audio - fast and cheap compared to letting Demucs
    discover a too-long file is a problem only after starting to process
    it.
    """
    ffprobe_path = FFMPEG_PATH.replace("ffmpeg", "ffprobe")
    try:
        result = subprocess.run(
            [
                ffprobe_path, "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                file_path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        return float(result.stdout.strip())
    except Exception as e:
        raise SeparationError(f"Could not read audio duration: {e}")


def run_separation(input_path: str, job_id: str) -> Tuple[str, str]:
    """
    Runs Demucs on input_path, returns (vocals_path, instrumental_path)
    on success. Fully synchronous/blocking (subprocess + waiting for
    Demucs to finish) - MUST be called via utils.run_blocking() from an
    async endpoint, same threading rule as every other blocking call in
    this codebase (yt-dlp, ffmpeg, Essentia).

    Raises SeparationError on any failure - duration limit exceeded,
    Demucs process failure, or timeout. Caller (routes.py) is responsible
    for calling jobs.mark_failed() with the error text.
    """
    duration = get_audio_duration_seconds(input_path)
    if duration > MAX_SEPARATION_DURATION_SECONDS:
        raise SeparationError(
            f"Track is {int(duration // 60)} min long, which exceeds the "
            f"{MAX_SEPARATION_DURATION_SECONDS // 60} min limit for separation."
        )

    # Demucs writes into a directory structure it creates itself:
    #   {output_root}/{model_name}/{input_filename_without_ext}/vocals.wav
    #   {output_root}/{model_name}/{input_filename_without_ext}/no_vocals.wav
    # Using job_id as a dedicated, unique output_root per job avoids any
    # collision between concurrent/adjacent jobs and makes cleanup trivial
    # (just delete the whole job_id directory tree when done).
    job_output_root = os.path.join(SEPARATION_DIR, f"_work_{job_id}")
    os.makedirs(job_output_root, exist_ok=True)

    cmd = [
        "demucs",
        "-n", SEPARATION_MODEL,
        "--two-stems", "vocals",
        "-o", job_output_root,
        input_path,
    ]

    logger.info(f"[SEPARATION] Starting Demucs for job {job_id}: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=DEMUCS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        shutil.rmtree(job_output_root, ignore_errors=True)
        raise SeparationError(
            f"Separation timed out after {DEMUCS_TIMEOUT_SECONDS}s. "
            f"Try a shorter track."
        )

    if result.returncode != 0:
        shutil.rmtree(job_output_root, ignore_errors=True)
        logger.error(f"[SEPARATION] Demucs failed for job {job_id}: {result.stderr[-2000:]}")
        raise SeparationError("Separation failed while processing the audio.")

    # Locate Demucs' output - it names the subfolder after the input
    # filename without extension, so we search rather than hardcoding the
    # exact path (robust to how the input file was actually named).
    input_stem = os.path.splitext(os.path.basename(input_path))[0]
    demucs_track_dir = os.path.join(job_output_root, SEPARATION_MODEL, input_stem)
    vocals_src = os.path.join(demucs_track_dir, "vocals.wav")
    instrumental_src = os.path.join(demucs_track_dir, "no_vocals.wav")

    if not (os.path.exists(vocals_src) and os.path.exists(instrumental_src)):
        shutil.rmtree(job_output_root, ignore_errors=True)
        raise SeparationError("Separation completed but output files were not found.")

    # Move (not copy) the two files we actually need straight into
    # SEPARATION_DIR under simple, predictable names, then delete the
    # whole Demucs working directory tree (which also contained the
    # original input file copy Demucs makes internally) - keeps
    # SEPARATION_DIR flat and avoids accumulating Demucs' intermediate
    # directory structure for every job.
    final_vocals_path = os.path.join(SEPARATION_DIR, f"{job_id}_vocals.wav")
    final_instrumental_path = os.path.join(SEPARATION_DIR, f"{job_id}_instrumental.wav")
    shutil.move(vocals_src, final_vocals_path)
    shutil.move(instrumental_src, final_instrumental_path)
    shutil.rmtree(job_output_root, ignore_errors=True)

    logger.info(f"[SEPARATION] Job {job_id} complete: {final_vocals_path}, {final_instrumental_path}")
    return final_vocals_path, final_instrumental_path