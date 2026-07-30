"""
separation.py - Runs Demucs (Meta/Facebook Research's open-source music
source separation model) over an uploaded track, in one of two output
shapes:

  run_separation()      -> (vocals_path, instrumental_path)
                           Uses --two-stems=vocals. Powers /separate and
                           /separate-hq (the vocal remover).

  run_stem_separation() -> {"vocals": path, "drums": path, "bass": path,
                            "other": path}
                           No --two-stems flag. Powers /stems and
                           /stems-hq (the full stem splitter).

IMPORTANT: these two cost the SAME CPU time. --two-stems=vocals does not
make Demucs do less work - it separates every source internally either
way and simply sums the non-vocal ones into no_vocals.wav for us. The
flag decides what we get back, not how much compute happens. That's why
both share one set of tunables in config.py and one semaphore in
routes.py.

Every run knob (model, overlap, timeout, duration cap) is a PARAMETER
rather than a module-level constant read at import time, so a single code
path serves both the standard (htdemucs) and high-quality (htdemucs_ft +
raised overlap) tiers. Defaults match the standard tier, so a caller that
passes nothing keeps the original behaviour exactly.

STORAGE: finished stems are written to local disk (SEPARATION_DIR), NOT
R2 - see config.py's SEPARATION section for why. jobs.py handles TTL
cleanup of these files, including walking the stems dict.

Demucs is invoked as a subprocess (its own CLI), not imported as a Python
library - this keeps this module simple and isolates a Demucs crash/hang
to a subprocess we can timeout and kill, rather than risking it taking
down the whole FastAPI worker thread it runs in.
"""
import os
import shutil
import subprocess
from typing import Dict, Tuple

from config import (
    logger,
    ALLOWED_SEPARATION_MODELS,
    MODEL_STEM_NAMES,
    SEPARATION_MODEL,
    SEPARATION_OVERLAP,
    SEPARATION_DIR,
    DEMUCS_TIMEOUT_SECONDS,
    MAX_SEPARATION_DURATION_SECONDS,
    FFMPEG_PATH,
)


class SeparationError(Exception):
    """Raised for any separation failure that should surface as a clean
    error to the caller (routes.py) - covers Demucs subprocess failures,
    timeouts, disallowed models, and duration-limit rejections alike."""
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


def _run_demucs(
    input_path: str,
    job_id: str,
    model: str,
    overlap: float,
    timeout_seconds: int,
    max_duration_seconds: int,
    two_stems: bool,
) -> Tuple[str, str]:
    """
    Shared engine for both public entry points: validates, invokes the
    Demucs CLI, and returns (job_output_root, demucs_track_dir) - the
    working directory tree and the specific folder Demucs wrote its
    output files into.

    The CALLER is responsible for moving the files it wants out of
    demucs_track_dir and then deleting job_output_root. On any failure
    here, this function cleans up job_output_root itself before raising,
    so a failed job never leaves a work tree behind.
    """
    # Defence in depth. config.py already validates env-supplied model
    # names, but this arg is reachable from route code, and the value
    # lands directly in a subprocess arg list - a string starting with
    # "-" would be read by Demucs as a FLAG rather than a model name.
    # Cheap check, closes the whole class of arg-position confusion.
    if model not in ALLOWED_SEPARATION_MODELS:
        logger.error(f"[SEPARATION] Job {job_id} rejected - disallowed model '{model}'")
        raise SeparationError("Separation failed: unsupported model requested.")

    duration = get_audio_duration_seconds(input_path)
    if duration > max_duration_seconds:
        raise SeparationError(
            f"Track is {int(duration // 60)} min long, which exceeds the "
            f"{max_duration_seconds // 60} min limit for separation."
        )

    # Demucs writes into a directory structure it creates itself:
    #   {output_root}/{model_name}/{input_filename_without_ext}/<stem>.wav
    # Using job_id as a dedicated, unique output_root per job avoids any
    # collision between concurrent/adjacent jobs and makes cleanup trivial
    # (just delete the whole job_id directory tree when done).
    job_output_root = os.path.join(SEPARATION_DIR, f"_work_{job_id}")
    os.makedirs(job_output_root, exist_ok=True)

    cmd = ["demucs", "-n", model]
    if two_stems:
        cmd += ["--two-stems", "vocals"]
    cmd += ["--overlap", str(overlap), "-o", job_output_root, input_path]

    logger.info(
        f"[SEPARATION] Starting Demucs for job {job_id} "
        f"(model={model}, overlap={overlap}, two_stems={two_stems}, "
        f"timeout={timeout_seconds}s, duration={duration:.1f}s): {' '.join(cmd)}"
    )

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        shutil.rmtree(job_output_root, ignore_errors=True)
        raise SeparationError(
            f"Separation timed out after {timeout_seconds}s. "
            f"Try a shorter track."
        )

    if result.returncode != 0:
        shutil.rmtree(job_output_root, ignore_errors=True)
        logger.error(f"[SEPARATION] Demucs failed for job {job_id}: {result.stderr[-2000:]}")
        raise SeparationError("Separation failed while processing the audio.")

    # Demucs names the subfolder after the input filename without its
    # extension, so derive it rather than hardcoding (robust to how the
    # input file was actually named).
    #
    # The model-name level of this path MUST come from the `model` we
    # actually ran, not from the SEPARATION_MODEL constant - otherwise
    # every HQ (htdemucs_ft) job would look for its output under
    # .../htdemucs/... , find nothing, and fail with "output files were
    # not found" after burning 15+ minutes of CPU.
    input_stem = os.path.splitext(os.path.basename(input_path))[0]
    demucs_track_dir = os.path.join(job_output_root, model, input_stem)

    return job_output_root, demucs_track_dir


def _missing_output_error(job_id: str, job_output_root: str, demucs_track_dir: str, model: str):
    """Logs where we looked and what was actually there, then cleans up
    and raises. Without the directory listing this failure mode is
    near-impossible to diagnose from logs alone - it looks identical
    whether Demucs changed its output layout, wrote a different model
    folder, or produced nothing at all."""
    logger.error(
        f"[SEPARATION] Job {job_id} produced no usable output at {demucs_track_dir} "
        f"(model={model}) - contents of {job_output_root}: "
        f"{os.listdir(job_output_root) if os.path.isdir(job_output_root) else 'missing'}"
    )
    shutil.rmtree(job_output_root, ignore_errors=True)
    raise SeparationError("Separation completed but output files were not found.")


def run_separation(
    input_path: str,
    job_id: str,
    model: str = SEPARATION_MODEL,
    overlap: float = SEPARATION_OVERLAP,
    timeout_seconds: int = DEMUCS_TIMEOUT_SECONDS,
    max_duration_seconds: int = MAX_SEPARATION_DURATION_SECONDS,
) -> Tuple[str, str]:
    """
    Two-stem (vocal remover) mode. Returns (vocals_path,
    instrumental_path) on success.

    Fully synchronous/blocking (subprocess + waiting for Demucs to
    finish) - MUST be called via utils.run_blocking() from an async
    endpoint, same threading rule as every other blocking call in this
    codebase (yt-dlp, ffmpeg, Essentia).

    The four tunables default to the standard (fast) tier's config
    values. routes.py passes the HQ set explicitly for /separate-hq.
    They're resolved by the CALLER at job submission time, not read
    here, so a config change mid-job can't alter the behaviour of a run
    that's already in flight.

    Raises SeparationError on any failure - disallowed model, duration
    limit exceeded, Demucs process failure, or timeout. Caller
    (routes.py) is responsible for calling jobs.mark_failed() with the
    error text.
    """
    job_output_root, demucs_track_dir = _run_demucs(
        input_path, job_id, model, overlap, timeout_seconds, max_duration_seconds,
        two_stems=True,
    )

    vocals_src = os.path.join(demucs_track_dir, "vocals.wav")
    instrumental_src = os.path.join(demucs_track_dir, "no_vocals.wav")

    if not (os.path.exists(vocals_src) and os.path.exists(instrumental_src)):
        _missing_output_error(job_id, job_output_root, demucs_track_dir, model)

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

    logger.info(
        f"[SEPARATION] Job {job_id} complete (model={model}): "
        f"{final_vocals_path}, {final_instrumental_path}"
    )
    return final_vocals_path, final_instrumental_path


def run_stem_separation(
    input_path: str,
    job_id: str,
    model: str = SEPARATION_MODEL,
    overlap: float = SEPARATION_OVERLAP,
    timeout_seconds: int = DEMUCS_TIMEOUT_SECONDS,
    max_duration_seconds: int = MAX_SEPARATION_DURATION_SECONDS,
) -> Dict[str, str]:
    """
    Full multi-stem mode. Returns a {stem_name: path} dict - four entries
    (vocals/drums/bass/other) for htdemucs and htdemucs_ft, six if a
    6-source model is ever used.

    Same blocking/threading rules and same failure semantics as
    run_separation() above, and the same CPU cost - the only differences
    are the omitted --two-stems flag and the fact that four files are
    kept instead of two summed into one.

    Which stems to expect comes from config's MODEL_STEM_NAMES rather
    than a hardcoded list, so this function needs no change to support a
    model with a different stem set.
    """
    expected_stems = MODEL_STEM_NAMES.get(model)
    if not expected_stems:
        # Reachable only if a model is added to ALLOWED_SEPARATION_MODELS
        # without a matching MODEL_STEM_NAMES entry. Caught here, before
        # spending any CPU, rather than after the run when the output
        # lookup would fail for a much less obvious reason.
        logger.error(f"[STEMS] Job {job_id} rejected - no stem list configured for model '{model}'")
        raise SeparationError("Separation failed: unsupported model requested.")

    job_output_root, demucs_track_dir = _run_demucs(
        input_path, job_id, model, overlap, timeout_seconds, max_duration_seconds,
        two_stems=False,
    )

    sources = {stem: os.path.join(demucs_track_dir, f"{stem}.wav") for stem in expected_stems}

    # All-or-nothing: a partial result would give the frontend a stem
    # list with dead entries in it, so treat any missing file as a
    # failed job rather than returning what happened to survive.
    if not all(os.path.exists(path) for path in sources.values()):
        _missing_output_error(job_id, job_output_root, demucs_track_dir, model)

    final_paths = {}
    for stem, src in sources.items():
        dest = os.path.join(SEPARATION_DIR, f"{job_id}_{stem}.wav")
        shutil.move(src, dest)
        final_paths[stem] = dest

    shutil.rmtree(job_output_root, ignore_errors=True)

    logger.info(
        f"[STEMS] Job {job_id} complete (model={model}, {len(final_paths)} stems): "
        f"{', '.join(final_paths.keys())}"
    )
    return final_paths