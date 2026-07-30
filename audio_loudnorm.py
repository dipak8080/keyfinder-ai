"""
audio_loudnorm.py - Normalizes a track to a target integrated loudness
(LUFS) using ffmpeg's loudnorm filter, run TWO-PASS.

WHY TWO PASSES, NOT ONE: loudnorm can run in a single pass, but its
single-pass mode is a real-time approximation - it estimates the
correction as it streams through the file rather than knowing the whole
file's loudness distribution in advance. The two-pass mode runs a first
analysis-only pass to measure the file's actual integrated loudness,
true peak, and loudness range, then feeds those exact measured values
into a second pass that applies the correction. The difference is
real: single-pass loudnorm can miss the target by a full LU or more on
tracks with uneven dynamics, which defeats the entire point of a tool
whose job is hitting a specific number. Two-pass costs one extra
full-file decode; on a CPU-bound box that's a trivial price for a
result that's actually correct.

PRESETS vs CUSTOM: most producers think in terms of "streaming" or
"club", not a raw LUFS number, so the route accepts a preset name by
default and a bounded custom_lufs override for anyone who wants a
specific figure. Both paths converge on the same run_loudnorm() call -
the preset is resolved to a number before this module ever sees it.

Same subprocess-per-call pattern as every other tool here, and
normalize_loudness() is blocking - it MUST be dispatched via
utils.run_blocking() from the async route.
"""
import json
import os
import subprocess
from typing import Tuple

from config import (
    logger,
    FFMPEG_PATH,
    LOUDNORM_PRESETS,
    LOUDNORM_MIN_LUFS,
    LOUDNORM_MAX_LUFS,
    LOUDNORM_TRUE_PEAK,
    LOUDNORM_LRA,
    LOUDNORM_ANALYSIS_TIMEOUT_SECONDS,
    LOUDNORM_APPLY_TIMEOUT_SECONDS,
)
from audio_common import AudioToolError


def resolve_target_lufs(preset: str = None, custom_lufs: float = None) -> float:
    """
    Turns a (preset, custom_lufs) pair from the route into a single
    target LUFS value.

    custom_lufs takes priority if provided - it's the explicit "I know
    exactly what number I want" path. Otherwise falls back to the named
    preset. Raises AudioToolError for an unknown preset name or a
    custom value outside the sane bounds, rather than silently clamping
    it - a normalization target that's silently different from what the
    user asked for is a worse failure mode than a clear rejection.
    """
    if custom_lufs is not None:
        if custom_lufs < LOUDNORM_MIN_LUFS or custom_lufs > LOUDNORM_MAX_LUFS:
            raise AudioToolError(
                f"custom_lufs must be between {LOUDNORM_MIN_LUFS} and {LOUDNORM_MAX_LUFS}."
            )
        return custom_lufs

    preset = (preset or "streaming").strip().lower()
    if preset not in LOUDNORM_PRESETS:
        raise AudioToolError(
            f"preset must be one of: {', '.join(sorted(LOUDNORM_PRESETS.keys()))}"
        )
    return LOUDNORM_PRESETS[preset]


def _measure_loudness(input_path: str, target_lufs: float) -> dict:
    """
    Pass 1: analysis only. Runs loudnorm with print_format=json and no
    real output file (-f null) - ffmpeg decodes the whole track and
    measures its actual integrated loudness, true peak, and loudness
    range, then prints those measurements as JSON on stderr rather than
    writing any audio. Nothing here touches disk beyond reading the
    input.
    """
    cmd = [
        FFMPEG_PATH, "-i", input_path,
        "-af", f"loudnorm=I={target_lufs}:TP={LOUDNORM_TRUE_PEAK}:LRA={LOUDNORM_LRA}:print_format=json",
        "-f", "null", "-",
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=LOUDNORM_ANALYSIS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise AudioToolError("Timed out while analyzing the track's loudness.")

    # loudnorm's JSON report is printed to stderr, mixed in with ffmpeg's
    # normal log lines - the { ... } block is the last such structure in
    # the output, so it's extracted by finding the outermost braces
    # rather than assuming a fixed line count.
    stderr = result.stderr
    start = stderr.rfind("{")
    end = stderr.rfind("}")

    if start == -1 or end == -1 or end < start:
        logger.error(f"[LOUDNORM] Could not find measurement JSON in ffmpeg output: {stderr[-2000:]}")
        raise AudioToolError("Could not measure this track's loudness.")

    try:
        return json.loads(stderr[start:end + 1])
    except json.JSONDecodeError as e:
        logger.error(f"[LOUDNORM] Failed to parse measurement JSON: {e} - raw: {stderr[start:end + 1][:500]}")
        raise AudioToolError("Could not measure this track's loudness.")


def normalize_loudness(input_path: str, output_path: str, target_lufs: float) -> Tuple[float, float]:
    """
    Runs the full two-pass normalization. Returns
    (measured_input_lufs, output_lufs_target) so the route can tell the
    user both where the track started and what it was corrected to -
    "was -8.2 LUFS, normalized to -14 LUFS" is a genuinely useful result
    line, not just a success/fail flag.

    Raises AudioToolError on any measurement or encode failure.
    """
    measured = _measure_loudness(input_path, target_lufs)

    required_keys = ("input_i", "input_tp", "input_lra", "input_thresh")
    if not all(k in measured for k in required_keys):
        raise AudioToolError("Loudness measurement returned an unexpected result.")

    # Pass 2: apply the correction using the EXACT measured values from
    # pass 1 (linear=true tells loudnorm to use them directly rather than
    # re-measuring), rather than letting loudnorm re-estimate from
    # scratch in a single combined pass - this is the step that actually
    # delivers the "hits the target accurately" promise.
    filter_str = (
        f"loudnorm=I={target_lufs}:TP={LOUDNORM_TRUE_PEAK}:LRA={LOUDNORM_LRA}:"
        f"measured_I={measured['input_i']}:"
        f"measured_TP={measured['input_tp']}:"
        f"measured_LRA={measured['input_lra']}:"
        f"measured_thresh={measured['input_thresh']}:"
        f"linear=true:print_format=summary"
    )

    cmd = [FFMPEG_PATH, "-y", "-i", input_path, "-af", filter_str, output_path]

    logger.info(
        f"[LOUDNORM] Normalizing {input_path}: measured {measured['input_i']} LUFS "
        f"-> target {target_lufs} LUFS"
    )

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=LOUDNORM_APPLY_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise AudioToolError(
            f"Normalization timed out after {LOUDNORM_APPLY_TIMEOUT_SECONDS}s. Try a shorter file."
        )

    if result.returncode != 0:
        logger.error(f"[LOUDNORM] ffmpeg apply pass failed: {result.stderr[-2000:]}")
        raise AudioToolError("Failed to normalize this track's loudness.")

    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        raise AudioToolError("Normalization produced an empty file.")

    return round(float(measured["input_i"]), 1), target_lufs