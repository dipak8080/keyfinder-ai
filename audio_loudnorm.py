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
specific figure. Both paths converge on the same normalize_loudness()
call - the preset is resolved to a number before this module ever sees
it.

Same subprocess-per-call pattern as every other tool here, and
normalize_loudness() is blocking - it MUST be dispatched via
utils.run_blocking() from the async route.

--------------------------------------------------------------------------
WHAT CHANGED (2026-08-22): PASS 1 COULD PRODUCE VALUES PASS 2 REFUSED

Production failure on a 4.8-second .ogg:

    [LOUDNORM] Normalizing ...: measured 0.14 LUFS -> target -14.0 LUFS
    [Parsed_loudnorm_0] Value 0.140000 for parameter 'measured_I'
    out of range [-99 - 0]
    Error applying option 'measured_I' to filter 'loudnorm':
    Numerical result out of range

ffmpeg measured the file at +0.14 LUFS in pass 1, then rejected its own
measurement in pass 2 because measured_I only accepts [-99, 0]. The two
passes disagree about what is a legal number, and this module was
passing pass 1's output through untouched.

A positive integrated loudness means the file is slammed to full scale -
brickwall-limited, effectively clipping. That is not exotic: it is
normal for short meme clips, sound effects, and anything mastered for
maximum loudness. So this is a whole CLASS of ordinary file that could
never be normalized, which is unfortunate given normalizing is exactly
what such a file needs most.

Three things were added, and all three are about the same principle -
a measurement is an observation, not a promise that the observation is
in range:

  1. Every measured value is now clamped into the range ffmpeg will
     actually accept (_LOUDNORM_PARAM_RANGES / _clamp_measured below).
     measured_I was the one that fired, but it is not the only one that
     can: measured_LRA, measured_TP and measured_thresh each have their
     own bounds and any of them can land outside on a pathological file.
  2. Non-finite measurements ("-inf" on a silent file, "nan" on a
     corrupt one) are detected and rejected with a message that says
     what is actually wrong, instead of being clamped to -99 and
     triggering an ~85 dB gain on what is essentially noise.
  3. Both ffmpeg commands now go through as_audio_only_ffmpeg(). This
     module builds and runs its own subprocess calls rather than using
     audio_common.run_subprocess() - pass 1 needs the stderr that
     run_subprocess() discards - so it had silently missed the -vn fix
     applied everywhere else the same day. An .ogg or .mp3 with embedded
     cover art would have failed here for a completely different reason
     than the one above.
--------------------------------------------------------------------------
"""
import json
import math
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
from audio_common import AudioToolError, as_audio_only_ffmpeg


# Accepted ranges for loudnorm's measured_* parameters, as enforced by
# ffmpeg's own filter option parser (libavfilter/af_loudnorm.c).
#
# These are FFMPEG'S limits, not this app's policy, which is why they
# are hardcoded here rather than exposed in config.py. Making them
# env-tunable would invite someone to "fix" a rejection by widening a
# bound ffmpeg is going to enforce regardless.
#
# The asymmetry is not arbitrary. Integrated loudness and the gating
# threshold are dBFS-referenced and cannot exceed 0 for a signal that
# isn't clipping outright. True peak CAN legitimately exceed 0 on an
# inter-sample peak, so it is allowed positive. LRA is a range - a
# difference between two loudness values - so it can only be positive.
_LOUDNORM_PARAM_RANGES = {
    "measured_I": (-99.0, 0.0),
    "measured_LRA": (0.0, 99.0),
    "measured_TP": (-99.0, 99.0),
    "measured_thresh": (-99.0, 0.0),
}

# Maps the keys loudnorm PRINTS in pass 1 to the parameter names it
# ACCEPTS in pass 2. They are not the same strings, which is its own
# small trap - "input_i" is printed, "measured_I" is accepted.
_MEASURED_KEY_MAP = {
    "input_i": "measured_I",
    "input_lra": "measured_LRA",
    "input_tp": "measured_TP",
    "input_thresh": "measured_thresh",
}


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

    Worth contrasting with _clamp_measured() below, which DOES clamp
    silently, because the two are opposite situations. This value is
    what the USER ASKED FOR: changing it without saying so would hand
    back a file that isn't what they requested. A measured value is what
    the FILE TURNED OUT TO BE: nudging +0.14 to 0.0 changes the applied
    gain by a seventh of a decibel, which nobody can hear, and the only
    alternative is refusing to process the file at all.
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


def _parse_measured_value(raw, printed_key: str) -> float:
    """
    Turns one value from loudnorm's JSON report into a usable float.

    loudnorm prints its measurements as JSON STRINGS, not numbers, and
    some of those strings are not parseable as finite floats:

        "input_i": "-inf"    - the file is digital silence
        "input_i": "nan"     - the measurement failed outright

    float() accepts both of those happily and returns -inf / nan, which
    then propagate silently into an f-string and reach ffmpeg as the
    literal text "-inf". Catching them here, where there is still enough
    context to say something useful, is the difference between "this
    track appears to be silent" and a numeric filter error the user
    cannot act on.

    Silence in particular must NOT be clamped to the -99 floor: that
    would tell loudnorm the file sits at -99 LUFS and ask it to lift the
    result by ~85 dB, turning dither and noise into a full-scale hiss.
    Refusing is the only correct answer.
    """
    try:
        value = float(raw)
    except (TypeError, ValueError):
        logger.error(f"[LOUDNORM] Measurement '{printed_key}' was not numeric: {raw!r}")
        raise AudioToolError("Could not measure this track's loudness.")

    if math.isnan(value):
        logger.error(f"[LOUDNORM] Measurement '{printed_key}' came back as NaN.")
        raise AudioToolError(
            "Could not measure this track's loudness. The file may be corrupt."
        )

    if math.isinf(value):
        logger.warning(
            f"[LOUDNORM] Measurement '{printed_key}' is {value} - the track contains "
            f"no measurable audio (digital silence)."
        )
        raise AudioToolError(
            "This track appears to be silent, so there's no loudness to normalize."
        )

    return value


def _clamp_measured(param: str, value: float) -> float:
    """
    Clamps one measured value into the range ffmpeg's pass 2 accepts,
    logging whenever it actually has to.

    Clamping rather than rejecting is the right call here and the
    reasoning is worth keeping: the value came from ffmpeg's OWN
    measurement of the file, so refusing it means refusing a file
    because ffmpeg disagreed with itself. The correction that clamping
    introduces is bounded by how far outside the range the value was -
    +0.14 becoming 0.0 shifts the applied gain by 0.14 dB, which is
    inaudible and in the conservative direction (very slightly quieter
    than the exact target).

    Logged at WARNING, not INFO: it is normal, it is handled, and it is
    still worth being able to grep for. A sudden run of these would mean
    something has changed about the files people are uploading.
    """
    low, high = _LOUDNORM_PARAM_RANGES[param]
    if value < low or value > high:
        clamped = min(max(value, low), high)
        logger.warning(
            f"[LOUDNORM] {param}={value} is outside ffmpeg's accepted range "
            f"[{low}, {high}] - clamping to {clamped}. This is normal for "
            f"heavily limited or near-silent material."
        )
        return clamped
    return value


def _measure_loudness(input_path: str, target_lufs: float) -> dict:
    """
    Pass 1: analysis only. Runs loudnorm with print_format=json and no
    real output file (-f null) - ffmpeg decodes the whole track and
    measures its actual integrated loudness, true peak, and loudness
    range, then prints those measurements as JSON on stderr rather than
    writing any audio. Nothing here touches disk beyond reading the
    input.
    """
    cmd = as_audio_only_ffmpeg([
        FFMPEG_PATH, "-i", input_path,
        "-af", f"loudnorm=I={target_lufs}:TP={LOUDNORM_TRUE_PEAK}:LRA={LOUDNORM_LRA}:print_format=json",
        "-f", "null", "-",
    ])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=LOUDNORM_ANALYSIS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise AudioToolError("Timed out while analyzing the track's loudness.")

    # Checked BEFORE parsing. A failed analysis pass can still leave a
    # brace-delimited fragment somewhere in ffmpeg's banner output, so
    # going straight to the JSON hunt below would report "could not find
    # measurements" for what is really a decode failure - a much less
    # useful thing to be told, and a much harder one to debug from a log.
    if result.returncode != 0:
        logger.error(f"[LOUDNORM] Analysis pass failed (exit {result.returncode}): {result.stderr[-2000:]}")
        raise AudioToolError(
            "Could not read this file as valid audio. It may be corrupt or in an "
            "unsupported format."
        )

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

    The returned measured value is the REAL one, before clamping. If a
    file measured +0.14 LUFS, that is what the user is told, because it
    is the true fact about their file - the clamp exists to satisfy
    ffmpeg's parser, not to rewrite history.

    Raises AudioToolError on any measurement or encode failure.
    """
    measured = _measure_loudness(input_path, target_lufs)

    missing = [k for k in _MEASURED_KEY_MAP if k not in measured]
    if missing:
        logger.error(f"[LOUDNORM] Measurement JSON missing keys {missing}: {measured}")
        raise AudioToolError("Loudness measurement returned an unexpected result.")

    # Parse (rejecting inf/nan), then clamp into ffmpeg's accepted range.
    # Order matters: clamping first would turn "-inf" into a plausible
    # -99 and lose the fact that the file is silent.
    params = {}
    for printed_key, param_name in _MEASURED_KEY_MAP.items():
        value = _parse_measured_value(measured[printed_key], printed_key)
        params[param_name] = _clamp_measured(param_name, value)

    true_measured_i = float(measured["input_i"])

    # Pass 2: apply the correction using the measured values from pass 1
    # (linear=true tells loudnorm to use them directly rather than
    # re-measuring), rather than letting loudnorm re-estimate from
    # scratch in a single combined pass - this is the step that actually
    # delivers the "hits the target accurately" promise.
    filter_str = (
        f"loudnorm=I={target_lufs}:TP={LOUDNORM_TRUE_PEAK}:LRA={LOUDNORM_LRA}:"
        f"measured_I={params['measured_I']}:"
        f"measured_TP={params['measured_TP']}:"
        f"measured_LRA={params['measured_LRA']}:"
        f"measured_thresh={params['measured_thresh']}:"
        f"linear=true:print_format=summary"
    )

    cmd = as_audio_only_ffmpeg(
        [FFMPEG_PATH, "-y", "-i", input_path, "-af", filter_str, output_path]
    )

    logger.info(
        f"[LOUDNORM] Normalizing {input_path}: measured {true_measured_i} LUFS "
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

    return round(true_measured_i, 1), target_lufs