"""
silence_splitter.py - Splits one audio file into several, cutting at
silent gaps rather than removing them.

HOW THIS DIFFERS FROM silence_remover.py: that module finds silent gaps
and DELETES them, splicing what remains into one continuous file (good
for tightening a podcast). This module finds the same kind of gaps and
uses them as CUT POINTS, keeping every segment between them as its own
file (good for turning one long recording - a DJ mix, a vinyl side, a
multi-idea voice memo - into individual tracks). Same detection
approach, opposite thing done with the result.

TWO-STEP PROCESS:
  1. Run ffmpeg's silencedetect filter once over the whole file (a
     single fast pass, no re-encode) to find every silence_start/
     silence_end pair.
  2. Turn the gaps between those silences into segment boundaries, then
     cut each segment out with its own ffmpeg call.

This costs one full-file scan plus N cheap stream-copy-where-possible
cuts, rather than N full re-encodes - cutting on the container's own
keyframes via -c copy where the target format allows it, falling back
to a real re-encode only when the output format differs from the input
(e.g. splitting an MP3 into WAV segments).

Same subprocess-per-call, blocking pattern as the rest of this codebase -
every public function here MUST be dispatched via utils.run_blocking()
from the async route.
"""
import os
import re
import subprocess
from typing import Dict, List, Tuple

from config import (
    logger,
    FFMPEG_PATH,
    SEPARATION_DIR,
    SILENCE_SPLIT_MAX_SEGMENTS,
    SILENCE_SPLIT_MIN_SEGMENT_SECONDS,
    SILENCE_SPLIT_DETECT_TIMEOUT_SECONDS,
    SILENCE_SPLIT_CUT_TIMEOUT_SECONDS,
)
from audio_common import AudioToolError, validate_duration

_SILENCE_START_RE = re.compile(r"silence_start:\s*([\d.]+)")
_SILENCE_END_RE = re.compile(r"silence_end:\s*([\d.]+)")

_ENCODE_ARGS = {
    "mp3": ["-c:a", "libmp3lame", "-q:a", "2"],
    "wav": ["-c:a", "pcm_s16le"],
    "flac": ["-c:a", "flac"],
    "m4a": ["-c:a", "aac", "-b:a", "192k"],
    "aac": ["-c:a", "aac", "-b:a", "192k"],
    "ogg": ["-c:a", "libvorbis", "-q:a", "5"],
    "aiff": ["-c:a", "pcm_s16be"],
}


def _detect_silences(input_path: str, threshold_db: float, min_duration_seconds: float) -> List[Tuple[float, float]]:
    """
    Single pass over the file with silencedetect (no re-encode, no
    output file - just analysis printed to stderr). Returns a list of
    (start, end) tuples for every detected silent span, in order.

    silencedetect can print a silence_start with no matching
    silence_end if the file ends while still silent - that dangling
    start is intentionally dropped rather than guessed at, since the
    segment-boundary logic below only needs to know where NON-silent
    audio is, and an unterminated trailing silence doesn't create a new
    segment boundary either way.
    """
    cmd = [
        FFMPEG_PATH, "-i", input_path,
        "-af", f"silencedetect=noise={threshold_db}dB:d={min_duration_seconds}",
        "-f", "null", "-",
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=SILENCE_SPLIT_DETECT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise AudioToolError("Timed out while scanning for silence.")

    silences = []
    pending_start = None
    for line in result.stderr.splitlines():
        start_match = _SILENCE_START_RE.search(line)
        if start_match:
            pending_start = float(start_match.group(1))
            continue

        end_match = _SILENCE_END_RE.search(line)
        if end_match and pending_start is not None:
            silences.append((pending_start, float(end_match.group(1))))
            pending_start = None

    return silences


def _segments_from_silences(
    total_duration: float,
    silences: List[Tuple[float, float]],
    min_segment_seconds: float,
) -> List[Tuple[float, float]]:
    """
    Converts a list of silent spans into a list of (start, end) segment
    boundaries covering the non-silent audio between them.

    Segments shorter than min_segment_seconds are DROPPED rather than
    kept as tiny fragments - a half-second blip between two silences
    (a cough, a click) isn't a track anyone wants as its own file, and
    keeping it would just inflate the output count with noise.
    """
    boundaries = []
    cursor = 0.0

    for silence_start, silence_end in silences:
        if silence_start > cursor:
            boundaries.append((cursor, silence_start))
        cursor = max(cursor, silence_end)

    if cursor < total_duration:
        boundaries.append((cursor, total_duration))

    return [
        (start, end) for start, end in boundaries
        if (end - start) >= min_segment_seconds
    ]


def split_on_silence(
    input_path: str,
    job_id: str,
    target_format: str,
    threshold_db: float,
    min_duration_seconds: float,
) -> Dict[str, str]:
    """
    Detects silence in input_path and cuts it into one file per
    non-silent segment. Returns {"segment_01": path, "segment_02": path,
    ...} - the same dict shape jobs.mark_stems_complete() already
    accepts, so this reuses that job type rather than inventing a
    fourth output shape.

    Raises AudioToolError if: the file is entirely silence (zero usable
    segments), the file is entirely one segment (no silence found - not
    an error exactly, but nothing to split, so the caller gets a clear
    message instead of a one-item "split"), or the detected segment
    count exceeds SILENCE_SPLIT_MAX_SEGMENTS (a safety valve against a
    mostly-silent file producing hundreds of fragments).
    """
    target_format = target_format.strip().lower()
    if target_format not in _ENCODE_ARGS:
        raise AudioToolError(f"'{target_format}' isn't a supported output format.")

    total_duration = validate_duration(input_path)

    silences = _detect_silences(input_path, threshold_db, min_duration_seconds)
    segments = _segments_from_silences(total_duration, silences, SILENCE_SPLIT_MIN_SEGMENT_SECONDS)

    if not segments:
        raise AudioToolError(
            "No usable segments were found - the file may be entirely silent, "
            "or too short relative to the silence settings used."
        )

    if len(segments) == 1:
        raise AudioToolError(
            "No silence was detected in this file, so there's nothing to split. "
            "Try lowering the threshold if you expected a split here."
        )

    if len(segments) > SILENCE_SPLIT_MAX_SEGMENTS:
        raise AudioToolError(
            f"This file would split into {len(segments)} segments, which exceeds the "
            f"{SILENCE_SPLIT_MAX_SEGMENTS} limit. Try raising the silence threshold or "
            f"minimum duration to merge nearby segments."
        )

    logger.info(
        f"[SILENCE_SPLIT] Job {job_id}: {len(segments)} segments detected "
        f"from {len(silences)} silence spans ({total_duration:.1f}s total)"
    )

    output_paths = {}
    for index, (start, end) in enumerate(segments, start=1):
        segment_name = f"segment_{index:02d}"
        output_path = os.path.join(SEPARATION_DIR, f"{job_id}_{segment_name}.{target_format}")

        # -ss before -i seeks at the container level (fast, keyframe-ish)
        # rather than decoding from the start of the file - meaningful on
        # a track with many segments, since the Nth cut otherwise re-reads
        # everything before it.
        cmd = [
            FFMPEG_PATH, "-y",
            "-ss", str(start), "-to", str(end),
            "-i", input_path,
        ]
        cmd += _ENCODE_ARGS[target_format]
        cmd += [output_path]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=SILENCE_SPLIT_CUT_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            for path in output_paths.values():
                _safe_remove(path)
            raise AudioToolError("Timed out while cutting segments.")

        if result.returncode != 0 or not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            logger.error(f"[SILENCE_SPLIT] Segment {index} failed: {result.stderr[-1000:]}")
            for path in output_paths.values():
                _safe_remove(path)
            raise AudioToolError(f"Failed to cut segment {index} of {len(segments)}.")

        output_paths[segment_name] = output_path

    logger.info(f"[SILENCE_SPLIT] Job {job_id} complete: {len(output_paths)} files written")
    return output_paths


def _safe_remove(path: str):
    """All-or-nothing cleanup helper: if any segment fails partway
    through the batch, every segment already written is deleted rather
    than left as a partial, confusing result set."""
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception as e:
        logger.warning(f"[SILENCE_SPLIT] Failed to clean up {path}: {e}")