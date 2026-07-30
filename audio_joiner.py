"""
audio_joiner.py - Concatenates several uploaded audio files into one,
end to end, in the order given.

WHY THE SLOWER FFMPEG METHOD IS THE RIGHT ONE HERE:

ffmpeg offers two ways to concatenate. The concat DEMUXER (-f concat)
copies streams without re-encoding, so it's nearly free - but it requires
every input to share an identical codec, sample rate and channel layout.
Users joining files will routinely upload a 44.1kHz stereo MP3 followed
by a 48kHz mono WAV, and the demuxer's behaviour there ranges from a hard
failure to silently producing output where the second half plays at the
wrong speed and pitch. A tool that mangles audio without erroring is
worse than one that refuses.

So this module uses the concat FILTER instead. It re-encodes, which costs
real CPU, but it lets each input be normalised to a common sample format,
sample rate and channel layout FIRST - which means mismatched inputs join
correctly instead of corrupting. Given that mismatched input is the normal
case for this tool rather than the exception, that trade is clearly worth
it.

ORDER: the sequence of input_paths is the sequence of the output. The
route derives that from the upload order, so the frontend is responsible
for sending files in whatever order the user arranged them.

Same subprocess-not-library approach as the rest of the codebase, and
join_audio() is blocking - it MUST be dispatched via
utils.run_blocking() from the async route.
"""
import os
import subprocess
from typing import List

from config import (
    logger,
    FFMPEG_PATH,
    FFPROBE_PATH,
    JOIN_MAX_FILES,
    JOIN_MAX_TOTAL_DURATION_SECONDS,
    JOIN_OUTPUT_SAMPLE_RATE,
    JOIN_TIMEOUT_SECONDS,
)
from audio_common import AudioToolError

# Mirrors video_to_audio.py's encoder settings so a given output format
# sounds the same whichever tool produced it. Duplicated rather than
# shared for now because two consumers isn't yet worth a new home in
# audio_common - if a third tool needs these, move them there instead of
# copying a third time.
_ENCODE_ARGS = {
    "mp3": ["-c:a", "libmp3lame", "-q:a", "2"],
    "wav": ["-c:a", "pcm_s16le"],
    "flac": ["-c:a", "flac"],
    "m4a": ["-c:a", "aac", "-b:a", "192k"],
    "aac": ["-c:a", "aac", "-b:a", "192k"],
    "ogg": ["-c:a", "libvorbis", "-q:a", "5"],
    "aiff": ["-c:a", "pcm_s16be"],
}


def _probe_duration(file_path: str) -> float:
    """Container-level duration for one input. Returns 0.0 rather than
    raising when the metadata is missing, so one file with an unreadable
    header doesn't block a join that would otherwise succeed - the
    total-duration check below just under-counts slightly in that case."""
    try:
        result = subprocess.run(
            [
                FFPROBE_PATH, "-v", "error",
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
    except Exception:
        return 0.0


def validate_total_duration(input_paths: List[str]) -> float:
    """
    Sums every input's duration and rejects the batch if the combined
    length exceeds the cap. Checked as a TOTAL rather than per-file,
    because ten four-minute files are a forty-minute encode regardless
    of how modest each one looks individually.
    """
    total = sum(_probe_duration(p) for p in input_paths)

    if total > JOIN_MAX_TOTAL_DURATION_SECONDS:
        raise AudioToolError(
            f"Combined length is {int(total // 60)} min, which exceeds the "
            f"{JOIN_MAX_TOTAL_DURATION_SECONDS // 60} min limit for joining."
        )

    return total


def _build_filter_complex(count: int) -> str:
    """
    Builds the filter graph: normalise every input, then concatenate.

    aformat on each input is the part that makes this robust. Without it,
    concat receives streams with differing sample rates and channel
    counts and refuses (or misbehaves); with it, every stream arrives at
    the concat node in an identical format, so the join is always valid.

    Produces, for two inputs:
      [0:a]aformat=...[a0];[1:a]aformat=...[a1];[a0][a1]concat=n=2:v=0:a=1[out]
    """
    normalise = [
        f"[{i}:a]aformat=sample_fmts=fltp:"
        f"sample_rates={JOIN_OUTPUT_SAMPLE_RATE}:"
        f"channel_layouts=stereo[a{i}]"
        for i in range(count)
    ]

    labels = "".join(f"[a{i}]" for i in range(count))
    normalise.append(f"{labels}concat=n={count}:v=0:a=1[out]")

    return ";".join(normalise)


def join_audio(input_paths: List[str], output_path: str, target_format: str) -> float:
    """
    Concatenates input_paths into output_path, in order. Returns the
    combined source duration in seconds.

    Raises AudioToolError for too few/many files, an unsupported output
    format, a combined length over the cap, or any ffmpeg failure.
    """
    target_format = target_format.strip().lower()
    if target_format not in _ENCODE_ARGS:
        raise AudioToolError(f"'{target_format}' isn't a supported output format.")

    if len(input_paths) < 2:
        raise AudioToolError("Joining needs at least two files.")
    if len(input_paths) > JOIN_MAX_FILES:
        raise AudioToolError(f"You can join up to {JOIN_MAX_FILES} files at a time.")

    total_duration = validate_total_duration(input_paths)

    cmd = [FFMPEG_PATH, "-y"]
    for path in input_paths:
        cmd += ["-i", path]
    cmd += [
        "-filter_complex", _build_filter_complex(len(input_paths)),
        "-map", "[out]",
    ]
    cmd += _ENCODE_ARGS[target_format]
    cmd += [output_path]

    logger.info(
        f"[JOIN] Joining {len(input_paths)} files -> {target_format} "
        f"({total_duration:.1f}s combined)"
    )

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=JOIN_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise AudioToolError(
            f"Joining timed out after {JOIN_TIMEOUT_SECONDS}s. Try fewer or shorter files."
        )

    if result.returncode != 0:
        logger.error(f"[JOIN] ffmpeg failed: {result.stderr[-2000:]}")
        raise AudioToolError("Failed to join these files. One of them may be corrupt or unreadable.")

    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        raise AudioToolError("Joining produced an empty file.")

    return total_duration