"""
video_to_audio.py - Extracts the audio track out of a video file into any
of the supported audio formats.

THE KEY IDEA: extraction is often FREE. Most videos already carry their
audio as a compressed stream (usually AAC in an MP4). If the user's
requested output format can hold that stream as-is, ffmpeg can copy it
out with `-c:a copy` - no decoding, no re-encoding, no quality loss, and
it finishes in about a second regardless of whether the input is 5MB or
500MB, because ffmpeg is just moving bytes between containers.

That matters more here than anywhere else in this codebase: CPU is the
binding constraint on this VPS, and the single most common request this
endpoint will get (an MP4 going to M4A/AAC) costs essentially nothing.
Only when the target format genuinely can't hold the source stream do we
fall through to a real decode-and-re-encode.

A note worth passing on to users via the UI: choosing WAV/FLAC/AIFF for a
video whose audio is lossy AAC does NOT recover quality. It produces a
much larger file containing exactly the same audio information. The
lossless-container formats are only the right choice when the source
audio is itself lossless.

Same subprocess-not-library approach as separation.py: ffmpeg is invoked
as its own process so a hang or crash is isolated and killable, and all
calls here are blocking - they MUST be dispatched via utils.run_blocking()
from the async route.
"""
import os
import subprocess
from typing import Optional, Tuple

from config import (
    logger,
    FFMPEG_PATH,
    FFPROBE_PATH,
    ALLOWED_VIDEO_INPUT_FORMATS,
    VIDEO_EXTRACT_MAX_DURATION_SECONDS,
    VIDEO_TO_AUDIO_TIMEOUT_SECONDS,
)
from audio_common import AudioToolError

# Which source audio codecs can be stream-copied (no re-encode) into
# which target containers. Conservative on purpose - a copy that the
# container can't legally hold produces a corrupt output file rather than
# a clean error, so anything not listed here takes the safe re-encode
# path instead.
#
# Notably absent: pcm_s16le -> aiff. AIFF expects big-endian PCM
# (pcm_s16be), so copying little-endian PCM into it would produce a file
# that plays as noise. That one has to re-encode.
_COPY_COMPATIBLE = {
    "aac": {"m4a", "aac"},
    "mp3": {"mp3"},
    "flac": {"flac"},
    "opus": {"ogg"},
    "vorbis": {"ogg"},
    "pcm_s16le": {"wav"},
}

# Encoder settings per target format, used on the re-encode path.
# Quality levels chosen to match what the existing /convert endpoint
# produces so a user gets the same result either way.
_ENCODE_ARGS = {
    "mp3": ["-c:a", "libmp3lame", "-q:a", "2"],
    "wav": ["-c:a", "pcm_s16le"],
    "flac": ["-c:a", "flac"],
    "m4a": ["-c:a", "aac", "-b:a", "192k"],
    "aac": ["-c:a", "aac", "-b:a", "192k"],
    "ogg": ["-c:a", "libvorbis", "-q:a", "5"],
    "aiff": ["-c:a", "pcm_s16be"],
}


def validate_video_input_format(filename: Optional[str]) -> str:
    """
    Checks the uploaded file's extension against the video whitelist.
    Deliberately NOT audio_common.validate_input_format() - that one
    validates against the AUDIO input formats, and video extensions
    aren't (and shouldn't be) in that set, since they're valid only as
    input to this one endpoint and never as an output target.
    """
    if not filename or "." not in filename:
        raise AudioToolError("Could not determine the file type from the filename.")

    ext = filename.rsplit(".", 1)[-1].strip().lower()
    if ext not in ALLOWED_VIDEO_INPUT_FORMATS:
        raise AudioToolError(
            f"'{ext}' isn't a supported video format. Supported: "
            f"{', '.join(sorted(ALLOWED_VIDEO_INPUT_FORMATS))}."
        )
    return ext


def probe_audio_stream(file_path: str) -> Tuple[str, float]:
    """
    Returns (audio_codec_name, duration_seconds) for the file's FIRST
    audio stream.

    Two failure modes handled explicitly here rather than being allowed
    to surface later as a confusing ffmpeg error:

    1. No audio stream at all. Silent screen recordings and muted phone
       clips are common, and "this video has no audio track" is a far
       more useful message than whatever ffmpeg would say after being
       asked to map a stream that doesn't exist.
    2. Missing duration metadata. Some containers (particularly
       partially-downloaded or stream-captured files) don't report it,
       so a missing value is treated as unknown (0.0) and left to the
       duration check to decide about, rather than crashing on a float
       conversion.
    """
    try:
        result = subprocess.run(
            [
                FFPROBE_PATH, "-v", "error",
                "-select_streams", "a:0",
                "-show_entries", "stream=codec_name",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                file_path,
            ],
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        )
    except subprocess.TimeoutExpired:
        raise AudioToolError("Timed out while inspecting the video file.")
    except Exception as e:
        logger.error(f"[VIDEO_TO_AUDIO] ffprobe failed for {file_path}: {e}")
        raise AudioToolError("Could not read this video file. It may be corrupt or in an unsupported format.")

    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]

    if not lines:
        raise AudioToolError("This video doesn't contain an audio track to extract.")

    codec = lines[0].lower()

    duration = 0.0
    if len(lines) > 1:
        try:
            duration = float(lines[1])
        except ValueError:
            duration = 0.0

    return codec, duration


def extract_audio(input_path: str, output_path: str, target_format: str) -> bool:
    """
    Pulls the audio out of input_path into output_path.

    Returns True if the audio was stream-copied (lossless, near-instant),
    False if it had to be re-encoded - the route passes this back to the
    frontend so the UI can tell the user which happened, since "lossless
    copy" vs "re-encoded" is exactly the thing someone extracting audio
    wants to know.

    Raises AudioToolError on any failure, including a missing audio
    track or a file longer than the duration cap.
    """
    target_format = target_format.strip().lower()
    if target_format not in _ENCODE_ARGS:
        raise AudioToolError(f"'{target_format}' isn't a supported output format.")

    codec, duration = probe_audio_stream(input_path)

    if duration > VIDEO_EXTRACT_MAX_DURATION_SECONDS:
        raise AudioToolError(
            f"Video is {int(duration // 60)} min long, which exceeds the "
            f"{VIDEO_EXTRACT_MAX_DURATION_SECONDS // 60} min limit."
        )

    can_copy = target_format in _COPY_COMPATIBLE.get(codec, set())

    # -vn drops video entirely; -map 0:a:0 takes only the first audio
    # stream, which matters for files with multiple audio tracks (e.g. a
    # movie rip with several language tracks) where ffmpeg's default
    # stream selection could otherwise pick an unexpected one.
    cmd = [FFMPEG_PATH, "-y", "-i", input_path, "-vn", "-map", "0:a:0"]
    cmd += ["-c:a", "copy"] if can_copy else _ENCODE_ARGS[target_format]
    cmd += [output_path]

    logger.info(
        f"[VIDEO_TO_AUDIO] {'Copying' if can_copy else 'Re-encoding'} "
        f"{codec} -> {target_format} ({duration:.1f}s): {' '.join(cmd)}"
    )

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=VIDEO_TO_AUDIO_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise AudioToolError(
            f"Extraction timed out after {VIDEO_TO_AUDIO_TIMEOUT_SECONDS}s. Try a shorter video."
        )

    if result.returncode != 0:
        logger.error(f"[VIDEO_TO_AUDIO] ffmpeg failed: {result.stderr[-2000:]}")
        raise AudioToolError("Failed to extract audio from this video.")

    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        raise AudioToolError("Extraction produced an empty file.")

    return can_copy