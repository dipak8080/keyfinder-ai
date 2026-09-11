"""
youtube_source.py - "source" delivery for /download WAV requests.

Instead of converting YouTube's compressed stream to WAV here and sending
~10 MB per minute, the server keeps the original stream (Opus in WebM, or
AAC in M4A) and sends that (~1 MB per minute). The browser decodes it and
writes the WAV itself. Same samples either way, since the WAV was always
decoded from this same stream.

Long videos still get a server-side WAV: decoding an hour of audio in a
phone tab is not safe.
"""
import glob
import json
import os
import subprocess
from typing import Optional, Tuple

from config import logger, FFMPEG_PATH, FFPROBE_PATH, UPLOAD_DIR
from audio_common import run_subprocess
from utils import cleanup_file

# codec the browser says it can decode -> (file extension, yt-dlp format selector)
SOURCE_CODECS = {
    "opus": ("webm", "bestaudio[acodec=opus]/bestaudio"),
    "aac": ("m4a", "bestaudio[ext=m4a]/bestaudio"),
}

SOURCE_MAX_SECONDS = int(os.environ.get("DOWNLOAD_SOURCE_MAX_SECONDS", "600"))

_PROBE_TIMEOUT_SECONDS = 30
_WAV_TIMEOUT_SECONDS = 300


def probe_audio(path: str) -> Tuple[Optional[float], Optional[int]]:
    """(duration_seconds, sample_rate) of the first audio stream, None where unreadable."""
    cmd = [
        FFPROBE_PATH, "-v", "error",
        "-select_streams", "a:0",
        "-show_entries", "stream=sample_rate:format=duration",
        "-of", "json",
        path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=_PROBE_TIMEOUT_SECONDS)
        data = json.loads(result.stdout or "{}")
    except (subprocess.TimeoutExpired, ValueError):
        return None, None

    duration = None
    sample_rate = None
    try:
        duration = float(data.get("format", {}).get("duration"))
    except (TypeError, ValueError):
        pass
    try:
        sample_rate = int(data.get("streams", [{}])[0].get("sample_rate"))
    except (TypeError, ValueError, IndexError):
        pass
    return duration, sample_rate


def find_download(temp_id: str) -> Optional[str]:
    """The finished file yt-dlp wrote for temp_id, ignoring partials."""
    for path in sorted(glob.glob(os.path.join(UPLOAD_DIR, f"{temp_id}.*"))):
        if not path.endswith((".part", ".ytdl", ".temp", ".json")):
            return path
    return None


def cleanup_downloads(temp_id: str) -> None:
    for path in glob.glob(os.path.join(UPLOAD_DIR, f"{temp_id}.*")):
        cleanup_file(path)


def _to_wav(src: str, dst: str) -> None:
    run_subprocess(
        [FFMPEG_PATH, "-y", "-i", src, "-c:a", "pcm_s16le", dst],
        timeout=_WAV_TIMEOUT_SECONDS,
    )


def finalize_source(temp_id: str, codec: str) -> Tuple[str, str, Optional[int], Optional[float]]:
    """
    Decides what to cache after a source download. Returns
    (path, fmt, sample_rate, duration) where fmt is "webm"/"m4a" when the
    original can go to the browser, or "wav" when it was converted here
    (wrong codec for this browser, or longer than SOURCE_MAX_SECONDS).
    Raises FileNotFoundError if yt-dlp produced nothing.
    """
    path = find_download(temp_id)
    if not path:
        raise FileNotFoundError(f"no output for {temp_id}")

    expected_ext = SOURCE_CODECS[codec][0]
    actual_ext = os.path.splitext(path)[1].lstrip(".").lower()
    duration, sample_rate = probe_audio(path)

    too_long = duration is None or duration > SOURCE_MAX_SECONDS
    if actual_ext == expected_ext and not too_long:
        return path, actual_ext, sample_rate, duration

    wav_path = os.path.join(UPLOAD_DIR, f"{temp_id}.wav")
    reason = "too long" if too_long else f"got {actual_ext}, wanted {expected_ext}"
    logger.info(f"[DOWNLOAD] Source -> server WAV ({reason}, duration={duration})")
    try:
        _to_wav(path, wav_path)
    finally:
        cleanup_file(path)
    return wav_path, "wav", sample_rate, duration