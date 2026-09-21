"""
MP3 copies of separation stems, encoded on demand.

The GPU workers only ever write WAV. When a download asks for
?format=mp3, the WAV is encoded once with the VPS ffmpeg (320 kbps CBR,
same as the converter tools) and kept next to it, so a second download of
the same stem is a plain file read. jobs.cleanup_expired_jobs() removes
the copy together with the WAV.
"""

import os
import uuid

STEM_MP3_BITRATE = "320k"
STEM_MP3_TIMEOUT_SECONDS = 180


def mp3_sibling(wav_path: str) -> str:
    return os.path.splitext(wav_path)[0] + ".320.mp3"


def ensure_stem_mp3(wav_path: str) -> str:
    """Blocking. Call through utils.run_blocking from a route."""
    from audio_common import run_subprocess
    from config import FFMPEG_PATH

    out_path = mp3_sibling(wav_path)
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return out_path

    # Unique temp name + atomic rename: two people downloading the same
    # stem at once each encode their own temp file and neither can serve a
    # half-written MP3.
    tmp_path = f"{out_path}.{uuid.uuid4().hex}.tmp.mp3"
    try:
        run_subprocess(
            [
                FFMPEG_PATH, "-y", "-i", wav_path,
                "-codec:a", "libmp3lame", "-b:a", STEM_MP3_BITRATE,
                tmp_path,
            ],
            timeout=STEM_MP3_TIMEOUT_SECONDS,
        )
        os.replace(tmp_path, out_path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    return out_path