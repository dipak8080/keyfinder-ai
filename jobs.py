"""
jobs.py - In-memory job tracking for long-running background work:
vocal/instrumental separation, full multi-stem separation, the
audio-tools (convert, trim, pitch, tempo, volume, reverse, noise-remove,
voice-clean, echo-remove, video-to-audio, join, loudnorm),
silence-split, speech-to-text transcription, and the /youtube/* chained
tools (download + analyze/separate/stems in one job).

Same pattern as rate_limit.py / monitoring.py: in-memory, per-instance,
thread-safe via a single lock. Fine for a single-VPS deployment; if this
ever scales to multiple instances behind a load balancer, job state would
need to move to something shared (Redis, etc.) since a status poll could
otherwise land on an instance that never actually ran the job.

Why a job table at all, instead of just returning the result directly:
some of this work (Demucs separation, rubberband pitch/tempo on long
files, Whisper transcription, YouTube download+processing) takes well
beyond a normal HTTP request's comfortable window. Instead, POST
<endpoint> returns a job_id almost immediately, the actual work runs in
the background, and the frontend polls GET .../status/{id} every few
seconds until it flips to "complete" or "failed".

FOUR JOB SHAPES, ONE TABLE:
- Separation-shaped jobs (job_type="separation", also reused by
  "youtube_separate") produce TWO output files (vocals_path,
  instrumental_path) - unchanged from the original design.
- Stems-shaped jobs (job_type="stems", also reused by "silence_split"
  and "youtube_stems") produce N output files held in a {name: path}
  dict (stems) - four for htdemucs/htdemucs_ft, six if a 6-source model
  is ever wired up, or however many segments silence-split finds. A
  dict rather than named fields precisely so the count isn't baked into
  this schema.
- Audio-tool jobs (job_type="convert"/"trim"/"pitch"/"tempo"/"volume"/
  "reverse"/"noise_remove"/"voice_clean"/"echo_remove"/"video_to_audio"/
  "join"/"loudnorm") produce ONE output file (output_path).
- Data jobs (job_type="transcribe" or "youtube_analyze") produce inline
  data (result_data) instead of a file - a JSON-serializable dict, not
  audio, so there's no file to write/clean up for this job type.
  mark_transcription_complete() and mark_data_complete() do the exact
  same update; two names are kept so each call site reads clearly
  regardless of which chained/standalone tool produced the result.
Keeping all four shapes in the same table (rather than separate parallel
job systems) means one cleanup sweep, one lock, one TTL mechanism to
reason about - the mark_*_complete() functions below just populate
different fields on the same underlying dict.

Existing call sites (create_job() with no args, mark_complete() with the
original 4 positional args) are unaffected - both keep their original
default behavior for separation jobs.
"""
import time
import threading
import uuid
from typing import Optional

from config import logger, SEPARATION_JOB_TTL_SECONDS, AUDIO_TOOL_JOB_TTL_SECONDS

_lock = threading.Lock()

# job_id -> {
#   status: "processing" | "complete" | "failed",
#   job_type: "separation" | "stems" | "convert" | "trim" | "pitch" |
#             "tempo" | "volume" | "reverse" | "noise_remove" |
#             "voice_clean" | "echo_remove" | "silence_remove" |
#             "video_to_audio" | "join" | "loudnorm" | "silence_split" |
#             "transcribe" | "youtube_analyze" | "youtube_separate" |
#             "youtube_stems",
#   created_at: float,
#   ttl_seconds: int,
#   title: Optional[str],
#   error: Optional[str],
#   # separation-shaped jobs only (also youtube_separate):
#   vocals_path: Optional[str],
#   instrumental_path: Optional[str],
#   # stems-shaped jobs only (also silence_split, youtube_stems):
#   stems: Optional[dict],          # {"vocals": path, "drums": path, ...}
#   # audio-tool jobs only:
#   output_path: Optional[str],
#   output_format: Optional[str],
#   # data jobs only (transcribe, youtube_analyze):
#   result_data: Optional[dict],
# }
_jobs = {}

# job_types that share separation's longer TTL rather than the shorter
# audio-tools default - anything backed by a Demucs run takes long
# enough to produce that expiring it after only an hour would throw away
# expensive work while a user is still previewing it. silence_split is
# NOT here - it's ffmpeg-only and cheap to redo, so it uses the
# audio-tools default despite sharing the "stems" dict storage shape.
_LONG_TTL_JOB_TYPES = ("separation", "stems", "youtube_separate", "youtube_stems")


def create_job(job_type: str = "separation", ttl_seconds: Optional[int] = None) -> str:
    """
    Creates a new job entry and returns its id.

    job_type defaults to "separation" so the existing `create_job()`
    call in routes.py's /separate endpoint is unaffected. Every other
    route should pass its own job_type explicitly (create_job("convert"),
    create_job("pitch"), create_job("youtube_analyze"), etc). Routes with
    their own TTL requirements (transcription, /youtube/analyze) should
    pass ttl_seconds explicitly rather than relying on either default.

    ttl_seconds defaults to SEPARATION_JOB_TTL_SECONDS for job types in
    _LONG_TTL_JOB_TYPES and AUDIO_TOOL_JOB_TTL_SECONDS for everything
    else, unless explicitly overridden.
    """
    if ttl_seconds is None:
        ttl_seconds = (
            SEPARATION_JOB_TTL_SECONDS
            if job_type in _LONG_TTL_JOB_TYPES
            else AUDIO_TOOL_JOB_TTL_SECONDS
        )

    job_id = uuid.uuid4().hex
    with _lock:
        _jobs[job_id] = {
            "status": "processing",
            "job_type": job_type,
            "created_at": time.time(),
            "ttl_seconds": ttl_seconds,
            "title": None,
            "error": None,
            "vocals_path": None,
            "instrumental_path": None,
            "stems": None,
            "output_path": None,
            "output_format": None,
            "result_data": None,
        }
    return job_id


def mark_complete(job_id: str, title: str, vocals_path: str, instrumental_path: str):
    """Marks a SEPARATION-shaped job complete with its two stem paths.
    Used by /separate, /separate-hq, and /youtube/separate."""
    with _lock:
        if job_id in _jobs:
            _jobs[job_id].update({
                "status": "complete",
                "title": title,
                "vocals_path": vocals_path,
                "instrumental_path": instrumental_path,
            })


def mark_stems_complete(job_id: str, title: str, stems: dict):
    """
    Marks a STEMS-shaped job complete. Unlike mark_complete()'s fixed
    vocals_path/instrumental_path pair, this stores a dict of
    {name: path} - "vocals"/"drums"/"bass"/"other" for htdemucs and
    htdemucs_ft (plus "guitar"/"piano" if a 6-source model is ever wired
    up), or "segment_01"/"segment_02"/... for silence-split.

    A dict rather than named fields so the entry count isn't baked into
    the schema: cleanup_expired_jobs() iterates the values, and the
    preview/download routes validate the requested name against the
    dict's own keys, so supporting a different set needs no change to
    this file at all.

    Used by /stems, /stems-hq, /silence-split, and /youtube/stems.
    """
    with _lock:
        if job_id in _jobs:
            _jobs[job_id].update({
                "status": "complete",
                "title": title,
                "stems": stems,
            })


def mark_tool_complete(job_id: str, title: str, output_path: str, output_format: Optional[str] = None):
    """
    Marks an AUDIO-TOOL job (convert/trim/pitch/tempo/volume/reverse/
    noise_remove/voice_clean/echo_remove/video_to_audio/join/loudnorm)
    complete with its single output file path. Separate from
    mark_complete() above since these jobs only ever produce one file,
    not a vocals/instrumental pair.
    """
    with _lock:
        if job_id in _jobs:
            _jobs[job_id].update({
                "status": "complete",
                "title": title,
                "output_path": output_path,
                "output_format": output_format,
            })


def mark_transcription_complete(job_id: str, title: str, result_data: dict):
    """
    Marks a TRANSCRIPTION job complete with its result stored INLINE in
    the job dict, not as a file path - transcription output is text
    (a JSON-serializable dict: {text, language, segments}), typically
    just a few KB even for a long recording, so writing/reading it as
    a file on disk would be unnecessary I/O for no real benefit.

    Mechanically identical to mark_data_complete() below - kept as a
    separate function purely so /speech-to-text's call site reads
    clearly rather than looking like a generic data dump.
    """
    with _lock:
        if job_id in _jobs:
            _jobs[job_id].update({
                "status": "complete",
                "title": title,
                "result_data": result_data,
            })


def mark_data_complete(job_id: str, title: str, result_data: dict):
    """
    Generic version of mark_transcription_complete() for any OTHER job
    whose output is inline data rather than a file - currently
    /youtube/analyze, which returns key/BPM/Camelot JSON instead of an
    audio file, the same shape /analyze itself returns synchronously.

    Kept as a distinct function from mark_transcription_complete() (even
    though the body is identical) so each call site's job type stays
    self-documenting - "this is a data-shaped job" without needing to
    know it happens to share code with transcription.
    """
    with _lock:
        if job_id in _jobs:
            _jobs[job_id].update({
                "status": "complete",
                "title": title,
                "result_data": result_data,
            })


def mark_failed(job_id: str, error: str):
    with _lock:
        if job_id in _jobs:
            _jobs[job_id].update({
                "status": "failed",
                "error": error,
            })


def get_job(job_id: str) -> Optional[dict]:
    with _lock:
        job = _jobs.get(job_id)
        return dict(job) if job else None


def cleanup_expired_jobs():
    """
    Deletes job entries (and their on-disk output files, where
    applicable) older than their own ttl_seconds. Call this
    periodically (e.g. once per request, or on a background timer)
    rather than relying on job entries to be cleaned up individually -
    a user who never comes back to download their result would
    otherwise leave both the job dict entry and its output files on
    disk forever.

    Handles all four job shapes: separation-shaped jobs clean up
    vocals_path + instrumental_path, stems-shaped jobs clean up every
    path in their stems dict, audio-tool jobs clean up output_path,
    data jobs (transcribe/youtube_analyze) have no file to clean up
    (result_data is just removed along with the dict entry itself). A
    job with a None path (never got that far, already cleaned up, or a
    data job) is skipped safely.

    The stems dict MUST be walked separately from the named path fields -
    a stems job holds 4+ full-length WAVs, so missing them here would
    leak roughly double (or more) a separation job's disk per expired
    job, forever.
    """
    import os

    now = time.time()
    to_delete = []

    with _lock:
        for job_id, job in _jobs.items():
            ttl = job.get("ttl_seconds", SEPARATION_JOB_TTL_SECONDS)
            if now - job["created_at"] > ttl:
                to_delete.append(job_id)

        for job_id in to_delete:
            job = _jobs.pop(job_id, None)
            if not job:
                continue

            paths = [job.get(k) for k in ("vocals_path", "instrumental_path", "output_path")]

            stems = job.get("stems")
            if isinstance(stems, dict):
                paths.extend(stems.values())

            for path in paths:
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                    except Exception as e:
                        logger.warning(f"[JOBS] Failed to clean up expired file {path}: {e}")

    if to_delete:
        logger.info(f"[JOBS] Cleaned up {len(to_delete)} expired job(s)")