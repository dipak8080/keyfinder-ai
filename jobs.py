"""
jobs.py - In-memory job tracking for long-running background work:
vocal/instrumental separation, full multi-stem separation, the
audio-tools (convert, trim, pitch, tempo, volume, reverse, noise-remove,
voice-clean, echo-remove, silence-remove), and speech-to-text
transcription.

Same pattern as rate_limit.py / monitoring.py: in-memory, per-instance,
thread-safe via a single lock. Fine for a single-VPS deployment; if this
ever scales to multiple instances behind a load balancer, job state would
need to move to something shared (Redis, etc.) since a status poll could
otherwise land on an instance that never actually ran the job.

Why a job table at all, instead of just returning the result directly:
some of this work (Demucs separation, rubberband pitch/tempo on long
files, Whisper transcription) takes well beyond a normal HTTP request's
comfortable window. Instead, POST <endpoint> returns a job_id almost
immediately, the actual work runs in the background, and the frontend
polls GET .../status/{id} every few seconds until it flips to "complete"
or "failed".

FOUR JOB SHAPES, ONE TABLE:
- Separation jobs (job_type="separation") produce TWO output files
  (vocals_path, instrumental_path) - unchanged from the original design.
- Stems jobs (job_type="stems") produce N output files held in a
  {stem_name: path} dict (stems) - four for htdemucs/htdemucs_ft, six
  if a 6-source model is ever wired up. A dict rather than named fields
  precisely so the stem count isn't baked into this schema.
- Audio-tool jobs (job_type="convert"/"trim"/"pitch"/"tempo"/"volume"/
  "reverse"/"noise_remove"/"voice_clean"/"echo_remove"/"silence_remove")
  produce ONE output file (output_path).
- Transcription jobs (job_type="transcribe") produce inline TEXT data
  (result_data) instead of a file - transcription output is a small
  JSON-serializable dict, not audio, so there's no file to write/clean
  up for this job type.
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
#             "transcribe",
#   created_at: float,
#   ttl_seconds: int,
#   title: Optional[str],
#   error: Optional[str],
#   # separation jobs only:
#   vocals_path: Optional[str],
#   instrumental_path: Optional[str],
#   # stems jobs only:
#   stems: Optional[dict],          # {"vocals": path, "drums": path, ...}
#   # audio-tool jobs only:
#   output_path: Optional[str],
#   output_format: Optional[str],
#   # transcription jobs only:
#   result_data: Optional[dict],
# }
_jobs = {}


def create_job(job_type: str = "separation", ttl_seconds: Optional[int] = None) -> str:
    """
    Creates a new job entry and returns its id.

    job_type defaults to "separation" so the existing `create_job()`
    call in routes.py's /separate endpoint is unaffected. Audio-tool
    routes should call create_job("convert"), create_job("pitch"), etc.
    Transcription routes should call
    create_job("transcribe", ttl_seconds=TRANSCRIPTION_JOB_TTL_SECONDS)
    explicitly, since transcription doesn't share AUDIO_TOOL_JOB_TTL_SECONDS's
    default.

    ttl_seconds defaults to SEPARATION_JOB_TTL_SECONDS for BOTH Demucs
    job types (separation and stems) and AUDIO_TOOL_JOB_TTL_SECONDS for
    everything else, unless explicitly overridden. Stems deliberately
    shares separation's longer 2h TTL rather than falling through to the
    audio-tools 1h default: a stems job can take 15+ minutes of CPU to
    produce, so expiring its output after an hour would throw away
    expensive work while the user is still previewing it.
    """
    if ttl_seconds is None:
        ttl_seconds = (
            SEPARATION_JOB_TTL_SECONDS
            if job_type in ("separation", "stems")
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
    """Unchanged - marks a SEPARATION job complete with its two stem paths."""
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
    Marks a STEMS job complete. Unlike mark_complete()'s fixed
    vocals_path/instrumental_path pair, this stores a dict of
    {stem_name: path} - "vocals"/"drums"/"bass"/"other" for htdemucs and
    htdemucs_ft, plus "guitar"/"piano" if a 6-source model is ever
    wired up.

    A dict rather than four named fields so the stem count isn't baked
    into the schema: cleanup_expired_jobs() iterates the values, and the
    preview/download routes validate the requested stem against the
    dict's own keys, so supporting a different stem set needs no change
    to this file at all.
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
    noise_remove/voice_clean/echo_remove/silence_remove) complete with
    its single output file path. Separate from mark_complete() above
    since these jobs only ever produce one file, not a
    vocals/instrumental pair.
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
    a file on disk would be unnecessary I/O for no real benefit. This
    is the one job type that deviates from the "output_path" pattern
    every other tool uses, because the output type genuinely is
    different (data, not audio).
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

    Handles all four job shapes: separation jobs clean up vocals_path
    + instrumental_path, stems jobs clean up every path in their stems
    dict, audio-tool jobs clean up output_path, transcription jobs have
    no file to clean up (result_data is just removed along with the dict
    entry itself). A job with a None path (never got that far, already
    cleaned up, or a transcription job) is skipped safely.

    The stems dict MUST be walked separately from the named path fields -
    a stems job holds 4 full-length WAVs, so missing them here would
    leak roughly double a separation job's disk per expired job, forever.
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