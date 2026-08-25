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

--------------------------------------------------------------------------
THREE ADDITIONS (2026-08-02), each solving a failure this file could not
previously see:

1. count_processing(job_types) - the queue-depth reading that makes a
   BOUNDED queue possible. MAX_CONCURRENT_SEPARATIONS=1 caps how many
   Demucs runs happen at once, but nothing capped how many were allowed
   to PILE UP behind it. Each queued job holds its uploaded file on disk
   and its entry in this table indefinitely, and the user watching a
   spinner has no idea they are 12th in line. A route can now read the
   depth and reject with a clean 503 instead of silently enqueueing.

2. fail_if_unfinished(job_id, error) - the safety net for background
   tasks. Every _run_*_background() marks its job failed in an `except`
   block, but an exception raised OUTSIDE those handlers (most really:
   acquire_slot_or_503() raising HTTPException inside a background task
   where no HTTP layer exists to catch it, or a CancelledError on
   shutdown) skips every handler. The job then sits at "processing"
   forever, the frontend polls it until its own timeout, and the file
   stays on disk until TTL. Called from a `finally`, this guarantees a
   job always reaches a terminal state. It is deliberately a no-op when
   the job already completed or failed, so it can be called
   unconditionally without clobbering a real result.

3. cleanup_expired_jobs() no longer deletes files while holding the
   lock. It previously did os.remove() inside `with _lock:` - a stems
   job holds 4+ full-length WAVs, so a sweep could hold the lock across
   many blocking disk syscalls. Since get_job() needs that same lock,
   every in-flight status poll stalled for the duration of the sweep.
   The lock is now held only long enough to pop the expired entries and
   collect their paths; deletion happens after releasing it.
--------------------------------------------------------------------------

--------------------------------------------------------------------------
ADDED 2026-08-25: SOURCE INPUT RETENTION (input_path + set_job_input)

A fifth field, and the first one that changes what this table is FOR:
input_path records the uploaded source file a job was created from,
rather than only its outputs.

WHY. The paid HQ tier needs an "upgrade this to HQ" button on a finished
standard separation - re-running the same track through htdemucs_ft
without asking for a second upload. That is only possible if the source
file still exists, and until now it did not: _run_tool_job's
cleanup_paths deleted every job's input in its `finally`, usually within
seconds of the job finishing.

The alternative was a free HQ preview on a 30-second excerpt, which was
rejected on cost - see routes/separation_upgrade.py's docstring for that
reasoning. Retention is the cheaper answer by a wide margin: the demo is
the standard job that already ran.

SCOPE, DELIBERATELY NARROW. Only the separation routes call
set_job_input(). Every other tool still passes its input in
cleanup_paths and still deletes it immediately, because a /convert input
is worthless the moment its output exists and eighteen tools retaining
inputs for an hour would be real disk for no reason at all.

WHAT IT COSTS. A separation input now lives for the job's own TTL -
SEPARATION_JOB_TTL_SECONDS, 2 hours, the same window its OUTPUTS already
live for. An upgrade offer that outlived the file it needs would be
worse than no offer. Worst case is "separations in 2 hours" x
MAX_UPLOAD_BYTES; realistically far less, since most uploads are 5-10MB
rather than the 80MB ceiling. SEPARATION_JOB_TTL_SECONDS is the knob if
that ever gets uncomfortable, and shrinking it shortens the upgrade
window in exactly the same proportion.

THE PART THAT MATTERS MOST is in cleanup_expired_jobs() below:
input_path is swept alongside the output paths. Without that line every
separation input would stay on disk permanently - the same leak this
file's own docstring warns about for the stems dict, in a new place.
--------------------------------------------------------------------------
"""
import os
import time
import threading
import uuid
from typing import Iterable, Optional

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
#   # separation-shaped jobs only, set via set_job_input():
#   input_path: Optional[str],      # the SOURCE upload, retained for upgrades
# }
_jobs = {}

# job_types that share separation's longer TTL rather than the shorter
# audio-tools default - anything backed by a Demucs run takes long
# enough to produce that expiring it after only an hour would throw away
# expensive work while a user is still previewing it. silence_split is
# NOT here - it's ffmpeg-only and cheap to redo, so it uses the
# audio-tools default despite sharing the "stems" dict storage shape.
_LONG_TTL_JOB_TYPES = ("separation", "stems", "youtube_separate", "youtube_stems")

# Every job_type that ends up contending for the single separation
# semaphore. Grouped here rather than spelled out at the call site so
# adding a fifth Demucs-backed route later can't accidentally escape the
# queue-depth check - see count_processing() below.
SEPARATION_JOB_TYPES = ("separation", "stems", "youtube_separate", "youtube_stems")

# All three transcription flows share ONE semaphore, so they must be
# counted together for the queue guard - counting any one endpoint in
# isolation would let the other two fill the queue invisibly.
TRANSCRIPTION_JOB_TYPES = ("transcribe", "youtube_transcribe", "video_transcribe")


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
            # The uploaded SOURCE file. Stays None for every job type
            # except the separation routes, which call set_job_input()
            # so a completed standard job can be re-run at HQ without a
            # second upload. See this module's 2026-08-25 note.
            "input_path": None,
        }
    return job_id


def set_job_input(job_id: str, input_path: str):
    """
    Records the source file a job was created from.

    Separate from create_job() rather than a parameter on it, because
    the job id is needed to BUILD the input path in the first place
    (build_temp_input_path(job_id, filename)) - the job must exist
    before the path does. That is the same ordering _accept_upload()
    already relies on.

    RETENTION, NOT OWNERSHIP. Setting this does not make the caller
    responsible for deleting the file - it makes cleanup_expired_jobs()
    responsible, on the job's own TTL. A route that calls this must
    therefore NOT also pass the same path in _run_tool_job's
    cleanup_paths, or the file is deleted seconds after the job
    finishes and the retention is silently undone.

    Note that an upgrade job and its source job legitimately point at
    the SAME input_path, so the sweep may try to delete it twice. That
    is already safe: cleanup_expired_jobs() wraps each os.remove() in
    try/except and logs a warning, so the second attempt is a harmless
    no-op rather than an error worth chasing.
    """
    with _lock:
        if job_id in _jobs:
            _jobs[job_id]["input_path"] = input_path


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


def fail_if_unfinished(job_id: str, error: str = "The job ended unexpectedly.") -> bool:
    """
    Marks a job failed ONLY if it is still "processing". Returns True if
    it actually changed anything.

    Meant to be called from a `finally` block in every background task,
    where it is a no-op on the happy path (the job is already "complete")
    and a rescue on the paths no `except` clause covers:

      - acquire_slot_or_503() raising HTTPException inside a background
        task - there is no HTTP layer out there to catch it, so the
        exception propagates out of the task and every mark_failed() in
        the except-chain is skipped.
      - asyncio.CancelledError during shutdown.
      - Any bug in the handler code itself, including inside an existing
        `except` block.

    Without this, such a job sits at "processing" forever: the frontend
    polls until its own client-side timeout and reports something vague
    like "connection dropped", the input file waits for the TTL sweep,
    and the server-side logs show no failure at all because none was
    ever recorded. That combination is exactly what makes this class of
    bug so hard to trace after the fact.

    The WARNING it logs is deliberate and load-bearing: reaching this
    function at all means an exception escaped its intended handler,
    which is worth seeing in the dashboard even though the user now gets
    a clean error.
    """
    with _lock:
        job = _jobs.get(job_id)
        if job is None or job["status"] != "processing":
            return False
        job.update({"status": "failed", "error": error})

    logger.warning(
        f"[JOBS] Job {job_id} was still 'processing' when its task ended - "
        f"force-failed via safety net: {error}"
    )
    return True


def get_job(job_id: str) -> Optional[dict]:
    with _lock:
        job = _jobs.get(job_id)
        return dict(job) if job else None


def count_processing(job_types: Optional[Iterable[str]] = None) -> int:
    """
    How many jobs are currently "processing", optionally restricted to
    specific job_types.

    This is the reading a route needs to enforce a BOUNDED queue.
    MAX_CONCURRENT_SEPARATIONS caps how many Demucs runs happen at once,
    but the semaphore is acquired INSIDE the background task - so extra
    submissions are not rejected, they queue in memory with no limit,
    each holding an uploaded file on disk and a table entry until it
    eventually runs. Ten queued separations on a one-slot machine is
    roughly fifty minutes of invisible waiting for whoever is last.

    Counting live rather than maintaining a separate counter avoids the
    classic drift bug where an incremented counter never gets
    decremented on some error path - the job table is already the source
    of truth for what is running.

    Note the count includes jobs actively running, not just waiting, so
    a route comparing against a threshold is measuring total in-flight
    work. That is the number worth bounding.
    """
    with _lock:
        if job_types is None:
            return sum(1 for j in _jobs.values() if j["status"] == "processing")
        wanted = set(job_types)
        return sum(
            1 for j in _jobs.values()
            if j["status"] == "processing" and j["job_type"] in wanted
        )


def get_job_stats() -> dict:
    """
    Snapshot of the job table for /admin/status and periodic logging:
    totals by status, plus the separation-queue depth that the bounded
    queue keys on. One pass under one lock rather than several calls
    that could each see a different moment.
    """
    with _lock:
        total = len(_jobs)
        by_status = {"processing": 0, "complete": 0, "failed": 0}
        separation_processing = 0
        oldest_processing_age = 0.0
        now = time.time()

        for job in _jobs.values():
            status = job["status"]
            by_status[status] = by_status.get(status, 0) + 1
            if status == "processing":
                if job["job_type"] in SEPARATION_JOB_TYPES:
                    separation_processing += 1
                age = now - job["created_at"]
                if age > oldest_processing_age:
                    oldest_processing_age = age

    return {
        "total": total,
        **by_status,
        "separation_queue_depth": separation_processing,
        "oldest_processing_seconds": round(oldest_processing_age, 1),
    }


def cleanup_expired_jobs() -> int:
    """
    Deletes job entries (and their on-disk output files, where
    applicable) older than their own ttl_seconds. Returns how many were
    removed.

    Call this on a background timer (see main.py's _job_cleanup_loop)
    rather than from inside request handlers - a user who never comes
    back to download their result would otherwise leave both the job
    dict entry and its output files on disk forever, but doing the sweep
    on the request path means every upload pays for someone else's
    cleanup.

    Handles all four job shapes: separation-shaped jobs clean up
    vocals_path + instrumental_path, stems-shaped jobs clean up every
    path in their stems dict, audio-tool jobs clean up output_path,
    data jobs (transcribe/youtube_analyze) have no file to clean up
    (result_data is just removed along with the dict entry itself). A
    job with a None path (never got that far, already cleaned up, or a
    data job) is skipped safely.

    ALSO SWEEPS input_path (added 2026-08-25). This is the line that
    keeps source-input retention bounded rather than permanent. The
    separation routes deliberately stopped passing their input in
    _run_tool_job's cleanup_paths so it survives for the upgrade-to-HQ
    path; if it were not collected HERE instead, every separation input
    would stay on disk forever - the same leak this docstring already
    warns about for the stems dict, just in a newer place.

    An upgrade job and its source job share one input_path, so a sweep
    that expires both will attempt the same delete twice. The per-path
    try/except below already makes that a no-op with a warning line, not
    an error.

    The stems dict MUST be walked separately from the named path fields -
    a stems job holds 4+ full-length WAVs, so missing them here would
    leak roughly double (or more) a separation job's disk per expired
    job, forever.

    LOCK SCOPE: the lock is held only long enough to identify expired
    jobs, pop them, and collect their paths. File deletion happens after
    it is released. Deleting inside the lock (as this originally did)
    meant a sweep over a few stems jobs held it across a dozen blocking
    os.remove() syscalls, and since get_job() needs the same lock, every
    concurrent status poll stalled for that whole window.
    """
    now = time.time()
    paths_to_delete = []
    expired_count = 0

    with _lock:
        expired_ids = [
            job_id for job_id, job in _jobs.items()
            if now - job["created_at"] > job.get("ttl_seconds", SEPARATION_JOB_TTL_SECONDS)
        ]

        for job_id in expired_ids:
            job = _jobs.pop(job_id, None)
            if not job:
                continue
            expired_count += 1

            for key in ("vocals_path", "instrumental_path", "output_path", "input_path"):
                path = job.get(key)
                if path:
                    paths_to_delete.append(path)

            stems = job.get("stems")
            if isinstance(stems, dict):
                paths_to_delete.extend(p for p in stems.values() if p)

    # Lock released - the blocking disk work happens out here so status
    # polls are never queued behind it.
    deleted_files = 0
    for path in paths_to_delete:
        try:
            if os.path.exists(path):
                os.remove(path)
                deleted_files += 1
        except Exception as e:
            logger.warning(f"[JOBS] Failed to clean up expired file {path}: {e}")

    if expired_count:
        logger.info(
            f"[JOBS] Cleaned up {expired_count} expired job(s), "
            f"{deleted_files} file(s) removed"
        )

    return expired_count