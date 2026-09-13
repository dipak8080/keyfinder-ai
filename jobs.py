"""
jobs.py - Job tracking for long-running background work: separation, stems,
the audio-tools, silence-split, transcription, and the /youtube/* chained
tools.

State lives in Redis rather than process memory, so a status poll can land on
any container and a deploy no longer wipes in-flight jobs. The public API is
byte-identical to the in-memory version - no call site changes.

FOUR JOB SHAPES, ONE TABLE:
- Separation-shaped ("separation", "youtube_separate"): vocals_path +
  instrumental_path.
- Stems-shaped ("stems", "silence_split", "youtube_stems"): a {name: path}
  dict, so the entry count is not baked into the schema.
- Audio-tool jobs: a single output_path.
- Data jobs ("transcribe", "youtube_analyze"): inline result_data, no file.

EXPIRY IS NOT REDIS TTL. Each key carries a long safety TTL only.
cleanup_expired_jobs() is still the real sweep, because output files must be
deleted from disk BEFORE the record holding their paths disappears. Letting
Redis expire records on the real TTL would leak every output file permanently.

CROSS-CONTAINER CLAIM. With shared state, two containers can sweep at the
same time. cleanup_expired_jobs() claims each expired job with SREM and only
proceeds when SREM reports it did the removal, so exactly one container
deletes a given job's files.
"""
import json
import os
import time
import uuid
from typing import Iterable, Optional

from config import logger, SEPARATION_JOB_TTL_SECONDS, AUDIO_TOOL_JOB_TTL_SECONDS
from redis_store import client as _r

_KEY_PREFIX = "af:job:"
_INDEX_KEY = "af:jobs:index"

# How long a Redis key outlives its own TTL. Only a backstop for records the
# sweep never reached (sweeper down, container killed mid-sweep).
_SAFETY_TTL_MARGIN = 86400

_LONG_TTL_JOB_TYPES = ("separation", "stems", "youtube_separate", "youtube_stems")

SEPARATION_JOB_TYPES = ("separation", "stems", "youtube_separate", "youtube_stems")

TRANSCRIPTION_JOB_TYPES = ("transcribe", "youtube_transcribe", "video_transcribe")

MIDI_HQ_JOB_TYPES = ("audio_to_midi_hq",)

MIDI_JOB_TYPES = ("audio_to_midi",)

_INSTANCE_SLOT = os.environ.get("INSTANCE_SLOT", "")
if _INSTANCE_SLOT not in ("a", "b"):
    _INSTANCE_SLOT = ""


def _key(job_id: str) -> str:
    return _KEY_PREFIX + job_id


def _enc(value) -> str:
    return json.dumps(value)


def _dec(raw):
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return None


# Field writes are conditional on the record still existing, which preserves
# the original `if job_id in _jobs` guard without a read-modify-write race
# between containers.
_UPDATE_IF_EXISTS = _r.register_script("""
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
for i = 1, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
return 1
""")

_FAIL_IF_PROCESSING = _r.register_script("""
local current = redis.call('HGET', KEYS[1], 'status')
if not current or current ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'status', ARGV[2], 'error', ARGV[3])
return 1
""")


def _update(job_id: str, **fields) -> bool:
    args = []
    for name, value in fields.items():
        args.append(name)
        args.append(_enc(value))
    return bool(_UPDATE_IF_EXISTS(keys=[_key(job_id)], args=args))


def _scan(fields):
    """Reads the named fields for every indexed job in one round trip, and
    drops index entries whose record is gone."""
    job_ids = list(_r.smembers(_INDEX_KEY))
    if not job_ids:
        return []

    pipe = _r.pipeline()
    for job_id in job_ids:
        pipe.hmget(_key(job_id), fields)
    rows = pipe.execute()

    results = []
    orphaned = []
    for job_id, row in zip(job_ids, rows):
        if not row or all(v is None for v in row):
            orphaned.append(job_id)
            continue
        results.append((job_id, {f: _dec(v) for f, v in zip(fields, row)}))

    if orphaned:
        _r.srem(_INDEX_KEY, *orphaned)

    return results


def new_routed_id() -> str:
    """uuid4 hex, prefixed with this container's deploy slot."""
    return _INSTANCE_SLOT + uuid.uuid4().hex


def instance_slot() -> str:
    return _INSTANCE_SLOT


def create_job(job_type: str = "separation", ttl_seconds: Optional[int] = None) -> str:
    """
    Creates a new job entry and returns its id.

    ttl_seconds defaults to SEPARATION_JOB_TTL_SECONDS for job types in
    _LONG_TTL_JOB_TYPES and AUDIO_TOOL_JOB_TTL_SECONDS for everything else.
    """
    if ttl_seconds is None:
        ttl_seconds = (
            SEPARATION_JOB_TTL_SECONDS
            if job_type in _LONG_TTL_JOB_TYPES
            else AUDIO_TOOL_JOB_TTL_SECONDS
        )

    job_id = new_routed_id()
    record = {
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
        "input_path": None,
    }

    pipe = _r.pipeline()
    pipe.hset(_key(job_id), mapping={k: _enc(v) for k, v in record.items()})
    pipe.expire(_key(job_id), int(ttl_seconds) + _SAFETY_TTL_MARGIN)
    pipe.sadd(_INDEX_KEY, job_id)
    pipe.execute()

    return job_id


def set_job_input(job_id: str, input_path: str):
    """
    Records the source file a job was created from, so a finished standard
    separation can be re-run at HQ without a second upload.

    RETENTION, NOT OWNERSHIP. Setting this makes cleanup_expired_jobs()
    responsible for the file, on the job's own TTL. A route that calls this
    must NOT also pass the same path in _run_tool_job's cleanup_paths, or the
    file is deleted seconds after the job finishes and the retention is
    silently undone.
    """
    _update(job_id, input_path=input_path)


def mark_complete(job_id: str, title: str, vocals_path: str, instrumental_path: str):
    """Marks a SEPARATION-shaped job complete with its two stem paths."""
    _update(
        job_id,
        status="complete",
        title=title,
        vocals_path=vocals_path,
        instrumental_path=instrumental_path,
    )


def mark_stems_complete(job_id: str, title: str, stems: dict):
    """
    Marks a STEMS-shaped job complete with a {name: path} dict - the demucs
    stem names, or segment_01/segment_02/... for silence-split. A dict rather
    than named fields so the entry count is not baked into the schema.
    """
    _update(job_id, status="complete", title=title, stems=stems)


def mark_tool_complete(job_id: str, title: str, output_path: str, output_format: Optional[str] = None):
    """Marks an AUDIO-TOOL job complete with its single output file path."""
    _update(
        job_id,
        status="complete",
        title=title,
        output_path=output_path,
        output_format=output_format,
    )


def mark_transcription_complete(job_id: str, title: str, result_data: dict):
    """
    Marks a TRANSCRIPTION job complete with its result stored inline rather
    than as a file - the output is a few KB of JSON, so writing it to disk
    would be I/O for no benefit.
    """
    _update(job_id, status="complete", title=title, result_data=result_data)


def mark_data_complete(job_id: str, title: str, result_data: dict):
    """
    Generic version of mark_transcription_complete() for any other job whose
    output is inline data rather than a file - currently /youtube/analyze.
    Kept distinct so each call site's job shape stays self-documenting.
    """
    _update(job_id, status="complete", title=title, result_data=result_data)


def mark_failed(job_id: str, error: str):
    _update(job_id, status="failed", error=error)


def fail_if_unfinished(job_id: str, error: str = "The job ended unexpectedly.") -> bool:
    """
    Marks a job failed ONLY if it is still "processing". Returns True if it
    actually changed anything.

    Meant to be called from a `finally` block in every background task, where
    it is a no-op on the happy path and a rescue on the paths no `except`
    clause covers: acquire_slot_or_503() raising inside a background task,
    CancelledError on shutdown, or a bug inside an existing except block.
    Without it such a job sits at "processing" forever and the server-side
    logs show no failure at all.

    The WARNING is load-bearing: reaching this function means an exception
    escaped its intended handler.
    """
    changed = bool(
        _FAIL_IF_PROCESSING(
            keys=[_key(job_id)],
            args=[_enc("processing"), _enc("failed"), _enc(error)],
        )
    )
    if not changed:
        return False

    logger.warning(
        f"[JOBS] Job {job_id} was still 'processing' when its task ended - "
        f"force-failed via safety net: {error}"
    )
    return True


def get_job(job_id: str) -> Optional[dict]:
    raw = _r.hgetall(_key(job_id))
    if not raw:
        return None
    return {name: _dec(value) for name, value in raw.items()}


def count_processing(job_types: Optional[Iterable[str]] = None) -> int:
    """
    How many jobs are currently "processing", optionally restricted to
    specific job_types. This is the reading a route needs to enforce a
    bounded queue: the semaphore is acquired INSIDE the background task, so
    without this, extra submissions queue with no limit, each holding an
    uploaded file on disk.

    Counted live rather than from a separate counter, which would drift the
    first time an error path skipped its decrement.
    """
    rows = _scan(["status", "job_type"])
    if job_types is None:
        return sum(1 for _, r in rows if r["status"] == "processing")

    wanted = set(job_types)
    return sum(
        1 for _, r in rows
        if r["status"] == "processing" and r["job_type"] in wanted
    )


def get_job_stats() -> dict:
    """Snapshot of the job table for /admin/status and periodic logging."""
    rows = _scan(["status", "job_type", "created_at"])
    now = time.time()

    by_status = {"processing": 0, "complete": 0, "failed": 0}
    separation_processing = 0
    oldest_processing_age = 0.0

    for _, record in rows:
        status = record["status"]
        by_status[status] = by_status.get(status, 0) + 1
        if status == "processing":
            if record["job_type"] in SEPARATION_JOB_TYPES:
                separation_processing += 1
            created_at = record["created_at"]
            age = now - created_at if created_at is not None else 0.0
            if age > oldest_processing_age:
                oldest_processing_age = age

    return {
        "total": len(rows),
        **by_status,
        "separation_queue_depth": separation_processing,
        "oldest_processing_seconds": round(oldest_processing_age, 1),
    }


def cleanup_expired_jobs() -> int:
    """
    Deletes job entries and their on-disk files once older than their own
    ttl_seconds. Returns how many were removed.

    Call this on a background timer (main.py's _job_cleanup_loop), not from a
    request handler - otherwise every upload pays for someone else's cleanup.

    Handles all four job shapes. The stems dict MUST be walked separately from
    the named path fields: a stems job holds 4+ full-length WAVs, so missing
    them here leaks roughly double a separation job's disk per expired job,
    forever.

    ALSO SWEEPS input_path. The separation routes deliberately stopped passing
    their input in _run_tool_job's cleanup_paths so it survives for the
    upgrade-to-HQ path. If it were not collected here, every separation input
    would stay on disk permanently.

    An upgrade job and its source job share one input_path, so a sweep that
    expires both attempts the same delete twice. The per-path try/except makes
    that a harmless no-op.
    """
    now = time.time()
    paths_to_delete = []
    expired_count = 0

    candidates = []
    for job_id, record in _scan(["created_at", "ttl_seconds"]):
        created_at = record["created_at"]
        ttl = record["ttl_seconds"]
        if created_at is None:
            continue
        if ttl is None:
            ttl = SEPARATION_JOB_TTL_SECONDS
        if now - created_at > ttl:
            candidates.append(job_id)

    for job_id in candidates:
        # SREM returning 1 means this container removed the index entry, so
        # it owns the cleanup. Another container sweeping concurrently gets 0
        # and skips, which is what stops both deleting the same files.
        if _r.srem(_INDEX_KEY, job_id) != 1:
            continue

        record = _r.hgetall(_key(job_id))
        _r.delete(_key(job_id))
        if not record:
            continue
        expired_count += 1

        job = {name: _dec(value) for name, value in record.items()}

        for key in ("vocals_path", "instrumental_path", "output_path", "input_path"):
            path = job.get(key)
            if path:
                paths_to_delete.append(path)

        stems = job.get("stems")
        if isinstance(stems, dict):
            paths_to_delete.extend(p for p in stems.values() if p)

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