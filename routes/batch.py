"""
routes/batch.py - Studio Quality batch separation.

    POST /batch/create                 kind=separate|stems, count=N
    POST /batch/{batch_id}/add         one file per call (Cloudflare body cap)
    POST /batch/{batch_id}/start
    GET  /batch/{batch_id}
    POST /batch/{batch_id}/cancel
    GET  /batch/{batch_id}/download?format=wav|mp3   streamed ZIP

Each file becomes an ordinary separation/stems job, so every existing
/separate/* and /stems/* status, preview and download route works on it
and Forge Mixer needs nothing new. Credits are held per job at add time
and returned by the normal runner on failure. Queued batch jobs sit at
status "queued", which count_processing() ignores, so a batch never
fills the shared separation queue; the runner feeds one job at a time
into the same _run_tool_job path the single routes use.
"""
from __future__ import annotations

import asyncio
import os
import re
import time
import zipfile
from typing import Iterable, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Path, Query, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from config import (
    logger,
    SEPARATION_HQ_ENABLED,
    SEPARATION_MODEL_HQ,
    SEPARATION_OVERLAP_HQ,
    DEMUCS_TIMEOUT_SECONDS_HQ,
    MAX_SEPARATION_DURATION_SECONDS_HQ,
    MAX_QUEUED_SEPARATIONS,
    BATCH_MAX_FILES,
    BATCH_MAX_CONCURRENT,
    BATCH_QUEUE_RESERVE,
    BATCH_MAX_PENDING_JOBS,
    BATCH_JOB_TTL_SECONDS,
    BATCH_COLLECT_TIMEOUT_SECONDS,
    BATCH_RATE_LIMIT_MAX_REQUESTS,
    BATCH_RATE_LIMIT_WINDOW_SECONDS,
)
from rate_limit import rate_limited
from redis_store import client as _r
from jobs import (
    _enc,
    _dec,
    _update,
    create_job,
    get_job,
    set_job_input,
    mark_complete,
    mark_stems_complete,
    mark_failed,
    count_processing,
    new_routed_id,
    SEPARATION_JOB_TYPES,
)
from separation import run_separation, run_stem_separation, get_audio_duration_seconds, SeparationError
from stem_mp3 import ensure_stem_mp3
from utils import _separation_semaphore, run_blocking, cleanup_file
from log_stream import remember_job_tags, set_job_context, tag_from_job
import job_tasks

from credits import metering, paywall
from credits import ledger as ledger_mod
from credits.identity import Identity
from credits.ledger import InsufficientCredits

from ._shared import spawn_background_task, _accept_upload, _log_queued, _run_tool_job

router = APIRouter()

_KEY = "af:batch:"
_INDEX = "af:batches:index"
_LOCK_TTL = 120
_batch_semaphore = asyncio.Semaphore(BATCH_MAX_CONCURRENT)

_KINDS = {
    "separate": {"job_type": "separation", "tool": "SEPARATION_HQ", "rule": "separate-hq", "metric": "/separate-hq"},
    "stems": {"job_type": "stems", "tool": "STEMS_HQ", "rule": "stems-hq", "metric": "/stems-hq"},
}

_limit = rate_limited(BATCH_RATE_LIMIT_MAX_REQUESTS, BATCH_RATE_LIMIT_WINDOW_SECONDS)


# --- store ---------------------------------------------------------------

def _bkey(batch_id: str) -> str:
    return _KEY + batch_id


def _jobs_key(batch_id: str) -> str:
    return _KEY + batch_id + ":jobs"


def _lock_key(batch_id: str) -> str:
    return _KEY + batch_id + ":lock"


def _get_batch(batch_id: str) -> Optional[dict]:
    raw = _r.hgetall(_bkey(batch_id))
    if not raw:
        return None
    batch = {k: _dec(v) for k, v in raw.items()}
    batch["job_ids"] = list(_r.lrange(_jobs_key(batch_id), 0, -1))
    return batch


def _set_batch(batch_id: str, **fields) -> None:
    _r.hset(_bkey(batch_id), mapping={k: _enc(v) for k, v in fields.items()})


def _create_batch(kind: str, expected: int, identity: Identity) -> str:
    batch_id = new_routed_id()
    ttl = BATCH_JOB_TTL_SECONDS + 86400
    pipe = _r.pipeline()
    pipe.hset(_bkey(batch_id), mapping={k: _enc(v) for k, v in {
        "kind": kind,
        "status": "collecting",
        "expected": expected,
        "subject_id": identity.subject_id,
        "created_at": time.time(),
        "started_at": None,
        "finished_at": None,
    }.items()})
    pipe.expire(_bkey(batch_id), ttl)
    pipe.sadd(_INDEX, batch_id)
    pipe.execute()
    return batch_id


def _append_job(batch_id: str, job_id: str) -> int:
    pipe = _r.pipeline()
    pipe.rpush(_jobs_key(batch_id), job_id)
    pipe.expire(_jobs_key(batch_id), BATCH_JOB_TTL_SECONDS + 86400)
    return int(pipe.execute()[0])


def _acquire_lock(batch_id: str) -> bool:
    return bool(_r.set(_lock_key(batch_id), "1", nx=True, ex=_LOCK_TTL))


def _refresh_lock(batch_id: str) -> None:
    _r.expire(_lock_key(batch_id), _LOCK_TTL)


def _release_lock(batch_id: str) -> None:
    _r.delete(_lock_key(batch_id))


def _pending_jobs_total() -> int:
    total = 0
    for batch_id in _r.smembers(_INDEX):
        batch = _get_batch(batch_id)
        if not batch or batch.get("status") not in ("collecting", "running"):
            continue
        for job_id in batch["job_ids"]:
            job = get_job(job_id)
            if job and job.get("status") == "queued":
                total += 1
    return total


def _load_or_404(batch_id: str) -> dict:
    batch = _get_batch(batch_id)
    if batch is None:
        raise HTTPException(404, "Batch not found (it may have expired).")
    return batch


def _require_owner(batch: dict, identity: Identity) -> None:
    if batch.get("subject_id") and batch["subject_id"] != identity.subject_id:
        raise HTTPException(403, "This batch belongs to another session.")


# --- affordability -------------------------------------------------------

def _affordable(identity: Identity, rule_key: str, count: int) -> tuple[bool, int, int, int]:
    from credits.db import connect

    decision = paywall.decide(rule_key, None)
    per_job = max(decision.credits, 1)
    with connect() as conn:
        balance = ledger_mod.get_balance(conn, identity)
        remaining = ledger_mod.free_remaining(conn, identity)
    if not decision.billable:
        return True, balance, remaining, 0
    needed = max(0, count - remaining) * per_job
    return balance >= needed, balance, remaining, needed


def _refund_queued(job_id: str, message: str, reason: str) -> None:
    job = get_job(job_id)
    if job is None or job.get("status") != "queued":
        return
    mark_failed(job_id, message)
    try:
        ledger_mod.refund_job(job_id, reason=reason)
    except Exception:
        logger.error(f"[BATCH] refund failed for job {job_id}", exc_info=True)
    try:
        metering.record_job_finished(job_id, status="failed", error=reason, client_side=True)
    except Exception:
        logger.warning(f"[BATCH] metrics close failed for job {job_id}", exc_info=True)


# --- runner --------------------------------------------------------------

async def _wait_for_room(batch_id: str) -> bool:
    while True:
        batch = _get_batch(batch_id)
        if batch is None or batch.get("status") != "running":
            return False
        depth = await asyncio.to_thread(count_processing, SEPARATION_JOB_TYPES)
        if depth <= MAX_QUEUED_SEPARATIONS - BATCH_QUEUE_RESERVE:
            return True
        _refresh_lock(batch_id)
        await asyncio.sleep(3)


def _hold_state(job_id: str) -> Optional[str]:
    from credits.db import connect

    with connect() as conn:
        row = conn.execute("SELECT status FROM job_charges WHERE job_id=?", (job_id,)).fetchone()
    return row["status"] if row else None


def _paid_with_credits(job_id: str) -> bool:
    from credits.db import connect

    with connect() as conn:
        row = conn.execute("SELECT charge_type FROM job_charges WHERE job_id=?", (job_id,)).fetchone()
    return bool(row) and row["charge_type"] == "credit"


async def _run_one(batch_id: str, kind: dict, job_id: str) -> None:
    job = get_job(job_id)
    if job is None or job.get("status") != "queued":
        return

    hold = await asyncio.to_thread(_hold_state, job_id)
    if hold == "refunded":
        mark_failed(job_id, "This track waited too long in the batch and its credit was returned. Run it again.")
        return

    input_path = job.get("input_path")
    if not input_path or not os.path.exists(input_path):
        _refund_queued(job_id, "The uploaded file expired before this track ran.", "batch_input_expired")
        return

    title = job.get("title") or os.path.basename(input_path)
    is_stems = kind["job_type"] == "stems"
    paid = await asyncio.to_thread(_paid_with_credits, job_id)
    set_job_context(tool="STEMS" if is_stems else "SEPARATION", tier="hq")

    if is_stems:
        work = lambda: run_stem_separation(
            input_path, job_id, SEPARATION_MODEL_HQ, SEPARATION_OVERLAP_HQ,
            DEMUCS_TIMEOUT_SECONDS_HQ, MAX_SEPARATION_DURATION_SECONDS_HQ, paid=paid,
        )
        on_success = lambda stems: mark_stems_complete(job_id, title, stems)
        success_detail = lambda stems: f"{len(stems)} stems"
        generic_error = "Stem separation failed unexpectedly."
    else:
        work = lambda: run_separation(
            input_path, job_id, SEPARATION_MODEL_HQ, SEPARATION_OVERLAP_HQ,
            DEMUCS_TIMEOUT_SECONDS_HQ, MAX_SEPARATION_DURATION_SECONDS_HQ, paid=paid,
        )
        on_success = lambda paths: mark_complete(job_id, title, paths[0], paths[1])
        success_detail = None
        generic_error = "Separation failed unexpectedly."

    if not _update(job_id, status="processing"):
        return

    logger.info(f"[BATCH] batch={batch_id} job={job_id} starting '{title}'")
    task = spawn_background_task(_run_tool_job(
        tool=kind["tool"],
        metric=kind["metric"],
        job_id=job_id,
        semaphore=_separation_semaphore,
        work=work,
        on_success=on_success,
        generic_error=generic_error,
        cleanup_paths=[],
        success_detail=success_detail,
        gpu_billed=False,
        metered_tool=kind["rule"],
    ))
    try:
        while True:
            done, _ = await asyncio.wait([task], timeout=30)
            if done:
                break
            _refresh_lock(batch_id)
    except asyncio.CancelledError:
        task.cancel()
        raise


async def _run_batch(batch_id: str) -> None:
    try:
        batch = _get_batch(batch_id)
        if batch is None or batch.get("status") != "running":
            return
        kind = _KINDS[batch["kind"]]
        for job_id in batch["job_ids"]:
            if not await _wait_for_room(batch_id):
                break
            async with _batch_semaphore:
                _refresh_lock(batch_id)
                await _run_one(batch_id, kind, job_id)

        batch = _get_batch(batch_id)
        if batch and batch.get("status") == "running":
            _set_batch(batch_id, status="done", finished_at=time.time())
            logger.info(f"[BATCH] batch={batch_id} done ({len(batch['job_ids'])} jobs)")
    except asyncio.CancelledError:
        logger.warning(f"[BATCH] batch={batch_id} runner cancelled (shutdown); will resume elsewhere")
        raise
    except Exception:
        logger.error(f"[BATCH] batch={batch_id} runner crashed", exc_info=True)
    finally:
        _release_lock(batch_id)


def _start_runner(batch_id: str) -> bool:
    if not _acquire_lock(batch_id):
        return False
    spawn_background_task(_run_batch(batch_id))
    return True


async def batch_reaper_loop(interval_seconds: int = 60) -> None:
    await asyncio.sleep(15)
    while True:
        try:
            for batch_id in await asyncio.to_thread(_reap_once):
                if _start_runner(batch_id):
                    logger.warning(f"[BATCH] batch={batch_id} had no runner, resumed")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.error("[BATCH] reaper failed", exc_info=True)
        await asyncio.sleep(interval_seconds)


def _reap_once() -> list:
    now = time.time()
    resume = []
    for batch_id in list(_r.smembers(_INDEX)):
        batch = _get_batch(batch_id)
        if batch is None:
            _r.srem(_INDEX, batch_id)
            continue
        status = batch.get("status")
        created = batch.get("created_at") or 0
        if status == "collecting" and now - created > BATCH_COLLECT_TIMEOUT_SECONDS:
            for job_id in batch["job_ids"]:
                _refund_queued(job_id, "This batch was never started and its credits were returned.", "batch_never_started")
            _set_batch(batch_id, status="expired", finished_at=now)
            logger.info(f"[BATCH] batch={batch_id} expired unstarted")
        elif status == "running" and not _r.exists(_lock_key(batch_id)):
            resume.append(batch_id)
        elif status in ("done", "cancelled", "expired") and now - created > BATCH_JOB_TTL_SECONDS:
            _r.srem(_INDEX, batch_id)
    return resume


# --- routes --------------------------------------------------------------

@router.post("/batch/create", dependencies=[Depends(_limit)])
async def batch_create(
    kind: str = Form(...),
    count: int = Form(...),
    identity: Identity = Depends(paywall.get_identity),
):
    if kind not in _KINDS:
        raise HTTPException(400, "kind must be 'separate' or 'stems'")
    if count < 1 or count > BATCH_MAX_FILES:
        raise HTTPException(400, f"count must be between 1 and {BATCH_MAX_FILES}")
    if not SEPARATION_HQ_ENABLED:
        raise HTTPException(503, "Studio Quality separation is temporarily unavailable.")

    rule_key = _KINDS[kind]["rule"]
    affordable, balance, remaining, needed = await asyncio.to_thread(_affordable, identity, rule_key, count)
    if not affordable:
        exc = InsufficientCredits(balance=balance, free_remaining=remaining, tool=rule_key, needed=needed)
        raise paywall.insufficient_credits_response(exc)

    pending = await asyncio.to_thread(_pending_jobs_total)
    if pending + count > BATCH_MAX_PENDING_JOBS:
        raise HTTPException(503, "The batch queue is full right now. Try again in a few minutes.")

    batch_id = _create_batch(kind, count, identity)
    logger.info(f"[BATCH] batch={batch_id} created kind={kind} count={count}")
    return {
        "batch_id": batch_id,
        "kind": kind,
        "expected": count,
        "credits_per_track": max(paywall.decide(rule_key, None).credits, 1),
        "balance": balance,
        "free_remaining": remaining,
    }


@router.post("/batch/{batch_id}/add", dependencies=[Depends(_limit)])
async def batch_add(
    batch_id: str = Path(..., max_length=64),
    file: UploadFile = File(...),
    identity: Identity = Depends(paywall.get_identity),
):
    batch = _load_or_404(batch_id)
    _require_owner(batch, identity)
    if batch.get("status") != "collecting":
        raise HTTPException(409, "This batch is no longer accepting files.")
    if len(batch["job_ids"]) >= int(batch["expected"]):
        raise HTTPException(409, "This batch already has all its files.")

    kind = _KINDS[batch["kind"]]
    set_job_context(tool="STEMS" if kind["job_type"] == "stems" else "SEPARATION", tier="hq")

    original_filename = file.filename
    job_id = create_job(job_type=kind["job_type"], ttl_seconds=BATCH_JOB_TTL_SECONDS)
    remember_job_tags(job_id)
    file_path, size = await _accept_upload(file, job_id, label=kind["tool"].lower())
    set_job_input(job_id, file_path)

    metering.record_job_created(
        job_id=job_id, tool=kind["rule"],
        subject_id=identity.subject_id, account_id=identity.account_id, ip_hash=identity.ip_hash,
        input_bytes=size, charge_type=None,
    )

    try:
        duration = await run_blocking(get_audio_duration_seconds, file_path)
    except SeparationError as e:
        cleanup_file(file_path)
        mark_failed(job_id, str(e))
        metering.record_job_rejected(job_id, "unreadable_audio")
        raise HTTPException(400, {"kind": "unreadable_audio", "message": str(e), "job_id": job_id})

    if duration > MAX_SEPARATION_DURATION_SECONDS_HQ:
        message = (
            f"This track is {int(duration // 60)} min long. Studio Quality is limited "
            f"to {MAX_SEPARATION_DURATION_SECONDS_HQ // 60} min per track."
        )
        cleanup_file(file_path)
        mark_failed(job_id, message)
        metering.record_job_rejected(job_id, "hq_duration_exceeded")
        raise HTTPException(400, {
            "kind": "hq_duration_exceeded", "message": message, "job_id": job_id,
            "input_seconds": round(duration, 1), "max_seconds": MAX_SEPARATION_DURATION_SECONDS_HQ,
        })

    decision = paywall.decide(kind["rule"], duration)
    try:
        charge = await asyncio.to_thread(
            ledger_mod.charge_for_job, identity,
            job_id=job_id, tool=kind["rule"],
            credits_needed=max(decision.credits, 1),
            free_ops_needed=max(decision.free_ops, 1),
            billable=decision.billable,
        )
    except InsufficientCredits as exc:
        cleanup_file(file_path)
        mark_failed(job_id, "Out of credits.")
        metering.record_job_rejected(job_id, "blocked_at_submit")
        raise paywall.insufficient_credits_response(exc) from exc

    _update(job_id, status="queued", title=original_filename)
    metering.record_job_created(
        job_id=job_id, tool=kind["rule"],
        subject_id=identity.subject_id, account_id=identity.account_id, ip_hash=identity.ip_hash,
        input_seconds=duration, input_bytes=size, charge_type=charge.charge_type,
    )
    index = _append_job(batch_id, job_id)
    _log_queued(kind["tool"], job_id, original_filename, size, f"batch={batch_id} #{index} charged={charge.charge_type}")

    return JSONResponse({
        "batch_id": batch_id,
        "job_id": job_id,
        "index": index,
        "title": original_filename,
        "input_seconds": round(duration, 1),
        "billing": {
            "charged": charge.charge_type,
            "credits": charge.credits,
            "balance": charge.balance_after,
            "free_remaining": charge.free_remaining_after,
        },
    })


@router.post("/batch/{batch_id}/start", dependencies=[Depends(_limit)])
async def batch_start(
    batch_id: str = Path(..., max_length=64),
    identity: Identity = Depends(paywall.get_identity),
):
    batch = _load_or_404(batch_id)
    _require_owner(batch, identity)
    if batch.get("status") == "running":
        return {"batch_id": batch_id, "status": "running"}
    if batch.get("status") != "collecting":
        raise HTTPException(409, f"This batch is {batch.get('status')}.")
    if not batch["job_ids"]:
        raise HTTPException(400, "Add at least one file before starting.")

    _set_batch(batch_id, status="running", started_at=time.time(), expected=len(batch["job_ids"]))
    _start_runner(batch_id)
    logger.info(f"[BATCH] batch={batch_id} started with {len(batch['job_ids'])} jobs")
    return {"batch_id": batch_id, "status": "running", "jobs": len(batch["job_ids"])}


def _job_view(index: int, job_id: str) -> dict:
    job = get_job(job_id)
    if job is None:
        return {"job_id": job_id, "index": index, "status": "expired", "title": None, "error": None, "stems": []}
    stems: list = []
    if job.get("status") == "complete":
        names = set((job.get("stems") or {}).keys())
        if job.get("vocals_path"):
            names |= {"vocals", "instrumental"}
        stems = sorted(names)
    return {
        "job_id": job_id,
        "index": index,
        "status": job.get("status"),
        "title": job.get("title"),
        "error": job.get("error"),
        "stems": stems,
    }


@router.get("/batch/{batch_id}")
async def batch_status(batch_id: str = Path(..., max_length=64)):
    batch = _load_or_404(batch_id)
    jobs = [_job_view(i + 1, jid) for i, jid in enumerate(batch["job_ids"])]
    counts = {"complete": 0, "failed": 0, "processing": 0, "queued": 0, "expired": 0}
    for j in jobs:
        counts[j["status"] if j["status"] in counts else "failed"] += 1
    created = batch.get("created_at") or time.time()
    return {
        "batch_id": batch_id,
        "kind": batch.get("kind"),
        "status": batch.get("status"),
        "expected": batch.get("expected"),
        "jobs": jobs,
        "counts": counts,
        "created_at": created,
        "expires_at": created + BATCH_JOB_TTL_SECONDS,
        "elapsed_seconds": round(time.time() - created, 1),
    }


@router.post("/batch/{batch_id}/cancel", dependencies=[Depends(_limit)])
async def batch_cancel(
    batch_id: str = Path(..., max_length=64),
    identity: Identity = Depends(paywall.get_identity),
):
    batch = _load_or_404(batch_id)
    _require_owner(batch, identity)
    if batch.get("status") in ("done", "cancelled", "expired"):
        return {"batch_id": batch_id, "status": batch.get("status"), "cancelled": False}

    _set_batch(batch_id, status="cancelled", finished_at=time.time())
    refunded = 0
    stopped = 0
    for job_id in batch["job_ids"]:
        job = get_job(job_id)
        if job is None:
            continue
        if job.get("status") == "queued":
            await asyncio.to_thread(_refund_queued, job_id, "You cancelled this batch before this track ran.", "batch_cancelled")
            refunded += 1
        elif job.get("status") == "processing" and job_tasks.cancel(job_id):
            stopped += 1
    logger.info(f"[BATCH] batch={batch_id} cancelled by user (refunded={refunded} stopped={stopped})")
    return {"batch_id": batch_id, "status": "cancelled", "cancelled": True, "refunded": refunded, "stopped": stopped}


# --- zip download --------------------------------------------------------

class _Sink:
    def __init__(self):
        self.buf = bytearray()

    def write(self, data):
        self.buf.extend(data)
        return len(data)

    def flush(self):
        pass

    def drain(self) -> bytes:
        out = bytes(self.buf)
        self.buf.clear()
        return out


def _safe_name(name: str) -> str:
    base = os.path.splitext(os.path.basename(name or ""))[0]
    base = re.sub(r"[^\w\s().,&+'-]", "_", base, flags=re.UNICODE).strip()
    return base[:80] or "track"


def _collect_entries(batch: dict, fmt: str) -> list:
    entries = []
    for i, job_id in enumerate(batch["job_ids"], start=1):
        job = get_job(job_id)
        if not job or job.get("status") != "complete":
            continue
        title = _safe_name(job.get("title") or job_id)
        files = dict(job.get("stems") or {})
        if job.get("vocals_path"):
            files.update({"vocals": job.get("vocals_path"), "instrumental": job.get("instrumental_path")})
        for stem, path in sorted(files.items()):
            if path and os.path.exists(path):
                entries.append((f"{i:02d} - {title} - {stem}.{fmt}", path))
    return entries


def _zip_stream(entries: Iterable[tuple], fmt: str):
    sink = _Sink()
    with zipfile.ZipFile(sink, mode="w", compression=zipfile.ZIP_STORED, allowZip64=True) as zf:
        for name, wav_path in entries:
            src = ensure_stem_mp3(wav_path) if fmt == "mp3" else wav_path
            info = zipfile.ZipInfo(name, date_time=time.localtime(time.time())[:6])
            info.compress_type = zipfile.ZIP_STORED
            with open(src, "rb") as fh, zf.open(info, "w", force_zip64=True) as dst:
                while True:
                    chunk = fh.read(1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)
                    yield sink.drain()
            yield sink.drain()
    yield sink.drain()


@router.get("/batch/{batch_id}/download")
async def batch_download(
    batch_id: str = Path(..., max_length=64),
    format: str = Query("wav", pattern="^(wav|mp3)$"),
):
    batch = _load_or_404(batch_id)
    entries = await asyncio.to_thread(_collect_entries, batch, format)
    if not entries:
        raise HTTPException(409, "No finished tracks to download yet.")
    tag_from_job(batch["job_ids"][0])
    filename = f"audioforges-batch-{batch_id[:8]}-{format}.zip"
    return StreamingResponse(
        _zip_stream(entries, format),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )