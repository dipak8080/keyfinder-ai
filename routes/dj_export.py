"""DJ export: every stem of a finished separation in one ZIP, named with
the track's Camelot key and BPM so it drops straight into rekordbox or
Serato.

    GET /separate/analysis/{job_id}   key, Camelot and BPM (cached on the job)
    GET /separate/export/{job_id}     ZIP of all stems, wav or mp3

Works for upload and YouTube jobs, vocal remover and stems alike. Key and
BPM come from the original mix when it is still kept, otherwise from the
instrumental, and are computed once per job."""

import asyncio
import os
import re

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from fastapi.responses import StreamingResponse

from config import logger, ANALYSIS_MAX_SECONDS
from jobs import get_job, set_job_fields
from log_stream import tag_from_job
from rate_limit import check_rate_limit
from utils import _analysis_semaphore, acquire_slot_or_503, release_memory_to_os, run_blocking, get_camelot

from .batch import _zip_stream

router = APIRouter()

def _export_limit(request: Request) -> None:
    check_rate_limit(request, max_requests=20, window_seconds=3600, bucket_key="/separate/export")


def _analysis_limit(request: Request) -> None:
    check_rate_limit(request, max_requests=60, window_seconds=3600, bucket_key="/separate/analysis")


_analysis_locks: dict = {}

EXPORT_JOB_TYPES = ("separation", "stems", "youtube_separate", "youtube_stems")
_ANALYSIS_SOURCES = ("instrumental", "other", "drums", "bass", "vocals")


def _load_job(job_id: str) -> dict:
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job.get("job_type") not in EXPORT_JOB_TYPES:
        raise HTTPException(404, "Job not found (it may have expired).")
    if job.get("status") != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job.get('status')}).")
    return job


def _stem_files(job: dict) -> dict:
    files = {k: v for k, v in (job.get("stems") or {}).items() if v and os.path.exists(v)}
    for stem, key in (("vocals", "vocals_path"), ("instrumental", "instrumental_path")):
        path = job.get(key)
        if path and os.path.exists(path):
            files[stem] = path
    return files


def _analyze(path: str) -> dict:
    import audio_analysis as aa

    trimmed = aa.trim_audio_for_analysis(path, ANALYSIS_MAX_SECONDS) if ANALYSIS_MAX_SECONDS else path
    audio = None
    try:
        key, scale, key_conf, bpm, bpm_conf, audio, sr = aa.detect_key_bpm_essentia(trimmed)
        key, scale, key_conf, bpm, bpm_conf, _ = aa.cross_check_with_librosa(
            audio, sr, key, scale, key_conf, bpm, bpm_conf)
    finally:
        del audio
        if trimmed != path:
            try:
                os.remove(trimmed)
            except OSError:
                pass
        release_memory_to_os()
    return {"key": f"{key} {scale}", "camelot": get_camelot(key, scale), "bpm": int(round(bpm)) if bpm else None}


async def _key_bpm(job_id: str, job: dict) -> dict:
    if job.get("dj_analysis"):
        return job["dj_analysis"]
    lock = _analysis_locks.setdefault(job_id, asyncio.Lock())
    try:
        async with lock:
            fresh = get_job(job_id) or job
            if fresh.get("dj_analysis"):
                return fresh["dj_analysis"]
            return await _analyze_once(job_id, fresh)
    finally:
        if not lock.locked() and not getattr(lock, "_waiters", None):
            _analysis_locks.pop(job_id, None)


async def _analyze_once(job_id: str, job: dict) -> dict:
    files = _stem_files(job)
    source = job.get("input_path") if job.get("input_path") and os.path.exists(job["input_path"]) else None
    source = source or next((files[s] for s in _ANALYSIS_SOURCES if s in files), None)
    if source is None:
        return {}
    await acquire_slot_or_503(_analysis_semaphore, "analysis")
    try:
        result = await run_blocking(_analyze, source)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[DJ_EXPORT] job={job_id} key/BPM analysis failed: {e}")
        result = {}
    finally:
        _analysis_semaphore.release()
    if result:
        set_job_fields(job_id, dj_analysis=result)
    return result


def _clean(text: str) -> str:
    text = os.path.splitext(os.path.basename(text or ""))[0]
    text = re.sub(r"[^\w\s().,&+'-]", "_", text, flags=re.UNICODE).strip()
    return text[:80] or "track"


def _tag(analysis: dict) -> str:
    parts = []
    if analysis.get("camelot") and analysis["camelot"] != "Unknown":
        parts.append(analysis["camelot"])
    if analysis.get("bpm"):
        parts.append(f"{analysis['bpm']} BPM")
    return " - ".join(parts)


@router.get("/separate/analysis/{job_id}", dependencies=[Depends(_analysis_limit)])
async def dj_analysis(job_id: str = Path(..., max_length=64)) -> dict:
    job = _load_job(job_id)
    return {"job_id": job_id, **(await _key_bpm(job_id, job))}


@router.get("/separate/export/{job_id}", dependencies=[Depends(_export_limit)])
async def dj_export(
    job_id: str = Path(..., max_length=64),
    format: str = Query("wav", pattern="^(wav|mp3)$"),
):
    job = _load_job(job_id)
    files = _stem_files(job)
    if not files:
        raise HTTPException(404, "Stem files not found (they may have expired).")
    analysis = await _key_bpm(job_id, job)
    title = _clean(job.get("title") or job_id)
    tag = _tag(analysis)
    prefix = f"{title} - {tag}" if tag else title
    entries = [(f"{prefix} - {stem}.{format}", path) for stem, path in sorted(files.items())]
    zip_name = f"{prefix} - stems.zip".replace('"', "")
    logger.info(f"[DJ_EXPORT] job={job_id} {len(entries)} stems as {format} ({tag or 'no key/BPM'})")
    return StreamingResponse(
        _zip_stream(entries, format),
        media_type="application/zip",
        headers={
            "Content-Disposition": (f'attachment; filename="{_ascii(zip_name)}"; '
                                    f"filename*=UTF-8''{_quote(zip_name)}"),
            "Cache-Control": "no-store",
        },
    )


def _quote(name: str) -> str:
    from urllib.parse import quote
    return quote(name, safe="")


def _ascii(name: str) -> str:
    return name.encode("ascii", "ignore").decode().replace('"', "") or "stems.zip"