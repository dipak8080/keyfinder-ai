"""
tiktok/runner.py - Spawns tiktok/worker.py as a killable subprocess.

Mirrors utils.run_in_killable_subprocess but kept separate because the
payload shape differs (no ydl_opts, no proxy_url, no breaker_state) and
overloading the YouTube runner with a source flag would put two
unrelated contracts in one function.

THE POINT OF ALL THIS: start_new_session=True puts the child in its own
PROCESS GROUP. On timeout we then SIGKILL the whole group - the worker,
yt-dlp, and the ffmpeg process underneath it. Killing only the direct
child leaves ffmpeg running unsupervised, holding CPU and disk on a box
with no swap. Confirmed necessary on the YouTube path in production.
"""
import os
import sys
import glob
import json
import signal
import asyncio

from config import logger

# A normal conversion takes ~10s. A stalled TikTok CDN transfer hangs
# silently, so the budget is split and a stalled attempt is killed and
# retried instead of eating the whole wall clock.
STALL_ATTEMPTS = 2
_TIMEOUT_RESULT = {
    "ok": False,
    "kind": "unknown",
    "error": "This conversion is taking too long. Please try again.",
}


def _clear_partials(out_dir: str, job_id: str) -> None:
    for p in glob.glob(os.path.join(out_dir, f"{job_id}_tiktok.*")):
        try:
            os.remove(p)
        except OSError:
            pass


async def run_tiktok_in_subprocess(
    url: str,
    out_dir: str,
    job_id: str,
    timeout_seconds: int,
    request_id: str = "-",
) -> dict:
    """
    Returns the worker's result dict. NEVER raises for a conversion
    failure - a failure comes back as {"ok": False, "kind": ..., "error": ...}
    so the route layer has exactly one shape to handle.
    """
    per_attempt = max(45, timeout_seconds // STALL_ATTEMPTS)
    for attempt in range(1, STALL_ATTEMPTS + 1):
        result = await _run_once(url, out_dir, job_id, per_attempt, request_id, attempt)
        if result is not None:
            return result
        _clear_partials(out_dir, job_id)
        if attempt < STALL_ATTEMPTS:
            logger.warning(f"[TIKTOK] job={job_id} attempt {attempt} stalled, retrying")
    return dict(_TIMEOUT_RESULT)


async def _run_once(
    url: str,
    out_dir: str,
    job_id: str,
    timeout_seconds: int,
    request_id: str,
    attempt: int,
) -> dict | None:
    """One worker run. Returns None on a wall-clock stall so the caller can retry."""
    in_path = os.path.join(out_dir, f"{job_id}_tt_in.json")
    out_path = os.path.join(out_dir, f"{job_id}_tt_out.json")

    payload = {
        "url": url,
        "out_dir": out_dir,
        "job_id": job_id,
        "request_id": request_id,
    }

    proc = None
    try:
        with open(in_path, "w") as f:
            json.dump(payload, f)

        # sys.executable, NOT the literal "python". This container's
        # interpreter is python3 and there is no guarantee a bare
        # `python` exists on PATH - if it does not, EVERY request fails
        # with FileNotFoundError before the worker ever runs.
        # sys.executable is by definition the interpreter already
        # running this process, so it is always correct.
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "tiktok.worker", in_path, out_path,
            # stdout/stderr inherited on purpose - see worker docstring.
            start_new_session=True,
        )

        try:
            await asyncio.wait_for(proc.wait(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            logger.warning(
                f"[TIKTOK] job={job_id} attempt {attempt} wall-clock timeout "
                f"({timeout_seconds}s) - killing process group"
            )
            _kill_group(proc, job_id)
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass
            return None

        if not os.path.exists(out_path):
            # Worker died without writing a result - OOM killer, segfault,
            # or an import error. Distinct from a classified failure.
            logger.error(
                f"[TIKTOK] job={job_id} worker exited rc={proc.returncode} "
                f"without writing a result file"
            )
            return {
                "ok": False,
                "kind": "crashed",
                "error": "Something went wrong while converting this TikTok. "
                         "Please try again.",
            }

        with open(out_path) as f:
            return json.load(f)

    except Exception as e:
        logger.error(f"[TIKTOK] job={job_id} runner error: {e}", exc_info=True)
        if proc is not None and proc.returncode is None:
            _kill_group(proc, job_id)
        return {
            "ok": False,
            "kind": "crashed",
            "error": "Something went wrong while converting this TikTok. "
                     "Please try again.",
        }

    finally:
        # Temp JSON is cleaned regardless of outcome. These are tiny but
        # one per request adds up, and a stale _tt_in.json is confusing
        # to find on disk during an incident.
        for p in (in_path, out_path):
            try:
                if os.path.exists(p):
                    os.remove(p)
                    logger.info(f"Cleaned up temp file: {p}")
            except OSError as e:
                logger.warning(f"[TIKTOK] could not remove {p}: {e}")


def _kill_group(proc, job_id: str):
    """SIGKILL the child's whole process group.

    Wrapped in try/except because the process may have exited between
    the timeout firing and this call - a race that would otherwise turn
    a handled timeout into an unhandled ProcessLookupError."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass  # already gone - nothing to kill
    except Exception as e:
        logger.warning(f"[TIKTOK] job={job_id} could not kill process group: {e}")