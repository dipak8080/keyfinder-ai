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
import json
import signal
import asyncio

from config import logger


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
                f"[TIKTOK] job={job_id} wall-clock timeout ({timeout_seconds}s) - "
                f"killing process group"
            )
            _kill_group(proc, job_id)
            return {
                "ok": False,
                "kind": "unknown",
                "error": "This conversion is taking too long. Please try again.",
            }

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