"""
tiktok/worker.py - Subprocess entrypoint for a single TikTok conversion.

WHY A SUBPROCESS AT ALL: identical reasoning to download_worker.py. A
wall-clock timeout in the parent can only abandon an asyncio await - it
cannot kill a thread, and it cannot kill the ffmpeg process yt-dlp
spawned underneath. Confirmed in production on the YouTube path
(2026-08-14): a timeout logged at 04:51 did not stop the underlying
call, which kept running and logged its own failure at 04:55, four
minutes later, holding a semaphore slot the whole time.

Running in its own process group (start_new_session=True in the parent)
means SIGKILL to the group takes yt-dlp AND ffmpeg with it.

NO BREAKER STATE HERE. download_worker.py has to export/import circuit
breaker state across the process boundary because youtube.py keeps
breakers in module-level globals that die with the process. tiktok/core.py
deliberately has no breakers - no proxy tier means nothing to trip - so
there is nothing to marshal. If someone later adds a breaker to core.py,
this file needs the same two-way plumbing download_worker.py has, and
forgetting it would silently disable the breaker rather than error.

USAGE (invoked by the parent, not by hand):
    python -m tiktok.worker <input_json_path> <output_json_path>

stdout/stderr are INHERITED from the parent deliberately - that is what
puts this process's [TIKTOK] lines into the same container log stream as
everything else. Piping them silently discarded every log line.
"""
import sys
import json
import traceback

from config import logger
from tiktok.core import (
    extract_and_download,
    TikTokError,
    TikTokTooLongError,
)


def main():
    if len(sys.argv) != 3:
        print("Usage: python -m tiktok.worker <in.json> <out.json>", file=sys.stderr)
        sys.exit(1)

    input_path, output_path = sys.argv[1], sys.argv[2]

    # A failure to even read the input is still reported as a normal
    # result rather than a crash, so the parent gets a classified error
    # instead of an empty output file it has to guess about.
    try:
        with open(input_path) as f:
            payload = json.load(f)
        url = payload["url"]
        out_dir = payload["out_dir"]
        job_id = payload["job_id"]
    except Exception as e:
        logger.error(f"[TIKTOK_WORKER] Could not read input payload: {e}")
        _write(output_path, {
            "ok": False,
            "kind": "crashed",
            "error": "Something went wrong while starting the conversion.",
        })
        return

    try:
        mp3_path, title, info = extract_and_download(url, out_dir, job_id)
        result = {
            "ok": True,
            "path": mp3_path,
            "title": title,
            # Returned so the parent can cache under the RESOLVED id.
            # Short links (vt./vm.) carry no id in the URL, so this is
            # the only place the real one becomes knowable.
            "id": str(info.get("id") or ""),
            "duration": info.get("duration"),
            "uploader": info.get("uploader") or info.get("channel") or "",
        }

    except TikTokTooLongError as e:
        result = {"ok": False, "kind": "too_long", "error": e.message}

    except TikTokError as e:
        # Already classified AND already user-facing - core.py never
        # lets raw yt-dlp text into `message`. The parent maps `kind` to
        # a status code and returns `error` verbatim.
        result = {"ok": False, "kind": e.kind, "error": e.message}

    except Exception as e:
        # Anything core.py did not anticipate. The traceback goes to the
        # log; the user gets a generic message. Never leak internals.
        logger.error(
            f"[TIKTOK_WORKER] Unhandled error for job={job_id}: {e}\n"
            f"{traceback.format_exc()}"
        )
        result = {
            "ok": False,
            "kind": "crashed",
            "error": "Something went wrong while converting this TikTok. "
                     "Please try again.",
        }

    _write(output_path, result)


def _write(path: str, result: dict):
    """Last line of defence: if the result file cannot be written the
    parent sees an empty/missing output and reports a crash, which is
    the correct outcome - but log loudly, because a disk-full condition
    here would otherwise look like a mysterious conversion failure."""
    try:
        with open(path, "w") as f:
            json.dump(result, f)
    except Exception as e:
        logger.error(f"[TIKTOK_WORKER] Could not write result file {path}: {e}")


if __name__ == "__main__":
    main()