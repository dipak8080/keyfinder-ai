"""
download_progress.py - yt-dlp progress hook that surfaces live download
percentage into the System Logs dashboard (/admin/logs -> System Logs tab).

HOW TO WIRE THIS IN:
In wherever you build the ydl_opts dict for a real download (likely in
routes.py, right before calling youtube.download_with_fallback), add:

    from download_progress import make_progress_hook

    ydl_opts = {
        ...(your existing options)...,
        "progress_hooks": [make_progress_hook(video_id_or_url)],
    }

That's the only change needed - this file is self-contained and uses the
same `logger` (from config.py) that's already being captured into the
System Logs stream via log_stream.py's BufferLogHandler, so no other
wiring is required.
"""

import time
from config import logger

# Only log a new line every this many percent, so a fast download doesn't
# flood the System Logs stream with dozens of near-identical lines a
# second - yt-dlp's hook fires very frequently (multiple times/sec).
PROGRESS_LOG_STEP_PERCENT = 10

# Also throttle by TIME, independent of percent - protects against a
# large/slow download where percent barely moves for a while (the time
# throttle guarantees you still see periodic "still going" updates rather
# than nothing for 30+ seconds).
PROGRESS_LOG_MIN_INTERVAL_SECONDS = 3


def make_progress_hook(label: str):
    """
    Returns a NEW hook function scoped to this specific download (label is
    typically the video_id or URL) - each download gets its own throttle
    state, so concurrent downloads don't interfere with each other's
    "last logged at X%" tracking.
    """
    state = {"last_logged_percent": -1, "last_logged_time": 0.0}

    def hook(d):
        try:
            status = d.get("status")

            if status == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate")
                downloaded = d.get("downloaded_bytes", 0)
                if not total:
                    return

                percent = (downloaded / total) * 100
                now = time.time()

                crossed_step = percent - state["last_logged_percent"] >= PROGRESS_LOG_STEP_PERCENT
                enough_time_passed = now - state["last_logged_time"] >= PROGRESS_LOG_MIN_INTERVAL_SECONDS

                if crossed_step or (enough_time_passed and percent > state["last_logged_percent"]):
                    speed = d.get("speed")
                    eta = d.get("eta")
                    speed_str = f"{speed / 1024 / 1024:.1f} MB/s" if speed else "..."
                    eta_str = f"{eta}s" if eta is not None else "..."
                    downloaded_mb = downloaded / 1024 / 1024
                    total_mb = total / 1024 / 1024

                    logger.info(
                        f"[DOWNLOAD] {label}: {percent:.0f}% "
                        f"({downloaded_mb:.1f}MB / {total_mb:.1f}MB) "
                        f"- {speed_str} - ETA {eta_str}"
                    )
                    state["last_logged_percent"] = percent
                    state["last_logged_time"] = now

            elif status == "finished":
                logger.info(f"[DOWNLOAD] {label}: 100% complete - post-processing...")

            elif status == "error":
                logger.warning(f"[DOWNLOAD] {label}: download hook reported an error")

        except Exception as e:
            logger.warning(f"[DOWNLOAD] Progress hook error (non-fatal): {e}")

    return hook