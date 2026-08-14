"""
download_worker.py - Subprocess entrypoint for a single YouTube download
attempt. Spawned by utils.run_in_killable_subprocess() as its own OS
process (own process group, via start_new_session=True), so a wall-clock
timeout in the parent can SIGKILL this ENTIRE process tree - this script
plus whatever yt-dlp spawns under it (the Node PO-token generator, a
possible Deno JS-challenge solver, ffmpeg for postprocessing) - instead
of only abandoning an asyncio await while those children keep running
unsupervised.

WHY THIS EXISTS: see utils.py's "KILLABLE SUBPROCESS FOR YOUTUBE
DOWNLOADS" comment block for the full incident writeup. Short version -
confirmed in production 2026-08-14: a wall-clock timeout logged at
4:51:04 didn't actually stop the underlying yt-dlp call, which kept
running and logged its own failure at 4:55:26 - over 4 minutes later,
on a thread pool with no way to force-kill a thread. This script exists
so that timeout can kill something real.

USAGE (invoked by utils.run_in_killable_subprocess, not run by hand):
    python download_worker.py <input_json_path> <output_json_path>

input_json_path contains: {"ydl_opts": {...}, "url": "...", "proxy_url": "..."}
  - ydl_opts must NOT contain 'logger' or 'progress_hooks' - neither
    survives a JSON boundary. This script reconstructs its own
    ytdlp_alert_logger below. progress_hooks (used only by the live
    /download progress UI) are not reconstructed here - the chained
    /youtube/* tools never had them either, and /download's progress
    polling falls back gracefully to plain status-based waiting for the
    duration of a subprocess-backed download.

output_json_path is written with one of:
    {"ok": true, "title": "..."}
    {"ok": false, "kind": "too_long", "error": "..."}
    {"ok": false, "kind": "error", "error": "..."}

Exit code is always 0 on a clean run (even a classified download
failure is NOT a crash - the result dict carries that information).
A non-zero exit code or a missing output file means this script itself
crashed unexpectedly (bad input JSON, import failure, etc.) - the
parent (run_in_killable_subprocess) already handles that case by
returning kind="crashed".
"""
import sys
import json

from youtube import (
    download_with_fallback,
    ytdlp_alert_logger,
    VideoTooLongError,
)


def main():
    if len(sys.argv) != 3:
        print("Usage: python download_worker.py <input_json_path> <output_json_path>", file=sys.stderr)
        sys.exit(1)

    input_path, output_path = sys.argv[1], sys.argv[2]

    with open(input_path) as f:
        payload = json.load(f)

    ydl_opts = payload["ydl_opts"]
    url = payload["url"]
    proxy_url = payload.get("proxy_url")

    # Reconstructed here rather than passed in - the live logger object
    # in youtube.py can't survive a JSON boundary, and this process needs
    # its own reference to the SAME shared alert/cookie-health machinery
    # (per-account failure tracking, cookie-expiry Discord alerts, etc.)
    # that youtube.py already defines. Importing ytdlp_alert_logger here
    # gives this subprocess the identical behavior /download and the
    # chained tools had before - the alerting logic itself lives in
    # youtube.py, unchanged.
    ydl_opts["logger"] = ytdlp_alert_logger

    try:
        info = download_with_fallback(ydl_opts, url, proxy_url)
        result = {"ok": True, "title": info.get("title", "Unknown")}

    except VideoTooLongError as e:
        result = {"ok": False, "kind": "too_long", "error": str(e)}

    except Exception as e:
        # Every other failure - permanent, geo-restricted, bot-check,
        # CDN timeout, TLS handshake, format-unavailable, etc. - is
        # returned as a plain classified-later string. The parent
        # (routes.py / youtube_chain.py) runs the SAME is_permanent_
        # error() / is_bot_check_error() / etc. classification chain on
        # result["error"] that it always did on str(e) - nothing about
        # that logic changes, only where the exception was caught.
        result = {"ok": False, "kind": "error", "error": str(e)}

    with open(output_path, "w") as f:
        json.dump(result, f)


if __name__ == "__main__":
    main()