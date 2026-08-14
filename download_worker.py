"""
download_worker.py - Subprocess entrypoint for a single YouTube download
attempt. Spawned by utils.run_in_killable_subprocess() as its own OS
process (own process group, via start_new_session=True), so a wall-clock
timeout in the parent can SIGKILL this ENTIRE process tree - this script
plus whatever yt-dlp spawns under it (the Node PO-token generator, a
possible Deno JS-challenge solver, ffmpeg for postprocessing) - instead
of only abandoning an asyncio await while those children keep running
unsupervised.

WHY THIS EXISTS: confirmed in production 2026-08-14 - a wall-clock
timeout logged at 4:51:04 didn't stop the underlying yt-dlp call, which
kept running and logged its own failure at 4:55:26, over 4 minutes
later, on a thread pool with no way to force-kill a thread.

BREAKER STATE (added 2026-08-14): this process imports youtube.py fresh
every run, so every circuit breaker and counter in that module starts
empty here and dies on exit. import_breaker_state() adopts the parent's
breakers on the way in; drain_events() reports back what tripped on the
way out, and the parent replays them into its own long-lived state. See
youtube.py's "CROSS-PROCESS BREAKER STATE" section for the full writeup.

USAGE (invoked by utils.run_in_killable_subprocess, not by hand):
    python download_worker.py <input_json_path> <output_json_path>

stdout/stderr are INHERITED from the parent, deliberately - that is what
puts this process's [COOKIES]/[PROXY]/[CDN] lines and yt-dlp's verbose
output into the same container log stream as everything else. Piping
them silently discarded every log line a download produced.
"""
import sys
import json

from youtube import (
    download_with_fallback,
    ytdlp_alert_logger,
    VideoTooLongError,
    enable_event_recording,
    import_breaker_state,
    drain_events,
)
from download_progress import make_progress_hook


def main():
    if len(sys.argv) != 3:
        print("Usage: python download_worker.py <in.json> <out.json>", file=sys.stderr)
        sys.exit(1)

    input_path, output_path = sys.argv[1], sys.argv[2]

    with open(input_path) as f:
        payload = json.load(f)

    ydl_opts = payload["ydl_opts"]
    url = payload["url"]
    proxy_url = payload.get("proxy_url")

    # Order matters: recording on BEFORE any work, so nothing that trips
    # during import_breaker_state or the download itself is missed.
    enable_event_recording()
    import_breaker_state(payload.get("breaker_state"))

    # Reconstructed here, not passed in - a live logger object cannot
    # cross a JSON boundary. Importing the shared instance from
    # youtube.py gives this process the identical cookie-expiry
    # detection the in-process version had.
    ydl_opts["logger"] = ytdlp_alert_logger

# Progress hook likewise reconstructed rather than serialized. Only
    # /download sends progress_label (the chained /youtube/* tools poll
    # job status instead and never had one), so this is skipped for them.
    # request_id is threaded through so progress rows written from this
    # subprocess (see download_progress.py's write_system_log_direct call)
    # group under the same request in the dashboard as everything else
    # that request logged. tool/tier are hardcoded here since progress_label
    # is only ever sent by /download, which always tags itself this way.
    progress_label = payload.get("progress_label")
    if progress_label:
        ydl_opts["progress_hooks"] = [make_progress_hook(
            progress_label,
            request_id=payload.get("request_id", "-"),
            tool="DOWNLOAD",
            tier="standard",
        )]

    try:
        info = download_with_fallback(ydl_opts, url, proxy_url)
        result = {"ok": True, "title": info.get("title", "Unknown")}
    except VideoTooLongError as e:
        result = {"ok": False, "kind": "too_long", "error": str(e)}
    except Exception as e:
        # Everything else - permanent, geo, bot-check, CDN timeout, TLS,
        # format-unavailable - comes back as a plain string. The parent
        # runs the SAME is_permanent_error()/is_bot_check_error()/etc.
        # chain on it that it always ran on str(e).
        result = {"ok": False, "kind": "error", "error": str(e)}

    # Drained LAST, outside the try, so events are returned even when the
    # download failed - a failure is exactly when breakers matter most.
    result["events"] = drain_events()

    with open(output_path, "w") as f:
        json.dump(result, f)


if __name__ == "__main__":
    main()