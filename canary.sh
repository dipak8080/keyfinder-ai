#!/usr/bin/env bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
# AudioForges extraction canary.
#
# WHY THIS BYPASSES THE API: hitting /download would populate the cache on
# run 1 and return a cache HIT forever after, so the canary would report
# green while extraction was completely broken. It calls yt-dlp directly
# so it tests the only layer that actually breaks.
#
# WHY IT REALLY DOWNLOADS: on 2026-08-18 --skip-download reported every
# client healthy while real downloads 403'd on the media fetch. A canary
# that doesn't pull bytes lies.
#
# WHY FAILURES ARE LOGGED TO DISK (added 2026-08-30): on 2026-08-29 this
# fired ALL-clients-FAILED three separate times (12:45, 16:15, 23:15 UTC)
# and every window self-recovered within one or two cron cycles. There was
# NOTHING to diagnose from afterwards: `docker exec` output never enters
# the container's own log stream, and this script sent it to /dev/null, so
# three real outage alerts produced zero evidence. The failing client's
# last 30 lines now land in data/canary_failures.log - the bind mount, so
# it outlives the container the same way cookies and cache do.
set -uo pipefail

VIDEO="https://www.youtube.com/shorts/EzbugeXQMeY"
CLIENTS=(tv_simply web_embedded android_vr)
STATE="/home/deploy/app/data/canary_state.json"
FAILLOG="/home/deploy/app/data/canary_failures.log"
POT="/root/bgutil-ytdlp-pot-provider/server/build/generate_once.js"
HOOK=$(grep -m1 '^ALERT_WEBHOOK_URL=' /home/deploy/app/.env | cut -d= -f2-)

# Skip when the container is mid-deploy. deploy.yml removes the old
# container before starting the new one, and during that window every
# `docker exec` fails - which reads as ALL clients failing at once.
# Confirmed 2026-08-18: a curl-cffi deploy produced exactly that false
# alarm, followed by "Recovered" on the next run. A real YouTube change
# breaks clients individually, never all three simultaneously, so this
# guard costs nothing and removes the one alert that would train you to
# ignore the others.
docker exec audioforges-api true 2>/dev/null || {
  logger -t audioforges-canary "container not ready (deploy in progress?) - skipping run"
  exit 0
}

results=""; failed=""; passed=""
for c in "${CLIENTS[@]}"; do
  if timeout 90 docker exec audioforges-api yt-dlp \
      -f bestaudio/best -o "/tmp/canary_$c.%(ext)s" --no-progress \
      --extractor-args "youtube:player_client=$c" \
      --extractor-args "youtubepot-bgutilscript:script_path=$POT" \
      "$VIDEO" >"/tmp/canary_out_$c.txt" 2>&1; then
    passed="$passed $c"; results="${results}${c}=OK "
  else
    failed="$failed $c"; results="${results}${c}=FAIL "
    # Written on EVERY failed client, not only on alert-worthy state
    # changes. A client that fails on two consecutive runs produces one
    # alert and two log entries - and the second entry is what tells you
    # whether it is the same error twice or a different one.
    {
      date -u +"%Y-%m-%dT%H:%M:%SZ [$c] ---------------------------"
      tail -30 "/tmp/canary_out_$c.txt" 2>/dev/null || echo "(no output captured)"
    } >> "$FAILLOG" 2>/dev/null
  fi
  docker exec audioforges-api rm -f "/tmp/canary_$c.webm" "/tmp/canary_$c.m4a" 2>/dev/null
  rm -f "/tmp/canary_out_$c.txt" 2>/dev/null
done

# Keep the failure log bounded. Unbounded it would grow forever on a bad
# night and nothing else prunes it.
if [ -f "$FAILLOG" ] && [ "$(wc -l < "$FAILLOG" 2>/dev/null || echo 0)" -gt 2000 ]; then
  tail -1000 "$FAILLOG" > "$FAILLOG.tmp" && mv "$FAILLOG.tmp" "$FAILLOG"
fi

current=$(echo "$results" | tr -d ' ')
previous=$(cat "$STATE" 2>/dev/null || echo "")
echo "$current" > "$STATE"

# Only alert on CHANGE. Alerting every run would train you to ignore it,
# which is the failure mode that makes monitoring useless.
[ "$current" = "$previous" ] && exit 0

if [ -n "$failed" ] && [ -z "$passed" ]; then
  msg="[CANARY] ALL extraction clients FAILED ($results) - downloads are down. Check yt-dlp/YouTube changes, and confirm the container is running. Error output: tail -60 $FAILLOG"
elif [ -n "$failed" ]; then
  msg="[CANARY] Client change:$results Working:${passed}. Promote a working client to rung 0 of CLIENT_LADDER_NO_COOKIES in youtube.py. Error output: tail -60 $FAILLOG"
else
  msg="[CANARY] Recovered - all clients healthy again ($results)."
fi

logger -t audioforges-canary "$msg"
[ -n "$HOOK" ] && curl -s -m 10 -H 'Content-Type: application/json' \
  -d "$(printf '{"content":%s}' "$(printf '%s' "$msg" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')")" \
  "$HOOK" >/dev/null