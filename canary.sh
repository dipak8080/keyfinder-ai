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
set -uo pipefail

VIDEO="https://www.youtube.com/shorts/EzbugeXQMeY"
CLIENTS=(tv_simply web_embedded android_vr)
STATE="/home/deploy/app/data/canary_state.json"
POT="/root/bgutil-ytdlp-pot-provider/server/build/generate_once.js"
HOOK=$(grep -m1 '^ALERT_WEBHOOK_URL=' /home/deploy/app/.env | cut -d= -f2-)

results=""; failed=""; passed=""
for c in "${CLIENTS[@]}"; do
  if timeout 90 docker exec audioforges-api yt-dlp \
      -f bestaudio/best -o "/tmp/canary_$c.%(ext)s" --no-progress \
      --extractor-args "youtube:player_client=$c" \
      --extractor-args "youtubepot-bgutilscript:script_path=$POT" \
      "$VIDEO" >/dev/null 2>&1; then
    passed="$passed $c"; results="${results}${c}=OK "
  else
    failed="$failed $c"; results="${results}${c}=FAIL "
  fi
  docker exec audioforges-api rm -f "/tmp/canary_$c.webm" "/tmp/canary_$c.m4a" 2>/dev/null
done

current=$(echo "$results" | tr -d ' ')
previous=$(cat "$STATE" 2>/dev/null || echo "")
echo "$current" > "$STATE"

# Only alert on CHANGE. Alerting every run would train you to ignore it,
# which is the failure mode that makes monitoring useless.
[ "$current" = "$previous" ] && exit 0

if [ -n "$failed" ] && [ -z "$passed" ]; then
  msg="[CANARY] ALL extraction clients FAILED ($results) - downloads are down. Check yt-dlp/YouTube changes."
elif [ -n "$failed" ]; then
  msg="[CANARY] Client change:$results Working:${passed}. Put a working client FIRST in PLAYER_CLIENTS_NO_COOKIES in youtube.py."
else
  msg="[CANARY] Recovered - all clients healthy again ($results)."
fi

logger -t audioforges-canary "$msg"
[ -n "$HOOK" ] && curl -s -m 10 -H 'Content-Type: application/json' \
  -d "$(printf '{"content":%s}' "$(printf '%s' "$msg" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')")" \
  "$HOOK" >/dev/null
