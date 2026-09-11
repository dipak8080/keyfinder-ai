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
# WHY FAILURES ARE LOGGED TO DISK (added 2026-08-30): `docker exec` output
# never enters the container's own log stream, so without this an alert
# leaves no evidence. Lands in data/ so it outlives the container.
#
# TWO LEGS (2026-09-11): the old canary only tested direct, and on
# 2026-09-11 the VPS IP was bot-checked for EVERY client, so it sat at
# all-FAIL and went silent while users were served by the proxy. Now:
#   direct leg - free, every run, the anon ladder's clients. A bot-check
#                on all of them is reported as the IP state, not a
#                client failure.
#   proxy leg  - paid, only while direct is not fully healthy, at most
#                once per PROXY_EVERY_MIN. Tests exactly what the paid
#                path walks: web_embedded+cookies (rung 0), tv_simply
#                (rung 1). ~1 GB/month worst case, zero when direct works.
set -uo pipefail

VIDEO="https://www.youtube.com/shorts/EzbugeXQMeY"
DIRECT_CLIENTS=(tv_simply web_embedded visionos)
PROXY_CHECKS=(web_embedded:ck tv_simply:)
PROXY_EVERY_MIN=120
STATE="/home/deploy/app/data/canary_state.json"
PROXY_STATE="/home/deploy/app/data/canary_proxy_state"
FAILLOG="/home/deploy/app/data/canary_failures.log"
POT="/root/bgutil-ytdlp-pot-provider/server/build/generate_once.js"
HOOK=$(grep -m1 '^ALERT_WEBHOOK_URL=' /home/deploy/app/.env | cut -d= -f2-)

exec 9>/tmp/audioforges-canary.lock
flock -n 9 || exit 0

# Skip when the container is mid-deploy: every `docker exec` fails in that
# window and reads as all clients failing at once (false alarm 2026-08-18).
docker exec audioforges-api true 2>/dev/null || {
  logger -t audioforges-canary "container not ready (deploy in progress?) - skipping run"
  exit 0
}

# check <client> <use_proxy 0|1> <use_cookies 0|1> -> prints OK | BOT | FAIL
check() {
  local c=$1 px=$2 ck=$3 tag="$1_$2$3" out
  out=$(timeout 120 docker exec -e C="$c" -e PX="$px" -e CK="$ck" -e V="$VIDEO" \
      -e POT="$POT" -e T="$tag" audioforges-api sh -c '
    P=""; [ "$PX" = 1 ] && P="--proxy $YT_PROXY_URL"
    K=""; if [ "$CK" = 1 ]; then cp "$YT_COOKIES_PATH" "/tmp/canary_ck_$T.txt" && K="--cookies /tmp/canary_ck_$T.txt"; fi
    yt-dlp $P $K -f bestaudio/best -o "/tmp/canary_$T.%(ext)s" --force-overwrites --no-progress \
      --extractor-args "youtube:player_client=$C" \
      --extractor-args "youtubepot-bgutilscript:script_path=$POT" "$V"
    rc=$?; rm -f /tmp/canary_$T.* "/tmp/canary_ck_$T.txt"; exit $rc' 2>&1)
  if [ $? -eq 0 ]; then
    echo OK
  elif printf '%s' "$out" | grep -qi "not a bot"; then
    echo BOT
  else
    {
      date -u +"%Y-%m-%dT%H:%M:%SZ [$tag] ---------------------------"
      printf '%s\n' "$out" | tail -30 | sed -E 's#://[^@/ ]+@#://***@#g'
    } >> "$FAILLOG" 2>/dev/null
    echo FAIL
  fi
}

direct=""; d_ok=0; d_bot=0; d_fail=""
for c in "${DIRECT_CLIENTS[@]}"; do
  r=$(check "$c" 0 0)
  direct="${direct}${c}=${r},"
  case $r in OK) d_ok=$((d_ok+1)) ;; BOT) d_bot=$((d_bot+1)) ;; *) d_fail="$d_fail $c" ;; esac
done
if [ "$d_bot" -eq "${#DIRECT_CLIENTS[@]}" ]; then
  direct_state="direct=BOTCHECKED"
else
  direct_state="direct:${direct%,}"
fi

if [ "$d_ok" -eq "${#DIRECT_CLIENTS[@]}" ]; then
  proxy_state="proxy:idle"
elif [ -s "$PROXY_STATE" ] && [ -n "$(find "$PROXY_STATE" -mmin -"$PROXY_EVERY_MIN" 2>/dev/null)" ]; then
  proxy_state=$(cat "$PROXY_STATE")
else
  p=""
  for pc in "${PROXY_CHECKS[@]}"; do
    c=${pc%%:*}; ck=0; [ "${pc##*:}" = ck ] && ck=1
    p="${p}${pc%:}=$(check "$c" 1 "$ck"),"
  done
  proxy_state="proxy:${p%,}"
  echo "$proxy_state" > "$PROXY_STATE"
fi

if [ -f "$FAILLOG" ] && [ "$(wc -l < "$FAILLOG" 2>/dev/null || echo 0)" -gt 2000 ]; then
  tail -1000 "$FAILLOG" > "$FAILLOG.tmp" && mv "$FAILLOG.tmp" "$FAILLOG"
fi

current="${direct_state} | ${proxy_state}"
previous=$(cat "$STATE" 2>/dev/null || echo "")
echo "$current" > "$STATE"

# Only alert on CHANGE. Alerting every run trains you to ignore it.
[ "$current" = "$previous" ] && exit 0

p_fail=$(printf '%s' "$proxy_state" | grep -oE '=(FAIL|BOT)' | wc -l)
p_ok=$(printf '%s' "$proxy_state" | grep -o '=OK' | wc -l)

p_bot=$(printf '%s' "$proxy_state" | grep -o '=BOT' | wc -l)

if [ "$p_bot" -gt 0 ] && [ "$p_bot" -eq "$p_fail" ] && [ "$p_ok" -eq 0 ] && [ "$d_ok" -eq 0 ]; then
  msg="[CANARY] Downloads DOWN: direct IP and proxy exits are both bot-checked ($current). Usually clears as the provider rotates exits; if it lasts over an hour check the proxy bot-check breaker in /admin/status."
elif [ "$p_fail" -gt 0 ] && [ "$p_ok" -eq 0 ] && [ "$d_ok" -eq 0 ]; then
  msg="[CANARY] Downloads DOWN: no direct client works and every proxy check failed ($current). Check yt-dlp/bgutil/YouTube changes. Error output: tail -60 $FAILLOG"
elif [ "$p_fail" -gt 0 ]; then
  msg="[CANARY] Paid-path client change ($current). web_embedded:ck is proxy rung 0, tv_simply rung 1 (CLIENT_LADDER_WITH_COOKIES in youtube.py). Error output: tail -60 $FAILLOG"
elif [ -n "$d_fail" ]; then
  msg="[CANARY] Free-path client change ($current). Failing:${d_fail}. See CLIENT_LADDER_NO_COOKIES in youtube.py. Error output: tail -60 $FAILLOG"
elif [ "$direct_state" = "direct=BOTCHECKED" ]; then
  msg="[CANARY] Direct IP is bot-checked: every download is paying for proxy extraction. Paid path healthy ($current). No action unless it lasts days."
else
  msg="[CANARY] Healthy ($current)."
fi

logger -t audioforges-canary "$msg"
[ -n "$HOOK" ] && curl -s -m 10 -H 'Content-Type: application/json' \
  -d "$(printf '{"content":%s}' "$(printf '%s' "$msg" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')")" \
  "$HOOK" >/dev/null