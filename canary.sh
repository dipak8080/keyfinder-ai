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
#
# COOKIE LEG + ACCOUNTS LEG (2026-09-11): ~97% of downloads run on the
# cookie ladder, which the canary never tested. Every run: web_embedded and
# mweb with the primary cookie, direct (free). mweb uses a video with
# embedding disabled, the exact case rung 2 exists for. Hourly: each cookie
# slot on its own, so a rotated or challenged account is reported before
# anyone needs it. Proxy leg drops to every 6 h while the cookie path is OK.
set -uo pipefail

VIDEO="https://www.youtube.com/shorts/EzbugeXQMeY"
NE_VIDEO="https://www.youtube.com/watch?v=M5YZm8chnrs"
DIRECT_CLIENTS=(tv_simply web_embedded visionos)
COOKIE_CHECKS=(web_embedded mweb)
PROXY_CHECKS=(web_embedded:ck tv_simply:)
PROXY_EVERY_MIN=120
PROXY_EVERY_MIN_HEALTHY=360
ACCOUNTS_EVERY_MIN=60
STATE="/home/deploy/app/data/canary_state.json"
PROXY_STATE="/home/deploy/app/data/canary_proxy_state"
ACCOUNTS_STATE="/home/deploy/app/data/canary_accounts_state"
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

# check <client> <use_proxy 0|1> <cookie slot 0|1|2|3> [video]
#   -> OK | BOT | ROTATED | MISSING | FAIL
check() {
  local c=$1 px=$2 ck=$3 v=${4:-$VIDEO} tag="$1_$2$3" out rc
  out=$(timeout 120 docker exec -e C="$c" -e PX="$px" -e CK="$ck" -e V="$v" \
      -e POT="$POT" -e T="$tag" audioforges-api sh -c '
    P=""; [ "$PX" = 1 ] && P="--proxy $YT_PROXY_URL"
    if [ "$CK" = 0 ]; then F=""
    elif [ "$CK" = 1 ]; then F="$YT_COOKIES_PATH"
    else
      # Slot N: honour COOKIE_ACCOUNT_N_PATH, else sit next to slot 1.
      eval "F=\${COOKIE_ACCOUNT_${CK}_PATH:-}"
      [ -n "$F" ] || F="$(dirname "$YT_COOKIES_PATH")/cookies_$CK.txt"
    fi
    K=""
    if [ -n "$F" ]; then
      [ -s "$F" ] || { echo CANARY_NOFILE; exit 3; }
      cp "$F" "/tmp/canary_ck_$T.txt" && K="--cookies /tmp/canary_ck_$T.txt"
    fi
    yt-dlp $P $K -f bestaudio/best -o "/tmp/canary_$T.%(ext)s" --force-overwrites --no-progress \
      --extractor-args "youtube:player_client=$C" \
      --extractor-args "youtubepot-bgutilscript:script_path=$POT" "$V"
    rc=$?; rm -f /tmp/canary_$T.* "/tmp/canary_ck_$T.txt"; exit $rc' 2>&1)
  rc=$?
  if printf '%s' "$out" | grep -q CANARY_NOFILE; then
    echo MISSING; return
  fi
  if [ "$ck" != 0 ] && printf '%s' "$out" | grep -qi "no longer valid"; then
    r=ROTATED
  elif [ $rc -eq 0 ]; then
    echo OK; return
  elif printf '%s' "$out" | grep -qi "not a bot"; then
    echo BOT; return
  else
    r=FAIL
  fi
  {
    date -u +"%Y-%m-%dT%H:%M:%SZ [$tag] $r ---------------------------"
    printf '%s\n' "$out" | tail -30 | sed -E 's#://[^@/ ]+@#://***@#g'
  } >> "$FAILLOG" 2>/dev/null
  echo $r
}

send() {
  logger -t audioforges-canary "$1"
  [ -n "$HOOK" ] && curl -s -m 10 -H 'Content-Type: application/json' \
    -d "$(printf '{"content":%s}' "$(printf '%s' "$1" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')")" \
    "$HOOK" >/dev/null
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

cookie=""; c_ok=0
for c in "${COOKIE_CHECKS[@]}"; do
  v=$VIDEO; [ "$c" = mweb ] && v=$NE_VIDEO
  r=$(check "$c" 0 1 "$v")
  cookie="${cookie}${c}=${r},"
  [ "$r" = OK ] && c_ok=$((c_ok+1))
done
cookie_state="cookie:${cookie%,}"

proxy_every=$PROXY_EVERY_MIN
[ "$c_ok" -eq "${#COOKIE_CHECKS[@]}" ] && proxy_every=$PROXY_EVERY_MIN_HEALTHY

if [ "$d_ok" -eq "${#DIRECT_CLIENTS[@]}" ]; then
  proxy_state="proxy:idle"
elif [ -s "$PROXY_STATE" ] && [ -n "$(find "$PROXY_STATE" -mmin -"$proxy_every" 2>/dev/null)" ]; then
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

if [ ! -s "$ACCOUNTS_STATE" ] || [ -z "$(find "$ACCOUNTS_STATE" -mmin -"$ACCOUNTS_EVERY_MIN" 2>/dev/null)" ]; then
  # Every configured slot, skipping empty ones: a slot exists only once
  # its file is uploaded, so unused slots never report MISSING.
  SLOTS=$(docker exec audioforges-api sh -c \
    'python3 -c "from config import cookie_slot_paths; print(\" \".join(str(n) for n in sorted(cookie_slot_paths())))"' 2>/dev/null)
  [ -n "$SLOTS" ] || SLOTS="1 2 3"
  acc=""
  for n in $SLOTS; do
    r=$(check web_embedded 0 "$n")
    [ "$r" = MISSING ] && continue
    label="Primary"; [ "$n" != 1 ] && label="Backup$((n - 1))"
    acc="$acc$label=$r "
  done
  acc=${acc% }
  acc_prev=$(cat "$ACCOUNTS_STATE" 2>/dev/null || echo "")
  echo "$acc" > "$ACCOUNTS_STATE"
  if [ "$acc" != "$acc_prev" ]; then
    case "$acc" in
      *ROTATED*|*BOT*|*FAIL*)
        send "[CANARY] Cookie accounts: $acc. ROTATED: re-export that account (incognito, log in, open youtube.com/robots.txt in the same tab, export, close the window) and upload it to its slot. BOT: YouTube is challenging that account; rotation already tries it last. Details: tail -60 $FAILLOG" ;;
      *)
        send "[CANARY] Cookie accounts: $acc." ;;
    esac
  fi
fi

current="${direct_state} | ${cookie_state} | ${proxy_state}"
previous=$(cat "$STATE" 2>/dev/null || echo "")
echo "$current" > "$STATE"

# Only alert on CHANGE. Alerting every run trains you to ignore it.
[ "$current" = "$previous" ] && exit 0

p_fail=$(printf '%s' "$proxy_state" | grep -oE '=(FAIL|BOT)' | wc -l)
p_ok=$(printf '%s' "$proxy_state" | grep -o '=OK' | wc -l)

p_bot=$(printf '%s' "$proxy_state" | grep -o '=BOT' | wc -l)

if [ "$p_bot" -gt 0 ] && [ "$p_bot" -eq "$p_fail" ] && [ "$p_ok" -eq 0 ] && [ "$d_ok" -eq 0 ] && [ "$c_ok" -eq 0 ]; then
  msg="[CANARY] Downloads DOWN: VPS IP, cookie path and proxy exits are all bot-checked ($current). Usually clears as the provider rotates exits; if it lasts over an hour check the proxy bot-check breaker in /admin/status and re-export the primary cookie."
elif [ "$p_fail" -gt 0 ] && [ "$p_ok" -eq 0 ] && [ "$d_ok" -eq 0 ] && [ "$c_ok" -eq 0 ]; then
  msg="[CANARY] Downloads DOWN: no direct, cookie or proxy check works ($current). Check yt-dlp/bgutil/YouTube changes. Error output: tail -60 $FAILLOG"
elif [ "$c_ok" -lt "${#COOKIE_CHECKS[@]}" ]; then
  msg="[CANARY] Cookie path problem ($current). This path carries ~97% of downloads (CLIENT_LADDER_WITH_COOKIES in youtube.py). ROTATED or BOT on both = primary cookie, re-export it. FAIL on mweb only = client change, or test video M5YZm8chnrs changed. Rotation and the proxy cover users meanwhile. Error output: tail -60 $FAILLOG"
elif [ "$p_fail" -gt 0 ]; then
  msg="[CANARY] Paid-path client change ($current). web_embedded:ck is proxy rung 0, tv_simply rung 1 (CLIENT_LADDER_WITH_COOKIES in youtube.py). Error output: tail -60 $FAILLOG"
elif [ -n "$d_fail" ]; then
  msg="[CANARY] Free-path client change ($current). Failing:${d_fail}. See CLIENT_LADDER_NO_COOKIES in youtube.py. Error output: tail -60 $FAILLOG"
elif [ "$direct_state" = "direct=BOTCHECKED" ]; then
  msg="[CANARY] No-cookie path is bot-checked on the VPS IP. Downloads run on cookies for free; the proxy is only a fallback. No action needed ($current)."
else
  msg="[CANARY] Healthy ($current)."
fi

send "$msg"