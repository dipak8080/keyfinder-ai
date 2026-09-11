#!/usr/bin/env bash
# Stops the previous blue-green slot's container once it has no work left.
# Started detached by deploy.yml: drain_old.sh <container id> <host port>
# Uses the container ID, not the name, so a later deploy renaming things
# can never make this stop the live container.
set -u
CID=$1
PORT=$2
MIN_SECONDS=200     # outlasts synchronous requests (download wall clock is 180s)
GRACE_SECONDS=900   # after the last job finishes, time for users to fetch results
CAP_SECONDS=2700

log() { echo "$(date -u +%FT%TZ) [drain ${CID:0:12}] $*"; }

start=$(date +%s)
idle_since=""
log "draining on port $PORT"
while docker inspect "$CID" >/dev/null 2>&1; do
  now=$(date +%s)
  elapsed=$((now - start))
  n=$(curl -s -m 5 "http://127.0.0.1:$PORT/internal/drain-status" \
      | python3 -c 'import json,sys; print(int(json.load(sys.stdin).get("processing", 0)))' 2>/dev/null || echo 0)
  if [ "$n" -eq 0 ]; then idle_since=${idle_since:-$now}; else idle_since=""; fi
  if [ "$elapsed" -ge "$CAP_SECONDS" ]; then
    log "cap reached with $n job(s) still running - stopping"
    break
  fi
  if [ "$elapsed" -ge "$MIN_SECONDS" ] && [ -n "$idle_since" ] && [ $((now - idle_since)) -ge "$GRACE_SECONDS" ]; then
    log "idle for $((now - idle_since))s - stopping"
    break
  fi
  sleep 15
done
docker stop -t 30 "$CID" >/dev/null 2>&1 || true
docker rm "$CID" >/dev/null 2>&1 || true
log "done"
rm -f "$0"