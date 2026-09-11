#!/bin/bash
# Zero-downtime restart after a .env change: starts the current image on
# the idle blue-green slot with the new .env, health-checks it, switches
# nginx, then drains the old container in the background. Same steps as
# deploy.yml minus the build. Lives outside ~/app because every deploy
# runs `git reset --hard` there.
# Usage: restart-api.sh [--force]   (--force drops jobs still running on
# a container left over from the previous switch)
set -euo pipefail
cd /home/deploy/app

ACTIVE=$(grep -o 'af_slot_[ab]' /etc/nginx/conf.d/audioforges-active.conf | head -1 | cut -d_ -f3)
ACTIVE=${ACTIVE:-a}
if [ "$ACTIVE" = a ]; then ACTIVE_PORT=8000; NEW=b; NEW_PORT=8002; else ACTIVE_PORT=8002; NEW=a; NEW_PORT=8000; fi

drain_count() {
  curl -s -m 5 "http://127.0.0.1:$1/internal/drain-status" \
    | python3 -c 'import json,sys; print(int(json.load(sys.stdin).get("processing", 0)))' 2>/dev/null || echo 0
}

switch_slot() {
  if [ "$(id -u)" = 0 ]; then /usr/local/sbin/af-switch "$1"; else sudo -n /usr/local/sbin/af-switch "$1"; fi
}

if docker inspect audioforges-api-old >/dev/null 2>&1; then
  n=$(drain_count "$NEW_PORT")
  if [ "$n" -gt 0 ] && [ "${1:-}" != "--force" ]; then
    echo "The previous slot is still finishing $n job(s). Wait a few minutes, or rerun with --force to drop them."
    exit 1
  fi
  docker rm -f audioforges-api-old >/dev/null
fi
docker rm -f audioforges-api-next >/dev/null 2>&1 || true

docker run -d --name audioforges-api-next --restart unless-stopped \
  --network audioforges-net --env-file .env -e PORT=8000 -e INSTANCE_SLOT="$NEW" \
  -v /home/deploy/app/data:/app/data -p 127.0.0.1:$NEW_PORT:8000 \
  audioforges-api:latest >/dev/null
echo "Started slot $NEW on port $NEW_PORT, waiting for health..."

ok=""
for i in $(seq 1 18); do
  sleep 5
  if curl -sf "http://127.0.0.1:$NEW_PORT/health" >/dev/null; then ok=1; break; fi
done
if [ -z "$ok" ]; then
  docker logs audioforges-api-next --tail 40 || true
  docker rm -f audioforges-api-next >/dev/null
  echo "New container never became healthy - removed it. Users stayed on slot $ACTIVE."
  exit 1
fi

if ! switch_slot "$NEW"; then
  docker rm -f audioforges-api-next >/dev/null
  echo "nginx switch failed - removed the new container. Users stayed on slot $ACTIVE."
  exit 1
fi

OLD_ID=$(docker inspect -f '{{.Id}}' audioforges-api 2>/dev/null || true)
if [ -n "$OLD_ID" ]; then
  docker rename audioforges-api audioforges-api-old
fi
docker rename audioforges-api-next audioforges-api
if [ -n "$OLD_ID" ]; then
  DRAIN=/tmp/af-drain-${OLD_ID:0:12}.sh
  cp /home/deploy/app/deploy/drain_old.sh "$DRAIN"
  setsid nohup bash "$DRAIN" "$OLD_ID" "$ACTIVE_PORT" >> /home/deploy/app/data/drain.log 2>&1 < /dev/null &
fi

KEY=$(grep -m1 '^ADMIN_STATUS_KEY=' .env | cut -d= -f2- | tr -d "\"'" || true)
curl -s -m 10 "http://127.0.0.1:$NEW_PORT/admin/status?key=$KEY" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('slot: $NEW | midi:', d['midi_worker'].get('reachable'), '| split:', d['split_tunnel']['state'])
" 2>/dev/null || echo "slot: $NEW (status summary unavailable)"