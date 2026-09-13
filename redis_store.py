"""
redis_store.py - The single Redis connection used by jobs.py and rate_limit.py.

Redis runs as a container on the audioforges-net Docker network, reachable
only from other containers on that network. No host port is published.

maxmemory-policy MUST be noeviction on the server. Any eviction policy would
silently drop job records under memory pressure, and a status poll for an
evicted job returns a phantom 404 while the work is still running.
"""
import os

import redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")

client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=5,
    socket_connect_timeout=5,
    retry_on_timeout=True,
    health_check_interval=30,
)


def ping() -> bool:
    try:
        return bool(client.ping())
    except Exception:
        return False