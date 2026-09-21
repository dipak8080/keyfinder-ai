"""Real GPU spend from RunPod's billing API, split across tools by run time.

The day totals are RunPod's invoice. Only the split between tools that
share an endpoint is proportional to logged gpu_seconds. Spend on a day
with no logged jobs for that endpoint lands on IDLE_TOOL.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone

import requests

log = logging.getLogger("credits.runpod_billing")

API_URL = "https://api.runpod.io/v2/billing/serverless"
CACHE_SECONDS = 600
CHUNK_DAYS = 30
IDLE_TOOL = "idle-or-unlogged"

_ENDPOINT_ENV = {
    "RUNPOD_DEMUCS_ENDPOINT_ID": "separation",
    "RUNPOD_WHISPER_ENDPOINT_ID": "transcription",
    "RUNPOD_MT3_ENDPOINT_ID": "mt3",
    "RUNPOD_PIANO_ENDPOINT_ID": "piano",
}

_cache: dict[tuple[str, str], tuple[float, dict]] = {}
_lock = threading.Lock()


def _groups_by_endpoint() -> dict[str, str]:
    out = {}
    for env, group in _ENDPOINT_ENV.items():
        eid = os.environ.get(env, "").strip()
        if eid:
            out[eid] = group
    return out


def serves(group: str, tool: str) -> bool:
    if group == "separation":
        return tool.startswith(("separate", "stems", "youtube/")) or tool == "audio-to-midi-hq-mix"
    if group == "transcription":
        return tool == "transcribe"
    if group in ("mt3", "piano"):
        return tool.startswith("audio-to-midi-hq") or tool == "audio-to-sheet"
    return False


def _fetch_window(key: str, start: date, end_exclusive: date) -> list[dict]:
    resp = requests.get(
        API_URL,
        params={
            "bucketSize": "day",
            "grouping": "endpointId",
            "startTime": f"{start.isoformat()}T00:00:00Z",
            "endTime": f"{end_exclusive.isoformat()}T00:00:00Z",
        },
        headers={"Authorization": f"Bearer {key}"},
        timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    records = body.get("records") or []
    expected = (body.get("metadata") or {}).get("recordCount")
    if isinstance(expected, int) and expected > len(records):
        log.warning("runpod billing returned %s of %s records", len(records), expected)
    return records


def daily_spend(date_from: str, date_to: str) -> dict:
    """{"available", "daily": {day: {group: usd}}, "total_usd", "by_group", "error"}."""
    ck = (date_from, date_to)
    now = time.monotonic()
    with _lock:
        hit = _cache.get(ck)
        if hit and now - hit[0] < CACHE_SECONDS:
            return hit[1]

    key = os.environ.get("RUNPOD_API_KEY", "").strip()
    if not key:
        return {"available": False, "error": "RUNPOD_API_KEY not set", "daily": {}, "total_usd": 0.0, "by_group": {}}

    groups = _groups_by_endpoint()
    daily: dict[str, dict[str, float]] = {}
    by_group: dict[str, float] = {}
    try:
        start = date.fromisoformat(date_from)
        end_exclusive = date.fromisoformat(date_to) + timedelta(days=1)
        cursor = start
        while cursor < end_exclusive:
            stop = min(cursor + timedelta(days=CHUNK_DAYS), end_exclusive)
            for rec in _fetch_window(key, cursor, stop):
                day = str(rec.get("startTime", ""))[:10]
                if not day:
                    continue
                sid = rec.get("serverlessId") or "unknown"
                group = groups.get(sid, f"endpoint:{sid}")
                usd = float(rec.get("totalAmount") or 0)
                daily.setdefault(day, {})
                daily[day][group] = daily[day].get(group, 0.0) + usd
                by_group[group] = by_group.get(group, 0.0) + usd
            cursor = stop
    except Exception as exc:  # noqa: BLE001
        log.warning("runpod billing fetch failed: %s", exc)
        return {"available": False, "error": str(exc)[:200], "daily": {}, "total_usd": 0.0, "by_group": {}}

    out = {
        "available": True,
        "error": None,
        "daily": daily,
        "total_usd": round(sum(by_group.values()), 4),
        "by_group": {g: round(v, 4) for g, v in sorted(by_group.items(), key=lambda kv: -kv[1])},
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with _lock:
        _cache[ck] = (now, out)
    return out


def _idle_row(day: str) -> dict:
    return {
        "day": day, "tool": IDLE_TOOL, "jobs": 0, "completed": 0, "failed": 0,
        "rejected": 0, "input_minutes": 0, "gpu_seconds": 0, "est_cost_usd": 0.0,
        "paid_jobs": 0, "free_jobs": 0, "metered_est_cost_usd": 0,
    }


def apply_real_costs(rows: list[dict], billing: dict) -> list[dict]:
    """Replace est_cost_usd with RunPod's real spend. Keeps the old value as metered_est_cost_usd."""
    if not billing.get("available"):
        return rows

    rows = [dict(r) for r in rows]
    for r in rows:
        r["metered_est_cost_usd"] = r.get("est_cost_usd") or 0
        r["est_cost_usd"] = 0.0

    by_day: dict[str, list[dict]] = {}
    for r in rows:
        by_day.setdefault(r["day"], []).append(r)

    idle: dict[str, dict] = {}
    for day, spend in billing["daily"].items():
        day_rows = by_day.get(day, [])
        for group, usd in spend.items():
            if usd <= 0:
                continue
            cands = [r for r in day_rows if serves(group, r["tool"])]
            weights = [float(r.get("gpu_seconds") or 0) for r in cands]
            if not cands:
                row = idle.setdefault(day, _idle_row(day))
                row["est_cost_usd"] += usd
                continue
            if sum(weights) <= 0:
                weights = [float(r.get("jobs") or 0) or 1.0 for r in cands]
            total_w = sum(weights)
            for r, w in zip(cands, weights):
                r["est_cost_usd"] += usd * w / total_w

    rows.extend(idle.values())
    for r in rows:
        r["est_cost_usd"] = round(r["est_cost_usd"], 4)
    rows.sort(key=lambda r: r["tool"])
    rows.sort(key=lambda r: r["day"], reverse=True)
    return rows


def real_totals(usage: dict, days: int) -> dict:
    """Overwrite a metering.totals() dict with the real spend for the same window."""
    today = datetime.now(timezone.utc).date()
    billing = daily_spend((today - timedelta(days=days - 1)).isoformat(), today.isoformat())
    usage = dict(usage)
    usage["runpod"] = {k: billing.get(k) for k in ("available", "error", "total_usd", "by_group", "fetched_at")}
    if not billing.get("available"):
        return usage
    usage["metered_est_cost_usd"] = usage.get("est_cost_usd")
    cost = billing["total_usd"]
    usage["est_cost_usd"] = cost
    jobs = usage.get("jobs") or 0
    if jobs:
        usage["cost_per_job_usd"] = round(cost / jobs, 4)
    paid = usage.get("paid_jobs") or 0
    if paid:
        usage["est_cost_per_paid_job_usd"] = round(cost / paid, 4)
    return usage