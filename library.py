"""Library: Studio results kept for signed-in accounts.

A finished Studio job owned by an account is re-encoded to lossless FLAC
and stored in R2 under <account_id>/<job_id>/<stem>.flac. Downloads and
playback go straight to R2 through short-lived signed URLs, so library
traffic never touches the VPS. Items expire after LIBRARY_RETENTION_DAYS.
Each account keeps at most LIBRARY_MAX_ITEMS_PER_ACCOUNT items, dropping its
own oldest first. The global LIBRARY_MAX_TOTAL_GB cap only ever evicts the
saving account's own items; if that is not enough, the new item is not
saved and the owner is alerted."""

import json
import os
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone

from config import logger, FFMPEG_PATH
from credits.config import get_settings
from credits.db import connect, now_iso, tx

_client = None


def _env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def configured() -> bool:
    return all(_env(n) for n in ("R2_ACCOUNT_ID", "R2_LIBRARY_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"))


def enabled() -> bool:
    return get_settings().library_enabled and configured()


def bucket() -> str:
    return _env("R2_LIBRARY_BUCKET")


def client():
    global _client
    if _client is None:
        import boto3
        from botocore.config import Config

        _client = boto3.client(
            "s3",
            endpoint_url=_env("R2_ENDPOINT") or f"https://{_env('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
            aws_access_key_id=_env("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=_env("R2_SECRET_ACCESS_KEY"),
            region_name="auto",
            config=Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"}),
        )
    return _client


def _to_flac(wav_path: str, dest: str) -> None:
    result = subprocess.run(
        [FFMPEG_PATH, "-y", "-v", "error", "-i", wav_path, "-c:a", "flac", "-compression_level", "5", dest],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0 or not os.path.exists(dest):
        raise RuntimeError(f"flac encode failed: {result.stderr[-300:]}")


def _delete_prefix(prefix: str) -> None:
    s3 = client()
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket(), Prefix=prefix).get("Contents", [])]
    if keys:
        s3.delete_objects(Bucket=bucket(), Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True})


def _alert(message: str) -> None:
    try:
        from monitoring import alert_now
        alert_now(message)
    except Exception:  # noqa: BLE001
        logger.error(message)


def _make_room(account_id: str, incoming: int) -> bool:
    """Frees space for one new item using only this account's own items.
    Returns False if the global cap still cannot fit it."""
    s = get_settings()
    per_account = s.library_max_items_per_account
    limit = int(s.library_max_total_gb * 1024 ** 3)
    with connect() as conn:
        own = conn.execute("SELECT job_id, size_bytes FROM library_items WHERE account_id=? ORDER BY created_at",
                           (account_id,)).fetchall()
        total = conn.execute("SELECT COALESCE(SUM(size_bytes),0) AS t FROM library_items").fetchone()["t"]
    own = list(own)
    while own and (len(own) >= per_account or total + incoming > limit):
        oldest = own.pop(0)
        remove(oldest["job_id"], account_id)
        total -= oldest["size_bytes"]
        logger.info(f"[LIBRARY] evicted {oldest['job_id']} from {account_id} to make room")
    if total + incoming > limit:
        _alert(f"Library is full ({s.library_max_total_gb} GB): a new item for {account_id} was not saved. "
               f"Raise LIBRARY_MAX_TOTAL_GB or shorten retention.")
        return False
    return True


def archive(job_id: str, job: dict) -> bool:
    """Blocking. Copies a finished Studio job into the owner's library."""
    account_id = job.get("library_account")
    if not account_id or not enabled():
        return False
    stems = dict(job.get("stems") or {})
    if job.get("vocals_path"):
        stems["vocals"] = job["vocals_path"]
        stems["instrumental"] = job.get("instrumental_path")
    missing = sorted(k for k, v in stems.items() if not v or not os.path.exists(v))
    if missing or not stems:
        logger.warning(f"[LIBRARY] job={job_id} not saved: stems missing {missing or 'all'}")
        return False

    s3 = client()
    stored, total = {}, 0
    with tempfile.TemporaryDirectory() as tmp:
        encoded = {}
        for stem, path in stems.items():
            dest = os.path.join(tmp, f"{stem}.flac")
            _to_flac(path, dest)
            encoded[stem] = dest
            total += os.path.getsize(dest)
        if not _make_room(account_id, total):
            return False
        title = os.path.splitext(os.path.basename(job.get("title") or job_id))[0][:120]
        for stem, dest in encoded.items():
            key = f"{account_id}/{job_id}/{stem}.flac"
            s3.upload_file(dest, bucket(), key, ExtraArgs={"ContentType": "audio/flac"})
            stored[stem] = {"key": key, "size": os.path.getsize(dest)}

    days = get_settings().library_retention_days
    expires = (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    kind = "stems" if job.get("job_type") in ("stems", "youtube_stems") else "vocal_remover"
    with connect() as conn, tx(conn):
        conn.execute(
            """INSERT OR REPLACE INTO library_items (job_id, account_id, title, kind, stems, analysis,
                   size_bytes, created_at, expires_at) VALUES (?,?,?,?,?,?,?,?,?)""",
            (job_id, account_id, title, kind, json.dumps(stored),
             json.dumps(job.get("dj_analysis")) if job.get("dj_analysis") else None,
             total, now_iso(), expires),
        )
    logger.info(f"[LIBRARY] job={job_id} saved for {account_id}: {len(stored)} stems, {total / 1024 ** 2:.1f} MB")
    return True


def items_for(account_id: str) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            """SELECT job_id, title, kind, stems, analysis, size_bytes, created_at, expires_at
               FROM library_items WHERE account_id=? AND expires_at > strftime('%Y-%m-%dT%H:%M:%SZ','now')
               ORDER BY created_at DESC LIMIT 200""", (account_id,)).fetchall()
    return [{
        "job_id": r["job_id"], "title": r["title"], "kind": r["kind"],
        "stems": sorted(json.loads(r["stems"]).keys()),
        "analysis": json.loads(r["analysis"]) if r["analysis"] else None,
        "size_mb": round(r["size_bytes"] / 1024 ** 2, 1),
        "created_at": r["created_at"], "expires_at": r["expires_at"],
    } for r in rows]


def item(account_id: str, job_id: str):
    with connect() as conn:
        row = conn.execute("SELECT * FROM library_items WHERE job_id=? AND account_id=?",
                           (job_id, account_id)).fetchone()
    return dict(row) if row else None


def signed_url(row: dict, stem: str, download: bool, ttl: int = 3600) -> str | None:
    stems = json.loads(row["stems"])
    if stem not in stems:
        return None
    params = {"Bucket": bucket(), "Key": stems[stem]["key"]}
    if download:
        params["ResponseContentDisposition"] = content_disposition(f"{row['title'] or 'track'} - {stem}.flac")
    return client().generate_presigned_url("get_object", Params=params, ExpiresIn=ttl)


def content_disposition(filename: str) -> str:
    """ASCII fallback plus RFC 5987 filename* so non-Latin titles survive."""
    from urllib.parse import quote
    cleaned = "".join(ch for ch in filename if ch.isprintable() and ch not in '"\\/').strip() or "track.flac"
    ascii_name = "".join(ch for ch in cleaned if ch.isascii() and (ch.isalnum() or ch in " -_().")).strip()
    if not ascii_name or ascii_name.startswith("."):
        ascii_name = "track" + (ascii_name if ascii_name.startswith(".") else ".flac")
    return f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{quote(cleaned, safe="")}'


def remove(job_id: str, account_id: str) -> bool:
    _delete_prefix(f"{account_id}/{job_id}/")
    with connect() as conn, tx(conn):
        return conn.execute("DELETE FROM library_items WHERE job_id=? AND account_id=?",
                            (job_id, account_id)).rowcount > 0


def sweep_expired() -> int:
    if not configured():
        return 0
    with connect() as conn:
        rows = conn.execute("""SELECT job_id, account_id FROM library_items
                               WHERE expires_at <= strftime('%Y-%m-%dT%H:%M:%SZ','now') LIMIT 500""").fetchall()
    removed = 0
    for r in rows:
        try:
            removed += remove(r["job_id"], r["account_id"])
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[LIBRARY] could not remove expired {r['job_id']}: {e}")
    if removed:
        logger.info(f"[LIBRARY] removed {removed} expired items")
    return removed


def mark_owner(job_id: str, identity) -> None:
    """Flags a Studio job so its result is saved to the account's library."""
    if identity is not None and getattr(identity, "account_id", None) and enabled():
        from jobs import set_job_fields
        set_job_fields(job_id, library_account=identity.account_id)