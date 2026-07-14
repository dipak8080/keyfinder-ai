"""
cache.py - Optional R2 (Cloudflare, S3-compatible) caching layer for
downloaded audio files, keyed by video_id + format.

Fully optional and fails safe: if R2 credentials aren't configured, or
any R2 call errors for any reason, every function here returns None
(or does nothing on write) and logs a warning - the caller (routes.py)
always falls through to a normal fresh download in that case. Caching
is a pure latency/cost optimization for REPEAT video requests, never a
hard dependency - the app must keep working exactly as before even if
R2 is fully unreachable or never configured at all.
"""
import time
import json
from typing import Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from config import (
    logger,
    CACHE_ENABLED,
    R2_ACCOUNT_ID,
    R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY,
    R2_BUCKET_NAME,
    R2_ENDPOINT_URL,
    CACHE_MAX_AGE_SECONDS,
)

_client = None


def _is_configured() -> bool:
    return bool(
        CACHE_ENABLED
        and R2_ACCOUNT_ID
        and R2_ACCESS_KEY_ID
        and R2_SECRET_ACCESS_KEY
        and R2_BUCKET_NAME
    )


def _get_client():
    """
    Lazily creates ONE shared boto3 S3 client pointed at R2's
    S3-compatible endpoint. boto3 clients are thread-safe for concurrent
    use (this app calls into cache.py via run_blocking from multiple
    worker threads), so a single shared instance built once is correct
    and avoids reconnecting on every request.
    """
    global _client
    if _client is not None:
        return _client
    if not _is_configured():
        return None
    try:
        _client = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT_URL,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
            region_name="auto",  # R2 ignores region; "auto" is R2's documented value
        )
        logger.info(f"[CACHE] R2 client initialized (bucket: {R2_BUCKET_NAME})")
    except Exception as e:
        logger.warning(f"[CACHE] Failed to initialize R2 client (non-fatal, caching disabled): {e}")
        _client = None
    return _client


def _audio_key(video_id: str, fmt: str) -> str:
    return f"audio/{video_id}_{fmt}.bin"


def _meta_key(video_id: str, fmt: str) -> str:
    # Title is stored as a small separate JSON object rather than an S3
    # object-metadata header, since S3/R2 metadata headers must be
    # ASCII/latin-1 - many real video titles contain unicode (emoji,
    # non-Latin scripts) that would either break or get silently mangled
    # in a metadata header. A JSON body has no such restriction.
    return f"audio/{video_id}_{fmt}.json"


def get_cached_audio(video_id: str, fmt: str) -> Tuple[Optional[bytes], Optional[str]]:
    """
    Returns (audio_bytes, title) if a fresh cache entry exists, else
    (None, None). NEVER raises - not-configured, network errors, missing
    object, or a stale entry are all treated identically as "not cached",
    so the caller always has a clean, simple fallback path (just proceed
    with a normal download).
    """
    if not video_id:
        return None, None
    client = _get_client()
    if client is None:
        return None, None

    audio_key = _audio_key(video_id, fmt)
    try:
        response = client.get_object(Bucket=R2_BUCKET_NAME, Key=audio_key)
        last_modified = response["LastModified"].timestamp()
        age_seconds = time.time() - last_modified
        if age_seconds > CACHE_MAX_AGE_SECONDS:
            logger.info(f"[CACHE] MISS (stale, {int(age_seconds)}s old): {audio_key}")
            return None, None

        data = response["Body"].read()

        title = "Unknown"
        try:
            meta_response = client.get_object(Bucket=R2_BUCKET_NAME, Key=_meta_key(video_id, fmt))
            meta = json.loads(meta_response["Body"].read())
            title = meta.get("title", "Unknown")
        except Exception:
            # Missing/corrupt metadata is NOT worth treating as a full
            # cache miss - the audio itself is still perfectly valid and
            # far more expensive to regenerate than a title string.
            pass

        logger.info(f"[CACHE] HIT: {audio_key} ({len(data)} bytes, age {int(age_seconds)}s, title='{title}')")
        return data, title

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("NoSuchKey", "404"):
            logger.info(f"[CACHE] MISS: {audio_key}")
        else:
            logger.warning(f"[CACHE] Read error for {audio_key} (non-fatal, proceeding without cache): {e}")
        return None, None
    except Exception as e:
        logger.warning(f"[CACHE] Unexpected read error for {audio_key} (non-fatal): {e}")
        return None, None


def put_cached_audio(video_id: str, fmt: str, data: bytes, title: str):
    """
    Saves a successfully downloaded file (+ its title) to R2 for future
    requests. Any failure here is logged and swallowed, NEVER raised -
    a caching write failure must not fail the download that already
    succeeded and is about to be returned to the user.
    """
    if not video_id or not data:
        return
    client = _get_client()
    if client is None:
        return

    audio_key = _audio_key(video_id, fmt)
    try:
        content_type = "audio/mpeg" if fmt == "mp3" else "audio/wav"
        client.put_object(Bucket=R2_BUCKET_NAME, Key=audio_key, Body=data, ContentType=content_type)

        try:
            meta_body = json.dumps({"title": title or "Unknown"}).encode("utf-8")
            client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=_meta_key(video_id, fmt),
                Body=meta_body,
                ContentType="application/json",
            )
        except Exception as meta_err:
            # Audio itself already saved successfully - a failed title
            # save just means a future cache HIT shows "Unknown" instead
            # of the real title. Not worth failing the whole cache save
            # over.
            logger.warning(f"[CACHE] Saved audio but failed to save title metadata for {audio_key} (non-fatal): {meta_err}")

        logger.info(f"[CACHE] SAVED: {audio_key} ({len(data)} bytes, title='{title}')")
    except Exception as e:
        logger.warning(f"[CACHE] Failed to save {audio_key} (non-fatal, download still succeeded): {e}")