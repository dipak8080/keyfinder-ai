"""
cache.py - Local VPS-disk audio cache, replacing the R2 (Cloudflare)
cache entirely. No more per-GB cloud storage cost.

Same public interface and behavior guarantees as the R2 version this
replaces:
- get_cached_audio(video_id, fmt) -> (data, title) or (None, None),
  NEVER raises. Not-configured/error/missing/stale are all treated
  identically as "not cached" so the caller always falls through to a
  normal download.
- put_cached_audio(video_id, fmt, data, title) saves for future
  requests. NEVER raises - a caching write failure must not fail the
  download that already succeeded and is about to be returned.
- Same CACHE_MAX_AGE_SECONDS staleness check as before (an entry older
  than this is treated as a miss, same as R2 version's LastModified
  check), PLUS a new total-size cap with LRU eviction, since local disk
  is finite in a way R2 effectively wasn't.

Storage: actual audio bytes as plain files under CACHE_DIR. A small
SQLite table (same pattern already used for logs.db elsewhere in this
project) tracks metadata (video_id, format, title, size, timestamps) -
consistent with the rest of the codebase rather than a new paradigm.
Reuses the same persistent bind-mounted directory (/app/data on the VPS)
that already holds logs.db and survives container redeploys - no new
deploy.yml/volume change needed.

The cache size cap (CACHE_MAX_BYTES) can be overridden two ways:
- CACHE_MAX_GB / CACHE_MAX_BYTES env vars (checked once at startup)
- set_cache_max_gb() at runtime via the admin panel, which persists the
  override into the cache_settings table so it survives container
  restarts without needing to touch .env or redeploy.
"""
import os
import shutil
import sqlite3
import time
from contextlib import contextmanager
from typing import Optional, Tuple

from config import logger, CACHE_MAX_AGE_SECONDS

CACHE_DIR = os.environ.get("CACHE_DIR", "/app/data/cache")
CACHE_DB_PATH = os.environ.get("CACHE_DB_PATH", "/app/data/cache_meta.db")

# 25GB default. Prefer setting CACHE_MAX_GB (a plain number like 25) in
# your .env - easier than computing raw bytes by hand. CACHE_MAX_BYTES
# still works too if set, and takes priority if both happen to be set.
_DEFAULT_CACHE_MAX_GB = 25
CACHE_MAX_BYTES = int(os.environ.get(
    "CACHE_MAX_BYTES",
    int(os.environ.get("CACHE_MAX_GB", _DEFAULT_CACHE_MAX_GB)) * 1024 * 1024 * 1024,
))

os.makedirs(CACHE_DIR, exist_ok=True)


def _init_db():
    with sqlite3.connect(CACHE_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache_entries (
                video_id TEXT NOT NULL,
                format TEXT NOT NULL,
                title TEXT,
                file_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                created_at REAL NOT NULL,
                last_accessed_at REAL NOT NULL,
                PRIMARY KEY (video_id, format)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_last_accessed ON cache_entries(last_accessed_at)")
        # Small key/value settings table - currently just holds an
        # admin-set override for the cache size cap, so it can be changed
        # from the admin panel at runtime and survive container restarts,
        # without anyone needing to touch .env or redeploy.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.commit()


_init_db()


@contextmanager
def _get_db():
    conn = sqlite3.connect(CACHE_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _load_persisted_max_bytes(env_default: int) -> int:
    """An admin-set override (via set_cache_max_gb below) takes priority
    over the CACHE_MAX_GB/CACHE_MAX_BYTES env vars once one has been
    saved - this is what makes the limit editable from the admin panel
    without touching .env or restarting the container."""
    try:
        with _get_db() as conn:
            row = conn.execute(
                "SELECT value FROM cache_settings WHERE key = 'max_bytes'"
            ).fetchone()
            if row:
                return int(row["value"])
    except Exception as e:
        logger.warning(f"[CACHE] Failed to load persisted cache limit override (non-fatal): {e}")
    return env_default


# The env-derived value above is only the DEFAULT - _load_persisted_max_bytes
# checks for an admin-set override in the DB and uses that instead if present.
CACHE_MAX_BYTES = _load_persisted_max_bytes(CACHE_MAX_BYTES)


def _cache_file_path(video_id: str, fmt: str) -> str:
    # video_id is a YouTube ID (URL-safe characters only), safe to use
    # directly in a filename without sanitization.
    return os.path.join(CACHE_DIR, f"{video_id}_{fmt}.bin")


def get_cached_audio(video_id: str, fmt: str) -> Tuple[Optional[bytes], Optional[str]]:
    """
    Returns (audio_bytes, title) if a fresh cache entry exists, else
    (None, None). NEVER raises - not-found, missing file, or a stale
    entry are all treated identically as "not cached", so the caller
    always has a clean, simple fallback path (just proceed with a
    normal download) - same guarantee the R2 version made.
    """
    if not video_id:
        return None, None

    try:
        with _get_db() as conn:
            row = conn.execute(
                "SELECT file_path, title, created_at FROM cache_entries WHERE video_id = ? AND format = ?",
                (video_id, fmt),
            ).fetchone()

            if row is None:
                logger.info(f"[CACHE] MISS: {video_id}_{fmt}")
                return None, None

            age_seconds = time.time() - row["created_at"]
            if age_seconds > CACHE_MAX_AGE_SECONDS:
                logger.info(f"[CACHE] MISS (stale, {int(age_seconds)}s old): {video_id}_{fmt}")
                # Clean up the stale entry now rather than leaving dead
                # weight sitting in the cache until LRU eviction gets to it.
                _delete_entry(conn, video_id, fmt, row["file_path"])
                return None, None

            file_path = row["file_path"]
            if not os.path.exists(file_path):
                logger.warning(f"[CACHE] Metadata found for {video_id}_{fmt} but file missing on disk, treating as a miss")
                conn.execute("DELETE FROM cache_entries WHERE video_id = ? AND format = ?", (video_id, fmt))
                conn.commit()
                return None, None

            with open(file_path, "rb") as f:
                data = f.read()

            # Touch last_accessed_at - this is what makes size-cap
            # eviction genuinely LRU (recently-served files survive
            # longer) rather than just oldest-created-first.
            conn.execute(
                "UPDATE cache_entries SET last_accessed_at = ? WHERE video_id = ? AND format = ?",
                (time.time(), video_id, fmt),
            )
            conn.commit()

            title = row["title"] or "Unknown"
            logger.info(f"[CACHE] HIT: {video_id}_{fmt} ({len(data)} bytes, age {int(age_seconds)}s, title='{title}')")
            return data, title

    except Exception as e:
        logger.warning(f"[CACHE] Unexpected read error for {video_id}_{fmt} (non-fatal): {e}")
        return None, None


def put_cached_audio(video_id: str, fmt: str, data: bytes, title: str):
    """
    Saves a successfully downloaded file (+ its title) for future
    requests. Any failure here is logged and swallowed, NEVER raised -
    a caching write failure must not fail the download that already
    succeeded and is about to be returned to the user. Triggers
    size-cap eviction afterward if needed.
    """
    if not video_id or not data:
        return

    file_path = _cache_file_path(video_id, fmt)
    try:
        with open(file_path, "wb") as f:
            f.write(data)

        now = time.time()
        with _get_db() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO cache_entries
                    (video_id, format, title, file_path, size_bytes, created_at, last_accessed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (video_id, fmt, title or "Unknown", file_path, len(data), now, now),
            )
            conn.commit()

        logger.info(f"[CACHE] SAVED: {video_id}_{fmt} ({len(data)} bytes, title='{title}')")

        _evict_if_over_limit()

    except Exception as e:
        logger.warning(f"[CACHE] Failed to save {video_id}_{fmt} (non-fatal, download still succeeded): {e}")


def _delete_entry(conn, video_id: str, fmt: str, file_path: str):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except OSError as e:
        logger.warning(f"[CACHE] Failed to remove stale file {file_path}: {e}")
    conn.execute("DELETE FROM cache_entries WHERE video_id = ? AND format = ?", (video_id, fmt))
    conn.commit()


def _evict_if_over_limit() -> None:
    """Deletes least-recently-accessed entries (DB row + file on disk)
    until total cache size is back under CACHE_MAX_BYTES. Runs
    automatically after every write - no manual step needed for normal
    day-to-day operation. This is genuinely new behavior vs. the R2
    version, since R2 storage wasn't size-constrained the same way local
    disk is."""
    try:
        with _get_db() as conn:
            total = conn.execute("SELECT COALESCE(SUM(size_bytes), 0) as total FROM cache_entries").fetchone()["total"]

            if total <= CACHE_MAX_BYTES:
                return

            evicted_count = 0
            evicted_bytes = 0

            while total > CACHE_MAX_BYTES:
                oldest = conn.execute(
                    "SELECT video_id, format, file_path, size_bytes FROM cache_entries "
                    "ORDER BY last_accessed_at ASC LIMIT 1"
                ).fetchone()

                if oldest is None:
                    break

                try:
                    if os.path.exists(oldest["file_path"]):
                        os.remove(oldest["file_path"])
                except OSError as e:
                    logger.warning(f"[CACHE] Failed to remove evicted file {oldest['file_path']}: {e}")

                conn.execute(
                    "DELETE FROM cache_entries WHERE video_id = ? AND format = ?",
                    (oldest["video_id"], oldest["format"]),
                )
                conn.commit()

                total -= oldest["size_bytes"]
                evicted_count += 1
                evicted_bytes += oldest["size_bytes"]

            logger.info(
                f"[CACHE] Evicted {evicted_count} least-recently-used entries "
                f"({evicted_bytes / (1024*1024):.1f} MB) to stay under the "
                f"{CACHE_MAX_BYTES / (1024*1024*1024):.1f} GB cap"
            )
    except Exception as e:
        logger.warning(f"[CACHE] Eviction check failed (non-fatal): {e}")


def get_disk_usage() -> dict:
    """Real filesystem usage for the disk the cache actually lives on -
    separate from the cache's own bookkeeping below, since the VPS's disk
    is also shared with the OS, Docker images, and every other app file.
    Lets the admin panel show 'X of Y GB used on the whole disk' next to
    'X of Y GB allocated to the cache specifically', so picking a cache
    size isn't a guessing game against unknown free space."""
    total, used, free = shutil.disk_usage(CACHE_DIR)
    return {
        "disk_total_gb": round(total / (1024 ** 3), 2),
        "disk_used_gb": round(used / (1024 ** 3), 2),
        "disk_free_gb": round(free / (1024 ** 3), 2),
        "disk_percent_used": round(100 * used / total, 1) if total > 0 else 0,
    }


def get_cache_stats() -> dict:
    """For the admin endpoint - current cache size, entry count, the
    configured limit, and real disk usage, so you can check status
    without SSHing in."""
    with _get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as count, COALESCE(SUM(size_bytes), 0) as total_bytes FROM cache_entries"
        ).fetchone()
    stats = {
        "entry_count": row["count"],
        "total_bytes": row["total_bytes"],
        "total_gb": round(row["total_bytes"] / (1024 * 1024 * 1024), 3),
        "max_bytes": CACHE_MAX_BYTES,
        "max_gb": round(CACHE_MAX_BYTES / (1024 * 1024 * 1024), 3),
        "percent_full": round(100 * row["total_bytes"] / CACHE_MAX_BYTES, 1) if CACHE_MAX_BYTES > 0 else 0,
    }
    stats.update(get_disk_usage())
    return stats


def set_cache_max_gb(gb: float) -> dict:
    """Updates the cache size cap at runtime from the admin panel and
    persists it to the settings table so it survives container restarts -
    no .env edit or redeploy needed. Immediately re-checks eviction in
    case the new limit is now below what's currently cached."""
    global CACHE_MAX_BYTES
    if gb <= 0:
        raise ValueError("gb must be a positive number")

    new_max_bytes = int(gb * 1024 * 1024 * 1024)
    with _get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO cache_settings (key, value) VALUES ('max_bytes', ?)",
            (str(new_max_bytes),),
        )
        conn.commit()

    CACHE_MAX_BYTES = new_max_bytes
    logger.info(f"[CACHE] Max cache size updated to {gb} GB via admin panel")
    _evict_if_over_limit()
    return get_cache_stats()


def clear_cache() -> dict:
    """For the admin endpoint - manually wipes the entire cache (all
    files + all metadata), for whenever you want a clean slate without
    waiting for automatic LRU eviction to get there gradually."""
    with _get_db() as conn:
        rows = conn.execute("SELECT file_path FROM cache_entries").fetchall()
        removed = 0
        for row in rows:
            try:
                if os.path.exists(row["file_path"]):
                    os.remove(row["file_path"])
                    removed += 1
            except OSError as e:
                logger.warning(f"[CACHE] Failed to remove {row['file_path']} during clear: {e}")
        conn.execute("DELETE FROM cache_entries")
        conn.commit()
    logger.info(f"[CACHE] Manually cleared - removed {removed} files")
    return {"files_removed": removed}