-- Studio results kept for signed-in accounts, stored as FLAC in R2.
CREATE TABLE IF NOT EXISTS library_items (
    job_id      TEXT PRIMARY KEY,
    account_id  TEXT NOT NULL,
    title       TEXT,
    kind        TEXT NOT NULL,
    stems       TEXT NOT NULL,
    analysis    TEXT,
    size_bytes  INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL,
    expires_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_library_account ON library_items(account_id, created_at);
CREATE INDEX IF NOT EXISTS idx_library_expiry ON library_items(expires_at);