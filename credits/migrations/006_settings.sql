-- Runtime-editable overrides for anything credits/config.py reads from env.
-- Resolution order everywhere: settings row -> env var -> code default.

CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    updated_by TEXT NOT NULL DEFAULT 'admin'
);

-- Append-only. Every set and every clear lands here, including the value
-- that was in force before, so a bad change can be read back and undone.
CREATE TABLE IF NOT EXISTS settings_audit (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    key        TEXT NOT NULL,
    old_value  TEXT,
    new_value  TEXT,
    action     TEXT NOT NULL,
    actor      TEXT NOT NULL DEFAULT 'admin',
    note       TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_settings_audit_key ON settings_audit (key, created_at);
CREATE INDEX IF NOT EXISTS idx_settings_audit_time ON settings_audit (created_at);