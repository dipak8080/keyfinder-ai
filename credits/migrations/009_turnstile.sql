-- One row per IP hash that solved a Turnstile challenge. Free GPU runs past
-- the daily threshold need a valid row here; credit-paid runs never do.
CREATE TABLE IF NOT EXISTS turnstile_passes (
    ip_hash        TEXT PRIMARY KEY,
    verified_until TEXT NOT NULL,
    passes         INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);