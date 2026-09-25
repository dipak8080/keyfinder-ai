-- One welcome grant per account, recorded with the requesting network so
-- the per-IP cap can be enforced without reading the ledger.
CREATE TABLE IF NOT EXISTS signup_bonuses (
    account_id  TEXT PRIMARY KEY,
    ip_hash     TEXT,
    credits     INTEGER NOT NULL,
    method      TEXT NOT NULL,
    granted_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_signup_bonuses_ip ON signup_bonuses(ip_hash, granted_at);