-- Studio Pass credit lots: each paid Pass cycle is one lot that rolls over
-- for STUDIO_PASS_ROLLOVER_MONTHS and then its unused part expires.
CREATE TABLE IF NOT EXISTS pass_credit_lots (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id       TEXT NOT NULL,
    payment_id       TEXT NOT NULL UNIQUE,
    subscription_id  TEXT,
    credits          INTEGER NOT NULL,
    remaining        INTEGER NOT NULL,
    expired_credits  INTEGER NOT NULL DEFAULT 0,
    expires_at       TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'active',
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pass_lots_account ON pass_credit_lots(account_id, status, expires_at);
CREATE INDEX IF NOT EXISTS idx_pass_lots_expiry ON pass_credit_lots(status, expires_at);

CREATE TABLE IF NOT EXISTS pass_lot_usage (
    job_id   TEXT NOT NULL,
    lot_id   INTEGER NOT NULL,
    credits  INTEGER NOT NULL,
    PRIMARY KEY (job_id, lot_id)
);

ALTER TABLE subscriptions ADD COLUMN last_synced_at TEXT;
ALTER TABLE subscriptions ADD COLUMN ended_at TEXT;
UPDATE subscriptions SET ended_at = updated_at
 WHERE status IN ('cancelled', 'expired', 'failed') AND ended_at IS NULL;

ALTER TABLE orders ADD COLUMN credits_reversed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN subscription_id TEXT;
ALTER TABLE order_sources ADD COLUMN account_id TEXT;

ALTER TABLE email_outbox ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE email_outbox ADD COLUMN next_attempt_at TEXT;
ALTER TABLE email_outbox ADD COLUMN not_after TEXT;

-- One row per periodic task, so a redeploy never resets its schedule.
CREATE TABLE IF NOT EXISTS scheduler_runs (
    name              TEXT PRIMARY KEY,
    last_started_at   TEXT,
    last_finished_at  TEXT,
    last_ok           INTEGER,
    last_error        TEXT,
    last_result       TEXT
);