-- AudioForges credits / paywall schema
-- SQLite 3.35+. Idempotent: safe to run on every boot.
-- Lives in its own DB file (data/credits.db) so it never contends with
-- cache_meta.db or logs.db on WAL writes.

PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------------------
-- Identity
-- ---------------------------------------------------------------------------

-- Created on first purchase (from the checkout email) or first magic-link login.
CREATE TABLE IF NOT EXISTS accounts (
    id            TEXT PRIMARY KEY,
    email         TEXT NOT NULL COLLATE NOCASE UNIQUE,
    status        TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'blocked')),
    created_at    TEXT NOT NULL,
    last_login_at TEXT
);

-- One row per browser (signed cookie). Anonymous until linked to an account.
CREATE TABLE IF NOT EXISTS subjects (
    id            TEXT PRIMARY KEY,
    account_id    TEXT REFERENCES accounts(id) ON DELETE SET NULL,
    first_ip_hash TEXT,
    last_ip_hash  TEXT,
    created_at    TEXT NOT NULL,
    last_seen_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_subjects_account ON subjects(account_id);
CREATE INDEX IF NOT EXISTS idx_subjects_last_ip ON subjects(last_ip_hash);

CREATE TABLE IF NOT EXISTS sessions (
    id         TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    subject_id TEXT,
    ip_hash    TEXT,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    revoked_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_sessions_account ON sessions(account_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);

-- Only the HMAC of the token is stored, never the token itself.
CREATE TABLE IF NOT EXISTS magic_links (
    token_hash TEXT PRIMARY KEY,
    email      TEXT NOT NULL COLLATE NOCASE,
    subject_id TEXT,
    purpose    TEXT NOT NULL DEFAULT 'login',
    ip_hash    TEXT,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    used_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_magic_email ON magic_links(email, created_at);

-- Ko-fi's webhook carries no custom data, so the buyer's browser can't be
-- identified from the payload — only their email. We record the intent to buy
-- here before sending them to Ko-fi, then match on email when the webhook
-- lands, so credits appear in the tab they bought from. Best-effort: if the
-- match misses, the receipt email's magic link still works.
CREATE TABLE IF NOT EXISTS pending_claims (
    email      TEXT PRIMARY KEY COLLATE NOCASE,
    subject_id TEXT NOT NULL,
    pack       TEXT,
    ip_hash    TEXT,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    claimed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_claims_subject ON pending_claims(subject_id);

-- ---------------------------------------------------------------------------
-- Credits
-- ---------------------------------------------------------------------------

-- Append-only. Balance = SUM(delta). Nothing expires, nothing is ever updated.
-- idempotency_key is what makes double-crediting and double-refunding impossible.
CREATE TABLE IF NOT EXISTS credit_ledger (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_type      TEXT NOT NULL CHECK (owner_type IN ('account', 'subject')),
    owner_id        TEXT NOT NULL,
    delta           INTEGER NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN (
                        'purchase', 'job_hold', 'job_refund',
                        'admin_adjust', 'chargeback', 'bonus')),
    job_id          TEXT,
    order_id        TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    note            TEXT,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ledger_owner ON credit_ledger(owner_type, owner_id);
CREATE INDEX IF NOT EXISTS idx_ledger_job   ON credit_ledger(job_id);
CREATE INDEX IF NOT EXISTS idx_ledger_time  ON credit_ledger(created_at);

-- The free tier is a monthly counter, not credits: it resets, credits don't.
-- scope 'owner' = account:<id> | subject:<id>, scope 'ip' = <ip_hash>
CREATE TABLE IF NOT EXISTS free_usage (
    period     TEXT NOT NULL,          -- 'YYYY-MM' (UTC)
    scope      TEXT NOT NULL CHECK (scope IN ('owner', 'ip')),
    scope_key  TEXT NOT NULL,
    used       INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (period, scope, scope_key)
);

-- Exactly one row per job that reached the paywall. Drives refund idempotency.
CREATE TABLE IF NOT EXISTS job_charges (
    job_id        TEXT PRIMARY KEY,
    tool          TEXT NOT NULL,
    charge_type   TEXT NOT NULL CHECK (charge_type IN ('free', 'credit', 'none')),
    owner_type    TEXT NOT NULL,
    owner_id      TEXT NOT NULL,
    subject_id    TEXT NOT NULL,
    ip_hash       TEXT,
    period        TEXT NOT NULL,
    credits       INTEGER NOT NULL DEFAULT 0,
    status        TEXT NOT NULL DEFAULT 'held' CHECK (status IN ('held', 'settled', 'refunded')),
    created_at    TEXT NOT NULL,
    settled_at    TEXT,
    refunded_at   TEXT,
    refund_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_charges_owner  ON job_charges(owner_type, owner_id);
CREATE INDEX IF NOT EXISTS idx_charges_status ON job_charges(status, created_at);

-- ---------------------------------------------------------------------------
-- Payments — provider-neutral (kofi | lemonsqueezy | paddle)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS orders (
    id                TEXT PRIMARY KEY,
    provider          TEXT NOT NULL DEFAULT 'kofi',
    provider_order_id TEXT NOT NULL,      -- Ko-fi: kofi_transaction_id
    provider_ref      TEXT,               -- Ko-fi: message_id / receipt url
    account_id        TEXT REFERENCES accounts(id) ON DELETE SET NULL,
    subject_id        TEXT,
    email             TEXT COLLATE NOCASE,
    pack              TEXT,
    price_ref         TEXT,               -- Ko-fi: shop item direct_link_code
    credits           INTEGER NOT NULL DEFAULT 0,
    amount_cents      INTEGER,
    currency          TEXT,
    status            TEXT NOT NULL DEFAULT 'paid',
    test_mode         INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL,
    raw               TEXT,
    UNIQUE (provider, provider_order_id)
);
CREATE INDEX IF NOT EXISTS idx_orders_email ON orders(email);

-- Webhook replay protection + audit trail. Ko-fi retries on non-200, and can
-- deliver the same message_id more than once.
CREATE TABLE IF NOT EXISTS webhook_events (
    event_id     TEXT PRIMARY KEY,
    provider     TEXT NOT NULL DEFAULT 'kofi',
    event_name   TEXT NOT NULL,
    received_at  TEXT NOT NULL,
    processed_at TEXT,
    error        TEXT,
    payload      TEXT
);

-- ---------------------------------------------------------------------------
-- Metering — every GPU job, paid or free, paywall on or off
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gpu_job_metrics (
    job_id          TEXT PRIMARY KEY,
    tool            TEXT NOT NULL,
    subject_id      TEXT,
    account_id      TEXT,
    ip_hash         TEXT,
    charge_type     TEXT,                 -- free | credit | none
    paywall_enabled INTEGER NOT NULL DEFAULT 0,
    input_seconds   REAL,                 -- media duration submitted
    input_bytes     INTEGER,
    runpod_job_id   TEXT,
    gpu_type        TEXT,
    gpu_seconds     REAL,                 -- RunPod executionTime / 1000
    queue_seconds   REAL,                 -- RunPod delayTime / 1000
    wall_seconds    REAL,
    est_cost_usd    REAL,
    status          TEXT NOT NULL DEFAULT 'created'
                      CHECK (status IN ('created', 'running', 'completed',
                                        'failed', 'cancelled', 'timeout')),
    error           TEXT,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    ended_at        TEXT
);
CREATE INDEX IF NOT EXISTS idx_gpu_tool_time ON gpu_job_metrics(tool, created_at);
CREATE INDEX IF NOT EXISTS idx_gpu_status    ON gpu_job_metrics(status, created_at);

-- Ready to render in the admin dashboard.
CREATE VIEW IF NOT EXISTS gpu_cost_daily AS
SELECT substr(created_at, 1, 10)                                 AS day,
       tool,
       COUNT(*)                                                  AS jobs,
       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)     AS completed,
       SUM(CASE WHEN status = 'failed'    THEN 1 ELSE 0 END)     AS failed,
       ROUND(SUM(COALESCE(input_seconds, 0)) / 60.0, 2)          AS input_minutes,
       ROUND(SUM(COALESCE(gpu_seconds, 0)), 1)                   AS gpu_seconds,
       ROUND(SUM(COALESCE(est_cost_usd, 0)), 4)                  AS est_cost_usd,
       SUM(CASE WHEN charge_type = 'credit' THEN 1 ELSE 0 END)   AS paid_jobs,
       SUM(CASE WHEN charge_type = 'free'   THEN 1 ELSE 0 END)   AS free_jobs
FROM gpu_job_metrics
GROUP BY day, tool;