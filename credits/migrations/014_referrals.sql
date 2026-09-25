-- Referral codes, the first code each browser arrived with, and the
-- attribution that pays out once the friend makes their first purchase.
CREATE TABLE IF NOT EXISTS referral_codes (
    account_id  TEXT PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS referral_claims (
    subject_id          TEXT PRIMARY KEY,
    code                TEXT NOT NULL,
    referrer_account_id TEXT NOT NULL,
    ip_hash             TEXT,
    created_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS referrals (
    referee_account_id  TEXT PRIMARY KEY,
    referrer_account_id TEXT NOT NULL,
    code                TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    order_id            TEXT,
    created_at          TEXT NOT NULL,
    rewarded_at         TEXT
);
CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_account_id, status, rewarded_at);