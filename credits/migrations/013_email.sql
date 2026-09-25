-- Email preferences on the account, and an outbox so non-urgent emails
-- (low balance, monthly free song, updates) go out under a daily cap.
ALTER TABLE accounts ADD COLUMN email_updates INTEGER NOT NULL DEFAULT 0;
ALTER TABLE accounts ADD COLUMN email_notices INTEGER NOT NULL DEFAULT 1;
ALTER TABLE magic_links ADD COLUMN email_updates INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS email_outbox (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id  TEXT,
    email       TEXT NOT NULL,
    kind        TEXT NOT NULL,
    dedupe_key  TEXT NOT NULL UNIQUE,
    priority    INTEGER NOT NULL DEFAULT 5,
    subject     TEXT,
    html        TEXT,
    text        TEXT,
    unsubscribe_url TEXT,
    status      TEXT NOT NULL DEFAULT 'queued',
    error       TEXT,
    created_at  TEXT NOT NULL,
    sent_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_email_outbox_queue ON email_outbox(status, priority, id);
CREATE INDEX IF NOT EXISTS idx_email_outbox_sent ON email_outbox(sent_at);