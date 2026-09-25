-- Studio Pass subscriptions, mirrored from Dodo subscription webhooks.
-- Credits are never granted from this table: every paid cycle arrives as
-- its own payment.succeeded and goes through the normal ledger path.
CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id             TEXT PRIMARY KEY,
    provider                    TEXT NOT NULL DEFAULT 'dodo',
    account_id                  TEXT,
    email                       TEXT,
    customer_id                 TEXT,
    product_id                  TEXT,
    status                      TEXT NOT NULL,
    cancel_at_next_billing_date INTEGER NOT NULL DEFAULT 0,
    next_billing_date           TEXT,
    last_event                  TEXT,
    last_event_at               TEXT,
    created_at                  TEXT NOT NULL,
    updated_at                  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_subscriptions_account ON subscriptions(account_id, status);