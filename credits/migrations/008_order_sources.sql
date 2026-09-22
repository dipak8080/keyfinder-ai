-- Where a purchase came from. Written when the PayPal order is created
-- (before payment), keyed by the provider's order id so it joins to
-- orders.provider_order_id once the capture lands.
CREATE TABLE IF NOT EXISTS order_sources (
    provider          TEXT NOT NULL DEFAULT 'paypal',
    provider_order_id TEXT PRIMARY KEY,
    source            TEXT,      -- first touch this session: ytwav-funnel, ?src= campaign, or NULL
    tool              TEXT,      -- what opened checkout: gate tool key, or 'pricing'
    page              TEXT,      -- pathname where checkout opened
    subject_id        TEXT,
    created_at        TEXT NOT NULL
);