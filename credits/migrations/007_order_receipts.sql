-- Receipt email outcome per order. Until now a failed receipt only reached
-- the log, so a buyer with a missed claim AND a failed receipt had no
-- recovery path and the admin had no record of it.

ALTER TABLE orders ADD COLUMN receipt_sent_at TEXT;
ALTER TABLE orders ADD COLUMN receipt_error TEXT;