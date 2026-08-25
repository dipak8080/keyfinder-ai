-- 002 — Upgrade idempotency.
--
-- WHY THIS TABLE EXISTS
-- A double-click on "Upgrade to Studio Quality" charged twice. The
-- upgrade routes create a NEW job each call, so nothing in job_charges
-- (keyed on the new job_id) could catch it — every click looked like a
-- legitimately distinct job.
--
-- The invariant that actually matters is one HQ child per SOURCE job,
-- so that is what gets the primary key. A second call returns the first
-- call's child job id instead of charging again, which makes a
-- double-click indistinguishable from a single one at the API level —
-- no client-side guard required, though the frontend should still
-- disable the button for the obvious reason.
--
-- WHY A SEPARATE TABLE rather than a column on job_charges: a charge
-- row is created per job and this constraint is per SOURCE job. Putting
-- a unique index on a nullable column of a table with different
-- cardinality would work but reads as an afterthought; this states the
-- rule directly.
--
-- The row is inserted BEFORE the credit is charged and deleted if the
-- charge or enqueue fails — see routes/separation_upgrade.py. That
-- ordering is what closes the race between two concurrent clicks, which
-- checking-then-inserting would not.

CREATE TABLE IF NOT EXISTS job_upgrades (
    source_job_id  TEXT PRIMARY KEY,
    upgrade_job_id TEXT NOT NULL,
    tool           TEXT NOT NULL,
    created_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_upgrades_child ON job_upgrades(upgrade_job_id);