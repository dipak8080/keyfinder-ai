-- 005 - gate_events: the paywall funnel.
--
-- WHAT WAS MISSING
-- gpu_job_metrics only gets a row once a job RUNS. A caller who is
-- refused for payment never reaches a worker, so the refusal was raised
-- as a 402 and then discarded. The result: "how many people hit the
-- upgrade wall?" had no answer anywhere in the database, and every
-- pricing and free-tier decision was being made against jobs that
-- succeeded - a sample that excludes, by construction, everyone who was
-- stopped.
--
-- TWO EVENTS, NOT ONE
--   preview_blocked  the frontend asked /credits/preview before submit
--                    and got will_use='blocked'. This is the gate being
--                    SEEN, and it is the larger and more useful number.
--   submit_402       they submitted anyway and the guard refused. A
--                    subset: either preview was skipped, or the server's
--                    ffprobe duration disagreed with the browser's.
--
-- Both are kept because the gap between them is itself the signal. A
-- large preview_blocked count with near-zero submit_402 means the UI is
-- stopping people correctly; the reverse means the preview call is not
-- firing where it should.
--
-- NO CHECK CONSTRAINT ON `event`, DELIBERATELY.
-- free_usage carries CHECK (scope IN ('owner','ip')) and SQLite cannot
-- ALTER a CHECK - adding a third scope there required a table rebuild
-- and took sign-in down in production (see 003's header). A funnel table
-- is exactly the kind of thing that grows a new event type later, so it
-- is not being given the same trap. Unknown values are a read-side
-- filter problem, which is recoverable; a constraint that needs a
-- rebuild on a live money database is not.
--
-- WRITES ARE BEST-EFFORT. paywall.py wraps every insert here in its own
-- try/except: an analytics row is never allowed to turn a working 402
-- into a 500, or to stop a preview from returning.

CREATE TABLE IF NOT EXISTS gate_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    event          TEXT NOT NULL,
    tool           TEXT NOT NULL,
    owner_type     TEXT NOT NULL,
    owner_id       TEXT NOT NULL,
    subject_id     TEXT NOT NULL,
    account_id     TEXT,
    ip_hash        TEXT,
    period         TEXT NOT NULL,
    credits_needed INTEGER NOT NULL DEFAULT 0,
    balance        INTEGER NOT NULL DEFAULT 0,
    free_remaining INTEGER NOT NULL DEFAULT 0,
    input_seconds  REAL,
    created_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gate_time    ON gate_events(created_at);
CREATE INDEX IF NOT EXISTS idx_gate_tool    ON gate_events(tool, created_at);

-- The throttle in paywall.py reads this one: same subject, same tool,
-- same event, most recent first. Column order matters - it is an
-- equality-equality-equality-range lookup, so created_at goes last.
CREATE INDEX IF NOT EXISTS idx_gate_subject ON gate_events(subject_id, tool, event, created_at);

-- Daily rollup. DISTINCT subject_id, not COUNT(*): /credits/preview
-- fires on every file drop, so raw event counts overstate people. The
-- per-day distinct count is the number worth reading.
CREATE VIEW IF NOT EXISTS gate_daily AS
SELECT substr(created_at, 1, 10)                  AS day,
       tool,
       event,
       COUNT(*)                                   AS events,
       COUNT(DISTINCT subject_id)                 AS subjects,
       COUNT(DISTINCT ip_hash)                    AS ips,
       SUM(CASE WHEN account_id IS NOT NULL THEN 1 ELSE 0 END) AS signed_in_events
FROM gate_events
GROUP BY day, tool, event;