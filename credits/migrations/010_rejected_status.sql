-- Adds 'rejected' to the status CHECK: a submit refused before any GPU
-- work existed (402, over-length 400, budget 503, turnstile 428). Such
-- rows previously sat at 'created' forever and were counted as in-flight
-- by the free-GPU budget projection. SQLite cannot alter a CHECK, so the
-- table is rebuilt; the view referencing it is dropped first and
-- recreated identically.
BEGIN;

DROP VIEW IF EXISTS gpu_cost_daily;

ALTER TABLE gpu_job_metrics RENAME TO gpu_job_metrics_old;

CREATE TABLE gpu_job_metrics (
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
                                        'failed', 'cancelled', 'timeout',
                                        'rejected')),
    error           TEXT,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    ended_at        TEXT,
    failure_side    TEXT
                      CHECK (failure_side IN ('client', 'server'))
);

INSERT INTO gpu_job_metrics
    (job_id, tool, subject_id, account_id, ip_hash, charge_type,
     paywall_enabled, input_seconds, input_bytes, runpod_job_id, gpu_type,
     gpu_seconds, queue_seconds, wall_seconds, est_cost_usd, status, error,
     created_at, started_at, ended_at, failure_side)
SELECT job_id, tool, subject_id, account_id, ip_hash, charge_type,
       paywall_enabled, input_seconds, input_bytes, runpod_job_id, gpu_type,
       gpu_seconds, queue_seconds, wall_seconds, est_cost_usd, status, error,
       created_at, started_at, ended_at, failure_side
FROM gpu_job_metrics_old;

DROP TABLE gpu_job_metrics_old;

CREATE INDEX idx_gpu_tool_time ON gpu_job_metrics(tool, created_at);
CREATE INDEX idx_gpu_status    ON gpu_job_metrics(status, created_at);
CREATE INDEX idx_gpu_side      ON gpu_job_metrics(failure_side, created_at);

-- Close orphans left before this fix: rows still 'created' after two
-- hours never reached the runner and never will.
UPDATE gpu_job_metrics
   SET status='rejected', failure_side='client',
       error=COALESCE(error, 'orphaned_before_010'),
       ended_at=strftime('%Y-%m-%dT%H:%M:%S.000Z','now')
 WHERE status='created'
   AND created_at < strftime('%Y-%m-%dT%H:%M:%S.000Z','now','-2 hours');

CREATE VIEW gpu_cost_daily AS
SELECT substr(created_at, 1, 10)                                 AS day,
       tool,
       COUNT(*)                                                  AS jobs,
       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)     AS completed,
       SUM(CASE WHEN status IN ('failed', 'timeout', 'cancelled')
                 AND COALESCE(failure_side, 'server') = 'server'
                THEN 1 ELSE 0 END)                               AS failed,
       SUM(CASE WHEN failure_side = 'client' THEN 1 ELSE 0 END)  AS rejected,
       ROUND(SUM(COALESCE(input_seconds, 0)) / 60.0, 2)          AS input_minutes,
       ROUND(SUM(COALESCE(gpu_seconds, 0)), 1)                   AS gpu_seconds,
       ROUND(SUM(COALESCE(est_cost_usd, 0)), 4)                  AS est_cost_usd,
       SUM(CASE WHEN charge_type = 'credit' THEN 1 ELSE 0 END)   AS paid_jobs,
       SUM(CASE WHEN charge_type = 'free'   THEN 1 ELSE 0 END)   AS free_jobs
FROM gpu_job_metrics
GROUP BY day, tool;

COMMIT;