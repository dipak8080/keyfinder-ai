-- 004 - failure_side on gpu_job_metrics.
--
-- WHAT WAS WRONG
-- Every job that did not finish counted as a failure: a 3-hour video too
-- long to transcribe, a drum loop with no notes, a removed YouTube video.
-- On 2026-09-11 the dashboard read "Transcription failed on 110 of 432
-- jobs (25%)" and alerts fired for requests the server handled correctly.
-- A number that mixes "the input can't be processed" with "we broke"
-- cannot tell you when something is actually broken.
--
-- WHAT THIS COLUMN IS
-- Whose outcome a non-finished job was: 'client' (the input or the video)
-- or 'server' (ours). NULL while running and on success. status stays
-- 'failed' either way, because the job did not produce output; the side
-- is what decides whether it counts as a failure.
--
-- BACKFILL IS CONSERVATIVE BY DESIGN
-- Old rows are marked 'client' only when their stored error or refund
-- reason matches a message the code raises for input problems. Anything
-- else, including errors that were never stored, stays 'server'. Wrongly
-- calling a real failure "the user's" would hide an outage; wrongly
-- calling a rejection a failure only overstates, which is what the
-- dashboard already did.
--
-- SAFE UNDER BLUE-GREEN: the draining container's queries never name the
-- new column, so they keep working while this runs.

BEGIN;

ALTER TABLE gpu_job_metrics ADD COLUMN failure_side TEXT
    CHECK (failure_side IN ('client', 'server'));

UPDATE gpu_job_metrics
SET failure_side = CASE
    WHEN error = 'duration_exceeded'
      OR job_id IN (SELECT job_id FROM job_charges
                    WHERE refund_reason = 'too_long_for_transcription')
      OR error LIKE 'Unsupported file type%'
      OR error LIKE 'Conversion from %'
      OR error LIKE 'Could not read this file as valid audio%'
      OR error LIKE 'Audio is too long%'
      OR error LIKE 'Track is % min long, which exceeds%'
      OR error LIKE 'Video is % min long, which exceeds%'
      OR error LIKE 'This video is % min long. Right now only%'
      OR error LIKE 'No speech was detected%'
      OR error LIKE 'That file is empty%'
      OR error LIKE 'That file appears to be empty%'
      OR error LIKE 'Unknown instrument%'
      OR error LIKE 'No notes were detected%'
      OR error LIKE 'No piano notes were detected%'
      OR error LIKE 'Every detected note fell outside%'
      OR error LIKE 'This track appears to be silent or drums-only%'
      OR error LIKE 'Not enough clear notes were found%'
      OR error LIKE 'This video is unavailable%'
      OR error LIKE 'This video is restricted by the uploader%'
      OR error LIKE 'This video is age-restricted%'
      OR error LIKE 'This video is exclusive to that channel%'
      OR error LIKE 'This video is a scheduled premiere%'
      OR error LIKE 'This track is exclusive to YouTube Music Premium%'
    THEN 'client'
    ELSE 'server'
END
WHERE status IN ('failed', 'timeout', 'cancelled');

CREATE INDEX IF NOT EXISTS idx_gpu_side ON gpu_job_metrics(failure_side, created_at);

-- failed now means OUR failures, and includes timeout and cancelled, the
-- same definition metering.totals() uses. rejected is the input's.
DROP VIEW IF EXISTS gpu_cost_daily;
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