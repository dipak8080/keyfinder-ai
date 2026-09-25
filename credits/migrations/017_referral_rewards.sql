-- What each referral actually paid, so the monthly cap and the totals shown
-- to users stay right after a reward setting changes or a refund reverses it.
ALTER TABLE referrals ADD COLUMN reward_credits INTEGER;
ALTER TABLE referrals ADD COLUMN referrer_paid INTEGER;