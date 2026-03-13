-- Add ML feature vector storage to bet_logs for training feedback loop.
-- When a bet fires, the 47-element feature vector used for inference is
-- persisted alongside the prediction.  Once grading fills in `actual`,
-- we have a complete (features, label) training sample.

ALTER TABLE bet_logs ADD COLUMN IF NOT EXISTS features JSONB;
