-- ML model weights table (mirrors the Python ML service schema).
-- The ML service creates this via SQLAlchemy on startup, but we declare it
-- here so the sync server can query it for conflict resolution even when the
-- ML service hasn't run yet.

CREATE TABLE IF NOT EXISTS ml_models (
  id SERIAL PRIMARY KEY,
  league_id TEXT NOT NULL,
  model_name TEXT NOT NULL,
  metadata TEXT,
  samples_used INTEGER,
  total_mae DOUBLE PRECISION,
  updated_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (league_id, model_name)
);

CREATE INDEX IF NOT EXISTS idx_ml_models_league ON ml_models(league_id);
