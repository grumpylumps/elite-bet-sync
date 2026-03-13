CREATE TABLE IF NOT EXISTS ml_training_runs (
  id SERIAL PRIMARY KEY,
  league_id TEXT NOT NULL,
  model_name TEXT NOT NULL,
  samples_used INTEGER,
  metrics JSONB,
  started_at TIMESTAMPTZ DEFAULT now(),
  completed_at TIMESTAMPTZ,
  status TEXT DEFAULT 'completed'
);
CREATE INDEX IF NOT EXISTS idx_ml_training_runs_league ON ml_training_runs(league_id, model_name);
