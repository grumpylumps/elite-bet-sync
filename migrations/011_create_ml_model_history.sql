CREATE TABLE IF NOT EXISTS ml_model_history (
  id SERIAL PRIMARY KEY,
  league_id TEXT NOT NULL,
  model_name TEXT NOT NULL,
  metadata TEXT,
  trained_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ml_model_history_league ON ml_model_history(league_id, trained_at DESC);
