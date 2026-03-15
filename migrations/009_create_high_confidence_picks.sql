CREATE TABLE IF NOT EXISTS high_confidence_picks (
  id SERIAL PRIMARY KEY,
  league_id TEXT NOT NULL,
  game_id TEXT NOT NULL,
  home_team TEXT NOT NULL,
  away_team TEXT NOT NULL,
  pick_type TEXT NOT NULL,
  direction TEXT NOT NULL,
  line DOUBLE PRECISION,
  prediction DOUBLE PRECISION NOT NULL,
  confidence DOUBLE PRECISION NOT NULL,
  edge DOUBLE PRECISION,
  game_time TIMESTAMPTZ NOT NULL,
  picked_at TIMESTAMPTZ DEFAULT now(),
  result TEXT,
  actual_value DOUBLE PRECISION,
  graded_at TIMESTAMPTZ,
  model_version TEXT,
  UNIQUE (league_id, game_id, pick_type)
);
CREATE INDEX IF NOT EXISTS idx_hcp_league ON high_confidence_picks(league_id);
