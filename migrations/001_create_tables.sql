-- Core app tables (subset based on client schema)

CREATE TABLE IF NOT EXISTS bet_logs (
  id SERIAL PRIMARY KEY,
  league_id TEXT NOT NULL,
  game_id TEXT NOT NULL,
  period INTEGER NOT NULL,
  trigger TEXT NOT NULL,
  line DOUBLE PRECISION,
  proj DOUBLE PRECISION,
  edge DOUBLE PRECISION,
  probability DOUBLE PRECISION,
  direction TEXT,
  captured_at TIMESTAMPTZ DEFAULT now(),
  capture_type TEXT,
  actual DOUBLE PRECISION,
  result TEXT,
  result_logged_at TIMESTAMPTZ,
  stake DOUBLE PRECISION DEFAULT 100.0,
  home_team TEXT DEFAULT '',
  away_team TEXT DEFAULT '',
  UNIQUE (league_id, game_id, period, trigger)
);

CREATE TABLE IF NOT EXISTS user_bets (
  id SERIAL PRIMARY KEY,
  uuid TEXT NOT NULL UNIQUE,  -- UUID for cross-device sync (local IDs don't work across devices)
  league_id TEXT NOT NULL,
  game_id TEXT NOT NULL,
  period INTEGER NOT NULL,
  home_team TEXT,
  away_team TEXT,
  current_total DOUBLE PRECISION NOT NULL,
  proj_total DOUBLE PRECISION,
  amount DOUBLE PRECISION NOT NULL,
  direction TEXT NOT NULL,
  line DOUBLE PRECISION NOT NULL,
  clock TEXT,
  bet_type TEXT,
  scope TEXT,
  actual_total DOUBLE PRECISION,
  result TEXT,
  profit_loss DOUBLE PRECISION,
  created_at TIMESTAMPTZ DEFAULT now(),
  graded_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS elo_ratings (
  league_id TEXT NOT NULL,
  team_id TEXT NOT NULL,
  team_name TEXT,
  elo DOUBLE PRECISION NOT NULL,
  last_updated TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (league_id, team_id)
);

CREATE TABLE IF NOT EXISTS team_stats (
  league_id TEXT NOT NULL,
  team_id TEXT NOT NULL,
  team_name TEXT,
  ppg DOUBLE PRECISION,
  period_avg TEXT,
  games_analyzed INTEGER DEFAULT 0,
  pace DOUBLE PRECISION,
  fg_pct DOUBLE PRECISION,
  three_pct DOUBLE PRECISION,
  ft_pct DOUBLE PRECISION,
  wins INTEGER DEFAULT 0,
  losses INTEGER DEFAULT 0,
  avg_score_allowed DOUBLE PRECISION,
  last_updated TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (league_id, team_id)
);

CREATE TABLE IF NOT EXISTS game_odds (
  league_id TEXT NOT NULL,
  game_id TEXT NOT NULL,
  over_odds DOUBLE PRECISION,
  under_odds DOUBLE PRECISION,
  total_line DOUBLE PRECISION,
  bookmaker TEXT,
  spread_home TEXT,
  spread_away TEXT,
  moneyline_home DOUBLE PRECISION,
  moneyline_away DOUBLE PRECISION,
  last_updated TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (league_id, game_id)
);

CREATE TABLE IF NOT EXISTS trigger_alerts (
  id SERIAL PRIMARY KEY,
  league_id TEXT NOT NULL,
  game_id TEXT NOT NULL,
  period INTEGER NOT NULL,
  trigger TEXT NOT NULL,
  period_target INTEGER,
  projected_period_total DOUBLE PRECISION,
  direction TEXT,
  probability DOUBLE PRECISION,
  timestamp TIMESTAMPTZ DEFAULT now(),
  message TEXT,
  best_time_json TEXT,
  is_best BOOLEAN DEFAULT FALSE,
  UNIQUE (league_id, game_id, period, trigger)
);

CREATE TABLE IF NOT EXISTS cached_games (
  league_id TEXT NOT NULL,
  game_id TEXT NOT NULL,
  home_team_id TEXT,
  home_team_name TEXT,
  home_team_abbrev TEXT,
  away_team_id TEXT,
  away_team_name TEXT,
  away_team_abbrev TEXT,
  home_score INTEGER,
  away_score INTEGER,
  period INTEGER,
  clock TEXT,
  status TEXT,
  status_detail TEXT,
  start_time TIMESTAMPTZ,
  period_scores TEXT,
  game_data TEXT,
  elo_updated BOOLEAN DEFAULT FALSE,
  last_updated TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (league_id, game_id)
);

-- Server-side change log and applied changes for idempotency
CREATE TABLE IF NOT EXISTS server_changes (
  server_seq BIGSERIAL PRIMARY KEY,
  table_name TEXT NOT NULL,
  pk TEXT NOT NULL,
  op TEXT NOT NULL,
  payload JSONB,
  change_id TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS applied_changes (
  change_id TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ DEFAULT now()
);

-- Simple index for quick fetch
CREATE INDEX IF NOT EXISTS idx_server_changes_seq ON server_changes(server_seq);
