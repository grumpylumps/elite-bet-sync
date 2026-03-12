CREATE TABLE IF NOT EXISTS device_sessions (
  device_id      TEXT        PRIMARY KEY,
  last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_server_seq BIGINT     NOT NULL DEFAULT 0,
  ip             TEXT
);
