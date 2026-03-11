-- Add user_email to user_bets so bets can be fetched per user on login
ALTER TABLE user_bets ADD COLUMN IF NOT EXISTS user_email TEXT DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_user_bets_user_email ON user_bets(user_email);
