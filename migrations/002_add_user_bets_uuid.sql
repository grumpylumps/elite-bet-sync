-- Add uuid column to user_bets for cross-device sync
-- Local auto-increment IDs don't work across devices, so we need a global unique identifier

-- Add the uuid column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'user_bets' AND column_name = 'uuid'
    ) THEN
        ALTER TABLE user_bets ADD COLUMN uuid TEXT;

        -- Generate UUIDs for existing rows that don't have one
        UPDATE user_bets SET uuid = gen_random_uuid()::text WHERE uuid IS NULL;

        -- Make uuid NOT NULL and UNIQUE
        ALTER TABLE user_bets ALTER COLUMN uuid SET NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS user_bets_uuid_idx ON user_bets(uuid);
    END IF;
END $$;
