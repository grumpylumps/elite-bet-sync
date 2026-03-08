-- Create a dedicated DB role for the sync server (if missing) and grant privileges
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flutter_sync') THEN
    CREATE ROLE flutter_sync WITH LOGIN;
  END IF;
END$$;

-- Grant privileges on existing tables and sequences
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO flutter_sync;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO flutter_sync;

-- Ensure future tables and sequences will grant privileges automatically
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO flutter_sync;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO flutter_sync;
