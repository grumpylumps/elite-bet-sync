# Sync Prototype Server

> Note: The `test:e2e` script uses `docker-compose` and a POSIX shell (Linux/Mac). On Windows use the included Docker Compose commands and run the migration via PowerShell manually before running `npm test`.

Authentication:

- To secure the sync endpoints set the environment variable `SYNC_API_TOKEN` to a secret string and configure clients to send `Authorization: Bearer <token>` on POST requests to `/sync` and `/sync/dryrun`.
- When `SYNC_API_TOKEN` is not set the server allows anonymous access (convenient for local development).

Simple Node + Express server that accepts client sync changes and writes them to Postgres.

Environment:

- Set `DATABASE_URL` to your Postgres DB (defaults to `postgresql://postgres:postgres@localhost:5432/elite_bet_sync`).

Run migrations (use psql):
psql $DATABASE_URL -f migrations/001_create_tables.sql

CI note: Our GitHub Actions workflow (`.github/workflows/sync-migrate.yml`) runs Postgres and executes `npm run migrate` before running tests to ensure the `flutter_sync` role and privileges are created and migrations are applied automatically in CI.

Start server:
cd sync_prototype/server
npm install
npm start

Quick local development note:

- The web launch scripts (`launch_web.bat`, `launch_web_build_serve.bat`, `smart_launch.ps1`) first try to contact a running sync server at `http://localhost:8081/health`.
- If none is found they will attempt to start the _real_ sync server automatically by running `sync_prototype/server/start_sync_server.ps1`.
- If the real server fails to become healthy (for example, when Postgres is not available) the scripts will report the failure; the legacy stub has been removed. Ensure Postgres is available and start the server with `start_sync_server.ps1` or `START_SERVER_SIMPLE.bat`.
- To run the real server manually use: `powershell -NoProfile -ExecutionPolicy Bypass -File sync_prototype/server/start_sync_server.ps1`.
- To run the server manually use: `powershell -NoProfile -ExecutionPolicy Bypass -File sync_prototype/server/start_sync_server.ps1`.

E2E with Docker Compose (recommended for testing):

# Bring up Postgres

docker-compose up -d

# Wait a few seconds then run migrations

export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/elite_bet_sync
psql $DATABASE_URL -f migrations/001_create_tables.sql

# Run tests against the local Postgres (this will tear down the DB service when finished)

npm run test:e2e

Endpoint: POST /sync
Payload: { device_id, last_server_seq, changes: [{ change_id, table, pk, op, payload }] }

Server now implements per-table conflict resolution rules (last-updated where applicable, result merging for bet logs, Best-entry handling for trigger_alerts). The server records applied changes idempotently in `applied_changes` and only writes `server_changes` rows when a change is actually applied (so other clients will only pull meaningful updates).

Response: { applied: [change_id...], server_changes: [...], new_server_seq }
