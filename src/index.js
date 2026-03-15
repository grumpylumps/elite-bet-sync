const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const crypto = require('crypto');
const fs = require('fs');
const https = require('https');
const db = require('./db');
const betGen = require('./bet-generator');
const mlInference = require('./ml-inference');
const mlTraining = require('./ml-training');
const gameCacher = require('./game-cacher');
const teamStatsBuilder = require('./team-stats-builder');

const app = express();

// Configure CORS for web builds - allow all origins in development
const corsOptions = {
  origin: true, // Allow all origins
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Origin', 'X-Requested-With', 'Content-Type', 'Accept', 'Authorization'],
};

app.use(cors(corsOptions));

// Log CORS handling for debugging
app.use((req, res, next) => {
  if (req.method === 'OPTIONS') {
    console.log(`[cors] Handling OPTIONS preflight from ${req.ip} for ${req.path}`);
  }
  next();
});

// Increased limit to handle larger sync payloads
// Can be overridden with BODY_PARSER_LIMIT env var (e.g., '10mb', '50mb')
const bodyLimit = process.env.BODY_PARSER_LIMIT || '50mb';
app.use(bodyParser.json({ limit: bodyLimit }));

// Mapping tables to conflict target columns for upsert
const tableConflictTargets = {
  bet_logs: ['league_id', 'game_id', 'period', 'trigger'],
  user_bets: ['id'],
  elo_ratings: ['league_id', 'team_id'],
  team_stats: ['league_id', 'team_id'],
  game_odds: ['league_id', 'game_id'],
  trigger_alerts: ['league_id', 'game_id', 'period', 'trigger'],
  cached_games: ['league_id', 'game_id'],
  ml_models: ['league_id', 'model_name'],
};

// NOTE: Removed a trivial /health handler that returned a static OK.
// The detailed DB-aware /health handler later in the file performs an actual
// PostgreSQL probe (SELECT 1) and reports 'db: available' or 'unavailable'.
// Keeping only the DB-aware handler ensures health checks reflect DB state.

// Fields that should be treated as timestamps
const timestampFields = [
  'created_at',
  'graded_at',
  'last_updated',
  'captured_at',
  'result_logged_at',
  'timestamp',
];

// Convert Unix timestamp (seconds or milliseconds) to Date object
function convertTimestamp(val) {
  if (!val) return null;
  if (val instanceof Date) return val;
  if (typeof val === 'string') {
    // Try parsing as ISO string first
    const d = new Date(val);
    if (!isNaN(d.getTime())) return d;
  }
  if (typeof val === 'number') {
    // If it looks like seconds (before year 3000), multiply by 1000
    // Unix timestamps in seconds are ~10 digits, milliseconds are ~13 digits
    if (val < 100000000000) {
      return new Date(val * 1000);
    }
    return new Date(val);
  }
  return null;
}

// Derive a primary key string from payload based on table's conflict targets.
// Used by /sync/batch where the client may not send an explicit `pk`.
function derivePkFromPayload(table, payload) {
  if (!payload || typeof payload !== 'object') return null;
  const targets = tableConflictTargets[table];
  // user_bets syncs on uuid, not autoincrement id
  if (table === 'user_bets' && payload.uuid) return payload.uuid;
  if (!targets) {
    return payload.id != null ? String(payload.id) : null;
  }
  if (targets.length === 1) {
    const val = payload[targets[0]];
    return val != null ? String(val) : null;
  }
  // Composite key: build JSON object
  const pkObj = {};
  for (const col of targets) {
    pkObj[col] = payload[col];
  }
  return JSON.stringify(pkObj);
}

function buildUpsertQuery(table, payload, conflictCols) {
  // Convert timestamp fields before building query
  const processedPayload = { ...payload };
  for (const field of timestampFields) {
    if (field in processedPayload) {
      processedPayload[field] = convertTimestamp(processedPayload[field]);
    }
  }

  const cols = Object.keys(processedPayload);
  const vals = cols.map((_, i) => `$${i + 1}`);
  const set = cols.map((c) => `${c} = EXCLUDED.${c}`).join(', ');
  const sql = `INSERT INTO ${table}(${cols.join(',')}) VALUES(${vals.join(',')}) ON CONFLICT(${conflictCols.join(',')}) DO UPDATE SET ${set}`;
  return { sql, params: cols.map((c) => processedPayload[c]) };
}

function parseTs(val) {
  if (!val) return null;
  const d = new Date(val);
  return isNaN(d.getTime()) ? null : d;
}

async function simulateChangeWithRules(client, ch) {
  const { change_id, table, pk, op, payload } = ch;

  if (op === 'delete') {
    // If delete would remove an existing row, report wouldApply=true
    let pkObj = null;
    try {
      pkObj = JSON.parse(pk);
    } catch (e) {}

    if (pkObj && typeof pkObj === 'object') {
      const whereClauses = Object.keys(pkObj).map((k, i) => `${k} = $${i + 1}`);
      const params = Object.keys(pkObj).map((k) => pkObj[k]);
      const r = await client.query(
        `SELECT 1 FROM ${table} WHERE ${whereClauses.join(' AND ')}`,
        params
      );
      return { wouldApply: r.rowCount > 0, reason: r.rowCount > 0 ? 'exists' : 'not_found' };
    } else {
      const r = await client.query(`SELECT 1 FROM ${table} WHERE id = $1`, [pk]);
      return { wouldApply: r.rowCount > 0, reason: r.rowCount > 0 ? 'exists' : 'not_found' };
    }
  }

  // Per-table simulation rules
  switch (table) {
    case 'elo_ratings': {
      const league = payload.league_id;
      const team = payload.team_id;
      const incomingTs = parseTs(payload.last_updated);
      const existing = await client.query(
        'SELECT elo, last_updated FROM elo_ratings WHERE league_id=$1 AND team_id=$2',
        [league, team]
      );
      if (existing.rowCount === 0) return { wouldApply: true, reason: 'insert' };
      const existingTs = parseTs(existing.rows[0].last_updated);
      if (incomingTs && (!existingTs || incomingTs > existingTs))
        return { wouldApply: true, reason: 'newer' };
      return { wouldApply: false, reason: 'stale' };
    }

    case 'team_stats': {
      const league = payload.league_id;
      const team = payload.team_id;
      const existing = await client.query(
        'SELECT games_analyzed, last_updated FROM team_stats WHERE league_id=$1 AND team_id=$2',
        [league, team]
      );
      if (existing.rowCount === 0) return { wouldApply: true, reason: 'insert' };
      const incomingGames = payload.games_analyzed || 0;
      const existingGames = existing.rows[0].games_analyzed || 0;
      if (incomingGames > existingGames) return { wouldApply: true, reason: 'richer' };
      const incomingTs = parseTs(payload.last_updated);
      const existingTs = parseTs(existing.rows[0].last_updated);
      if (incomingTs && (!existingTs || incomingTs > existingTs))
        return { wouldApply: true, reason: 'newer' };
      return { wouldApply: false, reason: 'stale_or_lower_info' };
    }

    case 'game_odds': {
      const league = payload.league_id;
      const gameId = payload.game_id;
      const existing = await client.query(
        'SELECT total_line, last_updated FROM game_odds WHERE league_id=$1 AND game_id=$2',
        [league, gameId]
      );
      if (existing.rowCount === 0) return { wouldApply: true, reason: 'insert' };
      if (payload.total_line != null && payload.total_line !== existing.rows[0].total_line)
        return { wouldApply: true, reason: 'total_line_change' };
      const incomingTs = parseTs(payload.last_updated);
      const existingTs = parseTs(existing.rows[0].last_updated);
      if (incomingTs && (!existingTs || incomingTs > existingTs))
        return { wouldApply: true, reason: 'newer' };
      return { wouldApply: false, reason: 'stale_or_no_change' };
    }

    case 'bet_logs': {
      let pkObj = null;
      try {
        pkObj = JSON.parse(pk);
      } catch (e) {}
      if (!pkObj) return { wouldApply: false, reason: 'invalid_pk' };
      const { league_id, game_id, period, trigger } = pkObj;
      const existing = await client.query(
        'SELECT result, result_logged_at FROM bet_logs WHERE league_id=$1 AND game_id=$2 AND period=$3 AND trigger=$4',
        [league_id, game_id, period, trigger]
      );
      if (existing.rowCount === 0) return { wouldApply: true, reason: 'insert' };
      const incomingResult = payload.result;
      const incomingResultLoggedAt = parseTs(payload.result_logged_at);
      const existingResultLoggedAt = parseTs(existing.rows[0].result_logged_at);
      if (incomingResult != null) {
        if (
          existing.rows[0].result == null ||
          (incomingResultLoggedAt &&
            (!existingResultLoggedAt || incomingResultLoggedAt > existingResultLoggedAt))
        ) {
          return { wouldApply: true, reason: 'new_result' };
        }
      }
      return { wouldApply: false, reason: 'no_significant_change' };
    }

    case 'user_bets': {
      // Use uuid for cross-device sync (uuid is unique across all devices)
      const uuid = payload.uuid || pk;
      const existing = await client.query('SELECT graded_at FROM user_bets WHERE uuid=$1', [uuid]);
      if (existing.rowCount === 0) return { wouldApply: true, reason: 'insert' };
      const incomingGraded = parseTs(payload.graded_at);
      const existingGraded = parseTs(existing.rows[0].graded_at);
      if (incomingGraded && (!existingGraded || incomingGraded > existingGraded))
        return { wouldApply: true, reason: 'newer_grade' };
      return { wouldApply: false, reason: 'stale' };
    }

    case 'trigger_alerts': {
      const pkObj = JSON.parse(pk);
      const { league_id, game_id, period, trigger } = pkObj;
      const existing = await client.query(
        'SELECT timestamp, probability, is_best FROM trigger_alerts WHERE league_id=$1 AND game_id=$2 AND period=$3 AND trigger=$4',
        [league_id, game_id, period, trigger]
      );
      if (existing.rowCount === 0) return { wouldApply: true, reason: 'insert' };
      const incomingTs = parseTs(payload.timestamp);
      const existingTs = parseTs(existing.rows[0].timestamp);
      const existingIsBest = existing.rows[0].is_best === true;
      const incomingIsBest = payload.is_best === true;
      if (incomingIsBest && !existingIsBest) return { wouldApply: true, reason: 'best_replace' };
      if (incomingIsBest && existingIsBest) {
        if ((payload.probability || 0) > (existing.rows[0].probability || 0))
          return { wouldApply: true, reason: 'higher_probability' };
        return { wouldApply: false, reason: 'lower_probability' };
      }
      if (incomingTs && (!existingTs || incomingTs > existingTs))
        return { wouldApply: true, reason: 'newer' };
      return { wouldApply: false, reason: 'stale' };
    }

    case 'ml_models': {
      const { league_id, model_name } = payload;
      const existing = await client.query(
        'SELECT updated_at FROM ml_models WHERE league_id=$1 AND model_name=$2',
        [league_id, model_name]
      );
      if (existing.rowCount === 0) return { wouldApply: true, reason: 'insert' };
      const incomingTs = parseTs(payload.updated_at);
      const existingTs = parseTs(existing.rows[0].updated_at);
      if (incomingTs && (!existingTs || incomingTs > existingTs))
        return { wouldApply: true, reason: 'newer' };
      return { wouldApply: false, reason: 'stale' };
    }

    default: {
      // conservative: if row missing or payload has id not matching existing, report apply
      try {
        const pkObj = pk && pk.startsWith('{') ? JSON.parse(pk) : null;
        if (pkObj) {
          const where = Object.keys(pkObj)
            .map((k) => `${k} = $${Object.keys(pkObj).indexOf(k) + 1}`)
            .join(' AND ');
          const params = Object.keys(pkObj).map((k) => pkObj[k]);
          const r = await client.query(`SELECT 1 FROM ${table} WHERE ${where}`, params);
          return { wouldApply: r.rowCount === 0, reason: r.rowCount === 0 ? 'insert' : 'exists' };
        }
        if (payload && payload.id != null) {
          const r = await client.query(`SELECT 1 FROM ${table} WHERE id = $1`, [payload.id]);
          return { wouldApply: r.rowCount === 0, reason: r.rowCount === 0 ? 'insert' : 'exists' };
        }
      } catch (e) {
        return { wouldApply: true, reason: 'unknown' };
      }
      return { wouldApply: true, reason: 'unknown' };
    }
  }
}

async function applyChangeWithRules(client, ch) {
  const { change_id, table, pk, op, payload } = ch;

  // Idempotency check
  const r = await client.query('SELECT 1 FROM applied_changes WHERE change_id = $1', [change_id]);
  if (r.rowCount > 0) return { applied: false, reason: 'already_applied' };

  if (op === 'delete') {
    let pkObj = null;
    try {
      pkObj = JSON.parse(pk);
    } catch (e) {}

    if (pkObj && typeof pkObj === 'object') {
      const whereClauses = Object.keys(pkObj).map((k, i) => `${k} = $${i + 1}`);
      const params = Object.keys(pkObj).map((k) => pkObj[k]);
      await client.query(`DELETE FROM ${table} WHERE ${whereClauses.join(' AND ')}`, params);
      return { applied: true };
    } else {
      await client.query(`DELETE FROM ${table} WHERE id = $1`, [pk]);
      return { applied: true };
    }
  }

  // Per-table conflict resolution
  switch (table) {
    case 'elo_ratings': {
      // Use last_updated timestamp to decide
      const league = payload.league_id;
      const team = payload.team_id;
      const incomingTs = parseTs(payload.last_updated);
      const existing = await client.query(
        'SELECT elo, last_updated FROM elo_ratings WHERE league_id=$1 AND team_id=$2',
        [league, team]
      );
      if (existing.rowCount === 0) {
        // insert
        const { sql, params } = buildUpsertQuery(table, payload, ['league_id', 'team_id']);
        await client.query(sql, params);
        return { applied: true };
      }
      const existingTs = parseTs(existing.rows[0].last_updated);
      if (incomingTs && (!existingTs || incomingTs > existingTs)) {
        const { sql, params } = buildUpsertQuery(table, payload, ['league_id', 'team_id']);
        await client.query(sql, params);
        return { applied: true };
      }
      // ignore older or equal
      return { applied: false, reason: 'stale' };
    }

    case 'team_stats': {
      const league = payload.league_id;
      const team = payload.team_id;
      const incomingTs = parseTs(payload.last_updated);
      const existing = await client.query(
        'SELECT games_analyzed, wins, losses, ppg, period_avg, last_updated FROM team_stats WHERE league_id=$1 AND team_id=$2',
        [league, team]
      );
      if (existing.rowCount === 0) {
        const { sql, params } = buildUpsertQuery(table, payload, ['league_id', 'team_id']);
        await client.query(sql, params);
        return { applied: true };
      }
      const existingRow = existing.rows[0];
      const existingTs = parseTs(existingRow.last_updated);

      // Field-level merge: if incoming has higher games_analyzed, prefer its aggregate fields
      const incomingGames = payload.games_analyzed || 0;
      const existingGames = existingRow.games_analyzed || 0;
      if (incomingGames > existingGames) {
        const merged = Object.assign({}, existingRow, payload);
        const { sql, params } = buildUpsertQuery(table, merged, ['league_id', 'team_id']);
        await client.query(sql, params);
        return { applied: true };
      }

      // Otherwise, use timestamp if provided
      if (incomingTs && (!existingTs || incomingTs > existingTs)) {
        const { sql, params } = buildUpsertQuery(table, payload, ['league_id', 'team_id']);
        await client.query(sql, params);
        return { applied: true };
      }
      return { applied: false, reason: 'stale_or_lower_info' };
    }

    case 'game_odds': {
      const league = payload.league_id;
      const gameId = payload.game_id;
      const incomingTs = parseTs(payload.last_updated);
      const existing = await client.query(
        'SELECT last_updated, total_line FROM game_odds WHERE league_id=$1 AND game_id=$2',
        [league, gameId]
      );
      if (existing.rowCount === 0) {
        const { sql, params } = buildUpsertQuery(table, payload, ['league_id', 'game_id']);
        await client.query(sql, params);
        return { applied: true };
      }
      const existingRow = existing.rows[0];
      const existingTs = parseTs(existingRow.last_updated);

      // Field-level merge example: if payload has total_line and it's different, prefer the latest non-null total_line
      if (payload.total_line != null && payload.total_line !== existingRow.total_line) {
        const { sql, params } = buildUpsertQuery(table, payload, ['league_id', 'game_id']);
        await client.query(sql, params);
        return { applied: true };
      }

      if (incomingTs && (!existingTs || incomingTs > existingTs)) {
        const { sql, params } = buildUpsertQuery(table, payload, ['league_id', 'game_id']);
        await client.query(sql, params);
        return { applied: true };
      }
      return { applied: false, reason: 'stale_or_no_change' };
    }

    case 'bet_logs': {
      // Insert if not exists. If exists, prefer updates that include 'result' or later result_logged_at.
      let pkObj = null;
      try {
        pkObj = JSON.parse(pk);
      } catch (e) {}
      if (!pkObj) return { applied: false, reason: 'invalid_pk' };
      const { league_id, game_id, period, trigger } = pkObj;
      const existing = await client.query(
        'SELECT result, result_logged_at, captured_at FROM bet_logs WHERE league_id=$1 AND game_id=$2 AND period=$3 AND trigger=$4',
        [league_id, game_id, period, trigger]
      );
      if (existing.rowCount === 0) {
        const { sql, params } = buildUpsertQuery(table, payload, [
          'league_id',
          'game_id',
          'period',
          'trigger',
        ]);
        await client.query(sql, params);
        return { applied: true };
      }

      // if incoming has result and either existing result is null or incoming result_logged_at is newer
      const incomingResult = payload.result;
      const incomingResultLoggedAt = parseTs(payload.result_logged_at);
      const existingResult = existing.rows[0].result;
      const existingResultLoggedAt = parseTs(existing.rows[0].result_logged_at);

      if (incomingResult != null) {
        if (
          existingResult == null ||
          (incomingResultLoggedAt &&
            (!existingResultLoggedAt || incomingResultLoggedAt > existingResultLoggedAt))
        ) {
          const { sql, params } = buildUpsertQuery(table, payload, [
            'league_id',
            'game_id',
            'period',
            'trigger',
          ]);
          await client.query(sql, params);
          return { applied: true };
        }
      }

      // Otherwise ignore
      return { applied: false, reason: 'no_significant_change' };
    }

    case 'user_bets': {
      // Use uuid for cross-device sync (uuid is unique across all devices)
      const uuid = payload.uuid || pk;
      const existing = await client.query('SELECT graded_at FROM user_bets WHERE uuid=$1', [uuid]);
      if (existing.rowCount === 0) {
        const { sql, params } = buildUpsertQuery(table, payload, ['uuid']);
        await client.query(sql, params);
        return { applied: true };
      }
      const incomingGraded = parseTs(payload.graded_at);
      const existingGraded = parseTs(existing.rows[0].graded_at);
      if (incomingGraded && (!existingGraded || incomingGraded > existingGraded)) {
        const { sql, params } = buildUpsertQuery(table, payload, ['uuid']);
        await client.query(sql, params);
        return { applied: true };
      }
      return { applied: false, reason: 'stale' };
    }

    case 'trigger_alerts': {
      // Prefer newer timestamp for alerts
      const pkObj = JSON.parse(pk);
      const { league_id, game_id, period, trigger } = pkObj;
      const existing = await client.query(
        'SELECT timestamp, probability, is_best FROM trigger_alerts WHERE league_id=$1 AND game_id=$2 AND period=$3 AND trigger=$4',
        [league_id, game_id, period, trigger]
      );
      const incomingTs = parseTs(payload.timestamp);
      if (existing.rowCount === 0) {
        const { sql, params } = buildUpsertQuery(table, payload, [
          'league_id',
          'game_id',
          'period',
          'trigger',
        ]);
        await client.query(sql, params);
        return { applied: true };
      }
      const existingTs = parseTs(existing.rows[0].timestamp);
      const existingIsBest = existing.rows[0].is_best === true;
      const incomingIsBest = payload.is_best === true;

      // If incoming is a Best entry and existing isn't, replace. If both are Best prefer higher probability.
      if (incomingIsBest && !existingIsBest) {
        const { sql, params } = buildUpsertQuery(table, payload, [
          'league_id',
          'game_id',
          'period',
          'trigger',
        ]);
        await client.query(sql, params);
        return { applied: true };
      }
      if (incomingIsBest && existingIsBest) {
        if ((payload.probability || 0) > (existing.rows[0].probability || 0)) {
          const { sql, params } = buildUpsertQuery(table, payload, [
            'league_id',
            'game_id',
            'period',
            'trigger',
          ]);
          await client.query(sql, params);
          return { applied: true };
        }
        return { applied: false, reason: 'lower_probability' };
      }

      if (incomingTs && (!existingTs || incomingTs > existingTs)) {
        const { sql, params } = buildUpsertQuery(table, payload, [
          'league_id',
          'game_id',
          'period',
          'trigger',
        ]);
        await client.query(sql, params);
        return { applied: true };
      }
      return { applied: false, reason: 'stale' };
    }

    case 'ml_models': {
      const { league_id, model_name } = payload;
      const incomingTs = parseTs(payload.updated_at);
      const existing = await client.query(
        'SELECT updated_at FROM ml_models WHERE league_id=$1 AND model_name=$2',
        [league_id, model_name]
      );
      if (
        existing.rowCount === 0 ||
        !existing.rows[0].updated_at ||
        (incomingTs && incomingTs > parseTs(existing.rows[0].updated_at))
      ) {
        const { sql, params } = buildUpsertQuery(table, payload, ['league_id', 'model_name']);
        await client.query(sql, params);
        return { applied: true };
      }
      return { applied: false, reason: 'stale' };
    }

    default: {
      // Generic upsert
      const conflictCols = tableConflictTargets[table] || ['id'];
      const { sql, params } = buildUpsertQuery(table, payload, conflictCols);
      await client.query(sql, params);
      return { applied: true };
    }
  }
}

const jwt = require('jsonwebtoken');
const { Pool: PgPool } = require('pg');

// ── Auth-DB connection (read-only, for session-version checks) ──────────
// Connects to the auth server's database to validate that the JWT's
// session_version (`sv`) matches the current value on the user row.
// This enforces single-device login at the sync layer.
let authDb = null;
(() => {
  const host = process.env.AUTH_DB_HOST || process.env.DB_HOST || 'localhost';
  const port = process.env.AUTH_DB_PORT || process.env.DB_PORT || '5432';
  const user = process.env.AUTH_DB_USER || process.env.DB_USER || 'postgres';
  const pass = process.env.AUTH_DB_PASSWORD || process.env.DB_PASSWORD || 'postgres';
  const name = process.env.AUTH_DB_NAME || 'elite_bet_auth';
  const url = `postgresql://${user}:${encodeURIComponent(pass)}@${host}:${port}/${name}`;
  try {
    authDb = new PgPool({
      connectionString: url,
      max: 3,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
      ssl: (host === 'localhost' || host === '127.0.0.1') ? false : { rejectUnauthorized: false },
    });
    console.log('[auth-db] Pool created for session-version checks');
  } catch (e) {
    console.warn('[auth-db] Failed to create pool:', e.message);
  }
})();

function requireAuth(req, res, next) {
  const staticToken = process.env.SYNC_API_TOKEN;
  const auth = req.headers['authorization'];

  if (!auth || !auth.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const provided = auth.split(' ')[1];

  // If a static token is configured, it takes priority: accept on match,
  // reject on mismatch (no JWT fallback when static token is active).
  if (staticToken) {
    return provided === staticToken
      ? next()
      : res.status(401).json({ error: 'unauthorized' });
  }

  // No static token — validate as a user JWT signed by the auth server.
  const jwtSecret = process.env.AUTH_JWT_SECRET;
  if (!jwtSecret) {
    console.warn('AUTH_JWT_SECRET not set; accepting any Bearer token');
    return next();
  }

  try {
    const decoded = jwt.verify(provided, jwtSecret);
    req.user = { email: decoded.email || '', userId: decoded.sub || '' };

    // Validate session_version against the auth DB to enforce single-device
    // login. If the auth DB is unavailable, allow the request (fail-open for
    // sync availability — the auth server still enforces on its endpoints).
    const tokenSv = parseInt(decoded.sv || '0', 10);
    const userId = decoded.sub;
    if (authDb && userId) {
      authDb.query(
        'SELECT session_version FROM users WHERE id = $1',
        [userId],
      ).then(result => {
        if (result.rows.length === 0) {
          return res.status(401).json({ error: 'user not found' });
        }
        const dbSv = result.rows[0].session_version || 0;
        if (tokenSv !== dbSv) {
          return res.status(401).json({ error: 'Session superseded by another login' });
        }
        return next();
      }).catch(err => {
        // Auth DB query failed — fail-open to preserve sync availability
        console.warn('[auth-db] session check failed, allowing request:', err.message);
        return next();
      });
    } else {
      return next();
    }
  } catch (e) {
    return res.status(401).json({ error: 'unauthorized' });
  }
}

app.post('/sync', requireAuth, async (req, res) => {
  console.log(
    `[sync] Received POST /sync from ${req.ip} with ${req.body.changes?.length || 0} changes`
  );
  const { device_id, last_server_seq = 0, changes = [] } = req.body;

  let client = null;
  try {
    client = await db.getClient();
    await client.query('BEGIN');

    for (const ch of changes) {
      const { change_id } = ch;
      const result = await applyChangeWithRules(client, ch);

      // Record applied_changes regardless to guarantee idempotency across repeated syncs
      await client.query(
        'INSERT INTO applied_changes(change_id) VALUES($1) ON CONFLICT (change_id) DO NOTHING',
        [change_id]
      );

      // If actually applied, insert into server_changes so other clients can pick it up
      if (result.applied) {
        await client.query(
          'INSERT INTO server_changes(table_name, pk, op, payload, change_id) VALUES($1,$2,$3,$4,$5)',
          [ch.table, ch.pk, ch.op, ch.payload ? JSON.stringify(ch.payload) : null, change_id]
        );
      }
    }

    // Track device last-seen (fire-and-forget, non-blocking)
    if (device_id) {
      client.query(
        `INSERT INTO device_sessions (device_id, last_seen_at, last_server_seq, ip)
         VALUES ($1, NOW(), $2, $3)
         ON CONFLICT (device_id) DO UPDATE
           SET last_seen_at = NOW(),
               last_server_seq = EXCLUDED.last_server_seq,
               ip = EXCLUDED.ip`,
        [device_id, last_server_seq, req.ip]
      ).catch(() => {}); // ignore errors — this is observability only
    }

    // fetch server changes since last_server_seq (paginated)
    // Filter user_bets to only return the authenticated user's bets.
    const pullLimit = Math.min(Math.max(parseInt(req.body.pull_limit) || 500, 1), 50000);
    const userEmail = req.user?.email || '';
    const srv = await client.query(
      `SELECT server_seq, table_name, pk, op, payload, change_id
       FROM server_changes
       WHERE server_seq > $1
         AND (table_name != 'user_bets' OR payload->>'user_email' = $3 OR payload->>'user_email' IS NULL)
       ORDER BY server_seq ASC LIMIT $2`,
      [last_server_seq, pullLimit, userEmail]
    );
    await client.query('COMMIT');

    const hasMore = srv.rows.length === pullLimit;
    res.json({
      applied: changes.map((c) => c.change_id),
      server_changes: srv.rows,
      new_server_seq: srv.rows.length ? srv.rows[srv.rows.length - 1].server_seq : last_server_seq,
      has_more: hasMore,
    });
  } catch (err) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (e) {
        /* ignore rollback errors */
      }
    }
    console.error('Sync failed', err);
    res.status(500).json({ error: err.message });
  } finally {
    if (client) {
      try {
        client.release();
      } catch (e) {
        /* ignore release errors */
      }
    }
  }
});

// Dry-run endpoint: simulate applying changes without persisting
app.post('/sync/dryrun', requireAuth, async (req, res) => {
  const { changes = [] } = req.body;
  let client = null;
  try {
    client = await db.getClient();
    const results = [];
    for (const ch of changes) {
      const sim = await simulateChangeWithRules(client, ch);
      results.push({
        change_id: ch.change_id,
        table: ch.table,
        would_apply: sim.wouldApply,
        reason: sim.reason,
      });
    }
    res.json({ results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    if (client) {
      try {
        client.release();
      } catch (e) {
        /* ignore release errors */
      }
    }
  }
});

// Batch sync endpoint: accepts the { operations: [...] } format used by the
// client's count-based batching path and 404-fallback path.
app.post('/sync/batch', requireAuth, async (req, res) => {
  const { operations = [] } = req.body;
  console.log(
    `[sync/batch] Received POST /sync/batch from ${req.ip} with ${operations.length} operations`
  );

  let client = null;
  try {
    client = await db.getClient();
    await client.query('BEGIN');

    const results = [];

    for (const op of operations) {
      const table = op.table;
      const opType = (op.operation || 'INSERT').toLowerCase();
      const payload = op.data || {};
      const changeId = op.change_id || crypto.randomUUID();
      const pk = op.pk || derivePkFromPayload(table, payload);

      const ch = { change_id: changeId, table, pk, op: opType, payload };
      const result = await applyChangeWithRules(client, ch);

      await client.query(
        'INSERT INTO applied_changes(change_id) VALUES($1) ON CONFLICT (change_id) DO NOTHING',
        [changeId]
      );

      if (result.applied) {
        await client.query(
          'INSERT INTO server_changes(table_name, pk, op, payload, change_id) VALUES($1,$2,$3,$4,$5)',
          [table, pk, opType, JSON.stringify(payload), changeId]
        );
      }

      results.push({
        change_id: changeId,
        table,
        applied: result.applied,
        reason: result.reason || null,
      });
    }

    // Get latest server_seq for response
    const seqResult = await client.query('SELECT MAX(server_seq) as max_seq FROM server_changes');
    const newServerSeq = seqResult.rows[0]?.max_seq || 0;

    await client.query('COMMIT');

    res.json({
      results,
      new_server_seq: newServerSeq,
    });
  } catch (err) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (e) {
        /* ignore */
      }
    }
    console.error('Batch sync failed', err);
    res.status(500).json({ error: err.message });
  } finally {
    if (client) {
      try {
        client.release();
      } catch (e) {
        /* ignore */
      }
    }
  }
});

// Health endpoint for basic checks
app.get('/health', async (req, res) => {
  try {
    const r = await db.query('SELECT 1');
    const dbStatus = r && typeof r.rowCount === 'number' ? 'available' : 'unavailable';
    res.json({ status: 'ok', db: dbStatus });
  } catch (e) {
    res.json({ status: 'ok', db: 'unavailable', error: e.message });
  }
});

// Alternative health endpoint (used by some clients)
app.get('/_health', async (req, res) => {
  res.json({ status: 'ok' });
});

// ---------------------------------------------------------------------------
// APK download — no auth required
// ---------------------------------------------------------------------------
const APK_PATH = require('path').join(__dirname, '..', 'downloads', 'elite-bet.apk');

app.get('/downloads/elite-bet.apk', (req, res) => {
  if (!fs.existsSync(APK_PATH)) {
    return res.status(404).json({ error: 'APK not available yet.' });
  }
  res.setHeader('Content-Type', 'application/vnd.android.package-archive');
  res.setHeader('Content-Disposition', 'attachment; filename="elite-bet.apk"');
  fs.createReadStream(APK_PATH).pipe(res);
});

const CHECKSUM_PATH = APK_PATH + '.sha256';

app.get('/downloads/elite-bet.apk.sha256', (req, res) => {
  if (!fs.existsSync(CHECKSUM_PATH)) {
    return res.status(404).json({ error: 'Checksum not available.' });
  }
  res.setHeader('Content-Type', 'text/plain');
  res.send(fs.readFileSync(CHECKSUM_PATH, 'utf8').trim());
});

// ---------------------------------------------------------------------------
// Bet logs endpoint — returns system bet log history for a league
// ---------------------------------------------------------------------------
app.get('/api/bet-logs/:league', async (req, res) => {
  const { league } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 100, 5000);
  const since = req.query.since; // ISO timestamp — only return rows newer than this
  try {
    let query;
    let params;
    if (since) {
      query = `SELECT league_id, game_id, period, trigger, line, proj, edge, probability,
              direction, captured_at, capture_type, actual, result, result_logged_at,
              stake, home_team, away_team
       FROM bet_logs
       WHERE league_id = $1 AND captured_at > $2
       ORDER BY captured_at DESC
       LIMIT $3`;
      params = [league, since, limit];
    } else {
      query = `SELECT league_id, game_id, period, trigger, line, proj, edge, probability,
              direction, captured_at, capture_type, actual, result, result_logged_at,
              stake, home_team, away_team
       FROM bet_logs
       WHERE league_id = $1
       ORDER BY captured_at DESC
       LIMIT $2`;
      params = [league, limit];
    }
    const result = await db.query(query, params);
    res.json(result.rows);
  } catch (e) {
    console.error('[bet-logs] query error:', e.message);
    res.status(500).json({ error: 'Failed to fetch bet logs' });
  }
});

// ---------------------------------------------------------------------------
// User bets endpoint — returns a user's personal bet history for a league
// Requires ?email=user@example.com query param
// ---------------------------------------------------------------------------
app.get('/api/user-bets/:league', async (req, res) => {
  const { league } = req.params;
  const { email } = req.query;
  if (!email) {
    return res.status(400).json({ error: 'email query param required' });
  }
  const limit = Math.min(parseInt(req.query.limit) || 500, 2000);
  try {
    const result = await db.query(
      `SELECT uuid, league_id, game_id, period, home_team, away_team,
              current_total, proj_total, amount, direction, line, clock,
              bet_type, scope, actual_total, result, profit_loss, created_at, graded_at
       FROM user_bets
       WHERE league_id = $1 AND user_email = $2
       ORDER BY created_at DESC
       LIMIT $3`,
      [league, email, limit]
    );
    res.json(result.rows);
  } catch (e) {
    console.error('[user-bets] query error:', e.message);
    res.status(500).json({ error: 'Failed to fetch user bets' });
  }
});

// ---------------------------------------------------------------------------
// Create a user bet — inserts a new row into user_bets
// Requires user_email in the JSON body for ownership
// ---------------------------------------------------------------------------
app.post('/api/user-bets', async (req, res) => {
  const {
    uuid, league_id, game_id, period, home_team, away_team,
    current_total, proj_total, amount, direction, line, clock,
    bet_type, scope, actual_total, result, profit_loss,
    created_at, graded_at, user_email,
  } = req.body;

  if (!user_email) {
    return res.status(400).json({ error: 'user_email is required' });
  }
  if (!uuid || !league_id || !game_id || period == null || current_total == null || amount == null || !direction || line == null) {
    return res.status(400).json({ error: 'Missing required fields: uuid, league_id, game_id, period, current_total, amount, direction, line' });
  }

  try {
    const { rows } = await db.query(
      `INSERT INTO user_bets
         (uuid, league_id, game_id, period, home_team, away_team,
          current_total, proj_total, amount, direction, line, clock,
          bet_type, scope, actual_total, result, profit_loss,
          created_at, graded_at, user_email)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
       RETURNING *`,
      [
        uuid, league_id, game_id, period, home_team || null, away_team || null,
        current_total, proj_total || null, amount, direction, line, clock || null,
        bet_type || null, scope || null, actual_total || null, result || null, profit_loss || null,
        created_at || new Date().toISOString(), graded_at || null, user_email,
      ]
    );
    res.status(201).json(rows[0]);
  } catch (e) {
    console.error('[user-bets] insert error:', e.message);
    if (e.code === '23505') {
      return res.status(409).json({ error: 'A bet with this UUID already exists' });
    }
    res.status(500).json({ error: 'Failed to create user bet' });
  }
});

// ---------------------------------------------------------------------------
// Grade a user bet — update actual_total, result, profit_loss by UUID
// Requires user_email in the JSON body for ownership verification
// ---------------------------------------------------------------------------
app.patch('/api/user-bets/:uuid/grade', async (req, res) => {
  const { uuid } = req.params;
  const { actual_total, result, profit_loss, user_email } = req.body;

  if (!user_email) {
    return res.status(400).json({ error: 'user_email is required' });
  }
  if (actual_total == null || !result || profit_loss == null) {
    return res.status(400).json({ error: 'Missing required fields: actual_total, result, profit_loss' });
  }

  try {
    const { rows, rowCount } = await db.query(
      `UPDATE user_bets
       SET actual_total = $1, result = $2, profit_loss = $3, graded_at = now()
       WHERE uuid = $4 AND user_email = $5
       RETURNING *`,
      [actual_total, result, profit_loss, uuid, user_email]
    );
    if (rowCount === 0) {
      return res.status(404).json({ error: 'User bet not found or does not belong to this user' });
    }
    res.json(rows[0]);
  } catch (e) {
    console.error('[user-bets] grade error:', e.message);
    res.status(500).json({ error: 'Failed to grade user bet' });
  }
});

// ---------------------------------------------------------------------------
// ML models endpoint — returns stored model weights for a league
// ---------------------------------------------------------------------------
app.get('/api/ml-models/:league', async (req, res) => {
  const { league } = req.params;
  try {
    const result = await db.query(
      `SELECT model_name AS "modelName", metadata, updated_at
       FROM ml_models
       WHERE league_id = $1
       ORDER BY model_name`,
      [league]
    );
    res.json(result.rows);
  } catch (e) {
    console.error('[ml-models] query error:', e.message);
    res.status(500).json({ error: 'Failed to fetch ML models' });
  }
});

// ---------------------------------------------------------------------------
// Elo ratings endpoint — returns all team Elo ratings for a league
// ---------------------------------------------------------------------------
app.get('/api/elo-ratings/:league', async (req, res) => {
  const { league } = req.params;
  try {
    const result = await db.query(
      `SELECT league_id, team_id, team_name, elo, last_updated
       FROM elo_ratings
       WHERE league_id = $1
       ORDER BY elo DESC`,
      [league]
    );
    res.json(result.rows);
  } catch (e) {
    console.error('[elo-ratings] query error:', e.message);
    res.status(500).json({ error: 'Failed to fetch elo ratings' });
  }
});

// ---------------------------------------------------------------------------
// Game odds endpoint — returns odds for all games (or filtered) in a league
// ---------------------------------------------------------------------------
app.get('/api/game-odds/:league', async (req, res) => {
  const { league } = req.params;
  const gameIdsParam = req.query.game_ids; // optional comma-separated game IDs
  try {
    let result;
    if (gameIdsParam) {
      const gameIds = gameIdsParam.split(',').map(id => id.trim()).filter(Boolean);
      result = await db.query(
        `SELECT league_id, game_id, over_odds, under_odds, total_line, bookmaker,
                spread_home, spread_away, moneyline_home, moneyline_away, last_updated
         FROM game_odds
         WHERE league_id = $1 AND game_id = ANY($2)
         ORDER BY last_updated DESC`,
        [league, gameIds]
      );
    } else {
      result = await db.query(
        `SELECT league_id, game_id, over_odds, under_odds, total_line, bookmaker,
                spread_home, spread_away, moneyline_home, moneyline_away, last_updated
         FROM game_odds
         WHERE league_id = $1
         ORDER BY last_updated DESC
         LIMIT 500`,
        [league]
      );
    }
    res.json(result.rows);
  } catch (e) {
    console.error('[game-odds] query error:', e.message);
    res.status(500).json({ error: 'Failed to fetch game odds' });
  }
});

// ---------------------------------------------------------------------------
// ML prediction endpoints — Ridge Regression inference on stored weights
// ---------------------------------------------------------------------------
app.post('/predict/ingame', async (req, res) => {
  try {
    const result = await mlInference.predictIngame(req.body);
    if (!result) {
      return res.status(404).json({ error: 'No ML weights available', used_ml_model: false });
    }
    res.json(result);
  } catch (e) {
    console.error('[predict/ingame] error:', e.message);
    res.status(500).json({ error: 'Prediction failed', used_ml_model: false });
  }
});

app.post('/predict/pregame', async (req, res) => {
  try {
    const result = await mlInference.predictPregame(req.body);
    if (!result) {
      return res.status(404).json({ error: 'No ML weights or team data available', used_ml_model: false });
    }
    res.json(result);
  } catch (e) {
    console.error('[predict/pregame] error:', e.message);
    res.status(500).json({ error: 'Prediction failed', used_ml_model: false });
  }
});

// ---------------------------------------------------------------------------
// ML Training admin endpoints
// ---------------------------------------------------------------------------

const ML_ALL_LEAGUES = ['nba', 'wnba', 'ncaam', 'ncaaw', 'nfl', 'cfb', 'mlb', 'cbb', 'nhl'];

app.post('/admin/train/:league', async (req, res) => {
  const league = req.params.league;
  if (!ML_ALL_LEAGUES.includes(league)) {
    return res.status(400).json({ error: `Unknown league: ${league}` });
  }
  try {
    console.log(`[admin] Training triggered for ${league}`);
    const results = await mlTraining.trainAll(db, league);
    res.json({ league, status: 'completed', results });
  } catch (e) {
    console.error(`[admin] Training failed for ${league}:`, e.message);
    res.status(500).json({ error: 'Training failed', message: e.message });
  }
});

app.post('/admin/train-all', async (req, res) => {
  try {
    console.log('[admin] Full training triggered for all leagues');
    const results = {};
    for (const league of ML_ALL_LEAGUES) {
      try {
        results[league] = await mlTraining.trainAll(db, league);
      } catch (e) {
        console.error(`[admin] Training failed for ${league}:`, e.message);
        results[league] = { error: e.message };
      }
    }
    res.json({ status: 'completed', results });
  } catch (e) {
    console.error('[admin] Full training failed:', e.message);
    res.status(500).json({ error: 'Training failed', message: e.message });
  }
});

app.get('/admin/training-status', async (req, res) => {
  try {
    const result = await db.query(
      `SELECT league_id, model_name, samples_used, metrics, completed_at, status
       FROM ml_training_runs
       ORDER BY completed_at DESC
       LIMIT 100`
    );
    res.json({ runs: result.rows });
  } catch (e) {
    // If table doesn't exist yet, return empty
    if (e.message.includes('does not exist')) {
      return res.json({ runs: [], note: 'ml_training_runs table not yet created; run migration 008' });
    }
    console.error('[admin] training-status error:', e.message);
    res.status(500).json({ error: 'Failed to fetch training status' });
  }
});

// ---------------------------------------------------------------------------
// Game cacher admin endpoints
// ---------------------------------------------------------------------------

app.post('/admin/backfill-games', async (req, res) => {
  const days = parseInt(req.query.days || '90', 10);
  try {
    console.log(`[admin] Game backfill triggered (${days} days)`);
    const total = await gameCacher.backfill(db, days);
    res.json({ status: 'completed', days, games_cached: total });
  } catch (e) {
    console.error('[admin] Game backfill failed:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('/admin/game-cache-status', async (req, res) => {
  try {
    const result = await db.query(`
      SELECT league_id, status, COUNT(*) AS cnt
      FROM cached_games
      GROUP BY league_id, status
      ORDER BY league_id, status
    `);
    const total = await db.query('SELECT COUNT(*) AS cnt FROM cached_games');
    res.json({ total: parseInt(total.rows[0].cnt, 10), breakdown: result.rows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/admin/rebuild-team-stats', async (req, res) => {
  try {
    const total = await teamStatsBuilder.buildAll(db);
    res.json({ status: 'completed', teams_updated: total });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/admin/team-stats-status', async (req, res) => {
  try {
    const result = await db.query(`
      SELECT league_id, COUNT(*) AS teams, SUM(games_analyzed) AS total_games,
             ROUND(AVG(ppg)::numeric, 1) AS avg_ppg
      FROM team_stats
      GROUP BY league_id
      ORDER BY league_id
    `);
    const total = await db.query('SELECT COUNT(*) AS cnt FROM team_stats');
    res.json({ total: parseInt(total.rows[0].cnt, 10), breakdown: result.rows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// ESPN proxy routes (server fetches ESPN, caches, and serves to clients)
// ---------------------------------------------------------------------------

const ESPN_BASE = 'https://site.api.espn.com/apis/site/v2/sports';
const ESPN_CORE = 'https://sports.core.api.espn.com/v2/sports';

const LEAGUE_PATHS = {
  nba: 'basketball/nba',
  wnba: 'basketball/wnba',
  ncaam: 'basketball/mens-college-basketball',
  ncaaw: 'basketball/womens-college-basketball',
  nfl: 'football/nfl',
  cfb: 'football/college-football',
  mlb: 'baseball/mlb',
  cbb: 'baseball/college-baseball',
  nhl: 'hockey/nhl',
};

const LEAGUE_GROUPS = { ncaam: '50', ncaaw: '50', cfb: '80' };
const VALID_ESPN_LEAGUES = new Set(Object.keys(LEAGUE_PATHS));

// In-memory TTL cache for ESPN responses (bounded to prevent leaks)
const _espnCache = new Map(); // key -> { data, expiresAt }
const ESPN_CACHE_MAX = 500;

function espnCacheGet(key) {
  const entry = _espnCache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) { _espnCache.delete(key); return null; }
  return entry.data;
}
function espnCacheSet(key, data, ttlSeconds) {
  // Evict expired entries when approaching the limit
  if (_espnCache.size >= ESPN_CACHE_MAX) {
    const now = Date.now();
    for (const [k, v] of _espnCache) {
      if (now > v.expiresAt) _espnCache.delete(k);
    }
    // If still over limit, drop oldest entries
    if (_espnCache.size >= ESPN_CACHE_MAX) {
      const excess = _espnCache.size - ESPN_CACHE_MAX + 1;
      const iter = _espnCache.keys();
      for (let i = 0; i < excess; i++) _espnCache.delete(iter.next().value);
    }
  }
  _espnCache.set(key, { data, expiresAt: Date.now() + ttlSeconds * 1000 });
}

// HTTPS GET helper using Node built-in
function espnFetch(url) {
  return new Promise((resolve, reject) => {
    const opts = { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept-Encoding': 'gzip, deflate' } };
    https.get(url, opts, (res) => {
      const encoding = (res.headers['content-encoding'] || '').toLowerCase();
      let stream = res;
      if (encoding === 'gzip') {
        const zlib = require('zlib');
        stream = res.pipe(zlib.createGunzip());
      } else if (encoding === 'deflate') {
        const zlib = require('zlib');
        stream = res.pipe(zlib.createInflate());
      }
      const chunks = [];
      stream.on('data', (chunk) => chunks.push(chunk));
      stream.on('end', () => {
        try {
          const body = Buffer.concat(chunks).toString('utf8');
          resolve({ status: res.statusCode, data: JSON.parse(body) });
        } catch (e) { reject(new Error(`ESPN JSON parse error: ${e.message}`)); }
      });
      stream.on('error', reject);
    }).on('error', reject);
  });
}

// URL builders
function espnScoreboardUrl(league, date) {
  const path = LEAGUE_PATHS[league];
  const today = date || new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const groups = LEAGUE_GROUPS[league] ? `&groups=${LEAGUE_GROUPS[league]}` : '';
  return `${ESPN_BASE}/${path}/scoreboard?limit=1000&dates=${today}${groups}`;
}
function espnSummaryUrl(league, gameId) {
  return `${ESPN_BASE}/${LEAGUE_PATHS[league]}/summary?event=${gameId}`;
}
function espnOddsUrl(league, gameId) {
  return `${ESPN_BASE}/${LEAGUE_PATHS[league]}/events/${gameId}/odds`;
}
function espnTeamScheduleUrl(league, teamId, seasonType) {
  const qs = seasonType ? `?seasontype=${seasonType}` : '';
  return `${ESPN_BASE}/${LEAGUE_PATHS[league]}/teams/${teamId}/schedule${qs}`;
}
function espnTeamInfoUrl(league, teamId) {
  const [sport, leagueName] = LEAGUE_PATHS[league].split('/');
  return `${ESPN_CORE}/${sport}/leagues/${leagueName}/teams/${teamId}`;
}

// Background polling state
const _activeLiveGames = {}; // league -> Set<gameId>
const _liveGameData = {};    // league -> { gameId: summaryJson }
let _espnPollingActive = false;

function _extractLiveGameIds(scoreboardJson) {
  const ids = new Set();
  for (const event of (scoreboardJson.events || [])) {
    if (event?.status?.type?.state === 'in') ids.add(String(event.id));
  }
  return ids;
}

async function _pollScoreboards() {
  while (_espnPollingActive) {
    for (const league of VALID_ESPN_LEAGUES) {
      try {
        const { status, data } = await espnFetch(espnScoreboardUrl(league));
        if (status === 200) {
          espnCacheSet(`scoreboard:${league}:today`, data, 30);
          _activeLiveGames[league] = _extractLiveGameIds(data);
          if (_activeLiveGames[league].size > 0)
            console.log(`[ESPN] ${league.toUpperCase()}: ${_activeLiveGames[league].size} live games`);
        }
      } catch (e) {
        console.error(`[ESPN] Scoreboard poll error (${league}):`, e.message);
      }
    }
    await new Promise((r) => setTimeout(r, 15000));
  }
}

async function _pollLiveGames() {
  while (_espnPollingActive) {
    let hasLive = false;
    for (const [league, gameIds] of Object.entries(_activeLiveGames)) {
      for (const gameId of [...gameIds]) {
        hasLive = true;
        try {
          const { status, data } = await espnFetch(espnSummaryUrl(league, gameId));
          if (status === 200) {
            espnCacheSet(`summary:${league}:${gameId}`, data, 15);
            (_liveGameData[league] = _liveGameData[league] || {})[gameId] = data;
            const comp = (data?.header?.competitions || [])[0];
            if (comp?.status?.type?.completed) {
              gameIds.delete(gameId);
              if (_liveGameData[league]) delete _liveGameData[league][gameId];
              // Grade bets for completed game
              betGen.gradeCompletedGame(league, gameId, data).catch((e) =>
                console.error(`[bet-gen] Grade error (${league}/${gameId}):`, e.message)
              );
            } else {
              // Process live game for bet generation
              betGen.processLiveGame(league, gameId, data).catch((e) =>
                console.error(`[bet-gen] Process error (${league}/${gameId}):`, e.message)
              );
            }
          }
        } catch (e) {
          console.error(`[ESPN] Live poll error (${league}/${gameId}):`, e.message);
        }
        await new Promise((r) => setTimeout(r, 500));
      }
    }
    // Cleanup fired triggers for games no longer live
    betGen.cleanupFiredTriggers(_activeLiveGames);

    // Grade any pending bets whose games have since completed (mirrors Flutter's gradeAllPendingBets)
    betGen.gradeAllPendingBets((league, gameId) => espnFetch(espnSummaryUrl(league, gameId)))
      .catch((e) => console.error('[bet-gen] gradeAllPendingBets error:', e.message));
    await new Promise((r) => setTimeout(r, hasLive ? 1000 : 5000));
  }
}

// Batch game odds from Postgres (fallback when ESPN strips odds for live/final games)
app.post('/api/game-odds/batch', requireAuth, async (req, res) => {
  const { league_id, game_ids } = req.body;
  if (!league_id || !Array.isArray(game_ids) || game_ids.length === 0) {
    return res.status(400).json({ error: 'league_id and game_ids[] required' });
  }
  if (game_ids.length > 50) {
    return res.status(400).json({ error: 'max 50 game_ids per request' });
  }
  try {
    const placeholders = game_ids.map((_, i) => `$${i + 2}`).join(',');
    const { rows } = await db.query(
      `SELECT game_id, total_line, spread_home, spread_away, moneyline_home, moneyline_away, over_odds, under_odds
       FROM game_odds WHERE league_id=$1 AND game_id IN (${placeholders})`,
      [league_id, ...game_ids]
    );
    const result = {};
    for (const row of rows) {
      result[row.game_id] = {
        total_line: row.total_line != null ? parseFloat(row.total_line) : null,
        spread_home: row.spread_home,
        spread_away: row.spread_away,
        moneyline_home: row.moneyline_home != null ? parseFloat(row.moneyline_home) : null,
        moneyline_away: row.moneyline_away != null ? parseFloat(row.moneyline_away) : null,
        over_odds: row.over_odds != null ? parseFloat(row.over_odds) : null,
        under_odds: row.under_odds != null ? parseFloat(row.under_odds) : null,
      };
    }
    res.json(result);
  } catch (e) {
    console.error('[game-odds-batch] error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ESPN routes
app.get('/espn/:league/scoreboard', async (req, res) => {
  const { league } = req.params;
  const { date } = req.query;
  if (!VALID_ESPN_LEAGUES.has(league)) return res.status(400).json({ error: `Unknown league: ${league}` });
  const cacheKey = `scoreboard:${league}:${date || 'today'}`;
  const cached = espnCacheGet(cacheKey);
  if (cached) return res.json(cached);
  try {
    const { status, data } = await espnFetch(espnScoreboardUrl(league, date));
    if (status !== 200) return res.status(status).json({ error: 'ESPN API error' });
    espnCacheSet(cacheKey, data, 10);
    res.json(data);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.get('/espn/:league/games/:gameId/summary', async (req, res) => {
  const { league, gameId } = req.params;
  if (!VALID_ESPN_LEAGUES.has(league)) return res.status(400).json({ error: `Unknown league: ${league}` });
  const cacheKey = `summary:${league}:${gameId}`;
  const cached = espnCacheGet(cacheKey);
  if (cached) return res.json(cached);
  try {
    const { status, data } = await espnFetch(espnSummaryUrl(league, gameId));
    if (status !== 200) return res.status(status).json({ error: 'ESPN API error' });
    espnCacheSet(cacheKey, data, 5);
    res.json(data);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.get('/espn/:league/games/:gameId/odds', async (req, res) => {
  const { league, gameId } = req.params;
  if (!VALID_ESPN_LEAGUES.has(league)) return res.status(400).json({ error: `Unknown league: ${league}` });
  const cacheKey = `odds:${league}:${gameId}`;
  const cached = espnCacheGet(cacheKey);
  if (cached) return res.json(cached);
  try {
    const { status, data } = await espnFetch(espnOddsUrl(league, gameId));
    if (status !== 200) return res.status(status).json({ error: 'ESPN API error' });
    espnCacheSet(cacheKey, data, 30);
    res.json(data);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.get('/espn/:league/teams/:teamId/schedule', async (req, res) => {
  const { league, teamId } = req.params;
  const { seasontype } = req.query;
  if (!VALID_ESPN_LEAGUES.has(league)) return res.status(400).json({ error: `Unknown league: ${league}` });
  const cacheKey = `schedule:${league}:${teamId}:${seasontype || 'null'}`;
  const cached = espnCacheGet(cacheKey);
  if (cached) return res.json(cached);
  try {
    const { status, data } = await espnFetch(espnTeamScheduleUrl(league, teamId, seasontype));
    if (status !== 200) return res.status(status).json({ error: 'ESPN API error' });
    espnCacheSet(cacheKey, data, 1800);
    res.json(data);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.get('/espn/:league/teams/:teamId', async (req, res) => {
  const { league, teamId } = req.params;
  if (!VALID_ESPN_LEAGUES.has(league)) return res.status(400).json({ error: `Unknown league: ${league}` });
  const cacheKey = `team:${league}:${teamId}`;
  const cached = espnCacheGet(cacheKey);
  if (cached) return res.json(cached);
  try {
    const { status, data } = await espnFetch(espnTeamInfoUrl(league, teamId));
    if (status !== 200) return res.status(status).json({ error: 'ESPN API error' });
    espnCacheSet(cacheKey, data, 3600);
    res.json(data);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.get('/espn/:league/stream', (req, res) => {
  const { league } = req.params;
  if (!VALID_ESPN_LEAGUES.has(league)) return res.status(400).json({ error: `Unknown league: ${league}` });
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  let lastHash = '';
  const interval = setInterval(() => {
    const games = Object.values(_liveGameData[league] || {});
    if (games.length > 0) {
      const payload = JSON.stringify({ games });
      const hash = crypto.createHash('md5').update(payload).digest('hex');
      if (hash !== lastHash) { res.write(`data: ${payload}\n\n`); lastHash = hash; }
    } else {
      res.write(': heartbeat\n\n');
    }
  }, 3000);
  req.on('close', () => clearInterval(interval));
});

// Log memory usage every 5 minutes and sweep expired ESPN cache entries
function _startMemoryMonitor() {
  setInterval(() => {
    const used = process.memoryUsage();
    const mb = (bytes) => Math.round(bytes / 1024 / 1024);
    console.log(`[memory] heap=${mb(used.heapUsed)}MB rss=${mb(used.rss)}MB espnCache=${_espnCache.size} liveGames=${Object.values(_liveGameData).reduce((n, g) => n + Object.keys(g).length, 0)}`);
    // Sweep expired ESPN cache entries proactively
    const now = Date.now();
    for (const [k, v] of _espnCache) {
      if (now > v.expiresAt) _espnCache.delete(k);
    }
  }, 5 * 60 * 1000);
}

if (require.main === module) (async () => {
  // Run database migrations before starting the server
  try {
    const path = require('path');
    const migrationDir = path.join(__dirname, '..', 'migrations');
    const migrationFiles = fs.readdirSync(migrationDir)
      .filter((f) => f.endsWith('.sql'))
      .sort();
    const migrClient = await db.getClient();
    try {
      for (const file of migrationFiles) {
        const sql = fs.readFileSync(path.join(migrationDir, file), 'utf8');
        await migrClient.query(sql);
        console.log(`[migration] Applied: ${file}`);
      }
    } finally {
      migrClient.release();
    }
    console.log('[migration] All migrations applied');
  } catch (e) {
    console.error('[migration] Failed (continuing anyway):', e.message);
  }

  const sslCert = process.env.SSL_CERT_PATH;
  const sslKey = process.env.SSL_KEY_PATH;
  const port = process.env.PORT || 8081;
  const host = process.env.HOST || '0.0.0.0';

  // Helper to print start info
  async function printStartupInfo(listenUrl) {
    console.log('');
    console.log('========================================');
    console.log('  SYNC SERVER WITH POSTGRESQL');
    console.log('========================================');
    console.log('');
    console.log(`Server URL: ${listenUrl}`);

    // Test database connection
    try {
      const r = await db.query('SELECT 1');
      if (r && typeof r.rowCount === 'number') {
        console.log('Database:   PostgreSQL ✓ CONNECTED');
        console.log(`Connection: ${process.env.DATABASE_URL || 'localhost:5432/elite_bet_sync'}`);
      } else {
        console.log('Database:   In-memory mode (PostgreSQL unavailable)');
      }
    } catch (e) {
      console.log('Database:   In-memory mode (PostgreSQL error:', e.message + ')');
    }

    console.log('');
    if (host === '0.0.0.0') {
      const proto = listenUrl.startsWith('https') ? 'https' : 'http';
      console.log('Accessible from:');
      console.log(`  - Local:   ${proto}://localhost:${port}`);
      console.log(`  - Network: ${proto}://<your-ip>:${port}`);
    }
    console.log('');
    console.log('Endpoints:');
    console.log('  POST /sync              - Main sync');
    console.log('  POST /sync/batch        - Batch sync');
    console.log('  GET  /health            - Health check');
    console.log('  GET  /espn/:league/...  - ESPN proxy');
    console.log('');
    console.log('Data is PERSISTENT (stored in PostgreSQL)');
    console.log('');
    console.log('Server ready! ✅');
    console.log('========================================');
    console.log('');
  }

  if (sslCert && sslKey) {
    // Start HTTPS server
    try {
      const cert = fs.readFileSync(sslCert);
      const key = fs.readFileSync(sslKey);
      const httpsServer = https.createServer({ key, cert }, app);
      httpsServer.listen(port, host, async () => {
        await printStartupInfo(`https://${host === '0.0.0.0' ? 'localhost' : host}:${port}`);
        _espnPollingActive = true;
        _pollScoreboards().catch((e) => console.error('[ESPN] Scoreboard polling stopped:', e.message));
        _pollLiveGames().catch((e) => console.error('[ESPN] Live game polling stopped:', e.message));
        console.log('[ESPN] Background polling started');
        mlTraining.scheduleTraining(db, 6);
        console.log('[ML] Training scheduler started (6h interval)');
        gameCacher.startScheduler(db, 1);
        teamStatsBuilder.startScheduler(db, 1);
        _startMemoryMonitor();
      });
      return;
    } catch (e) {
      console.error('Failed to start HTTPS server, falling back to HTTP:', e.message);
    }
  }

  // Fall back to HTTP
  app.listen(port, host, async () => {
    await printStartupInfo(`http://${host === '0.0.0.0' ? 'localhost' : host}:${port}`);
    _espnPollingActive = true;
    _pollScoreboards().catch((e) => console.error('[ESPN] Scoreboard polling stopped:', e.message));
    _pollLiveGames().catch((e) => console.error('[ESPN] Live game polling stopped:', e.message));
    console.log('[ESPN] Background polling started');
    mlTraining.scheduleTraining(db, 6);
    console.log('[ML] Training scheduler started (6h interval)');
    gameCacher.startScheduler(db, 1);
    teamStatsBuilder.startScheduler(db, 1);
    _startMemoryMonitor();
  });
})();

module.exports = app;
