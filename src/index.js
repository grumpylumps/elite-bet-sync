const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const crypto = require('crypto');
const fs = require('fs');
const https = require('https');
const db = require('./db');

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
    jwt.verify(provided, jwtSecret);
    return next();
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

    // fetch server changes since last_server_seq (paginated)
    const pullLimit = Math.min(Math.max(parseInt(req.body.pull_limit) || 500, 1), 50000);
    const srv = await client.query(
      'SELECT server_seq, table_name, pk, op, payload, change_id FROM server_changes WHERE server_seq > $1 ORDER BY server_seq ASC LIMIT $2',
      [last_server_seq, pullLimit]
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

const port = process.env.PORT || 8081;
const host = process.env.HOST || '0.0.0.0';
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
// Bet logs endpoint — returns system bet log history for a league
// ---------------------------------------------------------------------------
app.get('/api/bet-logs/:league', async (req, res) => {
  const { league } = req.params;
  const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
  try {
    const result = await db.query(
      `SELECT league_id, game_id, period, trigger, line, proj, edge, probability,
              direction, captured_at, capture_type, actual, result, result_logged_at,
              stake, home_team, away_team
       FROM bet_logs
       WHERE league_id = $1
       ORDER BY captured_at DESC
       LIMIT $2`,
      [league, limit]
    );
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

// In-memory TTL cache for ESPN responses
const _espnCache = new Map(); // key -> { data, expiresAt }

function espnCacheGet(key) {
  const entry = _espnCache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) { _espnCache.delete(key); return null; }
  return entry.data;
}
function espnCacheSet(key, data, ttlSeconds) {
  _espnCache.set(key, { data, expiresAt: Date.now() + ttlSeconds * 1000 });
}

// HTTPS GET helper using Node built-in
function espnFetch(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'Mozilla/5.0' } }, (res) => {
      let body = '';
      res.on('data', (chunk) => (body += chunk));
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(body) }); }
        catch (e) { reject(new Error(`ESPN JSON parse error: ${e.message}`)); }
      });
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
            }
          }
        } catch (e) {
          console.error(`[ESPN] Live poll error (${league}/${gameId}):`, e.message);
        }
        await new Promise((r) => setTimeout(r, 500));
      }
    }
    await new Promise((r) => setTimeout(r, hasLive ? 1000 : 5000));
  }
}

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

(async () => {
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
  });
})();

module.exports = app;
