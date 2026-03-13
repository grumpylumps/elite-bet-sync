'use strict';

const https = require('https');
const zlib = require('zlib');

// ---------------------------------------------------------------------------
// Game Cacher Module
//
// Periodically fetches ESPN scoreboards for all leagues and upserts completed
// games into the cached_games table in PostgreSQL. This provides the training
// data needed by the pregame ML models.
// ---------------------------------------------------------------------------

const ESPN_BASE = 'https://site.api.espn.com/apis/site/v2/sports';

const LEAGUE_PATHS = {
  nba: 'basketball/nba',
  wnba: 'basketball/wnba',
  ncaam: 'basketball/mens-college-basketball',
  ncaaw: 'basketball/womens-college-basketball',
  nfl: 'football/nfl',
  cfb: 'football/college-football',
  mlb: 'baseball/mlb',
  nhl: 'hockey/nhl',
};

const LEAGUE_GROUPS = { ncaam: '50', ncaaw: '50', cfb: '80' };

function espnFetch(url) {
  return new Promise((resolve, reject) => {
    const opts = { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept-Encoding': 'gzip, deflate' } };
    https.get(url, opts, (res) => {
      const encoding = (res.headers['content-encoding'] || '').toLowerCase();
      let stream = res;
      if (encoding === 'gzip') stream = res.pipe(zlib.createGunzip());
      else if (encoding === 'deflate') stream = res.pipe(zlib.createInflate());
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

function scoreboardUrl(league, date) {
  const path = LEAGUE_PATHS[league];
  const groups = LEAGUE_GROUPS[league] ? `&groups=${LEAGUE_GROUPS[league]}` : '';
  return `${ESPN_BASE}/${path}/scoreboard?limit=1000&dates=${date}${groups}`;
}

/**
 * Parse an ESPN scoreboard event into a cached_games row.
 */
function parseEvent(league, ev) {
  const comp = ev.competitions?.[0];
  if (!comp) return null;

  const home = comp.competitors?.find(c => c.homeAway === 'home');
  const away = comp.competitors?.find(c => c.homeAway === 'away');
  if (!home || !away) return null;

  const status = comp.status?.type?.state || 'pre'; // pre, in, post
  const period = comp.status?.period || 0;
  const clock = comp.status?.displayClock || null;
  const statusDetail = comp.status?.type?.shortDetail || null;
  const startTime = ev.date || null;

  // Period scores from linescores
  let periodScores = null;
  try {
    const homeLine = home.linescores?.map(l => l.value) || [];
    const awayLine = away.linescores?.map(l => l.value) || [];
    if (homeLine.length > 0 || awayLine.length > 0) {
      periodScores = JSON.stringify({ home: homeLine, away: awayLine });
    }
  } catch (_) {}

  return {
    league_id: league,
    game_id: ev.id,
    home_team_id: home.team?.id || home.id || '',
    home_team_name: home.team?.displayName || home.team?.name || '',
    home_team_abbrev: home.team?.abbreviation || null,
    away_team_id: away.team?.id || away.id || '',
    away_team_name: away.team?.displayName || away.team?.name || '',
    away_team_abbrev: away.team?.abbreviation || null,
    home_score: parseInt(home.score, 10) || 0,
    away_score: parseInt(away.score, 10) || 0,
    period,
    clock,
    status,
    status_detail: statusDetail,
    start_time: startTime,
    period_scores: periodScores,
    game_data: null,
    elo_updated: false,
  };
}

/**
 * Fetch scoreboard for a league/date and upsert all games into cached_games.
 */
async function cacheScoreboard(db, league, date) {
  const url = scoreboardUrl(league, date);
  try {
    const { status, data } = await espnFetch(url);
    if (status !== 200) {
      console.log(`[GAME_CACHE] ESPN ${league} ${date} returned ${status}`);
      return 0;
    }

    const events = data.events || [];
    if (events.length === 0) return 0;

    const rows = events.map(ev => parseEvent(league, ev)).filter(Boolean);
    if (rows.length === 0) return 0;

    // Batch upsert
    let upserted = 0;
    for (const row of rows) {
      try {
        await db.query(`
          INSERT INTO cached_games (
            league_id, game_id, home_team_id, home_team_name, home_team_abbrev,
            away_team_id, away_team_name, away_team_abbrev,
            home_score, away_score, period, clock, status, status_detail,
            start_time, period_scores, game_data, elo_updated, last_updated
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,now())
          ON CONFLICT (league_id, game_id) DO UPDATE SET
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            period = EXCLUDED.period,
            clock = EXCLUDED.clock,
            status = EXCLUDED.status,
            status_detail = EXCLUDED.status_detail,
            period_scores = EXCLUDED.period_scores,
            last_updated = now()
        `, [
          row.league_id, row.game_id,
          row.home_team_id, row.home_team_name, row.home_team_abbrev,
          row.away_team_id, row.away_team_name, row.away_team_abbrev,
          row.home_score, row.away_score,
          row.period, row.clock, row.status, row.status_detail,
          row.start_time, row.period_scores, row.game_data, row.elo_updated,
        ]);
        upserted++;
      } catch (e) {
        console.error(`[GAME_CACHE] upsert error ${league}/${row.game_id}: ${e.message}`);
      }
    }
    return upserted;
  } catch (e) {
    console.error(`[GAME_CACHE] fetch error ${league} ${date}: ${e.message}`);
    return 0;
  }
}

/**
 * Generate date strings (YYYYMMDD) for a range of days back from today.
 */
function dateRange(daysBack) {
  const dates = [];
  for (let i = 0; i <= daysBack; i++) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    dates.push(d.toISOString().slice(0, 10).replace(/-/g, ''));
  }
  return dates;
}

/**
 * Run a full backfill: fetch the last N days of scoreboards for all leagues.
 * Good for initial population.
 */
async function backfill(db, daysBack = 90) {
  const leagues = Object.keys(LEAGUE_PATHS);
  const dates = dateRange(daysBack);
  let total = 0;

  console.log(`[GAME_CACHE] Starting backfill: ${leagues.length} leagues x ${dates.length} days`);

  for (const league of leagues) {
    let leagueTotal = 0;
    for (const date of dates) {
      const n = await cacheScoreboard(db, league, date);
      leagueTotal += n;
      // Small delay to be nice to ESPN API
      if (n > 0) await new Promise(r => setTimeout(r, 200));
    }
    if (leagueTotal > 0) {
      console.log(`[GAME_CACHE] ${league}: cached ${leagueTotal} games`);
    }
    total += leagueTotal;
  }

  console.log(`[GAME_CACHE] Backfill complete: ${total} total games cached`);
  return total;
}

/**
 * Cache today's and yesterday's games for all leagues (daily refresh).
 */
async function refreshRecent(db) {
  const leagues = Object.keys(LEAGUE_PATHS);
  const dates = dateRange(1); // today + yesterday
  let total = 0;

  for (const league of leagues) {
    for (const date of dates) {
      total += await cacheScoreboard(db, league, date);
    }
  }

  if (total > 0) {
    console.log(`[GAME_CACHE] Refreshed ${total} games`);
  }
  return total;
}

let _refreshInterval = null;

/**
 * Start periodic game caching. Runs an initial backfill if the table is empty,
 * then refreshes recent games every intervalHours.
 */
async function startScheduler(db, intervalHours = 1) {
  // Check if table is empty — if so, do initial backfill
  try {
    const { rows } = await db.query('SELECT COUNT(*) AS cnt FROM cached_games');
    const count = parseInt(rows[0].cnt, 10);
    if (count === 0) {
      console.log('[GAME_CACHE] Table empty — starting 90-day backfill...');
      await backfill(db, 90);
    } else {
      console.log(`[GAME_CACHE] Table has ${count} games — refreshing recent...`);
      await refreshRecent(db);
    }
  } catch (e) {
    console.error(`[GAME_CACHE] scheduler init error: ${e.message}`);
  }

  // Schedule periodic refresh
  _refreshInterval = setInterval(async () => {
    try {
      await refreshRecent(db);
    } catch (e) {
      console.error(`[GAME_CACHE] scheduled refresh error: ${e.message}`);
    }
  }, intervalHours * 60 * 60 * 1000);

  console.log(`[GAME_CACHE] Scheduler started (refresh every ${intervalHours}h)`);
}

function stopScheduler() {
  if (_refreshInterval) {
    clearInterval(_refreshInterval);
    _refreshInterval = null;
  }
}

module.exports = {
  cacheScoreboard,
  backfill,
  refreshRecent,
  startScheduler,
  stopScheduler,
  parseEvent,
};
