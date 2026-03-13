'use strict';

// ---------------------------------------------------------------------------
// Team Stats Builder
//
// Computes team_stats (PPG, wins, losses, avg_score_allowed, games_analyzed)
// from the cached_games table. Runs after game-cacher populates finished games.
// ---------------------------------------------------------------------------

const LEAGUES = [
  'nba', 'wnba', 'ncaam', 'ncaaw', 'nfl', 'cfb', 'mlb', 'nhl',
];

/**
 * Build team stats for a single league from cached_games.
 * Only considers completed games (status = 'post') with valid scores.
 */
async function buildForLeague(db, leagueId) {
  const result = await db.query(`
    SELECT home_team_id, home_team_name, away_team_id, away_team_name,
           home_score, away_score
    FROM cached_games
    WHERE league_id = $1
      AND status = 'post'
      AND home_score IS NOT NULL
      AND away_score IS NOT NULL
  `, [leagueId]);

  if (result.rows.length === 0) return 0;

  // Accumulate stats per team
  const teams = {}; // team_id -> { name, points, allowed, wins, losses, games }

  for (const row of result.rows) {
    const homeId = row.home_team_id;
    const awayId = row.away_team_id;
    const homeScore = parseInt(row.home_score, 10) || 0;
    const awayScore = parseInt(row.away_score, 10) || 0;

    // Home team
    if (homeId) {
      if (!teams[homeId]) {
        teams[homeId] = { name: row.home_team_name || '', points: 0, allowed: 0, wins: 0, losses: 0, games: 0 };
      }
      teams[homeId].points += homeScore;
      teams[homeId].allowed += awayScore;
      teams[homeId].games++;
      if (homeScore > awayScore) teams[homeId].wins++;
      else if (awayScore > homeScore) teams[homeId].losses++;
    }

    // Away team
    if (awayId) {
      if (!teams[awayId]) {
        teams[awayId] = { name: row.away_team_name || '', points: 0, allowed: 0, wins: 0, losses: 0, games: 0 };
      }
      teams[awayId].points += awayScore;
      teams[awayId].allowed += homeScore;
      teams[awayId].games++;
      if (awayScore > homeScore) teams[awayId].wins++;
      else if (homeScore > awayScore) teams[awayId].losses++;
    }
  }

  // Upsert into team_stats
  let upserted = 0;
  for (const [teamId, s] of Object.entries(teams)) {
    if (s.games === 0) continue;
    const ppg = Math.round((s.points / s.games) * 100) / 100;
    const avgAllowed = Math.round((s.allowed / s.games) * 100) / 100;

    await db.query(`
      INSERT INTO team_stats (league_id, team_id, team_name, ppg, games_analyzed, wins, losses, avg_score_allowed, last_updated)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
      ON CONFLICT (league_id, team_id) DO UPDATE SET
        team_name = EXCLUDED.team_name,
        ppg = EXCLUDED.ppg,
        games_analyzed = EXCLUDED.games_analyzed,
        wins = EXCLUDED.wins,
        losses = EXCLUDED.losses,
        avg_score_allowed = EXCLUDED.avg_score_allowed,
        last_updated = now()
    `, [leagueId, teamId, s.name, ppg, s.games, s.wins, s.losses, avgAllowed]);
    upserted++;
  }

  return upserted;
}

/**
 * Build team stats for all leagues.
 */
async function buildAll(db) {
  let total = 0;
  for (const league of LEAGUES) {
    try {
      const n = await buildForLeague(db, league);
      if (n > 0) {
        console.log(`[TEAM_STATS] ${league}: computed stats for ${n} teams`);
      }
      total += n;
    } catch (e) {
      console.error(`[TEAM_STATS] ${league} error: ${e.message}`);
    }
  }
  if (total > 0) {
    console.log(`[TEAM_STATS] Total: ${total} team stats updated`);
  }
  return total;
}

let _interval = null;

/**
 * Start periodic team stats computation.
 * Runs immediately on start, then every intervalHours.
 */
async function startScheduler(db, intervalHours = 1) {
  // Run immediately
  try {
    await buildAll(db);
  } catch (e) {
    console.error(`[TEAM_STATS] initial build error: ${e.message}`);
  }

  _interval = setInterval(async () => {
    try {
      await buildAll(db);
    } catch (e) {
      console.error(`[TEAM_STATS] scheduled build error: ${e.message}`);
    }
  }, intervalHours * 60 * 60 * 1000);

  console.log(`[TEAM_STATS] Scheduler started (rebuild every ${intervalHours}h)`);
}

function stopScheduler() {
  if (_interval) {
    clearInterval(_interval);
    _interval = null;
  }
}

module.exports = {
  buildForLeague,
  buildAll,
  startScheduler,
  stopScheduler,
};
