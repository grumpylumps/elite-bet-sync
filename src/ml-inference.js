'use strict';

const db = require('./db');

// ---------------------------------------------------------------------------
// ML Inference Module
//
// Implements Ridge Regression inference for:
//   1. In-game quarter projections (44-feature vector)
//   2. Pregame predictions (25-feature vector for total/spread/moneyline)
//
// Weights are loaded from the `ml_models` PostgreSQL table (populated by
// the Python training pipeline via export_models_for_flutter.py).
// ---------------------------------------------------------------------------

// In-memory weight cache: { "league:modelName": { weights: [...], loadedAt } }
const _modelCache = new Map();
const CACHE_TTL_MS = 5 * 60 * 1000; // Refresh weights every 5 minutes

// Feature names — MUST match Flutter ml_projection_service.dart featureNames exactly
const INGAME_FEATURE_NAMES = [
  'current_total', 'time_remaining_pct', 'elapsed_minutes', 'current_ppm',
  'home_pace', 'away_pace', 'avg_pace',
  'home_off_eff', 'away_off_eff', 'home_def_eff', 'away_def_eff',
  'book_quarter_avg', 'quarter_num', 'is_overtime', 'score_differential',
  'home_q_avg', 'away_q_avg', 'game_total_so_far', 'game_ppm',
  'is_close_game', 'is_blowout',
  'home_elo', 'away_elo', 'elo_diff',
  'fg_pct_game', 'three_pct_game', 'ft_pct_game',
  'prev_quarter_total', 'q1_total', 'q2_total', 'first_half_total',
  'home_recent_ppg', 'away_recent_ppg', 'home_recent_opp_ppg', 'away_recent_opp_ppg',
  'home_form_trend', 'away_form_trend', 'combined_recent_total',
  'pregame_spread', 'pregame_home_moneyline',
  'implied_margin', 'estimated_possessions_left', 'implied_by_estimated_current',
  'home_away_recent_diff', 'score_sign_diff', 'score_time_interaction',
  'implied_by_score',
];

// League configs for feature extraction (mirrors Flutter mlLeagueConfigs)
const ML_LEAGUE_CONFIGS = {
  nba:   { quarterLength: 720,  defaultQuarterAvg: 56.0, defaultPace: 100.0, periods: 4 },
  wnba:  { quarterLength: 600,  defaultQuarterAvg: 42.0, defaultPace: 95.0,  periods: 4 },
  ncaam: { quarterLength: 1200, defaultQuarterAvg: 70.0, defaultPace: 70.0,  periods: 2 },
  ncaaw: { quarterLength: 600,  defaultQuarterAvg: 35.0, defaultPace: 70.0,  periods: 4 },
  nfl:   { quarterLength: 900,  defaultQuarterAvg: 10.5, defaultPace: 1.0,   periods: 4 },
  cfb:   { quarterLength: 900,  defaultQuarterAvg: 12.0, defaultPace: 1.0,   periods: 4 },
  mlb:   { quarterLength: 600,  defaultQuarterAvg: 0.94, defaultPace: 4.5,   periods: 9 },
  cbb:   { quarterLength: 600,  defaultQuarterAvg: 1.28, defaultPace: 5.5,   periods: 9 },
};

const BASEBALL_LEAGUES = new Set(['mlb', 'cbb']);

// ---------------------------------------------------------------------------
// Weight loading
// ---------------------------------------------------------------------------

/**
 * Load model weights from the ml_models table, with in-memory caching.
 * @param {string} leagueId
 * @param {string} modelName  e.g. 'quarter_projection', 'pregame_total', etc.
 * @returns {number[]|null}  Weight array or null if not found
 */
async function loadWeights(leagueId, modelName) {
  const cacheKey = `${leagueId}:${modelName}`;
  const cached = _modelCache.get(cacheKey);
  if (cached && (Date.now() - cached.loadedAt) < CACHE_TTL_MS) {
    return cached.weights;
  }

  try {
    const result = await db.query(
      `SELECT metadata FROM ml_models WHERE league_id = $1 AND model_name = $2`,
      [leagueId, modelName]
    );
    if (result.rowCount === 0) return null;

    const metaRaw = result.rows[0].metadata;
    const meta = typeof metaRaw === 'string' ? JSON.parse(metaRaw) : metaRaw;
    const weights = meta?.weights;
    if (!Array.isArray(weights) || weights.length < 2) return null;

    _modelCache.set(cacheKey, { weights, loadedAt: Date.now() });
    return weights;
  } catch (e) {
    console.error(`[ml-inference] Error loading weights for ${cacheKey}:`, e.message);
    return null;
  }
}

/**
 * Load correction weights (in_app_linear_correction) for a league.
 * Returns { '': [a, b], '<trigger>': [a, b] } or empty object.
 */
async function loadCorrectionWeights(leagueId) {
  const cacheKey = `${leagueId}:_corrections`;
  const cached = _modelCache.get(cacheKey);
  if (cached && (Date.now() - cached.loadedAt) < CACHE_TTL_MS) {
    return cached.weights;
  }

  try {
    const result = await db.query(
      `SELECT model_name, metadata FROM ml_models
       WHERE league_id = $1 AND model_name LIKE 'in_app_linear_correction%'`,
      [leagueId]
    );
    const corrections = {};
    for (const row of result.rows) {
      const meta = typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata;
      const ws = meta?.weights;
      if (!Array.isArray(ws) || ws.length < 2) continue;
      const trigger = meta?.trigger?.trim() ||
        (row.model_name === 'in_app_linear_correction' ? '' :
          row.model_name.substring('in_app_linear_correction:'.length));
      corrections[trigger] = ws;
    }
    _modelCache.set(cacheKey, { weights: corrections, loadedAt: Date.now() });
    return corrections;
  } catch (e) {
    console.error(`[ml-inference] Error loading corrections for ${leagueId}:`, e.message);
    return {};
  }
}

// ---------------------------------------------------------------------------
// Ridge Regression inference (dot product + bias)
// ---------------------------------------------------------------------------

function ridgePredict(features, weights) {
  if (!weights || weights.length < 2) return null;
  let sum = weights[weights.length - 1]; // Bias term (last element)
  const n = Math.min(features.length, weights.length - 1);
  for (let i = 0; i < n; i++) {
    sum += features[i] * weights[i];
  }
  return sum;
}

// ---------------------------------------------------------------------------
// In-game feature extraction
// Mirrors Flutter ml_projection_service.dart extractFeatures()
// ---------------------------------------------------------------------------

function extractIngameFeatures(body) {
  const league = body.league || 'nba';
  const cfg = ML_LEAGUE_CONFIGS[league] || ML_LEAGUE_CONFIGS.nba;
  const isMLB = BASEBALL_LEAGUES.has(league);

  const currentTotal = body.current_total || 0;
  const timeRemaining = body.time_remaining || 0; // seconds
  const quarter = body.quarter || 1;
  const homeScore = body.home_score || 0;
  const awayScore = body.away_score || 0;
  const homeElo = body.home_elo || 1500;
  const awayElo = body.away_elo || 1500;
  const bookTotal = body.book_total || (cfg.defaultQuarterAvg * cfg.periods);
  const homePace = body.home_pace || cfg.defaultPace;
  const awayPace = body.away_pace || cfg.defaultPace;
  const homeOffEff = body.home_off_eff || 110.0;
  const awayOffEff = body.away_off_eff || 110.0;
  const homeDefEff = body.home_def_eff || 110.0;
  const awayDefEff = body.away_def_eff || 110.0;
  const fgPct = body.fg_pct || 0.45;
  const threePct = body.three_pct || 0.35;
  const ftPct = body.ft_pct || 0.75;
  const prevQuarters = body.prev_quarters || [];
  const homeRecentPpg = body.home_recent_ppg || 0;
  const awayRecentPpg = body.away_recent_ppg || 0;
  const homeRecentOppPpg = body.home_recent_opp_ppg || 0;
  const awayRecentOppPpg = body.away_recent_opp_ppg || 0;
  const homeSeasonPpg = body.home_season_ppg || 0;
  const awaySeasonPpg = body.away_season_ppg || 0;
  const pregameSpread = body.pregame_spread || 0;
  const pregameHomeMoneyline = body.pregame_home_moneyline || 0;
  const homeQAvg = body.home_q_avg || cfg.defaultQuarterAvg;
  const awayQAvg = body.away_q_avg || cfg.defaultQuarterAvg;

  // Time features
  let elapsed, elapsedMinutes, timeRemainingPct;
  if (isMLB) {
    elapsed = Math.max(0, (quarter - 1) * cfg.quarterLength + cfg.quarterLength * 0.5);
    elapsedMinutes = elapsed / 60.0;
    const totalGameLen = cfg.periods * cfg.quarterLength;
    timeRemainingPct = Math.max(0, Math.min(1, 1 - elapsed / totalGameLen));
  } else {
    elapsed = cfg.quarterLength - timeRemaining;
    if (elapsed < 0) elapsed = 0;
    elapsedMinutes = elapsed / 60.0;
    timeRemainingPct = cfg.quarterLength > 0
      ? Math.max(0, Math.min(1, timeRemaining / cfg.quarterLength))
      : 0;
  }

  const currentPpm = elapsedMinutes > 0.5 ? currentTotal / elapsedMinutes : 0;
  const avgPace = (homePace + awayPace) / 2;
  const bookQuarterAvg = bookTotal / cfg.periods;
  const isOvertime = quarter > cfg.periods ? 1 : 0;
  const scoreDiff = Math.abs(homeScore - awayScore);

  // Accumulate game totals from previous quarters
  const gameTotalSoFar = homeScore + awayScore - currentTotal;
  const prevPeriods = (quarter > 1) ? (quarter - 1) : 0;
  const gamePpm = prevPeriods > 0 && cfg.quarterLength > 0
    ? gameTotalSoFar / (prevPeriods * cfg.quarterLength / 60.0)
    : 0;

  const isCloseGame = scoreDiff < 10 ? 1 : 0;
  const isBlowout = scoreDiff > 20 ? 1 : 0;
  const eloDiff = homeElo - awayElo;

  // Previous quarter data
  const q1Total = prevQuarters.length >= 1 ? prevQuarters[0] : 0;
  const q2Total = prevQuarters.length >= 2 ? prevQuarters[1] : 0;
  const prevQuarterTotal = prevQuarters.length > 0
    ? prevQuarters[prevQuarters.length - 1] : 0;
  const firstHalfTotal = prevQuarters.slice(0, Math.min(2, prevQuarters.length))
    .reduce((a, b) => a + b, 0);

  // Form trends (use recent vs season as proxy)
  const leagueAvg = cfg.defaultQuarterAvg * cfg.periods;
  const homeFormTrend = homeSeasonPpg > 0
    ? (homeRecentPpg - homeSeasonPpg) / Math.max(leagueAvg, 1) : 0;
  const awayFormTrend = awaySeasonPpg > 0
    ? (awayRecentPpg - awaySeasonPpg) / Math.max(leagueAvg, 1) : 0;
  const combinedRecentTotal = homeRecentPpg + awayRecentPpg;

  // Derived features
  const impliedMargin = pregameSpread !== 0 ? -pregameSpread : 0;
  const possPerMin = isMLB ? 0 : (league === 'ncaam' || league === 'ncaaw' ? 1.75 : 2.08);
  const estimatedPossessionsLeft = isMLB ? 0 : (timeRemaining / 60.0) * possPerMin;
  const impliedByEstimatedCurrent = elapsedMinutes > 0.5
    ? currentTotal / elapsedMinutes * (cfg.quarterLength / 60.0) : bookQuarterAvg;
  const homeAwayRecentDiff = homeRecentPpg - awayRecentPpg;
  const scoreSignDiff = homeScore - awayScore;
  const scoreTimeInteraction = scoreSignDiff * timeRemainingPct;
  const impliedByScore = currentTotal + (currentPpm > 0
    ? currentPpm * (timeRemaining / 60.0) : bookQuarterAvg * timeRemainingPct);

  return [
    currentTotal, timeRemainingPct, elapsedMinutes, currentPpm,
    homePace, awayPace, avgPace,
    homeOffEff, awayOffEff, homeDefEff, awayDefEff,
    bookQuarterAvg, quarter, isOvertime, scoreDiff,
    homeQAvg, awayQAvg, gameTotalSoFar, gamePpm,
    isCloseGame, isBlowout,
    homeElo, awayElo, eloDiff,
    fgPct, threePct, ftPct,
    prevQuarterTotal, q1Total, q2Total, firstHalfTotal,
    homeRecentPpg, awayRecentPpg, homeRecentOppPpg, awayRecentOppPpg,
    homeFormTrend, awayFormTrend, combinedRecentTotal,
    pregameSpread, pregameHomeMoneyline,
    impliedMargin, estimatedPossessionsLeft, impliedByEstimatedCurrent,
    homeAwayRecentDiff, scoreSignDiff, scoreTimeInteraction,
    impliedByScore,
  ];
}

// ---------------------------------------------------------------------------
// In-game prediction (with correction weights + blend)
// ---------------------------------------------------------------------------

/**
 * Predict quarter total using Ridge Regression weights.
 * Falls back to null if no weights are available.
 */
async function predictIngame(body) {
  const league = body.league || 'nba';
  const gameId = body.game_id || null;

  // Load model weights for the league
  // Try 'quarter_projection' first (from Python export), then league-specific
  let weights = await loadWeights(league, 'quarter_projection');
  if (!weights) weights = await loadWeights(league, `quarter_proj_${league}`);
  if (!weights) return null;

  const features = extractIngameFeatures(body);
  let prediction = ridgePredict(features, weights);
  if (prediction == null) return null;

  // Sanity clamp
  const currentTotal = body.current_total || 0;
  prediction = Math.max(currentTotal, Math.min(prediction, currentTotal + 100));

  // Apply correction weights if available
  const corrections = await loadCorrectionWeights(league);
  const trigger = body.trigger || '';
  const corr = corrections[trigger] || corrections[''];
  if (corr && corr.length >= 2) {
    prediction = Math.max(currentTotal,
      Math.min(corr[0] + corr[1] * prediction, currentTotal + 100));
  }

  return {
    projected_total: Math.round(prediction * 100) / 100,
    used_ml_model: true,
    league,
    game_id: gameId,
  };
}

// ---------------------------------------------------------------------------
// Pregame feature extraction
// Mirrors Flutter pregame_ml_service.dart _extractFeatures()
// ---------------------------------------------------------------------------

function extractPregameFeatures({
  leagueId, homeElo, awayElo,
  homeSeasonWins, homeSeasonLosses, awaySeasonWins, awaySeasonLosses,
  homeLast5Wins, homeLast5Losses, awayLast5Wins, awayLast5Losses,
  homePpg, awayPpg, homeOppPpg, awayOppPpg,
  predTotalElo, predSpread, blendedTotal,
  bookTotalLine, bookSpreadHome, bookMoneylineHome, bookMoneylineAway,
  avgTotal, spreadNorm,
}) {
  const winPct = (w, l) => (w + l) > 0 ? w / (w + l) : 0.5;
  const norm = (v) => avgTotal > 0 ? v / avgTotal : 0;
  const mlToProb = (ml) => {
    if (ml == null) return 0.5;
    if (ml > 0) return 100.0 / (ml + 100.0);
    return (-ml) / ((-ml) + 100.0);
  };

  const homeWinPct = winPct(homeSeasonWins, homeSeasonLosses);
  const awayWinPct = winPct(awaySeasonWins, awaySeasonLosses);
  const homeLast5Pct = winPct(homeLast5Wins, homeLast5Losses);
  const awayLast5Pct = winPct(awayLast5Wins, awayLast5Losses);

  return [
    (homeElo - 1500) / 200,               //  0 homeElo
    (awayElo - 1500) / 200,               //  1 awayElo
    (homeElo - awayElo) / 200,            //  2 eloDiff
    homeWinPct,                            //  3 homeSeasonWinPct
    awayWinPct,                            //  4 awaySeasonWinPct
    homeLast5Pct,                          //  5 homeLast5WinPct
    awayLast5Pct,                          //  6 awayLast5WinPct
    norm(homePpg),                         //  7 homePpg
    norm(awayPpg),                         //  8 awayPpg
    norm(homeOppPpg),                      //  9 homeOppPpg
    norm(awayOppPpg),                      // 10 awayOppPpg
    norm(homePpg - awayPpg),              // 11 ppgDiff
    norm(awayOppPpg - homeOppPpg),        // 12 defDiff
    norm(predTotalElo ?? avgTotal),        // 13 predTotalElo
    (predSpread ?? 0) / spreadNorm,        // 14 predSpread
    norm(blendedTotal ?? avgTotal),         // 15 blendedTotal
    norm(bookTotalLine ?? avgTotal),        // 16 bookTotalLine
    (bookSpreadHome ?? 0) / spreadNorm,    // 17 bookSpreadHome
    mlToProb(bookMoneylineHome),           // 18 bookMLHome
    mlToProb(bookMoneylineAway),           // 19 bookMLAway
    1.0,                                    // 20 homeAdvantage
    homeWinPct - awayWinPct,              // 21 seasonWinPctDiff
    homeLast5Pct - awayLast5Pct,          // 22 last5WinPctDiff
    norm(homePpg + homeOppPpg),            // 23 homePaceProxy
    norm(awayPpg + awayOppPpg),            // 24 awayPaceProxy
  ];
}

// ---------------------------------------------------------------------------
// Pregame prediction
// ---------------------------------------------------------------------------

/**
 * Predict pregame total, spread, and moneyline using Ridge Regression.
 * Looks up team data from elo_ratings and team_stats tables.
 */
async function predictPregame(body) {
  const league = body.league || 'nba';
  const homeTeamId = body.home_team_id;
  const awayTeamId = body.away_team_id;

  if (!homeTeamId || !awayTeamId) return null;

  // Load all three pregame models
  const [totalWeights, spreadWeights, mlWeights] = await Promise.all([
    loadWeights(league, 'pregame_total'),
    loadWeights(league, 'pregame_spread'),
    loadWeights(league, 'pregame_moneyline'),
  ]);

  if (!totalWeights && !spreadWeights && !mlWeights) return null;

  // Look up team data from the database
  let homeElo = 1500, awayElo = 1500;
  let homeStats = null, awayStats = null;

  try {
    const [eloResult, homeStatsResult, awayStatsResult] = await Promise.all([
      db.query(
        `SELECT team_id, elo FROM elo_ratings WHERE league_id = $1 AND team_id IN ($2, $3)`,
        [league, homeTeamId, awayTeamId]
      ),
      db.query(
        `SELECT * FROM team_stats WHERE league_id = $1 AND team_id = $2`,
        [league, homeTeamId]
      ),
      db.query(
        `SELECT * FROM team_stats WHERE league_id = $1 AND team_id = $2`,
        [league, awayTeamId]
      ),
    ]);

    for (const row of eloResult.rows) {
      if (row.team_id === homeTeamId) homeElo = row.elo || 1500;
      if (row.team_id === awayTeamId) awayElo = row.elo || 1500;
    }
    homeStats = homeStatsResult.rows[0] || null;
    awayStats = awayStatsResult.rows[0] || null;
  } catch (e) {
    console.error('[ml-inference] Error loading team data:', e.message);
  }

  const cfg = ML_LEAGUE_CONFIGS[league] || ML_LEAGUE_CONFIGS.nba;
  const avgTotal = cfg.defaultQuarterAvg * cfg.periods;
  const spreadNorm = ['mlb', 'cbb', 'nhl'].includes(league) ? 5.0 : 20.0;

  const homePpg = homeStats?.ppg ?? avgTotal / 2;
  const awayPpg = awayStats?.ppg ?? avgTotal / 2;
  const homeOppPpg = homeStats?.avg_score_allowed ?? avgTotal / 2;
  const awayOppPpg = awayStats?.avg_score_allowed ?? avgTotal / 2;
  const homeWins = homeStats?.wins ?? 0;
  const homeLosses = homeStats?.losses ?? 0;
  const awayWins = awayStats?.wins ?? 0;
  const awayLosses = awayStats?.losses ?? 0;

  // Simple Elo-based priors for features that need them
  const eloDiff = homeElo - awayElo;
  const predSpread = -eloDiff / 25; // rough Elo-to-spread conversion
  const predTotalElo = avgTotal + (homePpg + awayPpg - avgTotal) * 0.5;
  const blendedTotal = body.book_total_line ?? predTotalElo;

  const features = extractPregameFeatures({
    leagueId: league,
    homeElo, awayElo,
    homeSeasonWins: homeWins, homeSeasonLosses: homeLosses,
    awaySeasonWins: awayWins, awaySeasonLosses: awayLosses,
    homeLast5Wins: Math.round(homeWins * 0.3), // rough estimate
    homeLast5Losses: Math.round(homeLosses * 0.3),
    awayLast5Wins: Math.round(awayWins * 0.3),
    awayLast5Losses: Math.round(awayLosses * 0.3),
    homePpg, awayPpg, homeOppPpg, awayOppPpg,
    predTotalElo, predSpread, blendedTotal,
    bookTotalLine: body.book_total_line ?? null,
    bookSpreadHome: body.book_spread_home ?? null,
    bookMoneylineHome: body.book_moneyline_home ?? null,
    bookMoneylineAway: body.book_moneyline_away ?? null,
    avgTotal, spreadNorm,
  });

  // Run predictions
  const predictedTotal = totalWeights
    ? ridgePredict(features, totalWeights) ?? avgTotal
    : avgTotal;
  const predictedSpread = spreadWeights
    ? ridgePredict(features, spreadWeights) ?? predSpread
    : predSpread;
  let homeWinProb = mlWeights
    ? ridgePredict(features, mlWeights) ?? 0.5
    : 0.5;
  // Clamp probability to [0.01, 0.99]
  homeWinProb = Math.max(0.01, Math.min(0.99, homeWinProb));

  const usedMlModel = !!(totalWeights || spreadWeights || mlWeights);

  return {
    league,
    home_team_id: homeTeamId,
    away_team_id: awayTeamId,
    predicted_total: Math.round(predictedTotal * 100) / 100,
    predicted_spread: Math.round(predictedSpread * 100) / 100,
    home_win_prob: Math.round(homeWinProb * 1000) / 1000,
    used_ml_model: usedMlModel,
  };
}

// ---------------------------------------------------------------------------
// Bet generator integration: enhanced projection using ML weights
// ---------------------------------------------------------------------------

/**
 * Try to produce an ML-enhanced period projection for the bet generator.
 * Returns the projection or null if no weights are available.
 */
async function mlProjectForBetGen({
  league, currentTotal, secondsLeft, periodLength, period,
  homeScore, awayScore, totalLine,
}) {
  const weights = await loadWeights(league, 'quarter_projection')
    || await loadWeights(league, `quarter_proj_${league}`);
  if (!weights) return null;

  const cfg = ML_LEAGUE_CONFIGS[league] || ML_LEAGUE_CONFIGS.nba;

  // Build a minimal request body from bet-generator game state
  const body = {
    league,
    current_total: currentTotal,
    time_remaining: secondsLeft,
    quarter: period || 1,
    home_score: homeScore || 0,
    away_score: awayScore || 0,
    book_total: totalLine || (cfg.defaultQuarterAvg * cfg.periods),
    home_pace: cfg.defaultPace,
    away_pace: cfg.defaultPace,
    home_q_avg: cfg.defaultQuarterAvg,
    away_q_avg: cfg.defaultQuarterAvg,
  };

  const features = extractIngameFeatures(body);
  let prediction = ridgePredict(features, weights);
  if (prediction == null) return null;

  // Sanity clamp
  prediction = Math.max(currentTotal, Math.min(prediction, currentTotal + 100));

  // Apply corrections
  const corrections = await loadCorrectionWeights(league);
  const corr = corrections[''];
  if (corr && corr.length >= 2) {
    prediction = Math.max(currentTotal,
      Math.min(corr[0] + corr[1] * prediction, currentTotal + 100));
  }

  // Return prediction + feature vector for training feedback
  return { prediction, features };
}

// ---------------------------------------------------------------------------
// Cache management
// ---------------------------------------------------------------------------

function clearCache() {
  _modelCache.clear();
}

module.exports = {
  predictIngame,
  predictPregame,
  mlProjectForBetGen,
  extractIngameFeatures,
  extractPregameFeatures,
  ridgePredict,
  loadWeights,
  clearCache,
  ML_LEAGUE_CONFIGS,
  INGAME_FEATURE_NAMES,
};
