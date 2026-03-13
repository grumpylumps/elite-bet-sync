'use strict';

const db = require('./db');
const mlInference = require('./ml-inference');

// ---------------------------------------------------------------------------
// ML Training Module
//
// Server-side Ridge Regression training for:
//   1. In-game quarter projections
//   2. Pregame total / spread / moneyline
//   3. In-app linear corrections
//
// Trains on data in PostgreSQL and writes weights to the ml_models table.
// ---------------------------------------------------------------------------

const ALL_LEAGUES = ['nba', 'wnba', 'ncaam', 'ncaaw', 'nfl', 'cfb', 'mlb', 'cbb', 'nhl'];

// Period lengths in seconds (for trigger parsing)
const PERIOD_LENGTHS = {
  nba: 720, wnba: 720, ncaam: 1200, ncaaw: 600,
  nfl: 900, cfb: 900, mlb: 270, cbb: 270, nhl: 1200,
};

// Regular number of periods per league
const REGULAR_PERIODS = {
  nba: 4, wnba: 4, ncaam: 2, ncaaw: 4,
  nfl: 4, cfb: 4, mlb: 9, cbb: 9, nhl: 3,
};

const ALPHAS = [0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 50.0, 100.0];

// ---------------------------------------------------------------------------
// Deterministic PRNG (seed 42) for reproducible train/validation splits
// ---------------------------------------------------------------------------

function mulberry32(seed) {
  let s = seed | 0;
  return function () {
    s = (s + 0x6d2b79f5) | 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * Deterministic Fisher-Yates shuffle with seed 42.
 */
function deterministicShuffle(arr) {
  const rng = mulberry32(42);
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// ---------------------------------------------------------------------------
// Ridge Regression solver (Gaussian elimination with partial pivoting)
// ---------------------------------------------------------------------------

/**
 * Solve (X^T X + alpha * I) w = X^T y via Gaussian elimination.
 * @param {number[][]} X  - m x n feature matrix (each row = sample)
 * @param {number[]}   y  - m-length target vector
 * @param {number}     alpha - L2 regularization strength
 * @returns {number[]} weight vector of length n+1 (last element is bias)
 */
function ridgeSolve(X, y, alpha) {
  const m = X.length;
  const n = X[0].length;

  // Augment X with a column of 1s for the bias term
  // Build X^T X + alpha * I  (size (n+1) x (n+1))
  const dim = n + 1;
  const XtX = new Array(dim);
  for (let i = 0; i < dim; i++) {
    XtX[i] = new Float64Array(dim);
  }
  const Xty = new Float64Array(dim);

  // Accumulate X^T X and X^T y row by row to save memory
  for (let s = 0; s < m; s++) {
    const row = X[s];
    const yi = y[s];
    for (let i = 0; i < n; i++) {
      const xi = row[i];
      Xty[i] += xi * yi;
      for (let j = i; j < n; j++) {
        XtX[i][j] += xi * row[j];
      }
      // bias column
      XtX[i][dim - 1] += xi;
    }
    // bias row
    Xty[dim - 1] += yi;
    XtX[dim - 1][dim - 1] += 1;
  }

  // Fill lower triangle (symmetric)
  for (let i = 0; i < n; i++) {
    for (let j = i; j < n; j++) {
      XtX[j][i] = XtX[i][j];
    }
    XtX[dim - 1][i] = XtX[i][dim - 1];
  }

  // Add alpha * I (don't regularize bias)
  for (let i = 0; i < n; i++) {
    XtX[i][i] += alpha;
  }

  // Gaussian elimination with partial pivoting
  // Build augmented matrix [XtX | Xty]
  const aug = new Array(dim);
  for (let i = 0; i < dim; i++) {
    aug[i] = new Float64Array(dim + 1);
    for (let j = 0; j < dim; j++) aug[i][j] = XtX[i][j];
    aug[i][dim] = Xty[i];
  }

  for (let col = 0; col < dim; col++) {
    // Partial pivoting
    let maxRow = col;
    let maxVal = Math.abs(aug[col][col]);
    for (let row = col + 1; row < dim; row++) {
      const v = Math.abs(aug[row][col]);
      if (v > maxVal) { maxVal = v; maxRow = row; }
    }
    if (maxRow !== col) {
      [aug[col], aug[maxRow]] = [aug[maxRow], aug[col]];
    }

    const pivot = aug[col][col];
    if (Math.abs(pivot) < 1e-12) continue; // singular, skip

    // Eliminate below
    for (let row = col + 1; row < dim; row++) {
      const factor = aug[row][col] / pivot;
      for (let j = col; j <= dim; j++) {
        aug[row][j] -= factor * aug[col][j];
      }
    }
  }

  // Back-substitution
  const w = new Float64Array(dim);
  for (let i = dim - 1; i >= 0; i--) {
    let sum = aug[i][dim];
    for (let j = i + 1; j < dim; j++) {
      sum -= aug[i][j] * w[j];
    }
    w[i] = Math.abs(aug[i][i]) > 1e-12 ? sum / aug[i][i] : 0;
  }

  return Array.from(w);
}

/**
 * Compute Mean Absolute Error.
 */
function computeMAE(predictions, actuals) {
  let sum = 0;
  for (let i = 0; i < predictions.length; i++) {
    sum += Math.abs(predictions[i] - actuals[i]);
  }
  return sum / predictions.length;
}

/**
 * Compute Root Mean Squared Error.
 */
function computeRMSE(predictions, actuals) {
  let sum = 0;
  for (let i = 0; i < predictions.length; i++) {
    const d = predictions[i] - actuals[i];
    sum += d * d;
  }
  return Math.sqrt(sum / predictions.length);
}

/**
 * Predict using weight vector: dot(features, weights[0..n-1]) + weights[n] (bias).
 */
function predict(features, weights) {
  let sum = weights[weights.length - 1]; // bias
  const n = Math.min(features.length, weights.length - 1);
  for (let i = 0; i < n; i++) {
    sum += features[i] * weights[i];
  }
  return sum;
}

/**
 * Train Ridge Regression with cross-validated alpha selection.
 * Returns { weights, alpha, mae, rmse, samplesUsed }.
 */
function trainRidge(X, y) {
  const shuffled = deterministicShuffle(
    X.map((row, i) => ({ x: row, y: y[i] }))
  );
  const splitIdx = Math.floor(shuffled.length * 0.8);
  const trainData = shuffled.slice(0, splitIdx);
  const valData = shuffled.slice(splitIdx);

  const Xtrain = trainData.map((d) => d.x);
  const ytrain = trainData.map((d) => d.y);
  const Xval = valData.map((d) => d.x);
  const yval = valData.map((d) => d.y);

  let bestAlpha = 1.0;
  let bestMAE = Infinity;
  let bestWeights = null;

  for (const alpha of ALPHAS) {
    const w = ridgeSolve(Xtrain, ytrain, alpha);
    const preds = Xval.map((row) => predict(row, w));
    const mae = computeMAE(preds, yval);
    if (mae < bestMAE) {
      bestMAE = mae;
      bestAlpha = alpha;
      bestWeights = w;
    }
  }

  // Retrain on full data with best alpha
  const finalWeights = ridgeSolve(X, y, bestAlpha);

  // Evaluate on validation set with final weights
  const valPreds = Xval.map((row) => predict(row, finalWeights));
  const finalMAE = computeMAE(valPreds, yval);
  const finalRMSE = computeRMSE(valPreds, yval);

  return {
    weights: finalWeights,
    alpha: bestAlpha,
    mae: finalMAE,
    rmse: finalRMSE,
    samplesUsed: X.length,
  };
}

// ---------------------------------------------------------------------------
// Trigger parsing — extract time_remaining seconds from trigger string
// ---------------------------------------------------------------------------

/**
 * Parse a trigger string like "Q2 8:30" or "H1 15:00" or "P3 12:00" into
 * { period, secondsRemaining }.
 */
function parseTrigger(trigger, rowPeriod) {
  if (!trigger) return null;
  // Try plain MM:SS format first (e.g., "5:00", "3:30") — period comes from row
  const plainMatch = trigger.match(/^(\d+):(\d+)$/);
  if (plainMatch) {
    const minutes = parseInt(plainMatch[1], 10);
    const seconds = parseInt(plainMatch[2], 10);
    return { period: rowPeriod || 1, secondsRemaining: minutes * 60 + seconds };
  }
  // Also match Q2 8:30, H1 15:00, P3 12:00, 1Q 8:30, etc.
  const match = trigger.match(/(?:[QHP](\d+)|(\d+)[QHP])\s+(\d+):(\d+)/i);
  if (!match) return null;
  const period = parseInt(match[1] || match[2], 10);
  const minutes = parseInt(match[3], 10);
  const seconds = parseInt(match[4], 10);
  return { period, secondsRemaining: minutes * 60 + seconds };
}

// ---------------------------------------------------------------------------
// Quarter Projection Training
// ---------------------------------------------------------------------------

async function trainQuarterProjection(dbConn, leagueId) {
  const tag = `[ml-train] quarter_projection/${leagueId}`;
  console.log(`${tag} Starting...`);

  const periodLength = PERIOD_LENGTHS[leagueId] || 720;
  const regularPeriods = REGULAR_PERIODS[leagueId] || 4;

  const result = await dbConn.query(
    `SELECT * FROM bet_logs
     WHERE league_id = $1
       AND actual IS NOT NULL
       AND proj IS NOT NULL
       AND line IS NOT NULL
       AND trigger != 'Best'
     ORDER BY captured_at DESC
     LIMIT 5000`,
    [leagueId]
  );

  const rows = result.rows;
  if (rows.length < 150) {
    console.log(`${tag} Insufficient samples (${rows.length} < 150), skipping`);
    return null;
  }

  const X = [];
  const y = [];

  for (const row of rows) {
    const parsed = parseTrigger(row.trigger, row.period);
    if (!parsed) continue;

    const timeRemainingSec = parsed.secondsRemaining;
    const timeRemainingPct = periodLength > 0 ? timeRemainingSec / periodLength : 0;
    const elapsedSec = periodLength - timeRemainingSec;
    const elapsedMinutes = Math.max(0, elapsedSec) / 60.0;

    // Estimate current total from proj and time remaining
    const currentTotal = (row.proj || 0) * (1 - timeRemainingPct) * 0.9;
    const currentPpm = elapsedMinutes > 0.5 ? currentTotal / elapsedMinutes : 0;

    const bookQuarterAvg = row.line || 0;
    const quarterNum = row.period || parsed.period || 1;
    const isOvertime = quarterNum > regularPeriods ? 1 : 0;

    // Default stats not available from bet_logs
    const homeElo = 1500;
    const awayElo = 1500;
    const pace = 100;

    const features = [
      timeRemainingPct,
      elapsedMinutes,
      currentTotal,
      currentPpm,
      bookQuarterAvg,
      quarterNum,
      isOvertime,
      homeElo,
      awayElo,
      homeElo - awayElo, // elo_diff
      pace,              // home_pace (default)
      pace,              // away_pace (default)
    ];

    X.push(features);
    y.push(row.actual);
  }

  if (X.length < 150) {
    console.log(`${tag} Insufficient valid samples after parsing (${X.length} < 150), skipping`);
    return null;
  }

  console.log(`${tag} Training on ${X.length} samples...`);
  const model = trainRidge(X, y);

  // Save to ml_models
  const metadata = {
    weights: model.weights,
    samples_used: model.samplesUsed,
    mae: model.mae,
    rmse: model.rmse,
    alpha: model.alpha,
    trained_at: new Date().toISOString(),
    feature_names: [
      'time_remaining_pct', 'elapsed_minutes', 'current_total', 'current_ppm',
      'book_quarter_avg', 'quarter_num', 'is_overtime',
      'home_elo', 'away_elo', 'elo_diff', 'home_pace', 'away_pace',
    ],
  };

  await saveModel(dbConn, leagueId, 'quarter_projection', metadata, model.samplesUsed, model.mae);

  // Log training run
  await logTrainingRun(dbConn, leagueId, 'quarter_projection', model.samplesUsed, {
    mae: model.mae, rmse: model.rmse, alpha: model.alpha,
  });

  console.log(`${tag} Done. MAE=${model.mae.toFixed(3)}, RMSE=${model.rmse.toFixed(3)}, alpha=${model.alpha}, samples=${model.samplesUsed}`);
  return model;
}

// ---------------------------------------------------------------------------
// Pregame Models Training
// ---------------------------------------------------------------------------

async function trainPregameModels(dbConn, leagueId) {
  const tag = `[ml-train] pregame/${leagueId}`;
  console.log(`${tag} Starting...`);

  const cfg = mlInference.ML_LEAGUE_CONFIGS[leagueId] || mlInference.ML_LEAGUE_CONFIGS.nba;
  const avgTotal = cfg.defaultQuarterAvg * cfg.periods;
  const spreadNorm = ['mlb', 'cbb', 'nhl'].includes(leagueId) ? 5.0 : 20.0;

  // Join completed games with odds, elo, and team stats
  const result = await dbConn.query(
    `SELECT
       g.game_id, g.home_team_id, g.away_team_id,
       g.home_score, g.away_score,
       o.total_line, o.spread_home, o.moneyline_home, o.moneyline_away,
       he.elo AS home_elo, ae.elo AS away_elo,
       hs.ppg AS home_ppg, hs.avg_score_allowed AS home_opp_ppg,
       hs.wins AS home_wins, hs.losses AS home_losses,
       as2.ppg AS away_ppg, as2.avg_score_allowed AS away_opp_ppg,
       as2.wins AS away_wins, as2.losses AS away_losses
     FROM cached_games g
     LEFT JOIN game_odds o ON g.league_id = o.league_id AND g.game_id = o.game_id
     LEFT JOIN elo_ratings he ON g.league_id = he.league_id AND g.home_team_id = he.team_id
     LEFT JOIN elo_ratings ae ON g.league_id = ae.league_id AND g.away_team_id = ae.team_id
     LEFT JOIN team_stats hs ON g.league_id = hs.league_id AND g.home_team_id = hs.team_id
     LEFT JOIN team_stats as2 ON g.league_id = as2.league_id AND g.away_team_id = as2.team_id
     WHERE g.league_id = $1
       AND g.status = 'post'
       AND g.home_score IS NOT NULL
       AND g.away_score IS NOT NULL
     ORDER BY g.last_updated DESC
     LIMIT 5000`,
    [leagueId]
  );

  const rows = result.rows;
  if (rows.length < 50) {
    console.log(`${tag} Insufficient samples (${rows.length} < 50), skipping`);
    return null;
  }

  const X = [];
  const yTotal = [];
  const ySpread = [];
  const yML = [];

  for (const row of rows) {
    const homeElo = parseFloat(row.home_elo) || 1500;
    const awayElo = parseFloat(row.away_elo) || 1500;
    const homeWins = parseInt(row.home_wins) || 0;
    const homeLosses = parseInt(row.home_losses) || 0;
    const awayWins = parseInt(row.away_wins) || 0;
    const awayLosses = parseInt(row.away_losses) || 0;
    const homePpg = parseFloat(row.home_ppg) || avgTotal / 2;
    const awayPpg = parseFloat(row.away_ppg) || avgTotal / 2;
    const homeOppPpg = parseFloat(row.home_opp_ppg) || avgTotal / 2;
    const awayOppPpg = parseFloat(row.away_opp_ppg) || avgTotal / 2;

    const eloDiff = homeElo - awayElo;
    const predSpread = -eloDiff / 25;
    const predTotalElo = avgTotal + (homePpg + awayPpg - avgTotal) * 0.5;

    const bookTotalLine = parseFloat(row.total_line) || null;
    const spreadHomeRaw = row.spread_home;
    const bookSpreadHome = spreadHomeRaw != null ? parseFloat(spreadHomeRaw) : null;
    const bookMoneylineHome = parseFloat(row.moneyline_home) || null;
    const bookMoneylineAway = parseFloat(row.moneyline_away) || null;
    const blendedTotal = bookTotalLine ?? predTotalElo;

    const features = mlInference.extractPregameFeatures({
      leagueId,
      homeElo, awayElo,
      homeSeasonWins: homeWins, homeSeasonLosses: homeLosses,
      awaySeasonWins: awayWins, awaySeasonLosses: awayLosses,
      homeLast5Wins: Math.round(homeWins * 0.3),
      homeLast5Losses: Math.round(homeLosses * 0.3),
      awayLast5Wins: Math.round(awayWins * 0.3),
      awayLast5Losses: Math.round(awayLosses * 0.3),
      homePpg, awayPpg, homeOppPpg, awayOppPpg,
      predTotalElo, predSpread, blendedTotal,
      bookTotalLine,
      bookSpreadHome,
      bookMoneylineHome,
      bookMoneylineAway,
      avgTotal, spreadNorm,
    });

    const homeScore = parseInt(row.home_score) || 0;
    const awayScore = parseInt(row.away_score) || 0;

    X.push(features);
    yTotal.push(homeScore + awayScore);
    ySpread.push(homeScore - awayScore);
    yML.push(homeScore > awayScore ? 1 : 0);
  }

  console.log(`${tag} Training on ${X.length} samples...`);

  const results = {};

  // Train pregame_total
  if (X.length >= 50) {
    const totalModel = trainRidge(X, yTotal);
    const meta = {
      weights: totalModel.weights,
      samples_used: totalModel.samplesUsed,
      mae: totalModel.mae,
      rmse: totalModel.rmse,
      alpha: totalModel.alpha,
      trained_at: new Date().toISOString(),
    };
    await saveModel(dbConn, leagueId, 'pregame_total', meta, totalModel.samplesUsed, totalModel.mae);
    await logTrainingRun(dbConn, leagueId, 'pregame_total', totalModel.samplesUsed, {
      mae: totalModel.mae, rmse: totalModel.rmse, alpha: totalModel.alpha,
    });
    results.pregame_total = { mae: totalModel.mae, rmse: totalModel.rmse };
    console.log(`${tag} pregame_total MAE=${totalModel.mae.toFixed(3)}`);
  }

  // Train pregame_spread
  if (X.length >= 50) {
    const spreadModel = trainRidge(X, ySpread);
    const meta = {
      weights: spreadModel.weights,
      samples_used: spreadModel.samplesUsed,
      mae: spreadModel.mae,
      rmse: spreadModel.rmse,
      alpha: spreadModel.alpha,
      trained_at: new Date().toISOString(),
    };
    await saveModel(dbConn, leagueId, 'pregame_spread', meta, spreadModel.samplesUsed, spreadModel.mae);
    await logTrainingRun(dbConn, leagueId, 'pregame_spread', spreadModel.samplesUsed, {
      mae: spreadModel.mae, rmse: spreadModel.rmse, alpha: spreadModel.alpha,
    });
    results.pregame_spread = { mae: spreadModel.mae, rmse: spreadModel.rmse };
    console.log(`${tag} pregame_spread MAE=${spreadModel.mae.toFixed(3)}`);
  }

  // Train pregame_moneyline
  if (X.length >= 50) {
    const mlModel = trainRidge(X, yML);
    const meta = {
      weights: mlModel.weights,
      samples_used: mlModel.samplesUsed,
      mae: mlModel.mae,
      rmse: mlModel.rmse,
      alpha: mlModel.alpha,
      trained_at: new Date().toISOString(),
    };
    await saveModel(dbConn, leagueId, 'pregame_moneyline', meta, mlModel.samplesUsed, mlModel.mae);
    await logTrainingRun(dbConn, leagueId, 'pregame_moneyline', mlModel.samplesUsed, {
      mae: mlModel.mae, rmse: mlModel.rmse, alpha: mlModel.alpha,
    });
    results.pregame_moneyline = { mae: mlModel.mae, rmse: mlModel.rmse };
    console.log(`${tag} pregame_moneyline MAE=${mlModel.mae.toFixed(3)}`);
  }

  console.log(`${tag} Done.`);
  return results;
}

// ---------------------------------------------------------------------------
// In-App Linear Correction Training
// ---------------------------------------------------------------------------

async function trainCorrections(dbConn, leagueId) {
  const tag = `[ml-train] corrections/${leagueId}`;
  console.log(`${tag} Starting...`);

  const result = await dbConn.query(
    `SELECT trigger, proj, actual FROM bet_logs
     WHERE league_id = $1
       AND actual IS NOT NULL
       AND proj IS NOT NULL
     ORDER BY captured_at DESC
     LIMIT 5000`,
    [leagueId]
  );

  const rows = result.rows;
  if (rows.length < 150) {
    console.log(`${tag} Insufficient samples (${rows.length} < 150), skipping`);
    return null;
  }

  const results = {};

  // Global correction: y = a + b * x
  const globalCorr = fitOLS(
    rows.map((r) => r.proj),
    rows.map((r) => r.actual)
  );

  if (globalCorr) {
    const meta = {
      weights: [globalCorr.intercept, globalCorr.slope],
      samples_used: rows.length,
      mae: globalCorr.mae,
      rmse: globalCorr.rmse,
      trained_at: new Date().toISOString(),
      trigger: '',
    };
    await saveModel(dbConn, leagueId, 'in_app_linear_correction', meta, rows.length, globalCorr.mae);
    await logTrainingRun(dbConn, leagueId, 'in_app_linear_correction', rows.length, {
      mae: globalCorr.mae, rmse: globalCorr.rmse,
      slope: globalCorr.slope, intercept: globalCorr.intercept,
    });
    results.global = { mae: globalCorr.mae, slope: globalCorr.slope, intercept: globalCorr.intercept };
    console.log(`${tag} global: slope=${globalCorr.slope.toFixed(4)}, intercept=${globalCorr.intercept.toFixed(3)}, MAE=${globalCorr.mae.toFixed(3)}`);
  }

  // Per-trigger corrections
  const byTriggerType = {};
  for (const row of rows) {
    const parsed = parseTrigger(row.trigger, row.period);
    if (!parsed) continue;
    // Normalize trigger to just the time pattern (e.g., "Q2 8:30" -> "Q2 8:30")
    const triggerKey = row.trigger.trim();
    if (!byTriggerType[triggerKey]) byTriggerType[triggerKey] = [];
    byTriggerType[triggerKey].push(row);
  }

  for (const [trigger, trigRows] of Object.entries(byTriggerType)) {
    if (trigRows.length < 30) continue; // Need minimal data per trigger

    const corr = fitOLS(
      trigRows.map((r) => r.proj),
      trigRows.map((r) => r.actual)
    );
    if (!corr) continue;

    const modelName = `in_app_linear_correction:${trigger}`;
    const meta = {
      weights: [corr.intercept, corr.slope],
      samples_used: trigRows.length,
      mae: corr.mae,
      rmse: corr.rmse,
      trained_at: new Date().toISOString(),
      trigger,
    };
    await saveModel(dbConn, leagueId, modelName, meta, trigRows.length, corr.mae);
    results[trigger] = { mae: corr.mae, slope: corr.slope, samples: trigRows.length };
  }

  console.log(`${tag} Done. ${Object.keys(results).length} correction models trained.`);
  return results;
}

/**
 * Simple OLS: y = a + b*x with sanity check on slope [0.5, 1.5].
 */
function fitOLS(xArr, yArr) {
  const n = xArr.length;
  if (n < 2) return null;

  let sumX = 0, sumY = 0, sumXX = 0, sumXY = 0;
  for (let i = 0; i < n; i++) {
    const x = xArr[i];
    const y = yArr[i];
    sumX += x;
    sumY += y;
    sumXX += x * x;
    sumXY += x * y;
  }

  const denom = n * sumXX - sumX * sumX;
  if (Math.abs(denom) < 1e-12) return null;

  let slope = (n * sumXY - sumX * sumY) / denom;
  let intercept = (sumY - slope * sumX) / n;

  // Sanity check: slope must be in [0.5, 1.5]
  if (slope < 0.5 || slope > 1.5) {
    console.log(`[ml-train] OLS slope ${slope.toFixed(4)} outside [0.5,1.5], clamping`);
    slope = Math.max(0.5, Math.min(1.5, slope));
    intercept = (sumY - slope * sumX) / n;
  }

  // Compute MAE and RMSE
  let maeSum = 0, rmseSum = 0;
  for (let i = 0; i < n; i++) {
    const pred = intercept + slope * xArr[i];
    const err = Math.abs(pred - yArr[i]);
    maeSum += err;
    rmseSum += err * err;
  }

  return {
    slope,
    intercept,
    mae: maeSum / n,
    rmse: Math.sqrt(rmseSum / n),
  };
}

// ---------------------------------------------------------------------------
// Model persistence helpers
// ---------------------------------------------------------------------------

async function saveModel(dbConn, leagueId, modelName, metadata, samplesUsed, mae) {
  await dbConn.query(
    `INSERT INTO ml_models (league_id, model_name, metadata, samples_used, total_mae, updated_at)
     VALUES ($1, $2, $3, $4, $5, NOW())
     ON CONFLICT (league_id, model_name)
     DO UPDATE SET metadata = $3, samples_used = $4, total_mae = $5, updated_at = NOW()`,
    [leagueId, modelName, JSON.stringify(metadata), samplesUsed, mae]
  );
}

async function logTrainingRun(dbConn, leagueId, modelName, samplesUsed, metrics) {
  try {
    await dbConn.query(
      `INSERT INTO ml_training_runs (league_id, model_name, samples_used, metrics, completed_at, status)
       VALUES ($1, $2, $3, $4, NOW(), 'completed')`,
      [leagueId, modelName, samplesUsed, JSON.stringify(metrics)]
    );
  } catch (e) {
    // Table may not exist yet if migration hasn't run; log but don't fail
    console.warn(`[ml-train] Could not log training run: ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Train all models for a league
// ---------------------------------------------------------------------------

async function trainAll(dbConn, leagueId) {
  const tag = `[ml-train] trainAll/${leagueId}`;
  console.log(`${tag} Starting full training...`);
  const startTime = Date.now();

  const results = {};

  try {
    results.quarter_projection = await trainQuarterProjection(dbConn, leagueId);
  } catch (e) {
    console.error(`${tag} quarter_projection failed:`, e.message);
    results.quarter_projection = { error: e.message };
  }

  try {
    results.pregame = await trainPregameModels(dbConn, leagueId);
  } catch (e) {
    console.error(`${tag} pregame failed:`, e.message);
    results.pregame = { error: e.message };
  }

  try {
    results.corrections = await trainCorrections(dbConn, leagueId);
  } catch (e) {
    console.error(`${tag} corrections failed:`, e.message);
    results.corrections = { error: e.message };
  }

  // Clear inference cache so new weights are picked up
  if (typeof mlInference.clearCache === 'function') {
    mlInference.clearCache();
    console.log(`${tag} Inference cache cleared.`);
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`${tag} All training complete in ${elapsed}s`);

  return results;
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

let _fastInterval = null;   // hourly: quarter_projection + corrections
let _slowInterval = null;   // 6-hourly: pregame models (heavier, needs cached_games)

function scheduleTraining(dbConn, intervalHours = 6) {
  if (_fastInterval) clearInterval(_fastInterval);
  if (_slowInterval) clearInterval(_slowInterval);

  const fastMs = 1 * 60 * 60 * 1000;          // 1 hour
  const slowMs = intervalHours * 60 * 60 * 1000; // 6 hours

  console.log(`[ml-train] Scheduling: quarter+corrections every 1h, pregame every ${intervalHours}h`);

  // Hourly: lightweight in-game models
  _fastInterval = setInterval(async () => {
    console.log('[ml-train] Hourly training (quarter + corrections) starting...');
    for (const leagueId of ALL_LEAGUES) {
      try {
        await trainQuarterProjection(dbConn, leagueId);
      } catch (e) {
        console.error(`[ml-train] quarter_projection failed for ${leagueId}:`, e.message);
      }
      try {
        await trainCorrections(dbConn, leagueId);
      } catch (e) {
        console.error(`[ml-train] corrections failed for ${leagueId}:`, e.message);
      }
    }
    if (typeof mlInference.clearCache === 'function') mlInference.clearCache();
    console.log('[ml-train] Hourly training complete.');
  }, fastMs);

  // 6-hourly: heavier pregame models
  _slowInterval = setInterval(async () => {
    console.log('[ml-train] Full training run (including pregame) starting...');
    for (const leagueId of ALL_LEAGUES) {
      try {
        await trainAll(dbConn, leagueId);
      } catch (e) {
        console.error(`[ml-train] Scheduled training failed for ${leagueId}:`, e.message);
      }
    }
    console.log('[ml-train] Full training run complete.');
  }, slowMs);
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = {
  trainAll,
  trainQuarterProjection,
  trainPregameModels,
  trainCorrections,
  scheduleTraining,
};
