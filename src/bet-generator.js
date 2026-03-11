'use strict';

const crypto = require('crypto');
const db = require('./db');

// ---------------------------------------------------------------------------
// League configurations (mirrored from Flutter league_configs.dart)
// ---------------------------------------------------------------------------
const LEAGUE_CONFIGS = {
  nba: {
    id: 'nba', periodType: 'quarter', periodCount: 4,
    periodLengthSeconds: 720, otLengthSeconds: 300,
    defaultPeriodAvg: 56.0, maxPeriodScore: 90.0,
    bayesianPow: 0.3, isBasketball: true, isNcaa: false,
    triggers: [
      { name: '7:00', targetSec: 420, add: 40 },
      { name: '6:00', targetSec: 360, add: 30 },
      { name: '5:00', targetSec: 300, add: 25 },
      { name: '4:00', targetSec: 240, add: 21 },
      { name: '3:30', targetSec: 210, add: 16 },
      { name: '2:00', targetSec: 120, add: 11 },
    ],
    otTriggers: [{ name: '3:30', targetSec: 210, add: 15 }],
  },
  wnba: {
    id: 'wnba', periodType: 'quarter', periodCount: 4,
    periodLengthSeconds: 600, otLengthSeconds: 300,
    defaultPeriodAvg: 42.0, maxPeriodScore: 70.0,
    bayesianPow: 0.45, isBasketball: true, isNcaa: false,
    triggers: [
      { name: '7:00', targetSec: 420, add: 30 },
      { name: '6:00', targetSec: 360, add: 25 },
      { name: '5:00', targetSec: 300, add: 20 },
      { name: '4:00', targetSec: 240, add: 16 },
      { name: '3:30', targetSec: 210, add: 14 },
      { name: '2:00', targetSec: 120, add: 8 },
    ],
    otTriggers: [{ name: '3:30', targetSec: 210, add: 14 }],
  },
  ncaam: {
    id: 'ncaam', periodType: 'half', periodCount: 2,
    periodLengthSeconds: 1200, otLengthSeconds: 300,
    defaultPeriodAvg: 70.0, maxPeriodScore: 110.0,
    bayesianPow: 0.45, isBasketball: true, isNcaa: true,
    triggers: [
      { name: '10:00', targetSec: 600, add: 40 },
      { name: '7:00', targetSec: 420, add: 30 },
      { name: '6:00', targetSec: 360, add: 25 },
      { name: '5:00', targetSec: 300, add: 20 },
      { name: '4:00', targetSec: 240, add: 17 },
      { name: '3:30', targetSec: 210, add: 15 },
      { name: '2:00', targetSec: 120, add: 14 },
    ],
    otTriggers: [{ name: '3:30', targetSec: 210, add: 14 }],
  },
  ncaaw: {
    id: 'ncaaw', periodType: 'quarter', periodCount: 4,
    periodLengthSeconds: 600, otLengthSeconds: 300,
    defaultPeriodAvg: 35.0, maxPeriodScore: 75.0,
    bayesianPow: 0.6, isBasketball: true, isNcaa: true,
    triggers: [
      { name: '7:00', targetSec: 420, add: 25 },
      { name: '6:00', targetSec: 360, add: 20 },
      { name: '5:00', targetSec: 300, add: 16 },
      { name: '4:00', targetSec: 240, add: 13 },
      { name: '3:30', targetSec: 210, add: 11 },
      { name: '2:00', targetSec: 120, add: 7 },
    ],
    otTriggers: [{ name: '3:30', targetSec: 210, add: 12 }],
  },
  nfl: {
    id: 'nfl', periodType: 'quarter', periodCount: 4,
    periodLengthSeconds: 900, otLengthSeconds: 600,
    defaultPeriodAvg: 11.0, maxPeriodScore: 28.0,
    bayesianPow: 0.5, isBasketball: false, isNcaa: false,
    triggers: [
      { name: '10:00', targetSec: 600, add: 7 },
      { name: '7:30', targetSec: 450, add: 5 },
      { name: '5:00', targetSec: 300, add: 4 },
    ],
    otTriggers: [
      { name: '7:30', targetSec: 450, add: 5 },
      { name: '5:00', targetSec: 300, add: 3 },
    ],
  },
  cfb: {
    id: 'cfb', periodType: 'quarter', periodCount: 4,
    periodLengthSeconds: 900, otLengthSeconds: 0,
    defaultPeriodAvg: 12.0, maxPeriodScore: 35.0,
    bayesianPow: 0.5, isBasketball: false, isNcaa: false,
    triggers: [
      { name: '10:00', targetSec: 600, add: 8 },
      { name: '7:30', targetSec: 450, add: 6 },
      { name: '5:00', targetSec: 300, add: 4 },
    ],
    otTriggers: [],
  },
  mlb: {
    id: 'mlb', periodType: 'inning', periodCount: 9,
    periodLengthSeconds: 0, otLengthSeconds: 0,
    defaultPeriodAvg: 0.94, maxPeriodScore: 10.0,
    bayesianPow: 0.3, isBasketball: false, isNcaa: false, isBaseball: true,
    triggers: [
      { name: 'Top', targetSec: 0, add: 0 },
      { name: 'Bot', targetSec: 0, add: 0 },
    ],
    otTriggers: [],
    scoringDistribution: {
      1: 0.125, 2: 0.105, 3: 0.105, 4: 0.100, 5: 0.115,
      6: 0.115, 7: 0.120, 8: 0.110, 9: 0.105,
    },
  },
  cbb: {
    id: 'cbb', periodType: 'inning', periodCount: 9,
    periodLengthSeconds: 0, otLengthSeconds: 0,
    defaultPeriodAvg: 1.28, maxPeriodScore: 12.0,
    bayesianPow: 0.3, isBasketball: false, isNcaa: false, isBaseball: true,
    triggers: [
      { name: 'Top', targetSec: 0, add: 0 },
      { name: 'Bot', targetSec: 0, add: 0 },
    ],
    otTriggers: [],
    scoringDistribution: {
      1: 0.125, 2: 0.105, 3: 0.105, 4: 0.100, 5: 0.115,
      6: 0.115, 7: 0.120, 8: 0.110, 9: 0.105,
    },
  },
  nhl: {
    id: 'nhl', periodType: 'period', periodCount: 3,
    periodLengthSeconds: 1200, otLengthSeconds: 300,
    defaultPeriodAvg: 2.0, maxPeriodScore: 6.0,
    bayesianPow: 0.5, isBasketball: false, isNcaa: false,
    triggers: [
      { name: '15:00', targetSec: 900, add: 2 },
      { name: '12:00', targetSec: 720, add: 1 },
      { name: '10:00', targetSec: 600, add: 1 },
      { name: '7:00', targetSec: 420, add: 1 },
      { name: '5:00', targetSec: 300, add: 1 },
      { name: '4:00', targetSec: 240, add: 1 },
      { name: '3:00', targetSec: 180, add: 0 },
      { name: '2:30', targetSec: 150, add: 0 },
    ],
    otTriggers: [
      { name: '3:00', targetSec: 180, add: 1 },
      { name: '1:30', targetSec: 90, add: 0 },
    ],
  },
};

// ---------------------------------------------------------------------------
// Model parameters (mirrored from Flutter model_params.dart)
// ---------------------------------------------------------------------------
const MODEL_PARAMS = {
  progressBonusStart: 0.6,
  progressBonusFactor: 0.04,
  bonusEfficiencyBoost: 0.025,
  bestWindowStartSec: 360, // 6:00
  bestWindowEndSec: 210,   // 3:30
};

// ---------------------------------------------------------------------------
// Fired triggers dedup: { "league_gameId_period_trigger": true }
// Cleared when a game ends or is removed from live tracking.
// ---------------------------------------------------------------------------
const _firedTriggers = new Map();

function triggerKey(league, gameId, period, triggerName) {
  return `${league}_${gameId}_${period}_${triggerName}`;
}

// ---------------------------------------------------------------------------
// Clock parsing (mirrors Flutter clock_parser.dart)
// ---------------------------------------------------------------------------
function parseClockToSeconds(clock) {
  if (!clock) return 0;
  const cleaned = clock.trim();
  if (cleaned.includes(':')) {
    const [min, sec] = cleaned.split(':');
    return (parseInt(min) || 0) * 60 + (parseInt(sec) || 0);
  }
  if (cleaned.includes('.')) return Math.floor(parseFloat(cleaned) || 0);
  return parseInt(cleaned) || 0;
}

// ---------------------------------------------------------------------------
// Trigger matching (asymmetric window: 5s early, 10s late)
// ---------------------------------------------------------------------------
function getTriggerName(secondsLeft, triggers) {
  for (const t of triggers) {
    if (secondsLeft >= (t.targetSec - 10) && secondsLeft <= (t.targetSec + 5)) {
      return t.name;
    }
  }
  return null;
}

function getTriggerAdd(secondsLeft, triggers) {
  const sorted = [...triggers].sort((a, b) => b.targetSec - a.targetSec);
  for (const t of sorted) {
    if (secondsLeft >= t.targetSec) return t.add;
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Projection engine (mirrors Flutter projection_service.dart)
// ---------------------------------------------------------------------------
function normalCdf(x) {
  const a1 = 0.254829592, a2 = -0.284496736, a3 = 1.421413741;
  const a4 = -1.453152027, a5 = 1.061405429, p = 0.3275911;
  const sign = x < 0 ? -1 : 1;
  const absX = Math.abs(x);
  const t = 1.0 / (1.0 + p * absX);
  const y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) *
    t * Math.exp(-absX * absX / 2);
  return 0.5 * (1.0 + sign * y);
}

function getPeriodCount(leagueId) {
  const cfg = LEAGUE_CONFIGS[leagueId];
  return cfg ? cfg.periodCount : 4;
}

function projectPeriodTotal({
  currentTotal, secondsLeft, periodLength, defaultAvg,
  leagueId, pregameTotalLine, period, maxScore,
}) {
  // Baseball: inning-based projection
  if (periodLength <= 0 && LEAGUE_CONFIGS[leagueId]?.isBaseball) {
    return projectMLBInning({ currentTotal, period: period || 1, defaultAvg, pregameTotalLine, maxScore, leagueId });
  }
  if (periodLength <= 0) return currentTotal + defaultAvg;

  const elapsed = periodLength - secondsLeft;
  if (elapsed <= 0) return currentTotal + defaultAvg;

  const remainingMinutes = secondsLeft / 60.0;
  const elapsedMinutes = elapsed / 60.0;
  const progress = elapsed / periodLength;

  const cfg = LEAGUE_CONFIGS[leagueId] || {};
  const bayesianPow = cfg.bayesianPow || 0.3;
  const bayesianWeight = Math.pow(progress, bayesianPow);

  const periodCount = getPeriodCount(leagueId);
  const expectedPeriodTotal = (pregameTotalLine && pregameTotalLine > 0)
    ? (pregameTotalLine / periodCount) + 0.5
    : defaultAvg + 0.5;

  const livePpm = currentTotal / elapsedMinutes;
  const linearProj = currentTotal + livePpm * remainingMinutes;

  let finalProj;

  if (cfg.isBasketball) {
    const isNcaa = cfg.isNcaa;
    const possessionsPerMinute = isNcaa ? 1.75 : 2.08;
    const estPossessionsRemaining = remainingMinutes * possessionsPerMinute;

    const gameLengthMinutes = isNcaa ? 40 : 48;
    const totalExpectedPossessions = gameLengthMinutes * possessionsPerMinute;
    const priorPpp = (pregameTotalLine && pregameTotalLine > 0)
      ? (pregameTotalLine / totalExpectedPossessions)
      : (isNcaa ? 1.03 : 1.10);

    const estPossessionsElapsed = elapsedMinutes * possessionsPerMinute;
    const livePpp = estPossessionsElapsed > 0
      ? (currentTotal / estPossessionsElapsed)
      : priorPpp;

    let blendedPpp = (livePpp * bayesianWeight) + (priorPpp * (1 - bayesianWeight));

    // Late-period progress bonus
    if (progress > MODEL_PARAMS.progressBonusStart) {
      const lastRegPeriod = periodCount;
      if (period != null && period === lastRegPeriod) {
        const effectiveFactor = MODEL_PARAMS.progressBonusFactor;
        const bonusMultiplier = 1.0 +
          (effectiveFactor * (progress - MODEL_PARAMS.progressBonusStart) /
            (1.0 - MODEL_PARAMS.progressBonusStart));
        blendedPpp *= bonusMultiplier;
      }
    }

    const possessionProj = currentTotal + (estPossessionsRemaining * blendedPpp);

    // Regression-smoothed projection
    const smoothingSeconds = 180.0;
    const avgPpm = expectedPeriodTotal / (periodLength / 60.0);
    const smoothedPpm = (currentTotal + (avgPpm * (smoothingSeconds / 60.0))) /
      ((elapsed + smoothingSeconds) / 60.0);
    const smoothedLinearProj = currentTotal + (smoothedPpm * remainingMinutes);

    finalProj = possessionProj * bayesianWeight +
      smoothedLinearProj * (1 - bayesianWeight);
  } else {
    finalProj = (linearProj * bayesianWeight) +
      (expectedPeriodTotal * (1 - bayesianWeight));
  }

  // Safety clamps
  if (maxScore != null && !['nfl', 'cfb'].includes(leagueId)) {
    const upper = Math.max(currentTotal, maxScore);
    return Math.min(Math.max(finalProj, currentTotal), upper);
  }
  return Math.min(Math.max(finalProj, currentTotal), 250.0);
}

// MLB inning projection
const MLB_SCORING_DIST = {
  1: 0.125, 2: 0.105, 3: 0.105, 4: 0.100, 5: 0.115,
  6: 0.115, 7: 0.120, 8: 0.110, 9: 0.105,
};

function projectMLBInning({ currentTotal, period, defaultAvg, pregameTotalLine, maxScore, leagueId }) {
  const totalInnings = 9;
  const clampedPeriod = Math.max(1, Math.min(period, 9));
  const distWeight = MLB_SCORING_DIST[clampedPeriod] || (1.0 / totalInnings);
  const scaleFactor = distWeight / (1.0 / totalInnings);

  const basePerInning = (pregameTotalLine && pregameTotalLine > 0)
    ? pregameTotalLine / totalInnings
    : defaultAvg;

  const expectedInningTotal = basePerInning * scaleFactor;
  const halfInningExpected = expectedInningTotal / 2.0;

  if (currentTotal > 0) {
    return currentTotal + Math.min(Math.max(halfInningExpected * 0.3, 0.1), 0.5);
  }
  return Math.max(expectedInningTotal, defaultAvg);
}

// ---------------------------------------------------------------------------
// ESPN summary data extraction
// ---------------------------------------------------------------------------
function extractGameState(league, gameId, summaryJson) {
  const comp = (summaryJson?.header?.competitions || [])[0];
  if (!comp) return null;

  const status = comp.status || {};
  const state = status?.type?.state;
  if (state !== 'in') return null;

  const period = status.period || 1;
  const clock = status.displayClock || '0:00';
  const secondsLeft = parseClockToSeconds(clock);

  // Extract competitors
  let homeTeam = '', awayTeam = '', homeScore = 0, awayScore = 0;
  const homeLinescores = [], awayLinescores = [];

  for (const c of (comp.competitors || [])) {
    const name = c.team?.displayName || c.team?.abbreviation || 'Unknown';
    const score = parseInt(c.score) || 0;
    const linescores = (c.linescores || []).map(ls =>
      parseFloat(ls.displayValue || ls.value) || 0
    );
    if (c.homeAway === 'home') {
      homeTeam = name; homeScore = score;
      homeLinescores.push(...linescores);
    } else {
      awayTeam = name; awayScore = score;
      awayLinescores.push(...linescores);
    }
  }

  // Current period score
  const periodIdx = period - 1;
  const homePeriodScore = homeLinescores[periodIdx] || 0;
  const awayPeriodScore = awayLinescores[periodIdx] || 0;
  const currentPeriodScore = homePeriodScore + awayPeriodScore;

  // Over/under from pickcenter
  let totalLine = null;
  const pickcenter = summaryJson?.pickcenter;
  if (Array.isArray(pickcenter) && pickcenter.length > 0) {
    totalLine = pickcenter[0].overUnder || null;
  }

  // For baseball: detect top/bottom inning from status detail
  let isTopInning = null;
  const detail = status?.type?.detail || '';
  if (detail.toLowerCase().includes('top')) isTopInning = true;
  else if (detail.toLowerCase().includes('bot')) isTopInning = false;
  // Also check shortDetail
  const shortDetail = status?.type?.shortDetail || '';
  if (isTopInning === null) {
    if (shortDetail.toLowerCase().includes('top')) isTopInning = true;
    else if (shortDetail.toLowerCase().includes('bot')) isTopInning = false;
  }

  return {
    gameId, league, period, clock, secondsLeft,
    homeTeam, awayTeam, homeScore, awayScore,
    homeLinescores, awayLinescores,
    currentPeriodScore, totalLine, isTopInning,
    completed: status?.type?.completed === true,
  };
}

// ---------------------------------------------------------------------------
// Core bet generation logic (mirrors Flutter game_sync_service.dart)
// ---------------------------------------------------------------------------
function generateBet(gameState, leagueConfig) {
  const {
    league, gameId, period, secondsLeft, currentPeriodScore,
    totalLine, homeTeam, awayTeam, isTopInning,
  } = gameState;

  const isOt = period > leagueConfig.periodCount;
  const periodLength = isOt ? leagueConfig.otLengthSeconds : leagueConfig.periodLengthSeconds;
  const triggers = isOt ? leagueConfig.otTriggers : leagueConfig.triggers;

  if (!triggers || triggers.length === 0) return null;

  // Trigger detection
  let triggerName;
  if (leagueConfig.isBaseball) {
    if (isTopInning === true) triggerName = 'Top';
    else if (isTopInning === false) triggerName = 'Bot';
    else return null;
  } else {
    triggerName = getTriggerName(secondsLeft, triggers);
  }
  if (!triggerName) return null;

  // Dedup
  const key = triggerKey(league, gameId, period, triggerName);
  if (_firedTriggers.has(key)) return null;

  // Minimum score check (basketball can fire at 0 for quarter start)
  if (currentPeriodScore <= 0 && !leagueConfig.isBasketball && !leagueConfig.isBaseball) {
    return null;
  }

  // Calculate period line = currentPeriodScore + triggerAdd
  let periodLine;
  if (leagueConfig.isBaseball) {
    const totalInnings = 9;
    const clampedPeriod = Math.max(1, Math.min(period, 9));
    const dist = leagueConfig.scoringDistribution || MLB_SCORING_DIST;
    const distWeight = dist[clampedPeriod] || (1.0 / totalInnings);
    const scaleFactor = distWeight / (1.0 / totalInnings);
    const basePerInning = (totalLine && totalLine > 0)
      ? totalLine / totalInnings : leagueConfig.defaultPeriodAvg;
    periodLine = basePerInning * scaleFactor;
  } else {
    const triggerAdd = getTriggerAdd(secondsLeft, triggers);
    periodLine = currentPeriodScore + triggerAdd;
  }

  // Project period total
  const periodProj = projectPeriodTotal({
    currentTotal: currentPeriodScore,
    secondsLeft,
    periodLength,
    defaultAvg: leagueConfig.defaultPeriodAvg,
    leagueId: league,
    pregameTotalLine: totalLine,
    period,
    maxScore: leagueConfig.maxPeriodScore,
  });

  // Basketball trigger floor: if proj < line, clamp up to line
  let adjustedProj = periodProj;
  if (leagueConfig.isBasketball && adjustedProj < periodLine) {
    adjustedProj = periodLine;
  }

  // Round to nearest 0.5
  const roundedProj = Math.round(adjustedProj * 2) / 2.0;

  // Edge and probability
  const periodEdge = roundedProj - periodLine;
  const stdDev = 4.5;
  const overProb = normalCdf(periodEdge / stdDev);
  const direction = overProb >= 0.5 ? 'OVER' : 'UNDER';
  const winProb = Math.min(Math.max(Math.max(overProb, 1 - overProb), 0.5), 0.95);

  // Edge floor check (skip if edge too small)
  const edgePercent = roundedProj > 0 ? Math.abs(periodEdge) / roundedProj : 0;
  if (edgePercent < 0.01) return null; // 1% minimum edge

  // Mark as fired
  _firedTriggers.set(key, true);

  const bet = {
    league_id: league,
    game_id: gameId,
    period,
    trigger: triggerName,
    line: Math.round(periodLine * 100) / 100,
    proj: roundedProj,
    edge: Math.round(periodEdge * 100) / 100,
    probability: Math.round(winProb * 1000) / 1000,
    direction,
    captured_at: new Date().toISOString(),
    capture_type: null,
    actual: null,
    result: null,
    result_logged_at: null,
    stake: 100,
    home_team: homeTeam,
    away_team: awayTeam,
  };

  // Check for BEST window (basketball only, 6:00-3:30)
  let bestBet = null;
  if (leagueConfig.isBasketball &&
      secondsLeft >= MODEL_PARAMS.bestWindowEndSec &&
      secondsLeft <= MODEL_PARAMS.bestWindowStartSec &&
      winProb > 0.5) {
    bestBet = {
      ...bet,
      trigger: 'BEST',
      capture_type: 'BEST',
    };
  }

  return { bet, bestBet };
}

// ---------------------------------------------------------------------------
// Persist bet to database + server_changes (so Flutter clients pick it up)
// ---------------------------------------------------------------------------
async function persistBet(client, betRow) {
  const {
    league_id, game_id, period, trigger, line, proj, edge,
    probability, direction, captured_at, capture_type,
    actual, result, result_logged_at, stake, home_team, away_team,
  } = betRow;

  // Upsert into bet_logs
  const upsertSql = `
    INSERT INTO bet_logs (league_id, game_id, period, trigger, line, proj, edge,
      probability, direction, captured_at, capture_type, actual, result,
      result_logged_at, stake, home_team, away_team)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
    ON CONFLICT (league_id, game_id, period, trigger) DO NOTHING
  `;
  const params = [
    league_id, game_id, period, trigger, line, proj, edge,
    probability, direction, captured_at, capture_type, actual, result,
    result_logged_at, stake, home_team, away_team,
  ];

  const res = await client.query(upsertSql, params);

  // If inserted (not a conflict), also write to server_changes so sync clients
  // pick up the new bet_log row.
  if (res.rowCount > 0) {
    const changeId = crypto.randomUUID();
    const pk = JSON.stringify({ league_id, game_id, period, trigger });
    await client.query(
      `INSERT INTO server_changes (table_name, pk, op, payload, change_id)
       VALUES ('bet_logs', $1, 'INSERT', $2, $3)`,
      [pk, JSON.stringify(betRow), changeId]
    );
    return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Grade completed games: fill in actual period totals and WIN/LOSS/PUSH
// ---------------------------------------------------------------------------
async function gradeCompletedGame(league, gameId, summaryJson) {
  const comp = (summaryJson?.header?.competitions || [])[0];
  if (!comp) return;

  // Build period totals from linescores
  const periodTotals = {};
  for (const c of (comp.competitors || [])) {
    for (let i = 0; i < (c.linescores || []).length; i++) {
      const val = parseFloat(c.linescores[i]?.displayValue || c.linescores[i]?.value) || 0;
      periodTotals[i + 1] = (periodTotals[i + 1] || 0) + val;
    }
  }

  if (Object.keys(periodTotals).length === 0) return;

  let client;
  try {
    client = await db.getClient();

    // Find ungraded bet_logs for this game
    const ungraded = await client.query(
      `SELECT id, period, trigger, line, direction
       FROM bet_logs
       WHERE league_id = $1 AND game_id = $2 AND result IS NULL`,
      [league, gameId]
    );

    if (ungraded.rowCount === 0) return;

    await client.query('BEGIN');
    let gradedCount = 0;

    for (const row of ungraded.rows) {
      const actual = periodTotals[row.period];
      if (actual == null) continue;

      let result;
      if (row.line == null) {
        result = null;
      } else if (actual === row.line) {
        result = 'PUSH';
      } else if (row.direction === 'OVER') {
        result = actual > row.line ? 'WIN' : 'LOSS';
      } else {
        result = actual < row.line ? 'WIN' : 'LOSS';
      }

      if (result) {
        const now = new Date().toISOString();
        await client.query(
          `UPDATE bet_logs SET actual = $1, result = $2, result_logged_at = $3
           WHERE id = $4`,
          [actual, result, now, row.id]
        );

        // Write grade update to server_changes for sync
        const changeId = crypto.randomUUID();
        const pk = JSON.stringify({
          league_id: league, game_id: gameId,
          period: row.period, trigger: row.trigger,
        });
        const payload = { actual, result, result_logged_at: now };
        await client.query(
          `INSERT INTO server_changes (table_name, pk, op, payload, change_id)
           VALUES ('bet_logs', $1, 'UPDATE', $2, $3)`,
          [pk, JSON.stringify(payload), changeId]
        );
        gradedCount++;
      }
    }

    await client.query('COMMIT');
    if (gradedCount > 0) {
      console.log(`[bet-gen] Graded ${gradedCount} bets for ${league}/${gameId}`);
    }
  } catch (e) {
    if (client) try { await client.query('ROLLBACK'); } catch (_) {}
    console.error(`[bet-gen] Grading error ${league}/${gameId}:`, e.message);
  } finally {
    if (client) try { client.release(); } catch (_) {}
  }
}

// ---------------------------------------------------------------------------
// Main processing function: called from the ESPN poll loop with live game data
// ---------------------------------------------------------------------------
async function processLiveGame(league, gameId, summaryJson) {
  const cfg = LEAGUE_CONFIGS[league];
  if (!cfg) return;

  const gameState = extractGameState(league, gameId, summaryJson);
  if (!gameState) return;

  const result = generateBet(gameState, cfg);
  if (!result) return;

  let client;
  try {
    client = await db.getClient();
    await client.query('BEGIN');

    const inserted = await persistBet(client, result.bet);
    if (inserted) {
      console.log(
        `[bet-gen] ${league.toUpperCase()} ${gameState.awayTeam} @ ${gameState.homeTeam} ` +
        `P${gameState.period} ${result.bet.trigger}: ` +
        `line=${result.bet.line} proj=${result.bet.proj} edge=${result.bet.edge} ` +
        `${result.bet.direction} (${(result.bet.probability * 100).toFixed(1)}%)`
      );
    }

    if (result.bestBet) {
      const bestInserted = await persistBet(client, result.bestBet);
      if (bestInserted) {
        console.log(`[bet-gen] ${league.toUpperCase()} BEST bet logged for ${gameId} P${gameState.period}`);
      }
    }

    await client.query('COMMIT');
  } catch (e) {
    if (client) try { await client.query('ROLLBACK'); } catch (_) {}
    console.error(`[bet-gen] Error processing ${league}/${gameId}:`, e.message);
  } finally {
    if (client) try { client.release(); } catch (_) {}
  }
}

// ---------------------------------------------------------------------------
// Cleanup fired triggers for games that are no longer live
// ---------------------------------------------------------------------------
function cleanupFiredTriggers(activeLiveGames) {
  const activeKeys = new Set();
  for (const [league, gameIds] of Object.entries(activeLiveGames)) {
    for (const gid of gameIds) {
      activeKeys.add(`${league}_${gid}_`);
    }
  }

  for (const key of _firedTriggers.keys()) {
    const prefix = key.substring(0, key.lastIndexOf('_', key.lastIndexOf('_') - 1) + 1);
    // Check if any active game matches this prefix pattern
    let stillActive = false;
    for (const ak of activeKeys) {
      if (key.startsWith(ak.substring(0, ak.lastIndexOf('_') + 1))) {
        // Match on league_gameId prefix
        const parts = key.split('_');
        const gamePrefix = `${parts[0]}_${parts[1]}_`;
        for (const ak2 of activeKeys) {
          if (ak2 === gamePrefix) { stillActive = true; break; }
        }
        break;
      }
    }
    // Simpler approach: extract league and gameId from key
    const parts = key.split('_');
    if (parts.length >= 4) {
      const keyLeague = parts[0];
      const keyGameId = parts[1];
      const gameSet = activeLiveGames[keyLeague];
      if (!gameSet || !gameSet.has(keyGameId)) {
        _firedTriggers.delete(key);
      }
    }
  }
}

module.exports = {
  LEAGUE_CONFIGS,
  processLiveGame,
  gradeCompletedGame,
  cleanupFiredTriggers,
  // Exported for testing
  extractGameState,
  generateBet,
  parseClockToSeconds,
  getTriggerName,
  projectPeriodTotal,
  normalCdf,
};
