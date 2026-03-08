const request = require('supertest');
const app = require('../src/index');
const db = require('../src/db');

async function clearTestData() {
  await db.query("DELETE FROM server_changes WHERE change_id LIKE 'test-%'");
  await db.query("DELETE FROM applied_changes WHERE change_id LIKE 'test-%'");
  await db.query('DELETE FROM team_stats WHERE league_id = $1 AND team_id = $2', [
    'nba',
    't-merge',
  ]);
  await db.query('DELETE FROM game_odds WHERE league_id = $1 AND game_id = $2', ['nba', 'g-merge']);
}

describe('Field-level merges', () => {
  afterAll(async () => {
    await clearTestData();
  });

  test('team_stats: prefer payload with higher games_analyzed', async () => {
    await clearTestData();
    const changeOld = {
      change_id: `test-${Date.now()}-a`,
      table: 'team_stats',
      pk: JSON.stringify({ league_id: 'nba', team_id: 't-merge' }),
      op: 'insert',
      payload: {
        league_id: 'nba',
        team_id: 't-merge',
        team_name: 'Team M',
        games_analyzed: 5,
        wins: 3,
        losses: 2,
        ppg: 100,
      },
    };

    const changeNew = {
      change_id: `test-${Date.now()}-b`,
      table: 'team_stats',
      pk: JSON.stringify({ league_id: 'nba', team_id: 't-merge' }),
      op: 'insert',
      payload: {
        league_id: 'nba',
        team_id: 't-merge',
        team_name: 'Team M',
        games_analyzed: 10,
        wins: 7,
        losses: 3,
        ppg: 105,
      },
    };

    process.env.SYNC_API_TOKEN = 'test-token';
    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer test-token')
      .send({ device_id: 'test', last_server_seq: 0, changes: [changeOld] })
      .expect(200);
    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer test-token')
      .send({ device_id: 'test', last_server_seq: 0, changes: [changeNew] })
      .expect(200);

    const r = await db.query(
      'SELECT games_analyzed, wins, losses, ppg FROM team_stats WHERE league_id=$1 AND team_id=$2',
      ['nba', 't-merge']
    );
    expect(r.rowCount).toBe(1);
    expect(r.rows[0].games_analyzed).toBe(10);
    expect(parseFloat(r.rows[0].ppg)).toBe(105);
  });

  test('game_odds: apply when total_line changes', async () => {
    await clearTestData();
    const ch1 = {
      change_id: `test-${Date.now()}-c`,
      table: 'game_odds',
      pk: JSON.stringify({ league_id: 'nba', game_id: 'g-merge' }),
      op: 'insert',
      payload: { league_id: 'nba', game_id: 'g-merge', total_line: 200 },
    };
    const ch2 = {
      change_id: `test-${Date.now()}-d`,
      table: 'game_odds',
      pk: JSON.stringify({ league_id: 'nba', game_id: 'g-merge' }),
      op: 'update',
      payload: { league_id: 'nba', game_id: 'g-merge', total_line: 210 },
    };

    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer test-token')
      .send({ device_id: 'test', last_server_seq: 0, changes: [ch1] })
      .expect(200);
    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer test-token')
      .send({ device_id: 'test', last_server_seq: 0, changes: [ch2] })
      .expect(200);

    const r = await db.query('SELECT total_line FROM game_odds WHERE league_id=$1 AND game_id=$2', [
      'nba',
      'g-merge',
    ]);
    expect(r.rowCount).toBe(1);
    expect(parseFloat(r.rows[0].total_line)).toBe(210);
  });
});
