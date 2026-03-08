const request = require('supertest');
const app = require('../src/index');
const db = require('../src/db');

async function clearTestData() {
  await db.query("DELETE FROM server_changes WHERE change_id LIKE 'test-%'");
  await db.query("DELETE FROM applied_changes WHERE change_id LIKE 'test-%'");
  await db.query('DELETE FROM elo_ratings WHERE league_id = $1 AND team_id = $2', [
    'nba',
    't-test',
  ]);
  await db.query(
    'DELETE FROM bet_logs WHERE league_id = $1 AND game_id = $2 AND period = $3 AND trigger = $4',
    ['nba', 'g-test', 1, 't1']
  );
}

describe('Conflict resolution', () => {
  afterAll(async () => {
    await clearTestData();
  });

  test('elo_ratings: newer last_updated wins; older is ignored', async () => {
    await clearTestData();

    const change1 = {
      change_id: `test-${Date.now()}-1`,
      table: 'elo_ratings',
      pk: JSON.stringify({ league_id: 'nba', team_id: 't-test' }),
      op: 'insert',
      payload: {
        league_id: 'nba',
        team_id: 't-test',
        team_name: 'Team T',
        elo: 1500,
        last_updated: '2026-01-01T12:00:00Z',
      },
    };

    process.env.SYNC_API_TOKEN = 'test-token';
    const res1 = await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer test-token')
      .send({ device_id: 'test', last_server_seq: 0, changes: [change1] })
      .expect(200);
    expect(res1.body.applied).toContain(change1.change_id);

    // older update - should be ignored
    const change2 = {
      change_id: `test-${Date.now()}-2`,
      table: 'elo_ratings',
      pk: JSON.stringify({ league_id: 'nba', team_id: 't-test' }),
      op: 'insert',
      payload: {
        league_id: 'nba',
        team_id: 't-test',
        team_name: 'Team T',
        elo: 1600,
        last_updated: '2025-12-31T12:00:00Z',
      },
    };

    const res2 = await request(app)
      .post('/sync')
      .send({ device_id: 'test', last_server_seq: 0, changes: [change2] })
      .expect(200);
    expect(res2.body.applied).toContain(change2.change_id);

    // Verify DB value remains 1500
    const r = await db.query(
      'SELECT elo, last_updated FROM elo_ratings WHERE league_id=$1 AND team_id=$2',
      ['nba', 't-test']
    );
    expect(r.rowCount).toBe(1);
    expect(parseFloat(r.rows[0].elo)).toBe(1500);
  });

  test('bet_logs: result applied when newer, older result ignored', async () => {
    await clearTestData();

    const changeInsert = {
      change_id: `test-${Date.now()}-3`,
      table: 'bet_logs',
      pk: JSON.stringify({ league_id: 'nba', game_id: 'g-test', period: 1, trigger: 't1' }),
      op: 'insert',
      payload: {
        league_id: 'nba',
        game_id: 'g-test',
        period: 1,
        trigger: 't1',
        captured_at: '2026-01-01T10:00:00Z',
      },
    };

    await request(app)
      .post('/sync')
      .send({ device_id: 'test', last_server_seq: 0, changes: [changeInsert] })
      .expect(200);

    const changeResultNew = {
      change_id: `test-${Date.now()}-4`,
      table: 'bet_logs',
      pk: JSON.stringify({ league_id: 'nba', game_id: 'g-test', period: 1, trigger: 't1' }),
      op: 'update',
      payload: {
        league_id: 'nba',
        game_id: 'g-test',
        period: 1,
        trigger: 't1',
        result: 'WIN',
        result_logged_at: '2026-01-01T12:00:00Z',
      },
    };

    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer test-token')
      .send({ device_id: 'test', last_server_seq: 0, changes: [changeResultNew] })
      .expect(200);

    // older conflicting result -> should be ignored
    const changeResultOld = {
      change_id: `test-${Date.now()}-5`,
      table: 'bet_logs',
      pk: JSON.stringify({ league_id: 'nba', game_id: 'g-test', period: 1, trigger: 't1' }),
      op: 'update',
      payload: {
        league_id: 'nba',
        game_id: 'g-test',
        period: 1,
        trigger: 't1',
        result: 'LOSS',
        result_logged_at: '2026-01-01T11:00:00Z',
      },
    };

    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer test-token')
      .send({ device_id: 'test', last_server_seq: 0, changes: [changeResultOld] })
      .expect(200);

    // Verify DB value is WIN
    const r = await db.query(
      'SELECT result, result_logged_at FROM bet_logs WHERE league_id=$1 AND game_id=$2 AND period=$3 AND trigger=$4',
      ['nba', 'g-test', 1, 't1']
    );
    expect(r.rowCount).toBe(1);
    expect(r.rows[0].result).toBe('WIN');
  });
});
