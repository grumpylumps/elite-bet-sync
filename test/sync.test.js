const request = require('supertest');
const app = require('../src/index');
const db = require('../src/db');

describe('POST /sync', () => {
  test('accepts a change and returns server_changes', async () => {
    const changeId = `test-${Date.now()}`;
    const payload = {
      change_id: changeId,
      table: 'elo_ratings',
      pk: JSON.stringify({ league_id: 'nba', team_id: 't1' }),
      op: 'insert',
      payload: { league_id: 'nba', team_id: 't1', team_name: 'Team 1', elo: 1500 },
    };

    process.env.SYNC_API_TOKEN = 'test-token';
    const res = await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer test-token')
      .send({ device_id: 'test-device', last_server_seq: 0, changes: [payload] })
      .expect(200);

    expect(res.body.applied).toContain(changeId);
    expect(res.body.server_changes.length).toBeGreaterThanOrEqual(1);
  });

  afterAll(async () => {
    // cleanup test records
    await db.query('DELETE FROM elo_ratings WHERE league_id = $1 AND team_id = $2', ['nba', 't1']);
    await db.query('DELETE FROM applied_changes WHERE change_id LIKE $1', ['test-%']);
    await db.query('DELETE FROM server_changes WHERE change_id LIKE $1', ['test-%']);
    await db.query('END');
    db.query('SELECT 1');
  });
});
