const request = require('supertest');
const app = require('../src/index');
const db = require('../src/db');

async function clearTestData() {
  await db.query("DELETE FROM server_changes WHERE change_id LIKE 'test-%'");
  await db.query("DELETE FROM applied_changes WHERE change_id LIKE 'test-%'");
  await db.query('DELETE FROM elo_ratings WHERE league_id = $1 AND team_id = $2', ['nba', 't-dry']);
}

describe('Dry-run endpoint', () => {
  afterAll(async () => {
    await clearTestData();
  });

  test('dryrun reports would_apply=false for stale elo rating updates', async () => {
    await clearTestData();

    const insert = {
      change_id: `test-${Date.now()}-1`,
      table: 'elo_ratings',
      pk: JSON.stringify({ league_id: 'nba', team_id: 't-dry' }),
      op: 'insert',
      payload: {
        league_id: 'nba',
        team_id: 't-dry',
        team_name: 'T Dry',
        elo: 1500,
        last_updated: '2026-01-02T12:00:00Z',
      },
    };

    // apply the first change
    process.env.SYNC_API_TOKEN = 'test-token';
    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer test-token')
      .send({ device_id: 'test', last_server_seq: 0, changes: [insert] })
      .expect(200);

    // Older update: dryrun should report would_apply = false
    const oldUpdate = {
      change_id: `test-${Date.now()}-2`,
      table: 'elo_ratings',
      pk: JSON.stringify({ league_id: 'nba', team_id: 't-dry' }),
      op: 'insert',
      payload: {
        league_id: 'nba',
        team_id: 't-dry',
        team_name: 'T Dry',
        elo: 1600,
        last_updated: '2026-01-01T12:00:00Z',
      },
    };

    const res = await request(app)
      .post('/sync/dryrun')
      .send({ changes: [oldUpdate] })
      .expect(200);
    expect(res.body.results[0].would_apply).toBe(false);
    expect(res.body.results[0].reason).toBe('stale');
  });
});
