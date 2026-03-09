const request = require('supertest');
const app = require('../src/index');
const db = require('../src/db');

describe('Smoke: server + DB integration', () => {
  const changeId = `smoke-${Date.now()}`;

  test('health endpoint and DB available', async () => {
    const res = await request(app).get('/health').expect(200);
    expect(res.body.status).toBe('ok');
    expect(res.body.db).toBeDefined();
  });

  test('CORS preflight returns CORS headers', async () => {
    const res = await request(app)
      .options('/sync')
      .set('Origin', 'https://elitestatsbets.com')
      .set('Access-Control-Request-Method', 'POST')
      .expect(204);
    expect(res.headers['access-control-allow-origin']).toBeDefined();
    expect(res.headers['access-control-allow-methods']).toBeDefined();
  });

  test('POST /sync accepts change and persists it', async () => {
    const payload = {
      change_id: changeId,
      table: 'elo_ratings',
      pk: JSON.stringify({ league_id: 'smoke', team_id: 't1' }),
      op: 'insert',
      payload: { league_id: 'smoke', team_id: 't1', team_name: 'Smoke Team', elo: 1500 },
    };

    const token = process.env.SYNC_API_TOKEN || 'smoke-token';

    const res = await request(app)
      .post('/sync')
      .set('Authorization', `Bearer ${token}`)
      .send({ device_id: 'smoke-device', last_server_seq: 0, changes: [payload] })
      .expect(200);

    expect(res.body.applied).toContain(changeId);

    // Verify DB row exists
    const r = await db.query(
      'SELECT elo, team_name FROM elo_ratings WHERE league_id=$1 AND team_id=$2',
      ['smoke', 't1']
    );
    expect(r.rowCount).toBeGreaterThanOrEqual(1);
    expect(r.rows[0].team_name).toBe('Smoke Team');
  });

  afterAll(async () => {
    try {
      await db.query('DELETE FROM elo_ratings WHERE league_id = $1 AND team_id = $2', [
        'smoke',
        't1',
      ]);
      await db.query("DELETE FROM applied_changes WHERE change_id LIKE 'smoke-%'");
    } catch (e) {
      // ignore cleanup errors
    }
  });
});
