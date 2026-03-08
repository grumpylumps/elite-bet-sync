const request = require('supertest');
const app = require('../src/index');

describe('Auth', () => {
  test('requires token for /sync', async () => {
    process.env.SYNC_API_TOKEN = 'test-token';
    await request(app)
      .post('/sync')
      .send({ device_id: 'x', last_server_seq: 0, changes: [] })
      .expect(401);
  });

  test('rejects wrong token for /sync', async () => {
    process.env.SYNC_API_TOKEN = 'test-token';
    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer bad-token')
      .send({ device_id: 'x', last_server_seq: 0, changes: [] })
      .expect(401);
  });

  test('accepts correct token for /sync', async () => {
    process.env.SYNC_API_TOKEN = 'test-token';
    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer test-token')
      .send({ device_id: 'x', last_server_seq: 0, changes: [] })
      .expect(200);
  });
});
