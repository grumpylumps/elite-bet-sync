const request = require('supertest');
const jwt = require('jsonwebtoken');
const app = require('../src/index');

const SYNC_BODY = { device_id: 'x', last_server_seq: 0, changes: [] };

describe('Auth – legacy static token', () => {
  beforeEach(() => {
    delete process.env.AUTH_JWT_SECRET;
    process.env.SYNC_API_TOKEN = 'test-token';
  });

  test('rejects request with no Authorization header', async () => {
    await request(app)
      .post('/sync')
      .send(SYNC_BODY)
      .expect(401);
  });

  test('rejects wrong token', async () => {
    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer bad-token')
      .send(SYNC_BODY)
      .expect(401);
  });

  test('accepts correct static token', async () => {
    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer test-token')
      .send(SYNC_BODY)
      .expect(200);
  });
});

describe('Auth – JWT', () => {
  const SECRET = 'test-jwt-secret';

  beforeEach(() => {
    delete process.env.SYNC_API_TOKEN;
    process.env.AUTH_JWT_SECRET = SECRET;
  });

  test('accepts a valid JWT', async () => {
    const token = jwt.sign({ sub: 'user-1' }, SECRET, { expiresIn: '1h' });
    await request(app)
      .post('/sync')
      .set('Authorization', `Bearer ${token}`)
      .send(SYNC_BODY)
      .expect(200);
  });

  test('rejects an expired JWT', async () => {
    const token = jwt.sign({ sub: 'user-1' }, SECRET, { expiresIn: '-1s' });
    await request(app)
      .post('/sync')
      .set('Authorization', `Bearer ${token}`)
      .send(SYNC_BODY)
      .expect(401);
  });

  test('rejects a JWT signed with wrong secret', async () => {
    const token = jwt.sign({ sub: 'user-1' }, 'wrong-secret');
    await request(app)
      .post('/sync')
      .set('Authorization', `Bearer ${token}`)
      .send(SYNC_BODY)
      .expect(401);
  });

  test('rejects a malformed token', async () => {
    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer not.a.jwt')
      .send(SYNC_BODY)
      .expect(401);
  });
});

describe('Auth – dev mode (no secret configured)', () => {
  beforeEach(() => {
    delete process.env.SYNC_API_TOKEN;
    delete process.env.AUTH_JWT_SECRET;
  });

  test('accepts any Bearer token when no secret is set', async () => {
    await request(app)
      .post('/sync')
      .set('Authorization', 'Bearer anything')
      .send(SYNC_BODY)
      .expect(200);
  });

  test('still rejects missing Authorization header', async () => {
    await request(app)
      .post('/sync')
      .send(SYNC_BODY)
      .expect(401);
  });
});
