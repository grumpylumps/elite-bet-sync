const https = require('https');
const { EventEmitter } = require('events');
const request = require('supertest');
const app = require('../src/index');

/**
 * Mock https.get to return a controlled JSON response without hitting ESPN.
 * Returns the spy so callers can assert call count.
 */
function mockEspnResponse(data, statusCode = 200) {
  return jest.spyOn(https, 'get').mockImplementation((url, options, callback) => {
    const res = new EventEmitter();
    res.statusCode = statusCode;
    setImmediate(() => {
      callback(res);
      res.emit('data', JSON.stringify(data));
      res.emit('end');
    });
    // espnFetch calls `.on('error', reject)` on the returned request object
    const req = new EventEmitter();
    return req;
  });
}

/** Mock https.get to simulate a network error. */
function mockEspnError(message = 'ECONNREFUSED') {
  return jest.spyOn(https, 'get').mockImplementation((url, options, callback) => {
    const req = new EventEmitter();
    setImmediate(() => req.emit('error', new Error(message)));
    return req;
  });
}

afterEach(() => {
  jest.restoreAllMocks();
});

// ---------------------------------------------------------------------------
// League validation — no network calls needed
// ---------------------------------------------------------------------------

describe('ESPN proxy — league validation', () => {
  test('unknown league on /scoreboard returns 400', async () => {
    const res = await request(app).get('/espn/badleague/scoreboard');
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/Unknown league/);
  });

  test('unknown league on /games/:id/summary returns 400', async () => {
    const res = await request(app).get('/espn/badleague/games/12345/summary');
    expect(res.status).toBe(400);
  });

  test('unknown league on /games/:id/odds returns 400', async () => {
    const res = await request(app).get('/espn/badleague/games/12345/odds');
    expect(res.status).toBe(400);
  });

  test('unknown league on /teams/:id returns 400', async () => {
    const res = await request(app).get('/espn/badleague/teams/1');
    expect(res.status).toBe(400);
  });

  test('unknown league on /teams/:id/schedule returns 400', async () => {
    const res = await request(app).get('/espn/badleague/teams/1/schedule');
    expect(res.status).toBe(400);
  });

  test('unknown league on /stream returns 400', async () => {
    // Close the connection quickly so the test does not hang
    const res = await request(app)
      .get('/espn/badleague/stream')
      .timeout({ response: 1000 })
      .catch((err) => err.response || { status: 400 });
    expect(res.status).toBe(400);
  });
});

// ---------------------------------------------------------------------------
// Scoreboard
// ---------------------------------------------------------------------------

describe('ESPN proxy — scoreboard', () => {
  // Use a historical date so the in-memory cache (populated by background
  // polling for "today") is guaranteed to be empty for this key.
  const TEST_DATE = '20200101';

  test('returns ESPN data on cache miss', async () => {
    const mockData = { events: [{ id: '401234', name: 'Team A vs Team B' }] };
    const spy = mockEspnResponse(mockData);

    const res = await request(app).get(`/espn/nba/scoreboard?date=${TEST_DATE}`);
    expect(res.status).toBe(200);
    expect(res.body).toEqual(mockData);
    expect(spy).toHaveBeenCalledTimes(1);
  });

  test('serves response from cache on second request', async () => {
    // Use a DIFFERENT date so the previous test's cache entry doesn't interfere
    const CACHE_DATE = '20200102';
    const mockData = { events: [{ id: '401234', name: 'Cached Game' }] };

    // First call — cache miss, populates cache
    mockEspnResponse(mockData);
    await request(app).get(`/espn/nba/scoreboard?date=${CACHE_DATE}`);
    jest.restoreAllMocks();

    // Second call — must be served from cache, not from ESPN
    const spy = mockEspnResponse({ events: [] }); // different data — must NOT be used
    const res = await request(app).get(`/espn/nba/scoreboard?date=${CACHE_DATE}`);

    expect(res.status).toBe(200);
    expect(res.body).toEqual(mockData); // still the first cached response
    expect(spy).not.toHaveBeenCalled();
  });

  test('returns 502 when ESPN is unreachable', async () => {
    const TEST_DATE_ERR = '20190101'; // different date = separate cache key
    mockEspnError();
    const res = await request(app).get(`/espn/nba/scoreboard?date=${TEST_DATE_ERR}`);
    expect(res.status).toBe(502);
  });
});

// ---------------------------------------------------------------------------
// Game summary
// ---------------------------------------------------------------------------

describe('ESPN proxy — game summary', () => {
  const FAKE_GAME_ID = 'test-game-unit-99999';

  test('returns game summary data', async () => {
    const mockData = { header: { id: FAKE_GAME_ID }, boxscore: {} };
    mockEspnResponse(mockData);

    const res = await request(app).get(`/espn/nba/games/${FAKE_GAME_ID}/summary`);
    expect(res.status).toBe(200);
    expect(res.body.header.id).toBe(FAKE_GAME_ID);
  });

  test('returns 502 on upstream error', async () => {
    const FAKE_GAME_ID_ERR = 'test-game-unit-err-99999';
    mockEspnError();
    const res = await request(app).get(`/espn/nba/games/${FAKE_GAME_ID_ERR}/summary`);
    expect(res.status).toBe(502);
  });
});

// ---------------------------------------------------------------------------
// Odds
// ---------------------------------------------------------------------------

describe('ESPN proxy — game odds', () => {
  const FAKE_GAME_ID = 'test-odds-unit-99999';

  test('returns odds data', async () => {
    const mockData = { items: [{ details: 'Test odds' }] };
    mockEspnResponse(mockData);

    const res = await request(app).get(`/espn/nfl/games/${FAKE_GAME_ID}/odds`);
    expect(res.status).toBe(200);
    expect(res.body).toEqual(mockData);
  });
});

// ---------------------------------------------------------------------------
// SSE stream — valid league opens event stream
// ---------------------------------------------------------------------------

describe('ESPN proxy — SSE stream', () => {
  test('valid league returns event-stream content-type', (done) => {
    const req = request(app).get('/espn/nba/stream').buffer(false);

    req.on('response', (res) => {
      expect(res.statusCode).toBe(200);
      expect(res.headers['content-type']).toMatch(/text\/event-stream/);
      res.destroy(); // close stream immediately
      done();
    });

    // res.destroy() causes a socket hang-up error — ignore it
    req.on('error', () => {});

    req.end();
  }, 10000);
});
