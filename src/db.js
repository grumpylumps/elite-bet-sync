const { Pool } = require('pg');

let pool = null;
let connectionTested = false;
let connectionError = null;

const dbUrl = process.env.DATABASE_URL || (() => {
  const host = process.env.DB_HOST;
  const port = process.env.DB_PORT || '5432';
  const user = process.env.DB_USER || 'postgres';
  const pass = process.env.DB_PASSWORD || 'postgres';
  const name = process.env.DB_NAME || 'elite_bet_sync';
  if (host) return `postgresql://${user}:${encodeURIComponent(pass)}@${host}:${port}/${name}`;
  return 'postgresql://postgres:postgres@localhost:5432/elite_bet_sync';
})();

try {
  pool = new Pool({
    connectionString: dbUrl,
    // Connection pool settings for reliability
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
    // AWS RDS uses its own CA — disable cert verification for intra-VPC connections
    ssl: dbUrl.includes('localhost') || dbUrl.includes('127.0.0.1')
      ? false
      : { rejectUnauthorized: false },
  });

  // Log connection attempt
  console.log('[db] Attempting PostgreSQL connection...');
  console.log('[db] URL:', dbUrl.replace(/:[^:@]+@/, ':****@')); // Hide password

} catch (e) {
  console.error('[db] Failed to create Postgres Pool:', e.message);
  connectionError = e;
}

// Test connection lazily
async function testConnection() {
  if (connectionTested) return;

  connectionTested = true;

  try {
    const result = await pool.query('SELECT current_database() as db, current_user as user');
    const { db, user } = result.rows[0];
    console.log(`[db] ✓ Connected to PostgreSQL: database='${db}', user='${user}'`);
  } catch (e) {
    console.error('[db] ✗ PostgreSQL connection failed:', e.message);
    console.error('[db] Make sure PostgreSQL is running and elite_bet_sync database exists');
    connectionError = e;
    // Don't throw, allow server to start without DB
  }
}

module.exports = {
  query: async (text, params) => {
    await testConnection();
    if (!pool) {
      throw new Error('PostgreSQL connection not available');
    }
    return pool.query(text, params);
  },
  getClient: async () => {
    await testConnection();
    if (!pool) {
      throw new Error('PostgreSQL connection not available');
    }
    return pool.connect();
  },
};
