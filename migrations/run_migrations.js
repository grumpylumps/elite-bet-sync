const { Client } = require('pg');
const fs = require('fs');
const path = require('path');

async function run() {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    console.error('DATABASE_URL must be set');
    process.exit(2);
  }

  // Run all .sql migration files in lexicographic order (001_, 002_, 003_, ...)
  const files = fs
    .readdirSync(__dirname)
    .filter((f) => f.endsWith('.sql'))
    .sort();

  const client = new Client({ connectionString: databaseUrl });
  try {
    await client.connect();
    for (const file of files) {
      console.log(`Applying migration: ${file}`);
      const sql = fs.readFileSync(path.join(__dirname, file), 'utf8');
      // Run the SQL file as a single script to safely handle DO $$ blocks and function definitions
      await client.query(sql);
    }

    console.log('Migrations applied');
    await client.end();
    process.exit(0);
  } catch (e) {
    console.error('Migration failed:', e);
    try {
      await client.end();
    } catch (_) {}
    process.exit(1);
  }
}

run();
