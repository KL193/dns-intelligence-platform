// CLI to query all IPs (and first_seen/last_seen) for a given domain
// Usage:
//   node query_domain.js --domain amazon.com
//   node query_domain.js --domain amazon.com --limit 100

const path = require('path');
const rocksdb = require('rocksdb');
const { config, validateConfig } = require('./config');
const { getShardId } = require('./shard');

validateConfig();

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { limit: 100 };
  for (let i = 0; i < args.length; i += 1) {
    const a = args[i];
    if (a === '--domain') {
      out.domain = args[i + 1];
      i += 1;
    } else if (a === '--limit') {
      out.limit = Number.parseInt(args[i + 1], 10);
      i += 1;
    }
  }
  if (!out.domain) {
    console.error('Usage: node query_domain.js --domain <domain> [--limit N]');
    process.exit(1);
  }
  if (!Number.isInteger(out.limit) || out.limit <= 0) {
    out.limit = 100;
  }
  return out;
}

function openDb(dbPath) {
  return new Promise((resolve, reject) => {
    const db = rocksdb(dbPath);
    db.open({ createIfMissing: false }, (err) => {
      if (err) return reject(err);
      resolve(db);
    });
  });
}

function closeDb(db) {
  return new Promise((resolve, reject) => {
    db.close((err) => {
      if (err) return reject(err);
      resolve();
    });
  });
}

function iterateByPrefix(db, prefix, limit) {
  return new Promise((resolve, reject) => {
    const it = db.iterator({
      gte: prefix,
      lt: `${prefix}\xff`,
      keyAsBuffer: false,
      valueAsBuffer: false,
    });

    const rows = [];

    function next() {
      it.next((err, key, value) => {
        if (err) {
          it.end(() => reject(err));
          return;
        }
        if (key === undefined || rows.length >= limit) {
          it.end((endErr) => {
            if (endErr) return reject(endErr);
            resolve(rows);
          });
          return;
        }
        rows.push({ key, value });
        next();
      });
    }

    next();
  });
}

(async () => {
  const { domain, limit } = parseArgs();
  const shard = getShardId(domain, config.NUM_SHARDS);
  const dbPath = path.join(config.DB_PATH, `shard_${shard}`);

  console.info(`Domain ${domain} maps to shard ${shard}`);
  console.info(`Opening DB at ${dbPath}`);

  let db;
  try {
    db = await openDb(dbPath);
  } catch (err) {
    console.error('Failed to open RocksDB shard', err);
    process.exit(1);
  }

  try {
    const prefix = `${domain}|`;
    const rows = await iterateByPrefix(db, prefix, limit);

    if (rows.length === 0) {
      console.log(`No records found for domain ${domain} (limit=${limit}).`);
      return;
    }

    console.log(`Found ${rows.length} records for domain ${domain} (limit=${limit}):`);
    for (const row of rows) {
      const keyStr = row.key.toString();
      const valueStr = row.value.toString();

      const parts = keyStr.split('|');
      const type = parts[1] || '';
      const ip = parts[2] || '';

      let parsed = null;
      try {
        parsed = JSON.parse(valueStr);
      } catch (e) {
        // ignore JSON parse error; show raw only
      }

      console.log('---');
      console.log(`Type: ${type}  IP: ${ip}`);
      console.log(`Raw value: ${valueStr}`);
      if (parsed) {
        console.log(`first_seen: ${parsed.first_seen}`);
        console.log(`last_seen:  ${parsed.last_seen}`);
      }
    }
  } finally {
    if (db) {
      try {
        await closeDb(db);
      } catch (err) {
        console.error('Error closing DB', err);
      }
    }
  }
})();
