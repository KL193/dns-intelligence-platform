// CLI to query domains for a given IP from the inverted index
// built by export-mmdb.js.
//
// Usage:
//   node query_ip.js --ip 8.8.8.8

const path = require('path');
const rocksdb = require('rocksdb');
const { config, validateConfig } = require('./config');

validateConfig();

function parseArgs() {
  const args = process.argv.slice(2);
  const out = {};
  for (let i = 0; i < args.length; i += 1) {
    const a = args[i];
    if (a === '--ip') {
      out.ip = args[i + 1];
      i += 1;
    }
  }
  if (!out.ip) {
    console.error('Usage: node query_ip.js --ip <ip>');
    process.exit(1);
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

function getAsync(db, key) {
  return new Promise((resolve, reject) => {
    db.get(key, (err, value) => {
      if (err) return reject(err);
      resolve(value);
    });
  });
}

(async () => {
  const { ip } = parseArgs();

  const ipIndexPath = path.join(config.OUTPUT_PATH, 'ip_index');
  console.info(`[query_ip] Opening IP index at ${ipIndexPath}`);

  let db;
  try {
    db = await openDb(ipIndexPath);
  } catch (err) {
    console.error('[query_ip] Failed to open IP index RocksDB', err);
    process.exit(1);
  }

  try {
    let raw;
    try {
      raw = await getAsync(db, ip);
    } catch (err) {
      if (err.notFound || /NotFound/i.test(err.message || '')) {
        console.log(`[query_ip] No record found for IP ${ip}`);
        return;
      }
      throw err;
    }

    if (!raw) {
      console.log(`[query_ip] No record found for IP ${ip}`);
      return;
    }

    let parsed;
    try {
      parsed = JSON.parse(raw.toString());
    } catch (e) {
      console.log(`[query_ip] Raw value for IP ${ip}: ${raw.toString()}`);
      console.warn('[query_ip] Failed to parse JSON value', e);
      return;
    }

    const domains = Array.isArray(parsed.domains) ? parsed.domains : [];
    const lastSeen = typeof parsed.last_seen === 'number' ? parsed.last_seen : 0;

    console.log(`IP: ${ip}`);
    console.log(`Domains (${domains.length}):`);
    for (const d of domains) {
      console.log(`  - ${d}`);
    }
    console.log(`last_seen: ${lastSeen}`);
  } finally {
    if (db) {
      try {
        await closeDb(db);
      } catch (err) {
        console.error('[query_ip] Error closing IP index DB', err);
      }
    }
  }
})();
