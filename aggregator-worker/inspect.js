// Simple CLI to inspect data stored in sharded RocksDB
// Usage examples:
//   node inspect.js --shard 0 --key "example.com|A|1.2.3.4"
//   node inspect.js --shard 1 --limit 20

const path = require('path');
const rocksdb = require('rocksdb');
const { config, validateConfig } = require('./config');

validateConfig();

function parseArgs() {
  const args = process.argv.slice(2);
  const out = {};
  for (let i = 0; i < args.length; i += 1) {
    const a = args[i];
    if (a === '--shard') {
      out.shard = Number.parseInt(args[i + 1], 10);
      i += 1;
    } else if (a === '--key') {
      out.key = args[i + 1];
      i += 1;
    } else if (a === '--limit') {
      out.limit = Number.parseInt(args[i + 1], 10);
      i += 1;
    }
  }
  if (!Number.isInteger(out.shard) || out.shard < 0) {
    out.shard = 0;
  }
  if (!Number.isInteger(out.limit) || out.limit <= 0) {
    out.limit = 20;
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

function iterate(db, limit) {
  return new Promise((resolve, reject) => {
    const it = db.iterator({ keyAsBuffer: false, valueAsBuffer: false });
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
  const { shard, key, limit } = parseArgs();

  if (shard >= config.NUM_SHARDS) {
    console.error(`Shard ${shard} is out of range; NUM_SHARDS=${config.NUM_SHARDS}`);
    process.exit(1);
  }

  const dbPath = path.join(config.DB_PATH, `shard_${shard}`);
  console.info(`Opening shard ${shard} at ${dbPath}`);

  let db;
  try {
    db = await openDb(dbPath);
  } catch (err) {
    console.error('Failed to open RocksDB shard', err);
    process.exit(1);
  }

  try {
    if (key) {
      try {
        const raw = await getAsync(db, key);
        if (!raw) {
          console.log('Key not found');
        } else {
          let parsed = null;
          try {
            parsed = JSON.parse(raw.toString());
          } catch (e) {
            // not JSON, print raw
          }
          console.log('Key:', key);
          console.log('Raw value:', raw.toString());
          if (parsed) {
            console.log('Parsed JSON:', parsed);
          }
        }
      } catch (err) {
        console.error('Error reading key from DB', err);
      }
    } else {
      const rows = await iterate(db, limit);
      if (rows.length === 0) {
        console.log('No records in this shard (or limit too low).');
      } else {
        console.log(`Showing up to ${limit} records from shard ${shard}:`);
        for (const row of rows) {
          let parsed = null;
          try {
            parsed = JSON.parse(row.value.toString());
          } catch (e) {
            // ignore
          }
          console.log('---');
          console.log('Key:', row.key.toString());
          console.log('Raw value:', row.value.toString());
          if (parsed) {
            console.log('Parsed JSON:', parsed);
          }
        }
      }
    }
  } finally {
    try {
      await closeDb(db);
    } catch (err) {
      console.error('Error closing DB', err);
    }
  }
})();
