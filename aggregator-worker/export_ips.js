// Export unique IPs from sharded RocksDB into a flat ips.txt file.
//
// Usage:
//   node export_ips.js [outputFile]
//
// It scans all shards under DB_PATH, parses keys of the form
//   "<domain>|<type>|<value>"
// and collects the "value" for A / AAAA records where the value is
// a valid IP address. The result is written as one IP per line.

const fs = require('fs');
const path = require('path');
const net = require('net');
const rocksdb = require('rocksdb');
const { config, validateConfig } = require('./config');

validateConfig();

function ensureDirSync(p) {
  if (!fs.existsSync(p)) {
    fs.mkdirSync(p, { recursive: true });
  }
}

function openDb(dbPath, createIfMissing) {
  return new Promise((resolve, reject) => {
    const db = rocksdb(dbPath);
    db.open({ createIfMissing: !!createIfMissing }, (err) => {
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

function iterateKeys(db, onKey) {
  return new Promise((resolve, reject) => {
    const it = db.iterator({ keyAsBuffer: false, valueAsBuffer: false });

    function next() {
      it.next((err, key) => {
        if (err) {
          it.end(() => reject(err));
          return;
        }
        if (key === undefined) {
          it.end((endErr) => {
            if (endErr) return reject(endErr);
            resolve();
          });
          return;
        }

        try {
          onKey(key.toString());
        } catch (e) {
          // eslint-disable-next-line no-console
          console.error('[export-ips] Error in onKey, skipping key', e);
        }

        next();
      });
    }

    next();
  });
}

async function collectIpsFromShard(shardId, ipSet) {
  const dbPath = path.join(config.DB_PATH, `shard_${shardId}`);
  // eslint-disable-next-line no-console
  console.info(`[export-ips] Processing shard ${shardId} at ${dbPath}`);

  let db;
  try {
    db = await openDb(dbPath, false);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[export-ips] Failed to open shard, skipping', { shardId, err });
    return;
  }

  try {
    await iterateKeys(db, (keyStr) => {
      const parts = keyStr.split('|');
      if (parts.length < 3) {
        return;
      }
      const type = parts[1];
      const value = parts[2];

      if (type !== 'A' && type !== 'AAAA') {
        return; // Only interested in IP-bearing records
      }

      if (!value || net.isIP(value) === 0) {
        return;
      }

      ipSet.add(value);
    });
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[export-ips] Error while iterating shard', { shardId, err });
  } finally {
    try {
      await closeDb(db);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('[export-ips] Error closing shard DB', { shardId, err });
    }
  }
}

async function main() {
  const outputDir = config.OUTPUT_PATH;
  ensureDirSync(outputDir);

  const cliOutput = process.argv[2];
  const outputFile = cliOutput || path.join(outputDir, 'ips.txt');

  // eslint-disable-next-line no-console
  console.info('[export-ips] Output file:', outputFile);

  const ipSet = new Set();

  for (let shard = 0; shard < config.NUM_SHARDS; shard += 1) {
    // eslint-disable-next-line no-await-in-loop
    await collectIpsFromShard(shard, ipSet);
  }

  // Write unique IPs to file
  ensureDirSync(path.dirname(outputFile));
  const stream = fs.createWriteStream(outputFile, { encoding: 'utf8' });

  let count = 0;
  // eslint-disable-next-line no-restricted-syntax
  for (const ip of ipSet) {
    stream.write(`${ip}\n`);
    count += 1;
  }

  await new Promise((resolve, reject) => {
    stream.end((err) => {
      if (err) return reject(err);
      resolve();
    });
  });

  // eslint-disable-next-line no-console
  console.info(`[export-ips] Wrote ${count} unique IPs`);
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('[export-ips] Fatal error', err);
  process.exit(1);
});
