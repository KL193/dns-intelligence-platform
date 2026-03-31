// Offline MMDB export pipeline
// Usage: node export-mmdb.js
//
// Streams all RocksDB shards, builds an IP -> domains inverted index
// into a temporary RocksDB, then walks that index to emit a structured
// file suitable as input to an MMDB builder.

const fs = require('fs');
const path = require('path');
const rocksdb = require('rocksdb');
const { config, validateConfig } = require('./config');

validateConfig();

const BATCH_SIZE_RECORDS = Number.parseInt(process.env.EXPORT_BATCH_SIZE || '50000', 10);
const LOG_EVERY = Number.parseInt(process.env.EXPORT_LOG_EVERY || '100000', 10);

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

function getAsync(db, key) {
  return new Promise((resolve, reject) => {
    db.get(key, (err, value) => {
      if (err) return reject(err);
      resolve(value);
    });
  });
}

function writeBatchAsync(batch) {
  return new Promise((resolve, reject) => {
    batch.write((err) => {
      if (err) return reject(err);
      resolve();
    });
  });
}

function iterateAll(db, onRow) {
  return new Promise((resolve, reject) => {
    const it = db.iterator({ keyAsBuffer: false, valueAsBuffer: false });

    function next() {
      it.next(async (err, key, value) => {
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
          await onRow(key.toString(), value.toString());
        } catch (e) {
          // eslint-disable-next-line no-console
          console.error('[export-mmdb] Error in onRow, skipping record', e);
        }

        next();
      });
    }

    next();
  });
}

// Flush a batch of in-memory IP aggregates into the on-disk inverted index DB.
async function flushBatchToIpIndex(batchMap, ipIndexDb) {
  if (!batchMap || batchMap.size === 0) return;
  const batch = ipIndexDb.batch();

  // eslint-disable-next-line no-restricted-syntax
  for (const [ip, data] of batchMap.entries()) {
    let existing = null;
    try {
      const raw = await getAsync(ipIndexDb, ip);
      if (raw) {
        try {
          existing = JSON.parse(raw.toString());
        } catch (e) {
          // eslint-disable-next-line no-console
          console.warn('[export-mmdb] Corrupt JSON in IP index for', ip, e);
          existing = null;
        }
      }
    } catch (err) {
      // NotFound is expected for new keys.
      if (!err.notFound && !/NotFound/i.test(err.message || '')) {
        // eslint-disable-next-line no-console
        console.error('[export-mmdb] Error reading from IP index', { ip, err });
      }
    }

    const domainsSet = new Set();
    if (existing && Array.isArray(existing.domains)) {
      for (const d of existing.domains) domainsSet.add(d);
    }
    if (data && Array.isArray(data.domains)) {
      for (const d of data.domains) domainsSet.add(d);
    }

    const lastSeen = Math.max(
      existing && typeof existing.last_seen === 'number' ? existing.last_seen : 0,
      typeof data.last_seen === 'number' ? data.last_seen : 0,
    );

    const value = JSON.stringify({
      domains: Array.from(domainsSet),
      last_seen: lastSeen,
    });

    batch.put(ip, value);
  }

  await writeBatchAsync(batch);
  batchMap.clear();
}

async function buildInvertedIndexFromShards(ipIndexDb) {
  let processed = 0;

  for (let shard = 0; shard < config.NUM_SHARDS; shard += 1) {
    const dbPath = path.join(config.DB_PATH, `shard_${shard}`);
    // eslint-disable-next-line no-console
    console.info(`[export-mmdb] Processing shard ${shard} at ${dbPath}`);

    let shardDb;
    try {
      shardDb = await openDb(dbPath, false);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('[export-mmdb] Failed to open shard, skipping', { shard, err });
      continue; // Continue with next shard
    }

    const batchMap = new Map(); // ip -> { domains: [], last_seen }

    try {
      await iterateAll(shardDb, async (keyStr, valueStr) => {
        const parts = keyStr.split('|');
        if (parts.length < 3) {
          // eslint-disable-next-line no-console
          console.warn('[export-mmdb] Malformed key, skipping', keyStr);
          return;
        }
        const domain = parts[0];
        const type = parts[1];
        const value = parts[2];

        if (type !== 'A') {
          return; // Only process A records for IP mapping
        }

        let parsed;
        try {
          parsed = JSON.parse(valueStr);
        } catch (e) {
          // Skip corrupted records but log once per key.
          // eslint-disable-next-line no-console
          console.warn('[export-mmdb] Corrupt JSON value, skipping', { key: keyStr, error: e.message });
          return;
        }

        const lastSeen = typeof parsed.last_seen === 'number' ? parsed.last_seen : 0;

        let entry = batchMap.get(value);
        if (!entry) {
          entry = { domains: [], last_seen: 0 };
          batchMap.set(value, entry);
        }
        entry.domains.push(domain);
        if (lastSeen > entry.last_seen) {
          entry.last_seen = lastSeen;
        }

        processed += 1;
        if (processed % LOG_EVERY === 0) {
          // eslint-disable-next-line no-console
          console.info(`[export-mmdb] Processed ${processed} records so far...`);
        }

        if (processed % BATCH_SIZE_RECORDS === 0) {
          await flushBatchToIpIndex(batchMap, ipIndexDb);
        }
      });

      // Flush any remaining batch for this shard.
      await flushBatchToIpIndex(batchMap, ipIndexDb);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('[export-mmdb] Error while iterating shard', { shard, err });
    } finally {
      try {
        await closeDb(shardDb);
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error('[export-mmdb] Error closing shard DB', { shard, err });
      }
    }
  }

  // eslint-disable-next-line no-console
  console.info(`[export-mmdb] Finished building inverted index. Total processed records: ${processed}`);
}

async function writeStructuredOutputFromIpIndex(ipIndexDb, outputFile) {
  // For simplicity and streaming, we write NDJSON lines of the form:
  // { "ip": "1.2.3.4", "domains": [...], "last_seen": 123 }
  //
  // This file can then be used as input to an MMDB builder tool
  // (e.g., mmdb-lib or maxmind-db) that supports streaming ingestion.

  ensureDirSync(path.dirname(outputFile));

  const stream = fs.createWriteStream(outputFile, { encoding: 'utf8' });

  let count = 0;

  await iterateAll(ipIndexDb, async (ip, valueStr) => {
    let parsed;
    try {
      parsed = JSON.parse(valueStr);
    } catch (e) {
      // eslint-disable-next-line no-console
      console.warn('[export-mmdb] Corrupt JSON in IP index while writing output, skipping', {
        ip,
        error: e.message,
      });
      return;
    }

    const record = {
      ip,
      domains: Array.isArray(parsed.domains) ? parsed.domains : [],
      last_seen: typeof parsed.last_seen === 'number' ? parsed.last_seen : 0,
    };

    stream.write(`${JSON.stringify(record)}\n`);
    count += 1;
    if (count % 100000 === 0) {
      // eslint-disable-next-line no-console
      console.info(`[export-mmdb] Wrote ${count} IP records to output...`);
    }
  });

  await new Promise((resolve, reject) => {
    stream.end((err) => {
      if (err) return reject(err);
      resolve();
    });
  });

  // eslint-disable-next-line no-console
  console.info(`[export-mmdb] Finished writing output. Total IPs: ${count}`);
}

async function main() {
  const outputDir = config.OUTPUT_PATH;
  const mmdbFilename = config.MMDB_FILENAME;
  ensureDirSync(outputDir);

  const ipIndexPath = path.join(outputDir, 'ip_index');
  const outputFile = path.join(outputDir, mmdbFilename);

  // eslint-disable-next-line no-console
  console.info('[export-mmdb] Starting export');
  // eslint-disable-next-line no-console
  console.info('[export-mmdb] Output directory:', outputDir);
  // eslint-disable-next-line no-console
  console.info('[export-mmdb] Intermediate IP index path:', ipIndexPath);
  // eslint-disable-next-line no-console
  console.info('[export-mmdb] Structured output file:', outputFile);

  let ipIndexDb;
  try {
    ipIndexDb = await openDb(ipIndexPath, true);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[export-mmdb] Failed to open IP index RocksDB', err);
    process.exit(1);
  }

  try {
    await buildInvertedIndexFromShards(ipIndexDb);
    await writeStructuredOutputFromIpIndex(ipIndexDb, outputFile);
  } finally {
    try {
      await closeDb(ipIndexDb);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('[export-mmdb] Error closing IP index DB', err);
    }
  }

  // eslint-disable-next-line no-console
  console.info('[export-mmdb] Export job completed successfully');
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('[export-mmdb] Fatal error in export job', err);
  process.exit(1);
});
