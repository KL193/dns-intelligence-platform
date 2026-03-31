const fs = require('fs');
const path = require('path');
const rocksdb = require('rocksdb');

function ensureDirSync(p) {
  if (!fs.existsSync(p)) {
    fs.mkdirSync(p, { recursive: true });
  }
}

function openDb(dbPath) {
  return new Promise((resolve, reject) => {
    const db = rocksdb(dbPath);
    db.open({ createIfMissing: true }, (err) => {
      if (err) return reject(err);
      resolve(db);
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

function closeDb(db) {
  return new Promise((resolve, reject) => {
    db.close((err) => {
      if (err) return reject(err);
      resolve();
    });
  });
}

class ShardedRocksDB {
  constructor(dbs) {
    this.dbs = dbs; // array indexed by shard id
  }

  static async create(numShards, basePath) {
    ensureDirSync(basePath);
    const dbs = [];
    for (let i = 0; i < numShards; i += 1) {
      const shardPath = path.join(basePath, `shard_${i}`);
      ensureDirSync(shardPath);
      const db = await openDb(shardPath);
      dbs.push(db);
    }
    return new ShardedRocksDB(dbs);
  }

  async upsertBatchByShard(shardMap) {
    // shardMap: Map<shardId, Map<key, { first_seen, last_seen }>>
    const shardIds = Array.from(shardMap.keys());
    await Promise.all(
      shardIds.map((shardId) => this._processShard(shardId, shardMap.get(shardId))),
    );
  }

  async _processShard(shardId, recordsMap) {
    if (!recordsMap || recordsMap.size === 0) return;
    const db = this.dbs[shardId];
    if (!db) throw new Error(`No DB for shard ${shardId}`);

    const entries = Array.from(recordsMap.entries());
    const batch = db.batch();

    for (const [key, incoming] of entries) {
      let existing = null;
      try {
        const raw = await getAsync(db, key);
        if (raw) {
          try {
            existing = JSON.parse(raw.toString());
          } catch (e) {
            // Corrupt JSON, ignore existing and overwrite
            existing = null;
          }
        }
      } catch (err) {
        // NotFound is expected for new keys. LevelDown-style backends use a "NotFound" error.
        if (!err.notFound && !/NotFound/i.test(err.message || '')) {
          throw err;
        }
      }

      const firstSeen = existing
        ? Math.min(existing.first_seen, incoming.first_seen)
        : incoming.first_seen;
      const lastSeen = existing
        ? Math.max(existing.last_seen, incoming.last_seen)
        : incoming.last_seen;

      const value = JSON.stringify({ first_seen: firstSeen, last_seen: lastSeen });
      batch.put(key, value);
    }

    await writeBatchAsync(batch);
  }

  async close() {
    await Promise.all(this.dbs.map((db) => closeDb(db)));
  }
}

module.exports = { ShardedRocksDB };
