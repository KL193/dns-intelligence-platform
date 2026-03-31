// HTTP Query API for DNS intelligence data
// Exposes GET /resolve?domain=example.com

const http = require('http');
const url = require('url');
const path = require('path');
const rocksdb = require('rocksdb');
const { config, validateConfig } = require('./config');
const { getShardId } = require('./shard');

validateConfig();

function normalizeDomain(raw) {
  if (!raw || typeof raw !== 'string') return '';
  let d = raw.trim().toLowerCase();
  if (d.endsWith('.')) d = d.slice(0, -1);
  return d;
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

// Iterate over keys with a given prefix in a single shard DB and aggregate results.
function resolveFromShard(db, domain) {
  return new Promise((resolve, reject) => {
    const prefix = `${domain}|`;
    const it = db.iterator({
      gte: prefix,
      lt: `${prefix}\xff`,
      keyAsBuffer: false,
      valueAsBuffer: false,
    });

    const ips = new Set();
    const cnames = new Set();
    let lastSeen = 0;

    function next() {
      it.next((err, key, value) => {
        if (err) {
          it.end(() => reject(err));
          return;
        }
        if (key === undefined) {
          it.end((endErr) => {
            if (endErr) {
              reject(endErr);
              return;
            }
            resolve({
              domain,
              ips: Array.from(ips),
              cnames: Array.from(cnames),
              last_seen: lastSeen || 0,
            });
          });
          return;
        }

        try {
          const keyStr = key.toString();
          const parts = keyStr.split('|');
          const type = parts[1] || '';
          const valueStr = value.toString();

          let parsed = null;
          try {
            parsed = JSON.parse(valueStr);
          } catch (e) {
            // Corrupt JSON, skip metrics but still allow IP/CNAME extraction.
          }

          if (parsed && typeof parsed.last_seen === 'number') {
            if (parsed.last_seen > lastSeen) {
              lastSeen = parsed.last_seen;
            }
          }

          const valPart = parts[2] || '';
          if (type === 'A' && valPart) {
            ips.add(valPart);
          } else if (type === 'CNAME' && valPart) {
            cnames.add(valPart);
          }
        } catch (e) {
          // Skip any malformed records.
        }

        next();
      });
    }

    next();
  });
}

async function main() {
  const shardDbs = [];
  const serverPort = Number.parseInt(process.env.PORT || '3000', 10);

  // Open all shard DBs once for high-QPS, low-latency queries.
  try {
    for (let i = 0; i < config.NUM_SHARDS; i += 1) {
      const dbPath = path.join(config.DB_PATH, `shard_${i}`);
      // Do not create if missing; this is a read-only service.
      const db = await openDb(dbPath);
      shardDbs.push(db);
      // eslint-disable-next-line no-console
      console.info(`[query-api] Opened shard ${i} at ${dbPath}`);
    }
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[query-api] Failed to open RocksDB shards', err);
    process.exit(1);
  }

  const server = http.createServer(async (req, res) => {
    const { method } = req;
    const parsedUrl = url.parse(req.url || '', true);

    if (parsedUrl.pathname === '/healthz') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok' }));
      return;
    }

    if (parsedUrl.pathname !== '/resolve' || method !== 'GET') {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Not found' }));
      return;
    }

    const rawDomain = parsedUrl.query.domain;
    const domain = normalizeDomain(rawDomain);

    if (!domain) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Invalid or missing domain parameter' }));
      return;
    }

    let shardId;
    try {
      shardId = getShardId(domain, config.NUM_SHARDS);
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Shard computation failed' }));
      return;
    }

    const db = shardDbs[shardId];
    if (!db) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: `Shard ${shardId} not available` }));
      return;
    }

    try {
      const result = await resolveFromShard(db, domain);

      // Domain not found: empty ips/cnames and last_seen 0.
      if (result.ips.length === 0 && result.cnames.length === 0) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(
          JSON.stringify({
            domain,
            ips: [],
            cnames: [],
            last_seen: 0,
          }),
        );
        return;
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('[query-api] Error resolving domain', { domain, err });
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Internal server error' }));
    }
  });

  server.listen(serverPort, () => {
    // eslint-disable-next-line no-console
    console.info(`[query-api] Listening on port ${serverPort}`);
  });

  function shutdown(signal) {
    // eslint-disable-next-line no-console
    console.info(`[query-api] Caught ${signal}, shutting down...`);
    server.close(async () => {
      // eslint-disable-next-line no-console
      console.info('[query-api] HTTP server closed, closing DBs...');
      await Promise.all(
        shardDbs.map(async (db, idx) => {
          try {
            await closeDb(db);
            // eslint-disable-next-line no-console
            console.info(`[query-api] Closed shard ${idx}`);
          } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`[query-api] Error closing shard ${idx}`, err);
          }
        }),
      );
      process.exit(0);
    });
  }

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('[query-api] Fatal error during startup', err);
  process.exit(1);
});
