/*
 * DNS Aggregator Worker
 *
 * JetStream (pull consumer) -> Aggregator Worker -> Sharded RocksDB
 */

const { connect, AckPolicy, DeliverPolicy, nanos } = require('nats');
const { config, validateConfig } = require('./config');
const { ShardedRocksDB } = require('./db');
const { getShardId } = require('./shard');

validateConfig();

let running = true;
let db = null;
let nc = null;
let js = null;

let totalEventsProcessed = 0;
let totalBatchesProcessed = 0;
const startTime = Date.now();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function logMetrics(batchEventsCount, shardMap) {
  totalBatchesProcessed += 1;
  totalEventsProcessed += batchEventsCount;

  const elapsedSec = (Date.now() - startTime) / 1000;
  const eps = elapsedSec > 0 ? (totalEventsProcessed / elapsedSec).toFixed(2) : '0.00';

  const shardDistribution = {};
  for (const [shardId, recordsMap] of shardMap.entries()) {
    shardDistribution[shardId] = recordsMap.size;
  }

  console.info('[worker] Batch processed', {
    batchEventsCount,
    totalBatchesProcessed,
    totalEventsProcessed,
    eventsPerSecond: eps,
    shardDistribution,
  });
}

async function initNats() {
  console.info('[worker] Connecting to NATS at', config.NATS_URL);
  nc = await connect({ servers: config.NATS_URL, name: 'dns-aggregator-worker' });

  nc.closed().then((err) => {
    if (err) {
      console.error('[nats] connection closed with error', err);
    } else {
      console.info('[nats] connection closed');
    }
    running = false;
  }).catch((err) => {
    console.error('[nats] closed() promise error', err);
  });

  js = nc.jetstream();
  const jsm = await nc.jetstreamManager();

  // Ensure durable pull consumer exists
  try {
    await jsm.consumers.info(config.STREAM_NAME, config.DURABLE_NAME);
    console.info('[worker] Using existing consumer', config.DURABLE_NAME);
  } catch (err) {
    console.info('[worker] Creating durable pull consumer', config.DURABLE_NAME);
    await jsm.consumers.add(config.STREAM_NAME, {
      durable_name: config.DURABLE_NAME,
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      filter_subject: config.SUBJECT,
      ack_wait: nanos(60 * 1000), // 60s
    });
  }

  console.info('[worker] Connected to NATS and consumer ready');
}

async function initDb() {
  console.info('[worker] Initializing sharded RocksDB at', config.DB_PATH, 'with', config.NUM_SHARDS, 'shards');
  db = await ShardedRocksDB.create(config.NUM_SHARDS, config.DB_PATH);
}

function extractEventsFromMessage(msg) {
  try {
    const jsonStr = Buffer.from(msg.data).toString('utf8');
    const payload = JSON.parse(jsonStr);
    // Ingest service publishes a JSON array of events: []Event
    if (Array.isArray(payload)) {
      return payload;
    }
    // Fallback: support { events: [...] } shape as well
    if (payload && Array.isArray(payload.events)) {
      return payload.events;
    }

    console.warn('[worker] Message without events array or array payload, skipping');
    return [];
  } catch (err) {
    console.error('[worker] Failed to parse message JSON, terminating message', err);
    // Terminate this message to avoid poison-pill redelivery loops.
    try {
      if (typeof msg.term === 'function') msg.term();
    } catch (e) {
      console.error('[worker] Failed to terminate bad message', e);
    }
    return [];
  }
}

async function processBatch(msgs) {
  // shardMap: Map<shardId, Map<key, { first_seen, last_seen }>>
  const shardMap = new Map();
  let batchEventsCount = 0;

  for (const msg of msgs) {
    const events = extractEventsFromMessage(msg);
    for (const ev of events) {
      if (!ev || !ev.domain || !ev.type || !ev.value) {
        console.warn('[worker] Skipping event with missing fields', ev);
        continue;
      }
      const key = `${ev.domain}|${ev.type}|${ev.value}`;
      const shardId = getShardId(ev.domain, config.NUM_SHARDS);
      const firstSeen = ev.first_seen;
      const lastSeen = ev.last_seen;

      let shardRecords = shardMap.get(shardId);
      if (!shardRecords) {
        shardRecords = new Map();
        shardMap.set(shardId, shardRecords);
      }

      const existing = shardRecords.get(key);
      if (existing) {
        existing.first_seen = Math.min(existing.first_seen, firstSeen);
        existing.last_seen = Math.max(existing.last_seen, lastSeen);
      } else {
        shardRecords.set(key, { first_seen: firstSeen, last_seen: lastSeen });
      }

      batchEventsCount += 1;
    }
  }

  if (batchEventsCount === 0) {
    return { batchEventsCount: 0, shardMap };
  }

  // Retry DB writes on failure
  let attempt = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    attempt += 1;
    try {
      await db.upsertBatchByShard(shardMap);
      break;
    } catch (err) {
      console.error('[worker] Failed to write batch to DB', { attempt, error: err });
      if (attempt >= config.DB_MAX_RETRIES) {
        // Do NOT ack; let messages be redelivered.
        throw err;
      }
      const delay = config.DB_RETRY_BASE_DELAY_MS * attempt;
      console.warn(`[worker] Retrying DB batch write in ${delay}ms`);
      await sleep(delay);
    }
  }

  // Only ACK after successful DB write
  for (const msg of msgs) {
    try {
      msg.ack();
    } catch (err) {
      console.error('[worker] Failed to ack message', err);
    }
  }

  logMetrics(batchEventsCount, shardMap);

  return { batchEventsCount, shardMap };
}

async function runLoop() {
  console.info('[worker] Starting main consumption loop with batch size', config.BATCH_SIZE);

  while (running) {
    try {
      const iter = await js.fetch(config.STREAM_NAME, config.DURABLE_NAME, {
        batch: config.BATCH_SIZE,
        expires: 5_000,
      });

      const batch = [];
      for await (const m of iter) {
        batch.push(m);
      }

      if (batch.length === 0) {
        // No messages available currently
        continue;
      }

      await processBatch(batch);
    } catch (err) {
      if (!running) break;
      console.error('[worker] Error in main loop', err);
      // Brief back-off to avoid hot error loop
      await sleep(1_000);
    }
  }

  console.info('[worker] Exiting main loop');
}

async function shutdown() {
  if (!running) return;
  console.info('[worker] Graceful shutdown requested');
  running = false;

  try {
    if (nc) {
      await nc.drain();
    }
  } catch (err) {
    console.error('[worker] Error draining NATS connection', err);
  }

  try {
    if (db) {
      await db.close();
    }
  } catch (err) {
    console.error('[worker] Error closing DB', err);
  }

  console.info('[worker] Shutdown complete');
}

process.on('SIGINT', () => {
  console.info('[worker] Caught SIGINT');
  shutdown().catch((err) => {
    console.error('[worker] Error during SIGINT shutdown', err);
    process.exit(1);
  });
});

process.on('SIGTERM', () => {
  console.info('[worker] Caught SIGTERM');
  shutdown().catch((err) => {
    console.error('[worker] Error during SIGTERM shutdown', err);
    process.exit(1);
  });
});

(async () => {
  try {
    await initDb();
    await initNats();
    await runLoop();
  } catch (err) {
    console.error('[worker] Fatal error during startup or runLoop', err);
    await shutdown();
    process.exit(1);
  }
})();
