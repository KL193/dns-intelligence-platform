# DNS Aggregator Worker

JetStream (pull-based) worker that aggregates DNS events and writes them into a sharded RocksDB layout.

## Prerequisites

- Node.js 18+
- NATS server with JetStream enabled
- Existing stream named `DNS_FEED` receiving messages on subject `dns.feed.v1`
- RocksDB native libraries (installed via your OS package manager, e.g. `librocksdb-dev` on Debian/Ubuntu)

## Installation

```bash
cd "aggregator-worker"
cp .env.example .env
npm install
```

Adjust `.env` if needed:

- `NATS_URL` – NATS server URL
- `STREAM_NAME` – JetStream stream name (default: `DNS_FEED`)
- `SUBJECT` – subject to consume from (default: `dns.feed.v1`)
- `DURABLE_NAME` – durable consumer name (default: `dns_worker`)
- `NUM_SHARDS` – number of RocksDB shards (default: `4`)
- `DB_PATH` – base path for RocksDB shards (default: `./data`)
- `BATCH_SIZE` – JetStream pull batch size (default: `100`)

## Running the worker

```bash
cd "aggregator-worker"
npm start
```

The worker will:

- Pull messages from JetStream using a durable pull consumer
- Extract and deduplicate DNS events in-memory per batch
- Shard events by domain across `NUM_SHARDS` RocksDB databases
- Use RocksDB WriteBatch for batched upserts
- ACK messages **only after** a successful DB batch write

## Example logs

Example startup and processing logs:

```text
[worker] Initializing sharded RocksDB at ./data with 4 shards
[worker] Connecting to NATS at nats://localhost:4222
[worker] Creating durable pull consumer dns_worker
[worker] Connected to NATS and pull consumer ready
[worker] Starting main consumption loop with batch size 100
[worker] Batch processed { batchEventsCount: 500, totalBatchesProcessed: 1, totalEventsProcessed: 500, eventsPerSecond: '2500.00', shardDistribution: { '0': 120, '1': 130, '2': 110, '3': 140 } }
```

## Testing the pipeline

1. Run your existing DNS event generator so that it sends events into the ingest API, which publishes to JetStream stream `DNS_FEED` on subject `dns.feed.v1`.
2. Start the aggregator worker as described above.
3. Observe logs for batches being processed and shard distribution.
4. Inspect RocksDB data (per-shard directories under `DB_PATH`, e.g. `./data/shard_0`, `./data/shard_1`, ...), using command-line RocksDB tools or a small inspection script, to confirm deduplicated keys of the form `domain|type|value` with JSON values `{"first_seen", "last_seen"}`.

## Graceful shutdown & retries

- The worker listens for `SIGINT`/`SIGTERM` and will drain NATS, close DBs, and exit gracefully.
- If a DB batch write fails, it will retry up to `DB_MAX_RETRIES` times with exponential backoff. Messages are **not** ACKed unless the write ultimately succeeds.

## Notes

- Bad or non-JSON messages are terminated and skipped to avoid poison-pill redelivery loops, but do not crash the worker.
- Events missing required fields (`domain`, `type`, `value`) are skipped for that batch.
