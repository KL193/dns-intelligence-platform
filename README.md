# dns-intelligence-platform

## Ingest service

The Go service in this repository exposes an HTTP ingest endpoint that accepts DNS feed JSON and publishes normalized events to NATS/JetStream. Downstream workers consume from NATS and write aggregated data into RocksDB and MMDB.

You can start the ingest service with:

```bash
NATS_URL="nats://localhost:4222" go run .
```

## Gateway → NATS publishing

For real gateway DNS data you do not need to send traffic through the HTTP ingest endpoint. Gateways can publish directly to NATS/JetStream on the same subject that the ingest service uses (`dns.feed.v1`).

This repository includes a small example publisher in `cmd/gateway-publisher` that simulates a gateway pushing DNS events directly to NATS:

```bash
NATS_URL="nats://localhost:4222" \
	go run ./cmd/gateway-publisher \
	-device-id my-gateway-1 \
	-eps 50
```

The example publishes JSON arrays of events that match the schema expected by the aggregator worker. Real gateways can follow the same pattern: connect to NATS, construct event objects (`domain`, `type`, `value`, `first_seen`, `last_seen`, `device_id`), and publish them to the configured subject.

## Raw gateway → Normalizer → Feed pipeline

In addition to publishing normalized events directly to `dns.feed.v1`, you can also send **raw DNS logs** to a separate subject and let a **normalizer** service convert them into feed events.

This repository includes:

- `cmd/raw-gateway-publisher`: example gateway that publishes raw DNS records to `dns.raw.v1`.
- `cmd/normalizer`: service that subscribes to `dns.raw.v1`, normalizes domains, and publishes normalized events to `dns.feed.v1`.

### 1. Start NATS and the aggregator worker

```bash
cd "$HOME/Desktop/DNS Intelligence"

# Start NATS with JetStream enabled (separate terminal)
nats-server -js

# Option A: Node.js aggregator worker (original)
cd aggregator-worker
npm install   # first time only
npm run start

# Option B: Go-based aggregator worker (shares the same DB layout)
cd "$HOME/Desktop/DNS Intelligence"
NATS_URL="nats://localhost:4222" \
	DB_PATH="aggregator-worker/data" \
	go run ./cmd/dns-worker-go
```

### 2. Start the normalizer

From the repo root:

```bash
cd "$HOME/Desktop/DNS Intelligence"

NATS_URL="nats://localhost:4222" \
	go run ./cmd/normalizer
```

This process subscribes to `dns.raw.v1` (queue group `dns_normalizer`) and publishes normalized events to `dns.feed.v1` using JetStream, which the worker already consumes.

### 3. Start the raw gateway publisher

In another terminal:

```bash
cd "$HOME/Desktop/DNS Intelligence"

NATS_URL="nats://localhost:4222" \
	go run ./cmd/raw-gateway-publisher \
	-device-id gateway-raw-1 \
	-eps 50
```

This simulates a gateway emitting raw DNS logs to `dns.raw.v1`. The normalizer converts them into normalized events on `dns.feed.v1`, and the existing worker writes them into RocksDB.

### 4. Export IPs and build MMDB

Once some data has been processed, you can export IPs and build the geo-enriched MMDB as before:

```bash
cd "$HOME/Desktop/DNS Intelligence/aggregator-worker"

npm run export-ips
npm run export-mmdb

cd "$HOME/Desktop/DNS Intelligence"
source .venv/bin/activate
python3 scripts/dns_geo_mmdb_pipeline.py \
	--ip-list aggregator-worker/output/ips.txt

### 5. (Alternative) Enrich dns-intel.mmdb directly in Go

If you just want the final NDJSON artifact with geo (`dns-intel.mmdb`) and
prefer to stay in Go, you can use the Go-based enrichment command instead of
the Python helper:

```bash
cd "$HOME/Desktop/DNS Intelligence"
NATS_URL="nats://localhost:4222" \
	go run ./cmd/enrich-dns-intel-geo-go
```

By default this reads `aggregator-worker/output/dns-intel.mmdb`, enriches each
IP with `country` and `city` (when available from the GeoLite2 CSVs), and
overwrites the same file atomically.
```

This completes the pipeline:

Gateway → `dns.raw.v1` → Normalizer → `dns.feed.v1` → RocksDB → MMDB.