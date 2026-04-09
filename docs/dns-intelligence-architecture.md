# DNS Intelligence Pipeline Overview

This document describes the full DNS Intelligence system in this repository, from ingesting DNS telemetry to generating an enriched IP → {domain, country, city} dataset. It is written so you can later plug in **real DNS data** in place of the synthetic generator.

---

## 1. High-Level Architecture

**Goal:**
- Ingest DNS correlation data (domains, answers, temporal info).
- Normalize and publish it to **NATS JetStream** as flat events.
- Aggregate events into a **sharded RocksDB** store keyed by `(domain, type, value)`.
- Build an **IP → domains** index and an **IP list** from RocksDB.
- Enrich those IPs with **geolocation data** from CSVs.
- Output a versioned enriched dataset: `IP → {domain, country, city}`.

**Main components:**
- HTTP ingest service (Go): `main.go`
- Synthetic feed generator (Go): `cmd/generator/main.go`
- DNS worker / aggregator (Go + RocksDB): `cmd/dns-worker-go/main.go`, `internal/...`
- Node.js aggregator tools (RocksDB queries + exports): `aggregator-worker/*.js`
- Geo enrichment pipeline (Python): `scripts/dns_geo_mmdb_pipeline.py`

---

## 2. Ingest Service (HTTP → NATS JetStream)

**File:** `main.go`

**Endpoint:**
- `POST /api/v1/dnsfeed`

**Input format (Feed):**
- `feed_type`: must be `"dns_intelligence_comprehensive"`
- `version`: arbitrary version string
- `device_id`: sensor / probe ID
- `timestamp`: Unix micros (int64)
- `domain_correlations[]`:
  - `domain`: the queried domain
  - `answer_ips[]`: array of IPs (may be empty)
  - `cname_target` (optional): CNAME chain target
  - `final_domain`: canonical/base domain
  - `temporal` (optional):
    - `first_seen`, `last_seen` (int64 micros)
    - `ttl`, `query_count`

**Internal Event format:**
- Each feed is flattened into `Event` values:
  - `Domain`: normalized (lowercased, stripped of dots, IDNA ASCII)
  - `Type`: `"A"` or `"CNAME"`
  - `Value`: IP (for `A`) or domain (for `CNAME`)
  - `FirstSeen`, `LastSeen`: from `temporal` or top-level timestamp
  - `DeviceID`

**Flow:**
1. `ingestHandler` parses JSON into `Feed`.
2. `validateFeed` ensures fields are present and shaped correctly.
3. `extractEvents`:
   - Normalizes domains with `normalizeDomain` (IDNA + lowercasing, no trailing dots).
   - Emits one `Event` per `answer_ips[]` (type `A`).
   - Optionally emits a `CNAME` event if `cname_target` is present.
4. `publishEventsToJetStream` serializes the `[]Event` as JSON and publishes to NATS JetStream subject (default `dns.feed.v1`).

**Run:**
```bash
# From repo root
NATS_URL="nats://127.0.0.1:4222" \
INGEST_HTTP_ADDR=":3000" \
go run .
```

---

## 3. Synthetic DNS Generator (for testing)

**File:** `cmd/generator/main.go`

**Purpose:**
- Produce realistic-looking DNS correlation feeds and POST them to the ingest API.
- When using real DNS data in the future, this component can be replaced by your own producer that hits `/api/v1/dnsfeed` with the same JSON schema.

**Key behavior:**
- Randomly chooses `baseDomains` such as `google.com`, `facebook.com`, `amazon.com`, `cloudflare.com`, `netflix.com`, `apple.com`, `tiktok.com`, `instagram.com`.
- Builds subdomains using prefixes like `www`, `api`, `cdn`, `video`, `static`, `auth`.
- Generates `AnswerIPs` via `randomIPs`:
  - Mix of:
    - Well-known public resolvers/CDN IPs (`publicIPPool`).
    - Random public IPv4 addresses from `randomPublicIPv4()` that **avoid private and reserved ranges** (10/8, 172.16/12, 192.168/16, 127/8, 0/8, 169.254/16, 224+/4). These are much more likely to have geo entries.
- Fills temporal and ML statistics (bytes, packets, threat scores, etc.).

**Run:**
```bash
# From repo root
go run ./cmd/generator \
  -endpoint http://localhost:3000/api/v1/dnsfeed \
  -devices 500 \
  -rps 100 \
  -batch 5 \
  -duration 2m
```

**Replacing with real DNS:**
- Have your real DNS processing pipeline build the same `Feed` JSON and POST to `/api/v1/dnsfeed`.
- The rest of the system (JetStream, RocksDB, exports, geo enrichment) stays the same.

---

## 4. Aggregation Worker (JetStream → Sharded RocksDB)

**File:** `cmd/dns-worker-go/main.go`

**Purpose:**
- Consume flattened `Event` objects from NATS JetStream and write them into a sharded RocksDB store for efficient aggregation.

**Configuration (env vars):**
- `NATS_URL` (default `nats://127.0.0.1:4222`)
- `STREAM_NAME` (default `DNS_FEED`)
- `SUBJECT` (default `dns.feed.v1`)
- `DURABLE_NAME` (default `dns_worker`)
- `NUM_SHARDS` (default `4`)
- `DB_PATH` (default `./aggregator-data`)
- `BATCH_SIZE`, `DB_MAX_RETRIES`, `DB_RETRY_BASE_DELAY_MS`

**Data model (RocksDB):**
- Keys are strings: `"<domain>|<type>|<value>"`.
- Values store a temporal range struct: `{ first_seen, last_seen }`.
- Sharding is done by hashing the domain: `sharding.GetShardID(domain, NumShards)`.

**Flow:**
1. Connect to JetStream and ensure a durable consumer exists.
2. Pull messages in batches (`BATCH_SIZE`).
3. For each message, parse JSON into `[]Event` (or `{ "events": [...] }` fallback).
4. For each `Event`, compute shard ID and key string, merge temporal ranges per key.
5. Commit batched updates to sharded RocksDB via `rocksstorage.NewShardedRocksDB` and `UpsertBatchByShard`.

**Run (aligned with Node shard layout):**
```bash
# From repo root, using the same directory layout as aggregator-worker
DB_PATH=./aggregator-worker/data \
NUM_SHARDS=4 \
NATS_URL="nats://127.0.0.1:4222" \
go run ./cmd/dns-worker-go
```

---

## 5. Node Aggregator & Exports (RocksDB → IP Index & IP List)

**Directory:** `aggregator-worker/`

Key scripts:

### 5.1. export-mmdb.js (IP Index)

**File:** `aggregator-worker/export-mmdb.js`

**Purpose:**
- Scan all RocksDB shards and build an **IP → domains** index.

**Flow (simplified):**
1. Iterate keys in each shard under `aggregator-worker/data/shard_*`.
2. Parse key as `domain|type|value`.
3. Only process records where `type === 'A'` (IPv4 A records).
4. For each A record:
   - `ip = value`
   - Aggregate into an in-memory map: `ip → { domains[], last_seen }`.
5. Write NDJSON to `aggregator-worker/output/dns-intel.mmdb`, where each line is:
   ```json
   { "ip": "1.2.3.4", "domains": ["example.com", "api.example.com"], "last_seen": 123456789 }
   ```

**Run:**
```bash
cd aggregator-worker
npm install           # once
npm run export-mmdb   # or: node export-mmdb.js
```

### 5.2. export_ips.js (IP List)

**File:** `aggregator-worker/export_ips.js`

**Purpose:**
- Produce a deduplicated list of all IPs (A and AAAA records) seen in RocksDB.

**Flow:**
1. Scan all shards.
2. For each key `domain|type|value` where `type` is `"A"` or `"AAAA"` and `value` is a valid IP string, add to a `Set`.
3. Write each unique IP to `aggregator-worker/output/ips.txt` (one IP per line).

**Run:**
```bash
cd aggregator-worker
npm run export-ips    # or: node export_ips.js
```

**Outputs used later:**
- IP index NDJSON: `aggregator-worker/output/dns-intel.mmdb`
- IP list: `aggregator-worker/output/ips.txt`

---

## 6. Geo Enrichment Pipeline (IP → {domain, country, city})

**File:** `scripts/dns_geo_mmdb_pipeline.py`

**Purpose:**
- Merge the IP→domains index with geolocation ranges (IPv4 + IPv6) to produce enriched data.

**Inputs:**
- `--input-mmdb` (default: `aggregator-worker/output/dns-intel.mmdb`)
  - NDJSON from `export-mmdb.js`, **not** a real MaxMind binary.
- `--ip-list` (required): IP list file, typically `aggregator-worker/output/ips.txt`.
- `--ipv4-csv` (default: `geo-data/geolite2-city-ipv4.csv`)
- `--ipv6-csv` (default: `geo-data/geolite2-city-ipv6.csv`)

**Geo CSV format (your current data):**
- Headerless rows: `start_ip,end_ip,country,...`
- The loader also supports headered CSVs with `network` or `ip_range_*` columns.

**Core logic:**
- Load IPv4 and IPv6 ranges into `GeoRecord{start, end, country, city}` arrays:
  - Convert IP strings to integers via Python `ipaddress`.
  - Store separate sorted lists for IPv4 and IPv6.
- For each `(ip, domain)` from `dns-intel.mmdb` filtered by `ip-list`:
  - Convert `ip` to integer and detect version.
  - Perform a **binary search** over the appropriate range list:
    - Find `rec` such that `rec.start <= ip_int <= rec.end`.
  - Output a record with any available geo info.

**Output modes:**
- In this environment, `maxminddb-writer` is not installed, so the script writes **NDJSON**:
  ```json
  { "ip": "1.2.3.4", "domain": "example.com", "country": "US", "city": "New York" }
  ```
- File is auto-versioned: `dns-geo-YYYYMMDDHHMMSS.ndjson` in the repo root.

**Run:**
```bash
# Activate venv and ensure maxminddb is installed
source .venv/bin/activate

# From repo root
python3 scripts/dns_geo_mmdb_pipeline.py \
  --ip-list aggregator-worker/output/ips.txt
```

Watch for logs:
- `Loaded <N> IPv4 geo ranges.`
- `Loaded <M> IPv6 geo ranges.`
- `Loaded <K> IPs to process.`
- `Enriched output written to: dns-geo-YYYYMMDDHHMMSS.ndjson`

---

## 7. End-to-End Run (Synthetic Data)

From an empty-ish state, a full synthetic run looks like this:

1. **Start NATS** (outside of this repo), e.g.:
   ```bash
   nats-server -js
   ```

2. **Start ingest service** (HTTP → JetStream):
   ```bash
   cd "$HOME/Desktop/DNS Intelligence"
   NATS_URL="nats://localhost:4222" \
   INGEST_HTTP_ADDR=":3000" \
   go run .
   ```

3. **Start Go DNS worker** (JetStream → RocksDB):
   ```bash
   cd "$HOME/Desktop/DNS Intelligence"
   DB_PATH=./aggregator-worker/data \
   NUM_SHARDS=4 \
   NATS_URL="nats://localhost:4222" \
   go run ./cmd/dns-worker-go
   ```

4. **Run synthetic generator** (DNS feeds → ingest API):
   ```bash
   cd "$HOME/Desktop/DNS Intelligence"
   go run ./cmd/generator \
     -endpoint http://localhost:3000/api/v1/dnsfeed \
     -devices 500 \
     -rps 100 \
     -batch 5 \
     -duration 2m
   ```

5. **Export IP index and IP list** from RocksDB via Node:
   ```bash
   cd "$HOME/Desktop/DNS Intelligence/aggregator-worker"
   npm install          # first time only
   npm run export-mmdb
   npm run export-ips
   ```

6. **Run geo enrichment** (IP → {domain, country, city} NDJSON):
   ```bash
   cd "$HOME/Desktop/DNS Intelligence"
   source .venv/bin/activate
   python3 scripts/dns_geo_mmdb_pipeline.py \
     --ip-list aggregator-worker/output/ips.txt
   ```

7. **Result:**
   - Look at the latest `dns-geo-*.ndjson` in the repo root for enriched IP records.

---

## 8. Using Real DNS Data in the Future

To move from synthetic to real data, the main change is **before the ingest API**:

- Replace `cmd/generator/main.go` with your real DNS → `Feed` producer (or add another producer).
- Ensure you POST the same `Feed` JSON schema to `/api/v1/dnsfeed`.
- Everything downstream (JetStream, RocksDB, Node exports, Python geo enrichment) can remain unchanged.

Checklist for real-world integration:
- [ ] Your DNS pipeline constructs `Feed` objects matching the ingest schema.
- [ ] `feed_type == "dns_intelligence_comprehensive"`.
- [ ] `domain_correlations[].answer_ips` contains all relevant A/AAAA answers.
- [ ] `temporal` fields (`first_seen`, `last_seen`) are populated or at least `timestamp` is set.
- [ ] Ingest service and DNS worker are attached to the same NATS JetStream instance.
- [ ] RocksDB directory (`aggregator-worker/data`) is mounted on persistent storage if needed.
- [ ] Geo CSVs (`geo-data/*`) are kept up to date and in the expected format.

Once your real producer is wired in, rerun steps 2–6 above; the enriched NDJSON output structure stays the same, but now reflects your real DNS traffic.
