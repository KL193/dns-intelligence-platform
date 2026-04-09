# DNS Intelligence – Deployment Guide

This document is for the deployment/operations team. It explains how to run the DNS Intelligence pipeline in production using Docker and Docker Compose.

## 1. Components

The system consists of:
- **NATS** – message broker with JetStream
- **dns-normalizer** – Go service that normalizes raw DNS events and manages JetStream streams
- **dns-aggregator** – Node.js + RocksDB consumer that stores DNS intelligence
- **dns-exporter** – Go batch job that geo-enriches exported data
- **dns-query-api** – Go HTTP API to query the enriched data by IP or domain

All application services use public Docker Hub images under `sandeeplgr/dns_intelligence` and are wired together via the compose file [docker-compose.prod.yml](docker-compose.prod.yml).

## 2. Prerequisites

- A Linux host (or VM) with:
  - Docker Engine installed
  - Docker Compose plugin available as `docker compose`
- Outbound internet access from this host to pull images from Docker Hub.
- Ports open as needed:
  - `4222/tcp` – NATS client port (for gateway systems)
  - `8080/tcp` – HTTP query API (for QA/consumers)

No Docker Hub credentials are required to **pull** images because the repository is public.

## 3. Images used

These images are already pushed to Docker Hub and referenced by [docker-compose.prod.yml](docker-compose.prod.yml):

- Go services (normalizer, exporter, query API, demo generator):
  - `sandeeplgr/dns_intelligence:go-V1`
- Aggregator (Node + RocksDB):
  - `sandeeplgr/dns_intelligence:aggregator-V1`

You do **not** need to rebuild these images on the server; Docker Compose will pull them automatically.

## 4. Files you need on the server

Copy or check out the project so that the following exist on the server:

- [docker-compose.prod.yml](docker-compose.prod.yml)
- [scripts/run-export.sh](scripts/run-export.sh)

These two files are sufficient for deployment and running exports.

## 5. Starting the core services

From the project directory (where `docker-compose.prod.yml` lives):

```bash
# Start NATS, normalizer, aggregator, and query API
docker compose -f docker-compose.prod.yml up -d \
  nats dns-normalizer dns-aggregator dns-query-api
```

Verify services:

```bash
# List containers
docker compose -f docker-compose.prod.yml ps

# Check NATS health endpoint
curl http://localhost:8222/healthz

# Check query API health endpoint
curl http://localhost:8080/healthz
```

At this point:
- NATS is running and listening on `4222`.
- dns-normalizer has auto-created the required JetStream streams.
- dns-aggregator is consuming normalized DNS events.
- dns-query-api is serving HTTP on port `8080`.

## 6. Configuration for gateway team (DNS feed producers)

Gateway / data producer systems should be configured as follows:

- **NATS URL** (from gateways to this server):
  - `nats://<SERVER-IP>:4222`
- **Subject to publish raw DNS events**:
  - `dns.raw.v1`

The normalizer creates and uses the JetStream streams internally; the gateways do **not** need to manage streams.

## 7. Creating an enriched export (snapshot + geo enrichment)

Whenever QA or other consumers need a fresh snapshot of enriched DNS intelligence data, run the export script from the project directory:

```bash
# Use the production compose file
COMPOSE_FILE=docker-compose.prod.yml ./scripts/run-export.sh
```

What this script does:
1. Takes a **filesystem snapshot** of the live RocksDB used by dns-aggregator (no downtime).
2. Runs the Node export job against the snapshot to generate `dns-intel.mmdb` inside the shared `output-data` volume.
3. Runs the Go geo-enrichment job to add geo fields (country, city, etc.) to that file.
4. Optionally cleans up the snapshot directory.

After it finishes, restart the query API so it reloads the updated file:

```bash
docker compose -f docker-compose.prod.yml restart dns-query-api
```

## 8. Querying the data (for QA / consumers)

The `dns-query-api` service exposes simple HTTP endpoints:

- Lookup by IP:

  ```bash
  curl "http://<SERVER-IP>:8080/ip?value=1.2.3.4"
  ```

- Lookup by domain/FQDN:

  ```bash
  curl "http://<SERVER-IP>:8080/domain?value=example.com"
  ```

These return JSON responses suitable for use by QA tools, dashboards, or ad-hoc scripts.

## 9. Stopping or restarting services

- Stop all services (containers remain, volumes preserved):

  ```bash
  docker compose -f docker-compose.prod.yml stop
  ```

- Stop and remove all services (volumes preserved unless you explicitly remove them):

  ```bash
  docker compose -f docker-compose.prod.yml down
  ```

- Restart a single service, for example the query API:

  ```bash
  docker compose -f docker-compose.prod.yml restart dns-query-api
  ```

## 10. Summary for operations

1. Ensure Docker + Docker Compose are installed and ports 4222/8080 are allowed.
2. Place `docker-compose.prod.yml` and `scripts/run-export.sh` on the server.
3. Start services with:
   ```bash
   docker compose -f docker-compose.prod.yml up -d \
     nats dns-normalizer dns-aggregator dns-query-api
   ```
4. Configure gateways to publish to `nats://<SERVER-IP>:4222` on subject `dns.raw.v1`.
5. When a fresh dataset is needed:
   ```bash
   COMPOSE_FILE=docker-compose.prod.yml ./scripts/run-export.sh
   docker compose -f docker-compose.prod.yml restart dns-query-api
   ```
6. QA/consumers query via HTTP on `http://<SERVER-IP>:8080`.
