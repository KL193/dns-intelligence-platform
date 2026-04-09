#!/usr/bin/env bash
set -euo pipefail

# Snapshot + export runner for DNS intelligence pipeline.
#
# Requirements:
# - docker and docker compose installed
# - docker compose up is running (nats, dns-normalizer, dns-aggregator, dns-generator)
# - dns-aggregator service has /usr/src/app/data and /usr/src/app/snapshot volumes
#
# This script:
#   1. Creates a RocksDB snapshot using rsync inside the dns-aggregator container
#   2. Runs the Node export-mmdb job against the snapshot (not the live DB)
#   3. Runs the Go geo enrichment job
#   4. Optionally cleans up the snapshot directory

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

# Which docker compose file to use. Default is docker-compose.yml.
# In production, set COMPOSE_FILE=docker-compose.prod.yml before
# running this script so it targets the prod stack.
COMPOSE_FILE=${COMPOSE_FILE:-docker-compose.yml}

SNAPSHOT_DIR_IN_CONTAINER="/usr/src/app/snapshot"
LIVE_DB_DIR_IN_CONTAINER="/usr/src/app/data"

CLEANUP_SNAPSHOT=${CLEANUP_SNAPSHOT:-true}

echo "[run-export] Using compose file: ${COMPOSE_FILE}"
echo "[run-export] Creating RocksDB snapshot inside dns-aggregator container..."

# rsync from live DB volume to snapshot volume. This does NOT touch the live DB,
# and the exporter will only ever open the snapshot path.
#
# We run this as root inside the container so we can write into the
# snapshot volume regardless of its ownership on the host.

docker compose -f "${COMPOSE_FILE}" exec -u root -T dns-aggregator bash -lc "\
  set -euo pipefail && \
  mkdir -p '${SNAPSHOT_DIR_IN_CONTAINER}' && \
  rsync -a --delete '${LIVE_DB_DIR_IN_CONTAINER}/' '${SNAPSHOT_DIR_IN_CONTAINER}/' \
"

echo "[run-export] Snapshot complete. Running export-mmdb against snapshot..."

# Run the Node exporter in a short-lived container, pointing DB_PATH at the
# snapshot directory. This avoids opening the live RocksDB from another process.
# We run as root so it can write dns-intel.mmdb into the shared output volume
# even if previous runs created it as root.

docker compose -f "${COMPOSE_FILE}" run --rm -u root \
  -e DB_PATH="${SNAPSHOT_DIR_IN_CONTAINER}" \
  dns-aggregator \
  node export-mmdb.js

echo "[run-export] Node export-mmdb completed. Running geo enrichment job..."

# Run the Go geo enrichment job (reads dns-intel.mmdb from the shared output-data
# volume, which is mounted at /data in the dns-exporter service).
#
# We run dns-exporter as root so it can create the temporary
# enriched file inside the shared output-data volume.

docker compose -f "${COMPOSE_FILE}" run --rm -u root dns-exporter

if [ "${CLEANUP_SNAPSHOT}" = "true" ]; then
  echo "[run-export] Cleaning up snapshot directory..."
  # Cleanup also runs as root for the same reason as rsync above.
  docker compose -f "${COMPOSE_FILE}" exec -u root -T dns-aggregator bash -lc "\
    set -euo pipefail && \
    rm -rf '${SNAPSHOT_DIR_IN_CONTAINER}'/* \
  "
fi

echo "[run-export] Export + enrichment finished successfully."
