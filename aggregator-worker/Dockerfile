# Node.js aggregator-worker with RocksDB

FROM node:20-bullseye-slim

# Install RocksDB native library and basic tools (rsync for snapshots)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    librocksdb-dev \
    rsync \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Install Node dependencies first (layer cache)
COPY package*.json ./
COPY .env.example ./
RUN npm install --only=production

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 10001 appuser && chown -R appuser /usr/src/app
USER appuser

# Default environment
ENV NATS_URL=nats://nats:4222
ENV DB_PATH=./data
ENV OUTPUT_PATH=./output
ENV MMDB_FILENAME=dns-intel.mmdb

# Data, snapshot, and output will be mounted as volumes
VOLUME ["/usr/src/app/data", "/usr/src/app/snapshot", "/usr/src/app/output"]

CMD ["npm","run","start"]
