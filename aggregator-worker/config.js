const path = require('path');
require('dotenv').config();

function parseIntEnv(name, def) {
  const raw = process.env[name];
  if (raw == null || raw === '') return def;
  const n = Number.parseInt(raw, 10);
  if (Number.isNaN(n) || n <= 0) {
    throw new Error(`Invalid numeric value for ${name}: ${raw}`);
  }
  return n;
}

const config = {
  NATS_URL: process.env.NATS_URL || 'nats://localhost:4222',
  STREAM_NAME: process.env.STREAM_NAME || 'DNS_FEED',
  SUBJECT: process.env.SUBJECT || 'dns.feed.v1',
  DURABLE_NAME: process.env.DURABLE_NAME || 'dns_worker',
  NUM_SHARDS: parseIntEnv('NUM_SHARDS', 4),
  DB_PATH: process.env.DB_PATH || path.join(__dirname, 'data'),
  BATCH_SIZE: parseIntEnv('BATCH_SIZE', 100),
  DB_MAX_RETRIES: parseIntEnv('DB_MAX_RETRIES', 3),
  DB_RETRY_BASE_DELAY_MS: parseIntEnv('DB_RETRY_BASE_DELAY_MS', 500),
  OUTPUT_PATH: process.env.OUTPUT_PATH || path.join(__dirname, 'output'),
  MMDB_FILENAME: process.env.MMDB_FILENAME || 'dns-intel.mmdb'
};

function validateConfig() {
  if (!config.NATS_URL) throw new Error('NATS_URL is required');
  if (!config.STREAM_NAME) throw new Error('STREAM_NAME is required');
  if (!config.SUBJECT) throw new Error('SUBJECT is required');
  if (!config.DURABLE_NAME) throw new Error('DURABLE_NAME is required');
  if (!config.NUM_SHARDS || config.NUM_SHARDS <= 0) {
    throw new Error('NUM_SHARDS must be > 0');
  }
  if (!config.DB_PATH) throw new Error('DB_PATH is required');
  if (!config.BATCH_SIZE || config.BATCH_SIZE <= 0) {
    throw new Error('BATCH_SIZE must be > 0');
  }
  if (!config.OUTPUT_PATH) throw new Error('OUTPUT_PATH is required');
  if (!config.MMDB_FILENAME) throw new Error('MMDB_FILENAME is required');
}

module.exports = { config, validateConfig };
