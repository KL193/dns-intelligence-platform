const crypto = require('crypto');

function hashDomain(domain) {
  const h = crypto.createHash('sha256').update(domain || '').digest();
  // Use first 4 bytes as unsigned int
  return h.readUInt32BE(0);
}

function getShardId(domain, numShards) {
  if (!Number.isInteger(numShards) || numShards <= 0) {
    throw new Error(`Invalid numShards: ${numShards}`);
  }
  const hash = hashDomain(domain);
  return hash % numShards;
}

module.exports = { getShardId };
