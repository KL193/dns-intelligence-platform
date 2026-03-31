// CLI helper to compute which shard a domain maps to
// Usage:
//   node compute_shard.js example.com
//   node compute_shard.js --domain example.com

const { config, validateConfig } = require('./config');
const { getShardId } = require('./shard');

validateConfig();

function parseArgs() {
  const args = process.argv.slice(2);
  if (args.length === 0) {
    return {};
  }
  if (args[0] === '--domain') {
    return { domain: args[1] };
  }
  return { domain: args[0] };
}

const { domain } = parseArgs();

if (!domain) {
  console.error('Usage: node compute_shard.js <domain> or --domain <domain>');
  process.exit(1);
}

const shard = getShardId(domain, config.NUM_SHARDS);
console.log(`Domain: ${domain}`);
console.log(`NUM_SHARDS: ${config.NUM_SHARDS}`);
console.log(`Shard ID: ${shard}`);
