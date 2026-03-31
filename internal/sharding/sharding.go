package sharding

import (
	"crypto/sha256"
	"encoding/binary"
)

// HashDomain computes a stable hash for a domain using SHA-256 and
// returning the first 4 bytes as an unsigned 32-bit integer.
func HashDomain(domain string) uint32 {
	h := sha256.Sum256([]byte(domain))
	return binary.BigEndian.Uint32(h[0:4])
}

// GetShardID maps a domain name to a shard in [0, numShards).
// It mirrors the JavaScript implementation in aggregator-worker/shard.js.
func GetShardID(domain string, numShards int) int {
	if numShards <= 0 {
		panic("invalid numShards")
	}
	h := HashDomain(domain)
	return int(h % uint32(numShards))
}
