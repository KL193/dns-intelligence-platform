package rocks

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/linxGnu/grocksdb"
)

// TemporalRange stores first_seen/last_seen timestamps for a key.
type TemporalRange struct {
	FirstSeen int64 `json:"first_seen"`
	LastSeen  int64 `json:"last_seen"`
}

// ShardRecords maps a composite key (domain|type|value) to its temporal range.
type ShardRecords map[string]TemporalRange

// ShardMap maps shard ID to its records.
type ShardMap map[int]ShardRecords

// ShardedRocksDB manages one RocksDB instance per shard, stored under
// basePath/shard_<id>.
type ShardedRocksDB struct {
	dbs []*grocksdb.DB
}

// NewShardedRocksDB opens or creates numShards DBs under basePath.
func NewShardedRocksDB(numShards int, basePath string) (*ShardedRocksDB, error) {
	if numShards <= 0 {
		return nil, fmt.Errorf("numShards must be > 0")
	}
	if err := os.MkdirAll(basePath, 0o755); err != nil {
		return nil, fmt.Errorf("create base path: %w", err)
	}

	dbs := make([]*grocksdb.DB, numShards)
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	for i := 0; i < numShards; i++ {
		shardPath := filepath.Join(basePath, fmt.Sprintf("shard_%d", i))
		if err := os.MkdirAll(shardPath, 0o755); err != nil {
			return nil, fmt.Errorf("create shard dir %d: %w", i, err)
		}
		db, err := grocksdb.OpenDb(opts, shardPath)
		if err != nil {
			return nil, fmt.Errorf("open shard %d: %w", i, err)
		}
		dbs[i] = db
	}

	return &ShardedRocksDB{dbs: dbs}, nil
}

// Close closes all shard DBs.
func (s *ShardedRocksDB) Close() error {
	for _, db := range s.dbs {
		if db != nil {
			db.Close()
		}
	}
	return nil
}

// UpsertBatchByShard applies the aggregated shard data atomically per
// shard, merging first_seen/last_seen with any existing value.
func (s *ShardedRocksDB) UpsertBatchByShard(m ShardMap) error {
	for shardID, records := range m {
		if len(records) == 0 {
			continue
		}
		if shardID < 0 || shardID >= len(s.dbs) {
			return fmt.Errorf("invalid shard id %d", shardID)
		}
			db := s.dbs[shardID]
			if db == nil {
				return fmt.Errorf("no db for shard %d", shardID)
			}

			ro := grocksdb.NewDefaultReadOptions()
			wo := grocksdb.NewDefaultWriteOptions()
			batch := grocksdb.NewWriteBatch()

			for key, incoming := range records {
				var existing TemporalRange
				val, err := db.Get(ro, []byte(key))
				if err == nil && val != nil && len(val.Data()) > 0 {
					data := val.Data()
					_ = json.Unmarshal(data, &existing) // best-effort; overwrite on error
				}
				if val != nil {
					val.Free()
				}

				first := incoming.FirstSeen
				last := incoming.LastSeen
				if existing.FirstSeen != 0 && existing.FirstSeen < first {
					first = existing.FirstSeen
				}
				if existing.LastSeen != 0 && existing.LastSeen > last {
					last = existing.LastSeen
				}

				value, err := json.Marshal(TemporalRange{FirstSeen: first, LastSeen: last})
				if err != nil {
					return fmt.Errorf("marshal value for key %s: %w", key, err)
				}
				batch.Put([]byte(key), value)
			}

			if err := db.Write(wo, batch); err != nil {
				return fmt.Errorf("write batch for shard %d: %w", shardID, err)
			}
			batch.Clear()
			batch.Destroy()
	}

	return nil
}
