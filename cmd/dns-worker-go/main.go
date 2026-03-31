package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"dns_ingest_service/internal/sharding"
	rocksstorage "dns_ingest_service/internal/storage/rocks"
)

// Event mirrors the ingest service's Event type.
type Event struct {
	Domain    string `json:"domain"`
	Type      string `json:"type"`
	Value     string `json:"value"`
	FirstSeen int64  `json:"first_seen"`
	LastSeen  int64  `json:"last_seen"`
	DeviceID  string `json:"device_id"`
}

type Config struct {
	NATSURL            string
	StreamName         string
	Subject            string
	DurableName        string
	NumShards          int
	DBPath             string
	BatchSize          int
	DBMaxRetries       int
	DBRetryBaseDelayMs int
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Fatalf("invalid numeric value for %s: %q", key, v)
	}
	return n
}

func loadConfig() Config {
	return Config{
		NATSURL:            getenv("NATS_URL", nats.DefaultURL),
		StreamName:         getenv("STREAM_NAME", "DNS_FEED"),
		Subject:            getenv("SUBJECT", "dns.feed.v1"),
		DurableName:        getenv("DURABLE_NAME", "dns_worker"),
		NumShards:          getenvInt("NUM_SHARDS", 4),
		DBPath:             getenv("DB_PATH", "./aggregator-data"),
		BatchSize:          getenvInt("BATCH_SIZE", 100),
		DBMaxRetries:       getenvInt("DB_MAX_RETRIES", 3),
		DBRetryBaseDelayMs: getenvInt("DB_RETRY_BASE_DELAY_MS", 500),
	}
}

func ensureConsumer(js nats.JetStreamContext, cfg Config) error {
	_, err := js.ConsumerInfo(cfg.StreamName, cfg.DurableName)
	if err == nil {
		log.Printf("[worker] Using existing consumer %s", cfg.DurableName)
		return nil
	}

	log.Printf("[worker] Creating durable pull consumer %s", cfg.DurableName)
	_, err = js.AddConsumer(cfg.StreamName, &nats.ConsumerConfig{
		Durable:       cfg.DurableName,
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
		FilterSubject: cfg.Subject,
		AckWait:       time.Minute,
	})
	return err
}

func extractEventsFromMsg(msg *nats.Msg) []Event {
	var events []Event
	if err := json.Unmarshal(msg.Data, &events); err == nil {
		return events
	}

	// Fallback: {"events": [...]} shape
	var wrapper struct {
		Events []Event `json:"events"`
	}
	if err := json.Unmarshal(msg.Data, &wrapper); err == nil && len(wrapper.Events) > 0 {
		return wrapper.Events
	}

	log.Printf("[worker] Message without parsable events, terminating")
	if err := msg.Term(); err != nil {
		log.Printf("[worker] Failed to term bad message: %v", err)
	}
	return nil
}

func processBatch(db *rocksstorage.ShardedRocksDB, cfg Config, msgs []*nats.Msg, totalBatches, totalEvents *uint64, startTime time.Time) error {
	shardMap := make(rocksstorage.ShardMap)
	var batchEvents int64

	for _, msg := range msgs {
		events := extractEventsFromMsg(msg)
		for _, ev := range events {
			if ev.Domain == "" || ev.Type == "" || ev.Value == "" {
				log.Printf("[worker] Skipping event with missing fields: %+v", ev)
				continue
			}
			key := ev.Domain + "|" + ev.Type + "|" + ev.Value
			shardID := sharding.GetShardID(ev.Domain, cfg.NumShards)
			first := ev.FirstSeen
			last := ev.LastSeen

			recs, ok := shardMap[shardID]
			if !ok {
				recs = make(rocksstorage.ShardRecords)
				shardMap[shardID] = recs
			}

			if existing, exists := recs[key]; exists {
				if first == 0 || (existing.FirstSeen != 0 && existing.FirstSeen < first) {
					first = existing.FirstSeen
				}
				if last == 0 || (existing.LastSeen != 0 && existing.LastSeen > last) {
					last = existing.LastSeen
				}
			}

			recs[key] = rocksstorage.TemporalRange{FirstSeen: first, LastSeen: last}
			batchEvents++
		}
	}

	if batchEvents == 0 {
		return nil
	}

	var attempt int
	for {
		attempt++
		if err := db.UpsertBatchByShard(shardMap); err != nil {
			log.Printf("[worker] Failed to write batch to DB (attempt %d): %v", attempt, err)
			if attempt >= cfg.DBMaxRetries {
				return err
			}
			delay := time.Duration(cfg.DBRetryBaseDelayMs*attempt) * time.Millisecond
			log.Printf("[worker] Retrying DB write in %s", delay)
			time.Sleep(delay)
			continue
		}
		break
	}

	for _, msg := range msgs {
		if err := msg.Ack(); err != nil {
			log.Printf("[worker] Failed to ack message: %v", err)
		}
	}

	atomic.AddUint64(totalBatches, 1)
	total := atomic.AddUint64(totalEvents, uint64(batchEvents))
	elapsed := time.Since(startTime).Seconds()
	var eps float64
	if elapsed > 0 {
		eps = float64(total) / elapsed
	}

	log.Printf("[worker] Batch processed: batchEvents=%d totalBatches=%d totalEvents=%d eps=%.2f", batchEvents, atomic.LoadUint64(totalBatches), total, eps)
	return nil
}

func runWorker(ctx context.Context, cfg Config) error {
	log.Printf("[worker] Connecting to NATS at %s", cfg.NATSURL)
	nc, err := nats.Connect(cfg.NATSURL,
		nats.Name("dns-aggregator-worker-go"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	// Ensure consumer exists (similar to JS worker's jetstreamManager usage).
	if err := ensureConsumer(js, cfg); err != nil {
		return err
	}

	sub, err := js.PullSubscribe(cfg.Subject, cfg.DurableName, nats.Bind(cfg.StreamName, cfg.DurableName))
	if err != nil {
		return err
	}

	db, err := rocksstorage.NewShardedRocksDB(cfg.NumShards, cfg.DBPath)
	if err != nil {
		return err
	}
	defer db.Close()

	log.Printf("[worker] Starting main loop with batch size %d", cfg.BatchSize)
	startTime := time.Now()
	var totalEvents uint64
	var totalBatches uint64

	for {
		select {
		case <-ctx.Done():
			log.Printf("[worker] Context cancelled, stopping loop")
			return nil
		default:
		}

		msgs, err := sub.Fetch(cfg.BatchSize, nats.MaxWait(5*time.Second))
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			log.Printf("[worker] Error fetching messages: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		if err := processBatch(db, cfg, msgs, &totalBatches, &totalEvents, startTime); err != nil {
			log.Printf("[worker] Fatal error processing batch: %v", err)
			return err
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	cfg := loadConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		s := <-sigCh
		log.Printf("[worker] Caught signal %s, shutting down", s)
		cancel()
	}()

	if err := runWorker(ctx, cfg); err != nil {
		log.Fatalf("[worker] Fatal error: %v", err)
	}
}
