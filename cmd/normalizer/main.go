package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/net/idna"
)

// RawDNSRecord must match what raw gateways publish on dns.raw.v1.
type RawDNSRecord struct {
	Domain    string `json:"domain"`
	Type      string `json:"type"`
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
	DeviceID  string `json:"device_id"`
}

// Temporal, DomainCorrelation, and Feed mirror the HTTP ingest types so we can
// accept full feed JSON from gateways via NATS as well.

type Temporal struct {
	FirstSeen  int64 `json:"first_seen"`
	LastSeen   int64 `json:"last_seen"`
	TTL        int64 `json:"ttl"`
	QueryCount int64 `json:"query_count"`
}

type DomainCorrelation struct {
	Domain      string    `json:"domain"`
	AnswerIPs   []string  `json:"answer_ips"`
	CNameTarget *string   `json:"cname_target"`
	FinalDomain string    `json:"final_domain"`
	Temporal    *Temporal `json:"temporal"`
}

type Feed struct {
	FeedType           string              `json:"feed_type"`
	Version            string              `json:"version"`
	DeviceID           string              `json:"device_id"`
	Timestamp          int64               `json:"timestamp"`
	DomainCorrelations []DomainCorrelation `json:"domain_correlations"`
}

// Event matches the normalized event schema consumed by the aggregator worker.
type Event struct {
	Domain    string `json:"domain"`
	Type      string `json:"type"`
	Value     string `json:"value"`
	FirstSeen int64  `json:"first_seen"`
	LastSeen  int64  `json:"last_seen"`
	DeviceID  string `json:"device_id"`
}

// ensureStreams makes sure the JetStream streams for raw input and normalized
// feed output exist. It is safe to call on every startup; if a stream already
// exists it will be reused.
func ensureStreams(js nats.JetStreamContext, rawStream, rawSubject, feedStream, feedSubject string) error {
	// Ensure raw stream
	if _, err := js.StreamInfo(rawStream); err != nil {
		if err == nats.ErrStreamNotFound {
			cfg := &nats.StreamConfig{
				Name:      rawStream,
				Subjects:  []string{rawSubject},
				Storage:   nats.FileStorage,
				Retention: nats.LimitsPolicy,
				Replicas:  1,
			}
			if _, err := js.AddStream(cfg); err != nil {
				return fmt.Errorf("add raw stream %s: %w", rawStream, err)
			}
		} else {
			return fmt.Errorf("raw stream info %s: %w", rawStream, err)
		}
	}

	// Ensure feed stream
	if _, err := js.StreamInfo(feedStream); err != nil {
		if err == nats.ErrStreamNotFound {
			cfg := &nats.StreamConfig{
				Name:      feedStream,
				Subjects:  []string{feedSubject},
				Storage:   nats.FileStorage,
				Retention: nats.LimitsPolicy,
				Replicas:  1,
			}
			if _, err := js.AddStream(cfg); err != nil {
				return fmt.Errorf("add feed stream %s: %w", feedStream, err)
			}
		} else {
			return fmt.Errorf("feed stream info %s: %w", feedStream, err)
		}
	}

	return nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getIntEnv(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		_, err := fmt.Sscanf(v, "%d", &n)
		if err == nil && n > 0 {
			return n
		}
	}
	return def
}

// normalizeDomain converts a raw domain to a canonical lowercase, no-trailing-dot, IDNA ASCII form.
func normalizeDomain(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	lower := strings.ToLower(trimmed)
	lower = strings.TrimRight(lower, ".")
	if lower == "" {
		return "", nil
	}
	ascii, err := idna.Lookup.ToASCII(lower)
	if err != nil {
		return "", err
	}
	return ascii, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	natsURL := getEnv("NATS_URL", nats.DefaultURL)
	rawSubject := getEnv("RAW_SUBJECT", "dns.raw.v1")
	feedSubject := getEnv("FEED_SUBJECT", "dns.feed.v1")
	rawStreamName := getEnv("RAW_STREAM_NAME", "DNS_RAW")
	feedStreamName := getEnv("FEED_STREAM_NAME", "DNS_FEED")
	durable := getEnv("NORMALIZER_DURABLE", "dns-normalizer")
	batchSize := getIntEnv("NORMALIZER_BATCH_SIZE", 100)

	log.Printf("[normalizer] connecting to NATS at %s", natsURL)

	nc, err := nats.Connect(natsURL,
		nats.Name("dns-normalizer"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("failed to create JetStream context: %v", err)
	}

	// Ensure required JetStream streams exist for raw and feed subjects.
	if err := ensureStreams(js, rawStreamName, rawSubject, feedStreamName, feedSubject); err != nil {
		log.Fatalf("[normalizer] failed to ensure JetStream streams: %v", err)
	}

	log.Printf("[normalizer] using rawStream=%s, feedStream=%s, durable=%s, subject=%s, batchSize=%d", rawStreamName, feedStreamName, durable, rawSubject, batchSize)

	sub, err := js.PullSubscribe(rawSubject, durable, nats.BindStream(rawStreamName))
	if err != nil {
		log.Fatalf("failed to create PullSubscribe on %s: %v", rawSubject, err)
	}

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)

	running := true
	go func() {
		<-stopCh
		log.Printf("[normalizer] shutdown signal received, stopping main loop")
		running = false
	}()

	for running {
		msgs, err := sub.Fetch(batchSize, nats.MaxWait(5*time.Second))
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			if !running {
				break
			}
			log.Printf("[normalizer] error fetching messages: %v", err)
			time.Sleep(time.Second)
			continue
		}

		for _, msg := range msgs {
			processMessage(js, msg, feedSubject)
		}
	}

	log.Printf("[normalizer] draining NATS connection")
	if err := nc.Drain(); err != nil {
		log.Printf("[normalizer] error draining NATS connection: %v", err)
	}
}

// processMessage inspects the payload and handles both raw DNS records and full
// Feed JSON messages. It publishes normalized events to feedSubject and
// acknowledges the JetStream message only on success.
func processMessage(js nats.JetStreamContext, msg *nats.Msg, feedSubject string) {
	// First, try interpreting the message as raw DNS records.
	if events, ok := eventsFromRaw(msg); ok {
		if err := publishAndAck(js, msg, feedSubject, events); err != nil {
			// Error already logged inside publishAndAck; do not ack to allow redelivery.
		}
		return
	}

	// Fallback: try full Feed JSON from gateway.
	if events, ok := eventsFromFeed(msg); ok {
		if err := publishAndAck(js, msg, feedSubject, events); err != nil {
			// Error already logged; do not ack.
		}
		return
	}

	// Unknown payload shape; ack to avoid poison-pill redelivery loop.
	log.Printf("[normalizer] message did not match raw or feed formats; acking and skipping")
	if err := msg.Ack(); err != nil {
		log.Printf("[normalizer] failed to ack unrecognized message: %v", err)
	}
}

// eventsFromRaw attempts to interpret the message payload as one or more
// RawDNSRecord values and returns the corresponding Events if successful.
func eventsFromRaw(msg *nats.Msg) ([]Event, bool) {
	var batch []RawDNSRecord
	if err := json.Unmarshal(msg.Data, &batch); err != nil {
		// Fallback: single record
		var single RawDNSRecord
		if err2 := json.Unmarshal(msg.Data, &single); err2 != nil {
			// Not a raw record payload.
			return nil, false
		}
		batch = []RawDNSRecord{single}
	}

	if len(batch) == 0 {
		return []Event{}, true
	}

	events := make([]Event, 0, len(batch))
	for _, r := range batch {
		if strings.TrimSpace(r.Domain) == "" || strings.TrimSpace(r.Value) == "" {
			continue
		}
		normDomain, err := normalizeDomain(r.Domain)
		if err != nil {
			log.Printf("[normalizer] failed to normalize domain %q: %v", r.Domain, err)
			continue
		}
		if normDomain == "" {
			continue
		}

		if r.Type != "A" && r.Type != "CNAME" {
			// Unknown or unsupported type; skip
			continue
		}

		// For now, first_seen == last_seen == raw timestamp.
		events = append(events, Event{
			Domain:    normDomain,
			Type:      r.Type,
			Value:     strings.TrimSpace(r.Value),
			FirstSeen: r.Timestamp,
			LastSeen:  r.Timestamp,
			DeviceID:  r.DeviceID,
		})
	}

	return events, true
}

// eventsFromFeed interprets the payload as a full Feed JSON document and
// flattens it into Events compatible with the worker and RocksDB pipeline.
func eventsFromFeed(msg *nats.Msg) ([]Event, bool) {
	var feed Feed
	if err := json.Unmarshal(msg.Data, &feed); err != nil {
		return nil, false
	}

	if feed.FeedType != "dns_intelligence_comprehensive" {
		log.Printf("[normalizer] feed with unexpected feed_type=%q", feed.FeedType)
	}

	events := make([]Event, 0, len(feed.DomainCorrelations)*2)
	for _, dc := range feed.DomainCorrelations {
		if strings.TrimSpace(dc.Domain) == "" {
			continue
		}
		normDomain, err := normalizeDomain(dc.Domain)
		if err != nil {
			log.Printf("[normalizer] failed to normalize feed domain %q: %v", dc.Domain, err)
			continue
		}
		if normDomain == "" {
			continue
		}

		var firstSeen, lastSeen int64
		if dc.Temporal != nil {
			firstSeen = dc.Temporal.FirstSeen
			lastSeen = dc.Temporal.LastSeen
		} else {
			firstSeen = feed.Timestamp
			lastSeen = feed.Timestamp
		}

		// A records from answer_ips
		for _, ip := range dc.AnswerIPs {
			if strings.TrimSpace(ip) == "" {
				continue
			}
			events = append(events, Event{
				Domain:    normDomain,
				Type:      "A",
				Value:     ip,
				FirstSeen: firstSeen,
				LastSeen:  lastSeen,
				DeviceID:  feed.DeviceID,
			})
		}

		// Optional CNAME target
		if dc.CNameTarget != nil && strings.TrimSpace(*dc.CNameTarget) != "" {
			normCName, err := normalizeDomain(*dc.CNameTarget)
			if err != nil {
				log.Printf("[normalizer] failed to normalize cname_target %q: %v", *dc.CNameTarget, err)
			} else if normCName != "" {
				events = append(events, Event{
					Domain:    normDomain,
					Type:      "CNAME",
					Value:     normCName,
					FirstSeen: firstSeen,
					LastSeen:  lastSeen,
					DeviceID:  feed.DeviceID,
				})
			}
		}
	}

	return events, true
}

// publishAndAck marshals events, publishes them to feedSubject with retry, and
// acks the JetStream message on success. If events is empty, the message is
// simply acked without publishing.
func publishAndAck(js nats.JetStreamContext, msg *nats.Msg, feedSubject string, events []Event) error {
	if len(events) == 0 {
		if err := msg.Ack(); err != nil {
			log.Printf("[normalizer] failed to ack message with no events: %v", err)
			return err
		}
		return nil
	}

	data, err := json.Marshal(events)
	if err != nil {
		log.Printf("[normalizer] failed to marshal events: %v", err)
		// Ack to avoid poison-pill redelivery of something we cannot serialize.
		if ackErr := msg.Ack(); ackErr != nil {
			log.Printf("[normalizer] also failed to ack after marshal error: %v", ackErr)
		}
		return err
	}

	if err := publishWithRetry(js, feedSubject, data, 3, time.Second); err != nil {
		log.Printf("[normalizer] failed to publish normalized events after retries: %v", err)
		// Do NOT ack; allow JetStream to redeliver.
		return err
	}

	if err := msg.Ack(); err != nil {
		log.Printf("[normalizer] failed to ack message after publish: %v", err)
		return err
	}

	log.Printf("[normalizer] published %d events to %s and acked message", len(events), feedSubject)
	return nil
}

// publishWithRetry publishes to JetStream with simple bounded retries.
func publishWithRetry(js nats.JetStreamContext, subject string, data []byte, maxAttempts int, baseDelay time.Duration) error {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		_, err := js.Publish(subject, data)
		if err == nil {
			return nil
		}
		lastErr = err
		log.Printf("[normalizer] publish attempt %d failed: %v", attempt, err)
		time.Sleep(baseDelay * time.Duration(attempt))
	}
	return lastErr
}
