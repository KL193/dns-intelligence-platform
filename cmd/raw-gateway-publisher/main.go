package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

// RawDNSRecord represents a raw DNS log event emitted by a gateway.
type RawDNSRecord struct {
	Domain    string `json:"domain"`
	Type      string `json:"type"`   // "A" or "CNAME"
	Value     string `json:"value"`  // IP or CNAME target
	Timestamp int64  `json:"timestamp"` // Unix micros
	DeviceID  string `json:"device_id"`
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var (
		natsURL   = flag.String("nats-url", getEnv("NATS_URL", nats.DefaultURL), "NATS server URL")
		subject   = flag.String("subject", getEnv("RAW_SUBJECT", "dns.raw.v1"), "NATS subject for raw DNS logs")
		deviceID  = flag.String("device-id", getEnv("GATEWAY_DEVICE_ID", "gateway-raw-1"), "Gateway device ID")
		eventsPerSecond = flag.Int("eps", 10, "Raw events per second to publish (demo)")
	)
	flag.Parse()

	log.Printf("[raw-gateway] connecting to NATS at %s", *natsURL)

	nc, err := nats.Connect(*natsURL,
		nats.Name("dns-raw-gateway-publisher"),
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

	log.Printf("[raw-gateway] publishing raw DNS to subject %s as device %s (JetStream)", *subject, *deviceID)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for t := range ticker.C {
		n := *eventsPerSecond
		if n <= 0 {
			continue
		}

		batch := make([]RawDNSRecord, 0, n)
		for i := 0; i < n; i++ {
			batch = append(batch, randomRawRecord(t, *deviceID))
		}

		data, err := json.Marshal(batch)
		if err != nil {
			log.Printf("[raw-gateway] failed to marshal raw batch: %v", err)
			continue
		}

		if err := publishWithRetry(js, *subject, data, 3, time.Second); err != nil {
			log.Printf("[raw-gateway] failed to publish raw batch after retries: %v", err)
			continue
		}

		log.Printf("[raw-gateway] published %d raw events to %s", len(batch), *subject)
	}
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
		log.Printf("[raw-gateway] publish attempt %d failed: %v", attempt, err)
		time.Sleep(baseDelay * time.Duration(attempt))
	}
	return lastErr
}

func randomRawRecord(now time.Time, deviceID string) RawDNSRecord {
	// A small pool of synthetic domains (mixed public-looking + internal).
	domains := []string{
		"example.com.",
		"api.example.com.",
		"shop.example.com.",
		"media.example.net.",
		"svc.internal.",
		"db.internal.",
		"api.service.local.",
		"gateway.lan.",
	}

	domain := domains[rand.Intn(len(domains))]

	// Generate more varied public-looking IPv4 addresses by sampling from
	// several documentation/public ranges. This is purely synthetic data.
	// Examples: 203.0.113.0/24, 198.51.100.0/24, 203.0.114.0/24, 198.18.0.0/15
	ipBlocks := []struct {
		o1, o2, o3 int
	}{
		{203, 0, 113},
		{198, 51, 100},
		{203, 0, 114},
		{198, 18, 0},
	}
	blk := ipBlocks[rand.Intn(len(ipBlocks))]
	lastOctet := rand.Intn(254) + 1 // 1–254
	ip := fmt.Sprintf("%d.%d.%d.%d", blk.o1, blk.o2, blk.o3, lastOctet)
	ts := now.UnixMicro()

	return RawDNSRecord{
		Domain:    domain,
		Type:      "A",
		Value:     ip,
		Timestamp: ts,
		DeviceID:  deviceID,
	}
}
