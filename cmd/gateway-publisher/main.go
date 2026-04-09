package main

import (
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

// Event matches the ingest service's Event JSON schema and what the
// aggregator worker expects from JetStream.
type Event struct {
	Domain    string `json:"domain"`
	Type      string `json:"type"` // "A" or "CNAME"
	Value     string `json:"value"`
	FirstSeen int64  `json:"first_seen"`
	LastSeen  int64  `json:"last_seen"`
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

	// Basic config via flags + env vars so this can be used
	// as a template for real gateways.
	var (
		natsURL   = flag.String("nats-url", getEnv("NATS_URL", nats.DefaultURL), "NATS server URL")
		subject   = flag.String("subject", getEnv("NATS_SUBJECT", "dns.feed.v1"), "NATS subject to publish events to")
		deviceID  = flag.String("device-id", getEnv("GATEWAY_DEVICE_ID", "gateway-1"), "Device ID to include in events")
		eventsPerSecond = flag.Int("eps", 10, "Events per second to publish (for demo)")
	)
	flag.Parse()

	log.Printf("[gateway-publisher] connecting to NATS at %s", *natsURL)

	nc, err := nats.Connect(*natsURL,
		nats.Name("dns-gateway-publisher"),
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

	log.Printf("[gateway-publisher] publishing to subject %s as device %s", *subject, *deviceID)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		t := <-ticker.C

		// For demo purposes, generate a batch of synthetic events
		n := *eventsPerSecond
		if n <= 0 {
			continue
		}

		batch := make([]Event, 0, n)
		for i := 0; i < n; i++ {
			ev := randomEvent(t, *deviceID)
			batch = append(batch, ev)
		}

		data, err := json.Marshal(batch)
		if err != nil {
			log.Printf("[gateway-publisher] failed to marshal events: %v", err)
			continue
		}

		ack, err := js.Publish(*subject, data)
		if err != nil {
			log.Printf("[gateway-publisher] failed to publish batch: %v", err)
			continue
		}

		log.Printf("[gateway-publisher] published %d events to %s (seq=%d)", len(batch), *subject, ack.Sequence)
	}
}

func randomEvent(now time.Time, deviceID string) Event {
	domains := []string{
		"example.com",
		"test.internal",
		"service.local",
		"gateway.lan",
	}
	ips := []string{
		"192.0.2.1",
		"192.0.2.2",
		"198.51.100.10",
		"203.0.113.5",
	}

	domain := domains[rand.Intn(len(domains))]
	ip := ips[rand.Intn(len(ips))]
	ts := now.UnixMicro()

	return Event{
		Domain:    domain,
		Type:      "A",
		Value:     ip,
		FirstSeen: ts,
		LastSeen:  ts,
		DeviceID:  deviceID,
	}
}
