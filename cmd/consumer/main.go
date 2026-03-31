package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// Event is the internal event format published to JetStream.
// It matches the ingest service's event structure.
type Event struct {
	Domain   string `json:"domain"`
	Type     string `json:"type"`
	Value    string `json:"value"`
	DeviceID string `json:"device_id"`
}

var (
	natsURL   = flag.String("nats", nats.DefaultURL, "NATS server URL")
	subject   = flag.String("subject", "dns.feed.v1", "JetStream subject")
	durable   = flag.String("durable", "dns_feed_consumer", "Durable consumer name")
	ackWait   = flag.Duration("ack-wait", 30*time.Second, "Ack wait duration")
	maxAck    = flag.Int("max-ack-pending", 1024, "Max ack pending")
)

func main() {
	flag.Parse()

	log.Printf("starting JetStream consumer (nats=%s, subject=%s, durable=%s)", *natsURL, *subject, *durable)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	nc, err := nats.Connect(*natsURL,
		nats.Name("dns-feed-consumer"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("disconnected from NATS: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("reconnected to NATS: %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Printf("NATS connection closed")
		}),
	)
	if err != nil {
		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("failed to get JetStream context: %v", err)
	}

	sub, err := js.Subscribe(*subject, func(m *nats.Msg) {
		var events []Event
		if err := json.Unmarshal(m.Data, &events); err != nil {
			log.Printf("failed to unmarshal events JSON: %v", err)
			// Terminate message to avoid redelivery loops on bad payloads.
			_ = m.Term()
			return
		}

		for _, ev := range events {
			log.Printf("[EVENT] domain: %s type: %s value: %s device: %s", ev.Domain, ev.Type, ev.Value, ev.DeviceID)
		}

		if err := m.Ack(); err != nil {
			log.Printf("failed to ack message: %v", err)
		}
	},
		nats.Durable(*durable),
		nats.ManualAck(),
		nats.AckWait(*ackWait),
		nats.MaxAckPending(*maxAck),
	)
	if err != nil {
		log.Fatalf("failed to subscribe: %v", err)
	}

	log.Printf("consumer subscribed, waiting for messages...")

	<-ctx.Done()
	log.Printf("shutdown signal received, draining subscription...")

	if err := sub.Drain(); err != nil {
		log.Printf("failed to drain subscription: %v", err)
	}

	if err := nc.Drain(); err != nil {
		log.Printf("failed to drain NATS connection: %v", err)
	}

	log.Printf("consumer stopped")
}
