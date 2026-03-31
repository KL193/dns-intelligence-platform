package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"golang.org/x/net/idna"
)

// Input types

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

// Internal event type

type Event struct {
	Domain    string `json:"domain"`
	Type      string `json:"type"` // "A" or "CNAME"
	Value     string `json:"value"`
	FirstSeen int64  `json:"first_seen"`
	LastSeen  int64  `json:"last_seen"`
	DeviceID  string `json:"device_id"`
}

// normalizeDomain converts a raw domain to a canonical lowercase, no-trailing-dot, IDNA ASCII form.
func normalizeDomain(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	lower := strings.ToLower(trimmed)
	// Remove all trailing dots
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

// validateFeed performs top-level and per-record validation.
func validateFeed(feed *Feed) error {
	if feed.FeedType != "dns_intelligence_comprehensive" {
		return &clientError{Message: "invalid feed_type"}
	}
	if len(feed.DomainCorrelations) == 0 {
		return &clientError{Message: "domain_correlations is required and must be non-empty"}
	}

	for i, dc := range feed.DomainCorrelations {
		if strings.TrimSpace(dc.Domain) == "" {
			return &clientError{Message: "domain is required for domain_correlations[" + strconv.Itoa(i) + "]"}
		}
		// answer_ips must be an array (can be empty); nil means field missing
		if dc.AnswerIPs == nil {
			return &clientError{Message: "answer_ips must be an array for domain_correlations[" + strconv.Itoa(i) + "]"}
		}
	}
	return nil
}

// extractEvents transforms the feed into flat internal events.
func extractEvents(feed *Feed) ([]Event, error) {
	// Rough capacity hint: up to 2 events per correlation (A records + optional CNAME)
	events := make([]Event, 0, len(feed.DomainCorrelations)*2)

	for _, dc := range feed.DomainCorrelations {
		normDomain, err := normalizeDomain(dc.Domain)
		if err != nil {
			return nil, &serverError{Message: "failed to normalize domain", Err: err}
		}
		if normDomain == "" {
			// Treat empty after normalization as client error
			return nil, &clientError{Message: "domain normalization resulted in empty domain"}
		}

		var firstSeen, lastSeen int64
		if dc.Temporal != nil {
			firstSeen = dc.Temporal.FirstSeen
			lastSeen = dc.Temporal.LastSeen
		} else {
			// Fallback to top-level timestamp if temporal is missing
			firstSeen = feed.Timestamp
			lastSeen = feed.Timestamp
		}

		for _, ip := range dc.AnswerIPs {
			// answer_ips may be empty; then this loop is skipped
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

		if dc.CNameTarget != nil && strings.TrimSpace(*dc.CNameTarget) != "" {
			normCName, err := normalizeDomain(*dc.CNameTarget)
			if err != nil {
				return nil, &serverError{Message: "failed to normalize cname_target", Err: err}
			}
			if normCName != "" {
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

	return events, nil
}

// Error types to distinguish client vs server errors.

type clientError struct {
	Message string
}

func (e *clientError) Error() string {
	return e.Message
}

type serverError struct {
	Message string
	Err     error
}

func (e *serverError) Error() string {
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

// errorResponse is the standard error payload.
type errorResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

// successResponse is the standard success payload.
type successResponse struct {
	Status          string `json:"status"`
	EventsProcessed int    `json:"events_processed"`
}

// JetStream context used to publish events.
var js nats.JetStreamContext

func publishEventsToJetStream(events []Event) error {
	if js == nil || len(events) == 0 {
		return nil
	}

	data, err := json.Marshal(events)
	if err != nil {
		return err
	}

	subject := getEnv("NATS_SUBJECT", "dns.feed.v1")
	_, err = js.Publish(subject, data)
	return err
}

// maxBodySizeMiddleware enforces a maximum request body size using Content-Length and MaxBytesReader.
func maxBodySizeMiddleware(limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if limit <= 0 {
			c.Next()
			return
		}

		if c.Request.ContentLength > limit {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, errorResponse{
				Status: "error",
				Error:  "request body too large",
			})
			return
		}

		// Wrap the body to enforce limit even if Content-Length is missing or wrong.
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
		c.Next()
	}
}

// errorHandlingMiddleware converts collected errors to JSON responses if handler didn't already write one.
func errorHandlingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		if len(c.Errors) == 0 {
			return
		}

		// If the handler already wrote a status code >= 400, don't override.
		status := c.Writer.Status()
		if status >= 400 {
			return
		}

		lastErr := c.Errors.Last().Err
		switch e := lastErr.(type) {
		case *clientError:
			c.JSON(http.StatusBadRequest, errorResponse{
				Status: "error",
				Error:  e.Message,
			})
		case *serverError:
			log.Printf("server error: %v", e)
			c.JSON(http.StatusInternalServerError, errorResponse{
				Status: "error",
				Error:  e.Message,
			})
		default:
			log.Printf("unhandled error: %v", lastErr)
			c.JSON(http.StatusInternalServerError, errorResponse{
				Status: "error",
				Error:  "internal server error",
			})
		}
	}
}

// ingestHandler is the POST /api/v1/dnsfeed handler.
func ingestHandler(c *gin.Context) {
	var feed Feed
	if err := c.ShouldBindJSON(&feed); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{
			Status: "error",
			Error:  "invalid JSON payload",
		})
		return
	}

	if err := validateFeed(&feed); err != nil {
		if _, ok := err.(*clientError); ok {
			c.JSON(http.StatusBadRequest, errorResponse{
				Status: "error",
				Error:  err.Error(),
			})
			return
		}
		log.Printf("validation server error: %v", err)
		c.JSON(http.StatusInternalServerError, errorResponse{
			Status: "error",
			Error:  "validation failed",
		})
		return
	}

	events, err := extractEvents(&feed)
	if err != nil {
		switch err.(type) {
		case *clientError:
			c.JSON(http.StatusBadRequest, errorResponse{
				Status: "error",
				Error:  err.Error(),
			})
		default:
			log.Printf("event extraction error: %v", err)
			c.JSON(http.StatusInternalServerError, errorResponse{
				Status: "error",
				Error:  "failed to extract events",
			})
		}
		return
	}

	// Publish to JetStream (best-effort; log failures and return 500).
	if err := publishEventsToJetStream(events); err != nil {
		log.Printf("jetstream publish error: %v", err)
		c.JSON(http.StatusInternalServerError, errorResponse{
			Status: "error",
			Error:  "failed to publish events",
		})
		return
	}

	// Structured logging: one line per event as JSON
	for _, ev := range events {
		b, err := json.Marshal(ev)
		if err != nil {
			log.Printf("failed to marshal event for logging: %v", err)
			continue
		}
		log.Printf("event=%s", string(b))
	}

	c.JSON(http.StatusOK, successResponse{
		Status:          "success",
		EventsProcessed: len(events),
	})
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getMaxBodyBytes() int64 {
	v := os.Getenv("INGEST_MAX_BODY_BYTES")
	if v == "" {
		// Default: 2MB
		return 2 * 1024 * 1024
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 2 * 1024 * 1024
	}
	return n
}

func main() {
	// Basic logger configuration
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	addr := getEnv("INGEST_HTTP_ADDR", ":3000")
	maxBody := getMaxBodyBytes()
	natsURL := getEnv("NATS_URL", nats.DefaultURL)

	// Connect to NATS JetStream for publishing events.
	nc, err := nats.Connect(natsURL,
		nats.Name("dns-ingest-service"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("failed to connect to NATS at %s: %v", natsURL, err)
	}
	defer nc.Close()

	jsCtx, err := nc.JetStream()
	if err != nil {
		log.Fatalf("failed to create JetStream context: %v", err)
	}
	js = jsCtx

	log.Printf("starting DNS ingest service on %s (max body %d bytes, nats=%s)", addr, maxBody, natsURL)

	// Gin in release mode for production-style behavior by default
	if ginMode := getEnv("GIN_MODE", "release"); ginMode != "" {
		gin.SetMode(ginMode)
	}

	r := gin.New()
	r.Use(gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		return fmt.Sprintf("%s - [%s] \"%s %s %s\" %d %s\n",
			param.ClientIP,
			param.TimeStamp.Format(time.RFC3339Nano),
			param.Method,
			param.Path,
			param.Request.Proto,
			param.StatusCode,
			param.Latency,
		)
	}))
	r.Use(gin.Recovery())
	r.Use(errorHandlingMiddleware())
	r.Use(maxBodySizeMiddleware(maxBody))

	r.POST("/api/v1/dnsfeed", ingestHandler)

	if err := r.Run(addr); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
