package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// Structures matching the full input format

type Feed struct {
	FeedType           string              `json:"feed_type"`
	Version            string              `json:"version"`
	DeviceID           string              `json:"device_id"`
	Timestamp          int64               `json:"timestamp"`
	FeedStats          FeedStats           `json:"feed_stats"`
	DomainCorrelations []DomainCorrelation `json:"domain_correlations"`
	MLInsights         MLInsights          `json:"ml_insights"`
}

type FeedStats struct {
	TotalCorrelations int `json:"total_correlations"`
	TotalQueries      int `json:"total_queries"`
	TotalResponses    int `json:"total_responses"`
	CacheHits         int `json:"cache_hits"`
	CacheMisses       int `json:"cache_misses"`
}

type DomainCorrelation struct {
	Domain       string        `json:"domain"`
	AnswerIPs    []string      `json:"answer_ips"`
	CNameTarget  *string       `json:"cname_target"`
	FinalDomain  string        `json:"final_domain"`
	NDPIProtocol NDPIProtocol  `json:"ndpi_protocol"`
	Intelligence Intelligence  `json:"intelligence"`
	Temporal     Temporal      `json:"temporal"`
}

type NDPIProtocol struct {
	ProtocolID      int    `json:"protocol_id"`
	ProtocolName    string `json:"protocol_name"`
	MasterID        int    `json:"master_id"`
	MasterName      string `json:"master_name"`
	ByIPID          int    `json:"by_ip_id"`
	ByIPName        string `json:"by_ip_name"`
	BytesSent       int64  `json:"bytes_sent"`
	BytesReceived   int64  `json:"bytes_received"`
	PacketsSent     int64  `json:"packets_sent"`
	PacketsReceived int64  `json:"packets_received"`
}

type Intelligence struct {
	IsWhatsappRelated  bool    `json:"is_whatsapp_related"`
	IsFacebookRelated  bool    `json:"is_facebook_related"`
	IsSuspicious       bool    `json:"is_suspicious"`
	IsMalware          bool    `json:"is_malware"`
	IsVPN              bool    `json:"is_vpn"`
	ThreatScore        float64 `json:"threat_score"`
	ConfidenceScore    float64 `json:"confidence_score"`
}

type Temporal struct {
	FirstSeen  int64 `json:"first_seen"`
	LastSeen   int64 `json:"last_seen"`
	TTL        int64 `json:"ttl"`
	QueryCount int64 `json:"query_count"`
}

type MLInsights struct {
	InfrastructurePatterns InfrastructurePatterns `json:"infrastructure_patterns"`
	BehavioralAnalysis     BehavioralAnalysis     `json:"behavioral_analysis"`
}

type InfrastructurePatterns struct {
	GoogleDomains      int `json:"google_domains"`
	CDNDomains         int `json:"cdn_domains"`
	SocialMediaDomains int `json:"social_media_domains"`
}

type BehavioralAnalysis struct {
	PeakQueryTime  int     `json:"peak_query_time"`
	AverageTTL     float64 `json:"average_ttl"`
	UniqueIPCount  int     `json:"unique_ip_count"`
}

var (
	endpoint   = flag.String("endpoint", "http://localhost:3000/api/v1/dnsfeed", "Ingest service endpoint")
	devices    = flag.Int("devices", 500, "Number of simulated devices")
	rps        = flag.Int("rps", 100, "Target requests per second")
	batchSize  = flag.Int("batch", 5, "Average domain correlations per feed")
	duration   = flag.Duration("duration", 0, "Run duration (0=infinite)")
	seed       = flag.Int64("seed", 0, "Random seed (0=use time)")
)

var httpClient *http.Client

func main() {
	flag.Parse()

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	rand.Seed(*seed)

	httpClient = &http.Client{
		Timeout: 5 * time.Second,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("starting DNS feed generator -> %s (devices=%d, rps=%d)", *endpoint, *devices, *rps)

	if *rps <= 0 {
		log.Fatalf("rps must be > 0")
	}

	var sent uint64

	// Rate limiter: global token bucket
	tokens := make(chan struct{}, *rps*2)
	ticker := time.NewTicker(time.Second / time.Duration(*rps))
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(tokens)
				return
			case <-ticker.C:
				select {
				case tokens <- struct{}{}:
				default:
				}
			}
		}
	}()

	// Optional duration-based shutdown
	if *duration > 0 {
		go func() {
			select {
			case <-ctx.Done():
			case <-time.After(*duration):
				log.Printf("duration %s reached, shutting down generator", *duration)
				cancel()
			}
		}()
	}

	workerCount := runtime.NumCPU() * 2
	log.Printf("starting %d workers", workerCount)

	for i := 0; i < workerCount; i++ {
		go func(id int) {
			for range tokens {
				if ctx.Err() != nil {
					return
				}
				feed := generateFeed(*devices, *batchSize)
				if err := sendFeed(ctx, feed); err != nil {
					log.Printf("worker %d: send error: %v", id, err)
					continue
				}
				atomic.AddUint64(&sent, 1)
			}
		}(i)
	}

	<-ctx.Done()
	log.Printf("generator stopped, total feeds sent=%d", atomic.LoadUint64(&sent))
}

func sendFeed(ctx context.Context, feed Feed) error {
	body, err := json.Marshal(feed)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, *endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Drain body to allow connection reuse.
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	return nil
}

func generateFeed(deviceCount, avgBatch int) Feed {
	ts := time.Now().UnixMicro()

	nCorrelations := 1 + rand.Intn(max(1, avgBatch*2))
	if nCorrelations < 1 {
		nCorrelations = 1
	}

	domainCorrelations := make([]DomainCorrelation, 0, nCorrelations)

	uniqueIPs := make(map[string]struct{})
	var totalQueries int
	var totalResponses int
	var ttlSum int64

	var googleDomains, cdnDomains, socialMediaDomains int

	for i := 0; i < nCorrelations; i++ {
		domain, base := randomDomain()

		ips := randomIPs(1 + rand.Intn(5))
		for _, ip := range ips {
			uniqueIPs[ip] = struct{}{}
		}

		firstSeen, lastSeen, ttl, queryCount := randomTemporal(ts)
		ttlSum += ttl
		totalQueries += int(queryCount)
		totalResponses += len(ips)

		if strings.Contains(base, "google") {
			googleDomains++
		}
		if strings.Contains(domain, "cdn.") {
			cdnDomains++
		}
		if strings.Contains(base, "facebook") || strings.Contains(base, "instagram") || strings.Contains(base, "tiktok") {
			socialMediaDomains++
		}

		var cname *string
		if rand.Float64() < 0.3 {
			c := "cdn." + base
			cname = &c
		}

		dc := DomainCorrelation{
			Domain:       domain,
			AnswerIPs:    ips,
			CNameTarget:  cname,
			FinalDomain:  base,
			NDPIProtocol: randomProtocol(),
			Intelligence: randomIntelligence(base),
			Temporal: Temporal{
				FirstSeen:  firstSeen,
				LastSeen:   lastSeen,
				TTL:        ttl,
				QueryCount: queryCount,
			},
		}
		domainCorrelations = append(domainCorrelations, dc)
	}

	uniqueIPCount := len(uniqueIPs)
	avgTTL := float64(0)
	if nCorrelations > 0 {
		avgTTL = float64(ttlSum) / float64(nCorrelations)
	}

	totalCorrelations := len(domainCorrelations)
	if totalCorrelations == 0 {
		totalCorrelations = 1
	}

	cacheHits := int(float64(totalQueries) * 0.7)
	cacheMisses := totalQueries - cacheHits
	if cacheHits < 0 {
		cacheHits = 0
	}
	if cacheMisses < 0 {
		cacheMisses = 0
	}

	deviceID := "sensor_" + strconv.Itoa(1+rand.Intn(max(1, deviceCount)))

	feed := Feed{
		FeedType:  "dns_intelligence_comprehensive",
		Version:   "1.0",
		DeviceID:  deviceID,
		Timestamp: ts,
		FeedStats: FeedStats{
			TotalCorrelations: totalCorrelations,
			TotalQueries:      totalQueries,
			TotalResponses:    totalResponses,
			CacheHits:         cacheHits,
			CacheMisses:       cacheMisses,
		},
		DomainCorrelations: domainCorrelations,
		MLInsights: MLInsights{
			InfrastructurePatterns: InfrastructurePatterns{
				GoogleDomains:      googleDomains,
				CDNDomains:         cdnDomains,
				SocialMediaDomains: socialMediaDomains,
			},
			BehavioralAnalysis: BehavioralAnalysis{
				PeakQueryTime: int(time.Now().Hour()),
				AverageTTL:    avgTTL,
				UniqueIPCount: uniqueIPCount,
			},
		},
	}

	return feed
}

var baseDomains = []string{
	"google.com",
	"facebook.com",
	"amazon.com",
	"cloudflare.com",
	"netflix.com",
	"apple.com",
	"tiktok.com",
	"instagram.com",
}

var subdomainPrefixes = []string{
	"www",
	"api",
	"cdn",
	"video",
	"static",
	"auth",
}

func randomDomain() (domain string, base string) {
	base = baseDomains[rand.Intn(len(baseDomains))]
	if rand.Float64() < 0.6 {
		prefix := subdomainPrefixes[rand.Intn(len(subdomainPrefixes))]
		return prefix + "." + base, base
	}
	return base, base
}

var publicIPPool = []string{
	"1.1.1.1",
	"8.8.8.8",
	"8.8.4.4",
	"9.9.9.9",
	"208.67.222.222",
	"208.67.220.220",
}

// randomPublicIPv4 returns a random IPv4 address that is not in
// common private/reserved ranges, so it is very likely to have
// geo data in public datasets.
func randomPublicIPv4() string {
	for {
		a := rand.Intn(256)
		b := rand.Intn(256)
		c := rand.Intn(256)
		d := rand.Intn(256)

		// Skip private, loopback, link-local, and multicast/reserved ranges.
		if a == 10 || // 10.0.0.0/8
			(a == 172 && b >= 16 && b <= 31) || // 172.16.0.0/12
			(a == 192 && b == 168) || // 192.168.0.0/16
			a == 127 || // 127.0.0.0/8
			a == 0 || // 0.0.0.0/8
			(a == 169 && b == 254) || // 169.254.0.0/16
			a >= 224 { // multicast and reserved
			continue
		}

		return fmt.Sprintf("%d.%d.%d.%d", a, b, c, d)
	}
}

func randomIPs(count int) []string {
	if count < 1 {
		count = 1
	}
	ips := make([]string, 0, count)
	for i := 0; i < count; i++ {
		if rand.Float64() < 0.3 {
			// well-known public resolvers/CDN IPs
			ips = append(ips, publicIPPool[rand.Intn(len(publicIPPool))])
		} else {
			// random public IPv4 outside private/reserved ranges
			ips = append(ips, randomPublicIPv4())
		}
	}
	return ips
}

func randomTemporal(nowMicros int64) (firstSeen, lastSeen, ttl, queryCount int64) {
	// TTL between 60 and 3600
	ttl = int64(60 + rand.Intn(3600-60+1))
	// queries between 1 and 100
	queryCount = int64(1 + rand.Intn(100))

	// first_seen < last_seen <= now
	window := int64(5 * time.Minute / time.Microsecond)
	if window <= 0 {
		window = int64(60 * time.Second / time.Microsecond)
	}
	delta := rand.Int63n(window)
	firstSeen = nowMicros - delta
	if firstSeen < 0 {
		firstSeen = 0
	}
	lastSeen = firstSeen + rand.Int63n(int64(time.Minute/time.Microsecond))
	if lastSeen > nowMicros {
		lastSeen = nowMicros
	}
	return
}

func randomProtocol() NDPIProtocol {
	type proto struct {
		id   int
		name string
	}
	protos := []proto{
		{0, "Unknown"},
		{80, "HTTP"},
		{443, "TLS"},
		{53, "DNS"},
	}

	// Mostly Unknown
	p := protos[0]
	v := rand.Float64()
	if v < 0.1 {
		p = protos[1]
	} else if v < 0.18 {
		p = protos[2]
	} else if v < 0.25 {
		p = protos[3]
	}

	bytesSent := int64(500 + rand.Intn(50_000))
	bytesRecv := int64(500 + rand.Intn(50_000))

	return NDPIProtocol{
		ProtocolID:      p.id,
		ProtocolName:    p.name,
		MasterID:        p.id,
		MasterName:      p.name,
		ByIPID:          p.id,
		ByIPName:        p.name,
		BytesSent:       bytesSent,
		BytesReceived:   bytesRecv,
		PacketsSent:     int64(1 + rand.Intn(500)),
		PacketsReceived: int64(1 + rand.Intn(500)),
	}
}

func randomIntelligence(baseDomain string) Intelligence {
	isSuspicious := rand.Float64() < 0.05
	isMalware := rand.Float64() < 0.01
	isVPN := rand.Float64() < 0.05

	isFacebook := strings.Contains(baseDomain, "facebook") || strings.Contains(baseDomain, "instagram")
	isWhatsapp := strings.Contains(baseDomain, "whatsapp")

	threat := 0.0
	if isMalware {
		threat = 80 + rand.Float64()*20
	} else if isSuspicious {
		threat = 40 + rand.Float64()*40
	} else {
		threat = rand.Float64() * 20
	}

	confidence := 0.3 + rand.Float64()*0.7

	return Intelligence{
		IsWhatsappRelated: isWhatsapp,
		IsFacebookRelated: isFacebook,
		IsSuspicious:      isSuspicious,
		IsMalware:         isMalware,
		IsVPN:             isVPN,
		ThreatScore:       threat,
		ConfidenceScore:   confidence,
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
