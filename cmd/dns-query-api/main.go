package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
)

// dnsIntelRecord mirrors the enriched MMDB NDJSON line structure.
type dnsIntelRecord struct {
	IP       string   `json:"ip"`
	Domains  []string `json:"domains"`
	LastSeen int64    `json:"last_seen"`
	Country  string   `json:"country,omitempty"`
	City     string   `json:"city,omitempty"`
}

// domainView is used in domain lookup responses.
type domainView struct {
	Domain string           `json:"domain"`
	IPs    []dnsIntelRecord `json:"ips"`
}

// indexes holds in-memory lookup structures.
type indexes struct {
	ipIndex     map[string]dnsIntelRecord
	domainIndex map[string][]dnsIntelRecord
}

func loadIndexes(path string) (*indexes, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open mmdb: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 0)

	ipIdx := make(map[string]dnsIntelRecord)
	domainIdx := make(map[string][]dnsIntelRecord)

	var count int
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var rec dnsIntelRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			// Skip malformed lines but continue.
			continue
		}
		if rec.IP == "" {
			continue
		}

		// Normalize nil domains
		if rec.Domains == nil {
			rec.Domains = []string{}
		}

		// For IP index, keep the record with the latest last_seen.
		if existing, ok := ipIdx[rec.IP]; ok {
			if rec.LastSeen <= existing.LastSeen {
				goto addDomains
			}
		}
		ipIdx[rec.IP] = rec

	addDomains:
		for _, d := range rec.Domains {
			if d == "" {
				continue
			}
			domainIdx[d] = append(domainIdx[d], rec)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan mmdb: %w", err)
	}

	log.Printf("[query-api] Loaded %d records from %s (unique IPs=%d, unique domains=%d)", count, path, len(ipIdx), len(domainIdx))
	return &indexes{ipIndex: ipIdx, domainIndex: domainIdx}, nil
}

func makeIPHandler(idx *indexes) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := r.URL.Query().Get("value")
		if ip == "" {
			http.Error(w, "missing query parameter 'value'", http.StatusBadRequest)
			return
		}
		rec, ok := idx.ipIndex[ip]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "not found", "ip": ip})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(rec); err != nil {
			log.Printf("[query-api] encode ip response error: %v", err)
		}
	}
}

func makeDomainHandler(idx *indexes) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		domain := r.URL.Query().Get("value")
		if domain == "" {
			http.Error(w, "missing query parameter 'value'", http.StatusBadRequest)
			return
		}

		entries, ok := idx.domainIndex[domain]
		if !ok || len(entries) == 0 {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "not found", "domain": domain})
			return
		}

		// Deduplicate by IP and keep the latest last_seen.
		best := make(map[string]dnsIntelRecord)
		for _, e := range entries {
			if cur, ok := best[e.IP]; !ok || e.LastSeen > cur.LastSeen {
				best[e.IP] = e
			}
		}

		ips := make([]dnsIntelRecord, 0, len(best))
		for _, v := range best {
			ips = append(ips, v)
		}
		sort.Slice(ips, func(i, j int) bool { return ips[i].LastSeen > ips[j].LastSeen })

		resp := domainView{
			Domain: domain,
			IPs:    ips,
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Printf("[query-api] encode domain response error: %v", err)
		}
	}
}

func main() {
	mmdbPath := flag.String("mmdb-path", "/data/dns-intel.mmdb", "Path to enriched dns-intel.mmdb NDJSON file")
	listen := flag.String("listen", ":8080", "HTTP listen address (e.g. :8080)")
	flag.Parse()

	log.Printf("[query-api] Starting DNS query API (mmdb=%s, listen=%s)", *mmdbPath, *listen)

	idx, err := loadIndexes(*mmdbPath)
	if err != nil {
		log.Fatalf("[query-api] Failed to load indexes: %v", err)
	}

	var mu sync.RWMutex
	cur := idx

	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	http.HandleFunc("/ip", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		makeIPHandler(cur)(w, r)
	})

	http.HandleFunc("/domain", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		makeDomainHandler(cur)(w, r)
	})

	log.Printf("[query-api] Listening on %s", *listen)
	if err := http.ListenAndServe(*listen, nil); err != nil {
		log.Fatalf("[query-api] HTTP server error: %v", err)
	}
}
