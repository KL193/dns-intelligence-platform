package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type geoRecord struct {
	start  net.IP
	end    net.IP
	country string
	city    string
}

type dnsIntelRecord struct {
	IP       string   `json:"ip"`
	Domains  []string `json:"domains"`
	LastSeen int64    `json:"last_seen"`
	Country  string   `json:"country,omitempty"`
	City     string   `json:"city,omitempty"`
}

func ipTo16(ip net.IP) net.IP {
	if ip == nil {
		return nil
	}
	if v4 := ip.To4(); v4 != nil {
		// Normalize IPv4 into IPv6-mapped 16-byte form for consistent ordering
		return v4.To16()
	}
	return ip.To16()
}

func compareIP(a, b net.IP) int {
	a16 := ipTo16(a)
	b16 := ipTo16(b)
	if a16 == nil && b16 == nil {
		return 0
	}
	if a16 == nil {
		return -1
	}
	if b16 == nil {
		return 1
	}
	return strings.Compare(string(a16), string(b16))
}

func loadGeoCSV(path string, ipVersion int) ([]geoRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)

	// Peek first record to detect header vs no-header (simple heuristic).
	first, err := reader.Read()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var (
		records []geoRecord
		isHeader bool
	)
	if len(first) >= 2 {
		// If first two columns don't start with a digit, treat as header.
		c0 := strings.TrimSpace(first[0])
		c1 := strings.TrimSpace(first[1])
		if (c0 == "" || (c0[0] < '0' || c0[0] > '9')) && (c1 == "" || (c1[0] < '0' || c1[0] > '9')) {
			isHeader = true
		}
	}

	parseRow := func(cols []string) {
		if len(cols) < 2 {
			return
		}

		// Headerless format: start_ip,end_ip,country,... (your current files).
		startStr := strings.TrimSpace(cols[0])
		endStr := strings.TrimSpace(cols[1])

		startIP := net.ParseIP(startStr)
		endIP := net.ParseIP(endStr)
		if startIP == nil || endIP == nil {
			return
		}

		if ipVersion == 4 {
			if startIP.To4() == nil || endIP.To4() == nil {
				return
			}
		} else {
			if startIP.To4() != nil || endIP.To4() != nil {
				return
			}
		}

		if compareIP(startIP, endIP) > 0 {
			return
		}

		country := ""
		if len(cols) >= 3 {
			country = strings.TrimSpace(cols[2])
		}
		city := ""
		if len(cols) > 5 {
			city = strings.TrimSpace(cols[5])
		}

		records = append(records, geoRecord{
			start:  ipTo16(startIP),
			end:    ipTo16(endIP),
			country: country,
			city:    city,
		})
	}

	if isHeader {
		// Skip header row, then parse remaining rows as headerless.
		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			parseRow(row)
		}
	} else {
		// First row is actual data.
		parseRow(first)
		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			parseRow(row)
		}
	}

	// Sort by start IP for binary search.
	sort.Slice(records, func(i, j int) bool {
		return compareIP(records[i].start, records[j].start) < 0
	})

	return records, nil
}

func binarySearchIP(ip net.IP, ranges []geoRecord) *geoRecord {
	if len(ranges) == 0 {
		return nil
	}
	target := ipTo16(ip)
	if target == nil {
		return nil
	}
	lo, hi := 0, len(ranges)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		rec := ranges[mid]
		if compareIP(target, rec.start) < 0 {
			hi = mid - 1
		} else if compareIP(target, rec.end) > 0 {
			lo = mid + 1
		} else {
			return &rec
		}
	}
	return nil
}

type geoEnricher struct {
	v4 []geoRecord
	v6 []geoRecord
}

func (g *geoEnricher) findGeo(ipStr string) *geoRecord {
	ip := net.ParseIP(strings.TrimSpace(ipStr))
	if ip == nil {
		return nil
	}
	if ip.To4() != nil {
		return binarySearchIP(ip, g.v4)
	}
	return binarySearchIP(ip, g.v6)
}

func enrichFile(inputPath, outputPath string, enricher *geoEnricher) error {
	in, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}

	out, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer out.Close()

	scanner := bufio.NewScanner(in)
	writer := bufio.NewWriter(out)
	defer writer.Flush()

	var total int
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

		if gr := enricher.findGeo(rec.IP); gr != nil {
			if gr.country != "" {
				rec.Country = gr.country
			}
			if gr.city != "" {
				rec.City = gr.city
			}
		}

		b, err := json.Marshal(rec)
		if err != nil {
			continue
		}
		if _, err := writer.Write(b); err != nil {
			return err
		}
		if err := writer.WriteByte('\n'); err != nil {
			return err
		}
		total++
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	fmt.Printf("Enriched %d IP records in %s -> %s\n", total, inputPath, outputPath)
	return nil
}

func main() {
	ipv4CSV := flag.String("ipv4-csv", "geo-data/geolite2-city-ipv4.csv", "Path to IPv4 geo CSV (uncompressed)")
	ipv6CSV := flag.String("ipv6-csv", "geo-data/geolite2-city-ipv6.csv", "Path to IPv6 geo CSV (uncompressed)")
	input := flag.String("input", "aggregator-worker/output/dns-intel.mmdb", "Input NDJSON file with {ip, domains, last_seen}")
	output := flag.String("output", "", "Optional output path for enriched NDJSON (default: overwrite input atomically)")
	flag.Parse()

	fmt.Println("Loading geo IPv4 CSV...")
	geoV4, err := loadGeoCSV(*ipv4CSV, 4)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load IPv4 CSV: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d IPv4 geo ranges.\n", len(geoV4))

	fmt.Println("Loading geo IPv6 CSV...")
	geoV6, err := loadGeoCSV(*ipv6CSV, 6)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load IPv6 CSV: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d IPv6 geo ranges.\n", len(geoV6))

	enricher := &geoEnricher{v4: geoV4, v6: geoV6}

	inPath := *input
	if _, err := os.Stat(inPath); err != nil {
		fmt.Fprintf(os.Stderr, "input file not found: %s (%v)\n", inPath, err)
		os.Exit(1)
	}

	outPath := *output
	if outPath == "" {
		outPath = inPath + ".tmp"
	}

	fmt.Printf("Enriching %s -> %s ...\n", inPath, outPath)
	if err := enrichFile(inPath, outPath, enricher); err != nil {
		fmt.Fprintf(os.Stderr, "enrichment failed: %v\n", err)
		os.Exit(1)
	}

	if *output == "" {
		if err := os.Rename(outPath, inPath); err != nil {
			fmt.Fprintf(os.Stderr, "failed to replace original file: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Replaced original file with enriched version: %s\n", inPath)
	}
}
