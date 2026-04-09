#!/usr/bin/env python3
"""Query DNS intelligence MMDB (NDJSON) efficiently.

This script loads the final enriched dns-intel.mmdb NDJSON file once,
builds two in-memory indexes, and lets you query:

- IP  -> {domains, last_seen, country, city}
- FQDN/domain -> list of {ip, last_seen, country, city}

Usage examples (from repo root, after copying out dns-intel.mmdb):

    python scripts/query_dns_intel.py --mmdb dns-intel.mmdb --ip 1.2.3.4
    python scripts/query_dns_intel.py --mmdb dns-intel.mmdb --domain example.com

"""

import argparse
import json
from collections import defaultdict
from typing import Any, Dict, List, Optional


def load_indexes(path: str) -> tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    ip_index: Dict[str, Dict[str, Any]] = {}
    domain_index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    count = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue

            ip = rec.get("ip")
            if not isinstance(ip, str):
                continue

            # Normalize record shape
            entry = {
                "ip": ip,
                "domains": rec.get("domains", []) or [],
                "last_seen": rec.get("last_seen"),
                "country": rec.get("country"),
                "city": rec.get("city"),
            }

            ip_index[ip] = entry

            for d in entry["domains"]:
                if not isinstance(d, str) or not d:
                    continue
                domain_index[d].append(entry)

            count += 1

    print(f"Loaded {count} records from {path}")
    print(f"Unique IPs: {len(ip_index)}; Unique domains: {len(domain_index)}")
    return ip_index, domain_index


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query dns-intel.mmdb (NDJSON) for IPs and domains.")
    parser.add_argument("--mmdb", required=True, help="Path to enriched dns-intel.mmdb NDJSON file")
    parser.add_argument("--ip", help="Lookup by IP address (ip -> domains + geo)")
    parser.add_argument("--domain", help="Lookup by domain/FQDN (domain -> IPs + geo)")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    if not args.ip and not args.domain:
        raise SystemExit("You must supply --ip or --domain (or both)")

    ip_index, domain_index = load_indexes(args.mmdb)

    if args.ip:
        rec = ip_index.get(args.ip)
        if not rec:
            print(f"No record found for IP {args.ip}")
        else:
            print(json.dumps(rec, indent=2, sort_keys=True))

    if args.domain:
        entries = domain_index.get(args.domain, [])
        if not entries:
            print(f"No records found for domain {args.domain}")
        else:
            # Deduplicate by IP for cleaner output
            seen: Dict[str, Dict[str, Any]] = {}
            for e in entries:
                ip = e["ip"]
                if ip not in seen or (e.get("last_seen") or 0) > (seen[ip].get("last_seen") or 0):
                    seen[ip] = e
            result = {
                "domain": args.domain,
                "ips": sorted(seen.values(), key=lambda x: (x.get("last_seen") or 0), reverse=True),
            }
            print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
