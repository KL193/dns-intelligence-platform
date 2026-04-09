#!/usr/bin/env python3
"""Enrich dns-intel.mmdb IP records with geo data in-place.

Takes the existing NDJSON file produced by aggregator-worker/export-mmdb.js,
which has lines of the form:

    {"ip": "1.2.3.4", "domains": ["example.com", ...], "last_seen": 123}

and adds geo fields per IP:

    {"ip": "1.2.3.4", "domains": [...], "last_seen": 123,
     "country": "US", "city": "New York"}

The default behavior is to read from aggregator-worker/output/dns-intel.mmdb
and overwrite the same path atomically via a temporary file.
"""

import argparse
import json
import os
import sys
from typing import Optional

from dns_geo_mmdb_pipeline import GeoEnricher, load_geo_csv


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Enrich dns-intel.mmdb (IP -> {domains, last_seen}) with geo data "
            "(country, city) from GeoLite2 CSVs."
        ),
    )
    parser.add_argument(
        "--ipv4-csv",
        default="geo-data/geolite2-city-ipv4.csv",
        help="Path to IPv4 geo CSV (uncompressed).",
    )
    parser.add_argument(
        "--ipv6-csv",
        default="geo-data/geolite2-city-ipv6.csv",
        help="Path to IPv6 geo CSV (uncompressed).",
    )
    parser.add_argument(
        "--input",
        default="aggregator-worker/output/dns-intel.mmdb",
        help=(
            "Input NDJSON file with records of the form "
            "{ip, domains, last_seen}."
        ),
    )
    parser.add_argument(
        "--output",
        help=(
            "Output path for enriched NDJSON. If omitted, the input file "
            "is overwritten atomically (write to .tmp then rename)."
        ),
    )
    return parser.parse_args(argv)


def enrich_file(input_path: str, output_path: str, enricher: GeoEnricher) -> None:
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    total = 0
    with open(input_path, "r", encoding="utf-8") as fin, open(
        output_path, "w", encoding="utf-8"
    ) as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception:
                # Skip malformed lines but keep going.
                continue

            ip = record.get("ip")
            if not isinstance(ip, str):
                continue

            geo_rec = None
            try:
                geo_rec = enricher.find_geo(ip)
            except Exception:
                # If geo lookup fails for any reason, just emit without geo.
                geo_rec = None

            if geo_rec is not None:
                if geo_rec.country:
                    record["country"] = geo_rec.country
                if geo_rec.city:
                    record["city"] = geo_rec.city

            fout.write(f"{json.dumps(record)}\n")
            total += 1

    print(f"Enriched {total} IP records in {input_path} -> {output_path}")


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)

    print("Loading geo IPv4 CSV...")
    geo_ipv4 = load_geo_csv(args.ipv4_csv, ip_version=4)
    print(f"Loaded {len(geo_ipv4)} IPv4 geo ranges.")

    print("Loading geo IPv6 CSV...")
    geo_ipv6 = load_geo_csv(args.ipv6_csv, ip_version=6)
    print(f"Loaded {len(geo_ipv6)} IPv6 geo ranges.")

    enricher = GeoEnricher(geo_ipv4=geo_ipv4, geo_ipv6=geo_ipv6)

    input_path = args.input
    if args.output:
        output_path = args.output
    else:
        # Overwrite input atomically via a temporary file.
        output_path = f"{input_path}.tmp"

    print(f"Enriching {input_path} -> {output_path} ...")
    enrich_file(input_path=input_path, output_path=output_path, enricher=enricher)

    if not args.output:
        # Replace original file atomically.
        os.replace(output_path, input_path)
        print(f"Replaced original file with enriched version: {input_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        sys.exit(1)
