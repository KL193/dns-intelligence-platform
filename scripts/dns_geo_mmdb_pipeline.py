#!/usr/bin/env python3
"""
DNS + Geo MMDB Enrichment Pipeline.

Builds an enriched MMDB with records of the form:

    IP/network -> {
        "domain": <domain>,
        "country": <ISO country code or None>,
        "city": <city name or None>
    }

Data sources:
  1. Existing MMDB: IP -> {"domain": ...}
  2. Geo CSVs: IPv4 and IPv6 city databases from @ip-location-db/geolite2-city.

Core constraints:
  - IPv4 and IPv6 data must never be mixed.
  - All IPs are converted to integers before comparison.
  - Range lookup is done via binary search, NOT linear scan.
  - Supports incremental updates via a staging layer.
"""

import argparse
import csv
import datetime as _dt
import ipaddress
import json
import os
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

try:
    # Optional dependency: only needed when writing a real MMDB file.
    from maxminddb.writer import Writer, IPVersion  # type: ignore

    _HAS_MMDB_WRITER = True
except Exception:  # ImportError or any other issue
    _HAS_MMDB_WRITER = False


# =========================
# Data structures
# =========================

@dataclass
class GeoRecord:
    start: int          # inclusive, integer IP
    end: int            # inclusive, integer IP
    country: str        # ISO country code (may be empty string)
    city: str           # city name (may be empty string)


# =========================
# GEO CSV LOADING
# =========================


def _parse_geo_row_dict(
    row: Dict[str, str],
    ip_version: int,
) -> Optional[GeoRecord]:
    """Parse a header-based CSV row into a GeoRecord.

    Supports two shapes:
      - 'network' field with CIDR, e.g. "1.0.0.0/24"
      - explicit 'ip_range_start' + 'ip_range_end' fields

    Returns None if the row is invalid or IP version does not match.
    """
    try:
        # Prefer CIDR 'network' column (used by some geolite2-city variants)
        if "network" in row and row["network"]:
            net_str = row["network"].strip()
            net = ipaddress.ip_network(net_str, strict=False)
            if net.version != ip_version:
                return None
            start_int = int(net.network_address)
            end_int = int(net.broadcast_address)
        else:
            # Fallback: explicit start/end columns (must be IP strings)
            start_col = row.get("ip_range_start") or row.get("start_ip")
            end_col = row.get("ip_range_end") or row.get("end_ip")
            if not start_col or not end_col:
                return None
            start_ip = ipaddress.ip_address(start_col.strip())
            end_ip = ipaddress.ip_address(end_col.strip())
            if start_ip.version != ip_version or end_ip.version != ip_version:
                return None
            start_int = int(start_ip)
            end_int = int(end_ip)
            if end_int < start_int:
                return None

        # Country / city columns in geolite2-city CSVs.
        country = (
            row.get("country_iso_code")
            or row.get("country_code")
            or row.get("country")
            or ""
        ).strip()

        city = (
            row.get("city_name")
            or row.get("city")
            or ""
        ).strip()

        return GeoRecord(
            start=start_int,
            end=end_int,
            country=country,
            city=city,
        )
    except Exception:
        # Defensive: skip any malformed row.
        return None


def _parse_geo_row_list(
    row: List[str],
    ip_version: int,
) -> Optional[GeoRecord]:
    """Parse a headerless "start_ip,end_ip,country,..." row into GeoRecord.

    This matches your current files, e.g.:
        1.0.1.0,1.0.3.255,CN,...
    """
    try:
        if len(row) < 3:
            return None

        start_ip_obj = ipaddress.ip_address(row[0].strip())
        end_ip_obj = ipaddress.ip_address(row[1].strip())

        if start_ip_obj.version != ip_version or end_ip_obj.version != ip_version:
            return None

        start_int = int(start_ip_obj)
        end_int = int(end_ip_obj)
        if end_int < start_int:
            return None

        country = row[2].strip() if row[2] else ""
        city = row[5].strip() if len(row) > 5 and row[5] else ""

        return GeoRecord(
            start=start_int,
            end=end_int,
            country=country,
            city=city,
        )
    except Exception:
        # Defensive: skip any malformed row.
        return None


def load_geo_csv(path: str, ip_version: int) -> List[GeoRecord]:
    """Load and preprocess a Geo CSV for a single IP version.

    Handles both:
      - Headerless range files: ``start_ip,end_ip,country,...`` (your case)
      - Headered files with either ``network`` CIDR or explicit range columns.

    - Converts IP ranges to integer start/end.
    - Filters to the requested IP version.
    - Sorts by start address for binary search.

    Returns a list[GeoRecord], sorted by .start.
    """
    records: List[GeoRecord] = []

    with open(path, "r", encoding="utf-8") as f:
        # Peek first line to decide if file has a header.
        first_line = f.readline()
        if not first_line:
            return []

        # Heuristic: if the first two columns don't start with a digit,
        # treat it as a header row and use DictReader.
        sample_cols = [col.strip() for col in first_line.split(",")]
        is_header = False
        if len(sample_cols) >= 2:
            is_header = not (sample_cols[0][:1].isdigit() and sample_cols[1][:1].isdigit())

        f.seek(0)

        if is_header:
            reader = csv.DictReader(f)
            for row in reader:
                rec = _parse_geo_row_dict(row, ip_version=ip_version)
                if rec is None:
                    continue
                records.append(rec)
        else:
            reader = csv.reader(f)
            for row in reader:
                rec = _parse_geo_row_list(row, ip_version=ip_version)
                if rec is None:
                    continue
                records.append(rec)

    records.sort(key=lambda r: r.start)
    return records


# =========================
# BINARY SEARCH ON RANGES
# =========================


def _binary_search_ip(ip_int: int, ranges: List[GeoRecord]) -> Optional[GeoRecord]:
    """Binary search over sorted ranges to find the record such that:
        rec.start <= ip_int <= rec.end

    Complexity: O(log N). No linear scan.
    """
    lo = 0
    hi = len(ranges) - 1

    while lo <= hi:
        mid = (lo + hi) // 2
        rec = ranges[mid]

        if ip_int < rec.start:
            hi = mid - 1
        elif ip_int > rec.end:
            lo = mid + 1
        else:
            # Found a range containing ip_int.
            return rec

    return None


class GeoEnricher:
    """Holds in-memory geo datasets and provides IP -> GeoRecord lookup
    with automatic IPv4/IPv6 routing.
    """

    def __init__(self, geo_ipv4: List[GeoRecord], geo_ipv6: List[GeoRecord]) -> None:
        self.geo_ipv4 = geo_ipv4
        self.geo_ipv6 = geo_ipv6

    def search_ipv4(self, ip_int: int) -> Optional[GeoRecord]:
        return _binary_search_ip(ip_int, self.geo_ipv4)

    def search_ipv6(self, ip_int: int) -> Optional[GeoRecord]:
        return _binary_search_ip(ip_int, self.geo_ipv6)

    def find_geo(self, ip: str) -> Optional[GeoRecord]:
        """Detect IP version, convert to integer, and route to the
        correct binary-search function.
        """
        ip_obj = ipaddress.ip_address(ip)
        ip_int = int(ip_obj)

        if ip_obj.version == 4:
            return self.search_ipv4(ip_int)
        else:
            return self.search_ipv6(ip_int)


# =========================
# EXISTING MMDB READER
# =========================


def iter_ip_domain_from_mmdb(
    mmdb_path: str,
    ip_iterable: Iterable[str],
) -> Iterator[Tuple[str, str]]:
    """Yield (ip, domain) pairs from the structured export file.

    In this repository, ``aggregator-worker/export-mmdb.js`` writes a
    structured NDJSON file to ``config.MMDB_FILENAME`` (default
    ``dns-intel.mmdb``). Each line has the shape:

        {"ip": "1.2.3.4", "domains": ["example.com", ...], "last_seen": 123}

    This function reads that file, filters to the supplied IP set
    (``ip_iterable``), and yields a single (ip, domain) pair per IP,
    choosing the first domain when multiple are present.
    """
    if not os.path.exists(mmdb_path):
        raise FileNotFoundError(f"Input file not found: {mmdb_path}")

    ip_set = set(ip_iterable)
    if not ip_set:
        return

    seen: set[str] = set()

    with open(mmdb_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception:
                continue

            ip = record.get("ip")
            if not ip or ip not in ip_set or ip in seen:
                continue

            domains = record.get("domains")
            domain: Optional[str] = None
            if isinstance(domains, list) and domains:
                domain = str(domains[0])
            elif isinstance(record.get("domain"), str):
                domain = record["domain"]

            if not domain:
                continue

            seen.add(ip)
            yield ip, domain


# =========================
# ENRICHED OUTPUT (MMDB OR NDJSON)
# =========================


def _build_writer(output_path: str) -> "Writer":
    """Create a MaxMind DB writer configured for dual-stack (IPv4+IPv6).

    Only used when maxminddb-writer is available in the environment.
    """
    f = open(output_path, "wb")

    writer = Writer(
        f,
        database_type="DNS-Geo-Intelligence",
        ip_version=IPVersion.V4 | IPVersion.V6,
        languages=["en"],
        description={"en": "IP -> {domain, country, city}"},
    )
    return writer


def _insert_enriched_record_mmdb(
    writer: "Writer",
    ip: str,
    domain: str,
    geo_rec: Optional[GeoRecord],
) -> None:
    """Insert a single-IP network record into the MMDB writer."""
    ip_obj = ipaddress.ip_address(ip)

    if ip_obj.version == 4:
        network = ipaddress.ip_network(f"{ip}/32", strict=False)
    else:
        network = ipaddress.ip_network(f"{ip}/128", strict=False)

    data: Dict[str, object] = {"domain": domain}

    if geo_rec is not None:
        if geo_rec.country:
            data["country"] = geo_rec.country
        if geo_rec.city:
            data["city"] = geo_rec.city

    writer.insert_network(network, data)


def _insert_enriched_record_ndjson(
    fh,
    ip: str,
    domain: str,
    geo_rec: Optional[GeoRecord],
) -> None:
    """Write an enriched record as a single NDJSON line.

    Fallback path used when maxminddb-writer is not installed.
    """
    record: Dict[str, object] = {"ip": ip, "domain": domain}
    if geo_rec is not None:
        if geo_rec.country:
            record["country"] = geo_rec.country
        if geo_rec.city:
            record["city"] = geo_rec.city

    fh.write(f"{json.dumps(record)}\n")


def build_enriched_output(
    existing_mmdb_path: str,
    geo_ipv4: List[GeoRecord],
    geo_ipv6: List[GeoRecord],
    ip_source: Iterable[str],
    output_path: str,
) -> None:
    """Core pipeline:

    If maxminddb-writer is available, writes a real MMDB file to
    ``output_path``. Otherwise, writes an NDJSON file with one record per
    line of the form:

        {"ip": "1.2.3.4", "domain": "example.com", "country": "US", "city": "NYC"}
    """
    enricher = GeoEnricher(geo_ipv4=geo_ipv4, geo_ipv6=geo_ipv6)

    if _HAS_MMDB_WRITER:
        writer = _build_writer(output_path)
        try:
            for ip, domain in iter_ip_domain_from_mmdb(existing_mmdb_path, ip_source):
                geo_rec = enricher.find_geo(ip)
                _insert_enriched_record_mmdb(writer, ip, domain, geo_rec)
        finally:
            writer.close()
    else:
        # Fallback: emit NDJSON so the enriched data is still usable.
        with open(output_path, "w", encoding="utf-8") as fh:
            for ip, domain in iter_ip_domain_from_mmdb(existing_mmdb_path, ip_source):
                geo_rec = enricher.find_geo(ip)
                _insert_enriched_record_ndjson(fh, ip, domain, geo_rec)


# =========================
# INCREMENTAL UPDATE NOTES
# =========================

"""Incremental update strategy (high level):

- Canonical store:
    Maintain durable canonical mapping:
        IP -> {domain, last_seen, source, version}
    in a DB or key-value store (e.g. RocksDB, PostgreSQL, etc.).
    The MMDB is a read-optimized derived artifact built from this store.

- Staging table / file:
    New DNS events arrive continuously (from NATS, Kafka, etc.) and are
    written into a staging layer, e.g. a table or a flat file:

        staging_ip_domain(ip, domain, first_seen, last_seen, source_batch)

    This staging layer is periodically compacted:
        - Normalize IP formats via ipaddress.ip_address.
        - Validate IP strings.
        - Deduplicate per IP, keeping the most recent or most frequent domain.

- Merge process (batch job, e.g. hourly):
    1. Read canonical IP->domain snapshot and staging.
    2. Identify new or changed IPs.
    3. Upsert into canonical store.
    4. The set of changed IPs from step 3 is used as ip_source for this script.

- Versioning and zero-downtime deployment:
    - Output files follow a versioned naming scheme, e.g.:
         dns-geo-YYYYMMDDHHMM.mmdb
    - Once a new MMDB is built and validated, atomically switch a symlink or
      configuration pointer so consumers start using the new file.
    - Keep previous versions for quick rollback.

Geo enrichment is deterministic given Geo CSVs and IP, so only new/changed
IPs need to be re-enriched between runs.
"""


# =========================
# SELF-TESTS (STEP 9)
# =========================


def _run_geo_search_tests() -> None:
    """Basic correctness tests for:
      - IPv4 lookup
      - IPv6 lookup
      - Unknown IP handling

    Uses an in-memory synthetic geo dataset so you can run this without
    real CSVs or MMDBs.
    """
    # Synthetic IPv4 range: 10.0.0.0/24 -> TestCountry4 / TestCity4
    v4_net = ipaddress.ip_network("10.0.0.0/24")
    geo_ipv4 = [
        GeoRecord(
            start=int(v4_net.network_address),
            end=int(v4_net.broadcast_address),
            country="TC4",
            city="TestCity4",
        )
    ]

    # Synthetic IPv6 range: 2001:db8::/120 -> TestCountry6 / TestCity6
    v6_net = ipaddress.ip_network("2001:db8::/120")
    geo_ipv6 = [
        GeoRecord(
            start=int(v6_net.network_address),
            end=int(v6_net.broadcast_address),
            country="TC6",
            city="TestCity6",
        )
    ]

    enricher = GeoEnricher(geo_ipv4=geo_ipv4, geo_ipv6=geo_ipv6)

    # IPv4 hit
    ip4 = "10.0.0.42"
    rec4 = enricher.find_geo(ip4)
    print(f"IPv4 test ({ip4}):", rec4)

    # IPv6 hit
    ip6 = "2001:db8::1"
    rec6 = enricher.find_geo(ip6)
    print(f"IPv6 test ({ip6}):", rec6)

    # Unknown IPv4
    ip4_unknown = "192.0.2.1"
    rec4_unknown = enricher.find_geo(ip4_unknown)
    print(f"IPv4 unknown ({ip4_unknown}):", rec4_unknown)

    # Unknown IPv6
    ip6_unknown = "2001:db8:ffff::1"
    rec6_unknown = enricher.find_geo(ip6_unknown)
    print(f"IPv6 unknown ({ip6_unknown}):", rec6_unknown)


# =========================
# CLI ENTRYPOINT
# =========================


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build enriched MMDB: IP -> {domain, country, city}",
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
        "--input-mmdb",
        default="aggregator-worker/output/dns-intel.mmdb",
        help="Path to existing MMDB with IP -> domain mappings.",
    )
    parser.add_argument(
        "--ip-list",
        help=(
            "Path to a text file with one IP per line "
            "representing the IPs to process in this run. "
            "Use canonical or staged IP lists for incremental builds."
        ),
    )
    parser.add_argument(
        "--output",
        help=(
            "Optional explicit path to write the enriched output. "
            "If maxminddb-writer is installed, this will be an MMDB file; "
            "otherwise, it will be an NDJSON file. If omitted, a versioned "
            "filename (dns-geo-YYYYMMDDHHMMSS.<ext>) is generated in the "
            "current working directory, where <ext> is 'mmdb' or 'ndjson' "
            "depending on writer availability."
        ),
    )
    parser.add_argument(
        "--output-prefix",
        default="dns-geo",
        help=(
            "Prefix to use for auto-generated versioned filenames when "
            "--output is not provided. Default: dns-geo."
        ),
    )
    parser.add_argument(
        "--run-tests",
        action="store_true",
        help="Run built-in geo lookup tests and exit.",
    )
    return parser.parse_args(argv)


def _load_ip_list(path: str) -> List[str]:
    ips: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            ips.append(s)
    return ips


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    if args.run_tests:
        _run_geo_search_tests()
        return

    if not args.ip_list:
        print(
            "--ip-list is required for an actual build. "
            "Provide a file containing one IP per line, representing "
            "the canonical or staged IPs to enrich.",
            file=sys.stderr,
        )
        sys.exit(1)

    print("Loading geo IPv4 CSV...")
    geo_ipv4 = load_geo_csv(args.ipv4_csv, ip_version=4)
    print(f"Loaded {len(geo_ipv4)} IPv4 geo ranges.")

    print("Loading geo IPv6 CSV...")
    geo_ipv6 = load_geo_csv(args.ipv6_csv, ip_version=6)
    print(f"Loaded {len(geo_ipv6)} IPv6 geo ranges.")

    print("Loading IP list...")
    ip_list = _load_ip_list(args.ip_list)
    print(f"Loaded {len(ip_list)} IPs to process.")

    if args.output:
        output_path = args.output
    else:
        ts = _dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        ext = "mmdb" if _HAS_MMDB_WRITER else "ndjson"
        output_path = f"{args.output_prefix}-{ts}.{ext}"

    mode_desc = "MMDB" if _HAS_MMDB_WRITER else "NDJSON (writer library not installed)"
    print(f"Building enriched output as {mode_desc}...")

    build_enriched_output(
        existing_mmdb_path=args.input_mmdb,
        geo_ipv4=geo_ipv4,
        geo_ipv6=geo_ipv6,
        ip_source=ip_list,
        output_path=output_path,
    )
    print(f"Enriched output written to: {output_path}")


if __name__ == "__main__":
    main()
