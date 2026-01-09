#!/usr/bin/env python3
"""
Server Log Parser for pplmag GCP logs.

Extracts request timing, endpoints, payloads from server-side application logs.

Usage:
    python parse_logs.py <log_file> [--output <output.jsonl>]
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path


# Regex patterns for different log lines
LOGGING_FILTER_PATTERN = re.compile(
    r'(\d{2} \w{3} \d{2}:\d{2}:\d{2}\.\d{3}) '
    r'LoggingFilter INFO\s+\[(\w+):(/[^\]]+)\] - COMPLETED '
    r'Request type: (\w+) '
    r'.*?elapsedTimeMs: (\d+) '
    r'IP: ([\d.]+) '
    r'UA: ([^\s]+)'
)

PLAYS_POST_PATTERN = re.compile(
    r'(\d{2} \w{3} \d{2}:\d{2}:\d{2}\.\d{3}) '
    r'PlaysPost INFO\s+\[PlaysServlet:/api/v1/plays\] - '
    r'Play patch json received.*?payloadJson: (\{.*?\})\s*$'
)

# Pattern to extract key-value pairs from log lines
KV_PATTERN = re.compile(r'(\w+): ([^\s]+)')


def parse_timestamp(ts_str: str, year: int = 2025) -> int:
    """Parse log timestamp to epoch milliseconds."""
    # Format: "19 Dec 00:00:00.458"
    try:
        dt = datetime.strptime(f"{year} {ts_str}", "%Y %d %b %H:%M:%S.%f")
        return int(dt.timestamp() * 1000)
    except ValueError:
        return 0


def parse_logging_filter_line(line: str, year: int = 2025) -> dict | None:
    """Parse a LoggingFilter COMPLETED line."""
    match = LOGGING_FILTER_PATTERN.search(line)
    if not match:
        return None
    
    ts_str, servlet, endpoint, method, elapsed_ms, ip, ua = match.groups()
    
    # Extract additional fields
    result = {
        "ts": parse_timestamp(ts_str, year),
        "servlet": servlet,
        "endpoint": endpoint,
        "method": method,
        "elapsedMs": int(elapsed_ms),
        "ip": ip,
        "ua": ua,
    }
    
    # Extract optional fields using key-value pattern
    if "userId:" in line:
        user_match = re.search(r'userId: (\S+)', line)
        if user_match:
            result["userId"] = user_match.group(1)
    
    if "puzzleIds:" in line:
        puzzle_match = re.search(r'puzzleIds: (\S+)', line)
        if puzzle_match:
            result["puzzleId"] = puzzle_match.group(1)
    
    if "series:" in line:
        series_match = re.search(r'series: (\S+)', line)
        if series_match:
            result["series"] = series_match.group(1)
    
    if "payloadJson:" in line:
        payload_match = re.search(r'payloadJson: (\{.*?\})\s+_sec', line)
        if payload_match:
            try:
                result["payload"] = json.loads(payload_match.group(1))
            except json.JSONDecodeError:
                pass
    
    return result


def parse_plays_post_line(line: str, year: int = 2025) -> dict | None:
    """Parse a PlaysPost line with full payload."""
    match = PLAYS_POST_PATTERN.search(line)
    if not match:
        return None
    
    ts_str, payload_str = match.groups()
    
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        return None
    
    return {
        "ts": parse_timestamp(ts_str, year),
        "type": "plays_post",
        "payload": payload,
        "userId": payload.get("userId", ""),
        "puzzleId": payload.get("id", ""),
        "series": payload.get("series", ""),
    }


def parse_log_file(log_path: str, output_path: str = None, year: int = 2025) -> dict:
    """Parse a log file and extract request data."""
    stats = {
        "total_lines": 0,
        "parsed_requests": 0,
        "plays_posts": 0,
        "endpoints": {},
        "errors": 0,
    }
    
    output_file = open(output_path, 'w') if output_path else None
    
    try:
        with open(log_path, 'r', errors='ignore') as f:
            for line in f:
                stats["total_lines"] += 1
                
                if stats["total_lines"] % 1_000_000 == 0:
                    print(f"Processed {stats['total_lines']:,} lines...", file=sys.stderr)
                
                # Try LoggingFilter pattern first (most common)
                if "LoggingFilter INFO" in line and "COMPLETED" in line:
                    record = parse_logging_filter_line(line, year)
                    if record:
                        stats["parsed_requests"] += 1
                        endpoint = record.get("endpoint", "unknown")
                        stats["endpoints"][endpoint] = stats["endpoints"].get(endpoint, 0) + 1
                        
                        if output_file:
                            output_file.write(json.dumps(record) + "\n")
                        continue
                
                # Try PlaysPost pattern (for payloads)
                if "PlaysPost INFO" in line and "payloadJson:" in line:
                    record = parse_plays_post_line(line, year)
                    if record:
                        stats["plays_posts"] += 1
                        
                        if output_file:
                            output_file.write(json.dumps(record) + "\n")
    
    finally:
        if output_file:
            output_file.close()
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Parse server logs to JSONL")
    parser.add_argument("log_file", help="Path to log file")
    parser.add_argument("--output", "-o", help="Output JSONL file path")
    parser.add_argument("--year", type=int, default=2025, help="Year for timestamp parsing")
    parser.add_argument("--stats-only", action="store_true", help="Only print stats, no output file")
    
    args = parser.parse_args()
    
    log_path = Path(args.log_file)
    if not log_path.exists():
        print(f"Error: File not found: {log_path}", file=sys.stderr)
        sys.exit(1)
    
    output_path = args.output
    if not output_path and not args.stats_only:
        output_path = str(log_path.with_suffix('.jsonl'))
    
    print(f"Parsing: {log_path}", file=sys.stderr)
    print(f"Output: {output_path or '(stats only)'}", file=sys.stderr)
    print("-" * 60, file=sys.stderr)
    
    stats = parse_log_file(str(log_path), output_path if not args.stats_only else None, args.year)
    
    print("\n" + "=" * 60, file=sys.stderr)
    print("PARSING COMPLETE", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Total lines:       {stats['total_lines']:,}", file=sys.stderr)
    print(f"Parsed requests:   {stats['parsed_requests']:,}", file=sys.stderr)
    print(f"PlaysPost records: {stats['plays_posts']:,}", file=sys.stderr)
    print(f"\nEndpoints:", file=sys.stderr)
    for endpoint, count in sorted(stats["endpoints"].items(), key=lambda x: -x[1]):
        print(f"  {endpoint}: {count:,}", file=sys.stderr)


if __name__ == "__main__":
    main()
