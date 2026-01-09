#!/usr/bin/env python3
"""
Log Distiller: Parses server logs into JSONL format for replay.

Usage:
    # Process first 10k lines for testing:
    head -n 10000 server.log | python log_distiller.py > traffic.jsonl

    # Process full file:
    python log_distiller.py server.log > traffic.jsonl
"""

import sys
import re
import json
from datetime import datetime

# Regex to extract timestamp and payloadJson from log lines
# Format: "21 Dec 00:00:00.134 PlaysPost INFO ... payloadJson: {...}"
LOG_PATTERN = re.compile(
    r'^(\d{1,2}\s+\w{3}\s+\d{2}:\d{2}:\d{2}\.\d{3})\s+\w+\s+INFO\s+\[([^\]]+)\].*?payloadJson:\s*(\{.*\})'
)

# Current year for timestamp parsing (logs don't include year)
CURRENT_YEAR = 2025


def parse_log_timestamp(ts_str: str) -> int:
    """Convert log timestamp to epoch milliseconds."""
    # Parse "21 Dec 00:00:00.134" format
    try:
        dt = datetime.strptime(f"{CURRENT_YEAR} {ts_str}", "%Y %d %b %H:%M:%S.%f")
        return int(dt.timestamp() * 1000)
    except ValueError:
        return 0


def extract_endpoint(context: str) -> str:
    """Extract endpoint from context like 'PlaysServlet:/api/v1/plays'."""
    if ':' in context:
        return context.split(':', 1)[1]
    return context


def process_line(line: str) -> dict | None:
    """Parse a log line and return structured data, or None if not relevant."""
    match = LOG_PATTERN.search(line)
    if not match:
        return None
    
    ts_str, context, payload_str = match.groups()
    
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        return None
    
    # Extract key fields
    user_id = payload.get('userId', '')
    if not user_id:
        return None
    
    return {
        'ts': parse_log_timestamp(ts_str),
        'endpoint': extract_endpoint(context),
        'user': user_id,
        'series': payload.get('series', ''),
        'id': payload.get('id', ''),
        'payload': payload
    }


def main():
    """Main entry point."""
    # Read from file or stdin
    if len(sys.argv) > 1 and sys.argv[1] != '-':
        input_stream = open(sys.argv[1], 'r', encoding='utf-8', errors='replace')
    else:
        input_stream = sys.stdin
    
    count = 0
    lines_read = 0
    PROGRESS_INTERVAL = 100_000  # Log every 100k lines
    
    for line in input_stream:
        lines_read += 1
        
        # Progress logging
        if lines_read % PROGRESS_INTERVAL == 0:
            print(f"[Progress] Read {lines_read:,} lines, extracted {count:,} events...", file=sys.stderr)
        
        result = process_line(line)
        if result:
            print(json.dumps(result))
            count += 1
    
    if input_stream != sys.stdin:
        input_stream.close()
    
    print(f"[Done] Read {lines_read:,} lines, extracted {count:,} events", file=sys.stderr)


if __name__ == '__main__':
    main()
