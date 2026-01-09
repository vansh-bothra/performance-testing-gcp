#!/usr/bin/env python3
"""
Full Log Distiller: Extracts ALL events from server logs into JSONL format.

This captures every log line with structured fields for analysis.

Usage:
    python3 log_distiller_full.py server.log > full_traffic.jsonl
"""

import sys
import re
import json
from datetime import datetime

# Generic log pattern: "21 Dec 00:00:00.134 ServiceName LEVEL [Context] - Message"
LOG_PATTERN = re.compile(
    r'^(\d{1,2}\s+\w{3}\s+\d{2}:\d{2}:\d{2}\.\d{3})\s+(\w+)\s+(\w+)\s+\[([^\]]*)\]\s+-\s+(.*)$'
)

# Pattern to extract payloadJson if present
PAYLOAD_PATTERN = re.compile(r'payloadJson:\s*(\{.*\})')

# Pattern to extract key-value pairs like "userId: xxx series: yyy"
KV_PATTERN = re.compile(r'(\w+):\s*([^\s]+)')

CURRENT_YEAR = 2025


def parse_log_timestamp(ts_str: str) -> int:
    """Convert log timestamp to epoch milliseconds."""
    try:
        dt = datetime.strptime(f"{CURRENT_YEAR} {ts_str}", "%Y %d %b %H:%M:%S.%f")
        return int(dt.timestamp() * 1000)
    except ValueError:
        return 0


def extract_endpoint(context: str) -> tuple[str, str]:
    """Extract servlet and endpoint from context like 'PlaysServlet:/api/v1/plays'."""
    if ':' in context:
        parts = context.split(':', 1)
        return parts[0], parts[1]
    return context, ''


def extract_payload_json(message: str) -> dict | None:
    """Try to extract payloadJson from message."""
    match = PAYLOAD_PATTERN.search(message)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            return None
    return None


def extract_key_values(message: str) -> dict:
    """Extract key-value pairs from log message."""
    # Common keys we care about
    keys_of_interest = {
        'userId', 'uid', 'series', 'id', 'playId', 'sessionId',
        'DBtimeTaken', 'elapsedTimeMs', 'status', 'dbName', 'IP'
    }
    result = {}
    for match in KV_PATTERN.finditer(message):
        key, value = match.groups()
        if key in keys_of_interest:
            result[key] = value
    return result


def process_line(line: str) -> dict | None:
    """Parse a log line and return structured data."""
    match = LOG_PATTERN.match(line.strip())
    if not match:
        return None
    
    ts_str, service, level, context, message = match.groups()
    servlet, endpoint = extract_endpoint(context)
    
    result = {
        'ts': parse_log_timestamp(ts_str),
        'service': service,
        'level': level,
        'servlet': servlet,
        'endpoint': endpoint,
        'message_preview': message[:200] if len(message) > 200 else message,
    }
    
    # Extract structured data
    kv_data = extract_key_values(message)
    if kv_data:
        result['fields'] = kv_data
    
    # Extract payloadJson if present
    payload = extract_payload_json(message)
    if payload:
        result['payload'] = payload
        result['user'] = payload.get('userId', '')
    
    return result


def main():
    """Main entry point."""
    if len(sys.argv) > 1 and sys.argv[1] != '-':
        input_stream = open(sys.argv[1], 'r', encoding='utf-8', errors='replace')
    else:
        input_stream = sys.stdin
    
    count = 0
    lines_read = 0
    PROGRESS_INTERVAL = 100_000
    
    for line in input_stream:
        lines_read += 1
        
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
