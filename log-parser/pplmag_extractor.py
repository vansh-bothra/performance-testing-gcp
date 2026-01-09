#!/usr/bin/env python3
"""
pplmag Log Extractor: Extracts HTTP request events from pplmag server logs into JSONL format.

Parses production logs and extracts completed HTTP requests suitable for traffic
pattern replay. Includes both GET and POST requests for load testing.

Endpoints included:
- /api/v1/plays (GET and POST)
- /api/v1/puzzles (GET)
- /postPickerStatus (POST)
- /crossword, /jigsaw, etc. (normalized to /crossword)

Output format:
{"ts": 1766102400, "endpoint": "/api/v1/plays", "method": "POST", "userId": "...", "delayMs": 50, "isLastReq": 0}

Usage:
    python3 pplmag_extractor.py <log_file> -o traffic.jsonl
    python3 pplmag_extractor.py <log_file> --stats-only
"""

import sys
import re
import json
import base64
from datetime import datetime
from typing import Optional
import argparse


# Log pattern for LoggingFilter COMPLETED lines
LOG_PATTERN = re.compile(
    r'^(\d{1,2}\s+\w{3}\s+\d{2}:\d{2}:\d{2}\.\d{3})\s+'  # timestamp
    r'LoggingFilter\s+INFO\s+'                           # service
    r'\[([^:]+):([^\]]+)\]\s+-\s+'                       # [Servlet:endpoint]
    r'COMPLETED\s+'                                      # COMPLETED marker
    r'Request type:\s*(\w+)'                             # HTTP method
)

# Patterns to extract userId
USER_ID_JSON_PATTERN = re.compile(r'"userId":\s*"([^"]+)"')
UID_PARAM_PATTERN = re.compile(r'\buid:\s*(\S+)')
USER_ID_PARAM_PATTERN = re.compile(r'\buserId:\s*(\S+)')

# Pattern to extract loadToken for decoding
LOAD_TOKEN_PATTERN = re.compile(r'"loadToken":\s*"([^"]+)"')

# Endpoints to include
ALLOWED_ENDPOINTS = {
    '/api/v1/plays',
    '/api/v1/puzzles',
    '/postPickerStatus',
    # Game pages - all normalized to /crossword
    '/crossword',
    '/jigsaw',
    '/sudoku',
    '/wordf',
    '/wordrow',
    '/wordsearch',
    '/quiz',
    '/codeword',
}

# Game pages to normalize
GAME_ENDPOINTS = {'/crossword', '/jigsaw', '/sudoku', '/wordf', '/wordrow', '/wordsearch', '/quiz', '/codeword'}

CURRENT_YEAR = 2025


def parse_log_timestamp(ts_str: str) -> int:
    """Convert log timestamp to epoch milliseconds."""
    try:
        dt = datetime.strptime(f"{CURRENT_YEAR} {ts_str}", "%Y %d %b %H:%M:%S.%f")
        return int(dt.timestamp() * 1000)
    except ValueError:
        return 0


def decode_load_token_uid(load_token: str) -> Optional[str]:
    """Decode loadToken JWT to extract uid from payload."""
    try:
        # JWT format: header.payload.signature (base64 encoded)
        parts = load_token.split('.')
        if len(parts) >= 2:
            # Decode payload (second part)
            payload_b64 = parts[1]
            # Add padding if needed
            padding = 4 - len(payload_b64) % 4
            if padding != 4:
                payload_b64 += '=' * padding
            decoded = base64.urlsafe_b64decode(payload_b64).decode('utf-8', errors='ignore')
            payload = json.loads(decoded)
            return payload.get('uid')
    except Exception:
        pass
    return None


def extract_user_id(line: str, method: str) -> Optional[str]:
    """Extract userId from log line."""
    # For POST requests, try JSON payload first
    if method == 'POST':
        # Try userId in JSON
        match = USER_ID_JSON_PATTERN.search(line)
        if match:
            return match.group(1)
        
        # Try decoding from loadToken
        token_match = LOAD_TOKEN_PATTERN.search(line)
        if token_match:
            uid = decode_load_token_uid(token_match.group(1))
            if uid:
                return uid
    
    # For GET requests, try query params
    # Try userId: param
    match = USER_ID_PARAM_PATTERN.search(line)
    if match:
        uid = match.group(1)
        # Clean up trailing punctuation
        uid = uid.rstrip(',')
        return uid
    
    # Try uid: param
    match = UID_PARAM_PATTERN.search(line)
    if match:
        uid = match.group(1)
        uid = uid.rstrip(',')
        return uid
    
    return None


def process_line(line: str) -> Optional[dict]:
    """Parse a log line and return structured data if it's a relevant HTTP request."""
    match = LOG_PATTERN.match(line.strip())
    if not match:
        return None
    
    ts_str, servlet, endpoint, method = match.groups()
    
    # Skip irrelevant endpoints
    if endpoint not in ALLOWED_ENDPOINTS:
        return None
    
    ts = parse_log_timestamp(ts_str)
    if ts == 0:
        return None
    
    # Normalize game endpoints to /crossword
    if endpoint in GAME_ENDPOINTS:
        endpoint = '/crossword'
    
    # Extract userId
    userId = extract_user_id(line, method)
    
    # For /api/v1/plays and /postPickerStatus, userId is required
    if endpoint in {'/api/v1/plays', '/postPickerStatus'} and userId is None:
        return None
    
    return {
        'ts': ts,
        'endpoint': endpoint,
        'method': method,
        'userId': userId,
    }


def main():
    parser = argparse.ArgumentParser(description='Extract HTTP requests from pplmag server logs')
    parser.add_argument('logfile', help='Log file to process')
    parser.add_argument('--output', '-o', help='Output JSONL file path')
    parser.add_argument('--stats-only', action='store_true', help='Only print stats, no output file')
    parser.add_argument('--year', type=int, default=2025, help='Year for timestamp parsing')
    
    args = parser.parse_args()
    
    global CURRENT_YEAR
    CURRENT_YEAR = args.year
    
    print(f"Parsing: {args.logfile}", file=sys.stderr)
    print("-" * 60, file=sys.stderr)
    
    # First pass: collect all events
    events = []
    lines_read = 0
    PROGRESS_INTERVAL = 500_000
    
    with open(args.logfile, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            lines_read += 1
            
            if lines_read % PROGRESS_INTERVAL == 0:
                print(f"[Progress] Read {lines_read:,} lines, collected {len(events):,} events...", file=sys.stderr)
            
            result = process_line(line)
            if result:
                events.append(result)
    
    print(f"\n[Info] Collected {len(events):,} events, computing delays...", file=sys.stderr)
    
    # Sort by timestamp (logs may not be perfectly ordered)
    events.sort(key=lambda x: x['ts'])
    
    # Mark last request for each user
    print(f"[Info] Marking last requests per user...", file=sys.stderr)
    seen_users = set()
    is_last_req = [0] * len(events)
    
    for i in range(len(events) - 1, -1, -1):
        user_id = events[i].get('userId')
        if user_id and user_id not in seen_users:
            is_last_req[i] = 1
            seen_users.add(user_id)
    
    print(f"[Info] Found {len(seen_users):,} unique users", file=sys.stderr)
    
    # Determine output path
    if args.stats_only:
        output_stream = None
        output_path = None
    elif args.output:
        output_path = args.output
        output_stream = open(output_path, 'w')
    else:
        output_path = args.logfile.replace('.log', '_traffic.jsonl')
        output_stream = open(output_path, 'w')
    
    # Output with delay computation
    endpoint_counts = {}
    method_counts = {}
    
    for i, event in enumerate(events):
        # Calculate delay to next request
        if i < len(events) - 1:
            delay_ms = events[i + 1]['ts'] - event['ts']
            if delay_ms < 0:
                delay_ms = 0
        else:
            delay_ms = 0
        
        output = {
            'ts': event['ts'],
            'endpoint': event['endpoint'],
            'method': event['method'],
            'userId': event['userId'],
            'delayMs': delay_ms,
            'isLastReq': is_last_req[i],
        }
        
        # Track stats
        ep = event['endpoint']
        endpoint_counts[ep] = endpoint_counts.get(ep, 0) + 1
        
        key = f"{event['method']} {ep}"
        method_counts[key] = method_counts.get(key, 0) + 1
        
        if output_stream:
            print(json.dumps(output), file=output_stream)
    
    if output_stream:
        output_stream.close()
    
    # Print summary
    print(f"\n{'='*60}", file=sys.stderr)
    print("EXTRACTION COMPLETE", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)
    print(f"Total lines:      {lines_read:,}", file=sys.stderr)
    print(f"Extracted events: {len(events):,}", file=sys.stderr)
    print(f"Unique users:     {len(seen_users):,}", file=sys.stderr)
    
    if output_path:
        print(f"Output file:      {output_path}", file=sys.stderr)
    
    print(f"\n[Endpoint Distribution]", file=sys.stderr)
    for ep, cnt in sorted(endpoint_counts.items(), key=lambda x: -x[1]):
        pct = 100.0 * cnt / len(events) if len(events) > 0 else 0
        print(f"  {ep}: {cnt:,} ({pct:.1f}%)", file=sys.stderr)
    
    print(f"\n[Method Distribution]", file=sys.stderr)
    for key, cnt in sorted(method_counts.items(), key=lambda x: -x[1]):
        pct = 100.0 * cnt / len(events) if len(events) > 0 else 0
        print(f"  {key}: {cnt:,} ({pct:.1f}%)", file=sys.stderr)


if __name__ == '__main__':
    main()
