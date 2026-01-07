#!/usr/bin/env python3
"""
Log Traffic Extractor: Extracts HTTP request events from server logs into JSONL format.

Parses production logs and extracts completed HTTP requests suitable for traffic
pattern replay. Filters to only include endpoints from the ApiFlow:
- /date-picker
- /postPickerStatus  
- /crossword (all game pages normalized to this)
- /api/v1/plays (postScore)

Output includes delayMs (time until the NEXT request should be sent).

Usage:
    python3 log_traffic_extractor.py server.log > traffic.jsonl
"""

import sys
import re
import json
import base64
from datetime import datetime
from typing import Optional
import argparse

# Log pattern: "21 Dec 00:00:08.220 LoggingFilter INFO  [PlaysServlet:/api/v1/plays] - COMPLETED ..."
LOG_PATTERN = re.compile(
    r'^(\d{1,2}\s+\w{3}\s+\d{2}:\d{2}:\d{2}\.\d{3})\s+'  # timestamp
    r'(\w+)\s+'                                          # service (LoggingFilter, etc.)
    r'INFO\s+'                                           # level
    r'\[([^:]+):([^\]]+)\]\s+-\s+'                       # [Servlet:endpoint]
    r'COMPLETED\s+'                                      # COMPLETED marker
    r'Request type:\s*(\w+)'                             # HTTP method
)

# Patterns to extract userId from the log message
PAYLOAD_JSON_PATTERN = re.compile(r'payloadJson:\s*(\{[^}]+(?:\{[^}]*\}[^}]*)*\})')
POST_SCORE_JSON_PATTERN = re.compile(r'postScoreJson:\s*(\{[^}]+(?:\{[^}]*\}[^}]*)*\})')
PICKER_STATUS_JSON_PATTERN = re.compile(r'pickerStatusJson:\s*(\{[^}]+(?:\{[^}]*\}[^}]*)*\})')
USER_ID_PATTERN = re.compile(r'"userId":\s*"([^"]+)"')  # userId in JSON
UID_PATTERN = re.compile(r'\buid:\s*(\S+)')  # uid in query params (for GET requests)
LOAD_TOKEN_PATTERN = re.compile(r'"loadToken":\s*"([^"]+)"')  # loadToken in JSON

# Endpoints we care about (from ApiFlow.java)
ALLOWED_ENDPOINTS = {
    '/date-picker',
    '/postPickerStatus',
    '/api/v1/plays',
    # Game pages - all will be normalized to /crossword
    '/crossword',
    '/sudoku',
    '/wordf',
    '/wordrow',
    '/wordsearch',
    '/quiz',
    '/codeword',
}

# Game pages to normalize to /crossword
GAME_ENDPOINTS = {'/crossword', '/sudoku', '/wordf', '/wordrow', '/wordsearch', '/quiz', '/codeword'}

CURRENT_YEAR = 2025


def parse_log_timestamp(ts_str: str) -> int:
    """Convert log timestamp to epoch milliseconds."""
    try:
        dt = datetime.strptime(f"{CURRENT_YEAR} {ts_str}", "%Y %d %b %H:%M:%S.%f")
        return int(dt.timestamp() * 1000)
    except ValueError:
        return 0


def safe_json_extract(json_str: str) -> Optional[dict]:
    """Try to parse JSON, fixing common truncation issues."""
    if not json_str:
        return None
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        fixed = json_str
        open_braces = fixed.count('{') - fixed.count('}')
        fixed += '}' * open_braces
        try:
            return json.loads(fixed)
        except json.JSONDecodeError:
            return None


def decode_load_token_uid(load_token: str) -> Optional[str]:
    """Decode loadToken (JWT-like) to extract uid.
    
    loadToken is base64 encoded with structure:
    {header}{payload}{signature}
    
    The payload contains 'uid' field.
    """
    try:
        # Add padding if needed
        padded = load_token + '=' * (4 - len(load_token) % 4)
        decoded = base64.b64decode(padded).decode('utf-8', errors='ignore')
        
        # The decoded string has format: {header}{payload}+signature
        # Find the second JSON object (payload) which contains uid
        # Look for "uid":"value"
        uid_match = re.search(r'"uid":\s*"([^"]+)"', decoded)
        if uid_match:
            return uid_match.group(1)
    except Exception:
        pass
    return None


def extract_load_token(line: str) -> Optional[str]:
    """Extract loadToken from log message."""
    match = LOAD_TOKEN_PATTERN.search(line)
    if match:
        return match.group(1)
    return None


def extract_user_id(line: str) -> Optional[str]:
    """Extract userId from log message."""
    # Try JSON payloads first
    for pattern in [PAYLOAD_JSON_PATTERN, POST_SCORE_JSON_PATTERN, PICKER_STATUS_JSON_PATTERN]:
        match = pattern.search(line)
        if match:
            payload = safe_json_extract(match.group(1))
            if payload and 'userId' in payload:
                return payload['userId']
    
    # Try userId in JSON (regex fallback for truncated JSON)
    user_match = USER_ID_PATTERN.search(line)
    if user_match:
        return user_match.group(1)
    
    # Try uid in query params (for GET requests like /date-picker)
    uid_match = UID_PATTERN.search(line)
    if uid_match:
        return uid_match.group(1)
    
    return None


def process_line(line: str) -> Optional[dict]:
    """Parse a log line and return structured data if it's a relevant HTTP request."""
    match = LOG_PATTERN.match(line.strip())
    if not match:
        return None
    
    ts_str, service, servlet, endpoint, method = match.groups()
    
    if service != 'LoggingFilter':
        return None
    
    if servlet == 'ErrorServlet':
        return None
    
    if endpoint not in ALLOWED_ENDPOINTS:
        return None
    
    ts = parse_log_timestamp(ts_str)
    if ts == 0:
        return None
    
    # Normalize game endpoints to /crossword
    if endpoint in GAME_ENDPOINTS:
        endpoint = '/crossword'
    
    # For postPickerStatus, extract userId from loadToken
    # For other endpoints, extract userId directly
    if endpoint == '/postPickerStatus':
        load_token = extract_load_token(line)
        if load_token:
            userId = decode_load_token_uid(load_token)
        else:
            userId = None
        # Skip postPickerStatus without userId
        if userId is None:
            return None
    else:
        userId = extract_user_id(line)
        if userId is None:
            return None  # Skip requests without userId for endpoints that need it
    
    return {
        'ts': ts,
        'endpoint': endpoint,
        'userId': userId,
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Extract HTTP requests from server logs')
    parser.add_argument('logfile', nargs='?', default='-', help='Log file to process (- for stdin)')
    parser.add_argument('--output', '-o', help='Output file path (default: stdout). Use "auto" for auto-generated name with timestamp')
    parser.add_argument('--stats', '-s', action='store_true', help='Print stats to stderr at the end')
    args = parser.parse_args()
    
    if args.logfile != '-':
        input_stream = open(args.logfile, 'r', encoding='utf-8', errors='replace')
    else:
        input_stream = sys.stdin
    
    # First pass: collect all events
    events = []
    lines_read = 0
    PROGRESS_INTERVAL = 500_000
    
    for line in input_stream:
        lines_read += 1
        
        if lines_read % PROGRESS_INTERVAL == 0:
            print(f"[Progress] Read {lines_read:,} lines, collected {len(events):,} events...", file=sys.stderr)
        
        result = process_line(line)
        if result:
            events.append(result)
    
    if input_stream != sys.stdin:
        input_stream.close()
    
    print(f"[Info] Collected {len(events):,} events, computing delays...", file=sys.stderr)
    
    # Determine output destination
    if args.output == 'auto':
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        output_path = f"results/traffic_{timestamp}.jsonl"
        output_stream = open(output_path, 'w')
        print(f"[Info] Writing to: {output_path}", file=sys.stderr)
    elif args.output:
        output_path = args.output
        output_stream = open(output_path, 'w')
        print(f"[Info] Writing to: {output_path}", file=sys.stderr)
    else:
        output_stream = sys.stdout
        output_path = None
    
    # Mark last request for each user (iterate backwards, first occurrence = last request)
    print(f"[Info] Marking last requests per user...", file=sys.stderr)
    seen_users = set()
    is_last_req = [0] * len(events)
    
    for i in range(len(events) - 1, -1, -1):
        user_id = events[i]['userId']
        if user_id is not None and user_id not in seen_users:
            is_last_req[i] = 1
            seen_users.add(user_id)
    
    print(f"[Info] Found {len(seen_users):,} unique users", file=sys.stderr)
    
    # Third pass: compute delayMs and output
    endpoint_counts = {}
    
    for i, event in enumerate(events):
        # Calculate delay to next request
        if i < len(events) - 1:
            delay_ms = events[i + 1]['ts'] - event['ts']
            if delay_ms < 0:
                delay_ms = 0
        else:
            delay_ms = 0  # Last event has no delay
        
        output = {
            'ts': event['ts'],
            'endpoint': event['endpoint'],
            'userId': event['userId'],
            'delayMs': delay_ms,
            'isLastReq': is_last_req[i],
        }
        
        # Track stats
        ep = event['endpoint']
        endpoint_counts[ep] = endpoint_counts.get(ep, 0) + 1
        
        print(json.dumps(output), file=output_stream)
    
    if output_stream != sys.stdout:
        output_stream.close()
    
    # Print summary
    print(f"\n[Done] Read {lines_read:,} lines, extracted {len(events):,} HTTP requests", file=sys.stderr)
    if output_path:
        print(f"[Saved] {output_path}", file=sys.stderr)
    
    if args.stats and endpoint_counts:
        print("\n[Endpoint Distribution]", file=sys.stderr)
        for ep, cnt in sorted(endpoint_counts.items(), key=lambda x: -x[1]):
            pct = 100.0 * cnt / len(events) if len(events) > 0 else 0
            print(f"  {ep}: {cnt:,} ({pct:.1f}%)", file=sys.stderr)


if __name__ == '__main__':
    main()

