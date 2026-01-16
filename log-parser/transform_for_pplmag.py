import json
import sys

DEFAULT_SERIES = "pplmag-site-puzzler"
DEFAULT_PUZZLE_ID = "ce996e5f"

# Supported (Method, Endpoint) tuples for PplmagReplayExecutor
SUPPORTED = {
    ("GET", "/api/v1/puzzles"),
    ("GET", "/api/v1/plays"),
    ("POST", "/api/v1/plays"),
    ("GET", "/crossword"),
    ("POST", "/postPickerStatus"),
}

def transform(input_path, output_path):
    print(f"Transforming {input_path} -> {output_path}")
    count_total = 0
    count_kept = 0
    count_modified = 0

    with open(input_path, 'r', encoding='utf-8') as f_in, \
         open(output_path, 'w', encoding='utf-8') as f_out:
        
        for line in f_in:
            line = line.strip()
            if not line:
                continue
            
            count_total += 1
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            endpoint = event.get("endpoint", "")
            method = event.get("method", "GET")

            # FIX 1: GET /postPickerStatus -> POST
            # The log apparently has GET for this, but the executor needs POST
            if endpoint == "/postPickerStatus" and method == "GET":
                event["method"] = "POST"
                method = "POST"
                count_modified += 1

            # FIX 2: Filter unsupported endpoints
            # e.g. /date-picker is not in SUPPORTED
            if (method, endpoint) not in SUPPORTED:
                continue

            # FIX 3: Default missing series/puzzleId
            if not event.get("series"):
                event["series"] = DEFAULT_SERIES
                # We can consider this a modification, but it's just filling defaults
            
            if not event.get("puzzleId"):
                event["puzzleId"] = DEFAULT_PUZZLE_ID

            f_out.write(json.dumps(event) + "\n")
            count_kept += 1

    print(f"Done. Processed {count_total} events.")
    print(f"Kept {count_kept} events (filtered {count_total - count_kept}).")
    print(f"Modified method for {count_modified} events.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 transform_for_pplmag.py <input_jsonl> <output_jsonl>")
        sys.exit(1)
    
    transform(sys.argv[1], sys.argv[2])
