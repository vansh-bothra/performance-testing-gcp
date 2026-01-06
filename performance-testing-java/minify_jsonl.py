import json
import sys

def minify_line(line):
    try:
        data = json.loads(line)
        
        # Keep minimal top-level fields
        min_data = {
            "ts": data.get("ts"),
            "user": data.get("user"),
            "endpoint": data.get("endpoint")
        }
        
        # Process payload
        if "payload" in data:
            p = data["payload"]
            min_payload = {}
            
            # Fields to KEEP explicitly requested or essential for game state
            keep_keys = {
                "userId", "timestamp", "updatedTimestamp", # Requested to keep
                "playState", "primaryState", "secondaryState", # Core game state
                "score", "timeTaken", "timeOnPage", # Metrics
                "nPrints", "nPrintsEmpty", "nPrintsFilled", "nPrintsSol", # Stats
                "nClearClicks", "nSettingsClicks", "nHelpClicks", "nResizes", "nExceptions",
                "postScoreReason", "streakLength"
            }
            
            for k, v in p.items():
                if k in keep_keys:
                    min_payload[k] = v
                    
            # Ensure userId is present if it was in source (mapped from top level if needed)
            if "userId" not in min_payload and "user" in min_data:
                min_payload["userId"] = min_data["user"]
                
            min_data["payload"] = min_payload
            
        return json.dumps(min_data)
    except Exception as e:
        return None

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 minify_jsonl.py input.jsonl output.jsonl")
        sys.exit(1)
        
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    count = 0
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            minified = minify_line(line)
            if minified:
                outfile.write(minified + '\n')
                count += 1
                if count % 100000 == 0:
                    print(f"Processed {count} lines...", end='\r')
                    
    print(f"\nFinished processing {count} lines.")

if __name__ == "__main__":
    main()
