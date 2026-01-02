# Performance Testing

Load testing tool for the Amuse Labs crossword API. Simulates user flows with wave-based execution.

## Files

| File | Description |
|------|-------------|
| `api_flow.py` | Main test script (Python) - runs API flow simulations |
| `performance-testing-java/` | Java project for high-performance load testing and replay |
| `view_results.py` | Generates HTML dashboard from CSV results |
| `requirements.txt` | Python dependencies |

## Setup

```bash
pip install -r requirements.txt
```

## Preparing Replay Logs

To run the replay, you first need to convert your server logs (containing `payloadJson`) into the JSONL format expected by the tool.

Use the `log_distiller.py` script included in the Java project:

```bash
# Process a server log file
python3 performance-testing-java/src/main/java/com/perftest/log_distiller.py server.log > traffic.jsonl

# Process from stdin (e.g., from unzipping)
cat server.log | python3 performance-testing-java/src/main/java/com/perftest/log_distiller.py > traffic.jsonl
```

The resulting `traffic.jsonl` file is what you pass to the `--replay` flag.

## Java Version (High Performance)

The Java version offers higher performance and concurrency than the Python version.

### Build
```bash
cd performance-testing-java
mvn clean package
```

### Usage
Run the jar directly from the target directory:
```bash
# Wave/Load Test
java -jar target/api-flow-v2.jar --rps 100 --duration 60 --random-uid --html --title "Java Load Test"

# Streaming Replay (from logs)
java -jar target/api-flow-v2.jar --replay replay_logs/traffic.jsonl --speed 2.0 --base-url https://api.yoursite.com/ --html
```

### CLI Options (Java)
| Flag | Description |
|------|-------------|
| `--rps N` | Requests per second |
| `--duration N` | Test duration in seconds |
| `--replay <file>` | Path to JSONL traffic log for replay |
| `--speed N` | Speed factor for replay (default 1.0) |
| `--base-url <url>` | Target base URL for replay |
| `--dry-run` | Simulate replay without network calls |
| `--html` | Generate HTML dashboard |
| `-v` | Verbose output |

## Python Version Usage

### Single Run
```bash
python api_flow.py --uid vansh
python api_flow.py --random-uid
```

### Parallel Execution
```bash
python api_flow.py --parallel 10 --random-uid
```

### Wave-Based Load Test
Starts N threads per second for M seconds:
```bash
python api_flow.py --rps 3 --duration 5 --title "Baseline Test" --output results/ --random-uid --html
```

## CLI Options (Python)

| Flag | Description |
|------|-------------|
| `--uid <name>` | User ID for the test |
| `--random-uid` | Generate random UID per run |
| `--uid-pool-size N` | Pick from pool of N random UIDs |
| `--parallel N` | Run N threads in parallel |
| `--rps N` | Requests per second (wave mode) |
| `--duration N` | Seconds to run (wave mode) |
| `--title "..."` | Title for the run (in filename) |
| `--output <path>` | Save CSV results to path |
| `--html` | Generate HTML dashboard after CSV |
