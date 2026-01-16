# Performance Testing (Java)

Java port of the crossword API load testing tool. Equivalent functionality to the Python version with wave-based execution.

## Requirements

- **Java 17** (recommended via SDKMAN)
- **Maven 3.6+**

## Setup

### Using SDKMAN (Recommended)
```bash
# Install Java 17
sdk install java 17.0.9-tem

# Enable auto-switching (one-time)
sdk config  # Set sdkman_auto_env=true

# The .sdkmanrc file will auto-switch to Java 17 when entering this directory
```

### Build
```bash
mvn package -q
```

This creates two JARs:
- `target/api-flow-v2.jar` - Standard flow
- `target/api-flow-v3.jar` - Flow with CDN resource fetches

## Usage

### V2 - Standard Flow

```bash
# Single run
java -jar target/api-flow-v2.jar --uid vansh -v

# Wave-based load test
java -jar target/api-flow-v2.jar --rps 5 --duration 10 --random-uid --output results/ --html -v

# Regenerate HTML from existing CSV
java -cp target/api-flow-v2.jar com.perftest.HtmlReportGenerator results/your-file.csv
```

### V3 - With CDN Fetches

V3 adds 4 CDN resource fetches to Step 1 (after date-picker):
- `date-picker-min.css`
- `picker-min.js`
- `font-awesome/all.min.css`
- `font-awesome/fa-solid-900.woff2`

```bash
# Single run
java -jar target/api-flow-v3.jar --uid vansh -v

# Wave-based load test  
java -jar target/api-flow-v3.jar --rps 5 --duration 10 --random-uid --output results/ --html -v
```

### Replay Mode (Streaming)

Replay traffic from JSONL logs at varying speeds.

```bash
# Basic replay
java -jar target/api-flow-v2.jar --replay traffic.jsonl --speed 2.0 --html -v

# Replay minified logs (auto-generate random payloads)
java -jar target/api-flow-v2.jar --replay traffic_minified.jsonl --randpayl --dry-run
```

### TrafficReplayExecutor (Standalone)

Replay production traffic with parallel pre-warming and detailed analytics.

**Features:**
- Parallel pre-warm (100 threads) for faster session setup
- CSV output with per-request timing
- HTML report generation
- Session caching for faster reruns

```bash
# Compile and run
mvn compile
mvn exec:java -Dexec.mainClass="com.perftest.TrafficReplayExecutor" \
  -Dexec.args="'path/to/traffic.jsonl' --speed 5 --html -v"

# With session caching
mvn exec:java -Dexec.mainClass="com.perftest.TrafficReplayExecutor" \
  -Dexec.args="'path/to/traffic.jsonl' --speed 5 --save-sessions --html -v"

# Reuse cached sessions (faster startup)
mvn exec:java -Dexec.mainClass="com.perftest.TrafficReplayExecutor" \
  -Dexec.args="'path/to/traffic.jsonl' --speed 10 --load-sessions --html -v"

# Dry run (no actual requests)
mvn exec:java -Dexec.mainClass="com.perftest.TrafficReplayExecutor" \
  -Dexec.args="'path/to/traffic.jsonl' --speed 1 --dry-run -v"
```

**CLI Options:**
- `--speed <factor>` - Replay speed multiplier (e.g., `5` = 5x faster)
- `--dry-run` - Simulate without sending requests
- `--html` - Generate HTML dashboard
- `--save-sessions` - Cache session tokens to file
- `--load-sessions` - Load cached session tokens
- `-v` - Verbose output

## CLI Options

| Flag                | Description                       |
| ------------------- | --------------------------------- |
| `--replay <file>`   | Replay traffic from JSONL log     |
| `--speed <factor>`  | Replay speed (e.g. 2.0 = 2x)      |
| `--randpayl`        | Random payloads (for minified logs)|
| `--uid <name>`      | User ID for the test              |
| `--random-uid`      | Generate random UID per run       |
| `--uid-pool-size N` | Pick from pool of N random UIDs   |
| `--parallel N`      | Run N threads in parallel         |
| `--rps N`           | Requests per second (wave mode)   |
| `--duration N`      | Seconds to run (wave mode)        |
| `--title "..."`     | Title for the run (in filename)   |
| `--output <path>`   | Save CSV results to path          |
| `--html`            | Generate HTML dashboard after CSV |
| `--dry-run`         | Simulate without sending requests |
| `--save-sessions`   | Cache session tokens to file      |
| `--load-sessions`   | Load cached session tokens        |
| `-v, --verbose`     | Show step-by-step progress        |

## Output

- **CSV**: Detailed results with step timestamps (`step1_start`, `step1_end`, etc.)
- **HTML**: Interactive dashboard with latency charts (requires `--html` flag)

## Additional Executors

### PplmagReplayExecutor

Replay traffic for the pplmag site with OAuth2 authentication.

**Requirements:**
- `auth_config.json` in the working directory with `client_id` and `client_secret`

```bash
# Create auth config
echo '{"client_id": "your_id", "client_secret": "your_secret"}' > auth_config.json

# Run replay
mvn exec:java -Dexec.mainClass="com.perftest.PplmagReplayExecutor" \
  -Dexec.args="'path/to/pplmag_traffic.jsonl' --speed 5 -v"
```

**Features:**
- OAuth2 token management (auto-refresh)
- Supports `/api/v1/puzzles`, `/api/v1/plays`, `/crossword`
- Session pre-warming
- HTML report generation

### StreamingReplayExecutor

Memory-efficient replay for very large log files.

```bash
mvn exec:java -Dexec.mainClass="com.perftest.StreamingReplayExecutor" \
  -Dexec.args="'large_traffic.jsonl' --speed 2 --html"
```

**Features:**
- Streaming event processing (low memory footprint)
- Progress reporting every 1000 events
- Aggregated statistics for HTML reports

## Log Parsers & Tools

The `log-parser/` directory contains Python scripts for preparing traffic logs:

### pplmag_extractor.py

Extract traffic events from pplmag server logs.

```bash
python3 ../log-parser/pplmag_extractor.py server.log -o traffic.jsonl
```

**Extracts:**
- `/api/v1/plays` (GET and POST)
- `/api/v1/puzzles` (GET)
- `/crossword` and other game pages
- `/postPickerStatus` (POST)

**Output format:**
```json
{"ts": 1736102400, "endpoint": "/api/v1/plays", "method": "POST", "userId": "...", "series": "pplmag-site-puzzler", "puzzleId": "b71c3aae", "offset": null, "delayMs": 50}
```

### transform_for_pplmag.py

Transform generic traffic logs to PplmagReplayExecutor format.

```bash
python3 ../log-parser/transform_for_pplmag.py \
  input_traffic.jsonl \
  output_pplmag_ready.jsonl
```

**Transformations:**
- Filters unsupported endpoints
- Fixes method mismatches (e.g., `GET /postPickerStatus` → `POST`)
- Adds default `series` and `puzzleId` if missing

### log_distiller.py

Basic traffic extractor for TrafficReplayExecutor.

```bash
python3 ../log-parser/log_distiller.py server.log > traffic.jsonl
```

### merge_traffic.py

Merge multiple traffic JSONL files by timestamp.

```bash
python3 ../log-parser/merge_traffic.py file1.jsonl file2.jsonl -o merged.jsonl
```

## Project Structure

```
src/main/java/com/perftest/
├── ApiFlowV2.java              # V2 Main entry point
├── ApiFlowV3Main.java          # V3 Main entry point
├── ApiConfig.java              # Configuration
├── ApiFlow.java                # V2 HTTP flow (4 steps)
├── ApiFlowV3.java              # V3 HTTP flow (4 steps + CDN)
├── WaveExecutor.java           # True RPS wave execution (supports V2/V3)
├── TrafficReplayExecutor.java  # Production traffic replay with pre-warming
├── PplmagReplayExecutor.java   # Pplmag site replay with OAuth2
├── StreamingReplayExecutor.java# Memory-efficient streaming replay
├── ReplayExecutor.java         # Generic replay executor
├── SessionManager.java         # Session token management
├── TrafficAnalyzer.java        # Traffic pattern analysis
├── CsvResultWriter.java        # CSV output
├── HtmlReportWriter.java       # HTML dashboard
├── HtmlReportGenerator.java    # Standalone HTML generator from CSV
└── ReplayReportWriter.java     # HTML reports for replay mode
```

## Compilation & Packaging

```bash
# Compile only
mvn compile

# Package JARs
mvn package -q

# Clean build
mvn clean package
```

