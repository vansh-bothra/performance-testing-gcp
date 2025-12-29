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

## CLI Options

| Flag                | Description                       |
| ------------------- | --------------------------------- |
| `--uid <name>`      | User ID for the test              |
| `--random-uid`      | Generate random UID per run       |
| `--uid-pool-size N` | Pick from pool of N random UIDs   |
| `--parallel N`      | Run N threads in parallel         |
| `--rps N`           | Requests per second (wave mode)   |
| `--duration N`      | Seconds to run (wave mode)        |
| `--title "..."`     | Title for the run (in filename)   |
| `--output <path>`   | Save CSV results to path          |
| `--html`            | Generate HTML dashboard after CSV |
| `-v, --verbose`     | Show step-by-step progress        |

## Output

- **CSV**: Detailed results with step timestamps (`step1_start`, `step1_end`, etc.)
- **HTML**: Interactive dashboard with latency charts (requires `--html` flag)

## Project Structure

```
src/main/java/com/perftest/
├── ApiFlowV2.java        # V2 Main entry point
├── ApiFlowV3Main.java    # V3 Main entry point
├── ApiConfig.java        # Configuration
├── ApiFlow.java          # V2 HTTP flow (4 steps)
├── ApiFlowV3.java        # V3 HTTP flow (4 steps + CDN)
├── WaveExecutor.java     # True RPS wave execution (supports V2/V3)
├── CsvResultWriter.java  # CSV output
└── HtmlReportWriter.java # HTML dashboard
```
