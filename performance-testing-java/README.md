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

## Usage

### Single Run
```bash
java -jar target/api-flow-v2.jar --uid vansh -v
java -jar target/api-flow-v2.jar --random-uid -v
```

### Parallel Execution
```bash
java -jar target/api-flow-v2.jar --parallel 10 --random-uid -v
```

### Wave-Based Load Test (True RPS)
Launches N threads per second for M seconds. Waves launch at precise 1-second intervals regardless of previous wave completion:
```bash
java -jar target/api-flow-v2.jar --rps 5 --duration 10 --title "Test" --output results/ --random-uid --html -v
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
├── ApiFlowV2.java       # Main entry point + CLI
├── ApiConfig.java       # Configuration
├── ApiFlow.java         # HTTP flow (4 steps)
├── WaveExecutor.java    # True RPS wave execution
├── CsvResultWriter.java # CSV output
└── HtmlReportWriter.java # HTML dashboard
```
