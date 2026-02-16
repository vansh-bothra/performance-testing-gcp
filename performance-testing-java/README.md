# Performance Testing Suite (Java)

A Java-based performance testing toolkit for crossword API flow testing and traffic replay execution.

---

## Project Structure

```
src/main/java/com/perftest/
├── common/                       # Shared configuration & utilities
│   └── ApiConfig.java            # Base URL, puzzle ID, UID generation, timeouts
│
├── flow/                         # Pre-defined API flow tests
│   ├── CrosswordFlow.java        # 4-step crossword API flow (date-picker → play posts)
│   ├── CrosswordFlowWithCdn.java # Same flow + CDN resource fetches in Steps 1 & 3
│   ├── FlowRunner.java           # CLI entry point for CrosswordFlow
│   ├── FlowRunnerWithCdn.java    # CLI entry point for CrosswordFlowWithCdn
│   ├── WaveExecutor.java         # Wave-based load testing with true RPS scheduling
│   ├── CsvResultWriter.java      # CSV output for flow test results
│   ├── HtmlReportWriter.java     # Interactive HTML dashboard for flow results
│   └── HtmlReportGenerator.java  # Standalone CSV → HTML report converter
│
├── replay/                       # Traffic log replay executors
│   ├── TrafficReplayExecutor.java    # Replay PMM traffic from JSONL logs
│   ├── PplmagReplayExecutor.java     # Replay Pplmag traffic (OAuth2 auth)
│   ├── CompositeReplayExecutor.java  # Replay merged PMM + Pplmag traffic
│   ├── SessionManager.java          # Session token management for replays
│   └── ReplayReportWriter.java      # HTML Gantt-timeline report for replays
│
└── legacy/                       # Deprecated / superseded
    ├── ReplayExecutor.java
    ├── StreamingReplayExecutor.java
    └── TrafficAnalyzer.java
```

### File Descriptions

#### `common/ApiConfig.java`

Shared configuration used by all flow tests. Provides a builder pattern for constructing configs with:

- **`baseUrl`** — Target server URL (default: `https://cdn-test.amuselabs.com/pmm/`)
- **`setParam`** — Puzzle set identifier (default: `gandalf`)
- **`uid`** — Default user ID (default: `vansh`)
- **`useRandomUid`** — Whether to generate random UIDs per request
- **`uidPool`** — Pre-generated pool of UIDs to sample from
- **`timeout`** — HTTP request timeout in seconds (default: `30`)
- **`PUZZLE_ID`** — Hardcoded crossword puzzle ID: `d4725144`
- **`STATE_LEN`** — Hardcoded state string length: `185`

> [!NOTE]
> To test against a different domain or puzzle, you must modify `baseUrl`, `setParam`, `PUZZLE_ID`, and `STATE_LEN` directly in `ApiConfig.java`.

#### `flow/CrosswordFlow.java`

Executes the core 4-step crossword API flow sequentially:
1. **Step 1** — `GET /date-picker?set={set}&uid={uid}` → extracts puzzle ID
2. **Step 2** — `GET /crossword?id={puzzleId}&set={set}` → loads crossword
3. **Step 3** — `POST /crossword` with play state
4. **Step 4** — `POST /crossword` with updated play state

Uses `ApiConfig` for all URLs and parameters.

#### `flow/CrosswordFlowWithCdn.java`

Same 4-step flow as `CrosswordFlow`, but also fetches CDN resources in parallel during Steps 1 and 3. CDN resource paths are hardcoded as static arrays:

- **`STEP1_CDN_RESOURCES`** — CSS/JS assets fetched after date-picker
- **`STEP3_CDN_RESOURCES`** — CSS/JS assets fetched after crossword load

> [!NOTE]
> CDN resource paths (e.g., commit hashes, file names) are hardcoded as `private static final String[]` arrays. Update these if the CDN layout changes.

#### `flow/WaveExecutor.java`

Schedules requests at a constant rate (true RPS) using wave-based execution. Each wave fires N requests simultaneously, waits for all to complete, then repeats. Supports incremental CSV writing every 30 waves.

#### `flow/CsvResultWriter.java` / `flow/HtmlReportWriter.java` / `flow/HtmlReportGenerator.java`

Reporting utilities for flow tests:
- **CsvResultWriter** — Writes per-thread latency data to CSV files
- **HtmlReportWriter** — Generates interactive HTML dashboards with Chart.js (histograms, wave latency trends, thread timelines, error groups)
- **HtmlReportGenerator** — Standalone tool to regenerate an HTML report from an existing CSV file

#### `replay/TrafficReplayExecutor.java`

Replays PMM traffic from JSONL log files with time-accurate scheduling. Sessions are pre-warmed by replaying each user's initial date-picker → crossword → play flow.

Hardcoded constants:
- **`SET_PARAM`** = `malhar-1`
- **`PUZZLE_ID`** = `7a3720d7`
- **CDN paths** (`DATE_PICKER_CDN_PATHS`, `CROSSWORD_CDN_PATHS`, `EXTERNAL_CDN_URLS`) — static arrays of resource paths

> [!IMPORTANT]
> To replay against a different puzzle set, update `SET_PARAM` and `PUZZLE_ID` constants in `TrafficReplayExecutor.java`.

#### `replay/PplmagReplayExecutor.java`

Replays Pplmag traffic with OAuth2 authentication. Fetches bearer tokens using client credentials from `auth_config.json`.

Hardcoded constants:
- **`BASE_URL`** = `https://cdn-test.amuselabs.com/pmm/`
- **`AUTH_ENDPOINT`** = `api/v1/token`
- **`SET_PARAM`** = `gandalf`
- **`FALLBACK_PUZZLE_ID`** = `ce996e5f`

**Required file:** `auth_config.json` in the working directory:
```json
{
  "client_id": "...",
  "client_secret": "..."
}
```

#### `replay/CompositeReplayExecutor.java`

Replays merged traffic from both PMM and Pplmag sources (produced by `merge_traffic.py`). Routes events to appropriate handlers based on the `source` field in each JSONL event.

Hardcoded constants:
- **`BASE_URL`** = `https://cdn-test.amuselabs.com/pmm/`
- **`AUTH_ENDPOINT`** = `api/v1/token`
- **`SET_PARAM`** = `gandalf`
- **`FALLBACK_PUZZLE_ID`** / **`PMM_PUZZLE_ID`** = `ce996e5f`

**Required file:** `auth_config.json` (same format as above)

#### `replay/SessionManager.java`

Manages session tokens (cookies) for replay scenarios. Fetches fresh sessions by hitting the date-picker and crossword endpoints. Uses `ApiConfig` for base URL.

#### `replay/ReplayReportWriter.java`

Generates HTML reports for replay test runs, including a Gantt-style timeline showing event scheduling and execution.

---

## Prerequisites

- **Java 17+**
- **Maven 3.6+**

## Build

```bash
mvn clean package -q
```

This produces 5 executable JARs in `target/`:

| JAR                    | Entry Point               | Description                 |
| ---------------------- | ------------------------- | --------------------------- |
| `flow-runner.jar`      | `FlowRunner`              | Crossword API flow tests    |
| `flow-runner-cdn.jar`  | `FlowRunnerWithCdn`       | Crossword flow + CDN assets |
| `traffic-replay.jar`   | `TrafficReplayExecutor`   | PMM traffic replay          |
| `composite-replay.jar` | `CompositeReplayExecutor` | Merged PMM + Pplmag replay  |
| `replay-executor.jar`  | `StreamingReplayExecutor` | Legacy streaming replay     |

---

## Usage

### 1. Flow Runner / Flow Runner with CDN

Both `flow-runner.jar` and `flow-runner-cdn.jar` accept the same CLI options.

#### Single Run
```bash
java -jar target/flow-runner.jar --uid vansh -v
```

#### Parallel Threads
```bash
java -jar target/flow-runner.jar --parallel 10 --random-uid -v
```

#### Wave Mode (True RPS)
```bash
java -jar target/flow-runner.jar \
  --rps 5 --duration 60 \
  --random-uid \
  --output results/ \
  --html \
  --title "5rps-60s-test" \
  -v
```

#### CDN Variant
```bash
java -jar target/flow-runner-cdn.jar \
  --rps 5 --duration 60 \
  --random-uid \
  --output results/ \
  --html
```

#### All CLI Options

| Flag                  | Description                                         | Default        |
| --------------------- | --------------------------------------------------- | -------------- |
| `--uid <name>`        | User ID for requests                                | `vansh`        |
| `--random-uid`        | Generate a random 8-char UID per request            | `false`        |
| `--uid-pool-size <n>` | Pre-generate a pool of N random UIDs to sample from | `0` (disabled) |
| `--parallel <n>`      | Run N threads concurrently (parallel mode)          | `0` (disabled) |
| `--rps <n>`           | Requests per second — enables wave mode             | `0` (disabled) |
| `--duration <s>`      | Duration in seconds for wave mode                   | `1`            |
| `--title <text>`      | Title label for the test run (used in reports)      | –              |
| `--output <path>`     | Output path for CSV results (file or directory)     | –              |
| `--html`              | Generate HTML dashboard alongside the CSV           | `false`        |
| `-v, --verbose`       | Print step-by-step request/response details         | `false`        |
| `-h, --help`          | Show help                                           | –              |

**Execution modes** (mutually exclusive, checked in order):
1. **Wave mode** — `--rps` is set → fires `rps` requests per wave for `duration` seconds
2. **Parallel mode** — `--parallel` is set → runs N threads, each executing one full flow
3. **Single mode** — neither set → runs one flow sequentially

---

### 2. Traffic Replay Executor (PMM)

```bash
java -jar target/traffic-replay.jar <traffic.jsonl> [options]
```

#### All CLI Options

| Flag                | Description                                                | Default                               |
| ------------------- | ---------------------------------------------------------- | ------------------------------------- |
| `--base-url <url>`  | Target server base URL                                     | `https://cdn-test.amuselabs.com/pmm/` |
| `--speed <factor>`  | Speed multiplier (e.g., `5.0` = 5× faster)                 | `1.0`                                 |
| `--dry-run`         | Parse and schedule events without sending HTTP requests    | `false`                               |
| `--html`            | Generate HTML report after execution                       | `false`                               |
| `--save-sessions`   | Pre-warm users and save sessions to `sessions.json`        | `false`                               |
| `--load-sessions`   | Load sessions from `sessions.json` (skip pre-warming)      | `false`                               |
| `--no-ssl`          | Disable SSL certificate validation (for self-signed certs) | `false`                               |
| `--fetch-cdn`       | Fetch CDN resources after date-picker and crossword steps  | `false`                               |
| `--cdn-env <env>`   | CDN environment prefix                                     | `cdn-test`                            |
| `--cdn-path <path>` | CDN path prefix                                            | `pmm`                                 |
| `--cdn-commit <id>` | CDN commit ID (used in resource URLs)                      | `dd97891`                             |
| `-v, --verbose`     | Verbose output                                             | `false`                               |

---

### 3. Pplmag Replay Executor

```bash
java -jar target/traffic-replay.jar <pplmag-traffic.jsonl> [options]
```

> [!IMPORTANT]
> Requires `auth_config.json` with OAuth2 credentials (`client_id`, `client_secret`) in the working directory.

**Note:** There is no standalone JAR for PplmagReplayExecutor. Run it via classpath:
```bash
java -cp target/traffic-replay.jar com.perftest.replay.PplmagReplayExecutor <pplmag-traffic.jsonl> [options]
```

#### All CLI Options

| Flag               | Description                        | Default |
| ------------------ | ---------------------------------- | ------- |
| `--speed <factor>` | Speed multiplier                   | `1.0`   |
| `--dry-run`        | Don't send HTTP requests           | `false` |
| `--html`           | Generate HTML report (default: on) | `true`  |
| `--no-html`        | Disable HTML report generation     | –       |
| `-v, --verbose`    | Verbose output (default: on)       | `true`  |
| `-q, --quiet`      | Quiet output (only summary)        | –       |

---

### 4. Composite Replay Executor (PMM + Pplmag)

```bash
java -jar target/composite-replay.jar <merged-traffic.jsonl> [options]
```

> [!IMPORTANT]
> Requires `auth_config.json` with OAuth2 credentials in the working directory.
> The input JSONL must have a `source` field (`pmm` or `pplmag`) on each event — use `merge_traffic.py` to produce this.

#### All CLI Options

| Flag               | Description                        | Default |
| ------------------ | ---------------------------------- | ------- |
| `--speed <factor>` | Speed multiplier                   | `1.0`   |
| `--dry-run`        | Don't send HTTP requests           | `false` |
| `--html`           | Generate HTML report (default: on) | `true`  |
| `--no-html`        | Disable HTML report generation     | –       |
| `-v, --verbose`    | Verbose output (default: on)       | `true`  |
| `-q, --quiet`      | Quiet output (only summary)        | –       |

---

### 5. HTML Report Regeneration

Convert an existing CSV to an HTML dashboard (useful for regenerating reports):

```bash
java -cp target/flow-runner.jar com.perftest.flow.HtmlReportGenerator <results.csv>
java -cp target/flow-runner.jar com.perftest.flow.HtmlReportGenerator <results.csv> -o dashboard.html
```

---

## Hardcoded Constants Reference

Constants that must be updated in source code when testing a different domain, puzzle, or CDN layout:

### `common/ApiConfig.java` (used by flow tests)

| Constant    | Value                                 | Purpose                               |
| ----------- | ------------------------------------- | ------------------------------------- |
| `baseUrl`   | `https://cdn-test.amuselabs.com/pmm/` | Target API server                     |
| `setParam`  | `gandalf`                             | Puzzle set identifier                 |
| `PUZZLE_ID` | `d4725144`                            | Crossword puzzle ID                   |
| `STATE_LEN` | `185`                                 | Length of state string for play posts |
| `uid`       | `vansh`                               | Default user ID                       |
| `timeout`   | `30`                                  | HTTP timeout (seconds)                |

### `replay/TrafficReplayExecutor.java`

| Constant                | Value      | Purpose                                 |
| ----------------------- | ---------- | --------------------------------------- |
| `SET_PARAM`             | `malhar-1` | Puzzle set for pre-warming              |
| `PUZZLE_ID`             | `7a3720d7` | Puzzle ID for pre-warming               |
| `DATE_PICKER_CDN_PATHS` | (array)    | CDN resources fetched after date-picker |
| `CROSSWORD_CDN_PATHS`   | (array)    | CDN resources fetched after crossword   |
| `EXTERNAL_CDN_URLS`     | (array)    | External CDN URLs (e.g., font-awesome)  |

### `replay/PplmagReplayExecutor.java`

| Constant             | Value                                 | Purpose                            |
| -------------------- | ------------------------------------- | ---------------------------------- |
| `BASE_URL`           | `https://cdn-test.amuselabs.com/pmm/` | Target API server                  |
| `AUTH_ENDPOINT`      | `api/v1/token`                        | OAuth2 token endpoint              |
| `SET_PARAM`          | `gandalf`                             | Puzzle set identifier              |
| `FALLBACK_PUZZLE_ID` | `ce996e5f`                            | Fallback puzzle ID if not in event |

### `replay/CompositeReplayExecutor.java`

| Constant             | Value                                 | Purpose                       |
| -------------------- | ------------------------------------- | ----------------------------- |
| `BASE_URL`           | `https://cdn-test.amuselabs.com/pmm/` | Target API server             |
| `AUTH_ENDPOINT`      | `api/v1/token`                        | OAuth2 token endpoint         |
| `SET_PARAM`          | `gandalf`                             | Puzzle set identifier         |
| `FALLBACK_PUZZLE_ID` | `ce996e5f`                            | Fallback puzzle ID            |
| `PMM_PUZZLE_ID`      | `ce996e5f`                            | Puzzle ID for PMM pre-warming |

### `flow/CrosswordFlowWithCdn.java`

| Constant              | Value   | Purpose                         |
| --------------------- | ------- | ------------------------------- |
| `STEP1_CDN_RESOURCES` | (array) | CDN assets fetched after Step 1 |
| `STEP3_CDN_RESOURCES` | (array) | CDN assets fetched after Step 3 |

---

## `auth_config.json` Format

Required by `PplmagReplayExecutor` and `CompositeReplayExecutor`. Place in the working directory:

```json
{
  "client_id": "your-oauth2-client-id",
  "client_secret": "your-oauth2-client-secret"
}
```
