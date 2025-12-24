# Performance Testing

Load testing tool for the Amuse Labs crossword API. Simulates user flows with wave-based execution.

## Files

| File | Description |
|------|-------------|
| `api_flow.py` | Main test script - runs API flow simulations |
| `view_results.py` | Generates HTML dashboard from CSV results |
| `requirements.txt` | Python dependencies |

## Setup

```bash
pip install -r requirements.txt
```

## Usage

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
python api_flow.py --rps 30 --duration 5 --title "Baseline Test" --output results/
```

### View Results
Generate HTML dashboard from CSV:
```bash
python view_results.py results/2024-12-24_Baseline_Test.csv --open
```

## CLI Options

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
