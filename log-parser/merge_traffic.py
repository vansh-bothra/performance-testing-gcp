#!/usr/bin/env python3
"""
Traffic Log Merger: Merges and sorts multiple traffic JSONL files by timestamp.

Combines logs from different sources (e.g., pplmag + pmm) into a single
chronologically sorted file. Preserves mixed schemas - each line keeps
its original format.

Usage:
    python3 merge_traffic.py file1.jsonl file2.jsonl -o composite.jsonl
    python3 merge_traffic.py --t1  # Merge t1 files from default location
    python3 merge_traffic.py --t2  # Merge t2 files from default location
"""

import argparse
import json
import sys
from pathlib import Path


DEFAULT_DIR = Path(__file__).parent.parent / "performance-testing-java/results/final logs"


def merge_and_sort(input_files: list[str], output_file: str) -> dict:
    """Merge multiple JSONL files and sort by timestamp."""
    stats = {
        "input_files": len(input_files),
        "total_records": 0,
        "records_per_file": {},
    }
    
    all_records = []
    
    for filepath in input_files:
        path = Path(filepath)
        if not path.exists():
            print(f"Warning: File not found: {path}", file=sys.stderr)
            continue
        
        print(f"Loading: {path.name}...", file=sys.stderr)
        file_records = 0
        
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    all_records.append(record)
                    file_records += 1
                except json.JSONDecodeError:
                    continue
        
        stats["records_per_file"][path.name] = file_records
        print(f"  Loaded {file_records:,} records", file=sys.stderr)
    
    stats["total_records"] = len(all_records)
    print(f"\nSorting {len(all_records):,} records by timestamp...", file=sys.stderr)
    
    # Sort by timestamp (ts field)
    all_records.sort(key=lambda r: r.get('ts', 0))
    
    # Recompute delayMs based on sorted order
    print("Recomputing delays...", file=sys.stderr)
    for i in range(len(all_records)):
        if i < len(all_records) - 1:
            delay = all_records[i + 1].get('ts', 0) - all_records[i].get('ts', 0)
            all_records[i]['delayMs'] = max(0, delay)
        else:
            all_records[i]['delayMs'] = 0
    
    # Write output
    print(f"Writing to: {output_file}...", file=sys.stderr)
    with open(output_file, 'w') as f:
        for record in all_records:
            f.write(json.dumps(record) + "\n")
    
    return stats


def main():
    parser = argparse.ArgumentParser(description='Merge and sort traffic JSONL files')
    parser.add_argument('files', nargs='*', help='Input JSONL files to merge')
    parser.add_argument('-o', '--output', help='Output file path')
    parser.add_argument('--t1', action='store_true', help='Merge t1 files from default location')
    parser.add_argument('--t2', action='store_true', help='Merge t2 files from default location')
    
    args = parser.parse_args()
    
    # Handle preset modes
    if args.t1:
        input_files = [
            str(DEFAULT_DIR / "traffic_pmm_t1.jsonl"),
            str(DEFAULT_DIR / "traffic_pplmag_t1.jsonl"),
        ]
        output_file = str(DEFAULT_DIR / "traffic_composite_t1.jsonl")
    elif args.t2:
        input_files = [
            str(DEFAULT_DIR / "traffic_pmm_t2.jsonl"),
            str(DEFAULT_DIR / "traffic_pplmag_t2.jsonl"),
        ]
        output_file = str(DEFAULT_DIR / "traffic_composite_t2.jsonl")
    else:
        if not args.files:
            parser.error("Either provide input files or use --t1/--t2")
        input_files = args.files
        output_file = args.output
        if not output_file:
            parser.error("Output file (-o) required when using custom input files")
    
    print("=" * 60, file=sys.stderr)
    print("TRAFFIC LOG MERGER", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Input files: {len(input_files)}", file=sys.stderr)
    for f in input_files:
        print(f"  - {f}", file=sys.stderr)
    print(f"Output: {output_file}", file=sys.stderr)
    print("-" * 60, file=sys.stderr)
    
    stats = merge_and_sort(input_files, output_file)
    
    print("\n" + "=" * 60, file=sys.stderr)
    print("MERGE COMPLETE", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Total records: {stats['total_records']:,}", file=sys.stderr)
    print(f"Output file: {output_file}", file=sys.stderr)
    
    # Verify output
    output_size = Path(output_file).stat().st_size / (1024 * 1024)
    print(f"Output size: {output_size:.1f} MB", file=sys.stderr)


if __name__ == "__main__":
    main()
