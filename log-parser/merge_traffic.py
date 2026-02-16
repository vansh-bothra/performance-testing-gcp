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


def merge_and_sort(input_files: list[str], output_file: str, source_map: dict[str, str] = None) -> dict:
    """Merge multiple JSONL files and sort by timestamp.
    
    Args:
        input_files: List of JSONL file paths to merge
        output_file: Output file path
        source_map: Optional dict mapping file paths to source tags (e.g., {"file.jsonl": "pmm"})
    """
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
        
        # Determine source tag for this file
        source_tag = None
        if source_map:
            source_tag = source_map.get(str(path)) or source_map.get(filepath)
        
        print(f"Loading: {path.name}...", file=sys.stderr)
        if source_tag:
            print(f"  Source tag: {source_tag}", file=sys.stderr)
        file_records = 0
        
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    # Inject source tag if provided
                    if source_tag:
                        record['source'] = source_tag
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
    parser = argparse.ArgumentParser(
        description='Merge and sort traffic JSONL files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Merge with explicit source tags:
  python3 merge_traffic.py --pmm-source traffic_pmm.jsonl --pplmag-source traffic_pplmag.jsonl -o composite.jsonl
  
  # Merge with preset (t1/t2) - sources auto-assigned:
  python3 merge_traffic.py --t1
  python3 merge_traffic.py --t2
"""
    )
    parser.add_argument('files', nargs='*', help='Input JSONL files to merge (no source tag)')
    parser.add_argument('-o', '--output', help='Output file path')
    parser.add_argument('--t1', action='store_true', help='Merge t1 files from default location')
    parser.add_argument('--t2', action='store_true', help='Merge t2 files from default location')
    parser.add_argument('--pmm-source', metavar='FILE', action='append', default=[],
                        help='Input file with source="pmm" tag (can be repeated)')
    parser.add_argument('--pplmag-source', metavar='FILE', action='append', default=[],
                        help='Input file with source="pplmag" tag (can be repeated)')
    
    args = parser.parse_args()
    
    # Build source map from explicit flags
    source_map = {}
    for f in args.pmm_source:
        source_map[f] = "pmm"
    for f in args.pplmag_source:
        source_map[f] = "pplmag"
    
    # Handle preset modes
    if args.t1:
        pmm_file = str(DEFAULT_DIR / "traffic_pmm_t1.jsonl")
        pplmag_file = str(DEFAULT_DIR / "traffic_pplmag_t1.jsonl")
        input_files = [pmm_file, pplmag_file]
        source_map = {pmm_file: "pmm", pplmag_file: "pplmag"}
        output_file = str(DEFAULT_DIR / "traffic_composite_t1.jsonl")
    elif args.t2:
        pmm_file = str(DEFAULT_DIR / "traffic_pmm_t2.jsonl")
        pplmag_file = str(DEFAULT_DIR / "traffic_pplmag_t2.jsonl")
        input_files = [pmm_file, pplmag_file]
        source_map = {pmm_file: "pmm", pplmag_file: "pplmag"}
        output_file = str(DEFAULT_DIR / "traffic_composite_t2.jsonl")
    elif args.pmm_source or args.pplmag_source:
        # Using explicit source flags
        input_files = args.pmm_source + args.pplmag_source + args.files
        if not input_files:
            parser.error("No input files specified")
        output_file = args.output
        if not output_file:
            parser.error("Output file (-o) required")
    else:
        if not args.files:
            parser.error("Either provide input files or use --t1/--t2 or --pmm-source/--pplmag-source")
        input_files = args.files
        output_file = args.output
        if not output_file:
            parser.error("Output file (-o) required when using custom input files")
    
    print("=" * 60, file=sys.stderr)
    print("TRAFFIC LOG MERGER", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Input files: {len(input_files)}", file=sys.stderr)
    for f in input_files:
        tag = source_map.get(f, "(no tag)")
        print(f"  - {f} [{tag}]", file=sys.stderr)
    print(f"Output: {output_file}", file=sys.stderr)
    print("-" * 60, file=sys.stderr)
    
    stats = merge_and_sort(input_files, output_file, source_map)
    
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

