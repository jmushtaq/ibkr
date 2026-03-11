#!/usr/bin/env python3
"""
S&P 500 Combined Ticker List Generator
Reads tickers.csv files for a range of years, combines them,
and outputs a unique sorted list to a single CSV file.

# Combine all tickers from 2001 to 2026
python combine_sp500_tickers.py --years 2001-2026

# Combine with custom input/output directories
python combine_tickers.py --years 2001-2026 --input-dir ./market_data --output-dir ./processed_data

# Single year
python combine_tickers.py --years 2023

# Show verbose output (full ticker list)
python combine_tickers.py --years 2001-2026 --verbose

# Custom output filename
python combine_tickers.py --years 2001-2026 --output-file my_tickers/combined_list.csv

"""

import os
import csv
import argparse
import sys
from typing import List, Set
from datetime import datetime

def parse_year_range(year_spec: str) -> List[int]:
    """
    Parse year specification like '2001' or '2001-2026' into list of years.
    """
    years = []

    if '-' in year_spec:
        # Range like 2001-2026
        start_year, end_year = map(int, year_spec.split('-'))
        if start_year > end_year:
            raise ValueError(f"Invalid year range: {year_spec}. Start year must be <= end year.")
        years = list(range(start_year, end_year + 1))
    else:
        # Single year like 2001
        years = [int(year_spec)]

    return years

def load_tickers_from_file(filepath: str, year: int) -> List[str]:
    """
    Load tickers from a tickers.csv file.
    Expected format: CSV with 'ticker' column or single column of tickers
    """
    tickers = []

    if not os.path.exists(filepath):
        print(f"Warning: File not found - {filepath}")
        return tickers

    try:
        with open(filepath, 'r') as f:
            first_line = f.readline().strip()
            f.seek(0)

            # Check if file has header
            if first_line.lower() == 'ticker' or first_line.lower().startswith('ticker,'):
                reader = csv.DictReader(f)
                for row in reader:
                    if 'ticker' in row:
                        ticker = row['ticker'].strip()
                        if ticker:
                            tickers.append(ticker)
            else:
                # No header, assume one ticker per line
                reader = csv.reader(f)
                for row in reader:
                    if row and row[0].strip():
                        tickers.append(row[0].strip())

        print(f"✓ Loaded {len(tickers)} tickers from {os.path.basename(filepath)}")
        return tickers

    except Exception as e:
        print(f"Error loading tickers from {filepath}: {str(e)}")
        return []

def combine_tickers(years: List[int], base_dir: str) -> Set[str]:
    """
    Load tickers from all years and return a unique set.
    """
    all_tickers = set()
    years_found = []
    years_missing = []

    for year in years:
        filepath = os.path.join(base_dir, "spy_tickers", str(year), "tickers.csv")
        tickers = load_tickers_from_file(filepath, year)

        if tickers:
            all_tickers.update(tickers)
            years_found.append(year)
        else:
            years_missing.append(year)

    if years_missing:
        print(f"\nNote: No ticker files found for years: {years_missing}")

    if years_found:
        print(f"\nLoaded tickers from years: {years_found}")

    return all_tickers

def save_combined_tickers(tickers: Set[str], output_file: str):
    """
    Save unique sorted tickers to a CSV file.
    """
    # Sort tickers alphabetically
    sorted_tickers = sorted(tickers)

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    try:
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['ticker'])  # Header

            for ticker in sorted_tickers:
                writer.writerow([ticker])

        print(f"\n✓ Successfully saved {len(sorted_tickers)} unique tickers to:")
        print(f"  {output_file}")

        # Show sample
        print(f"\nSample of first 20 tickers:")
        for i, ticker in enumerate(sorted_tickers[:20]):
            print(f"  {ticker}", end="  ")
            if (i + 1) % 5 == 0:
                print()
        if len(sorted_tickers) > 20:
            print(f"  ... and {len(sorted_tickers) - 20} more")

        return sorted_tickers

    except Exception as e:
        print(f"Error saving combined tickers: {str(e)}")
        return []

def print_statistics(tickers: Set[str], years: List[int], years_found: List[int]):
    """
    Print statistics about the combined ticker set.
    """
    print("\n" + "="*60)
    print("COMBINED TICKER STATISTICS")
    print("="*60)
    print(f"Total unique tickers: {len(tickers)}")
    print(f"Years requested: {years[0]}-{years[-1]} ({len(years)} years)")
    print(f"Years with data: {years_found[0]}-{years_found[-1]} ({len(years_found)} years)")

    if len(years_found) < len(years):
        print(f"Years missing: {len(years) - len(years_found)}")

    # Optional: Show ticker length distribution
    length_dist = {}
    for ticker in tickers:
        length = len(ticker)
        length_dist[length] = length_dist.get(length, 0) + 1

    print(f"\nTicker length distribution:")
    for length in sorted(length_dist.keys()):
        print(f"  {length} chars: {length_dist[length]} tickers")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Combine S&P 500 tickers from multiple years into a unique sorted list'
    )

    parser.add_argument(
        '--years',
        type=str,
        required=True,
        help='Year range (e.g., "2001-2026") or single year (e.g., "2001")'
    )

    parser.add_argument(
        '--input-dir',
        type=str,
        default='./data',
        help='Base directory containing spy_tickers folders (default: ./data)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='./data',
        help='Output directory for combined file (default: ./data)'
    )

    parser.add_argument(
        '--output-file',
        type=str,
        default='spy_tickers/tickers_combined_unique.csv',
        help='Output filename relative to output-dir (default: spy_tickers/tickers_combined_unique.csv)'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed ticker information'
    )

    return parser.parse_args()

def main():
    """Main execution function"""
    args = parse_arguments()

    print("="*60)
    print("S&P 500 COMBINED TICKER GENERATOR")
    print("="*60)

    # Parse years
    try:
        years = parse_year_range(args.years)
        print(f"Years requested: {args.years} ({len(years)} years)")
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Combine tickers from all years
    print(f"\nLoading ticker files from: {os.path.join(args.input_dir, 'spy_tickers')}")
    all_tickers = combine_tickers(years, args.input_dir)

    if not all_tickers:
        print("\nError: No tickers found for any of the specified years.")
        print("Please run spy_constituents_downloader.py first to download ticker files.")
        sys.exit(1)

    # Save combined file
    output_file = os.path.join(args.output_dir, args.output_file)
    sorted_tickers = save_combined_tickers(all_tickers, output_file)

    # Print statistics
    # We need years_found - reconstruct from existing files
    years_found = []
    for year in years:
        filepath = os.path.join(args.input_dir, "spy_tickers", str(year), "tickers.csv")
        if os.path.exists(filepath):
            years_found.append(year)

    print_statistics(all_tickers, years, years_found)

    # Optional verbose output
    if args.verbose and sorted_tickers:
        print("\n" + "="*60)
        print("COMPLETE TICKER LIST (ALPHABETICAL)")
        print("="*60)
        for i, ticker in enumerate(sorted_tickers, 1):
            print(f"{i:4d}. {ticker}")

if __name__ == "__main__":
    main()

