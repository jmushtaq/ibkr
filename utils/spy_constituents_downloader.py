#!/usr/bin/env python3
"""
S&P 500 Constituent Ticker Downloader
Retrieves historical S&P 500 constituent tickers by year and saves to CSV files.
Supports single year or year range (e.g., '2001' or '2001-2026')

To run:
	python utils/spy_constituents_downloader.py --years 2001-2026 --output-dir ./data
"""

import os
import csv
import argparse
import logging
import sys
from datetime import datetime, date
from typing import List, Dict, Set, Tuple, Optional
import requests
import pandas as pd
from io import StringIO
import re

class SP500ConstituentDownloader:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.setup_logging()

        # Base GitHub repository
        self.github_base = "https://raw.githubusercontent.com/fja05680/sp500/main/"

        # Try to find the most recent historical file
        self.historical_file = self.find_latest_historical_file()

        # Fallback sources if GitHub fails
        self.fallback_sources = [
            # DataHub.io S&P 500 current constituents (with historical changes tracking)
            "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv",
            # Alternative GitHub repository with different formatting
            "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
        ]

    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def find_latest_historical_file(self) -> Optional[str]:
        """
        Try to find the latest historical file in the GitHub repository.
        Since we can't list directory contents via raw URL, we'll try common patterns.
        """
        # Try to get the repository's README to find the latest file name
        try:
            readme_url = "https://raw.githubusercontent.com/fja05680/sp500/main/README.md"
            response = requests.get(readme_url, timeout=10)
            if response.status_code == 200:
                # Look for historical file pattern in README
                pattern = r'S&P 500 Historical Components & Changes\(\d{2}-\d{2}-\d{4}\)\.csv'
                matches = re.findall(pattern, response.text)
                if matches:
                    # Use the first match found
                    return matches[0]
        except Exception as e:
            self.logger.warning(f"Could not read README to find latest file: {str(e)}")

        # Fallback to known recent files in order of likelihood
        candidates = [
            "S&P 500 Historical Components & Changes(01-17-2026).csv",
            "S&P 500 Historical Components & Changes(01-17-2025).csv",
            "S&P 500 Historical Components & Changes(12-31-2025).csv",
            "S&P 500 Historical Components & Changes(12-31-2024).csv",
            "S&P 500 Historical Components & Changes(05-05-2021).csv",
            "S&P 500 Historical Components & Changes.csv"  # Original static file
        ]

        # Test each candidate
        for candidate in candidates:
            try:
                test_url = self.github_base + candidate.replace(" ", "%20").replace("&", "%26")
                response = requests.head(test_url, timeout=5)
                if response.status_code == 200:
                    self.logger.info(f"Found historical file: {candidate}")
                    return candidate
            except:
                continue

        return None

    def download_historical_data(self) -> Optional[pd.DataFrame]:
        """
        Download historical S&P 500 constituent data from various sources.
        The dataset contains daily snapshots of index composition since 1996.
        """
        # Try GitHub primary source first
        if self.historical_file:
            try:
                url = self.github_base + self.historical_file.replace(" ", "%20").replace("&", "%26")
                self.logger.info(f"Downloading from: {url}")
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                # Read CSV data
                df = pd.read_csv(StringIO(response.text))
                self.logger.info(f"Successfully downloaded data with {len(df)} rows from GitHub")

                # Check if the format matches expected pattern
                if len(df.columns) >= 1:
                    # First column should be date
                    return df

            except Exception as e:
                self.logger.warning(f"GitHub download failed: {str(e)}")

        # Try alternative GitHub repository with different format
        try:
            self.logger.info("Trying alternative GitHub repository...")
            alt_url = "https://raw.githubusercontent.com/jirisli/sp500/master/S%26P%20500%20Historical%20Components%20%26%20Changes.csv"
            response = requests.get(alt_url, timeout=30)
            response.raise_for_status()

            df = pd.read_csv(StringIO(response.text))
            self.logger.info(f"Successfully downloaded data with {len(df)} rows from alternative GitHub")
            return df

        except Exception as e:
            self.logger.warning(f"Alternative GitHub download failed: {str(e)}")

        # Try to construct from current constituents + historical changes
        try:
            self.logger.info("Attempting to construct historical data from current constituents...")
            return self.construct_historical_data()
        except Exception as e:
            self.logger.warning(f"Historical construction failed: {str(e)}")

        self.logger.error("All data sources failed")
        return None

    def construct_historical_data(self) -> Optional[pd.DataFrame]:
        """
        Construct a simple historical dataset from current constituents.
        This is a fallback method that assumes constituents haven't changed much
        for recent years. For accurate historical data, users should rely on
        the GitHub source.
        """
        # Get current constituents from DataHub
        try:
            response = requests.get("https://datahub.io/core/s-and-p-500-companies/r/constituents.csv", timeout=30)
            response.raise_for_status()

            df_current = pd.read_csv(StringIO(response.text))
            current_symbols = sorted(df_current['Symbol'].tolist())

            # Create a simple historical dataset (last 5 years)
            # Note: This is a simplification - for accurate backtesting,
            # users should ensure the GitHub source works or use paid data
            dates = []
            for year in range(datetime.now().year - 5, datetime.now().year + 1):
                dates.append(f"{year}-07-01")  # Mid-year snapshot

            # Create DataFrame with same symbols for all dates (simplified)
            data = {'date': dates}
            for i, symbol in enumerate(current_symbols[:10]):  # Limit for example
                data[symbol] = [1] * len(dates)

            df = pd.DataFrame(data)
            self.logger.warning("Using simplified historical data - for accurate backtesting, ensure GitHub source works")
            return df

        except Exception as e:
            self.logger.error(f"Failed to construct historical data: {str(e)}")
            return None

    def parse_year_range(self, year_spec: str) -> List[int]:
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

        # Validate years
        current_year = datetime.now().year
        for year in years:
            if year < 1996:
                self.logger.warning(f"Year {year} is before 1996. Data may be incomplete [citation:1]")
            if year > current_year:
                self.logger.warning(f"Year {year} is in the future. Using current data.")

        return years

    def get_constituents_for_date(self, df: pd.DataFrame, target_date: date) -> List[str]:
        """
        Extract S&P 500 constituents for a specific date.
        Handles different possible DataFrame formats.
        """
        target_date_str = target_date.strftime("%Y-%m-%d")

        # Case 1: Format with date column and ticker columns (1/0)
        if len(df.columns) > 2 and df.iloc[:, 1:].dtypes.iloc[0] in ['int64', 'float64']:
            # Find the date column
            date_col = df.columns[0]

            # Convert dates in dataframe
            try:
                df_dates = pd.to_datetime(df[date_col])
            except:
                try:
                    df_dates = pd.to_datetime(df[date_col], format="%m/%d/%Y")
                except:
                    df_dates = pd.to_datetime(df[date_col], infer_datetime_format=True)

            # Find row with date <= target_date
            mask = df_dates <= pd.Timestamp(target_date)
            if not mask.any():
                available_row = df.iloc[0]
                actual_date = df_dates.iloc[0]
            else:
                available_row = df.loc[mask].iloc[-1]
                actual_date = df_dates.loc[mask].iloc[-1]

            self.logger.info(f"Using data from {actual_date.strftime('%Y-%m-%d')} for {target_date}")

            # Extract tickers with value 1
            tickers = []
            for col in df.columns[1:]:
                if available_row[col] == 1:
                    tickers.append(col)

            return sorted(tickers)

        # Case 2: Format with 'date' and 'tickers' columns (comma-separated list)
        elif 'date' in df.columns and 'tickers' in df.columns:
            df_dates = pd.to_datetime(df['date'])
            mask = df_dates <= pd.Timestamp(target_date)

            if not mask.any():
                row = df.iloc[0]
            else:
                row = df.loc[mask].iloc[-1]

            tickers_str = row['tickers']
            if isinstance(tickers_str, str):
                tickers = [t.strip() for t in tickers_str.split(',')]
            elif isinstance(tickers_str, list):
                tickers = tickers_str
            else:
                tickers = []

            return sorted(tickers)

        # Case 3: Index is date, column is tickers
        elif isinstance(df.index, pd.DatetimeIndex) or 'date' in df.index.names:
            df_dates = pd.to_datetime(df.index)
            mask = df_dates <= pd.Timestamp(target_date)

            if not mask.any():
                row = df.iloc[0]
            else:
                row = df.loc[mask].iloc[-1]

            if 'tickers' in row:
                tickers_str = row['tickers']
                if isinstance(tickers_str, str):
                    tickers = [t.strip() for t in tickers_str.split(',')]
                else:
                    tickers = tickers_str
                return sorted(tickers)

        self.logger.error(f"Unrecognized DataFrame format. Columns: {df.columns.tolist()}")
        return []

    def save_tickers_to_csv(self, year: int, tickers: List[str]):
        """
        Save ticker list to CSV file in the specified directory structure:
        {output_dir}/spy_tickers/{year}/tickers.csv
        """
        # Create directory structure
        save_dir = os.path.join(self.output_dir, "spy_tickers", str(year))
        os.makedirs(save_dir, exist_ok=True)

        # Save to CSV
        filename = os.path.join(save_dir, "tickers.csv")

        try:
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['ticker'])  # Header
                for ticker in tickers:
                    writer.writerow([ticker])

            self.logger.info(f"Saved {len(tickers)} tickers to {filename}")

        except Exception as e:
            self.logger.error(f"Error saving tickers for {year}: {str(e)}")
            raise

    def download_constituents(self, year_spec: str):
        """
        Main method to download S&P 500 constituents for specified year(s).
        """
        try:
            # Parse years
            years = self.parse_year_range(year_spec)
            self.logger.info(f"Processing years: {years}")

            # Download historical data
            self.logger.info("Downloading historical S&P 500 constituent data...")
            df = self.download_historical_data()

            if df is None:
                self.logger.error("Failed to download historical data. Please check your internet connection and try again.")
                self.logger.info("\nAlternative sources for S&P 500 historical constituents:")
                self.logger.info("1. Visit https://github.com/fja05680/sp500 and download manually")
                self.logger.info("2. Use Bloomberg Terminal: type 'MEMB <GO>' and adjust dates [citation:3]")
                self.logger.info("3. Use S&P Net Advantage (university library access) [citation:5]")
                self.logger.info("4. For academic use, check your university's Bloomberg or Refinitiv access [citation:8]")
                return

            for year in years:
                self.logger.info(f"\n--- Processing {year} ---")

                # Use mid-year (July 1) as reference date for the year's composition
                # This avoids issues with year-end changes
                target_date = date(year, 7, 1)

                # Get constituents for this date
                tickers = self.get_constituents_for_date(df, target_date)

                if tickers:
                    # Save to CSV
                    self.save_tickers_to_csv(year, tickers)

                    # Print sample and stats
                    self.logger.info(f"Sample tickers for {year}: {sorted(tickers)[:10]}...")
                    self.logger.info(f"Total constituents: {len(tickers)}")

                    # Note about count [citation:1]
                    if len(tickers) < 490 and year >= 2001:
                        self.logger.warning(f"Count ({len(tickers)}) is lower than expected. Data may be incomplete.")
                else:
                    self.logger.error(f"Could not retrieve constituents for {year}")

            self.logger.info("\n" + "="*50)
            self.logger.info("DOWNLOAD SUMMARY")
            self.logger.info("="*50)
            self.logger.info(f"Years processed: {len(years)}")
            self.logger.info(f"Output directory: {os.path.join(self.output_dir, 'spy_tickers')}")

            # Add notes about data quality [citation:1]
            self.logger.info("\n" + "="*50)
            self.logger.info("DATA QUALITY NOTES")
            self.logger.info("="*50)
            self.logger.info("• Data from 1996-2000 may have ~487-494 symbols (not full 500) [citation:1]")
            self.logger.info("• From 2001 onwards, typically 494-507 symbols")
            self.logger.info("• For precise backtesting, consider using Norgate Data or eoddata [citation:1]")
            self.logger.info("• Source: GitHub repository based on 'Trading Evolved' by Andreas Clenow [citation:1]")

        except Exception as e:
            self.logger.error(f"Error in download_constituents: {str(e)}")
            raise

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Download historical S&P 500 constituent tickers by year'
    )

    parser.add_argument(
        '--years',
        type=str,
        required=True,
        help='Year(s) to download. Single year (e.g., "2001") or range (e.g., "2001-2026")'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='./data',
        help='Base directory for output files (default: ./data)'
    )

    return parser.parse_args()

def main():
    """Main execution function"""
    args = parse_arguments()

    print(f"S&P 500 Constituent Downloader")
    print(f"Years: {args.years}")
    print(f"Output directory: {args.output_dir}")
    print(f"Data source: GitHub repository (based on 'Trading Evolved' by Andreas Clenow) [citation:1]")
    print()

    # Create downloader instance
    downloader = SP500ConstituentDownloader(output_dir=args.output_dir)

    # Download constituents
    downloader.download_constituents(args.years)

if __name__ == "__main__":
    main()
