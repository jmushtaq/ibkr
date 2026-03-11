#!/usr/bin/env python3
"""
Historical Data Downloader for Interactive Brokers
Downloads OHLCV data for multiple symbols and saves to CSV files.
Supports chunked downloads for higher granularities.

To Run:
    python utils/historical_price_downloader.py --year 2022 --symbols "AAPL" --granularity 1min --output-dir ./data

# Original usage with direct symbols
python historical_price_downloader.py --year 2022 --symbols "AAPL,MSFT" --granularity 1D

# New usage with tickers file from spy_constituents_downloader.py
python historical_price_downloader.py --year 2022 --tickers-file ./data/spy_tickers/2022/tickers.csv --granularity 1D

# Resume interrupted download (skips already downloaded symbols)
python historical_price_downloader.py --year 2022 --tickers-file ./data/spy_tickers/2022/tickers.csv --granularity 1min --resume

# Resume with longer timeout for 1-minute data
python historical_price_downloader.py --year 2022 --tickers-file ./data/spy_tickers/2022/tickers.csv --granularity 1min --resume --timeout 3600

"""


from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import argparse
import os
import logging
import csv
from datetime import datetime, timedelta
import sys
from typing import List, Set, Tuple, Dict
import queue
from dateutil.relativedelta import relativedelta
import glob

class IBHistoricalDownloader(EWrapper, EClient):
    def __init__(self, symbols: List[str], year: int, granularity: str, output_dir: str, resume: bool = False):
        EClient.__init__(self, self)
        self.original_symbols = symbols.copy()  # Keep original list for reference
        self.symbols = symbols.copy()  # This will be modified during download
        self.year = year
        self.granularity = granularity
        self.output_dir = output_dir
        self.resume = resume
        self.data_queue = queue.Queue()
        self.current_symbol = None
        self.completed_symbols = []
        self.failed_symbols = []
        self.skipped_symbols = []
        self.invalid_symbols = []
        self.no_data_symbols = []
        self.timeout_symbols = []
        self.data_received = False
        self.connected = False
        self.chunk_end_date = None
        self.all_bars = []
        self.expected_chunks = 0
        self.chunks_received = 0
        self.symbol_start_time = None
        self.download_complete = threading.Event()
        self.max_retries = 2
        self.current_retry = 0
        self.resume_checked = False
        self.chunk_timeout = 30
        self.chunk_timer = None

        # Setup logging
        self.setup_logging()

        # Granularity mapping with appropriate chunk sizes and expected chunks
        self.granularity_map = {
            "1D": {
                "bar_size": "1 day",
                "chunk_duration": "1 Y",
                "chunks_per_year": 1,
                "seconds_per_chunk": 5
            },
            "4H": {
                "bar_size": "4 hours",
                "chunk_duration": "3 M",
                "chunks_per_year": 4,
                "seconds_per_chunk": 5
            },
            "1H": {
                "bar_size": "1 hour",
                "chunk_duration": "1 M",
                "chunks_per_year": 12,
                "seconds_per_chunk": 5
            },
            "15min": {
                "bar_size": "15 mins",
                "chunk_duration": "1 W",
                "chunks_per_year": 53,
                "seconds_per_chunk": 5
            },
            "1min": {
                "bar_size": "1 min",
                "chunk_duration": "2 D",
                "chunks_per_year": 183,
                "seconds_per_chunk": 8
            }
        }

        if granularity not in self.granularity_map:
            raise ValueError(f"Granularity {granularity} not supported. Choose from: {list(self.granularity_map.keys())}")

    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = os.path.join(self.output_dir, str(self.year))
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f'download_{self.year}.log')

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_expected_min_rows(self) -> int:
        """Get minimum expected rows for a complete download"""
        trading_days = 252

        if self.granularity == "1D":
            return trading_days
        elif self.granularity == "4H":
            return trading_days * 2
        elif self.granularity == "1H":
            return trading_days * 7
        elif self.granularity == "15min":
            return trading_days * 26
        elif self.granularity == "1min":
            return trading_days * 390
        return 0

    def get_downloaded_symbols(self) -> Set[str]:
        """
        Scan the output directory and return a set of symbols that have already been downloaded
        """
        downloaded = set()
        save_dir = os.path.join(self.output_dir, self.granularity, str(self.year))

        if not os.path.exists(save_dir):
            return downloaded

        # Look for CSV files matching the pattern
        pattern = os.path.join(save_dir, f"*_{self.year}.csv")
        for filepath in glob.glob(pattern):
            filename = os.path.basename(filepath)
            # Extract symbol from filename (remove _{year}.csv)
            symbol = filename.replace(f"_{self.year}.csv", "")

            # Verify the file has data (more than just header)
            try:
                with open(filepath, 'r') as f:
                    lines = f.readlines()
                    if len(lines) > 1:  # Has at least one data row
                        row_count = len(lines) - 1
                        expected_min_rows = self.get_expected_min_rows()

                        if row_count >= expected_min_rows:
                            downloaded.add(symbol)
                            self.logger.info(f"Found complete data for {symbol} ({row_count} rows)")
                        else:
                            self.logger.info(f"Found incomplete data for {symbol} ({row_count} rows), will re-download")
                    else:
                        self.logger.info(f"Found empty file for {symbol}, will re-download")
            except Exception as e:
                self.logger.warning(f"Error reading {filepath}: {str(e)}")

        return downloaded

    def filter_symbols_for_resume(self) -> Tuple[List[str], List[str]]:
        """
        Filter symbols list based on already downloaded files.
        Returns (symbols_to_download, already_downloaded)
        """
        if not self.resume:
            return self.original_symbols, []

        downloaded = self.get_downloaded_symbols()

        # Use set operations to find missing symbols
        all_symbols_set = set(self.original_symbols)
        symbols_to_download = list(all_symbols_set - downloaded)
        already_downloaded = list(downloaded & all_symbols_set)

        # Sort for consistency
        symbols_to_download.sort()
        already_downloaded.sort()

        self.logger.info(f"Resume check: {len(already_downloaded)} symbols already downloaded, {len(symbols_to_download)} remaining")

        if already_downloaded:
            self.logger.info(f"Already downloaded: {already_downloaded[:10]}" +
                           (f" and {len(already_downloaded)-10} more" if len(already_downloaded) > 10 else ""))

        return symbols_to_download, already_downloaded

    def nextValidId(self, orderId: int):
        """Start downloading data for first symbol"""
        self.connected = True
        self.logger.info("Connected to TWS/IB Gateway. Starting download...")

        # Filter symbols for resume if enabled
        if self.resume and not self.resume_checked:
            self.resume_checked = True
            symbols_to_download, already_downloaded = self.filter_symbols_for_resume()

            self.symbols = symbols_to_download
            self.skipped_symbols = already_downloaded

            if not symbols_to_download:
                self.logger.info("All symbols already downloaded. Nothing to do.")
                self.print_download_summary()
                self.download_complete.set()
                self.disconnect()
                return

        self.download_next_symbol()

    def download_next_symbol(self):
        """Download data for the next symbol in the list"""
        if not self.symbols:
            self.logger.info("All symbols processed. Disconnecting...")
            self.print_download_summary()
            self.download_complete.set()
            self.disconnect()
            return

        self.current_symbol = self.symbols.pop(0)
        self.all_bars = []
        self.chunks_received = 0
        self.current_retry = 0
        self.symbol_start_time = time.time()
        self.data_received = False

        self.expected_chunks = self.granularity_map[self.granularity]["chunks_per_year"]

        self.logger.info(f"Starting download for {self.current_symbol} (expected {self.expected_chunks} chunks)")

        self.chunk_end_date = datetime(self.year, 12, 31, 23, 59, 59)
        self.request_next_chunk()

    def request_next_chunk(self):
        """Request the next chunk of historical data"""
        year_start = datetime(self.year, 1, 1, 0, 0, 0)

        # Stop if we've gone past the start of the year
        if self.chunk_end_date < year_start:
            self.finish_symbol_download()
            return

        # Stop if we've reached expected chunks
        if self.chunks_received >= self.expected_chunks:
            self.finish_symbol_download()
            return

        # Cancel any existing timer
        if self.chunk_timer:
            self.chunk_timer.cancel()

        self.data_received = False
        self.logger.info(f"Requesting chunk {self.chunks_received + 1}/{self.expected_chunks} ending {self.chunk_end_date} for {self.current_symbol}")

        try:
            contract = Contract()
            contract.symbol = self.current_symbol
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"

            # For symbols with dots (like BRK.B), we need to handle them specially
            if '.' in self.current_symbol:
                base_symbol = self.current_symbol.split('.')[0]
                contract.symbol = base_symbol
                if base_symbol == 'BRK':
                    contract.primaryExchange = "NYSE"

            # Set a timeout for this request
            req_id = hash(f"{self.current_symbol}_{self.chunk_end_date}") % 10000
            self.chunk_timer = threading.Timer(self.chunk_timeout, self.chunk_timeout_handler, args=[req_id])
            self.chunk_timer.start()

            self.reqHistoricalData(
                reqId=req_id,
                contract=contract,
                endDateTime=self.chunk_end_date.strftime("%Y%m%d-%H:%M:%S"),
                durationStr=self.granularity_map[self.granularity]["chunk_duration"],
                barSizeSetting=self.granularity_map[self.granularity]["bar_size"],
                whatToShow="TRADES",
                useRTH=1,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[]
            )

        except Exception as e:
            self.logger.error(f"Error requesting chunk for {self.current_symbol}: {str(e)}")
            if self.chunk_timer:
                self.chunk_timer.cancel()
            self.failed_symbols.append(self.current_symbol)
            self.download_next_symbol()

    def chunk_timeout_handler(self, req_id: int):
        """Handle timeout for a chunk request"""
        if not self.data_received and self.current_symbol:
            self.logger.error(f"Timeout waiting for data for {self.current_symbol} (reqId: {req_id})")
            # Cancel the historical data request
            try:
                self.cancelHistoricalData(req_id)
            except:
                pass

            # If we've had multiple timeouts for this symbol, mark as failed
            if self.current_retry < self.max_retries:
                self.current_retry += 1
                self.logger.info(f"Retry {self.current_retry}/{self.max_retries} for {self.current_symbol} after timeout")
                # Reset and try again
                self.chunk_end_date = datetime(self.year, 12, 31, 23, 59, 59)
                self.chunks_received = 0
                self.all_bars = []
                self.request_next_chunk()
            else:
                self.logger.warning(f"Symbol {self.current_symbol} not responding after {self.max_retries} retries")
                self.timeout_symbols.append(self.current_symbol)
                # Move to next symbol
                self.download_next_symbol()

    def historicalData(self, reqId: int, bar):
        """Receive historical data bars"""
        # Cancel the timeout timer since we received data
        if self.chunk_timer:
            self.chunk_timer.cancel()
            self.chunk_timer = None

        self.data_received = True

        try:
            if ' ' in bar.date:
                date_part = ' '.join(bar.date.split()[:2])
                bar_date = datetime.strptime(date_part, "%Y%m%d %H:%M:%S")
            else:
                bar_date = datetime.strptime(bar.date, "%Y%m%d")
        except:
            bar_date = bar.date

        self.all_bars.append({
            'date': bar_date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume
        })

        if len(self.all_bars) % 1000 == 0:
            self.logger.debug(f"Received {len(self.all_bars)} bars so far for {self.current_symbol}")

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        """End of historical data chunk"""
        # Cancel any pending timeout timer
        if self.chunk_timer:
            self.chunk_timer.cancel()
            self.chunk_timer = None

        self.chunks_received += 1
        elapsed = time.time() - self.symbol_start_time

        if self.chunks_received > 0:
            avg_time_per_chunk = elapsed / self.chunks_received
            remaining_chunks = self.expected_chunks - self.chunks_received
            estimated_remaining = remaining_chunks * avg_time_per_chunk

            self.logger.info(f"Finished chunk {self.chunks_received}/{self.expected_chunks} for {self.current_symbol} "
                            f"(elapsed: {elapsed:.1f}s, est remaining: {estimated_remaining:.1f}s)")

        # Update chunk_end_date to go further back
        chunk_size = self.granularity_map[self.granularity]["chunk_duration"]

        try:
            if chunk_size.endswith('D'):
                days = int(chunk_size[:-1])
                self.chunk_end_date = self.chunk_end_date - timedelta(days=days)
            elif chunk_size.endswith('W'):
                weeks = int(chunk_size[:-1])
                self.chunk_end_date = self.chunk_end_date - timedelta(weeks=weeks)
            elif chunk_size.endswith('M'):
                months = int(chunk_size[:-1])
                self.chunk_end_date = self.chunk_end_date - relativedelta(months=months)
            elif chunk_size.endswith('Y'):
                years = int(chunk_size[:-1])
                self.chunk_end_date = self.chunk_end_date - relativedelta(years=years)
        except Exception as e:
            self.logger.error(f"Error calculating next chunk date: {str(e)}")
            self.chunk_end_date = self.chunk_end_date - timedelta(days=30)

        self.request_next_chunk()

    def finish_symbol_download(self):
        """Finish downloading for current symbol and save data"""
        # Cancel any pending timer
        if self.chunk_timer:
            self.chunk_timer.cancel()
            self.chunk_timer = None

        if not self.all_bars:
            # If we got no data at all, check if we already tried different approaches
            if self.current_retry < self.max_retries:
                self.current_retry += 1
                self.logger.info(f"Retry {self.current_retry}/{self.max_retries} for {self.current_symbol} with different settings")

                # Try with different exchange or primary exchange
                self.chunk_end_date = datetime(self.year, 12, 31, 23, 59, 59)
                self.chunks_received = 0
                self.all_bars = []
                self.request_next_chunk()
                return
            else:
                self.logger.warning(f"No data received for {self.current_symbol} after {self.max_retries} retries")
                self.failed_symbols.append(self.current_symbol)
        else:
            # Sort and filter bars
            self.all_bars.sort(key=lambda x: x['date'])

            year_start = datetime(self.year, 1, 1)
            year_end = datetime(self.year, 12, 31, 23, 59, 59)
            original_count = len(self.all_bars)
            self.all_bars = [bar for bar in self.all_bars if year_start <= bar['date'] <= year_end]

            self.logger.info(f"Filtered {original_count} bars to {len(self.all_bars)} bars within {self.year}")

            if self.all_bars:
                self.save_symbol_data()
                self.completed_symbols.append(self.current_symbol)
                elapsed = time.time() - self.symbol_start_time
                self.logger.info(f"Successfully downloaded {len(self.all_bars)} bars for {self.current_symbol} in {elapsed:.1f}s")
            else:
                self.logger.warning(f"No data within {self.year} for {self.current_symbol}")
                self.no_data_symbols.append(self.current_symbol)

        self.download_next_symbol()

    def save_symbol_data(self):
        """Save collected data to CSV file"""
        save_dir = os.path.join(self.output_dir, self.granularity, str(self.year))
        os.makedirs(save_dir, exist_ok=True)

        filename = os.path.join(save_dir, f"{self.current_symbol}_{self.year}.csv")

        try:
            with open(filename, 'w', newline='') as csvfile:
                fieldnames = ['date', 'open', 'high', 'low', 'close', 'volume']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for bar in self.all_bars:
                    if self.granularity == "1D":
                        date_str = bar['date'].strftime("%Y%m%d")
                    else:
                        date_str = bar['date'].strftime("%Y%m%d %H:%M:%S")

                    writer.writerow({
                        'date': date_str,
                        'open': bar['open'],
                        'high': bar['high'],
                        'low': bar['low'],
                        'close': bar['close'],
                        'volume': bar['volume']
                    })

            self.logger.info(f"Successfully saved {len(self.all_bars)} bars to {filename}")

        except Exception as e:
            self.logger.error(f"Error saving data for {self.current_symbol}: {str(e)}")
            self.failed_symbols.append(self.current_symbol)

    def error(self, reqId: int, errorTime: int, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        """Handle errors from TWS/IB Gateway"""
        # Cancel timeout timer if there's an error
        if self.chunk_timer:
            self.chunk_timer.cancel()
            self.chunk_timer = None

        # Informational messages (ignore)
        if errorCode in [2104, 2106, 2158, 2107, 2108]:
            self.logger.info(f"Info {errorCode}: {errorString}")
            return

        # Connection messages
        elif errorCode in [501, 502, 503, 504]:
            self.logger.error(f"Connection error {errorCode}: {errorString}")
            return

        # No security definition found - symbol doesn't exist or is invalid
        elif errorCode == 200:
            self.logger.error(f"Invalid symbol {self.current_symbol}: {errorString}")
            if self.current_symbol and self.current_symbol not in self.invalid_symbols:
                self.invalid_symbols.append(self.current_symbol)
                # Skip to next symbol immediately
                self.download_next_symbol()
            return

        # No historical data available - symbol exists but no data for this period
        elif errorCode == 162:
            self.logger.error(f"No historical data for {self.current_symbol}: {errorString}")
            if self.current_symbol and self.current_symbol not in self.no_data_symbols:
                self.no_data_symbols.append(self.current_symbol)
                # Skip to next symbol
                self.download_next_symbol()
            return

        # General errors
        else:
            self.logger.error(f"Error {errorCode}: {errorString}")
            if reqId != -1 and self.current_symbol:
                self.logger.error(f"Fatal error for {self.current_symbol}, moving to next symbol")
                if self.current_symbol not in self.failed_symbols:
                    self.failed_symbols.append(self.current_symbol)
                self.download_next_symbol()

    def connectionClosed(self):
        """Handle connection closed"""
        self.logger.info("Connection closed")
        self.connected = False
        self.download_complete.set()

    def print_download_summary(self):
        """Print detailed summary of download results"""
        self.logger.info("\n" + "="*70)
        self.logger.info("DOWNLOAD SUMMARY")
        self.logger.info("="*70)
        self.logger.info(f"Year: {self.year}")
        self.logger.info(f"Granularity: {self.granularity}")
        self.logger.info(f"Total symbols in tickers file: {len(self.original_symbols)}")
        self.logger.info(f"Successfully downloaded this session: {len(self.completed_symbols)}")

        if self.skipped_symbols:
            self.logger.info(f"Already existed (skipped): {len(self.skipped_symbols)}")

        if self.invalid_symbols:
            self.logger.info(f"Invalid symbols (no security definition): {len(self.invalid_symbols)}")
            if len(self.invalid_symbols) <= 20:
                for symbol in sorted(self.invalid_symbols):
                    self.logger.info(f"  ✗ {symbol}")
            else:
                self.logger.info(f"  (First 20 of {len(self.invalid_symbols)}): {sorted(self.invalid_symbols)[:20]}")

        if self.no_data_symbols:
            self.logger.info(f"Valid symbols but no data: {len(self.no_data_symbols)}")
            if len(self.no_data_symbols) <= 20:
                for symbol in sorted(self.no_data_symbols):
                    self.logger.info(f"  ○ {symbol}")
            else:
                self.logger.info(f"  (First 20 of {len(self.no_data_symbols)}): {sorted(self.no_data_symbols)[:20]}")

        if self.timeout_symbols:
            self.logger.info(f"Symbols that timed out: {len(self.timeout_symbols)}")
            if len(self.timeout_symbols) <= 20:
                for symbol in sorted(self.timeout_symbols):
                    self.logger.info(f"  ⏱ {symbol}")
            else:
                self.logger.info(f"  (First 20 of {len(self.timeout_symbols)}): {sorted(self.timeout_symbols)[:20]}")

        if self.failed_symbols:
            self.logger.info(f"Other failures: {len(self.failed_symbols)}")
            if len(self.failed_symbols) <= 20:
                for symbol in sorted(self.failed_symbols):
                    self.logger.info(f"  ✗ {symbol}")
            else:
                self.logger.info(f"  (First 20 of {len(self.failed_symbols)}): {sorted(self.failed_symbols)[:20]}")

        # Calculate overall progress
        total_downloaded = len(self.completed_symbols) + len(self.skipped_symbols)
        total_possible = len(self.original_symbols)

        if total_possible > 0:
            pct = (total_downloaded / total_possible) * 100
            self.logger.info(f"\nOverall progress: {total_downloaded}/{total_possible} ({pct:.1f}%)")


def load_tickers_from_file(filepath: str, year: int) -> List[str]:
    """
    Load tickers from a tickers.csv file.
    Expected format: CSV with 'ticker' column or single column of tickers
    """
    tickers = []

    try:
        with open(filepath, 'r') as f:
            first_line = f.readline().strip()
            f.seek(0)

            if first_line.lower() == 'ticker' or first_line.lower().startswith('ticker,'):
                reader = csv.DictReader(f)
                for row in reader:
                    if 'ticker' in row:
                        ticker = row['ticker'].strip()
                        if ticker:
                            tickers.append(ticker)
            else:
                reader = csv.reader(f)
                for row in reader:
                    if row and row[0].strip():
                        tickers.append(row[0].strip())

        print(f"Loaded {len(tickers)} tickers from {filepath}")
        return sorted(set(tickers))

    except Exception as e:
        print(f"Error loading tickers from {filepath}: {str(e)}")
        return []


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Download historical OHLCV data from Interactive Brokers'
    )

    symbol_group = parser.add_mutually_exclusive_group(required=True)
    symbol_group.add_argument(
        '--symbols',
        type=str,
        help='Comma-separated list of symbols (e.g., "AAPL,TSLA,GOOG")'
    )
    symbol_group.add_argument(
        '--tickers-file',
        type=str,
        help='Path to tickers.csv file from spy_constituents_downloader.py'
    )

    parser.add_argument(
        '--year',
        type=int,
        required=True,
        help='Year to download data for (e.g., 2022)'
    )

    parser.add_argument(
        '--granularity',
        type=str,
        required=True,
        choices=['1D', '4H', '1H', '15min', '1min'],
        help='Price granularity (1D, 4H, 1H, 15min, 1min)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='./data',
        help='Base directory for output files (default: ./data)'
    )

    parser.add_argument(
        '--host',
        type=str,
        default='127.0.0.1',
        help='TWS/IB Gateway host (default: 127.0.0.1)'
    )

    parser.add_argument(
        '--port',
        type=int,
        default=7497,
        help='TWS/IB Gateway port (default: 7497 for TWS, 4002 for Gateway)'
    )

    parser.add_argument(
        '--client-id',
        type=int,
        default=123,
        help='Client ID (default: 123)'
    )

    parser.add_argument(
        '--timeout',
        type=int,
        default=0,
        help='Timeout per symbol in seconds (0 = calculate based on granularity)'
    )

    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume download, skipping symbols that already have data files'
    )

    parser.add_argument(
        '--chunk-timeout',
        type=int,
        default=30,
        help='Timeout in seconds for each chunk request (default: 30)'
    )

    return parser.parse_args()


def main():
    """Main execution function"""
    args = parse_arguments()

    # Get symbols from either direct list or tickers file
    symbols = []

    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',') if s.strip()]
        print(f"Using {len(symbols)} symbols from command line")

    elif args.tickers_file:
        symbols = load_tickers_from_file(args.tickers_file, args.year)
        if not symbols:
            print(f"Error: No symbols found in {args.tickers_file}")
            sys.exit(1)

    # Granularity timing map
    granularity_timeout_map = {
        "1D": 60,
        "4H": 120,
        "1H": 180,
        "15min": 600,
        "1min": 1800
    }

    if args.timeout == 0:
        timeout_per_symbol = granularity_timeout_map[args.granularity]
        print(f"Using dynamic timeout: {timeout_per_symbol} seconds per symbol for {args.granularity} data")
    else:
        timeout_per_symbol = args.timeout
        print(f"Using user-specified timeout: {timeout_per_symbol} seconds per symbol")

    print(f"\nStarting download for {len(symbols)} symbols...")
    print(f"Year: {args.year}")
    print(f"Granularity: {args.granularity}")
    print(f"Output directory: {args.output_dir}")
    print(f"Chunk timeout: {args.chunk_timeout} seconds")
    if args.resume:
        print(f"Resume mode: Enabled (will skip symbols with existing data files)")
    print(f"Note: Invalid symbols (merged/acquired/delisted) will be automatically skipped")

    # Create app instance with ALL symbols first
    app = IBHistoricalDownloader(
        symbols=symbols.copy(),
        year=args.year,
        granularity=args.granularity,
        output_dir=args.output_dir,
        resume=args.resume
    )

    # Set chunk timeout
    app.chunk_timeout = args.chunk_timeout

    # Connect to TWS/IB Gateway
    app.connect(args.host, args.port, clientId=args.client_id)

    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    # Wait for connection and resume check to complete
    time.sleep(2)

    # If resume is enabled, the symbols list has already been filtered in nextValidId
    remaining_symbols = len(app.symbols)

    # If resume is enabled and we've already downloaded everything, exit
    if args.resume and remaining_symbols == 0:
        print("\nAll symbols already downloaded. Exiting.")
        sys.exit(0)

    total_timeout = remaining_symbols * timeout_per_symbol
    start_time = time.time()

    print(f"\nRemaining symbols to download: {remaining_symbols}")
    print(f"Maximum total timeout: {total_timeout} seconds")

    # Progress tracking
    last_completed = 0
    last_failed = 0

    while time.time() - start_time < total_timeout:
        if app.download_complete.is_set():
            break

        # Show progress every 30 seconds
        current_completed = len(app.completed_symbols)
        current_failed = len(app.failed_symbols) + len(app.invalid_symbols) + len(app.no_data_symbols) + len(app.timeout_symbols)

        if current_completed + current_failed > last_completed + last_failed:
            elapsed = time.time() - start_time
            pct = ((current_completed + current_failed) / remaining_symbols) * 100 if remaining_symbols > 0 else 0
            print(f"\rProgress: {current_completed} completed, {current_failed} failed, "
                  f"{remaining_symbols - current_completed - current_failed} remaining "
                  f"({pct:.1f}%) elapsed: {elapsed:.0f}s", end="", flush=True)
            last_completed = current_completed
            last_failed = current_failed

        total_processed = len(app.completed_symbols) + len(app.failed_symbols) + \
                         len(app.invalid_symbols) + len(app.no_data_symbols) + \
                         len(app.skipped_symbols) + len(app.timeout_symbols)

        if total_processed >= len(app.original_symbols):
            break

        time.sleep(1)

    print()  # New line after progress

    if app.isConnected():
        elapsed = time.time() - start_time
        app.logger.info(f"Timeout reached after {elapsed:.1f} seconds. Disconnecting...")
        app.disconnect()

    time.sleep(2)


if __name__ == "__main__":
    main()
