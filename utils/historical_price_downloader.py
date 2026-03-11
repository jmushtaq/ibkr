#!/usr/bin/env python3
"""
Historical Data Downloader for Interactive Brokers
Downloads OHLCV data for multiple symbols and saves to CSV files.
Supports chunked downloads for higher granularities.

python utils/historical_price_downloader.py --year 2022 --symbols "AAPL" --granularity 1min --output-dir ./data
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
from typing import List
import queue
from dateutil.relativedelta import relativedelta

class IBHistoricalDownloader(EWrapper, EClient):
    def __init__(self, symbols: List[str], year: int, granularity: str, output_dir: str):
        EClient.__init__(self, self)
        self.symbols = symbols
        self.year = year
        self.granularity = granularity
        self.output_dir = output_dir
        self.data_queue = queue.Queue()
        self.current_symbol = None
        self.completed_symbols = []
        self.failed_symbols = []
        self.data_received = False
        self.connected = False
        self.chunk_start_date = None
        self.chunk_end_date = None
        self.all_bars = []  # Store all bars for current symbol
        self.expected_chunks = 0
        self.chunks_received = 0
        self.symbol_start_time = None
        self.download_complete = threading.Event()

        # Setup logging
        self.setup_logging()

        # Granularity mapping with appropriate chunk sizes and expected chunks
        self.granularity_map = {
            "1D": {
                "bar_size": "1 day",
                "chunk_duration": "1 Y",
                "chunks_per_year": 1,
                "seconds_per_chunk": 5  # Approximate seconds per chunk
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
                "chunks_per_year": 183,  # 365/2 ≈ 183 chunks
                "seconds_per_chunk": 8     # 1-min chunks take a bit longer
            }
        }

        # Validate granularity
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

    def nextValidId(self, orderId: int):
        """Start downloading data for first symbol"""
        self.connected = True
        self.logger.info("Connected to TWS/IB Gateway. Starting download...")
        self.download_next_symbol()

    def download_next_symbol(self):
        """Download data for the next symbol in the list"""
        if not self.symbols:
            self.logger.info("All symbols processed. Disconnecting...")
            self.download_complete.set()
            self.disconnect()
            return

        self.current_symbol = self.symbols.pop(0)
        self.all_bars = []  # Reset for new symbol
        self.chunks_received = 0
        self.symbol_start_time = time.time()

        # Get expected chunks from map
        self.expected_chunks = self.granularity_map[self.granularity]["chunks_per_year"]

        self.logger.info(f"Starting download for {self.current_symbol} (expected {self.expected_chunks} chunks)")

        # Start with the end of the year and work backwards
        self.chunk_end_date = datetime(self.year, 12, 31, 23, 59, 59)
        self.request_next_chunk()

    def request_next_chunk(self):
        """Request the next chunk of historical data"""
        # Check if we've gone past the start of the year
        year_start = datetime(self.year, 1, 1, 0, 0, 0)
        if self.chunk_end_date < year_start:
            # We've downloaded all chunks for this symbol
            self.finish_symbol_download()
            return

        # Also check if we've reached expected chunks
        if self.chunks_received >= self.expected_chunks:
            self.logger.info(f"Reached expected chunk count ({self.expected_chunks}) for {self.current_symbol}")
            self.finish_symbol_download()
            return

        self.data_received = False
        self.logger.info(f"Requesting chunk {self.chunks_received + 1}/{self.expected_chunks} ending {self.chunk_end_date} for {self.current_symbol}")

        try:
            contract = Contract()
            contract.symbol = self.current_symbol
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"

            # Request historical data chunk
            self.reqHistoricalData(
                reqId=hash(f"{self.current_symbol}_{self.chunk_end_date}") % 10000,
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
            self.failed_symbols.append(self.current_symbol)
            self.download_next_symbol()

    def historicalData(self, reqId: int, bar):
        """Receive historical data bars"""
        self.data_received = True

        # Parse the date - handle different formats
        try:
            # Try to parse with timezone info
            if ' ' in bar.date:
                # Format like "20221227 09:30:00 US/Eastern"
                date_part = ' '.join(bar.date.split()[:2])
                bar_date = datetime.strptime(date_part, "%Y%m%d %H:%M:%S")
            else:
                # Format like "20221227"
                bar_date = datetime.strptime(bar.date, "%Y%m%d")
        except:
            # Fallback to string as is
            bar_date = bar.date

        self.all_bars.append({
            'date': bar_date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume
        })

        # Log progress periodically
        if len(self.all_bars) % 1000 == 0:
            self.logger.debug(f"Received {len(self.all_bars)} bars so far for {self.current_symbol}")

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        """End of historical data chunk"""
        self.chunks_received += 1
        elapsed = time.time() - self.symbol_start_time
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
                # Use relativedelta for safe month subtraction
                self.chunk_end_date = self.chunk_end_date - relativedelta(months=months)
            elif chunk_size.endswith('Y'):
                years = int(chunk_size[:-1])
                self.chunk_end_date = self.chunk_end_date - relativedelta(years=years)
        except Exception as e:
            self.logger.error(f"Error calculating next chunk date: {str(e)}")
            # Fallback to simple subtraction
            self.chunk_end_date = self.chunk_end_date - timedelta(days=30)

        # Request next chunk
        self.request_next_chunk()

    def finish_symbol_download(self):
        """Finish downloading for current symbol and save data"""
        if not self.all_bars:
            self.logger.warning(f"No data received for {self.current_symbol}")
            self.failed_symbols.append(self.current_symbol)
        else:
            # Sort bars by date
            self.all_bars.sort(key=lambda x: x['date'])

            # Filter to only include bars from the target year
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
                self.failed_symbols.append(self.current_symbol)

        # Move to next symbol
        self.download_next_symbol()

    def save_symbol_data(self):
        """Save collected data to CSV file"""
        # Create directory structure: output_dir/granularity/year/
        save_dir = os.path.join(self.output_dir, self.granularity, str(self.year))
        os.makedirs(save_dir, exist_ok=True)

        # Create filename: symbol_year.csv
        filename = os.path.join(save_dir, f"{self.current_symbol}_{self.year}.csv")

        try:
            with open(filename, 'w', newline='') as csvfile:
                fieldnames = ['date', 'open', 'high', 'low', 'close', 'volume']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                writer.writeheader()

                for bar in self.all_bars:
                    # Format date consistently
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
        # Informational messages (ignore these)
        if errorCode in [2104, 2106, 2158, 2107, 2108]:
            self.logger.info(f"Info {errorCode}: {errorString}")
            return

        # Connection messages
        elif errorCode in [501, 502, 503, 504]:
            self.logger.error(f"Connection error {errorCode}: {errorString}")
            return

        # Historical data errors
        elif errorCode in [162, 200]:
            self.logger.error(f"Historical data error {errorCode}: {errorString}")
            # Move to next chunk or symbol
            if self.current_symbol:
                # Try to move to next chunk
                self.logger.info(f"Skipping to next chunk for {self.current_symbol}")
                self.chunk_end_date = self.chunk_end_date - timedelta(days=30)  # Skip back a month
                self.request_next_chunk()
            return

        # General errors
        else:
            self.logger.error(f"Error {errorCode}: {errorString}")

            # If it's a fatal error for this symbol, move to next
            if reqId != -1 and self.current_symbol:
                self.logger.error(f"Fatal error for {self.current_symbol}, moving to next symbol")
                self.failed_symbols.append(self.current_symbol)
                self.download_next_symbol()

    def connectionClosed(self):
        """Handle connection closed"""
        self.logger.info("Connection closed")
        self.connected = False
        self.download_complete.set()


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Download historical OHLCV data from Interactive Brokers'
    )

    parser.add_argument(
        '--year',
        type=int,
        required=True,
        help='Year to download data for (e.g., 2022)'
    )

    parser.add_argument(
        '--symbols',
        type=str,
        required=True,
        help='Comma-separated list of symbols (e.g., "AAPL,TSLA,GOOG")'
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
        default=0,  # 0 means calculate dynamically
        help='Timeout per symbol in seconds (0 = calculate based on granularity, default: 0)'
    )

    return parser.parse_args()

def main():
    """Main execution function"""
    args = parse_arguments()

    # Parse symbols
    symbols = [s.strip() for s in args.symbols.split(',') if s.strip()]

    if not symbols:
        print("Error: No symbols provided")
        sys.exit(1)

    # Granularity timing map for dynamic timeout calculation
    granularity_timeout_map = {
        "1D": 60,      # 1 minute per symbol
        "4H": 120,      # 2 minutes per symbol
        "1H": 180,      # 3 minutes per symbol
        "15min": 600,   # 10 minutes per symbol
        "1min": 1800    # 30 minutes per symbol (183 chunks * ~8 seconds = ~24 minutes)
    }

    # Calculate timeout if not specified
    if args.timeout == 0:
        timeout_per_symbol = granularity_timeout_map[args.granularity]
        print(f"Using dynamic timeout: {timeout_per_symbol} seconds per symbol for {args.granularity} data")
    else:
        timeout_per_symbol = args.timeout
        print(f"Using user-specified timeout: {timeout_per_symbol} seconds per symbol")

    print(f"Starting download for {len(symbols)} symbols...")
    print(f"Year: {args.year}")
    print(f"Granularity: {args.granularity}")
    print(f"Output directory: {args.output_dir}")

    # Create app instance
    app = IBHistoricalDownloader(
        symbols=symbols.copy(),
        year=args.year,
        granularity=args.granularity,
        output_dir=args.output_dir
    )

    # Connect to TWS/IB Gateway
    app.connect(args.host, args.port, clientId=args.client_id)

    # Start the thread
    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    # Wait for connection
    time.sleep(2)

    # Wait for completion with dynamic timeout
    total_timeout = len(symbols) * timeout_per_symbol
    start_time = time.time()

    print(f"Maximum total timeout: {total_timeout} seconds")

    # Wait either for completion event or timeout
    while time.time() - start_time < total_timeout:
        # Check if download is complete
        if app.download_complete.is_set():
            break

        # Also check if all symbols are processed
        if len(app.completed_symbols) + len(app.failed_symbols) >= len(symbols):
            break

        time.sleep(1)

    # Check if still connected
    if app.isConnected():
        elapsed = time.time() - start_time
        app.logger.info(f"Timeout reached after {elapsed:.1f} seconds. Disconnecting...")
        app.disconnect()

    # Small delay to allow final processing
    time.sleep(2)

    # Print summary
    print("\n" + "="*50)
    print("DOWNLOAD SUMMARY")
    print("="*50)
    print(f"Total symbols: {len(symbols)}")
    print(f"Successful: {len(app.completed_symbols)}")
    for symbol in app.completed_symbols:
        print(f"  ✓ {symbol}")

    if app.failed_symbols:
        print(f"\nFailed: {len(app.failed_symbols)}")
        for symbol in app.failed_symbols:
            print(f"  ✗ {symbol}")

    print(f"\nLog file: {args.output_dir}/{args.year}/download_{args.year}.log")

if __name__ == "__main__":
    main()

