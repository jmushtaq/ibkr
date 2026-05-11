#!/usr/bin/env python3
"""
Index Price Downloader for Interactive Brokers
Downloads historical price data for major indices:
- S&P 500 (SPX)
- Dow Jones 30 (DJI/DOW)
- NASDAQ 100 (NDX)
- Russell 2000 (RUT)
- VIX (VIX)

To Run:
    python index_price_downloader.py --year 2024 --granularity 1D --output-dir ./index_data
    python index_price_downloader.py --year 2020-2024 --granularity 1H --output-dir ./index_data
    python index_price_downloader.py --year 2024 --granularity 1min --output-dir ./index_data
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
from typing import List, Dict
from dateutil.relativedelta import relativedelta


class IndexHistoricalDownloader(EWrapper, EClient):
    """Download historical price data for indices"""

    def __init__(self, indices: List[Dict], year: int, granularity: str, output_dir: str, resume: bool = False):
        EClient.__init__(self, self)
        self.indices = indices.copy()
        self.year = year
        self.granularity = granularity
        self.output_dir = output_dir
        self.resume = resume
        self.current_index = None
        self.current_contract = None
        self.completed_indices = []
        self.failed_indices = []
        self.all_bars = []
        self.connected = False
        self.data_received = False
        self.download_complete = threading.Event()
        self.chunk_timer = None
        self.chunk_timeout = 30

        # Granularity mapping
        self.granularity_map = {
            "1D": {"bar_size": "1 day", "chunk_duration": "1 Y", "chunks_per_year": 1, "seconds_per_chunk": 5},
            "4H": {"bar_size": "4 hours", "chunk_duration": "3 M", "chunks_per_year": 4, "seconds_per_chunk": 5},
            "1H": {"bar_size": "1 hour", "chunk_duration": "1 M", "chunks_per_year": 12, "seconds_per_chunk": 5},
            "15min": {"bar_size": "15 mins", "chunk_duration": "1 W", "chunks_per_year": 53, "seconds_per_chunk": 5},
            "1min": {"bar_size": "1 min", "chunk_duration": "2 D", "chunks_per_year": 183, "seconds_per_chunk": 8}
        }

        if granularity not in self.granularity_map:
            raise ValueError(f"Granularity {granularity} not supported. Choose from: {list(self.granularity_map.keys())}")

        self.setup_logging()

    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = os.path.join(self.output_dir, str(self.year))
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f'index_download_{self.year}.log')

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_index_contract(self, index_info: Dict) -> Contract:
        """Create IBKR contract for the index"""
        contract = Contract()
        contract.symbol = index_info['symbol']
        contract.secType = index_info.get('secType', 'IND')
        contract.exchange = index_info.get('exchange', 'CBOE')
        contract.currency = index_info.get('currency', 'USD')

        # Special handling for different indices
        if index_info['name'] == 'SP500':
            contract.symbol = 'SPX'
            contract.exchange = 'CBOE'
        elif index_info['name'] == 'DowJones':
            contract.symbol = 'DJI'
            contract.exchange = 'CBOE'
        elif index_info['name'] == 'NASDAQ100':
            contract.symbol = 'NDX'
            contract.exchange = 'CBOE'
        elif index_info['name'] == 'Russell2000':
            contract.symbol = 'RUT'
            contract.exchange = 'CBOE'
        elif index_info['name'] == 'VIX':
            contract.symbol = 'VIX'
            contract.exchange = 'CBOE'
            contract.secType = 'IND'

        return contract

    def nextValidId(self, orderId: int):
        """Start downloading data for first index"""
        self.connected = True
        self.logger.info("=" * 70)
        self.logger.info(f"STARTING YEAR {self.year}")
        self.logger.info("=" * 70)
        self.download_next_index()

    def download_next_index(self):
        """Download data for the next index"""
        if not self.indices:
            self.logger.info(f"Year {self.year} complete. Processed {len(self.completed_indices)} indices successfully.")
            self.print_download_summary()
            self.download_complete.set()
            self.disconnect()
            return

        self.current_index = self.indices.pop(0)
        self.current_contract = self.get_index_contract(self.current_index)
        self.all_bars = []
        self.chunks_received = 0
        self.data_received = False

        expected_chunks = self.granularity_map[self.granularity]["chunks_per_year"]

        self.logger.info(f"Starting download for {self.current_index['name']} ({self.current_index['symbol']}) "
                        f"(expected {expected_chunks} chunks)")

        self.chunk_end_date = datetime(self.year, 12, 31, 23, 59, 59)
        self.request_next_chunk()

    def request_next_chunk(self):
        """Request the next chunk of historical data"""
        year_start = datetime(self.year, 1, 1, 0, 0, 0)

        if self.chunk_end_date < year_start:
            self.finish_index_download()
            return

        # Cancel any existing timer
        if self.chunk_timer:
            self.chunk_timer.cancel()

        self.data_received = False
        chunks_per_year = self.granularity_map[self.granularity]["chunks_per_year"]

        self.logger.info(f"Requesting chunk {self.chunks_received + 1}/{chunks_per_year} "
                        f"ending {self.chunk_end_date} for {self.current_index['name']}")

        try:
            req_id = hash(f"{self.current_index['name']}_{self.chunk_end_date}") % 10000

            self.chunk_timer = threading.Timer(self.chunk_timeout, self.chunk_timeout_handler, args=[req_id])
            self.chunk_timer.start()

            self.reqHistoricalData(
                reqId=req_id,
                contract=self.current_contract,
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
            self.logger.error(f"Error requesting chunk for {self.current_index['name']}: {str(e)}")
            if self.chunk_timer:
                self.chunk_timer.cancel()
            self.failed_indices.append(self.current_index['name'])
            self.download_next_index()

    def chunk_timeout_handler(self, req_id: int):
        """Handle timeout for a chunk request"""
        if not self.data_received and self.current_index:
            self.logger.error(f"Timeout waiting for data for {self.current_index['name']} (reqId: {req_id})")
            try:
                self.cancelHistoricalData(req_id)
            except:
                pass
            self.failed_indices.append(self.current_index['name'])
            self.download_next_index()

    def historicalData(self, reqId: int, bar):
        """Receive historical data bars"""
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
            'volume': bar.volume if hasattr(bar, 'volume') else 0
        })

        if len(self.all_bars) % 1000 == 0:
            self.logger.debug(f"Received {len(self.all_bars)} bars so far for {self.current_index['name']}")

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        """End of historical data chunk"""
        if self.chunk_timer:
            self.chunk_timer.cancel()
            self.chunk_timer = None

        self.chunks_received += 1

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

    def finish_index_download(self):
        """Finish downloading for current index and save data"""
        if self.chunk_timer:
            self.chunk_timer.cancel()
            self.chunk_timer = None

        if not self.all_bars:
            self.logger.warning(f"No data received for {self.current_index['name']}")
            self.failed_indices.append(self.current_index['name'])
        else:
            # Sort and filter bars
            self.all_bars.sort(key=lambda x: x['date'])

            year_start = datetime(self.year, 1, 1)
            year_end = datetime(self.year, 12, 31, 23, 59, 59)
            original_count = len(self.all_bars)
            self.all_bars = [bar for bar in self.all_bars if year_start <= bar['date'] <= year_end]

            self.logger.info(f"Filtered {original_count} bars to {len(self.all_bars)} bars within {self.year}")

            if self.all_bars:
                self.save_index_data()
                self.completed_indices.append(self.current_index['name'])
                self.logger.info(f"Successfully downloaded {len(self.all_bars)} bars for {self.current_index['name']}")
            else:
                self.logger.warning(f"No data within {self.year} for {self.current_index['name']}")
                self.failed_indices.append(self.current_index['name'])

        self.download_next_index()

    def save_index_data(self):
        """Save collected data to CSV file"""
        save_dir = os.path.join(self.output_dir, self.granularity, str(self.year))
        os.makedirs(save_dir, exist_ok=True)

        filename = os.path.join(save_dir, f"{self.current_index['name']}_{self.year}.csv")

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
            self.logger.error(f"Error saving data for {self.current_index['name']}: {str(e)}")
            self.failed_indices.append(self.current_index['name'])

    def error(self, reqId: int, errorTime: int, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        """Handle errors from TWS/IB Gateway"""
        if self.chunk_timer:
            self.chunk_timer.cancel()
            self.chunk_timer = None

        # Informational messages (ignore)
        if errorCode in [2104, 2106, 2158, 2107, 2108]:
            self.logger.info(f"Info {errorCode}: {errorString}")
            return

        # No historical data available
        elif errorCode == 162:
            self.logger.warning(f"No historical data for {self.current_index['name']}: {errorString}")
            if self.current_index and self.current_index['name'] not in self.failed_indices:
                self.failed_indices.append(self.current_index['name'])
                self.download_next_index()
            return

        # General errors
        else:
            self.logger.error(f"Error {errorCode}: {errorString}")
            if reqId != -1 and self.current_index:
                self.logger.error(f"Error for {self.current_index['name']}, moving to next index")
                if self.current_index['name'] not in self.failed_indices:
                    self.failed_indices.append(self.current_index['name'])
                self.download_next_index()

    def connectionClosed(self):
        """Handle connection closed"""
        self.logger.info("Connection closed")
        self.connected = False
        self.download_complete.set()

    def print_download_summary(self):
        """Print summary of download results"""
        self.logger.info("\n" + "=" * 70)
        self.logger.info(f"YEAR {self.year} DOWNLOAD SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"Granularity: {self.granularity}")
        self.logger.info(f"Successfully downloaded: {len(self.completed_indices)}")

        if self.failed_indices:
            self.logger.info(f"Failed: {len(self.failed_indices)}")
            for idx in self.failed_indices:
                self.logger.info(f"  ✗ {idx}")

        self.logger.info("=" * 70)


def parse_year_range(year_spec: str) -> List[int]:
    """Parse year specification like '2001' or '2001-2026' into list of years"""
    years = []

    if '-' in year_spec:
        start_year, end_year = map(int, year_spec.split('-'))
        if start_year > end_year:
            raise ValueError(f"Invalid year range: {year_spec}. Start year must be <= end year.")
        years = list(range(start_year, end_year + 1))
    else:
        years = [int(year_spec)]

    return years


def get_indices_list() -> List[Dict]:
    """Return list of indices to download"""
    return [
        {'name': 'SP500', 'symbol': 'SPX', 'secType': 'IND', 'exchange': 'CBOE', 'currency': 'USD'},
        {'name': 'DowJones', 'symbol': 'DJI', 'secType': 'IND', 'exchange': 'CBOE', 'currency': 'USD'},
        {'name': 'NASDAQ100', 'symbol': 'NDX', 'secType': 'IND', 'exchange': 'CBOE', 'currency': 'USD'},
        {'name': 'Russell2000', 'symbol': 'RUT', 'secType': 'IND', 'exchange': 'CBOE', 'currency': 'USD'},
        {'name': 'VIX', 'symbol': 'VIX', 'secType': 'IND', 'exchange': 'CBOE', 'currency': 'USD'}
    ]


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Download historical index price data from Interactive Brokers'
    )

    parser.add_argument(
        '--year',
        type=str,
        required=True,
        help='Year or year range to download data for (e.g., "2024" or "2020-2024")'
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
        default='./index_data',
        help='Base directory for output files (default: ./index_data)'
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
        default=456,
        help='Client ID (default: 456)'
    )

    parser.add_argument(
        '--chunk-timeout',
        type=int,
        default=30,
        help='Timeout in seconds for each chunk request (default: 30)'
    )

    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume download, skipping indices that already have data files'
    )

    return parser.parse_args()


def main():
    """Main execution function"""
    args = parse_arguments()

    # Parse years
    try:
        years = parse_year_range(args.year)
        print(f"Years to process: {args.year} ({len(years)} years)")
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Get indices list
    indices = get_indices_list()
    print(f"\nIndices to download: {', '.join([idx['name'] for idx in indices])}")

    print(f"\nStarting download for {len(indices)} indices across {len(years)} years...")
    print(f"Granularity: {args.granularity}")
    print(f"Output directory: {args.output_dir}")
    print(f"Chunk timeout: {args.chunk_timeout} seconds")

    # Process each year sequentially
    for year_idx, year in enumerate(years, 1):
        print("\n" + "=" * 80)
        print(f"PROCESSING YEAR {year} ({year_idx}/{len(years)})")
        print("=" * 80)

        # Create app instance for this year
        app = IndexHistoricalDownloader(
            indices=indices.copy(),
            year=year,
            granularity=args.granularity,
            output_dir=args.output_dir,
            resume=args.resume
        )

        app.chunk_timeout = args.chunk_timeout

        # Connect to TWS/IB Gateway
        app.connect(args.host, args.port, clientId=args.client_id + year_idx)

        api_thread = threading.Thread(target=app.run, daemon=True)
        api_thread.start()

        # Wait for connection
        time.sleep(2)

        # Wait for download to complete
        total_timeout = 300  # 5 minutes max per index
        start_time = time.time()

        while time.time() - start_time < total_timeout * len(indices):
            if app.download_complete.is_set():
                break
            time.sleep(1)

        if app.isConnected():
            app.disconnect()

        time.sleep(2)

    print("\n" + "=" * 80)
    print("INDEX PRICE DOWNLOAD COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
