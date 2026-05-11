#!/usr/bin/env python3
"""
Ticker Enrichment Script
Reads tickers_combined_unique.csv and adds company_name, sector, industry information
from various free data sources.

Usage:
    python utils/ticker_enricher.py --input ./data/spy_tickers/tickers_combined_unique.csv --output ./data/spy_tickers/tickers_enriched.csv
"""

import csv
import time
import json
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import requests
from datetime import datetime
import re

# Try to import optional dependencies
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    print("yfinance not installed. Install with: pip install yfinance")

try:
    from sec_cik_mapper import StockMapper
    SEC_MAPPER_AVAILABLE = True
except ImportError:
    SEC_MAPPER_AVAILABLE = False
    print("sec-cik-mapper not installed. Install with: pip install sec-cik-mapper")

class TickerEnricher:
    def __init__(self, input_file: str, output_file: str, delay: float = 0.5,
                 alpha_vantage_key: str = None, sec_api_key: str = None):
        """
        Initialize the ticker enricher with multiple data sources.

        Args:
            input_file: Path to input CSV file with tickers
            output_file: Path to output CSV file
            delay: Delay between API calls (seconds)
            alpha_vantage_key: Alpha Vantage API key (optional)
            sec_api_key: SEC-API.io key (optional, get from https://sec-api.io)
        """
        self.input_file = Path(input_file)
        self.output_file = Path(output_file)
        self.delay = delay
        self.alpha_vantage_key = alpha_vantage_key
        self.sec_api_key = sec_api_key
        self.setup_logging()

        # Initialize data sources
        self.sec_mapper = None
        if SEC_MAPPER_AVAILABLE:
            try:
                self.logger.info("Initializing SEC CIK mapper...")
                self.sec_mapper = StockMapper()
                self.logger.info(f"SEC mapper loaded with {len(self.sec_mapper.ticker_to_company_name)} tickers")
            except Exception as e:
                self.logger.error(f"Failed to load SEC mapper: {e}")

        # Cache for API results
        self.cache: Dict[str, Dict] = {}
        self.load_cache()

        # Statistics
        self.stats = {
            'total': 0,
            'found': 0,
            'not_found': 0,
            'sources': {}
        }

    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('ticker_enrichment.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def load_cache(self):
        """Load existing enriched data as cache"""
        if self.output_file.exists():
            try:
                with open(self.output_file, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if 'ticker' in row:
                            self.cache[row['ticker']] = row
                self.logger.info(f"Loaded {len(self.cache)} cached entries")
            except Exception as e:
                self.logger.warning(f"Could not load cache: {e}")

    def read_tickers(self) -> List[str]:
        """Read tickers from input file and clean them"""
        tickers = []
        try:
            with open(self.input_file, 'r') as f:
                reader = csv.reader(f)
                header = next(reader)  # Skip header
                for row in reader:
                    if row and row[0].strip():
                        raw_ticker = row[0].strip()
                        # Clean up ticker - remove date suffixes like -201312
                        clean_ticker = re.sub(r'-\d{4,6}', '', raw_ticker)
                        tickers.append({
                            'raw': raw_ticker,
                            'clean': clean_ticker
                        })
            self.logger.info(f"Read {len(tickers)} tickers from {self.input_file}")
            return tickers
        except Exception as e:
            self.logger.error(f"Error reading tickers: {e}")
            return []

    def get_info_from_sec_mapper(self, ticker: str) -> Optional[Dict]:
        """Get company info from SEC CIK mapper (best for company names)"""
        if not self.sec_mapper:
            return None

        try:
            # Clean ticker for SEC lookup (BRK.B -> BRK-B)
            clean_ticker = ticker.replace('.', '-')

            if clean_ticker in self.sec_mapper.ticker_to_company_name:
                company_name = self.sec_mapper.ticker_to_company_name[clean_ticker]
                exchange = self.sec_mapper.ticker_to_exchange.get(clean_ticker, '')
                cik = self.sec_mapper.ticker_to_cik.get(clean_ticker, '')

                return {
                    'company_name': company_name,
                    'exchange': exchange,
                    'cik': cik,
                    'sector': '',  # SEC data doesn't include sector/industry
                    'industry': '',
                    'source': 'SEC Mapper',
                    'confidence': 'high'
                }
        except Exception as e:
            self.logger.debug(f"SEC mapper lookup failed for {ticker}: {e}")

        return None

    def get_info_from_yfinance(self, ticker: str) -> Optional[Dict]:
        """Get company info from Yahoo Finance (good for sectors/industries)"""
        if not YFINANCE_AVAILABLE:
            return None

        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            if info and info.get('longName'):
                return {
                    'company_name': info.get('longName', '') or info.get('shortName', ''),
                    'sector': info.get('sector', ''),
                    'industry': info.get('industry', ''),
                    'exchange': info.get('exchange', ''),
                    'market_cap': info.get('marketCap', ''),
                    'website': info.get('website', ''),
                    'cik': info.get('cik', ''),
                    'source': 'Yahoo Finance',
                    'confidence': 'high' if info.get('sector') else 'medium'
                }
        except Exception as e:
            self.logger.debug(f"Yahoo Finance lookup failed for {ticker}: {e}")

        return None

    def get_info_from_sec_api(self, ticker: str) -> Optional[Dict]:
        """
        Get company info from SEC-API.io Mapping API
        Excellent for historical/delisted companies - covers 20+ years of data [citation:8]
        """
        if not self.sec_api_key:
            return None

        try:
            url = f"https://api.sec-api.io/mapping/ticker/{ticker}"
            params = {'token': self.sec_api_key}

            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    company = data[0]  # Take first match
                    return {
                        'company_name': company.get('name', ''),
                        'sector': company.get('sector', '') or company.get('sicSector', ''),
                        'industry': company.get('industry', '') or company.get('sicIndustry', ''),
                        'exchange': company.get('exchange', ''),
                        'cik': company.get('cik', ''),
                        'cusip': company.get('cusip', ''),
                        'is_delisted': company.get('isDelisted', False),
                        'sic': company.get('sic', ''),
                        'location': company.get('location', ''),
                        'source': 'SEC-API.io',
                        'confidence': 'high'
                    }
        except Exception as e:
            self.logger.debug(f"SEC-API lookup failed for {ticker}: {e}")

        return None

    def get_info_from_alpha_vantage(self, ticker: str) -> Optional[Dict]:
        """Get company info from Alpha Vantage API [citation:5][citation:10]"""
        if not self.alpha_vantage_key:
            return None

        try:
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'OVERVIEW',
                'symbol': ticker,
                'apikey': self.alpha_vantage_key
            }

            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if data and 'Name' in data and data['Name']:
                    return {
                        'company_name': data.get('Name', ''),
                        'sector': data.get('Sector', ''),
                        'industry': data.get('Industry', ''),
                        'exchange': data.get('Exchange', ''),
                        'market_cap': data.get('MarketCapitalization', ''),
                        'website': data.get('Website', ''),
                        'cik': data.get('CIK', ''),
                        'source': 'Alpha Vantage',
                        'confidence': 'high'
                    }
        except Exception as e:
            self.logger.debug(f"Alpha Vantage lookup failed for {ticker}: {e}")

        return None

    def get_info_from_fmp(self, ticker: str) -> Optional[Dict]:
        """
        Get company info from Financial Modeling Prep (free tier available) [citation:6]
        Free tier: 250 requests/day, no credit card required
        """
        try:
            # Using free public endpoint (no API key required for basic company info)
            url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}"
            params = {'apikey': 'demo'}  # Demo key works for limited requests

            response = requests.get(url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    company = data[0]
                    return {
                        'company_name': company.get('companyName', ''),
                        'sector': company.get('sector', ''),
                        'industry': company.get('industry', ''),
                        'exchange': company.get('exchange', ''),
                        'market_cap': company.get('mktCap', ''),
                        'website': company.get('website', ''),
                        'cik': company.get('cik', ''),
                        'isin': company.get('isin', ''),
                        'source': 'Financial Modeling Prep',
                        'confidence': 'high'
                    }
        except Exception as e:
            self.logger.debug(f"FMP lookup failed for {ticker}: {e}")

        return None

    def get_info_from_sec_edgar(self, ticker: str) -> Optional[Dict]:
        """
        Direct SEC EDGAR lookup - official government source [citation:1][citation:7]
        Uses public API, no key required
        """
        try:
            # First try to get CIK from company name search
            url = "https://efts.sec.gov/LATEST/search-index"
            params = {
                'q': ticker,
                'forms': '10-K',  # Limit to annual reports
                'page': 1,
                'from': 0,
                'size': 1
            }
            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; TickerEnrichment/1.0; +http://yourdomain.com)'
            }

            response = requests.get(url, params=params, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                hits = data.get('hits', {}).get('hits', [])

                if hits:
                    hit = hits[0]
                    source = hit.get('_source', {})
                    cik = source.get('cik')

                    if cik:
                        # Get company details from CIK
                        return {
                            'company_name': source.get('entity_name', ''),
                            'cik': cik,
                            'ticker': source.get('tickers', [''])[0],
                            'exchange': source.get('exchange', ''),
                            'source': 'SEC EDGAR',
                            'confidence': 'medium'
                        }
        except Exception as e:
            self.logger.debug(f"SEC EDGAR lookup failed for {ticker}: {e}")

        return None

    def get_info_from_sec_bulk(self, ticker: str) -> Optional[Dict]:
        """
        Use SEC bulk mapping files for historical tickers [citation:8]
        This is a fallback for when other APIs fail
        """
        # This would load pre-downloaded bulk mapping files
        # For now, we'll return None and let other sources handle it
        return None

    def enrich_ticker(self, ticker_data: Dict) -> Dict:
        """
        Enrich a single ticker with company information from multiple sources
        Tries sources in order of reliability and completeness
        """
        raw_ticker = ticker_data['raw']
        clean_ticker = ticker_data['clean']

        # Check cache first
        if raw_ticker in self.cache:
            self.logger.debug(f"Using cached data for {raw_ticker}")
            return self.cache[raw_ticker]

        result = {
            'ticker': raw_ticker,
            'clean_ticker': clean_ticker,
            'company_name': '',
            'sector': '',
            'industry': '',
            'exchange': '',
            'market_cap': '',
            'website': '',
            'cik': '',
            'is_delisted': '',
            'source': '',
            'confidence': '',
            'error': ''
        }

        # Try sources in order of preference
        sources = [
            ('SEC Mapper', self.get_info_from_sec_mapper, 1.0),
            ('Yahoo Finance', self.get_info_from_yfinance, 0.8),
            ('SEC-API.io', self.get_info_from_sec_api, 0.9),  # Good for historical [citation:8]
            ('Financial Modeling Prep', self.get_info_from_fmp, 0.7),  # Free tier [citation:6]
            ('Alpha Vantage', self.get_info_from_alpha_vantage, 0.7),
            ('SEC EDGAR', self.get_info_from_sec_edgar, 0.5),  # Official source [citation:1]
        ]

        for source_name, source_func, confidence_weight in sources:
            try:
                # Try both clean and raw ticker
                info = source_func(clean_ticker)
                if not info:
                    info = source_func(raw_ticker)

                if info:
                    result.update(info)
                    result['source'] = source_name

                    # Track statistics
                    self.stats['sources'][source_name] = self.stats['sources'].get(source_name, 0) + 1

                    self.logger.info(f"Found data for {raw_ticker} from {source_name}")
                    break
            except Exception as e:
                self.logger.debug(f"{source_name} failed for {raw_ticker}: {e}")

            # Rate limiting
            time.sleep(self.delay)

        if not result['source']:
            result['error'] = 'No data found from any source'
            self.logger.warning(f"No data found for {raw_ticker}")
            self.stats['not_found'] += 1
        else:
            self.stats['found'] += 1

        self.stats['total'] += 1
        return result

    def process_all_tickers(self):
        """Process all tickers and save enriched data"""
        tickers = self.read_tickers()
        if not tickers:
            self.logger.error("No tickers to process")
            return

        self.logger.info(f"Processing {len(tickers)} tickers...")

        results = []

        for i, ticker_data in enumerate(tickers, 1):
            self.logger.info(f"Processing {i}/{len(tickers)}: {ticker_data['raw']}")

            result = self.enrich_ticker(ticker_data)
            results.append(result)

            # Save progress periodically
            if i % 50 == 0:
                self.save_results(results)
                self.logger.info(f"Progress saved: {i}/{len(tickers)} processed")
                self.print_progress_stats()

        # Final save
        self.save_results(results)
        self.print_final_stats()

    def save_results(self, results: List[Dict]):
        """Save results to CSV file"""
        fieldnames = [
            'ticker', 'clean_ticker', 'company_name', 'sector', 'industry',
            'exchange', 'market_cap', 'website', 'cik', 'is_delisted',
            'source', 'confidence', 'error'
        ]

        # Create output directory if needed
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            self.logger.info(f"Saved {len(results)} records to {self.output_file}")
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")

    def print_progress_stats(self):
        """Print current statistics"""
        if self.stats['total'] > 0:
            success_rate = (self.stats['found'] / self.stats['total']) * 100
            self.logger.info(f"Progress stats: Found: {self.stats['found']}, "
                           f"Not found: {self.stats['not_found']}, "
                           f"Success rate: {success_rate:.1f}%")

    def print_final_stats(self):
        """Print final statistics"""
        self.logger.info("=" * 60)
        self.logger.info("ENRICHMENT SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Total tickers processed: {self.stats['total']}")
        self.logger.info(f"Successfully enriched: {self.stats['found']}")
        self.logger.info(f"Failed: {self.stats['not_found']}")
        self.logger.info(f"Overall success rate: {(self.stats['found']/self.stats['total'])*100:.1f}%")

        self.logger.info("\nSources used:")
        for source, count in sorted(self.stats['sources'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / self.stats['found']) * 100 if self.stats['found'] > 0 else 0
            self.logger.info(f"  {source}: {count} ({percentage:.1f}%)")

        self.logger.info(f"\nOutput saved to: {self.output_file}")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Enrich ticker symbols with company information from multiple sources'
    )

    parser.add_argument(
        '--input',
        type=str,
        required=True,
        help='Input CSV file with tickers (tickers_combined_unique.csv)'
    )

    parser.add_argument(
        '--output',
        type=str,
        required=True,
        help='Output CSV file for enriched data'
    )

    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help='Delay between API calls in seconds (default: 0.5)'
    )

    parser.add_argument(
        '--alpha-vantage-key',
        type=str,
        help='Alpha Vantage API key (get from https://www.alphavantage.co/support/#api-key)'
    )

    parser.add_argument(
        '--sec-api-key',
        type=str,
        help='SEC-API.io key for historical/delisted companies (get from https://sec-api.io)'
    )

    parser.add_argument(
        '--use-demo-keys',
        action='store_true',
        help='Use demo API keys for testing (limited requests)'
    )

    return parser.parse_args()

def main():
    """Main execution function"""
    args = parse_arguments()

    print("=" * 60)
    print("ENHANCED TICKER ENRICHMENT SCRIPT")
    print("=" * 60)
    print(f"Input file: {args.input}")
    print(f"Output file: {args.output}")
    print(f"API delay: {args.delay} seconds")
    print("\nData sources (in order of preference):")
    print("  1. SEC CIK Mapper - Company names, exchanges [citation:8]")
    print("  2. Yahoo Finance - Sectors, industries, market data [citation:4]")
    print("  3. SEC-API.io - Historical/delisted companies (20+ years) [citation:8]")
    print("  4. Financial Modeling Prep - Free tier available [citation:6]")
    print("  5. Alpha Vantage - Comprehensive financial data [citation:5][citation:10]")
    print("  6. SEC EDGAR - Official government source [citation:1][citation:7]")
    print()

    # Check for API keys
    alpha_vantage_key = args.alpha_vantage_key
    sec_api_key = args.sec_api_key

    if args.use_demo_keys:
        alpha_vantage_key = 'demo'
        sec_api_key = 'demo'
        print("Using demo API keys (limited requests)")

    if not alpha_vantage_key:
        print("NOTE: No Alpha Vantage key provided. Get a free key at: https://www.alphavantage.co/support/#api-key")

    if not sec_api_key:
        print("NOTE: No SEC-API.io key provided. For historical/delisted companies, get a key at: https://sec-api.io")

    print()

    # Check if we have any data sources available
    if not (SEC_MAPPER_AVAILABLE or YFINANCE_AVAILABLE):
        print("WARNING: No primary data sources available!")
        print("Please install at least one of:")
        print("  pip install sec-cik-mapper")
        print("  pip install yfinance")

    # Create enricher and process
    enricher = TickerEnricher(
        input_file=args.input,
        output_file=args.output,
        delay=args.delay,
        alpha_vantage_key=alpha_vantage_key,
        sec_api_key=sec_api_key
    )

    enricher.process_all_tickers()

if __name__ == "__main__":
    main()
