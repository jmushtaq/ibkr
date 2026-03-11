# Usage

. venv/bin/activate
1. python utils/wrighter/download_bars.py --port 7497 --size "1 day" --start-date 20230208 --end-date 20230209 AAPL 
2. python utils/wrighter/get_ticks.py --port 7497 AAPL
3. python utils/wrighter/query_contracts.py --security-type STK --exchange NASDAQ --symbol QQQ (?)

