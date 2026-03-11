import os
import sys
from subprocess import Popen, PIPE
import argparse
from datetime import datetime

import pandas as pd


def main():
    '''
    # Run a single year
    python utils/wrighter/run_batch_download.py --restart True '2004-2004'

    # Run a range of years year
    python utils/wrighter/run_batch_download.py --restart True '2004-2020'

    # Check missing for a range of years year
    python utils/wrighter/run_batch_download.py --check_missing True '2004-2020'

    # Run source directly
    python utils/wrighter/download_bars.py --base-directory utils/ibkr/data --port 7497 --size "15 mins"  --restart True '2004-2020'



    '''
    argp = argparse.ArgumentParser(prog="TWSDownloadApp", formatter_class=argparse.RawDescriptionHelpFormatter)
#
    #argp.add_argument("--symbol", nargs="+")
    argp.add_argument("year", nargs="+")
    argp.add_argument("--symbol", type=str, default=None)
    argp.add_argument("--port", type=int, default=7497, help="bar size")
    argp.add_argument("--size", type=str, default="15 mins", help="bar size")
    argp.add_argument("--basedir", type=str, default="utils/ibkr/data", help="bar size")
    argp.add_argument("--startdate", type=str)
    argp.add_argument("--enddate", type=str)
    argp.add_argument("--restart", type=bool, default=False)
    argp.add_argument("--missing_symbol", type=bool, default=False)
    argp.add_argument("--check_missing", type=bool, default=False)

    args = argp.parse_args()
    now = datetime.now().strftime('%y%m%dT%H%M%S')

    import ipdb; ipdb.set_trace()
    years = range(int(args.year[0].split('-')[0]), int(args.year[0].split('-')[1])+1) if '-' in args.year[0] else int(args.year[0])
    #import ipdb; ipdb.set_trace()
    for count, year in enumerate(years, 1):
        size = args.size.replace(' ', '_')

        if year:
            startdate = f'{year}0103'
            enddate = f'{year}1231'
            print(f'Start Date: {startdate}')
            print(f'End Date: {enddate}')
        else:
            startdate = args.startdate
            enddate = args.enddate

        #df = pd.read_csv('/var/www/opstrat/data/spy_symbols_03Mar2023.csv')

        if args.symbol:
            spy_list = [args.symbol]
        elif args.restart or args.check_missing:
            # Get current completed files from SPY_year folder
            cur_symbols = [i.split('_')[0] for i in os.listdir(f'{args.basedir}/STK/{size}/SPY_{year}') if i.endswith('.csv')]
            spy_list = spy_symbols(year=year, src_dir='/var/www/opstrat/data')
            spy_list = list(set(spy_list).difference(cur_symbols))
            spy_list.sort()
            #import ipdb; ipdb.set_trace()
            print(spy_list)
            print(f'Current files: {len(cur_symbols)}')
            print('_'*125)
            if args.check_missing and count==len(years):
                sys.exit()
            if args.check_missing:
                continue
        else:
            # Get symols for given year from historical spy constituents file
            spy_list = spy_symbols(year=year, src_dir='/var/www/opstrat/data')

    #    idx = 0
    #    #import ipdb; ipdb.set_trace()
    #    if args.restart_symbol:
    #        idx=df.index[df['Ticker']==args.restart_symbol][0]
    #    spy_list = df[idx:].to_dict(orient='list')['Ticker']

        # Files to be stored in base_directory/<security_type>/<size>/<symbol>/
        log_path = f'{args.basedir}/STK/{size}'
        path = f'{args.basedir}/STK/{size}/SPY_{year}'
        #path = f'{args.basedir}/logs/'
        if not os.path.exists(path):
           os.makedirs(path)

        import ipdb; ipdb.set_trace()
        log_filename = f'{log_path}/stocks_{now}.log'
        error_log    = f'{log_path}/symbol_errors_{now}.log'
        success_log  = f'{log_path}/symbol_success_{now}.log'
        #error_log    = f'/var/www/opstrat/{args.basedir}/logs/stock_errors_{now}.log'
        #success_log  = f'/var/www/opstrat/{args.basedir}/logs/stock_success_{now}.log'
        print(f'Log filename: {log_filename}')
        with open(log_filename, "a") as f:
            #for symbol in args.symbol:
            for symbol in spy_list: #[:5]:
            #for symbol in ['AAPL']:
                command = f' \
                    cd /var/www/opstrat && . venv/bin/activate && \
                    python utils/wrighter/download_bars.py \
                        --base-directory {args.basedir} \
                        --port {args.port} \
                        --size "{args.size}" \
                        --start-date {startdate} \
                        --end-date {enddate} \
                        --error-log {error_log} \
                        --success-log {success_log} \
                        "{symbol}" \
                '
                process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
                stdout, stderr = process.communicate()
                #print(f'STDOUT: {symbol}\n{stdout.decode("utf-8")}')
                #print(f'STDERR:\n{stderr.decode("utf-8")}')

                #log_filename = f'{path}/{symbol}_{args.size.replace(" ","")}_{year}.log'
                #with open(log_filename, "a") as f:
                f.write(f'Symbol: {symbol}\n')
                f.write(stdout.decode("utf-8")+'\n\n')
                f.write(stderr.decode("utf-8")+'\n\n')
                f.write('-'*100 + '\n')


def spy_symbols(year, src_dir='/var/www/opstrat/data'):
    #import ipdb; ipdb.set_trace()
    # Get spy components for a given year --> as a list
    df = pd.read_csv(f'{src_dir}/SPY_Historical_Components_1996-2022.csv')
    df = df.set_index('date')
    df.index = pd.to_datetime(df.index)

    syms = [i.split(',') for i in df[df.index.year==year]['tickers'].tolist()]
    l = []
    for i in syms:
        l += i
    #import ipdb; ipdb.set_trace()
    symbols_year = pd.Series(l).drop_duplicates().tolist()

    return symbols_year


def spy_missing_symbols(year, basedir='utils/ibkr/data/STK/15_mins'):
    #import ipdb; ipdb.set_trace()
    # Get spy components for a given year --> as a list
    df = pd.read_csv('data/SPY_Historical_Components_1996-2022.csv')
    df = df.set_index('date')
    df.index = pd.to_datetime(df.index)

    syms = [i.split(',') for i in df[df.index.year==year]['tickers'].tolist()]
    l = []
    for i in syms:
        l += i
    symbols_year = pd.Series(l).drop_duplicates().tolist()

    # get current symbols already downloaded from SPY folder
    cur_symbols = [i.split('_')[0] for i in os.listdir(f'{basedir}/SPY_{year}') if i.endswith('.csv')]

    missing_symbols = list(set(cur_symbols).difference(symbols_year)) + list(set(symbols_year).difference(cur_symbols))

    return missing_symbols



if __name__ == "__main__":
    main()
