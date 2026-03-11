#!/usr/bin/env python

import argparse
import logging
from datetime import datetime, timedelta
from threading import Thread
from queue import Queue

import ibapi
from ibapi.common import TickerId
from ibapi.client import EClient
from ibapi.contract import Contract, ContractDetails
from ibapi.utils import iswrapper


#from ibapi.common import *
#from ibapi.order_condition import *
#from ibapi.contract import *
#from ibapi.order import *
#from ibapi.order_state import *
#from ibapi.ticktype import *
#from ibapi.account_summary_tags import *

def printWhenExecuting(fn):
    def fn2(self):
        print("   doing", fn.__name__)
        fn(self)
        print("   done w/", fn.__name__)

    return fn2

class ContractQuery(EClient, ibapi.wrapper.EWrapper):
    def __init__(self, contract: Contract, args: argparse.Namespace):
        EClient.__init__(self, wrapper=self)
        ibapi.wrapper.EWrapper.__init__(self)
        self.contract = contract
        self.request_id = 0
        self.started = False
        self.next_valid_order_id = None
        self.requests = {}
        self.args = args
        self.queue = Queue()

    def send_done(self, code):
        logging.info("Sending code %s", code)
        self.queue.put(code)

    def wait_done(self):
        logging.info("Waiting for thread to finish ...")
        code = self.queue.get()
        logging.info("Received code %s", code)
        self.queue.task_done()
        return code

    def next_request_id(self, contract: Contract) -> int:
        self.request_id += 1
        self.requests[self.request_id] = contract
        return self.request_id

    @iswrapper
    def connectAck(self):
        logging.info("Connected")

    @iswrapper
    def nextValidId(self, order_id: int):
        super().nextValidId(order_id)

        self.next_valid_order_id = order_id
        logging.info("nextValidId: %s", order_id)
        # we can start now
        self.start()

    @iswrapper
    def contractDetails(self, reqId: int, cd: ContractDetails) -> None:
        super().contractDetails(reqId, cd)
        logging.debug("ContractDetails for %s: %s", reqId, cd)

        print(f"{cd.contract.secType}:{cd.contract.symbol} Currency: {cd.contract.currency}")
        print(f"CUSIP: {cd.cusip}")
        print(f"Primary Exchange: {cd.contract.primaryExchange} {cd.contract.description}")
        print(f"Details for {cd.marketName} - {cd.longName}")
        print(f"Industry: {cd.industry}  Category: {cd.category}  Subcategory: {cd.subcategory}")
        print(f"OrderTypes: {cd.orderTypes}")
        print(f"ValidExchanges: {cd.validExchanges}")
        print(f"TradingHours: {cd.tradingHours}")
        print(f"LiquidHours: {cd.liquidHours}")
        #import ipdb; ipdb.set_trace()
        print(f"localSymbol: {cd.contract.localSymbol}")
        #self.reqMktData(self.next_request_id(cd.contract), cd.contract.localSymbol, 123, False, False, [])
        if cd.contractMonth:
            print(f"ContractMonth: {cd.contractMonth}")
        if cd.realExpirationDate:
            print(f"RealExpirationDate: {cd.realExpirationDate}")


    @iswrapper
    def contractDetailsEnd(self, reqId: int) -> None:
        self.send_done(0)

    def start(self):
        if self.started:
            return

        self.started = True
        #self.reqContractDetails(self.next_request_id(self.contract), self.contract)
        #self.reqMktData(self.next_request_id(self.contract), self.contract.symbol, 225, False, False, [])
        self.tickDataOperations_req()

#    def start(self):
#        if self.started:
#            return
#
#        self.started = True
#        for contract in self.contracts:
#            self.reqContractDetails(self.next_request_id(contract), contract)
#            self.reqMktData(self.next_request_id(contract), contract.localSymbol, 123, False, False, [])
#            self.pending_ends.add(rid)
#            self.request_contracts[rid] = contract

    @printWhenExecuting
    def tickDataOperations_req(self):
        #self.reqMktData(self.next_request_id(self.contract), self.contract.symbol, 225, False, False, [])
        #self.reqMktData(self.next_request_id(self.contract), self.USOptionContract(), 225, False, False, [])
        self.reqMktData(self.next_request_id(self.contract), self.TestOptionContract2(), 225, False, False, [])

    @staticmethod
    def OptionWithLocalSymbol():
        #! [optcontract_localsymbol]
        contract = Contract()
        #Watch out for the spaces within the local symbol!
        contract.localSymbol = "C DBK  DEC 20  1600"
        contract.secType = "OPT"
        contract.exchange = "DTB"
        contract.currency = "EUR"
        #! [optcontract_localsymbol]
        return contract

    @staticmethod
    def USOptionContract():
        #! [optcontract_us]
        contract = Contract()
        contract.symbol = "GOOG"
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = "20230602"
        contract.strike = 105
        contract.right = "C"
        contract.multiplier = "100"
        #! [optcontract_us]
        return contract

    @staticmethod
    def TestOptionContract1():
        #! [optcontract_us]
        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = "202305"
        contract.strike = 105
        contract.right = "C"
        contract.multiplier = "100"
        #! [optcontract_us]
        return contract

    @staticmethod
    def TestOptionContract2():
        #! [optcontract_localsymbol]
        contract = Contract()
        #Watch out for the spaces within the local symbol!
        #contract.localSymbol = "AAPL  230505P00192500"
        contract.localSymbol = "AAPL  230505C00050000"
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        #! [optcontract_localsymbol]
        return contract



    @iswrapper
    def error(self, req_id: TickerId, error_code: int, error: str, advancedOrderRejectJson: str=""):
        super().error(req_id, error_code, error)
        if req_id < 0:
            # we get error logs that really are just info
            logging.debug("Error. Id: %s Code %s Msg: %s", req_id, error_code, error)
        else:
            logging.error("Error. Id: %s Code %s Msg: %s", req_id, error_code, error)
            if error_code == 162: # no data returned, keep going
                self.handle_end(req_id, self.next_start_time())
            else:
                # we will always exit on error since data will need to be validated
                self.done = True


def make_contract(symbol: str, sec_type: str, currency: str, exchange: str, localsymbol: str) -> Contract:
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    if localsymbol:
        contract.localSymbol = localsymbol
    return contract


def main():

    argp = argparse.ArgumentParser(prog="ContractQuery",
                                   description="""
    Queries contracts and displays them for Interactive Brokers. Using TWS API, will invoke
    the contractDetails function and print formatted data returned
    """,
                                   epilog="""
    Examples:
    Get the details for the QQQ ETF
        ./query_contracts.py --security-type STK --exchange NASDAQ --symbol QQQ

    """,
                                   formatter_class=argparse.RawDescriptionHelpFormatter)
    argp.add_argument("--symbol", type=str, default="")
    argp.add_argument(
        "-d", "--debug", action="store_true", help="turn on debug logging"
    )
    argp.add_argument(
        "--logfile", help="log to file"
    )
    argp.add_argument(
        "-p", "--port", type=int, default=7497, help="local port for TWS connection"
    )
    argp.add_argument(
        "--currency", type=str, default="USD", help="currency for symbols"
    )
    argp.add_argument(
        "--exchange", type=str, default="SMART", help="exchange for symbols"
    )
    argp.add_argument(
        "--localsymbol", type=str, default="", help="local symbol (for futures)"
    )
    argp.add_argument(
        "--security-type", type=str, default="STK", help="security type for symbols"
    )
    args = argp.parse_args()

    logargs = dict(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                   datefmt='%H:%M:%S')
    if args.debug:
        logargs['level'] = logging.DEBUG
    else:
        logargs['level'] = logging.INFO

    if args.logfile:
        logargs['filemode'] = 'a'
        logargs['filename'] = args.logfile

    logging.basicConfig(**logargs)

    logging.debug(f"args={args}")
    contract = make_contract(args.symbol, args.security_type, args.currency, args.exchange, args.localsymbol)
    app = ContractQuery(contract, args)
    app.connect("127.0.0.1", args.port, clientId=123)
    Thread(target=app.run).start()

    code = app.wait_done()
    app.disconnect()

    return code


if __name__ == "__main__":
    main()
