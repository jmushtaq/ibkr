from ibapi.client import EClient
from ibapi.wrapper import EWrapper


class MyWrapper(EWrapper):

    def nextValidId(self, orderId:int):
        print("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId
        # start program here or use threading
        #app.reqContractDetails(4444, contract)

    def _contractDetails(self, reqId, contractDetails):
        print(reqId, contractDetails.contract)# my version doesnt use summary

    def contractDetails(self, reqId, cd):
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
        if cd.contractMonth:
            print(f"ContractMonth: {cd.contractMonth}")
        if cd.realExpirationDate:
            print(f"RealExpirationDate: {cd.realExpirationDate}")

    def contractDetailsEnd(self, reqId):
        print("ContractDetailsEnd. ", reqId)
        # this is the logical end of your program
        app.disconnect() # delete if threading and you want to stay connected

    def error(self, reqId, errorCode, errorString):
        print("Error. Id: " , reqId, " Code: " , errorCode , " Msg: " , errorString)

from ibapi.contract import Contract
def main():
    wrapper = MyWrapper()
    app = EClient(wrapper)
    app.connect("127.0.0.1", 7497, clientId=123)
    print("serverVersion:%s connectionTime:%s" % (app.serverVersion(), app.twsConnectionTime()))

    contract = Contract()
    contract.symbol = "XAUUSD"
    contract.secType = "CMDTY"
    contract.exchange = "SMART"
    contract.currency = "USD"

    app.run() # delete this line if threading


if __name__ == "__main__":
    main()
