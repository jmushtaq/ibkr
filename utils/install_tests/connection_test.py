from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time

class TestConnection(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    def nextValidId(self, orderId):
        print(f"Connection Ready. Next Order ID: {orderId}")
        # Request details for Apple stock
        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        self.reqContractDetails(reqId=1001, contract=contract)

    def contractDetails(self, reqId, contractDetails):
        print(f"Contract Details Received: {contractDetails.summary}")

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson="", errorTime=""):
        # Filter out the noisy status notifications if you want a cleaner console
        if errorCode not in [2104, 2106, 2158]:
            print(f"Error: {reqId}, {errorCode}, {errorString}")


app = TestConnection()
app.connect("127.0.0.1", 7497, 123)
app.run()

