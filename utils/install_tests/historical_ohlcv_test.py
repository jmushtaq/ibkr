from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time

class IBApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    def nextValidId(self, orderId):
        print("Connected. Requesting data...")
        contract = Contract()
        contract.symbol = "AAPL"
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        # Request data up to Feb 28, 2026
        self.reqHistoricalData(
            1, contract, "20260228-23:59:59", "56 D",
            "1 day", "TRADES", 1, 1, False, []
        )

    def historicalData(self, reqId, bar):
        print(f"Date: {bar.date} | Open: {bar.open} | Close: {bar.close} | Vol: {bar.volume}")

    def historicalDataEnd(self, reqId, start, end):
        self.disconnect()

# Execution
app = IBApp()
app.connect("127.0.0.1", 7497, clientId=1) # Ensure TWS/Gateway is open
threading.Thread(target=app.run, daemon=True).start()
time.sleep(3) # Wait for data

