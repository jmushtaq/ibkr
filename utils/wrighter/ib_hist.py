from ib.ext.Contract import Contract
from ib.ext.ContractDetails import ContractDetails
from ib.opt import ibConnection, message

import time
import datetime
from time import sleep, strftime

twsPort = 7497
gatewayPort = 4001

# https://stackoverflow.com/questions/50399889/getting-ib-historical-option-data

def watcher(msg):
    print("[watcher: ", msg, " ]")

def contractDetailsHandler(msg):
    print("[contractDetailsHandler:]")
    contractDetails = msg.contractDetails
    contract = msg.contractDetails.m_summary
    print(contractDetails.m_cusip, contractDetails.m_underConId, contractDetails.m_longName, contractDetails.m_industry,
          contractDetails.m_category, contractDetails.m_subcategory, contract.m_symbol, contract.m_secType,
          contract.m_strike, contract.m_right, contract.m_exchange,
          contract.m_currency, contract.m_secIdType, contract.m_secId, "\n")
    contracts.append(contractDetails.m_summary)

def contractDetailsEndHandler(msg):
    print("[contractDetailsEndHandler:]")


def contractHistDetailsHandler(msg):
    global DataWait
    print("[contractHistDetailsHandler:]")
    contracts.append(msg.historicalData)
    DataWait =  False


con = ibConnection()
con.host = "127.0.0.1"
#con.port = gatewayPort
con.port = twsPort
con.clientId = 5
con.registerAll(watcher)
con.register(contractDetailsHandler, 'ContractDetails')
con.register(contractDetailsEndHandler, 'ContractDetailsEnd')
con.register(contractHistDetailsHandler, message.historicalData)

con.connect()

contract = Contract()
contract.m_exchange     = "SMART"
contract.m_secType      = "OPT"
contract.m_symbol       = "AAPL"
contract.m_currency     = "USD"
contract.m_strike       = 260
contract.m_right        = "PUT"
#contract.m_expiry       = "20180615"
contract.m_expiry       = "20230315"
#contract.m_includeExpired = True

endtime = strftime('%Y%m%d %H:%M:%S')
#endtime = '20170102 01:00:00'

con.reqContractDetails(1, contract)

con.reqHistoricalData(2,contract,endtime,"14 D","30 min","MIDPOINT",0,1)

contracts = []

DataWait = True ;  i = 0
while DataWait and i < 90:
    i += 1 ; print(i),
    time.sleep(1)

time.sleep(1)

con.disconnect()
con.close()

print(contracts)
