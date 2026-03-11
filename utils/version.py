from ibapi.client import EClient
from ibapi.wrapper import EWrapper

def get_version(host="127.0.0.1", port=7497, client_id=123):
    class MyWrapper(EWrapper):
        pass

    wrapper = MyWrapper()
    app = EClient(wrapper)
    app.connect(host, port, clientId=client_id)
    print( app.serverVersion() )


if __name__ == "__main__":
    get_version()
