from my_logger import my_logger

import json
import time
import statistics
from datetime import datetime
import shioaji as sj

class fubon_ws_speed_tester():
    def __init__(self):
        self.tester_logger = my_logger(file_name="fb_ws")
        self.logger = self.tester_logger.logger
        self.logger.info(f"Current SDK Version: {sj.__version__}")

        self.api = sj.Shioaji()
        self.api.login(
            api_key="7cjHv9s9RvhJUqtbSXac6s67J6be9tx5hbvH1fFvpHR8", 
            secret_key="BKwoXhpGwgxxfmUay1cFLg3qmAqHqUoxvATJHMphutSP",
            contracts_cb=lambda security_type: print(f"{repr(security_type)} fetch done.")
        )

        self.reststock = self.sdk.marketdata.rest_client.stock
        self.ws_stock = self.sdk.marketdata.websocket_client.stock

        self.manully_logout = False
        self.subscribed_ids = {}
        self.latency_keeper = {}

        self.api.quote.set_on_tick_stk_v1_callback(self.handle_message)

        self.ws_stock.on('message', self.handle_message)
        self.ws_stock.on('connect', self.handle_connect)
        self.ws_stock.on('disconnect', self.handle_disconnect)
        self.ws_stock.on('error', self.handle_error)
        self.ws_stock.connect()
    
    def ws_subscribe(self, symbol):
        self.api.quote.subscribe(
            self.api.Contracts.Stocks[symbol], 
            quote_type = sj.constant.QuoteType.Tick,
            version = sj.constant.QuoteVersion.v1
        )

    def latency_statistics_cal(self):
        for symbol in  list(self.latency_keeper.keys()):
            self.logger.info(f"symbol:{symbol}, 
                               num:{len(self.latency_keeper[symbol])}, 
                               max:{max(self.latency_keeper[symbol])}, 
                               min:{min(self.latency_keeper[symbol])}, 
                               mean:{statistics.mean(self.latency_keeper[symbol])},
                               median:{statistics.median(self.latency_keeper[symbol])},
                               std:{statistics.stdev(self.latency_keeper[symbol])}")

    def handle_message(self, message):
        recived_time = time.time()
        msg = json.loads(message)
        event = msg["event"]
        data = msg["data"]
        # print(event, data)

         # subscribed事件處理
        if event == "subscribed":
            if type(data) == list:
                for subscribed_item in data:
                    sub_id = subscribed_item["id"]
                    symbol = subscribed_item["symbol"]
                    self.logger.info('訂閱成功...'+symbol)
                    self.subscribed_ids[symbol] = sub_id
                    self.latency_keeper[symbol] = []
            else:
                sub_id = data["id"]
                symbol = data["symbol"]
                self.logger.info('訂閱成功...'+symbol)
                self.subscribed_ids[symbol] = sub_id
                self.latency_keeper[symbol] = []
        
        elif event == "unsubscribed":
            for key, value in self.subscribed_ids.items():
                if value == data["id"]:
                    print(value)
                    remove_key = key
            self.subscribed_ids.pop(remove_key)
            self.logger.info(remove_key+"...成功移除訂閱")

        elif event == "data":
            # print(data)
            symbol = data['symbol']
            
            # print(recived_time)
            latency = (recived_time*1000000-data['time'])/1000.0
            self.logger.info(f"symbol: {symbol}, p: {data['price']}, v: {data['volume']}, Recived Time: {datetime.fromtimestamp(recived_time)}, Tick Time: {datetime.fromtimestamp(data['time']/1000000)}, Latency: {latency}ms")
            self.latency_keeper[symbol].append(latency)

    def handle_connect(self):
        self.logger.info('WS_whole market data connected')

    def handle_disconnect(self, code, message):
        self.logger.info(f"WS_whole Disconnect, code: {code}, msg: {message}")
        if self.manully_logout:
            pass
        else:
            self.sdk.init_realtime(self.whole_mode)
            self.reststock = self.sdk.marketdata.rest_client.stock
            self.ws_whole = self.sdk.marketdata.websocket_client.stock

            self.ws_whole.on('message', self.handle_message_whole)
            self.ws_whole.on('connect', self.handle_connect_whole)
            self.ws_whole.on('disconnect', self.handle_disconnect_whole)
            self.ws_whole.on('error', self.handle_error_whole)
            self.ws_whole.connect()

            self.ws_whole.subscribe({
                'channel':'aggregates',
                'symbols': list(self.subscribed_ids.keys())
            })

    def handle_error(self, error):
        self.logger.error(f'WS_whole data error: {error}')

if __name__=="__main__":
    speed_tester = fubon_ws_speed_tester()
    speed_tester.ws_subscribe('2330')
    speed_tester.ws_subscribe('0050')