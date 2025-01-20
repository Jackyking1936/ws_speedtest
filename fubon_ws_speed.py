from my_logger import my_logger

import json
import time
import statistics
from datetime import datetime
import fubon_neo
from fubon_neo.sdk import FubonSDK, Mode, Order

class fubon_ws_speed_tester():
    def __init__(self):
        self.done_flag = False

        self.tester_logger = my_logger(file_name="fb_ws")
        self.logger = self.tester_logger.logger
        self.logger.info(f"Current SDK Version: {fubon_neo.__version__}")

        self.sdk = FubonSDK()
        self.SDK_login("./account.json")

        self.ws_mode = Mode.Speed
        self.sdk.init_realtime(self.ws_mode)

        self.reststock = self.sdk.marketdata.rest_client.stock
        self.ws_stock = self.sdk.marketdata.websocket_client.stock

        self.subscribed_ids = {}
        self.latency_keeper = {}

        self.ws_stock.on('message', self.handle_message)
        self.ws_stock.on('connect', self.handle_connect)
        self.ws_stock.on('disconnect', self.handle_disconnect)
        self.ws_stock.on('error', self.handle_error)
        self.ws_stock.connect()
    
    def ws_subscribe(self, symbol):
        self.ws_stock.subscribe({
            'symbol': symbol,
            'channel': "trades"
        })

    def latency_statistics_cal(self):
        for symbol in  list(self.latency_keeper.keys()):
            if len(self.latency_keeper[symbol]) > 1:
                self.logger.info(f"""symbol:{symbol}, 
                                num:{len(self.latency_keeper[symbol])}, 
                                max:{max(self.latency_keeper[symbol])}, 
                                min:{min(self.latency_keeper[symbol])}, 
                                mean:{statistics.mean(self.latency_keeper[symbol])}, 
                                median:{statistics.median(self.latency_keeper[symbol])}, 
                                std:{statistics.stdev(self.latency_keeper[symbol])}""")
            else:
                self.logger.info(f"{symbol} length is not enough, length: {len(self.latency_keeper[symbol])}")

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
        self.logger.info('WS_stock market data connected')

    def handle_disconnect(self, code, message):
        self.logger.info(f"WS_stock Disconnect, code: {code}, msg: {message}")
        if self.done_flag:
            pass
        else:
            self.sdk.init_realtime(self.ws_mode)
            self.reststock = self.sdk.marketdata.rest_client.stock
            self.ws_stock = self.sdk.marketdata.websocket_client.stock

            self.ws_stock.on('message', self.handle_message_whole)
            self.ws_stock.on('connect', self.handle_connect_whole)
            self.ws_stock.on('disconnect', self.handle_disconnect_whole)
            self.ws_stock.on('error', self.handle_error_whole)
            self.ws_stock.connect()

            self.ws_stock.subscribe({
                'channel':'trades',
                'symbols': list(self.subscribed_ids.keys())
            })

    def handle_error(self, error):
        self.logger.error(f'WS_stock data error: {error}')

    def SDK_login(self, login_path):
        with open(login_path) as user_file:
            acc_json = json.load(user_file)

        if acc_json['cert_pwd']:
            accounts = self.sdk.login(acc_json['id'], acc_json['pwd'], acc_json['cert_path'], acc_json['cert_pwd'])
        else:
            accounts = self.sdk.login(acc_json['id'], acc_json['pwd'], acc_json['cert_path'])
        self.logger.info(str(accounts))

        for acc in accounts.data:
            if acc.account == acc_json['target_acc']:
                self.active_acc = acc
        self.logger.info("Current use: {}".format(self.active_acc))
    
    def close_trader(self):
        self.logger.info(f"Close signal recived, closing....")
        self.done_flag = True
        self.ws_stock.disconnect()
        logout_res = self.sdk.logout()
        if logout_res:
            self.logger.info(f"Successfully Log Out!")
        else:
            self.logger.error(f"Something Went Wrong when Log Out")

if __name__=="__main__":
    speed_tester = fubon_ws_speed_tester()
    speed_tester.ws_subscribe('2330')
    speed_tester.ws_subscribe('0050')
    while not speed_tester.done_flag:
        user_input = input()
        if user_input == 'done':
            speed_tester.close_trader()
        elif user_input == 'avg':
            speed_tester.latency_statistics_cal()