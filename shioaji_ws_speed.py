from my_logger import my_logger, StreamToLogger

import sys
import logging
import json
import time
import statistics
from datetime import datetime
import shioaji as sj

class sino_ws_speed_tester():
    def __init__(self):
        self.done_flag = False

        self.tester_logger = my_logger(file_name="sino_ws", logger_name="sino_ws")
        self.logger = self.tester_logger.logger
        sys.stdout = StreamToLogger(self.logger, logging.INFO)
        sys.stderr = StreamToLogger(self.logger, logging.ERROR)

        self.logger.info(f"Current SDK Version: {sj.__version__}")

        self.api = sj.Shioaji()
        self.api.login(
            api_key="7cjHv9s9RvhJUqtbSXac6s67J6be9tx5hbvH1fFvpHR8", 
            secret_key="BKwoXhpGwgxxfmUay1cFLg3qmAqHqUoxvATJHMphutSP",
            contracts_cb=lambda security_type: print(f"{repr(security_type)} fetch done.")
        )

        self.logger.info(f"Login Success")
        self.subscribed_ids = {}
        self.latency_keeper = {}

        self.logger.info(f"WebSocket Start")
        self.api.quote.set_on_tick_stk_v1_callback(self.handle_message)
    
    def ws_subscribe(self, symbol):
        self.api.quote.subscribe(
            self.api.Contracts.Stocks[symbol], 
            quote_type = sj.constant.QuoteType.Tick,
            version = sj.constant.QuoteVersion.v1
        )

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

    def handle_message(self, exchange, tick):
        recived_time = datetime.now()
        # self.logger.debug(f"Exchange: {exchange}, Tick: {tick}")
        symbol = tick.code
        price = tick.close
        volume = tick.total_volume
        tick_time = tick.datetime
        time_diff = recived_time-tick_time
        latency = time_diff.total_seconds()*1000
        latency = round(latency, 2)
        self.logger.info(f"symbol: {symbol}, p: {price}, v: {volume}, Recived Time: {recived_time}, Tick Time: {tick_time}, Latency: {latency}ms")

        if symbol not in self.latency_keeper:
            self.latency_keeper[symbol] = []

        self.latency_keeper[symbol].append(latency)
    
    def close_trader(self):
        self.logger.info(f"Close signal recived, closing....")
        self.done_flag = True
        logout_res = self.api.logout()
        if logout_res:
            self.logger.info(f"Successfully Log Out!")
        else:
            self.logger.error(f"Something Went Wrong when Log Out")

if __name__=="__main__":
    speed_tester = sino_ws_speed_tester()
    speed_tester.logger.info("Init Finish")
    speed_tester.ws_subscribe('2330')
    speed_tester.ws_subscribe('0050')
    speed_tester.ws_subscribe('2498')
    speed_tester.ws_subscribe('3231')
    speed_tester.ws_subscribe('00937B')
    speed_tester.ws_subscribe('9105')
    speed_tester.ws_subscribe('00637L')
    speed_tester.ws_subscribe('2888')
    speed_tester.ws_subscribe('6558')
    speed_tester.ws_subscribe('3645')
    speed_tester.ws_subscribe('4931')
    speed_tester.ws_subscribe('00929')
    while not speed_tester.done_flag:
        user_input = input()
        if user_input == 'done':
            speed_tester.close_trader()
        elif user_input == 'avg':
            speed_tester.latency_statistics_cal()