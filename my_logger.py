import os
import logging
from datetime import datetime

class my_logger():
    def __init__(self,
                 log_path='./log',
                 file_name='my_app',
                 logger_name="my_logger",
                 logger_level=logging.DEBUG,
                 file_handler_level=logging.DEBUG,
                 console_handler_level=logging.DEBUG,
                 log_signal=None,
                 gui_handler_level=logging.INFO):
        
        # create logger
        log_formatter = logging.Formatter("[%(asctime)s.%(msecs)03d][%(threadName)s][%(levelname)s]: %(message)s", datefmt = '%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logger_level)

        log_path = log_path
        file_name = file_name

        os.makedirs(log_path, exist_ok=True)

        self.today_date = datetime.today()
        self.today_str = datetime.strftime(self.today_date, "%Y%m%d")
        file_handler = logging.FileHandler("{0}/{1}.log.{2}".format(log_path, file_name, self.today_str), 'a', 'utf-8')
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(file_handler_level)
        self.logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(console_handler_level)
        self.logger.addHandler(console_handler)

        if log_signal:
            gui_log_formatter = logging.Formatter("[%(asctime)s.%(msecs)03d][%(levelname)s]: %(message)s", datefmt = '%Y-%m-%d %H:%M:%S')
            gui_handler = logger_with_pyside6(log_signal)
            gui_handler.setFormatter(gui_log_formatter)
            gui_handler.setLevel(gui_handler_level)
            self.logger.addHandler(gui_handler)


# 自定義 Log Handler，將 log 發送到 GUI
class logger_with_pyside6(logging.Handler):
    def __init__(self, log_signal):
        super().__init__()
        self.log_signal = log_signal

    def emit(self, record):
        # 格式化 log 訊息
        log_message = self.format(record)
        # 在 GUI 中追加 log 訊息
        self.log_signal.emit(log_message)

# 將 stdout 和 stderr 重定向到 logging
class StreamToLogger:
    def __init__(self, logger, log_level):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        pass