# logger properties
import os
import logging
from logging.handlers import TimedRotatingFileHandler
current_directory = os.getcwd()
current_dir = os.path.basename(current_directory)

FORMAT = '[%(asctime)-15s][%(filename)s:%(lineno)d][%(levelname)s] %(message)s'
loggers = {}

LOGGER_PATH = './logger/'
if not os.path.exists(LOGGER_PATH):
    os.makedirs(LOGGER_PATH, exist_ok=True)

class SizeAndTimedRotatingFileHandler(TimedRotatingFileHandler):
    """_summary_

    Args:
        TimedRotatingFileHandler (_type_): _description_
    """
    def __init__(self, filename, when='midnight', interval=1, backup_count=7, max_bytes=5*1024*1024, encoding="utf-8", delay=False, utc=False, at_time=None):
        """_summary_

        Args:
            filename (_type_): _description_
            when (str, optional): _description_. Defaults to 'midnight'.
            interval (int, optional): _description_. Defaults to 1.
            backupCount (int, optional): _description_. Defaults to 7.
            maxBytes (_type_, optional): _description_. Defaults to 5*1024*1024.
            encoding (_type_, optional): _description_. Defaults to None.
            delay (bool, optional): _description_. Defaults to False.
            utc (bool, optional): _description_. Defaults to False.
            atTime (_type_, optional): _description_. Defaults to None.
        """
        # Gọi hàm khởi tạo của TimedRotatingFileHandler
        super().__init__(filename, when=when, interval=interval, backupCount=backup_count, encoding=encoding, delay=delay, utc=utc, atTime=at_time)
        # use snake_case to align with style checks
        self.max_bytes = max_bytes

    def shouldRollover(self, record):
        """_summary_

        Args:
            record (_type_): _description_

        Returns:
            _type_: _description_
        """
        
        # Kiểm tra điều kiện quay vòng theo thời gian
        
        if super().shouldRollover(record):
            return True
        
        # Kiểm tra điều kiện quay vòng theo kích thước
        if self.stream is None:  # Nếu chưa mở file
            self.stream = self._open()
        
        if os.stat(self.baseFilename).st_size >= self.max_bytes:
            return True

        return False


def setup_logger(name, log_file, level=logging.DEBUG):
    """
    This function sets up a logger with the given name and log file.

    Parameters
    ----------
    name : str
        The name of the logger.
    log_file : str
        The path to the log file.
    level : int
        The logging level.

    Returns
    -------
    logger: The logger object.
    """
    name = f"{current_dir}_{name}" 
    if loggers.get(name):
        return loggers.get(name)
   
    formatter = logging.Formatter(FORMAT)
    handler = SizeAndTimedRotatingFileHandler(
                        filename=log_file,
                        when="midnight",        # Xoay vòng theo ngày
                        max_bytes=100*1024*1024,  # Giới hạn kích thước file là 100 MB
                        backup_count=2           # Giữ lại 3 file log cũ
                    )
    handler.setFormatter(formatter)
    handler.setLevel(level)
    logger2 = logging.getLogger(name)
    logger2.setLevel(level)
    logger2.addHandler(handler)
    logger2.propagate = False
    loggers[name] = logger2
    # print(logger2)
    return logger2

def setup_logger_global(name, log_file, level=logging.DEBUG):
    """
    Setup a global logger with the given name and log file.

    Parameters:
        name (str): The name of the logger.
        log_file (str): The path to the log file.
        level (int, optional): The logging level. Defaults to logging.DEBUG.

    Returns:
        logger: The logger object.
    """
    return setup_logger(name, LOGGER_PATH +log_file, level)

logger_arb = setup_logger("arbitrage", './logger/poly_arbitrage.log')
logger_polymarket = setup_logger("polymarket", './logger/polymarket.log')