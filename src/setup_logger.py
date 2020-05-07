import logging
from logging.handlers import RotatingFileHandler

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def setup_logger(name, log_file, level=logging.DEBUG):
    """
        To setup as many loggers as you want
    """
    handler = logging.FileHandler(log_file, mode='a')
    # rotate into 10 files when file reaches 50MB
    handler = RotatingFileHandler(log_file, maxBytes=50*1024*1024, backupCount=50)
    handler.setFormatter(formatter)
    handler.doRollover()

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

logger = setup_logger('first_logger', 'logs/logger.log')
opp_logger = setup_logger('second_logger', 'logs/opport.csv')
balance_csv_logger = setup_logger('third_logger', 'logs/balances.csv')
balance_logger = setup_logger('fourth_logger', 'logs/balances.log')