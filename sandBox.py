import ccxt
from datetime import datetime
import time
import threading
import api_keys
import random
from itertools import combinations

#logger modules
import logging
from logging.handlers import RotatingFileHandler




kraken = ccxt.kraken()

kraken.load_markets()

pass