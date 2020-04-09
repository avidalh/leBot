import ccxt
from datetime import datetime
import time
import threading
import api_keys
import random
import logging
from logging.handlers import RotatingFileHandler

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

# parameters
DEBUG = True
DEBUG_LEVEL = 0  # noy used by now
USE_THREADING = True  
CROSSING_MARGIN = 1.05  # 5% above delta
TRADING_SIZE = 20  # $20
EXCH_REQUEST_DELAY = 1.8  # seconds, take care here: if rate overpassed yo could get penalized!
EXPLOIT_THREAD_DELAY = 15

# exploit_threads = list()
# exch_locked = list()


class GlobalStorage:
    def __init__(self):
        self.exploit_threads = list()
        self.exch_locked = list()
        self.timer = {}


g_storage = GlobalStorage()


def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""
    handler = logging.FileHandler(log_file)        
    handler = RotatingFileHandler(log_file, maxBytes=50*1024*1024, backupCount=10)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def create_exchanges():
    ''' instantiate and load the markets'''
    coinbasepro = ccxt.coinbasepro({
        'apiKey': api_keys.coinbasepro['apiKey'],
        'secret': api_keys.coinbasepro['secret'],
        })

    poloniex = ccxt.poloniex({
        'apiKey': api_keys.poloniex['apiKey'],
        'secret': api_keys.poloniex['secret'],
        })

    bittrex = ccxt.bittrex({
        'apiKey': api_keys.bittrex['apiKey'],
        'secret': api_keys.bittrex['secret'],
        })

    binance = ccxt.binance({
        'apiKey': api_keys.binance['apiKey'],
        'secret': api_keys.binance['secret'],
        })

    bitfinex = ccxt.bitfinex({
        'apiKey': api_keys.bitfinex['apiKey'],
        'secret': api_keys.bitfinex['secret'],
        })

    kraken = ccxt.kraken({
        'apiKey': api_keys.kraken['apiKey'],
        'secret': api_keys.kraken['secret'],
        })

    bitmex = ccxt.bitmex({
        'apiKey': api_keys.bitmex['apiKey'],
        'secret': api_keys.bitmex['secret'],
        })

    okex = ccxt.okex({
        'apiKey': api_keys.okex['apiKey'],
        'secret': api_keys.okex['secret'],
        })

    exchanges = [coinbasepro, poloniex, bittrex, binance, bitfinex, kraken, bitmex, okex]
    timing_limits = [    .35,      .35,       1,     .35,        2,      1,      1,  .35]  # requesting time limits per exchange

    for exchange, timing in zip(exchanges, timing_limits):
        g_storage.timer[exchange.name] = [0, timing]

    return exchanges


def load_markets_thread(exchange, force):
    exchange.load_markets(force)


def load_markets(exchanges, force=False):
    ''' load markets'''
    if not USE_THREADING:
        for exchange in exchanges:
            exchange.load_markets(force)
    else:
        threads = list()
        for exchange in exchanges:
            thread = threading.Thread(target=load_markets_thread, args=(exchange, force))
            threads.append(thread)
            thread.start()
        for thread, exchange in zip(threads, exchanges):
            exchange = thread.join()


def get_trading_pairs():  # TODO: Check the pairs per exchange
    symbols_matrix = {      #1          #2        #3          #4         #5         #6         #7         #7         #8         #9          #10          #11         #12         #13
        'coinbasepro': ['BTC/USDC', 'BTC/USD', 'ETH/USD', 'BCH/USD', 'ZEC/USD', 'LTC/USD', 'XRP/USD', 'ADA/BTC', 'ADA/ETH', 'ADA/USDT', 'ADA/USDC', 'IOTA/BTC', 'IOTA/ETH', 'IOTA/USDT'],
        'poloniex':    ['BTC/USDC', 'BTC/USD', 'ETH/USD', 'BCH/USD', 'ZEC/USD', 'LTC/USD', 'XRP/USD', 'ADA/BTC', 'ADA/ETH', 'ADA/USDT', 'ADA/USDC', 'IOTA/BTC', 'IOTA/ETH', 'IOTA/USDT'],
        'bittrex':     ['BTC/USDT', 'BTC/USD', 'ETH/USD', 'BCH/USD', 'ZEC/USD', 'LTC/USD', 'XRP/USD', 'ADA/BTC', 'ADA/ETH', 'ADA/USDT', 'ADA/USDC', 'IOTA/BTC', 'IOTA/ETH', 'IOTA/USDT'],
        'binance':     ['BTC/USDT', 'BTC/USD', 'ETH/USD', 'BCH/USD', 'ZEC/USD', 'LTC/USD', 'XRP/USD', 'ADA/BTC', 'ADA/ETH', 'ADA/USDT', 'ADA/USDC', 'IOTA/BTC', 'IOTA/ETH', 'IOTA/USDT'],
        'bitfinex':    ['BTC/USDT', 'BTC/USD', 'ETH/USD', 'BCH/USD', 'ZEC/USD', 'LTC/USD', 'XRP/USD', 'ADA/BTC', 'ADA/ETH', 'ADA/USDT', 'ADA/USDC', 'IOTA/BTC', 'IOTA/ETH', 'IOTA/USDT'],
        'kraken':      ['BTC/USDT', 'BTC/USD', 'ETH/USD', 'BCH/USD', 'ZEC/USD', 'LTC/USD', 'XRP/USD', 'ADA/BTC', 'ADA/ETH', 'ADA/USDT', 'ADA/USDC', 'IOTA/BTC', 'IOTA/ETH', 'IOTA/USDT'],
        'bitmex':      ['BTC/USDT', 'BTC/USD', 'ETH/USD', 'BCH/USD', 'ZEC/USD', 'LTC/USD', 'XRP/USD', 'ADA/BTC', 'ADA/ETH', 'ADA/USDT', 'ADA/USDC', 'IOTA/BTC', 'IOTA/ETH', 'IOTA/USDT'],
        'okex':        ['BTC/USDT', 'BTC/USD', 'ETH/USD', 'BCH/USD', 'ZEC/USD', 'LTC/USD', 'XRP/USD', 'ADA/BTC', 'ADA/ETH', 'ADA/USDT', 'ADA/USDC', 'IOTA/BTC', 'IOTA/ETH', 'IOTA/USDT']
    }
    symbols_matrix_OLD_TO_DELETE = [  #cb pro       poloniex    bittrex     binance     bitfinex    kraken      bitmex      okex 
                        ['BTC/USDC', 'BTC/USDC', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT'],
                        ['BTC/USD', 'BTC/USD', 'BTC/USD', 'BTC/USD', 'BTC/USD', 'BTC/USD', 'BTC/USD', 'BTC/USD'],

                        ['ETH/USD', 'ETH/USD', 'ETH/USD', 'ETH/USD', 'ETH/USD', 'ETH/USD', 'ETH/USD', 'ETH/USD'],
                        ['BCH/USD', 'BCH/USD', 'BCH/USD', 'BCH/USD', 'BCH/USD', 'BCH/USD', 'BCH/USD', 'BCH/USD'],

                        ['ZEC/USD', 'ZEC/USD', 'ZEC/USD', 'ZEC/USD', 'ZEC/USD', 'ZEC/USD', 'ZEC/USD', 'ZEC/USD'],
                        ['LTC/USD', 'LTC/USD', 'LTC/USD', 'LTC/USD', 'LTC/USD', 'LTC/USD', 'LTC/USD', 'LTC/USD'],

                        ['XRP/USD', 'XRP/USD', 'XRP/USD', 'XRP/USD', 'XRP/USD', 'XRP/USD', 'XRP/USD', 'XRP/USD'],

                        ['ADA/BTC', 'ADA/BTC', 'ADA/BTC', 'ADA/BTC', 'ADA/BTC', 'ADA/BTC', 'ADA/BTC', 'ADA/BTC'],
                        ['ADA/ETH', 'ADA/ETH', 'ADA/ETH', 'ADA/ETH', 'ADA/ETH', 'ADA/ETH', 'ADA/ETH', 'ADA/ETH'],
                        ['ADA/USDT', 'ADA/USDT', 'ADA/USDT', 'ADA/USDT', 'ADA/USDT', 'ADA/USDT', 'ADA/USDT', 'ADA/USDT'],
                        ['ADA/USDC', 'ADA/USDC', 'ADA/USDC', 'ADA/USDC', 'ADA/USDC', 'ADA/USDC', 'ADA/USDC', 'ADA/USDC'],
                        
                        ['IOTA/BTC', 'IOTA/BTC', 'IOTA/BTC', 'IOTA/BTC', 'IOTA/BTC', 'IOTA/BTC', 'IOTA/BTC', 'IOTA/BTC'],
                        ['IOTA/ETH', 'IOTA/ETH', 'IOTA/ETH', 'IOTA/ETH', 'IOTA/ETH', 'IOTA/ETH', 'IOTA/ETH', 'IOTA/ETH'],
                        ['IOTA/USDT', 'IOTA/USDT', 'IOTA/USDT', 'IOTA/USDT', 'IOTA/USDT', 'IOTA/USDT', 'IOTA/USDT', 'IOTA/USDT']
                    ]
    
    return symbols_matrix


def get_market_pairs(exchanges):
    pairs = list()
    for exchange in exchanges:
        pairs.append(exchange.markets.keys())
    return pairs


class Balance:
    ''' balance class '''    
    def __init__(self):
        self.exchanges = {}
    
    def set_balance(self, exchange: str, coin: str, balance: float, change: float):
        if exchange not in self.exchanges:
            self.exchanges.update({exchange: {coin: {'amount': balance, 'change': change}}})
        else:
            if coin not in self.exchanges[exchange]:
                self.exchanges[exchange].update({coin: {'amount': balance, 'change': change}})
            else:
                print('Error: exchange already has that coin in its balance')
                return -1
        return 0


    def update_change(self, coin, change):
        flag = False
        for exchange in self.exchanges:
            if coin in self.exchanges[exchange]:
                flag = True
                self.exchanges[exchange][coin]['change'] = change
        if flag:
            return 0
        print('Error: that coin not in this balance')
        return -1


    def update_balance(self, exchange: str, coin: str, new_balance: float, change: float):
        if exchange in self.exchanges:
            if coin in self.exchanges[exchange]:
                new_balance = self.exchanges[exchange][coin]['amount'] + new_balance
                self.exchanges[exchange][coin].update({'amount': new_balance, 'change': change})
            else:
                self.exchanges[exchange].update({coin: {'amount': new_balance, 'change': change}})
        else:
            print('Error: exchange not in this balance')
            return -1
        return 0

    def get_full_balance(self):
        acc = 0
        for exchange in self.exchanges:
            for coin in self.exchanges[exchange].values():
                acc += coin['amount'] * coin['change']
        return acc


    def get_balance(self, exchange: str):
        if exchange in self.exchanges:
            acc = 0
            for coin in self.exchanges[exchange].values():
                acc += coin['amount'] * coin['change']
            return acc
        else:
            print('Error: exchange not in this balance')
            return -1


def init_balances(exchanges):
    bal = Balance()
    for exchange in exchanges:
        bal.set_balance(exchange.name, 'USDT', 1000.0, 1.0)
        bal.set_balance(exchange.name, 'BTC', 1.0, 7000.0)
    return bal


def get_order_books(exchanges, symbols_matrix):

    
    return 0
    

def pairs_generator(exchanges):
    ''' optimize the empairment to spread the empairments uniformly avoiding overpass request limits
    '''
    pairs = list()
    for i, j in zip([0,2,4,6, 0,1,4,5, 0,1,4,5, 0,1,2,3, 0,1,2,3, 1,0,3,2, 0,1,2,3],
                    [1,3,5,7, 2,3,6,7, 3,2,7,6, 4,5,6,7, 5,4,7,6, 7,6,5,4, 7,6,5,4]):
        pairs.append([exchanges[i], exchanges[j]])

    return pairs


def cross_exch_pairs(exch_pairs):
    pairs_to_cross = list()
    for exch_pair in exch_pairs:
        matched_pairs = list()
        for pair in exch_pair[0].markets.keys():
            if pair in exch_pair[1].markets.keys():  # crossing is possible!
                matched_pairs.append(pair)
        pairs_to_cross.append(matched_pairs)
    return pairs_to_cross


def cross_pairs(exch_pairs, pairs_to_cross):
    # global exch_locked
    # global exploit_threads
    while True:
        loop_time = time.time()

        for index, thread in enumerate(g_storage.exploit_threads, start=0):
            if thread.is_alive() == False:
                thread.join()
                g_storage.exploit_threads.pop(index)

        iterations = 0   
        for index, exch_pair in enumerate(exch_pairs, start=0):
            # if exch_pair[0] in exch_locked or exch_pair[1] in exch_locked:
            #     logger_1.info('{} or {} locked!'.format(exch_pair[0].name, exch_pair[1].name))
            #     continue
            if len(pairs_to_cross[index]) > 0:
                iterations += 1
                coin_pair = random.choice(pairs_to_cross[index])
                if [exch_pair[0], exch_pair[1], coin_pair] not in g_storage.exch_locked:

                    status = cross(exch_pair, coin_pair)
                
                else:
                    logger_1.info('{} and {} locked for {}!'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
        # try:
        #     time.sleep(iterations * 1.0 - (time.time() - loop_time))
        # except:
        #     time.sleep(3)
        logger_1.info('loop time: {}, treads: {}'.format(time.time() - loop_time, len(g_storage.exploit_threads)))

    return 0


def cross(exch_pair, coin_pair):  # TODO: use threading here
    # wait the required time, depending on the exchange
    if (g_storage.timer[exch_pair[0].name][1] - (time.time() - g_storage.timer[exch_pair[0].name][0])) > 0:
        time.sleep(g_storage.timer[exch_pair[0].name][1] - (time.time() - g_storage.timer[exch_pair[0].name][0]))

    if (g_storage.timer[exch_pair[1].name][1] - (time.time() - g_storage.timer[exch_pair[1].name][1])) > 0:
        time.sleep(g_storage.timer[exch_pair[1].name][1] - (time.time() - g_storage.timer[exch_pair[1].name][1]))

    try:
        orderbook_1 = exch_pair[0].fetch_order_book (coin_pair, limit=5)
        g_storage.timer[exch_pair[0].name][0] = time.time()  # timestampting request
    except:
        logger_1.critical('problems loading order books, request error on {}, adjusting timing limits'.format(exch_pair[0].name))
        g_storage.timer[exch_pair[0].name][1] += 0.05  # increasing delay. CAUTION HERE!
        logger_1.critical('new timming limit: {} seconds'.format(g_storage.timer[exch_pair[0].name][1]))
        return -1
    try:
        orderbook_2 = exch_pair[1].fetch_order_book (coin_pair, limit=5)
        g_storage.timer[exch_pair[1].name][0] = time.time()  # timestampting request
    except:
        logger_1.critical('problems loading order books, request error on {}, adjusting its timing limits'.format(exch_pair[1].name))
        g_storage.timer[exch_pair[1].name][1] += 0.05  # increasing delay. CAUTION HERE!
        logger_1.critical('new timming limit: {} seconds'.format(g_storage.timer[exch_pair[1].name][1]))
        return -1
    
    try:
        bid_1 = orderbook_1['bids'][0][0] if len (orderbook_1['bids']) > 0 else None
        ask_1 = orderbook_1['asks'][0][0] if len (orderbook_1['asks']) > 0 else None
        vol_bid_1 = orderbook_1['bids'][0][1] if len (orderbook_1['bids']) > 0 else None
        vol_ask_1 = orderbook_1['asks'][0][1] if len (orderbook_1['asks']) > 0 else None

        bid_2 = orderbook_2['bids'][0][0] if len (orderbook_2['bids']) > 0 else None
        ask_2 = orderbook_2['asks'][0][0] if len (orderbook_2['asks']) > 0 else None
        vol_bid_2 = orderbook_2['bids'][0][1] if len (orderbook_2['bids']) > 0 else None
        vol_ask_2 = orderbook_2['asks'][0][1] if len (orderbook_2['asks']) > 0 else None
        
    except:
        logger_1.error('not possible getting bids/asksfrom {} or {}'.format(exch_pair[0].name, exch_pair[1].name))
        return -1
    
    try:
        fee_1 = max(exch_pair[0].fees['trading']['maker'], exch_pair[0].fees['trading']['taker'])
    except:
        fee_1 = 0.005
        # logger_1.error('impossible to get fee from exchange {}'.format(exch_pair[0].name))
        # logger_1.error('setting a default value of {}'.format(fee_1))

    try:
        fee_2 = max(exch_pair[1].fees['trading']['maker'], exch_pair[1].fees['trading']['taker'])
    except:
        fee_2 = 0.005
        # logger_1.error('impossible to get fee from exchange {}'.format(exch_pair[1].name))
        # logger_1.error('setting a default value of {}'.format(fee_2))

    if bid_1 and bid_2 and ask_1 and ask_2:
        if ((bid_1 - ask_2)/ask_2) > (fee_1 + fee_2) * CROSSING_MARGIN:
            logger_2.info(
                ',   OPPORTUNITY, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
                    exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, vol_bid_1, ask_2, vol_ask_2, (bid_1 - ask_2)/ask_2, (fee_1+fee_2), (bid_1 - ask_2)/ask_2 - (fee_1+fee_2)))
            
            logger_1.info('locking exchanges {} and {} for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
            g_storage.exch_locked.append([exch_pair[0], exch_pair[1], coin_pair])

            # TODO: exploit opportunity: nos centramos en ese par de exchanges y monedas leyendo al máximo rate permitido. Analizar performance y meter mas o menos pasta en funcion de la tendencia...
            exploit_pair(exch_pair, coin_pair)

        elif ((bid_2 - ask_1)/ask_1) > (fee_1 + fee_2) * CROSSING_MARGIN:  # in the other direcction
            logger_2.info(
                ',R  OPPORTUNITY, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
                    exch_pair[1].name, exch_pair[0].name, coin_pair, bid_2, vol_bid_2, ask_1, vol_ask_1, (bid_2 - ask_1)/ask_1, (fee_1+fee_2), (bid_2 - ask_1)/ask_1 - (fee_1+fee_2)))
            
            logger_1.info('locking exchanges {} and {}'.format(exch_pair[0].name, exch_pair[1].name))
            g_storage.exch_locked.append([exch_pair[0], exch_pair[1], coin_pair])

            # TODO: exploit opportunity: nos centramos en ese par de exchanges y monedas leyendo al máximo rate permitido. Analizar performance y meter mas o menos pasta en funcion de la tendencia...
            exploit_pair(exch_pair, coin_pair, reverse=True)

        # else:
        #     logger_2.info(
        #         ',no opportunity, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
        #             exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, vol_bid_1, ask_2, vol_ask_2, (bid_1 - ask_2)/ask_2, (fee_1+fee_2), (bid_1 - ask_2)/ask_2 - (fee_1+fee_2)))
        #     logger_2.info(
        #         ',no opportunity, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
        #             exch_pair[1].name, exch_pair[0].name, coin_pair, bid_2, vol_bid_2, ask_1, vol_ask_1, (bid_2 - ask_1)/ask_1, (fee_1+fee_2), (bid_2 - ask_1)/ask_1 - (fee_1+fee_2)))
    
    else:
        logger_1.error('some bids or aks are NULL, {} {} {} {} {} {} {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, ask_1, bid_2, ask_2))

    # time.sleep(0)  # fine tune to not pass over exch rate limit! cuidado con los max rates!!!!
    return g_storage.exch_locked, 0


def exploit_pair(exch_pair, coin_pair, reverse=False):
    # TODO: to be implemented
    # global exch_locked
    # global exploit_threads
  
    logger_1.info('launching {} and {} thread for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair)) 
    thread = threading.Thread(target=exploit_thread, args=(exch_pair, coin_pair, reverse))
    g_storage.exploit_threads.append(thread)
    thread.start()

    return 0


def exploit_thread(exch_pair, coin_pair, reverse=False):
    
    # global exch_locked
    filename = './logs/' + exch_pair[0].name + '-' + exch_pair[1].name + '-' + coin_pair.replace('/', '-') + '.csv' if not reverse else './logs/' + exch_pair[1].name + '-' + exch_pair[0].name + '-' + coin_pair.replace('/', '-') + '.csv'

    while True:
        loop_time = time.time()
        now = datetime.now()

        if (g_storage.timer[exch_pair[0].name][1] - (time.time() - g_storage.timer[exch_pair[0].name][0])) > 0:
            time.sleep(g_storage.timer[exch_pair[0].name][1] - (time.time() - g_storage.timer[exch_pair[0].name][0]))

        if (g_storage.timer[exch_pair[1].name][1] - (time.time() - g_storage.timer[exch_pair[1].name][1])) > 0:
            time.sleep(g_storage.timer[exch_pair[1].name][1] - (time.time() - g_storage.timer[exch_pair[1].name][1]))

        try:
            orderbook_1 = exch_pair[0].fetch_order_book (coin_pair, limit=5)
            g_storage.timer[exch_pair[0].name][0] = time.time()
        except:
            logger_1.critical('Thread error loading order books, request error on {}, consider adjusting timing limits'.format(exch_pair[0].name))
            continue
        try:
            orderbook_2 = exch_pair[1].fetch_order_book (coin_pair, limit=5)
            g_storage.timer[exch_pair[1].name][0] = time.time()
        except:
            logger_1.critical('Thread error loading order books, request error on {}, consider adjusting timing limits'.format(exch_pair[1].name))
            continue
        
        # orderbook_1 = exch_pair[0].fetch_order_book (coin_pair, limit=5)
        # g_storage.timer[exch_pair[0].name][0] = time.time()
        # orderbook_2 = exch_pair[1].fetch_order_book (coin_pair, limit=5)
        # g_storage.timer[exch_pair[1].name][0] = time.time()

        try:
            bid_1 = orderbook_1['bids'][0][0] if len (orderbook_1['bids']) > 0 else None
            ask_1 = orderbook_1['asks'][0][0] if len (orderbook_1['asks']) > 0 else None
            vol_bid_1 = orderbook_1['bids'][0][1] if len (orderbook_1['bids']) > 0 else None
            vol_ask_1 = orderbook_1['asks'][0][1] if len (orderbook_1['asks']) > 0 else None

            bid_2 = orderbook_2['bids'][0][0] if len (orderbook_2['bids']) > 0 else None
            ask_2 = orderbook_2['asks'][0][0] if len (orderbook_2['asks']) > 0 else None
            vol_bid_2 = orderbook_2['bids'][0][1] if len (orderbook_2['bids']) > 0 else None
            vol_ask_2 = orderbook_2['asks'][0][1] if len (orderbook_2['asks']) > 0 else None
        except:
                logger_1.error('Thread error: not possible getting bids/asksfrom {} or {}'.format(exch_pair[0].name, exch_pair[1].name))
    
        try:
            fee_1 = max(exch_pair[0].fees['trading']['maker'], exch_pair[0].fees['trading']['taker'])
        except:
            fee_1 = 0.005
        try:
            fee_2 = max(exch_pair[1].fees['trading']['maker'], exch_pair[1].fees['trading']['taker'])
        except:
            fee_2 = 0.005

        with open(filename, 'a') as csv_file:
            if not reverse:
                csv_file.write(
                    '{}, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}\n'.format(
                        now.strftime("%Y-%m-%d %H:%M:%S"), exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, vol_bid_1, ask_2, vol_ask_2, (bid_1 - ask_2)/ask_2, (fee_1+fee_2), (bid_1 - ask_2)/ask_2 - (fee_1+fee_2))
                )
            else:
                csv_file.write(
                    '{}, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}\n'.format(
                        now.strftime("%Y-%m-%d %H:%M:%S"), exch_pair[1].name, exch_pair[0].name, coin_pair, bid_2, vol_bid_2, ask_1, vol_ask_1, (bid_2 - ask_1)/ask_1, (fee_1+fee_2), (bid_2 - ask_1)/ask_1 - (fee_1+fee_2))
                )
        if ((bid_1 - ask_2)/ask_2 - (fee_1 + fee_2)) <= 0 and not reverse:
            logger_1.info('UNlocking exchanges {} and {} for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
            g_storage.exch_locked.pop(g_storage.exch_locked.index([exch_pair[0], exch_pair[1], coin_pair]))
            return 0

        elif ((bid_2 - ask_1)/ask_1 - (fee_1 + fee_2)) <= 0 and reverse:
            logger_1.info('UNlocking exchanges {} and {} for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
            g_storage.exch_locked.pop(g_storage.exch_locked.index([exch_pair[0], exch_pair[1], coin_pair]))
            return 0
        
        try:
            time.sleep(EXPLOIT_THREAD_DELAY - (time.time() - loop_time))
        except:
            pass
    

def main():
    start_time = time.time()
    exchanges = create_exchanges()
    load_markets(exchanges)
    symbols_matrix = get_trading_pairs()
    balances = init_balances(exchanges)
    exch_pairs = pairs_generator(exchanges)

    pairs_to_cross = cross_exch_pairs(exch_pairs)
    cross_pairs(exch_pairs, pairs_to_cross)

    print("--- %s seconds ---" % (time.time() - start_time))
    print(balances.get_full_balance())


if __name__ ==  "__main__":
    logger_1 = setup_logger('first_logger', 'logs/logger.log', level=logging.DEBUG)
    logger_1.info('--------------- starting point ---------------')
    logger_2 = setup_logger('second_logger', 'logs/opport.csv')
    logger_2.info('--------------- starting point ---------------')

    # releasing the beast! XD 
    main()
    
