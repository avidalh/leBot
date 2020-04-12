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
DEMO_MODE = False  # TODO: implement a demo mode using balances
DEBUG = True
DEBUG_LEVEL = 0  # noy used by now
USE_THREADING = True  # only used in load markers function, TODO: to remove
CROSSING_MARGIN = 1.05  # 5% above delta
TRADING_SIZE = 20  # $20
# EXCH_REQUEST_DELAY = 1.8  # seconds, take care here: if rate overpassed yo could get penalized! TODO: to remove
EXPLOIT_THREAD_DELAY = 15  # exploit thread period
MAX_THREADS = 50  # limiting the number of threads


class GlobalStorage:
    """
        general purpose class to store global data
    """
    def __init__(self):
        self.exploit_threads = list()  # list of threads
        self.exch_locked = list()  # list of exchanges/coins already in use (threaded)
        self.timer = {}  # time stampt and exchange delay
        self.coins_white_list = list()  # coins allowed to use/cross
        self.initial_balance = 0
        self.current_balance = 0


g_storage = GlobalStorage()
# g_storage.coins_white_list = 'BCH BNB BSV BTC DASH EOS ETC ETH HT LINK LTC NEO OF OKB PAX QC QTUM TRX USDC USDT XML XRP ZEC XMR ADA ATOM'.split()
# taking only some coins...
g_storage.coins_white_list = 'BCH BTC ETH LTC EOS XMR XRP ZEC USDC USDT'.split()


def setup_logger(name, log_file, level=logging.INFO):
    """
        To setup as many loggers as you want
    """
    handler = logging.FileHandler(log_file)        
    handler = RotatingFileHandler(log_file, maxBytes=50*1024*1024, backupCount=10)  # rotate into 10 files when file reaches 50MB
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def create_exchanges():
    """
        instantiate the markets
        to include more exchanges use this function.
        new exchanges need to be hand-coded here
    """
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
    timing_limits = [    .35,      .35,       1,     .35,        2,      1,      1,  .35]  # requesting period limit per exchange

    for exchange, timing in zip(exchanges, timing_limits):
        g_storage.timer[exchange.name] = [0, timing]

    return exchanges


def load_markets(exchanges, force=False):
    """ 
        load markets in the passed list
        TODO: remove USE_THREADING option since it's not necessary/used
        TODO: modify these two functions to reload markets every couple of hours...
    """
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


def load_markets_thread(exchange, force):
    """
        thread to load the markets on the passed exchange
        :force: is a boolean which forces the reloading (overriding cached data)
    """
    exchange.load_markets(force)


def get_trading_pairs():  # TODO: to be removed
    """
        NOT USED
        returns the trading coins to be used in the trading
        TODO: to be removed
    """
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
    """
        once the markets are loaded gets the coin pairs used on them    
    """
    pairs = list()
    for exchange in exchanges:
        pairs.append(exchange.markets.keys())
    return pairs


class Balance:
    """
        class to store the current balance
        TODO: implement a demo mode to use fake balances 
    """
    def __init__(self):
        self.exchanges = {}
    
    def set_balance(self, exchange: str, coin: str, balance: float, change: float, trading_size: float):
        if exchange not in self.exchanges:
            self.exchanges.update({exchange: {coin: {'amount': balance, 'change': change, 'trading_size': trading_size}}})
        else:
            if coin not in self.exchanges[exchange]:
                self.exchanges[exchange].update({coin: {'amount': balance, 'change': change, 'trading_size': trading_size}})
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


    def update_balance(self, exchange: str, coin: str, new_balance: float):
        if exchange in self.exchanges:
            if coin in self.exchanges[exchange]:
                new_balance = self.exchanges[exchange][coin]['amount'] + new_balance
                self.exchanges[exchange][coin].update({'amount': new_balance})
            else:
                self.exchanges[exchange].update({coin: {'amount': new_balance}})
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

    def get_detailed_balance(self):
        detail = list()
        for exchange, name in zip(self.exchanges, self.exchanges.keys()):
            detail_exch = list()
            for coin, symbol in zip(self.exchanges[exchange].values(), self.exchanges[exchange].keys()):
                detail_exch.append({symbol: coin['amount']})
            detail.append({name: detail_exch})
        return detail


    def get_coin_balance(self, exchange: str, coin: str):
        if exchange in self.exchanges:
            return self.exchanges[exchange][coin]


balances = Balance()


def init_balances(exchanges):
    """
        initializes an instance of Balance class
        used in demo mode
    """
    
    for exchange in exchanges:
        balances.set_balance(exchange.name, 'USDT', 100.0, 1.0, 20.)
        balances.set_balance(exchange.name, 'BTC', .0145, 6868.0, 0.001)
        balances.set_balance(exchange.name, 'ETH', .633, 158.0, 0.3)
        balances.set_balance(exchange.name, 'XMR', 1.888, 52.97, .5)
        balances.set_balance(exchange.name, 'BCH', .43, 232.0, .4)
        balances.set_balance(exchange.name, 'XRP', 523.0, .191, 10.)
        balances.set_balance(exchange.name, 'ZEC', 2.78, 36.25, .6)
        balances.set_balance(exchange.name, 'EOS', 40.0, 2.50, 10.)
        balances.set_balance(exchange.name, 'LTC', 2.26, 42.43, .5)
        balances.set_balance(exchange.name, 'USDC', 100.0, 1.0, 20)
    
        # balances.get_detailed_balance()

    return 0


def get_order_books(exchanges, symbols_matrix): return 0  # TODO: to be removed
    

def pairs_generator(exchanges):
    """
        return a list of exchanges pairs and optimize the sequence for the requesting process
        optimize the empairment to spread the empairments uniformly, avoiding overpass requesting rate limits
        pairing was figured out by hand...
        TODO: look for a math function to perform the distribution
        TODO: currently uses 8 exchanges, to insert more a new empairement lists must be created! :-/
    """
    pairs = list()
    for i, j in zip([0,2,4,6, 0,1,4,5, 0,1,4,5, 0,1,2,3, 0,1,2,3, 1,0,3,2, 0,1,2,3],
                    [1,3,5,7, 2,3,6,7, 3,2,7,6, 4,5,6,7, 5,4,7,6, 7,6,5,4, 7,6,5,4]):
        pairs.append([exchanges[i], exchanges[j]])

    return pairs


def cross_exch_pairs(exch_pairs):
    """
        return a list of the possible coins pairs to cross in each exchange pair
    """
    pairs_to_cross = list()
    final_pairs = list()
    for exch_pair in exch_pairs:
        matched_pairs = list()
        for pair in exch_pair[0].markets.keys():
            if pair in exch_pair[1].markets.keys():  # crossing is possible!
                if pair.split('/')[0] in g_storage.coins_white_list and pair.split('/')[1] in g_storage.coins_white_list:
                    matched_pairs.append(pair)
        pairs_to_cross.append(matched_pairs)
    
    return pairs_to_cross

    # for symbol in g_storage.coins_white_list:
    #     for pair in matched_pairs:
    #         if symbol in pair and pair not in final_pairs:
    #             final_pairs.append(pair)
    # return final_pairs


def cross_pairs(exch_pairs, pairs_to_cross):
    """
        performs the crossing between exchanges using the possible coins pairs
        a random choosing is used.
        TODO: remove the random choosing and use a more consistent way...
    """
    while True:  # infinite loop
        loop_time = time.time()

        # first remove finished threads from the threads list:
        for index, thread in enumerate(g_storage.exploit_threads, start=0):
            if thread.is_alive() == False:
                thread.join()
                g_storage.exploit_threads.pop(index)
                logger_1.info('removing finished thread from the list')

        # randomly chooses a pair of coins and call the crossing function
        # iterations = 0  # not used  TODO: to be removed
        for index, exch_pair in enumerate(exch_pairs, start=0):
            if len(pairs_to_cross[index]) > 0:
                # iterations += 1  # not used  TODO: to be removed
                coin_pair = random.choice(pairs_to_cross[index])
                # check if this pair of echanges and coins are already locked 
                if [exch_pair[0], exch_pair[1], coin_pair] not in g_storage.exch_locked:

                    status = cross(exch_pair, coin_pair)  # launch the crossing procesure
                    logger_1.info('trying {} and {} for {} crossing'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
                
                else:
                    logger_1.info('{} and {} already locked for {}!'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
        
        logger_1.info('loop time: {}, threads: {}'.format(time.time() - loop_time, len(g_storage.exploit_threads)))

    return 0


def cross(exch_pair, coin_pair):
    """
        performs the exchange crossing for the coin pair
        TODO: if an exchange does not respond many times consecutively move it to a waiting list and disable it for requesting
              try again after some minutes. Use a thread for that functionality
        TODO: use more points and not only the best one. 
    """
    
    # this is to not over pass the exchanges request limits
    # wait the required time, depending on the exchange
    if (g_storage.timer[exch_pair[0].name][1] - (time.time() - g_storage.timer[exch_pair[0].name][0])) > 0:
        time.sleep(g_storage.timer[exch_pair[0].name][1] - (time.time() - g_storage.timer[exch_pair[0].name][0]))

    # if necessary also waits for the second exchange
    if (g_storage.timer[exch_pair[1].name][1] - (time.time() - g_storage.timer[exch_pair[1].name][1])) > 0:
        time.sleep(g_storage.timer[exch_pair[1].name][1] - (time.time() - g_storage.timer[exch_pair[1].name][1]))

    try:  # fetch the first order book
        orderbook_1 = exch_pair[0].fetch_order_book(coin_pair, limit=5)
        g_storage.timer[exch_pair[0].name][0] = time.time()  # timestampts the request/fetch
    except:
        logger_1.critical('problems loading order books, request error on {}, adjusting timing limits'.format(exch_pair[0].name))
        if g_storage.timer[exch_pair[0].name][1] <= 4.95:
            g_storage.timer[exch_pair[0].name][1] += 0.05  # increasing delay. CAUTION HERE!
            logger_1.critical('new timming limit: {} seconds'.format(g_storage.timer[exch_pair[0].name][1]))
        return -1

    try:  # and fetch the second order book
        orderbook_2 = exch_pair[1].fetch_order_book(coin_pair, limit=5)
        g_storage.timer[exch_pair[1].name][0] = time.time()  # timestampting request
    except:
        logger_1.critical('problems loading order books, request error on {}, adjusting its timing limits'.format(exch_pair[1].name))
        if g_storage.timer[exch_pair[1].name][1] <= 4.95:
            g_storage.timer[exch_pair[1].name][1] += 0.05  # increasing delay. CAUTION HERE!
            logger_1.critical('new timming limit: {} seconds'.format(g_storage.timer[exch_pair[1].name][1]))
        return -1
    
    try:  # gets the bids and asks for each exchange
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
    
    # gets the fees
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

    # check if there is an ipportunity of profit in both directions
    if bid_1 and bid_2 and ask_1 and ask_2:
        if ((bid_1 - ask_2)/ask_2) > (fee_1 + fee_2) * CROSSING_MARGIN:
            logger_2.info(
                ',   OPPORTUNITY, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
                    exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, vol_bid_1, ask_2, vol_ask_2, (bid_1 - ask_2)/ask_2, (fee_1+fee_2), (bid_1 - ask_2)/ask_2 - (fee_1+fee_2)))
            
            logger_1.info('locking exchanges {} and {} for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
            g_storage.exch_locked.append([exch_pair[0], exch_pair[1], coin_pair])

            # if profit is possible exploit the pair
            exploit_pair(exch_pair, coin_pair)

        elif ((bid_2 - ask_1)/ask_1) > (fee_1 + fee_2) * CROSSING_MARGIN:  # in the other direcction
            logger_2.info(
                ',R  OPPORTUNITY, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
                    exch_pair[1].name, exch_pair[0].name, coin_pair, bid_2, vol_bid_2, ask_1, vol_ask_1, (bid_2 - ask_1)/ask_1, (fee_1+fee_2), (bid_2 - ask_1)/ask_1 - (fee_1+fee_2)))
            
            logger_1.info('locking exchanges {} and {} for {}'.format(exch_pair[1].name, exch_pair[0].name, coin_pair))
            g_storage.exch_locked.append([exch_pair[1], exch_pair[0], coin_pair])

            # if profit is possible in the other direction exploit the pair as well
            exploit_pair(exch_pair, coin_pair, reverse=True)

        # else:  # non proffit branch, just to log data.  TODO: To be removed
        #     logger_2.info(
        #         ',no opportunity, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
        #             exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, vol_bid_1, ask_2, vol_ask_2, (bid_1 - ask_2)/ask_2, (fee_1+fee_2), (bid_1 - ask_2)/ask_2 - (fee_1+fee_2)))
        #     logger_2.info(
        #         ',no opportunity, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
        #             exch_pair[1].name, exch_pair[0].name, coin_pair, bid_2, vol_bid_2, ask_1, vol_ask_1, (bid_2 - ask_1)/ask_1, (fee_1+fee_2), (bid_2 - ask_1)/ask_1 - (fee_1+fee_2)))
    
    else:
        logger_1.error('some bids or aks are NULL, {} {} {} {} {} {} {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, ask_1, bid_2, ask_2))

    return 0


def exploit_pair(exch_pair, coin_pair, reverse=False):
    """
        launches the exploit thread
    """
    if not reverse:
        logger_1.info('launching {} and {} thread for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
    else:
        logger_1.info('launching {} and {} thread for {}'.format(exch_pair[1].name, exch_pair[0].name, coin_pair))
    
    # launch the thread
    thread = threading.Thread(target=exploit_thread, args=(exch_pair, coin_pair, reverse))
    g_storage.exploit_threads.append(thread)
    thread.start()

    return 0


def exploit_thread(exch_pair, coin_pair, reverse=False):
    """
        exploit thread
        it tracks a profit tendency between exchanges and coins pair
        stores the profit until the tendence turns into negative or not profitable
        TODO: this is where we have to IMPLEMENT THE ORDERS!!!!
    """
    # compose the log filename using exchanges and coins pair
    filename = './logs/' + exch_pair[0].name + '-' + exch_pair[1].name + '-' + coin_pair.replace('/', '-') + '.csv' if not reverse else './logs/' + exch_pair[1].name + '-' + exch_pair[0].name + '-' + coin_pair.replace('/', '-') + '.csv'

    while True:
        loop_time = time.time()
        now = datetime.now()

        # waits for the exchanges rate limits and gets the order books
        if (g_storage.timer[exch_pair[0].name][1] - (time.time() - g_storage.timer[exch_pair[0].name][0])) > 0:
            time.sleep(g_storage.timer[exch_pair[0].name][1] - (time.time() - g_storage.timer[exch_pair[0].name][0]))

        if (g_storage.timer[exch_pair[1].name][1] - (time.time() - g_storage.timer[exch_pair[1].name][1])) > 0:
            time.sleep(g_storage.timer[exch_pair[1].name][1] - (time.time() - g_storage.timer[exch_pair[1].name][1]))

        try:
            orderbook_1 = exch_pair[0].fetch_order_book (coin_pair, limit=5)
            g_storage.timer[exch_pair[0].name][0] = time.time()
        except:
            logger_1.critical('Thread error loading order books, request error on {}, awaiting for a while'.format(exch_pair[0].name))
            # g_storage.timer[exch_pair[0].name][0] = time.time()
            time.sleep(random.randint(5, 14))  # wait a moment...
            continue
        try:
            orderbook_2 = exch_pair[1].fetch_order_book (coin_pair, limit=5)
            g_storage.timer[exch_pair[1].name][0] = time.time()
        except:
            logger_1.critical('Thread error loading order books, request error on {}, awaiting for a while'.format(exch_pair[1].name))
            # g_storage.timer[exch_pair[1].name][0] = time.time()
            time.sleep(random.randint(5, 14))  # wait a moment...
            continue
        
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

        # gets the fees
        try:
            fee_1 = max(exch_pair[0].fees['trading']['maker'], exch_pair[0].fees['trading']['taker'])
        except:
            fee_1 = 0.005
        try:
            fee_2 = max(exch_pair[1].fees['trading']['maker'], exch_pair[1].fees['trading']['taker'])
        except:
            fee_2 = 0.005

        # gets the trading sizes and coins names
        base_coin = coin_pair.split('/')[0]
        quote_coin = coin_pair.split('/')[1]
        trading_size_1 = balances.get_coin_balance(exch_pair[0].name, base_coin)['trading_size']
        trading_size_2 = balances.get_coin_balance(exch_pair[1].name, base_coin)['trading_size']
        
        # logs results
        with open(filename, 'a') as csv_file:
            if not reverse:
                csv_file.write(
                    '{}, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}\n'.format(
                        now.strftime("%Y-%m-%d %H:%M:%S"), exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, vol_bid_1, ask_2, vol_ask_2, (bid_1 - ask_2)/ask_2, (fee_1+fee_2), (bid_1 - ask_2)/ask_2 - (fee_1+fee_2))
                )
                
                if vol_bid_1 >= trading_size_1 and vol_ask_2 > trading_size_2:
                    logger_1.info('Thread: ordering selling-buying on {} or {} for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
                    selling_order_demo(exch_pair[0], coin_pair, bid_1, trading_size_1, fee_1)
                    buying_order_demo (exch_pair[1], coin_pair, ask_2, trading_size_2, fee_2)
                else:
                    logger_1.info('Thread: not enough volume for ordering selling-buying on {} or {} for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))

            else:
                csv_file.write(
                    '{}, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}\n'.format(
                        now.strftime("%Y-%m-%d %H:%M:%S"), exch_pair[1].name, exch_pair[0].name, coin_pair, bid_2, vol_bid_2, ask_1, vol_ask_1, (bid_2 - ask_1)/ask_1, (fee_1+fee_2), (bid_2 - ask_1)/ask_1 - (fee_1+fee_2))
                )

                if vol_bid_2 >= trading_size_2 and vol_ask_1 > trading_size_1:
                    logger_1.info('Thread: ordering selling-buying on {} or {} for {}'.format(exch_pair[1].name, exch_pair[0].name, coin_pair))
                    selling_order_demo(exch_pair[1], coin_pair, bid_2, trading_size_2, fee_2)
                    buying_order_demo (exch_pair[0], coin_pair, ask_1, trading_size_1, fee_1)
                else:
                    logger_1.info('Thread: not enough volume for ordering selling-buying on {} or {} for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))


        if ((bid_1 - ask_2)/ask_2 - (fee_1 + fee_2)) <= 0 and not reverse:
            logger_1.info('UN-locking exchanges {} and {} for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
            g_storage.exch_locked.pop(g_storage.exch_locked.index([exch_pair[0], exch_pair[1], coin_pair]))
            logger_1.info('finishing thread for exchanges {} and {} for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
            return 0

        elif ((bid_2 - ask_1)/ask_1 - (fee_1 + fee_2)) <= 0 and reverse:
            logger_1.info('UN-locking exchanges {} and {} for {}'.format(exch_pair[1].name, exch_pair[0].name, coin_pair))
            g_storage.exch_locked.pop(g_storage.exch_locked.index([exch_pair[1], exch_pair[0], coin_pair]))
            logger_1.info('finishing thread for exchanges {} and {} for {}'.format(exch_pair[1].name, exch_pair[0].name, coin_pair))
            return 0

        try:
            time.sleep(EXPLOIT_THREAD_DELAY - (time.time() - loop_time))
        except:
            pass


def selling_order_demo(exchange, coin_pair, bid, size, fee):
    """ simulate a selling order """
    base_coin = coin_pair.split('/')[0]
    quote_coin = coin_pair.split('/')[1]

    logger_1.info('pre selling balance on, {}, {}, {}, {}'.format(exchange.name, coin_pair, balances.get_coin_balance(exchange.name, base_coin)['amount'], balances.get_coin_balance(exchange.name, quote_coin)['amount']))
    # list_balances(start=True)
    logger_4.info('full balance pre operation (USDT): {}, profit: {}, acc profit: {}'.format(balances.get_full_balance(),
                                                                                             balances.get_full_balance()-g_storage.current_balance,
                                                                                             balances.get_full_balance()-g_storage.initial_balance
                                                                                             ))
    g_storage.current_balance = balances.get_full_balance()
    quote_amount = size * bid
    base_amount = -(size + fee * size)
    balances.update_balance(exchange.name, base_coin, base_amount)
    balances.update_balance(exchange.name, quote_coin, quote_amount)
    logger_1.info('post selling balance on, {}, {}, {}, {}'.format(exchange.name, coin_pair, balances.get_coin_balance(exchange.name, base_coin)['amount'], balances.get_coin_balance(exchange.name, quote_coin)['amount']))
    # list_balances()
    return 0


def buying_order_demo(exchange, coin_pair, ask, size, fee):
    """ simulate a buying order """
    base_coin = coin_pair.split('/')[0]
    quote_coin = coin_pair.split('/')[1]

    logger_1.info('pre buying balance on, {}, {}, {}, {}'.format(exchange.name, coin_pair, balances.get_coin_balance(exchange.name, base_coin)['amount'], balances.get_coin_balance(exchange.name, quote_coin)['amount']))
    # list_balances()
    quote_amount = -(size + fee * size) * ask
    base_amount = size
    balances.update_balance(exchange.name, base_coin, base_amount)
    balances.update_balance(exchange.name, quote_coin, quote_amount)
    logger_1.info('post buying balance on, {}, {}, {}, {}'.format(exchange.name, coin_pair, balances.get_coin_balance(exchange.name, base_coin)['amount'], balances.get_coin_balance(exchange.name, quote_coin)['amount']))
    list_balances(end=True)
    logger_4.info('full balance pos operation (USDT): {}, profit: {}, acc profit: {}'.format(balances.get_full_balance(),
                                                                                             balances.get_full_balance()-g_storage.current_balance,
                                                                                             balances.get_full_balance()-g_storage.initial_balance
                                                                                             ))

    return 0


def list_balances(start=False, end=False):
    if start:
        logger_3.info('------------------------------------------------------------------------------------------')
    detailed_balance = balances.get_detailed_balance()
    for element in detailed_balance:
        for key, value in zip(element.keys(), element.values()):
            logger_3.info(', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}'.format(
                                key, value[0]['USDT'],
                                value[1]['BTC'],
                                value[2]['ETH'],
                                value[3]['XMR'],
                                value[4]['BCH'],
                                value[5]['XRP'],
                                value[6]['ZEC'],
                                value[7]['EOS'],
                                value[8]['LTC'],
                                value[9]['USDC'])
                         )
    if end:
        logger_3.info('------------------------------------------------------------------------------------------')

    # return 0


def main():
    start_time = time.time()
    exchanges = create_exchanges()
    load_markets(exchanges)
    # symbols_matrix = get_trading_pairs()
    init_balances(exchanges)
    g_storage.initial_balance = balances.get_full_balance()
    list_balances()

    exch_pairs = pairs_generator(exchanges)
    pairs_to_cross = cross_exch_pairs(exch_pairs)
    cross_pairs(exch_pairs, pairs_to_cross)

    print("--- %s seconds ---" % (time.time() - start_time))
    print(balances.get_full_balance())


if __name__ ==  "__main__":
    logger_1 = setup_logger('first_logger', 'logs/logger.log', level=logging.DEBUG)
    logger_1.info('--------------------------------------------- starting point ---------------------------------------------')
    logger_2 = setup_logger('second_logger', 'logs/opport.csv')
    logger_2.info('--------------------------------------------- starting point ---------------------------------------------')
    logger_3 = setup_logger('third_logger', 'logs/balances.csv')
    logger_3.info('--------------------------------------------- starting point ---------------------------------------------')
    logger_4 = setup_logger('fourth_logger', 'logs/balances.log')
    logger_4.info('--------------------------------------------- starting point ---------------------------------------------')

    # releasing the beast! XD 
    main()
    