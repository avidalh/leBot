import ccxt
from datetime import datetime
import time
import threading
import api_keys
import random
from itertools import combinations
import pprint
import json
import requests

# logger modules
import logging
from logging.handlers import RotatingFileHandler

# console modules
# import cmd, sys

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

# parameters
DEMO_MODE = True                           # TODO: implement a demo mode using balances
DEBUG = True
DEBUG_LEVEL = 0                             # noy used by now

LOG_PROFITS = True

USE_THREADING = True                        # only used in load markers function, TODO: to remove

# CROSSING_MARGIN = 1.05                      # 5% above delta
TRADING_SIZE = 25                          # $25

EXPLOIT_THREAD_DELAY = 15                   # exploit thread period
MAX_THREADS = 30                            # limiting the number of threads
PROFIT_THR_TO_OPEN_POSITIONS =  +0.00400     # open position threshold (values for testing the bot)
PROFIT_THR_TO_CLOSE_POSITIONS = +0.00005     # close positions threshold. Close all the positions openend by the arb thread.
MAX_ITER_TO_EXIT = 50
TRADES_TO_ALLOW_CLOSING = 1


class GlobalStorage:
    """
        general purpose class to store global data
    """

    def __init__(self):
        self.exploit_threads = list()  # list of threads
        self.exploit_thread_number = 0
        self.exch_locked = list()  # list of exchanges/coins already in use (threaded)
        self.timer = {}  # time stampt and exchange delay
        self.coins_white_list = list()  # coins allowed to use/cross
        self.initial_balance = 0
        self.current_balance = 0


g_storage = GlobalStorage()
# g_storage.coins_white_list = 'BCH BNB BSV BTC DASH EOS ETC ETH HT LINK LTC NEO OF OKB PAX QC QTUM TRX XML XRP ZEC XMR ADA ATOM'.split()
# taking only some coins...
g_storage.coins_white_list =   'BTC BCH ETH LTC EOS XMR XRP ZEC DASH NEO PIVX NANO ADA ETC HT LINK ATOM QTUM BNB BSV OF OKB PAX QC TRX XML USDC USDT EUR USD'.split()


def setup_logger(name, log_file, level=logging.INFO):
    """
        To setup as many loggers as you want
    """
    handler = logging.FileHandler(log_file, mode='a')
    # rotate into 10 files when file reaches 50MB
    handler = RotatingFileHandler(log_file, maxBytes=50*1024*1024, backupCount=10)
    handler.setFormatter(formatter)
    handler.doRollover()

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
        'enableRateLimit': True,
    })

    cex = ccxt.cex({
        'apiKey': api_keys.cex['apiKey'],
        'secret': api_keys.cex['secret'],
        'enableRateLimit': True,
    })

    poloniex = ccxt.poloniex({
        'apiKey': api_keys.poloniex['apiKey'],
        'secret': api_keys.poloniex['secret'],
        'enableRateLimit': True,
    })

    bittrex = ccxt.bittrex({
        'apiKey': api_keys.bittrex['apiKey'],
        'secret': api_keys.bittrex['secret'],
        'enableRateLimit': True,
    })

    binance = ccxt.binance({
        'apiKey': api_keys.binance['apiKey'],
        'secret': api_keys.binance['secret'],
        'enableRateLimit': True,
    })

    bitfinex = ccxt.bitfinex({
        'apiKey': api_keys.bitfinex['apiKey'],
        'secret': api_keys.bitfinex['secret'],
        'enableRateLimit': True,
    })

    kucoin = ccxt.kucoin({
        'apiKey': api_keys.kucoin['apiKey'],
        'secret': api_keys.kucoin['secret'],
        'enableRateLimit': True,
    })

    kraken = ccxt.kraken({
        'apiKey': api_keys.kraken['apiKey'],
        'secret': api_keys.kraken['secret'],
    'enableRateLimit': True,
        })

    bitmex = ccxt.bitmex({
        'apiKey': api_keys.bitmex['apiKey'],
        'secret': api_keys.bitmex['secret'],
        'enableRateLimit': True,
    })

    okex = ccxt.okex({
        'apiKey': api_keys.okex['apiKey'],
        'secret': api_keys.okex['secret'],
        'enableRateLimit': True,
    })

    # exchanges = [coinbasepro, poloniex, bittrex, binance, bitfinex, kucoin, bitmex, okex]
    exchanges =     [cex, poloniex, bittrex, binance, bitfinex, kucoin, bitmex, coinbasepro]
    timing_limits = [.35,      .35,       1,     .35,        2,        1,      1,    .35]  # requesting period limit per exchange

    for exchange, timing in zip(exchanges, timing_limits):
        g_storage.timer[exchange.name] = [0, timing]

    # exchanges.pop(exchanges.index(kraken))

    return exchanges


def load_markets(exchanges, force=False):
    thread = threading.Thread(target=load_markets_, name='loadMarkets', args=(exchanges, force))
    thread.start()
    # thread.join()

def load_markets_(exchanges, force=False):
    """ 
        load markets in the passed list
        TODO: remove USE_THREADING option since it's not necessary/used
        TODO: modify these two functions to reload markets every couple of hours...
    """
    # while True:
    threads = list()
    for exchange in exchanges:
        thread = threading.Thread(target=load_markets_thread, name=str('loadMarket'+exchange.name) ,args=(exchange, force))
        threads.append(thread)
        thread.start()
    for thread, exchange in zip(threads, exchanges):
        exchange = thread.join()
        # time.sleep(60*60*4)

def load_markets_thread(exchange, force):
    """
        thread to load the markets on the passed exchange
        :force: is a boolean which forces the reloading (overriding cached data)
    """
    exchange.load_markets(force)


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
            self.exchanges.update({exchange: {coin: {'amount': balance, 'change': change, 'trading_size': trading_size, 'in_use': False, 'acc_profit': 0}}})
        else:
            if coin not in self.exchanges[exchange]:
                self.exchanges[exchange].update({coin: {'amount': balance, 'change': change, 'trading_size': trading_size, 'in_use': False, 'acc_profit': 0}})
            else:
                print('Error: exchange already has that coin in its balance')
                return -1
        return 0

    def update_profit(self, exchange:str, coin: str, profit: float):
        flag = False
        # for exchange in self.exchanges:
        if coin in self.exchanges[exchange]:
            self.exchanges[exchange][coin]['acc_profit'] += profit
            return True
        print('Error: that coin does not exist in this balance')
        return False        

    def lock_coin(self, exchange: str, coin: str):
        self.exchanges[exchange][coin].update({'in_use': True})
        return 0

    def unlock_coin(self, exchange: str, coin: str):
        self.exchanges[exchange][coin].update({'in_use': False})
        return 0

    def update_change(self, exchange: str, coin: str, change: float):
        flag = False
        # for exchange in self.exchanges:
        if coin in self.exchanges[exchange]:
            self.exchanges[exchange][coin]['change'] = change
            self.exchanges[exchange][coin]['trading_size'] = TRADING_SIZE / change
            return True
        print('Error: that coin does not exist in this balance')
        return False

    def update_balance(self, exchange: str, coin: str, new_balance: float):
        if exchange in self.exchanges:
            if coin in self.exchanges[exchange]:
                new_balance = self.exchanges[exchange][coin]['amount'] + new_balance
                self.exchanges[exchange][coin].update({'amount': new_balance})
            else:
                self.exchanges[exchange].update(
                    {coin: {'amount': new_balance}})
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

    def get_detailed_balance(self, exchange = False):
        detail = list()
        if not exchange:
            for exchange, name in zip(self.exchanges, self.exchanges.keys()):
                detail_exch = list()
                for coin, coin_name in zip(self.exchanges[exchange].values(), self.exchanges[exchange].keys()):
                    detail_exch.append({coin_name: {'amount': coin['amount'], 'change': coin['change'],'acc profit': coin['acc_profit']}})
                detail.append({name: detail_exch})
            return detail
        else:
            detail_exch = list()
            for coin, coin_name in zip(self.exchanges[exchange].values(), self.exchanges[exchange].keys()):
                detail_exch.append({coin_name: {'amount': coin['amount'], 'change': coin['change'], 'acc profit': coin['acc_profit']}})
            detail.append({exchange: detail_exch})
            return detail

    def get_coin_balance(self, exchange: str, coin: str):
        if exchange in self.exchanges:
            return self.exchanges[exchange][coin]

    def get_coin_balance_usdt(self, exchange: str, coin: str):
        if exchange in self.exchanges:
            return self.exchanges[exchange][coin]['amount'] * self.exchanges[exchange][coin]['change']


balances = Balance()


def init_balances(exchanges):
    """
        initializes an instance of Balance class
        used in demo mode
    """
    FACTOR = 5
    for exchange in exchanges:
        for coin in g_storage.coins_white_list:
            balances.set_balance(exchange.name, coin, 0, 0, 0)
        
        balances.update_balance(exchange.name, 'BTC',  000.0133 * FACTOR)
        balances.update_balance(exchange.name, 'USD',  100.0000 * FACTOR)
        
        # initialize other coins to zero balance...
        
    return 0


def coins_prices_updater(exchanges):

    """ launches threads for basic operations """
    logger.info('launching basic threads')
    logger.info('launching coin change updater')
    thread = threading.Thread(target=coins_prices_updater_thread, name='coinsPricesUpdaterThread', args=(exchanges,))
    thread.start()

    return True


def coins_prices_updater_thread(exchanges):  # OK
    """ updates the cryptos price to USD """

    BASE_URL = 'https://min-api.cryptocompare.com/data/price?fsym={}&tsyms=USD'
    
    while True:
        for coin in g_storage.coins_white_list:
            resp = requests.get(BASE_URL.format(coin))
            try:
                last_price = resp.json()['USD']
            except:
                logger.error('coins_price_updater_thread() coin {} not available'.format(coin))
            for exchange in exchanges:  # Updates all exchanges balances with the same price.
                    logger.info('coins_price_updater_thread() updating exchange {} coin {} to {} USD'.format(exchange.name, coin, last_price))
                    balances.update_change(exchange.name, coin, last_price)
            time.sleep(5)
        time.sleep(60*60*1)  # wait for an hour
    return True


def markets_updater():

    return True


def pairs_generator(exchanges):
    """
        return a list of exchanges pairs and optimize the sequence for the requesting process
        optimize the empairment to spread the empairments uniformly, avoiding overpass requesting rate limits
        pairing was figured out by hand...
        TODO: look for a math function to perform the distribution
        TODO: currently uses 8 exchanges, to insert more a new empairement lists must be created! :-/
    """
    pairs = list()
    if len(exchanges) == 8:
        for i, j in zip([0, 2, 4, 6, 0, 1, 4, 5, 0, 1, 4, 5, 0, 1, 2, 3, 0, 1, 2, 3, 1, 0, 3, 2, 0, 1, 2, 3],
                        [1, 3, 5, 7, 2, 3, 6, 7, 3, 2, 7, 6, 4, 5, 6, 7, 5, 4, 7, 6, 7, 6, 5, 4, 7, 6, 5, 4]):
            pairs.append([exchanges[i], exchanges[j]])
    else:
        pairs = combinations(exchanges, 2)

    return pairs


def cross_exch_pairs(exch_pairs):
    """
        return a list of the possible coins pairs to cross in each exchange pair
    """
    pairs_to_cross = list()
    final_pairs = list()
    for exch_pair in exch_pairs:
        matched_pairs = list()
        try:
            for pair in exch_pair[0].markets.keys():
                if pair in exch_pair[1].markets.keys():  # crossing is possible!
                    if pair.split('/')[0] in g_storage.coins_white_list and pair.split('/')[1] in g_storage.coins_white_list:
                        matched_pairs.append(pair)
        except:
            pass
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
                logger.info('removing finished thread from the list')

        # randomly chooses a pair of coins and call the crossing function
        # iterations = 0  # not used  TODO: to be removed
        for index, exch_pair in enumerate(exch_pairs, start=0):
            if len(pairs_to_cross[index]) > 0:
                # iterations += 1  # not used  TODO: to be removed
                coin_pair = random.choice(pairs_to_cross[index])
                # check if this pair of echanges and coins are already locked
                if [exch_pair[0], exch_pair[1], coin_pair] not in g_storage.exch_locked:

                    # launch the crossing procesure
                    status = cross(exch_pair, coin_pair)
                    logger.info('trying {} and {} for {} crossing'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))

                else:
                    logger.info('{} and {} already locked for {}!'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))

        logger.info('loop time: {}, threads: {}'.format(time.time() - loop_time, len(g_storage.exploit_threads)))

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
        # timestampts the request/fetch
        g_storage.timer[exch_pair[0].name][0] = time.time()
    except:
        logger.critical('problems loading order books, request error on \t{}, adjusting timing limits'.format(exch_pair[0].name))
        if g_storage.timer[exch_pair[0].name][1] <= 4.95:
            # increasing delay. CAUTION HERE!
            g_storage.timer[exch_pair[0].name][1] += 0.05
            logger.critical('new timming limit: \t{} seconds'.format(g_storage.timer[exch_pair[0].name][1]))
        return -1

    try:  # and fetch the second order book
        orderbook_2 = exch_pair[1].fetch_order_book(coin_pair, limit=5)
        # timestampting request
        g_storage.timer[exch_pair[1].name][0] = time.time()
    except:
        logger.critical('problems loading order books, request error on \t{}, adjusting its timing limits'.format(exch_pair[1].name))
        if g_storage.timer[exch_pair[1].name][1] <= 4.95:
            # increasing delay. CAUTION HERE!
            g_storage.timer[exch_pair[1].name][1] += 0.05
            logger.critical('new timming limit: \t{} seconds'.format(g_storage.timer[exch_pair[1].name][1]))
        return -1

    try:  # gets the bids and asks for each exchange
        bid_1 = orderbook_1['bids'][0][0] if len(orderbook_1['bids']) > 0 else None
        ask_1 = orderbook_1['asks'][0][0] if len(orderbook_1['asks']) > 0 else None
        vol_bid_1 = orderbook_1['bids'][0][1] if len(orderbook_1['bids']) > 0 else None
        vol_ask_1 = orderbook_1['asks'][0][1] if len(orderbook_1['asks']) > 0 else None

        bid_2 = orderbook_2['bids'][0][0] if len(orderbook_2['bids']) > 0 else None
        ask_2 = orderbook_2['asks'][0][0] if len(orderbook_2['asks']) > 0 else None
        vol_bid_2 = orderbook_2['bids'][0][1] if len(orderbook_2['bids']) > 0 else None
        vol_ask_2 = orderbook_2['asks'][0][1] if len(orderbook_2['asks']) > 0 else None

    except:
        logger.error('not possible getting bids/asksfrom \t{} or \t{}'.format(exch_pair[0].name, exch_pair[1].name))
        return -1

    # gets the fees
    try:
        fee_1 = max(exch_pair[0].fees['trading']['maker'], exch_pair[0].fees['trading']['taker'])
    except:
        fee_1 = 0.005
        # logger.error('impossible to get fee from exchange {}'.format(exch_pair[0].name))
        # logger.error('setting a default value of {}'.format(fee_1))

    try:
        fee_2 = max(exch_pair[1].fees['trading']['maker'], exch_pair[1].fees['trading']['taker'])
    except:
        fee_2 = 0.005
        # logger.error('impossible to get fee from exchange {}'.format(exch_pair[1].name))
        # logger.error('setting a default value of {}'.format(fee_2))

    # check if there is an ipportunity of profit in both directions
    if bid_1 and bid_2 and ask_1 and ask_2:

        # (fee_1 + fee_2) * CROSSING_MARGIN:
        if ((bid_1 - ask_2)/ask_2) >= PROFIT_THR_TO_OPEN_POSITIONS:

            opp_logger.info(',   OPPORTUNITY, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
                    exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, vol_bid_1, ask_2, vol_ask_2, (bid_1 - ask_2)/ask_2, (fee_1+fee_2), (bid_1 - ask_2)/ask_2 - (fee_1+fee_2)))

            logger.info('locking exchanges \t{} and \t{} for \t{}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
            g_storage.exch_locked.append([exch_pair[0], exch_pair[1], coin_pair])

            # if profit is possible exploit the pair
            exploit_pair(exch_pair, coin_pair)

        # (fee_1 + fee_2) * CROSSING_MARGIN:  # in the other direcction
        elif ((bid_2 - ask_1)/ask_1) >= PROFIT_THR_TO_OPEN_POSITIONS:

            opp_logger.info(',R  OPPORTUNITY, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
                    exch_pair[1].name, exch_pair[0].name, coin_pair, bid_2, vol_bid_2, ask_1, vol_ask_1, (bid_2 - ask_1)/ask_1, (fee_1+fee_2), (bid_2 - ask_1)/ask_1 - (fee_1+fee_2)))

            logger.info('locking exchanges \t{} and \t{} for \t{}'.format(exch_pair[1].name, exch_pair[0].name, coin_pair))
            g_storage.exch_locked.append([exch_pair[1], exch_pair[0], coin_pair])

            # if profit is possible in the other direction exploit the pair as well
            exploit_pair(exch_pair, coin_pair, reverse=True)

        # else:  # non proffit branch, just to log data.  TODO: To be removed
        #     opp_logger.info(
        #         ',no opportunity, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
        #             exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, vol_bid_1, ask_2, vol_ask_2, (bid_1 - ask_2)/ask_2, (fee_1+fee_2), (bid_1 - ask_2)/ask_2 - (fee_1+fee_2)))
        #     opp_logger.info(
        #         ',no opportunity, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
        #             exch_pair[1].name, exch_pair[0].name, coin_pair, bid_2, vol_bid_2, ask_1, vol_ask_1, (bid_2 - ask_1)/ask_1, (fee_1+fee_2), (bid_2 - ask_1)/ask_1 - (fee_1+fee_2)))

    else:
        logger.error('some bids or aks are NULL, \t{} \t{} \t{} \t{} \t{} \t{} \t{}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, ask_1, bid_2, ask_2))

    return True


def exploit_pair(exch_pair, coin_pair, reverse=False):
    """
        launches the exploit thread
    """
    if len(g_storage.exploit_threads) < MAX_THREADS:
        g_storage.exploit_thread_number += 1
        if not reverse:
            logger.info('launching {} and {} thread for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
            thread = threading.Thread(target=exploit_thread, name='arb_' + str(g_storage.exploit_thread_number), args=(exch_pair[0], exch_pair[1], coin_pair))
        else:
            logger.info('launching {} and {} thread for {}'.format(exch_pair[1].name, exch_pair[0].name, coin_pair))
            thread = threading.Thread(target=exploit_thread, name='arb_' + str(g_storage.exploit_thread_number), args=(exch_pair[1], exch_pair[0], coin_pair))
        # launch the thread
        # thread = threading.Thread(target=exploit_thread, args=(exch_pair, coin_pair, reverse))
        g_storage.exploit_threads.append(thread)
        thread.setName
        thread.start()
        return True

    return False


def exploit_thread(exch_0, exch_1, coin_pair):
    """
        exploit thread
        it tracks a profit tendency between exchanges and coins pair
        stores the profit until the tendence turns into negative or not profitable
        TODO: this is where we have to IMPLEMENT THE ORDERS!!!!
    """

    # compose the log filename using exchanges and coins pair
    # if not reverse else './logs/' + exch_1.name + '-' + exch_0.name + '-' + coin_pair.replace('/', '-') + '.csv'
    filename = './logs/' + exch_0.name + '-' + exch_1.name + '-' + coin_pair.replace('/', '-') + '.csv'


    thread_number = 'arb_' + str(g_storage.exploit_thread_number)
    logger.info('{} STARTING'.format(thread_number))

    # movements accumulated
    accumulated_base_sold = 0  # on exch 1
    accumulated_base_bought = 0  # on exch 2

    # statistics data, still not used
    iterations = 1
    iterations_failed = 0
    acc_profit = 0

    mean_profit = 0
    ready_to_exit = True

    while True:
        loop_time = time.time()
        now = datetime.now()

        # waits for the exchanges rate limits and gets the order books
        if (g_storage.timer[exch_0.name][1] - (time.time() - g_storage.timer[exch_0.name][0])) > 0:
            time.sleep(g_storage.timer[exch_0.name][1] - (time.time() - g_storage.timer[exch_0.name][0]))

        if (g_storage.timer[exch_1.name][1] - (time.time() - g_storage.timer[exch_1.name][1])) > 0:
            time.sleep(g_storage.timer[exch_1.name][1] - (time.time() - g_storage.timer[exch_1.name][1]))
        # gets the trading sizes and coins names
        base_coin = coin_pair.split('/')[0]
        quote_coin = coin_pair.split('/')[1]

        # locks coins in exchange
        balances.lock_coin(exch_0.name, base_coin)
        balances.lock_coin(exch_0.name, quote_coin)
        balances.lock_coin(exch_1.name, base_coin)
        balances.lock_coin(exch_1.name, quote_coin)

        trading_size_0 = balances.get_coin_balance(exch_0.name, base_coin)['trading_size']
        trading_size_1 = balances.get_coin_balance(exch_1.name, base_coin)['trading_size']

        base_coin_balance_0 = balances.get_coin_balance(exch_0.name, base_coin)['amount']
        quote_coin_balance_1 = balances.get_coin_balance(exch_1.name, quote_coin)['amount']

        base_coin_balance_1 = balances.get_coin_balance(exch_1.name, base_coin)['amount']
        quote_coin_balance_0 = balances.get_coin_balance(exch_0.name, quote_coin)['amount']

        # get the best bid and ask for that amount
        bid = get_selling_price(exch_0, coin_pair, trading_size_0)
        ask = get_buying_price (exch_1, coin_pair, trading_size_1)

        # gets the fees
        try:
            fee_0 = max(exch_0.fees['trading']['maker'], exch_0.fees['trading']['taker'])
        except:
            fee_0 = 0.005
        try:
            fee_1 = max(exch_1.fees['trading']['maker'], exch_1.fees['trading']['taker'])
        except:
            fee_1 = 0.005

        if not bid or not ask:
            logger.info('{}: not enough volume for ordering selling-buying on \t{} and \t{} for \t{}'.format(thread_number, exch_0.name, exch_1.name, coin_pair))
            # delay time with a bit of stagger to avoid always falling on same point
            time.sleep(random.randint(EXPLOIT_THREAD_DELAY + random.randint(-5, 5)))
            continue

        # figure the profit out
        profit = (bid - ask)/ask - (fee_0 + fee_1)

        if profit >= PROFIT_THR_TO_OPEN_POSITIONS:
            acc_profit += profit
            mean_profit = acc_profit/iterations
            
            if (base_coin_balance_0 >= trading_size_0 * (1+fee_0)) and (quote_coin_balance_1 >= (trading_size_1 * (1+fee_1) * ask)):
                logger.info('{}: ordering selling-buying on \t{}/{} \t{}, profit \t{}'.format(thread_number, exch_0.name, exch_1.name, coin_pair, profit))

                # calls selling routine
                balance_logger.info('{}, {}: sell {} of {}, profit {}, full balance \t{}'.format(thread_number, exch_0.name, trading_size_0, coin_pair, profit, balances.get_full_balance()))
                # ----------------------------------------------------------------------
                selling_order_demo(exch_0, coin_pair, bid, trading_size_0, fee_0)
                # ----------------------------------------------------------------------

                balance_logger.info('{}, {}: buy  {} of {}, profit {}, full balance \t{}'.format(thread_number, exch_1.name, trading_size_1, coin_pair, profit, balances.get_full_balance()))
                # ----------------------------------------------------------------------
                buying_order_demo (exch_1, coin_pair, ask, trading_size_1, fee_1)
                # ----------------------------------------------------------------------
                
                balances.update_profit(exch_0.name, base_coin, trading_size_0 * profit)  # update profit on one of the exchanges
                accumulated_base_sold += trading_size_0 * (1 + fee_0)
                accumulated_base_bought += trading_size_1

                iterations += 1
                ready_to_exit = False

            else:
                logger.warning('{}: not enough cash for ordering selling-buying on \t{} and \t{} for \t{}'.format(thread_number, exch_0.name, exch_1.name, coin_pair))
                logger.warning('{} trying to RE-BALANCE...'.format(thread_number))

                if not balancer([exch_0, exch_1], coin_pair):
                    logger.error('{} REBALANCING was not possible'.format(thread_number))
                else:
                    logger.info('{} REBALANCING successful'.format(thread_number))
                
        else:
            logger.info('{}: trading not possible in \t{} and \t{} for \t{}, profit: \t{}'.format(thread_number, exch_0.name, exch_1.name, coin_pair, profit))

            iterations_failed += 1

        if profit <= PROFIT_THR_TO_CLOSE_POSITIONS and accumulated_base_sold >= TRADES_TO_ALLOW_CLOSING * trading_size_1:
            # closing positions
            logger.info('{} CLOSING POSITIONS in \t{} and \t{} for \t{}'.format(thread_number, exch_0.name, exch_1.name, coin_pair))

            balance_logger.info('{}, {}: sell {} of {}, profit {}, full balance \t{}'.format(thread_number, exch_1.name, accumulated_base_bought, coin_pair, profit, balances.get_full_balance()))
            # ----------------------------------------------------------------------
            selling_order_demo(exch_1, coin_pair, bid, accumulated_base_bought, fee_0)
            # ----------------------------------------------------------------------

            balance_logger.info('{}, {}: buy {} of {}, profit {}, full balance \t{}'.format(thread_number, exch_0.name, accumulated_base_sold, coin_pair, profit, balances.get_full_balance()))
            # ----------------------------------------------------------------------
            buying_order_demo (exch_0, coin_pair, ask, accumulated_base_sold, fee_1)
            # ----------------------------------------------------------------------

            balances.update_profit(exch_0.name, base_coin, -accumulated_base_sold * profit)  # update profit on one of the exchanges now in negative cause we are comming back
            accumulated_base_bought = 0
            accumulated_base_sold = 0

            iterations = 1
            ready_to_exit = True
            acc_profit = 0
            mean_profit = 0

        else:
            if iterations_failed >= MAX_ITER_TO_EXIT and ready_to_exit:
                logger.info('{} EXITING'.format(thread_number))
                # unlock coins
                balances.unlock_coin(exch_0.name, base_coin)
                balances.unlock_coin(exch_0.name, quote_coin)
                balances.unlock_coin(exch_1.name, base_coin)
                balances.unlock_coin(exch_1.name, quote_coin)

                return True
        try:
            # delay time with a bit of stagger to avoid always falling on the same point 
            time.sleep(EXPLOIT_THREAD_DELAY + random.randint(-5, 5) - (time.time() - loop_time))
        except:
            pass


def selling_order_demo(exchange, coin_pair, bid, size, fee):
    """ simulate a selling order """

    log_str = '{:3} sell, {:6}, {:9}, bid, {:10.5f}, size, {:9.5f}, fee, {:7.5f}, iBaseBal, {:+12.5f}, fBaseBal, {:+12.5f}, iQuoteBal, {:+12.5f}, fQuoteBal, {:+12.5f}'
    thread_number = threading.currentThread().name

    base_coin = coin_pair.split('/')[0]
    quote_coin = coin_pair.split('/')[1]
    pre_trade_balance_base  = balances.get_coin_balance(exchange.name, base_coin)['amount']
    pre_trade_balance_quote = balances.get_coin_balance(exchange.name, quote_coin)['amount']
    
    # ----------------------------------------------------------------------
    quote_amount = size * bid
    base_amount  = - (size + fee * size)
    # ----------------------------------------------------------------------

    balances.update_balance(exchange.name, base_coin, base_amount)
    balances.update_balance(exchange.name, quote_coin, quote_amount)

    balance_logger.info(log_str.format(
                                                    thread_number,
                                                    exchange.name[:5],
                                                    coin_pair,
                                                    bid,
                                                    size,
                                                    fee,
                                                    pre_trade_balance_base,
                                                    balances.get_coin_balance(exchange.name, base_coin)['amount'],
                                                    pre_trade_balance_quote,
                                                    balances.get_coin_balance(exchange.name, quote_coin)['amount']))
    # list_balances()
    return True


def buying_order_demo(exchange, coin_pair, ask, size, fee):
    """ simulate a buying order """

    # log_str = ' thrd {:3}, {:6}, {:9}, ask, {:10.5f}, size, {:9.5f}, fee, {:7.5f}, iBBal, {:+12.5f}, fBBal, {:+12.5f}, prof, {:+12.5f}, iQBal, {:+12.5f}, fQBal, {:+12.5f}, prof, {:+12.5f}, accProf, {:+11.5f}'
    log_str = '{:3} buy , {:6}, {:9}, ask, {:10.5f}, size, {:9.5f}, fee, {:7.5f}, iBaseBal, {:+12.5f}, fBaseBal, {:+12.5f}, iQuoteBal, {:+12.5f}, fQuoteBal, {:+12.5f}'
    thread_number = threading.currentThread().name

    base_coin = coin_pair.split('/')[0]
    quote_coin = coin_pair.split('/')[1]
    pre_trade_balance_base  = balances.get_coin_balance(exchange.name, base_coin)['amount']
    pre_trade_balance_quote = balances.get_coin_balance(exchange.name, quote_coin)['amount']
    
    # ----------------------------------------------------------------------
    quote_amount = - (size + fee * size) * ask
    base_amount  = size
    # ----------------------------------------------------------------------

    balances.update_balance(exchange.name, base_coin, base_amount)
    balances.update_balance(exchange.name, quote_coin, quote_amount)

    balance_logger.info(log_str.format(
                                                    thread_number,
                                                    exchange.name[:5],
                                                    coin_pair,
                                                    ask,
                                                    size,
                                                    fee,
                                                    pre_trade_balance_base,
                                                    balances.get_coin_balance(exchange.name, base_coin)['amount'],
                                                    pre_trade_balance_quote,
                                                    balances.get_coin_balance(exchange.name, quote_coin)['amount']))
    # list_balances()
    return True


def list_balances(start=False, end=False):
    if start:
        balance_csv_logger.info('<')
    detailed_balance = balances.get_detailed_balance()
    for element in detailed_balance:
        for key, value in zip(element.keys(), element.values()):
            balance_csv_logger.info(', \t{:5}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}, \t{:+.5f}'.format(
                key,
                value[0]['BCH']['amount'],
                value[1]['BTC']['amount'],
                value[2]['ETH']['amount'],
                value[3]['LTC']['amount'],
                value[4]['EOS']['amount'],
                value[5]['XMR']['amount'],
                value[6]['XRP']['amount'],
                value[7]['ZEC']['amount'],

                value[8]['DASH']['amount'],
                value[9]['NEO']['amount'],
                value[10]['PIVX']['amount'],
                value[11]['NANO']['amount'],
                value[12]['ADA']['amount'],

                value[13]['USDC']['amount'],
                value[14]['USDT']['amount'],
                value[15]['EUR']['amount'],
                value[16]['USD']['amount']
            ))
    if end:
        balance_csv_logger.info('>')

    # return 0


def balancer(exch_pair, coin_pair, expctd_profit=0):
    """ 
    exchange coins balancer
    tries to balance the exchanges to allow bot to perform buying and selling the profitable pair.
    If balancing it's not possible on both the exchanges returns a False

    """
    # logger.info('exchange_balancer({}, {}) initiating'.format(exchange.name, coin_dest))
    base_coin = coin_pair.split('/')[0]
    quote_coin = coin_pair.split('/')[1]
    coin_pair_reverse = quote_coin + '/' + base_coin

    exchange_0 = exch_pair[0]
    exchange_1 = exch_pair[1]
    waitting_time = 0.7

    pairs_avail_0 = [pair for pair in exchange_0.markets.keys()]
    pairs_avail_1 = [pair for pair in exchange_1.markets.keys()]
    fee_0 = max(exchange_0.fees['trading']['maker'], exchange_0.fees['trading']['taker'])
    fee_1 = max(exchange_1.fees['trading']['maker'], exchange_1.fees['trading']['taker'])
    
    # remove coin_pair from lists
    try:
        pairs_avail_0.pop(pairs_avail_0.index(coin_pair))
    except:
        pass
    try:
        pairs_avail_0.pop(pairs_avail_0.index(coin_pair_reverse))
    except:
        pass
    try:
        pairs_avail_1.pop(pairs_avail_1.index(coin_pair))
    except:
        pass
    try:
        pairs_avail_1.pop(pairs_avail_1.index(coin_pair_reverse))
    except:
        pass
    
    coin_dest_exch_0 = coin_pair.split('/')[0]  # base coin
    coin_dest_exch_1 = coin_pair.split('/')[1]  # quote coin

    tr_size_coin_dest_0 = balances.get_coin_balance(exchange_0.name, coin_dest_exch_0)['trading_size'] * balances.get_coin_balance(exchange_0.name, coin_dest_exch_0)['change']
    tr_size_coin_dest_1 = balances.get_coin_balance(exchange_1.name, coin_dest_exch_1)['trading_size'] * balances.get_coin_balance(exchange_1.name, coin_dest_exch_1)['change']

    exchange_0_avail_sell = list()
    exchange_0_avail_buy  = list()
    exchange_1_avail_sell = list()
    exchange_1_avail_buy  = list()
    
    # on exchange 0: look for possible pairings to sell or buy
    for coin, coin_name in zip(balances.exchanges[exchange_0.name].values(), balances.exchanges[exchange_0.name].keys()):
        if not coin['in_use'] and str(coin_name + '/' + coin_dest_exch_0) in pairs_avail_0 and coin['amount'] * coin['change'] > 2 * tr_size_coin_dest_0:
            symbol = str(coin_name + '/' + coin_dest_exch_0)
            amount = coin['amount'] * 1/2
            selling_price = get_selling_price(exchange_0, symbol, amount)
            time.sleep(waitting_time)
            if selling_price:
                exchange_0_avail_sell.append([symbol, coin['amount'], coin['amount'] * coin['change'], selling_price])
        
        if not coin['in_use'] and str(coin_dest_exch_0 + '/' + coin_name) in pairs_avail_0 and coin['amount'] * coin['change'] > 2 * tr_size_coin_dest_0:
            symbol = str(coin_dest_exch_0 + '/' + coin_name)
            amount = coin['amount'] * 1/2
            buying_price = get_buying_price(exchange_0, symbol, amount)
            time.sleep(waitting_time)
            if buying_price:
                exchange_0_avail_buy.append([symbol, coin['amount']/buying_price, coin['amount'] * coin['change'], buying_price])

    # on exchange 1: idem ob exch0
    for coin, coin_name in zip(balances.exchanges[exchange_1.name].values(), balances.exchanges[exchange_1.name].keys()):
        if not coin['in_use'] and str(coin_name + '/' + coin_dest_exch_1) in pairs_avail_1 and coin['amount'] * coin['change'] > 2 * tr_size_coin_dest_1:
            symbol = str(coin_name + '/' + coin_dest_exch_1)
            amount = coin['amount'] * 1/2
            selling_price = get_selling_price(exchange_1, symbol, amount)
            time.sleep(waitting_time)
            if selling_price:
                exchange_1_avail_sell.append([symbol, coin['amount'], coin['amount'] * coin['change'], selling_price])
        
        if not coin['in_use'] and str(coin_dest_exch_1 + '/' + coin_name) in pairs_avail_1 and coin['amount'] * coin['change'] > 2 * tr_size_coin_dest_1:
            symbol = str(coin_dest_exch_1 + '/' + coin_name)
            amount = coin['amount'] * 1/2
            buying_price = get_buying_price(exchange_1, symbol, amount)
            time.sleep(waitting_time)
            if buying_price:
                exchange_1_avail_buy.append([symbol, coin['amount']/buying_price, coin['amount'] * coin['change'], buying_price])

    # sort by the equivalent USDC amount to try balancing the the currency with bigger amount
    exchange_0_avail_sell = sorted(exchange_0_avail_sell, key=lambda x: x[2]) if len(exchange_0_avail_sell) else False
    exchange_0_avail_buy  = sorted(exchange_0_avail_buy,  key=lambda x: x[2]) if len(exchange_0_avail_buy ) else False
    exchange_1_avail_sell = sorted(exchange_1_avail_sell, key=lambda x: x[2]) if len(exchange_1_avail_sell) else False
    exchange_1_avail_buy  = sorted(exchange_1_avail_buy,  key=lambda x: x[2]) if len(exchange_1_avail_buy ) else False

    if (exchange_0_avail_sell or exchange_0_avail_buy) and (exchange_1_avail_sell or exchange_1_avail_buy):  # balancing is possible
        if exchange_0_avail_sell:
            bigger_element = exchange_0_avail_sell.pop()
            balance_logger.info('Balancing {}: sell {} of {} pair'.format(exchange_0.name, bigger_element[0], bigger_element[1]))
            selling_order_demo(exchange_0, bigger_element[0], bigger_element[3], bigger_element[1]/2, fee_0)
        
        elif exchange_0_avail_buy:
            bigger_element = exchange_0_avail_buy.pop()
            balance_logger.info('Balancing {}: buy {} of {} pair'.format(exchange_0.name, bigger_element[0], bigger_element[1]))
            buying_order_demo (exchange_0, bigger_element[0], bigger_element[3], bigger_element[1]/2, fee_0)

        if exchange_1_avail_sell:
            bigger_element = exchange_1_avail_sell.pop()
            balance_logger.info('Balancing {}: sell {} of {} pair'.format(exchange_1.name, bigger_element[0], bigger_element[1]))
            selling_order_demo(exchange_1, bigger_element[0], bigger_element[3], bigger_element[1]/2, fee_1)
        elif exchange_1_avail_buy:
            bigger_element = exchange_1_avail_buy.pop()
            balance_logger.info('Balancing {}: buy {} of {} pair'.format(exchange_1.name, bigger_element[0], bigger_element[1]))
            buying_order_demo (exchange_1, bigger_element[0], bigger_element[3], bigger_element[1]/2, fee_1)
        
        return True
    return False


def get_selling_price(exchange, symbol, amount):
    """ returns the best price for selling the amount of the coin's symbol in any exchange
        else: returns a False
    """
    orderbook = exchange.fetch_order_book(symbol)

    bids = [item[0] for item in orderbook['bids'][:]] if len(orderbook['bids']) > 0 else None
    vol_bids = [item[1] for item in orderbook['bids'][:]] if len(orderbook['bids']) > 0 else None
    acc_vol = 0
    for bid, vol in zip(bids, vol_bids):
        acc_vol += vol
        if vol >= amount:
            return bid
    return False


def get_buying_price(exchange, symbol, amount):
    """ returns the best price for buying the amount of the coin's symbol in any exchange
        else: returns a False
    """
    orderbook = exchange.fetch_order_book(symbol)

    asks = [item[0] for item in orderbook['asks'][:]] if len(orderbook['asks']) > 0 else None
    vol_asks = [item[1] for item in orderbook['asks'][:]] if len(orderbook['asks']) > 0 else None
    acc_vol = 0
    for ask, vol in zip(asks, vol_asks):
        acc_vol += vol
        if vol >= amount:
            return ask
    return False


def launch_console(exchanges):

    thread = threading.Thread(target=mini_console, name='miniConsole', args=(exchanges,))
    thread.start()


def mini_console(exchanges):
    welcome_message = '''
    this is leBot command console, '?? for help
    '''
    help_msg = '''
    possible commands are:
            - 'se': shows the exchanges list
            - 'ss': shows status config variables
            - 'sbr [exchName]': shows a raw json balance detailed, if exchName present: same information but for the specified exchange
            - 'sbf [exchName]': shows a json formatted detailed balance, if exchName present: same information but for the specified exchange
            - 'set variableName newValue': changes the current value to newValue on variable
            - 'set1 value': set new arb threshold into PROFIT_THR_TO_OPEN_POSITIONS variable
            - '?': shows this message/help
            (new commands soon!)
    '''
    print(welcome_message)
    global PROFIT_THR_TO_OPEN_POSITIONS

    while True:
        input_str = input('(leBot) > ')
        input_parsed = parse(input_str)

        if len(input_parsed) == 1:
            input_command = input_parsed[0]
            if input_command == 'ss':
                print('DEMO_MODE: ', DEMO_MODE)
                print('DEBUG: ', DEBUG)
                print('DEBUG_LEVEL: ', DEBUG_LEVEL)
                print('LOG_PROFITS: ', LOG_PROFITS)
                print('USE_THREADING: ', USE_THREADING)
                print('TRADING_SIZE: ', TRADING_SIZE)
                print('EXPLOIT_THREAD_DELAY: ', EXPLOIT_THREAD_DELAY)
                print('MAX_THREADS: ', MAX_THREADS)
                print('PROFIT_THR_TO_OPEN_POSITIONS: ', PROFIT_THR_TO_OPEN_POSITIONS)
                print('PROFIT_THR_TO_CLOSE_POSITIONS: ', PROFIT_THR_TO_CLOSE_POSITIONS)
                print('MAX_ITER_TO_EXIT: ', MAX_ITER_TO_EXIT)
                print('TRADES_TO_ALLOW_CLOSING: ', TRADES_TO_ALLOW_CLOSING)
            if input_command == 'se':
                print('exchanges list:')
                print([exchange.name for exchange in exchanges])
            elif input_command == 'sbr':
                balance = balances.get_detailed_balance()
                print(balance)
            elif input_command == 'sbf':
                detail = balances.get_detailed_balance()
                for e in detail:
                    print(json.dumps(e, indent=2))
            elif input_command == '?':
                print(help_msg)
            else:
                print('error: command not found')
        
        elif len(input_parsed)  == 2:
            input_command = input_parsed[0]
            arg = input_parsed[1]

            if input_command == 'sbf':
                try:
                    balance = balances.get_detailed_balance(arg)
                    print(json.dumps(balance, indent=2))
                except:
                    print('not present')

            elif input_command == 'sbr':
                try:
                    balance = balances.get_detailed_balance(arg)
                    print(balance)
                except:
                    print('not present')
            elif input_command == 'set1':
                PROFIT_THR_TO_OPEN_POSITIONS = float(arg)

            else:
                print('error: command not found')
            
            
    return 0


def parse(args):

    'Convert a series of zero or more numbers to an argument tuple'
    return args.split()


def main():
    start_time = time.time()
    exchanges = create_exchanges()
    launch_console(exchanges)
    load_markets_(exchanges)
    init_balances(exchanges)
    coins_prices_updater(exchanges)
    g_storage.initial_balance = balances.get_full_balance()
    
    # list_balances()
    
    exch_pairs = pairs_generator(exchanges)
    pairs_to_cross = cross_exch_pairs(exch_pairs)
    cross_pairs(exch_pairs, pairs_to_cross)

    print("--- %s seconds ---" % (time.time() - start_time))
    print(balances.get_full_balance())


if __name__ == "__main__":
    logger = setup_logger(
        'first_logger', 'logs/logger.log', level=logging.DEBUG)
    opp_logger = setup_logger('second_logger', 'logs/opport.csv')
    balance_csv_logger = setup_logger('third_logger', 'logs/balances.csv')
    balance_logger = setup_logger('fourth_logger', 'logs/balances.log')

    # releasing the beast! XD
    main()
