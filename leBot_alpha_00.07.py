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

import subprocess

# console modules
# import cmd, sys

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
current_milli_time = lambda: int(round(time.time() * 1000))

# parameters
DEMO_MODE = True                           # TODO: implement a demo mode using balances
DEBUG = True
DEBUG_LEVEL = 0                             # noy used by now

LOG_PROFITS = True

# USE_THREADING = True                        # only used in load markers function, TODO: to remove

COINS_TO_EXPLOIT = 'BTC BCH ETH LTC EOS XMR XRP ZEC DASH NEO PIVX NANO ADA ETC HT LINK ATOM QTUM BNB BSV OF OKB PAX QC TRX USDC USDT EUR USD'
UPDATE_PRICES_PERIOD = 2                    # hours

TRADING_SIZE = 50  #25                      # $25
EXPLOIT_THREAD_DELAY = 30  #15               # exploit thread period
RATE_LIMIT_FACTOR = 1.10  #1.05             # exchange rate limit factor to stay in a "requesting rate" safe zone
MAX_THREADS = 100                           # limiting the number of threads
PROFIT_THR_TO_OPEN_POSITIONS =   0.0000  #+0.000200     # open position threshold (values for testing the bot)
PROFIT_THR_TO_CLOSE_POSITIONS = -0.0000  #-0.000100     # close positions threshold. Close all the positions openend by the arb thread.
ENTRY_THR_FACTOR   = 1  #1.2                 # factor times fees to start a thread
OPERATE_THR_FACTOR = 1  #2.0                 # factor times fees to start a trade/arbitrage
MAX_ITER_TO_EXIT = 5000                      # number of empty iterations to kill the thread
TRADES_TO_ALLOW_CLOSING = 1                 # how many trades to allow closing positions


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
        self.prices_updated = False
        self.accumProfit = 0


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
        'options': {  # ←--------------------- inside 'options' subkey
            'fetchMinOrderAmounts': False,  # ←---------- set to False 
        }
    })

    # bitmex = ccxt.bitmex({
    #     'apiKey': api_keys.bitmex['apiKey'],
    #     'secret': api_keys.bitmex['secret'],
    #     'enableRateLimit': True,
    # })

    okex = ccxt.okex({
        'apiKey': api_keys.okex['apiKey'],
        'secret': api_keys.okex['secret'],
        'enableRateLimit': True,
    })

    exchanges =     [cex, poloniex, bittrex, binance, bitfinex, kucoin, kraken, coinbasepro]
    # timing_limits = [ 1.1,   .35,      1.1,     .35,     1.1,      1.1,    1.1,     .35    ]  # requesting period limit per exchange TODO: to remove

    # # TODO: remove
    # for exchange, timing in zip(exchanges, timing_limits):
    #     g_storage.timer[exchange.name] = [0, timing]

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
    threads = list()
    for exchange in exchanges:
        thread = threading.Thread(target=load_markets_thread, name=str('loadMarket'+exchange.name) ,args=(exchange, force))
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
        balances.update_balance(exchange.name, 'EUR',  100.0000 * FACTOR)
        # initialize other coins to zero balance...
        
    return 0


def fake_balances(exchanges, amount):  # to insert $amount in all wallets for testing
    for exchange in exchanges:
        for coin in g_storage.coins_white_list:
            balances.update_balance(exchange.name, coin, amount/(balances.get_coin_balance(exchange.name, coin)['change']))


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
                continue
            for exchange in exchanges:  # Updates all exchanges balances with the same price.
                    logger.info('coins_price_updater_thread() updating exchange {} coin {} to {} USD'.format(exchange.name, coin, last_price))
                    balances.update_change(exchange.name, coin, last_price)
            time.sleep(.3)
        g_storage.prices_updated = True
        time.sleep(60*60* UPDATE_PRICES_PERIOD)  # wait for the programmed period
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
        logger.info('exchanges {} and {} pairs available: \n {}'.format(exch_pair[0].name, exch_pair[1].name, matched_pairs))
        pairs_to_cross.append(matched_pairs)

    return pairs_to_cross


def cross_pairs(exch_pairs, pairs_to_cross):
    """
        performs the crossing between exchanges using the possible coins pairs
        a random choosing is used.
        TODO: remove the random choosing and use a more consistent way...
    """
    iterations = 0
    while True:  # infinite loop
        loop_time = time.time()

        # first remove finished threads from the threads list:
        for index, thread in enumerate(g_storage.exploit_threads, start=0):
            if thread.is_alive() == False:
                thread.join()
                g_storage.exploit_threads.pop(index)
                g_storage.exch_locked.pop(index)  # TODO: check this
                logger.info('removing finished thread from the list')

        # randomly chooses a pair of coins and call the crossing function
        for index, exch_pair in enumerate(exch_pairs, start=0):
            if len(pairs_to_cross[index]) > 0:
                sweeping_pointer = iterations % len(pairs_to_cross[index])  # creates cycling pointer
                coin_pair = pairs_to_cross[index][sweeping_pointer]  # takes a coins pair on each step
                
                # check if this pair of echanges and coins are already locked
                lock_string = exch_pair[0].name + ' ' + exch_pair[1].name + ' ' + coin_pair
                if lock_string not in g_storage.exch_locked:

                    # launch the crossing procesure
                    # ----------------------------------------------------------------------
                    status = cross(exch_pair, coin_pair, pairs_to_cross[index])
                    # ----------------------------------------------------------------------

                    logger.info('trying {} and {} for {} crossing'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))

                else:
                    logger.info('{} and {} already locked for {}!'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))

        iterations +=1
        logger.info('loop time: {}, threads: {}'.format(time.time() - loop_time, len(g_storage.exploit_threads)))

    return 0


def cross(exch_pair, coin_pair, coin_pairs_avail):
    """
        performs the exchange crossing for the coin pair
        TODO: if an exchange does not respond many times consecutively move it to a waiting list and disable it for requesting
              try again after some minutes. Use a thread for that functionality
    """

    # this is to not over pass the exchanges request limits
    # wait the required time, depending on the exchange

    # if (g_storage.timer[exch_pair[0].name][1] - (time.time() - g_storage.timer[exch_pair[0].name][0])) > 0:
    #     time.sleep(g_storage.timer[exch_pair[0].name][1] - (time.time() - g_storage.timer[exch_pair[0].name][0]))
    
    if  exch_pair[0].rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exch_pair[0].lastRestRequestTimestamp) > 0:
        time.sleep((exch_pair[0].rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exch_pair[0].lastRestRequestTimestamp))/1000)

    # if necessary also waits for the second exchange
    # if (g_storage.timer[exch_pair[1].name][1] - (time.time() - g_storage.timer[exch_pair[1].name][1])) > 0:
    #     time.sleep(g_storage.timer[exch_pair[1].name][1] - (time.time() - g_storage.timer[exch_pair[1].name][1]))

    if  exch_pair[1].rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exch_pair[1].lastRestRequestTimestamp) > 0:
        time.sleep((exch_pair[1].rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exch_pair[1].lastRestRequestTimestamp))/1000)


    try:  # fetch the first order book
        orderbook_1 = exch_pair[0].fetch_order_book(coin_pair, limit=5)
        # timestampts the request/fetch
        # g_storage.timer[exch_pair[0].name][0] = time.time()  # TODO: to remove
    except:
        logger.critical('problems loading order books, request error on \t{}'.format(exch_pair[0].name))
        # if g_storage.timer[exch_pair[0].name][1] <= 4.95:
            # increasing delay. CAUTION HERE!
            # g_storage.timer[exch_pair[0].name][1] += 0.05
            # logger.critical('new timming limit: \t{} seconds'.format(g_storage.timer[exch_pair[0].name][1]))
        # return False
        # TODO: maybe insert a delay here

    try:  # and fetch the second order book
        orderbook_2 = exch_pair[1].fetch_order_book(coin_pair, limit=5)
        # timestampting request
        # g_storage.timer[exch_pair[1].name][0] = time.time()
    except:
        logger.critical('problems loading order books, request error on \t{}'.format(exch_pair[1].name))
        # if g_storage.timer[exch_pair[1].name][1] <= 4.95:
            # increasing delay. CAUTION HERE!
            # g_storage.timer[exch_pair[1].name][1] += 0.05
            # logger.critical('new timming limit: \t{} seconds'.format(g_storage.timer[exch_pair[1].name][1]))
        # return False
        # TODO: maybe insert a delay here
    
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

        # entry threshold 1.2x above the fees:
        if ((bid_1 - ask_2)/ask_2 - ENTRY_THR_FACTOR * (fee_1+fee_2)) >= PROFIT_THR_TO_OPEN_POSITIONS:

            opp_logger.info(',   OPPORTUNITY, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
                    exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, vol_bid_1, ask_2, vol_ask_2, (bid_1 - ask_2)/ask_2, (fee_1+fee_2), (bid_1 - ask_2)/ask_2 - (fee_1+fee_2)))

            logger.info('locking exchanges \t{} and \t{} for \t{}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
            # lock_string = exch_pair[0].name + ' ' + exch_pair[1].name + ' ' + coin_pair
            # g_storage.exch_locked.append(lock_string)

            # if profit is possible exploit the pair
            # ----------------------------------------------------------------------
            exploit_pair(exch_pair, coin_pair, coin_pairs_avail)
            # ----------------------------------------------------------------------

        # in the other direcction
        elif ((bid_2 - ask_1)/ask_1 - ENTRY_THR_FACTOR * (fee_1+fee_2)) >= PROFIT_THR_TO_OPEN_POSITIONS:

            opp_logger.info(',R  OPPORTUNITY, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
                    exch_pair[1].name, exch_pair[0].name, coin_pair, bid_2, vol_bid_2, ask_1, vol_ask_1, (bid_2 - ask_1)/ask_1, (fee_1+fee_2), (bid_2 - ask_1)/ask_1 - (fee_1+fee_2)))

            logger.info('locking exchanges \t{} and \t{} for \t{}'.format(exch_pair[1].name, exch_pair[0].name, coin_pair))
            # lock_string = exch_pair[0].name + ' ' + exch_pair[1].name + ' ' + coin_pair
            # g_storage.exch_locked.append(lock_string)

            # if profit is possible in the other direction exploit the pair as well
            # ----------------------------------------------------------------------
            exploit_pair(exch_pair, coin_pair, coin_pairs_avail, reverse=True)
            # ----------------------------------------------------------------------

    else:
        logger.error('some bids or aks are NULL, \t{} \t{} \t{} \t{} \t{} \t{} \t{}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair, bid_1, ask_1, bid_2, ask_2))

    return True


def exploit_pair(exch_pair, coin_pair, coin_pairs_avail, reverse=False, ):
    """
        launches the exploit thread
    """
    if len(g_storage.exploit_threads) < MAX_THREADS:
        g_storage.exploit_thread_number += 1
        if not reverse:
            logger.info('launching {} and {} thread for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
            thread = threading.Thread(target=exploit_thread,
                                      name='arb_' + str(g_storage.exploit_thread_number) + '_' + exch_pair[0].name + '_' + exch_pair[1].name + '_' + coin_pair,
                                      args=(exch_pair[0], exch_pair[1], coin_pair, coin_pairs_avail))
        else:
            logger.info('launching {} and {} thread for {}'.format(exch_pair[1].name, exch_pair[0].name, coin_pair))
            thread = threading.Thread(target=exploit_thread,
                                      name='arb_' + str(g_storage.exploit_thread_number) + '_' + exch_pair[1].name + '_' + exch_pair[0].name + '_' + coin_pair,
                                      args=(exch_pair[1], exch_pair[0], coin_pair, coin_pairs_avail))
        # launch the thread
        g_storage.exploit_threads.append(thread)
        
        # lock exchs and coin pair        
        lock_string = exch_pair[0].name + ' ' + exch_pair[1].name + ' ' + coin_pair
        g_storage.exch_locked.append(lock_string)

        thread.setName
        thread.start()
        return True

    return False


def exploit_thread(exch_0, exch_1, coin_pair, coin_pairs_avail):
    """
        exploit thread
        it tracks a profit tendency between exchanges and coins pair
        stores the profit until the tendence turns into negative or not profitable
        TODO: this is where we have to IMPLEMENT THE ORDERS!!!!
    """

    # compose the log filename using exchanges and coins pair
    # if not reverse else './logs/' + exch_1.name + '-' + exch_0.name + '-' + coin_pair.replace('/', '-') + '.csv'
    filename = './logs/' + exch_0.name + '-' + exch_1.name + '-' + coin_pair.replace('/', '-') + '.csv'


    thread_name = g_storage.exploit_threads[-1].getName()
    logger.info('{} STARTING'.format(thread_name))

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
        # if (g_storage.timer[exch_0.name][1] - (time.time() - g_storage.timer[exch_0.name][0])) > 0:
        #     time.sleep(g_storage.timer[exch_0.name][1] - (time.time() - g_storage.timer[exch_0.name][0]))
        
        if  exch_0.rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exch_0.lastRestRequestTimestamp) > 0:
            time.sleep((exch_0.rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exch_0.lastRestRequestTimestamp))/1000)

        # if (g_storage.timer[exch_1.name][1] - (time.time() - g_storage.timer[exch_1.name][1])) > 0:
        #     time.sleep(g_storage.timer[exch_1.name][1] - (time.time() - g_storage.timer[exch_1.name][1]))
        
        if  exch_1.rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exch_1.lastRestRequestTimestamp) > 0:
            time.sleep((exch_1.rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exch_1.lastRestRequestTimestamp))/1000)
        
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
        # g_storage.timer[exch_0.name][0] = time.time()  # store requesting time

        ask = get_buying_price (exch_1, coin_pair, trading_size_1)
        # g_storage.timer[exch_1.name][0] = time.time()  # store requesting time

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
            logger.info('{}: not enough volume for ordering selling-buying'.format(thread_name))
            # delay time with a bit of stagger to avoid always falling on same point
            time.sleep(random.randint(EXPLOIT_THREAD_DELAY + random.randint(-5, 5)))
            continue

        # figure the profit out. Operation Threshold: 2x above fees
        profit = (bid - ask)/ask - OPERATE_THR_FACTOR * (fee_0 + fee_1)

        if profit >= PROFIT_THR_TO_OPEN_POSITIONS:
            logger.info('{}: trading possible, net profit: \t{}'.format(thread_name, profit))
            acc_profit += profit
            # g_storage.accumProfit += profit
            mean_profit = acc_profit/iterations
            
            # to delete:
            # a = balances.get_coin_balance(exch_0.name, 'USD')['amount']
            # b = trading_size_0 * (1+fee_0) * balances.exchanges[exch_0.name][base_coin]['change']
            # c = trading_size_1 * (1+fee_1) * ask
            # d = balances.get_coin_balance(exch_1.name, 'USD')['amount'] >= trading_size_1 * (1+fee_1)

            # option 0: ideal: base and quote
            if base_coin_balance_0 >= 1.3* trading_size_0 * (1+fee_0) and quote_coin_balance_1 >= 1.3 *(trading_size_1 * (1+fee_1) * ask):  # this is the ideal situation: money in both coins
                logger.info('{}: ordering selling-buying profit \t{}'.format(thread_name, profit))

                # calls selling routine
                # balance_logger.info('{}, {}: sell {} of {}, profit {}, full balance \t{}'.format(thread_name, exch_0.name, trading_size_0, coin_pair, profit, balances.get_full_balance()))
                # ----------------------------------------------------------------------
                selling_order_demo(exch_0, coin_pair, bid, trading_size_0, fee_0)
                # ----------------------------------------------------------------------

                # balance_logger.info('{}, {}: buy  {} of {}, profit {}, full balance \t{}'.format(thread_name, exch_1.name, trading_size_1, coin_pair, profit, balances.get_full_balance()))
                # ----------------------------------------------------------------------
                buying_order_demo (exch_1, coin_pair, ask, trading_size_1, fee_1)
                # ----------------------------------------------------------------------
                
                balances.update_profit(exch_0.name, base_coin, trading_size_0 * profit)  # update profit on one of the exchanges
                accumulated_base_sold += trading_size_0 * (1 + fee_0)
                accumulated_base_bought += trading_size_1

                iterations += 1
                ready_to_exit = False

            # TODO: finish this block:
            # option 1: USD and quote
            elif balances.get_coin_balance(exch_0.name, 'USD')['amount'] >= 1.3* TRADING_SIZE \
                and quote_coin_balance_1 >= 1.3* (trading_size_1 * (1+fee_1) * ask) \
                and quote_coin + '/USD' in exch_0.markets.keys():
                    logger.info('{} OPTION 1 cash in USD in first exch and cash in quote coin in second exch'.format(thread_name))
                    # TODO: exch0: buy quote coin using USD, exch1: buy base coin (using quote)
                    # 0: profitability study, 1: cancel or execute the op.
                    # get the best bid and ask for that amount
                    
                    pair_to_buy = quote_coin + '/USD'
                    option_1_ask = get_buying_price(exch_0, pair_to_buy, TRADING_SIZE)
                    if option_1_ask:
                        # ----------------------------------------------------------------------
                        buying_order_demo(exch_0, pair_to_buy, option_1_ask, TRADING_SIZE, fee_0)
                        buying_order_demo(exch_1, coin_pair, ask, trading_size_1, fee_1)
                        # ----------------------------------------------------------------------
                        balances.update_profit(exch_0.name, base_coin, trading_size_0 * profit)  # update profit on one of the exchanges
                        accumulated_base_sold += trading_size_0 * (1 + fee_0)
                        accumulated_base_bought += trading_size_1

                        iterations += 1
                        ready_to_exit = False
                    
            # option 2: base and USD
            elif base_coin_balance_0 >= 1.3* trading_size_0 * (1+fee_0) \
                and balances.get_coin_balance(exch_1.name, 'USD')['amount'] >= 1.3* TRADING_SIZE \
                and base_coin + '/USD' in exch_1.markets.keys():
                    logger.info('{} OPTION 2 cash in base coin in first exch and cash in USD in second exch'.format(thread_name))
                    # TODO: exch0: sell base coin (using quote), exch1: buy base coin using USD
                    pair_to_buy = base_coin_balance_1 + '/USD'
                    option_2_ask = get_buying_price(exch_1, pair_to_buy, TRADING_SIZE)
                    if option_2_ask:
                        # ----------------------------------------------------------------------
                        selling_order_demo(exch_0, coin_pair, bid, trading_size_0, fee_0)
                        buying_order_demo (exch_1, pair_to_buy, option_2_ask, trading_size_1, fee_1)
                        # ----------------------------------------------------------------------
                        balances.update_profit(exch_0.name, base_coin, trading_size_0 * profit)  # update profit on one of the exchanges
                        accumulated_base_sold += trading_size_0 * (1 + fee_0)
                        accumulated_base_bought += trading_size_1

                        iterations += 1
                        ready_to_exit = False
            
            # option 3: USD and USD
            elif balances.get_coin_balance(exch_0.name, 'USD')['amount'] >= 1.3* TRADING_SIZE \
                and balances.get_coin_balance(exch_1.name, 'USD')['amount'] >= 1.3* TRADING_SIZE \
                and base_coin + '/USD' in exch_1.markets.keys() \
                and quote_coin + '/USD' in exch_0.markets.keys():
                    logger.info('{} OPTION 3 cash in USD coin in first exch and cash in USD in second exch'.format(thread_name))
                    # TODO: buy quote using USD exch1: buy base using USD
            
            # option 4: USD and BTC
            elif balances.get_coin_balance(exch_0.name, 'USD')['amount'] >= TRADING_SIZE \
                and balances.get_coin_balance(exch_1.name, 'BTC')['amount'] >= balances.get_coin_balance(exch_1.name, 'BTC')['trading_size']:
                    logger.info('{} OPTION 4 cash in USD coin in first exch and cash in BTC in second exch'.format(thread_name))
                    # TODO: 
            
            # option 5: BTC and USD
            elif balances.get_coin_balance(exch_0.name, 'BTC')['amount'] >= balances.get_coin_balance(exch_0.name, 'BTC')['trading_size'] \
                and balances.get_coin_balance(exch_1.name, 'USD')['amount'] >= TRADING_SIZE:
                    logger.info('{} OPTION 5 cash in USD coin in first exch and cash in USD in second exch'.format(thread_name))
                    # TODO
            
            # option 6: BTC and BTC
            elif balances.get_coin_balance(exch_0.name, 'BTC')['amount'] >= balances.get_coin_balance(exch_0.name, 'BTC')['trading_size'] \
                and balances.get_coin_balance(exch_1.name, 'BTC')['amount'] >= balances.get_coin_balance(exch_1.name, 'BTC')['trading_size']:
                    logger.info('{} OPTION 6 cash in USD coin in first exch and cash in USD in second exch'.format(thread_name))            
                    # TODO
            # TODO: include options for EUR
            # the strategie is using stable coins for trading always going back the operations. 


            else:
                logger.warning('{}: not enough cash for ordering selling-buying'.format(thread_name))
                
        else:
            logger.info('{}: trading not possible, net profit: \t{}'.format(thread_name, profit))

            iterations_failed += 1

        if profit <= PROFIT_THR_TO_CLOSE_POSITIONS and accumulated_base_sold >= TRADES_TO_ALLOW_CLOSING * trading_size_1:
            # closing positions
            logger.info('{} CLOSING POSITIONS'.format(thread_name))

            # balance_logger.info('{}, {}: buy {} of {}, profit {}, full balance \t{}'.format(thread_name, exch_0.name, accumulated_base_sold, coin_pair, profit, balances.get_full_balance()))

            # ----------------------------------------------------------------------
            buying_order_demo (exch_0, coin_pair, ask, accumulated_base_sold, fee_1)
            # ----------------------------------------------------------------------

            # balance_logger.info('{}, {}: sell {} of {}, profit {}, full balance \t{}'.format(thread_name, exch_1.name, accumulated_base_bought, coin_pair, profit, balances.get_full_balance()))

            # ----------------------------------------------------------------------
            selling_order_demo(exch_1, coin_pair, bid, accumulated_base_bought, fee_0)
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
                logger.info('{} EXITING'.format(thread_name))
                # unlock coins
                balances.unlock_coin(exch_0.name, base_coin)
                balances.unlock_coin(exch_0.name, quote_coin)
                balances.unlock_coin(exch_1.name, base_coin)
                balances.unlock_coin(exch_1.name, quote_coin)

                return True
        try:
            # delay time with a bit of stagger to avoid always falling on the same point 
            time.sleep(EXPLOIT_THREAD_DELAY + random.randint(0, 3) - (time.time() - loop_time))
        except:
            pass


def check_profitab(exch_0, exch_1, b0, q0, fee_0, tz0, b1, q1, fee_1, tz1):
    """ TODO: finish this procedure """
    coin_pair_0 =   b0 + '/' + q0
    r_coin_pair_0 = q0 + '/' + b0
    coin_pair_1 =   b1 + '/' + q1
    r_coin_pair_1 = q1 + '/' + b1

    bid = get_selling_price(exch_0, coin_pair_0, tz0)

    if not bid:
        bid = get_buying_price(exch_0, r_coin_pair_0, tz0)
        if bid:
            bb_factor = get_selling_price(exch_0, [b1 + '/' + b0], 0)
            if bb_factor:
                bid *= bb_factor
            else:
                bb_factor = get_buying_price(exch_0, [b1 + '/' + b0], 0)
                if not bb_factor: return False
                bid *= bb_factor
        else:
            return False
    else:
        bb_factor = get_selling_price(exch_0, [b1 + '/' + b0], 0)
        bid *= bb_factor

    bb_factor = get_selling_price(exch_0, [b1 + '/' + b0], 0)
    if bb_factor:
        bid *= bb_factor
    else:
        bb_factor = get_buying_price(exch_0, [b1 + '/' + b0], 0)
        bid /= bb_factor

    ask = get_buying_price (exch_1, coin_pair_1, tz1)
    if not ask:
        ask = get_selling_price (exch_1, coin_pair_1, tz1)
    if not ask:
        return False
    
    profitab = (bid - ask)/ask - fee_0 - fee_1

    return profitab


def selling_order_demo(exchange, coin_pair, bid, size, fee):
    """ simulate a selling order """

    log_str = '{:3} sell, {:6}, {:9}, bid, {:10.5f}, size, {:9.5f}, fee, {:7.5f}, iBaseBal, {:+12.5f}, fBaseBal, {:+12.5f}, iQuoteBal, {:+12.5f}, fQuoteBal, {:+12.5f}, operProfit, {:+12.5f}, accProfit, {:+12.5f}'
    thread_name = threading.currentThread().name

    base_coin = coin_pair.split('/')[0]
    quote_coin = coin_pair.split('/')[1]
    pre_trade_balance_base  = balances.get_coin_balance(exchange.name, base_coin)['amount']
    pre_trade_balance_quote = balances.get_coin_balance(exchange.name, quote_coin)['amount']
    full_balance_i = balances.get_full_balance()

    # ----------------------------------------------------------------------
    quote_amount = size * bid
    base_amount  = - (size + fee * size)
    # ----------------------------------------------------------------------

    balances.update_balance(exchange.name, base_coin, base_amount)
    balances.update_balance(exchange.name, quote_coin, quote_amount)
    full_balance_f = balances.get_full_balance()

    full_balance_f = balances.get_full_balance()
    g_storage.accumProfit += full_balance_f - full_balance_i

    balance_logger.info(log_str.format(
                                                    thread_name,
                                                    exchange.name[:5],
                                                    coin_pair,
                                                    bid,
                                                    size,
                                                    fee,
                                                    pre_trade_balance_base,
                                                    balances.get_coin_balance(exchange.name, base_coin)['amount'],
                                                    pre_trade_balance_quote,
                                                    balances.get_coin_balance(exchange.name, quote_coin)['amount'],
                                                    full_balance_f - full_balance_i,
                                                    g_storage.accumProfit)
                                                    )
    # list_balances()
    return True


def buying_order_demo(exchange, coin_pair, ask, size, fee):
    """ simulate a buying order """

    # log_str = ' thrd {:3}, {:6}, {:9}, ask, {:10.5f}, size, {:9.5f}, fee, {:7.5f}, iBBal, {:+12.5f}, fBBal, {:+12.5f}, prof, {:+12.5f}, iQBal, {:+12.5f}, fQBal, {:+12.5f}, prof, {:+12.5f}, accProf, {:+11.5f}'
    log_str = '{:3} buy , {:6}, {:9}, ask, {:10.5f}, size, {:9.5f}, fee, {:7.5f}, iBaseBal, {:+12.5f}, fBaseBal, {:+12.5f}, iQuoteBal, {:+12.5f}, fQuoteBal, {:+12.5f}, operProfit, {:+12.5f}, accProfit, {:+12.5f}'
    thread_name = threading.currentThread().name

    base_coin = coin_pair.split('/')[0]
    quote_coin = coin_pair.split('/')[1]
    pre_trade_balance_base  = balances.get_coin_balance(exchange.name, base_coin)['amount']
    pre_trade_balance_quote = balances.get_coin_balance(exchange.name, quote_coin)['amount']
    full_balance_i = balances.get_full_balance()

    # ----------------------------------------------------------------------
    quote_amount = - (size + fee * size) * ask
    base_amount  = size
    # ----------------------------------------------------------------------

    balances.update_balance(exchange.name, base_coin, base_amount)
    balances.update_balance(exchange.name, quote_coin, quote_amount)
    full_balance_f = balances.get_full_balance()
    g_storage.accumProfit += full_balance_f - full_balance_i

    balance_logger.info(log_str.format(
                                                    thread_name,
                                                    exchange.name[:5],
                                                    coin_pair,
                                                    ask,
                                                    size,
                                                    fee,
                                                    pre_trade_balance_base,
                                                    balances.get_coin_balance(exchange.name, base_coin)['amount'],
                                                    pre_trade_balance_quote,
                                                    balances.get_coin_balance(exchange.name, quote_coin)['amount'],
                                                    full_balance_f - full_balance_i,
                                                    g_storage.accumProfit)
                                                    )
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
    # TODO: insert waiting loop and try/exceptions
    if  exchange.rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exchange.lastRestRequestTimestamp) > 0:
        time.sleep((exchange.rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exchange.lastRestRequestTimestamp))/1000)

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
    # TODO: insert waiting loop and try/exceptions
    if  exchange.rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exchange.lastRestRequestTimestamp) > 0:
        time.sleep((exchange.rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exchange.lastRestRequestTimestamp))/1000)

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
    this is leBot command console, '?' for help
    '''
    help_msg = '''
    possible commands are:
            - 'se': shows the exchanges list
            - 'ss': shows status config variables
            - 'sbr [exchName]': shows a raw json balance detailed, if exchName present: same information but for the specified exchange
            - 'sbf [exchName]': shows a json formatted detailed balance, if exchName present: same information but for the specified exchange
            - 'set variableName newValue': changes the current value to newValue on variable
            - 'sel': show list of exch pairs/coins locked
            - 'sat': show active list of active threads
            - 'set0 value': set new amount into TRADING_SIZE variable
            - 'set1 value': set new arb threshold into PROFIT_THR_TO_OPEN_POSITIONS variable
            - 'set2 value': set new arb threshold into PROFIT_THR_TO_CLOSE_POSITIONS variable
            - 'set3 value': set new arb threshold into ENTRY_THR_FACTOR variable
            - 'set4 value': set new arb threshold into OPERATE_THR_FACTOR variable
            - '?': shows this message/help
            (new commands soon!)
    '''
    print(welcome_message)
    global TRADING_SIZE
    global PROFIT_THR_TO_OPEN_POSITIONS
    global PROFIT_THR_TO_CLOSE_POSITIONS
    global ENTRY_THR_FACTOR
    global OPERATE_THR_FACTOR
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
                print('TRADING_SIZE: ', TRADING_SIZE)
                print('EXPLOIT_THREAD_DELAY: ', EXPLOIT_THREAD_DELAY)
                print('MAX_THREADS: ', MAX_THREADS)
                print('PROFIT_THR_TO_OPEN_POSITIONS: ', PROFIT_THR_TO_OPEN_POSITIONS)
                print('PROFIT_THR_TO_CLOSE_POSITIONS: ', PROFIT_THR_TO_CLOSE_POSITIONS)
                print('ENTRY_THR_FACTOR: ', ENTRY_THR_FACTOR)
                print('OPERATE_THR_FACTOR: ', OPERATE_THR_FACTOR)
                print('TRADES_TO_ALLOW_CLOSING: ', TRADES_TO_ALLOW_CLOSING)
            
            elif input_command == 'se':
                print('exchanges list:')
                print([exchange.name for exchange in exchanges])
            
            elif input_command == 'sbr':
                balance = balances.get_detailed_balance()
                print(balance)
            
            elif input_command == 'sbf':
                detail = balances.get_detailed_balance()
                for e in detail:
                    print(json.dumps(e, indent=2))
            
            elif input_command == 'sel':
                print('exchanges pairs locked {}'.format(len(g_storage.exch_locked)))
                for e in g_storage.exch_locked:
                    print(e)
            
            elif input_command == 'sat':
                print('active treads {}'.format(len(g_storage.exploit_threads)))
                for e in g_storage.exploit_threads:
                    print(e)
            
            # elif input_command == 'pp':
            #     subprocess.run(["python", "pltProfits.py"])
            
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

            elif input_command == 'set0':
                TRADING_SIZE = float(arg)
            
            elif input_command == 'set1':
                PROFIT_THR_TO_OPEN_POSITIONS = float(arg)
            
            elif input_command == 'set2':
                PROFIT_THR_TO_CLOSE_POSITIONS = float(arg)

            elif input_command == 'set3':
                ENTRY_THR_FACTOR = float(arg)
            
            elif input_command == 'set4':
                OPERATE_THR_FACTOR = float(arg)

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

    while not g_storage.prices_updated:
        logger.info('waiting for prices updater...')
        time.sleep(1)

    # fake_balances(exchanges, 1000)  # TODO: to remove. Just to test

    g_storage.initial_balance = balances.get_full_balance()
    
    # list_balances()
    
    exch_pairs = pairs_generator(exchanges)
    pairs_to_cross = cross_exch_pairs(exch_pairs)
    cross_pairs(exch_pairs, pairs_to_cross)

    print("--- %s seconds ---" % (time.time() - start_time))
    print(balances.get_full_balance())


if __name__ == "__main__":
    g_storage = GlobalStorage()
    g_storage.coins_white_list =  COINS_TO_EXPLOIT.split()
    balances = Balance()

    logger = setup_logger('first_logger', 'logs/logger.log', level=logging.DEBUG)
    opp_logger = setup_logger('second_logger', 'logs/opport.csv')
    balance_csv_logger = setup_logger('third_logger', 'logs/balances.csv')
    balance_logger = setup_logger('fourth_logger', 'logs/balances.log')

    # releasing the beast! XD
    main()
