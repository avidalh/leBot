from src.params import TRADING_SIZE, UPDATE_PRICES_PERIOD
from src.storage import storage
from src.setup_logger import logger
import requests
import threading
import time

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
    FACTOR = 10  #3.333
    for exchange in exchanges:
        for coin in storage.coins_white_list:
            balances.set_balance(exchange.name, coin, 0, 0, 0)
        
        balances.update_balance(exchange.name, 'BTC',  000.0133 * FACTOR)
        balances.update_balance(exchange.name, 'USD',  100.0000 * FACTOR)
        balances.update_balance(exchange.name, 'EUR',  100.0000 * FACTOR)
        # initialize other coins to zero balance...
        
    return 0


def coins_prices_updater(exchanges):

    """ launches threads for basic operations """
    logger.info('launching basic threads')
    logger.info('launching coin change updater')
    thread = threading.Thread(target=coins_prices_updater_thread, name='coinsPricesUpdaterThread', args=(exchanges,))
    thread.start()
    while not storage.prices_updated:
        time.sleep(1)
    return True


def coins_prices_updater_thread(exchanges):
    """ updates the cryptos price to USD """

    BASE_URL = 'https://min-api.cryptocompare.com/data/price?fsym={}&tsyms=USD'
    while True:
        for coin in storage.coins_white_list:
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
        storage.prices_updated = True
        time.sleep(60*60* UPDATE_PRICES_PERIOD)  # wait for the programmed period
    return True


