import ccxt
from datetime import datetime
import time
import logging
import threading



# parameters
DEBUG = True
DEBUG_LEVEL = 0
USE_THREADING = True  # accelerates markets loading by 4.64/0.035 = 132x!


def create_exchanges():
    ''' instantiate and load the markets'''
    coinbasepro = ccxt.coinbasepro()
    poloniex = ccxt.poloniex()
    bittrex = ccxt.bittrex()
    binance = ccxt.binance()
    bitfinex = ccxt.bitfinex()
    kraken = ccxt.kraken()
    bitmex = ccxt.bitmex()
    okex = ccxt.okex()
    exchanges = [coinbasepro, poloniex, bittrex, binance, bitfinex, kraken, bitmex, okex]
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
            thr = threading.Thread(target=load_markets_thread, args=(exchange, force))
            threads.append(thr)
            thr.start()
        for thread, exchange in zip(threads, exchanges):
            exchange = thread.join()


def get_trading_pairs():  # TODO: Check the pairs per exchange
    symbols_matrix = {
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


def main():
    start_time = time.time()
    exchanges = create_exchanges()
    load_markets(exchanges)
    symbols_matrix = get_trading_pairs()

    balances = init_balances(exchanges)

    get_order_books(exchanges, symbols_matrix)
    



    print("--- %s seconds ---" % (time.time() - start_time))
    print(balances.get_full_balance())


if __name__ ==  "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s', filename='testing_1.log', filemode='w', level=logging.DEBUG)
    main()
    
