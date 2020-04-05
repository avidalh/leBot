import ccxt
from datetime import datetime
import time
import logging
import threading
import api_keys


# parameters
DEBUG = True
DEBUG_LEVEL = 0
USE_THREADING = True  # accelerates markets loading by 4.64/0.035 = 132x!


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
    for exch_pair in exch_pairs:
        print()
        print('_' *30)
        print(exch_pair)
        for pair in exch_pair[0].markets.keys():
            if pair in exch_pair[1].markets.keys():  # crossing is possible!
                print(pair)
    return 0


def main():
    start_time = time.time()
    exchanges = create_exchanges()
    load_markets(exchanges)
    symbols_matrix = get_trading_pairs()
    # print(get_market_pairs(exchanges))
    balances = init_balances(exchanges)
    exch_pairs = pairs_generator(exchanges)

    cross_exch_pairs(exch_pairs)
    

    



    print("--- %s seconds ---" % (time.time() - start_time))
    print(balances.get_full_balance())


if __name__ ==  "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s', filename='testing_1.log', filemode='w', level=logging.DEBUG)
    main()
    