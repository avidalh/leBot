import ccxt
from datetime import datetime
# import plotly.graph_objects as go
import time

from ccxt.base.decimal_to_precision import ROUND                 # noqa F401


'''
# collect the candlestick data from Binance
# binance = ccxt.binance()
# binance_trading_pair = 'BTC/USDT'
# candles_binance = binance.fetch_ohlcv(trading_pair, '1m')

# coinbase = ccxt.coinbase()
# candles_coinbase = binance.fetch_ticker(trading_pair)  #fetch_ohlcv(trading_pair, '1m')


# dates = []
# open_data = []
# high_data = []
# low_data = []
# close_data = []
# format the data to match the charting library
# for candle in candles_binance:
#     dates.append(datetime.fromtimestamp(candle[0] / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f'))
#     open_data.append(candle[1])
#     high_data.append(candle[2])
#     low_data.append(candle[3])
#     close_data.append(candle[4])
# plot the candlesticks
# fig = go.Figure(data=[go.Candlestick(x=dates,
#                        open=open_data, high=high_data,
#                        low=low_data, close=close_data)])
# fig.show()
'''


# parameters
DEBUG = False
REQUEST_PERIOD = 2
PROFIT_THRESHOLD = 0.8 
TRADING_SIZE_USD = 20  # USD or USDT, or equivalent amount in other coins
TRADING_SIZE_BTC = 0.0030


# exchanges list
coinbasepro = ccxt.coinbasepro()
poloniex = ccxt.poloniex()
bittrex = ccxt.bittrex()
binance = ccxt.binance()
bitfinex = ccxt.bitfinex()
kraken = ccxt.kraken()
bitmex = ccxt.bitmex()
okex = ccxt.okex()

# move them into a list
exchanges_avail = [coinbasepro, poloniex, bittrex, binance, bitfinex, kraken, bitmex, okex]

# load the markets
for exchange in exchanges_avail:
    try:
        exchange.load_markets()
    except:
        pass

# and get exchanges names
exchanges_names = [exchange.name for exchange in exchanges_avail]

# get the symbols (just for information)
if DEBUG:
    for exchange, name in zip(exchanges_avail, exchanges_names):
        print('\n------------------------- ', name, ' -------------------------')
        print(exchange.symbols)

# exchanges_pairs_names = ['BTC/USDC', 'BTC/USDC', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT']
# exchanges_pairs_names = ['ETH/USDC', 'ETH/USDC', 'ETH/USDT', 'ETH/USDT', 'ETH/USDT', 'ETH/USDT', 'ETH/USDT']

# pairs matrix
symbols_matrix = [['BTC/USDC', 'BTC/USDC', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT'],
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

# creates an json's array with the balances:[{exchange_name:{USDT: xxx.xx}, {BTC: yyy.yyy}}]
balances = {}
amount_per_coin = 1000
def round_figure(f): return float(ccxt.decimal_to_precision(f, rounding_mode=ROUND, precision=8))
for name in exchanges_names:  # fill the balances, this will be populated using the market information in the real life
    balances[name] = {'BTC': round_figure(amount_per_coin / 6500),
                      'ETH': round_figure(amount_per_coin /133.32),
                      'BCH': round_figure(amount_per_coin /221.9), 
                      'ZEC': round_figure(amount_per_coin /30.7),
                      'LTC': round_figure(amount_per_coin /39.1),
                      'XRP': round_figure(amount_per_coin /0.178),
                      'ADA': round_figure(amount_per_coin /0.03034),
                      'IOTA': round_figure(amount_per_coin /.1445),
                      'USDT': round_figure(amount_per_coin),
                      'USDC': round_figure(amount_per_coin),
                      'USD': round_figure(amount_per_coin)}

# for exchange in exchanges_avail:
#     print('{} Symbols: {}', exchange.symbols)

# trading size per coin, aprox $20
trading_sizes = {'BTC': .0030,
                 'ETH': 0.086,
                 'BCH': 0.135,
                 'ZEC': 0.666,
                 'LTC': 0.51,
                 'XRP': 117.0,
                 'ADA': 666.0,
                 'IOTA': 142.0,
                 'USDT': 20.0,
                 'USDC': 20.0,
                 'USD': 20.0}

# prices matrix
closing_prices = {
                 'BTC': 0.0,
                 'ETH': 0.0,
                 'BCH': 0.0,
                 'ZEC': 0.0,
                 'LTC': 0.0,
                 'XRP': 0.0,
                 'ADA': 0.0,
                 'IOTA': 0.0,
                 'USDT': 0.0,
                 'USDC': 0.0,
                 'USD': 0.0}

def print_balances():
    for exchange in balances:
        for key in exchange:
            pass




while True:
    # sometimes exchanges get into maintenance so, just in case get that exchange out of the list: 
    exchanges_avail_confirmed = []
    exchanges_names_confirmed = []
    exchanges_pairs_names_confirmed = []
    # exchanges_fees_confirmed = []
    # tickers = []

    for symbols_row in symbols_matrix:  #[symbols_matrix[-1]]:
        start_time = time.time()
        bids = []
        asks = []
        spreads = []
        fees_taker = []
        fees_maker = []
        for exchange, name, trading_pair  in zip(exchanges_avail, exchanges_names, symbols_row):
            try:
                orderbook = exchange.fetch_order_book (trading_pair)
                bids.append(orderbook['bids'][0][0] if len (orderbook['bids']) > 0 else None)
                asks.append(orderbook['asks'][0][0] if len (orderbook['asks']) > 0 else None)
                spreads.append((asks[-1] - bids[-1]) if (bids[-1] and asks[-1]) else None)
                fees_taker.append(exchange.markets[trading_pair]['taker'])
                fees_maker.append(exchange.markets[trading_pair]['maker'])
                # print (exchange.id, 'market price', { 'bid': bid, 'ask': ask, 'spread': spread })

                exchanges_avail_confirmed.append(exchange)
                exchanges_names_confirmed.append(exchange.name)
                exchanges_pairs_names_confirmed.append(trading_pair)

            except:
                pass

        print('\n\t\tExchanges available for trading pair: ', trading_pair)
        for name, ask, bid, spread in zip(exchanges_names_confirmed, asks, bids, spreads):
            print(exchanges_names_confirmed.index(name), '-', name, '\t--> : ', ask, ', ', bid, ', ', spread)
        for i in range(len(asks)):
            # print('-' * 40)
            for j in range(len(asks)):
                if i != j:

                    exch_1 = exchanges_names_confirmed[i]
                    exch_2 = exchanges_names_confirmed[j]
                    bid_1 = bids[i]
                    ask_2 = asks[j]
                    fee_1 = fees_maker[i]  # double check the order here!!!!
                    fee_2 = fees_taker[j]  # and here
                    fee_sum = fee_1 + fee_2
                    delta_prices = bid_1 - ask_2
                    delta_percentage = delta_prices / ask_2

                    if delta_percentage >= 0.004:  #fee_sum:
                        print('-' * 40)
                        print('\033[92m {},\t {},\t {:8.3f},\t {:8.3f},\t {:7.3},\t {:7.3%},\t {:7.3%},\t {:7.3%},\t {:7.3%} \033[0m'.format(exch_1,
                                                                                                            exch_2,
                                                                                                            bid_1,
                                                                                                            ask_2,
                                                                                                            delta_prices,
                                                                                                            delta_percentage,
                                                                                                            fee_1,
                                                                                                            fee_2,
                                                                                                            fee_sum))

                        # possible_opportunity() ****************** operate here **********************
                        print('Opportunity...')
                        bal_exch_1_coin_1 = balances[exch_1][trading_pair.split('/')[0]]
                        bal_exch_2_coin_1 = balances[exch_2][trading_pair.split('/')[0]]
                        bal_exch_1_coin_2 = balances[exch_1][trading_pair.split('/')[1]]
                        bal_exch_2_coin_2 = balances[exch_2][trading_pair.split('/')[1]]
                        sell_size = trading_sizes[trading_pair.split('/')[0]]
                        buy_size = trading_sizes[trading_pair.split('/')[1]]
                        if  bal_exch_1_coin_1 >= sell_size and  bal_exch_2_coin_2 >= buy_size:
                            # lets operate
                            print(bal_exch_1_coin_1, bal_exch_1_coin_2, bal_exch_2_coin_1, bal_exch_2_coin_2)
                            
                            bal_exch_1_coin_1 -= (sell_size * (1+fee_1))
                            bal_exch_1_coin_2 += (sell_size * bid_1)

                            bal_exch_2_coin_2 -= (buy_size * (1+fee_2))
                            bal_exch_2_coin_1 += (buy_size / ask_2)  

                            balances[exch_1][trading_pair.split('/')[0]] = bal_exch_1_coin_1
                            balances[exch_2][trading_pair.split('/')[0]] = bal_exch_2_coin_1
                            balances[exch_1][trading_pair.split('/')[1]] = bal_exch_1_coin_2
                            balances[exch_2][trading_pair.split('/')[1]] = bal_exch_2_coin_2
                            print(bal_exch_1_coin_1, bal_exch_1_coin_2, bal_exch_2_coin_1, bal_exch_2_coin_2,)
                        else:
                            print('No cash available on any of those coins...')

                    else:
                        pass
                        # print('No opportunity...')
                        # print('-' * 40)
                        # print('{:12} / {:12} \t {:8.3f} / {:8.3f} --> {:5.3} {:7.3} {:7.3} {:7.3} {:7.3}'.format(exch_1,
                        #                                                                     exch_2, 
                        #                                                                     bid_1, 
                        #                                                                     ask_2,
                        #                                                                     delta_prices,
                        #                                                                     delta_percentage,
                        #                                                                     fee_1*100,
                        #                                                                     fee_2*100,
                        #                                                                     fee_1*100*2 + fee_2*100*2))

        try:
            time.sleep(REQUEST_PERIOD - time.time() + start_time)
        except ValueError:
            pass
        except KeyboardInterrupt:
            print('Exiting the app...')
            raise

        print("--- %s seconds ---" % (time.time() - start_time))
        print()