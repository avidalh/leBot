import ccxt
from datetime import datetime
# import plotly.graph_objects as go
import time

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

DEBUG = False
REQUEST_PERIOD = 2

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
exchanges_avail = [coinbasepro, poloniex, bittrex, binance, bitfinex, kraken,bitmex, okex]

for exchange in exchanges_avail:
    exchange.load_markets()


# get exchanges names
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
                  ['XRP/USD', 'XRP/USD', 'XRP/USD', 'XRP/USD', 'XRP/USD', 'XRP/USD', 'XRP/USD', 'XRP/USD']
                 ]

# si usamos varias monedas en el mismo script se realentiza demasiado! :-/

# for exchange in exchanges_avail:
#     print('{} Symbols: {}', exchange.symbols)





while True:
    # sometimes exchanges get into maintenance so, just in case get that exchange out of the list: 
    exchanges_avail_confirmed = []
    exchanges_names_confirmed = []
    exchanges_pairs_names_confirmed = []
    # tickers = []

    for symbols_row in symbols_matrix:  #[symbols_matrix[-1]]:
        start_time = time.time()
        bids = []
        asks = []
        spreads = []
        for exchange, name, trading_pair  in zip(exchanges_avail, exchanges_names, symbols_row):
            try:
                orderbook = exchange.fetch_order_book (trading_pair)
                bids.append(orderbook['bids'][0][0] if len (orderbook['bids']) > 0 else None)
                asks.append(orderbook['asks'][0][0] if len (orderbook['asks']) > 0 else None)
                spreads.append((asks[-1] - bids[-1]) if (bids[-1] and asks[-1]) else None)
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
            print('-' * 40)
            for j in range(len(asks)):
                if i != j:

                    exch_1 = exchanges_names_confirmed[i]
                    exch_2 = exchanges_names_confirmed[j]
                    bid_1 = asks[i]
                    ask_2 = bids[j]
                    delta_prices = bid_1 - ask_2
                    delta_percentage = delta_prices / ask_2 * 100

                    if abs(delta_percentage) >= 0.8:
                        print('\033[92m {:15} / {:15} \t {:8.3f} / {:8.3f} --> {:5.3} {:9.6} **** \033[0m'.format(exch_1,
                                                                                                            exch_2,
                                                                                                            bid_1,
                                                                                                            ask_2,
                                                                                                            delta_prices,
                                                                                                            delta_percentage))
                        # possible_opportunity()

                    else:
                        print('{:15} / {:15} \t {:8.3f} / {:8.3f} --> {:5.3} {:9.6}'.format(exch_1,
                                                                                            exch_2, 
                                                                                            bid_1, 
                                                                                            ask_2,
                                                                                            delta_prices,
                                                                                            delta_percentage))

        try:
            time.sleep(REQUEST_PERIOD - time.time() + start_time)
        except ValueError:
            pass
        except KeyboardInterrupt:
            raise

        print("--- %s seconds ---" % (time.time() - start_time))
        print()