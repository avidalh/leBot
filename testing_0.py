import ccxt
from datetime import datetime
import plotly.graph_objects as go
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



coinbasepro = ccxt.coinbasepro()
poloniex = ccxt.poloniex()
bittrex = ccxt.bittrex()
binance = ccxt.binance()
bitfinex = ccxt.bitfinex()
kraken = ccxt.kraken()
bitmex = ccxt.bitmex()
okex = ccxt.okex()

exchanges_avail = [coinbasepro, poloniex, bittrex, binance, bitfinex, kraken, okex]
exchanges_names = ['coinbasepro', 'poloniex', 'bittrex', 'binance', 'bitfinex', 'kraken', 'okex']
exchanges_pairs_names = ['BTC/USDC', 'BTC/USDC', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT', 'BTC/USDT']
exchanges_pairs_names = ['ETH/USDC', 'ETH/USDC', 'ETH/USDT', 'ETH/USDT', 'ETH/USDT', 'ETH/USDT', 'ETH/USDT']

for exchange in exchanges_avail:
    print('{} Symbols: {}', exchange.symbols)


while True:
    start_time = time.time()
    # sometimes exchanges get into maintenance so, if there is the case get that ex out of the list: 
    exchanges_avail_final = []
    exchanges_names_final = []
    exchanges_pairs_names_final = []
    tickers = []
    for exchange, name, trading_pair  in zip(exchanges_avail, exchanges_names, exchanges_pairs_names):
        try:
            tickers.append(exchange.fetch_ticker(trading_pair))  # TODO: use fetchOrderBook instead fetch tiker... it's more fast and has less traffic!!!!
            exchanges_avail_final.append(exchange)
            exchanges_names_final.append(name)
            exchanges_pairs_names_final.append(trading_pair)
        except:
            pass

    print()
    for name, ticker in zip(exchanges_names_final, tickers):
        print(exchanges_names_final.index(name), '-', name, '\t--> : ', ticker['ask'], ', ', ticker['bid'])
    k = 0
    for i in range(len(tickers)):
        print('-' * 40)
        for j in range(len(tickers)):
            if i != j:
                name_1 = exchanges_names_final[i]
                name_2 = exchanges_names_final[j]
                bid_1 = tickers[i]['bid']
                ask_2 = tickers[j]['ask']
                delta_prices = tickers[i]['bid'] - tickers[j]['ask']
                delta_percentage =delta_prices / ask_2

                if abs(delta_percentage) >= 0.004:
                    print('\033[92m {:15} / {:15} \t {:8.3f} / {:8.3f} --> {:5.3} {:9.6%}\033[0m'.format(name_1,
                                                                                                  name_2,
                                                                                                  bid_1,
                                                                                                  ask_2,
                                                                                                  delta_prices,
                                                                                                  delta_percentage))
                else:
                    print('{:15} / {:15} \t {:8.3f} / {:8.3f} --> {:5.3} {:9.6%}'.format(name_1,
                                                                                  name_2, 
                                                                                  bid_1, 
                                                                                  ask_2,
                                                                                  delta_prices,
                                                                                  delta_percentage))
                k += 1

    try:
        time.sleep(2 - time.time() + start_time)
    except ValueError:
        pass
    except KeyboardInterrupt:
        raise

    print("--- %s seconds ---" % (time.time() - start_time))
    print()