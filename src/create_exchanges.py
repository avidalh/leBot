import ccxt
import src.api_keys as api_keys

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

    exchanges =     [coinbasepro, cex, poloniex, bittrex, binance, bitfinex, kucoin, kraken, bitmex, okex]

    return exchanges

