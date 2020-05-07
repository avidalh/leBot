import threading

def load_markets(exchanges, force=False):
    thread = threading.Thread(target=load_markets_, name='loadMarkets', args=(exchanges, force))
    thread.start()


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