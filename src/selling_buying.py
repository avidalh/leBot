import threading
import time
from src.wallet import balances
from src.storage import storage
from src.setup_logger import balance_logger
from src.params import RATE_LIMIT_FACTOR


current_milli_time = lambda: int(round(time.time() * 1000))


def selling_order_demo(exchange, coin_pair, bid, size, fee):
    """ simulate a selling order """

    log_str = ', {:3} sell, {:6}, {:9}, bid, {:10.5f}, size, {:9.5f}, fee, {:7.5f}, iBaseBal, {:+12.5f}, fBaseBal, {:+12.5f}, iQuoteBal, {:+12.5f}, fQuoteBal, {:+12.5f}, operProfit, {:+12.5f}, accProfit, {:+12.5f}'
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
    storage.accumProfit += full_balance_f - full_balance_i

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
                                                    storage.accumProfit)
                                                    )
    return True


def buying_order_demo(exchange, coin_pair, ask, size, fee):
    """ simulate a buying order """

    # log_str = ' thrd {:3}, {:6}, {:9}, ask, {:10.5f}, size, {:9.5f}, fee, {:7.5f}, iBBal, {:+12.5f}, fBBal, {:+12.5f}, prof, {:+12.5f}, iQBal, {:+12.5f}, fQBal, {:+12.5f}, prof, {:+12.5f}, accProf, {:+11.5f}'
    log_str = ', {:3} buy , {:6}, {:9}, ask, {:10.5f}, size, {:9.5f}, fee, {:7.5f}, iBaseBal, {:+12.5f}, fBaseBal, {:+12.5f}, iQuoteBal, {:+12.5f}, fQuoteBal, {:+12.5f}, operProfit, {:+12.5f}, accProfit, {:+12.5f}'
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
    storage.accumProfit += full_balance_f - full_balance_i

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
                                                    storage.accumProfit)
                                                    )
    return True


def get_selling_price(exchange, symbol, amount):
    """ returns the best price for selling the amount of the coin's symbol in any exchange
        else: returns a False
    """
    # TODO: insert waiting loop and try/exceptions
    if  exchange.rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exchange.lastRestRequestTimestamp) > 0:
        try:
            time.sleep((exchange.rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exchange.lastRestRequestTimestamp))/1000)
        except:
            pass
    try:
        orderbook = exchange.fetch_order_book(symbol)
    except:
        return False

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
        try:
            time.sleep((exchange.rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exchange.lastRestRequestTimestamp))/1000)
        except:
            pass
    try:
        orderbook = exchange.fetch_order_book(symbol)
    except:
        return False

    asks = [item[0] for item in orderbook['asks'][:]] if len(orderbook['asks']) > 0 else None
    vol_asks = [item[1] for item in orderbook['asks'][:]] if len(orderbook['asks']) > 0 else None
    acc_vol = 0
    for ask, vol in zip(asks, vol_asks):
        acc_vol += vol
        if vol >= amount:
            return ask
    return False
