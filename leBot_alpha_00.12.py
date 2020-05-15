from datetime import datetime
import time
import threading
import random
from itertools import combinations

# src/ imports
from src.setup_logger import logger, balance_logger, balance_csv_logger, opp_logger
from src.markets import load_markets,load_markets_, load_markets_thread, get_market_pairs
from src.wallet import Wallet, init_balances, balances, coins_prices_updater
from src.params import (
    UPDATE_PRICES_PERIOD,
    RATE_LIMIT_FACTOR,
    ENTRY_THR_FACTOR,
    OPERATE_THR_FACTOR,
    PROFIT_THR_TO_OPEN_POSITIONS,
    PROFIT_THR_TO_OPEN_POSITIONS,
    MAX_THREADS,
    EXPLOIT_THREAD_DELAY,
    TRADING_SIZE_MARGIN,
    TRADING_SIZE,
    PROFIT_THR_TO_CLOSE_POSITIONS,
    TRADES_TO_ALLOW_CLOSING,
    MAX_ITER_TO_EXIT,
    DEMO_MODE,
    DEBUG,
    LOG_PROFITS,
    COINS_TO_EXPLOIT,
    DEBUG_LEVEL
)
from src.storage import storage
from src.create_exchanges import create_exchanges
from src.selling_buying import buying_order_demo, selling_order_demo, get_buying_price, get_selling_price
from src.mini_console import launch_console, mini_console
from src.exploit import exploit_thread

current_milli_time = lambda: int(round(time.time() * 1000))


def fake_balances(exchanges, amount):  # to insert $amount in all wallets for testing
    for exchange in exchanges:
        for coin in storage.coins_white_list:
            balances.update_balance(exchange.name, coin, amount/(balances.get_coin_balance(exchange.name, coin)['change']))


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
    return list(combinations(exchanges, 2))


def cross_exch_pairs(exch_pairs):
    """
        return a list of the possible coins pairs to cross in each exchange pair
    """
    pairs_per_exchs_pairs = list()  # [[...], [...], [...]...]
    all_pairs = list()
    for exch_pair in exch_pairs:
        matched_pairs = list()
        try:
            for pair in exch_pair[0].markets.keys():
                if pair in exch_pair[1].markets.keys():  # crossing is possible!
                    if pair.split('/')[0] in storage.coins_white_list and pair.split('/')[1] in storage.coins_white_list:
                        matched_pairs.append(pair)
                        if pair not in all_pairs:
                            all_pairs.append(pair)
        except Exception as e:
            pass
        logger.info('exchanges {} and {} pairs available: \n {}'.format(exch_pair[0].name, exch_pair[1].name, matched_pairs))
        pairs_per_exchs_pairs.append(matched_pairs)

    return pairs_per_exchs_pairs, all_pairs


def cross_pairs(exch_pairs, pairs_per_exchs_pairs, all_pairs):  # Nacho's algorithm. :)
    cross_threads = list()
    while True:
        starting_time = time.time()
        crosses = 0
        # first remove finished threads from the exploit threads list:
        for index, thread in enumerate(storage.exploit_threads, start=0):
            if thread.is_alive() == False:
                thread.join()
                storage.exploit_threads.pop(index)
                storage.exch_locked.pop(index)  # TODO: check this
                logger.info('removing finished thread from the exploit list')
                
        for coin_pair in all_pairs:
            for thread in cross_threads:
                    thread.join()
            cross_threads.clear()
            for exch_pair, pairs_per_exchs_pair in zip(exch_pairs, pairs_per_exchs_pairs):
                lock_string = exch_pair[0].name + ' ' + exch_pair[1].name + ' ' + coin_pair
                if coin_pair in pairs_per_exchs_pair\
                    and lock_string not in storage.exch_locked:
                    thread_name = 'cross_' + str(exch_pair[0].name + '_' + exch_pair[1].name + '_' + coin_pair)
                    thread = threading.Thread(target=cross,
                                              name=thread_name,
                                              args=(exch_pair, coin_pair, pairs_per_exchs_pair))
                    cross_threads.append(thread)
                    thread.start()
                    crosses +=1

        logger.info('loop time: {}, {} symbols and {} crosses analized'.format(time.time() - starting_time, len(all_pairs), crosses))


def cross_pairs_(exch_pairs, pairs_to_cross, all_pairs):
    """
        performs the crossing between exchanges using the possible coins pairs
        a random choosing is used.
        TODO: remove the random choosing and use a more consistent way...
    """
    iterations = 0
    while True:  # infinite loop
        loop_time = time.time()

        # first remove finished threads from the threads list:
        for index, thread in enumerate(storage.exploit_threads, start=0):
            if thread.is_alive() == False:
                thread.join()
                storage.exploit_threads.pop(index)
                storage.exch_locked.pop(index)  # TODO: check this
                logger.info('removing finished thread from the list')

        # chooses a pair of coins and call the crossing function
        for index, exch_pair in enumerate(exch_pairs, start=0):
            if len(pairs_to_cross[index]) > 0:
                sweeping_pointer = iterations % len(pairs_to_cross[index])  # creates cycling pointer
                coin_pair = pairs_to_cross[index][sweeping_pointer]  # takes a coins pair on each step
                
                # check if this pair of echanges and coins are already locked
                lock_string = exch_pair[0].name + ' ' + exch_pair[1].name + ' ' + coin_pair
                if lock_string not in storage.exch_locked:

                    # launch the crossing procesure
                    # ----------------------------------------------------------------------
                    status = cross(exch_pair, coin_pair, pairs_to_cross[index])
                    # ----------------------------------------------------------------------

                    logger.info('trying {} and {} for {} crossing'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))

                else:
                    logger.info('{} and {} already locked for {}!'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))

        iterations +=1
        logger.info('loop time: {}, threads: {}'.format(time.time() - loop_time, len(storage.exploit_threads)))

    return 0


def cross(exch_pair, coin_pair, coin_pairs_avail):
    """
        performs the exchange crossing for the coin pair
        TODO: if an exchange does not respond many times consecutively move it to a waiting list and disable it for requesting
              try again after some minutes. Use a thread for that functionality
    """

    if  exch_pair[0].rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exch_pair[0].lastRestRequestTimestamp) > 0:
        time.sleep((exch_pair[0].rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exch_pair[0].lastRestRequestTimestamp))/1000)

    if  exch_pair[1].rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exch_pair[1].lastRestRequestTimestamp) > 0:
        time.sleep((exch_pair[1].rateLimit * RATE_LIMIT_FACTOR - (current_milli_time() - exch_pair[1].lastRestRequestTimestamp))/1000)

    base_coin = coin_pair.split('/')[0]
    quote_coin = coin_pair.split('/')[1]

    tz_base = balances.get_coin_balance(exch_pair[0].name, base_coin)['trading_size']
    # tz_quote = balances.get_coin_balance(exch_pair[1].name, quote_coin)['trading_size']

    try:  # fetch the first order book
        orderbook_0 = exch_pair[0].fetch_order_book(coin_pair)
    except Exception as e:
        logger.critical('problems loading order books, request error on \t{}'.format(exch_pair[0].name))
        logger.critical(e)

    try:  # and fetch the second order book
        orderbook_1 = exch_pair[1].fetch_order_book(coin_pair)
    except Exception as e:
        logger.critical('problems loading order books, request error on \t{}'.format(exch_pair[1].name))
        logger.critical(e)
    
    try:  # gets the bids and asks for each exchange
        # bid_0 = orderbook_0['bids'][0][0] if len(orderbook_0['bids']) > 0 else None
        # ask_0 = orderbook_0['asks'][0][0] if len(orderbook_0['asks']) > 0 else None
        # vol_bid_0 = orderbook_0['bids'][0][1] if len(orderbook_0['bids']) > 0 else None
        # vol_ask_0 = orderbook_0['asks'][0][1] if len(orderbook_0['asks']) > 0 else None

        # bid_1 = orderbook_1['bids'][0][0] if len(orderbook_1['bids']) > 0 else None
        # ask_1 = orderbook_1['asks'][0][0] if len(orderbook_1['asks']) > 0 else None
        # vol_bid_1 = orderbook_1['bids'][0][1] if len(orderbook_1['bids']) > 0 else None
        # vol_ask_1 = orderbook_1['asks'][0][1] if len(orderbook_1['asks']) > 0 else None

        # 2020/5/13: fix: use trading size to avoid empty prices 
        bid_0 = get_selling_price(exch_pair[0], coin_pair, tz_base)
        bid_1 = get_selling_price(exch_pair[1], coin_pair, tz_base)
        ask_0 = get_buying_price(exch_pair[0], coin_pair, tz_base)
        ask_1 = get_buying_price(exch_pair[1], coin_pair, tz_base)

    except Exception as e:
        logger.error('not possible getting bids/asksfrom \t{} or \t{}'.format(exch_pair[0].name, exch_pair[1].name))
        logger.error(e)
        return -1

    # gets the fees
    try:
        fee_1 = max(exch_pair[0].fees['trading']['maker'], exch_pair[0].fees['trading']['taker'])
    except Exception as e:
        fee_1 = 0.005

    try:
        fee_2 = max(exch_pair[1].fees['trading']['maker'], exch_pair[1].fees['trading']['taker'])
    except Exception as e:
        fee_2 = 0.005

    # check if there is an ipportunity of profit in both directions
    if bid_0 and bid_1 and ask_0 and ask_1:

        # entry threshold 1.2x above the fees:
        if ((bid_0 - ask_1)/ask_1 - ENTRY_THR_FACTOR * (fee_1+fee_2)) >= PROFIT_THR_TO_OPEN_POSITIONS:

            opp_logger.info(',   OPPORTUNITY, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
                    exch_pair[0].name, exch_pair[1].name, coin_pair, bid_0, ask_1, (bid_0 - ask_1)/ask_1, (fee_1+fee_2), (bid_0 - ask_1)/ask_1 - (fee_1+fee_2)))

            logger.info('locking exchanges \t{} and \t{} for \t{}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))

            # if profit is possible exploit the pair
            # ----------------------------------------------------------------------
            exploit_pair(exch_pair, coin_pair, coin_pairs_avail)
            # ----------------------------------------------------------------------

        # in the other direcction
        elif ((bid_1 - ask_0)/ask_0 - ENTRY_THR_FACTOR * (fee_1+fee_2)) >= PROFIT_THR_TO_OPEN_POSITIONS:

            opp_logger.info(',R  OPPORTUNITY, \t{:12}, \t{:12}, \t{}, \t{}, \t{}, \t{:%}, \t{:%}, \t{:%}'.format(
                    exch_pair[1].name, exch_pair[0].name, coin_pair, bid_1, ask_0, (bid_1 - ask_0)/ask_0, (fee_1+fee_2), (bid_1 - ask_0)/ask_0 - (fee_1+fee_2)))

            logger.info('locking exchanges \t{} and \t{} for \t{}'.format(exch_pair[1].name, exch_pair[0].name, coin_pair))

            # if profit is possible in the other direction exploit the pair as well
            # ----------------------------------------------------------------------
            exploit_pair(exch_pair, coin_pair, coin_pairs_avail, reverse=True)
            # ----------------------------------------------------------------------

    else:
        logger.error('some bids or aks are NULL, \t{} \t{} \t{} \t{} \t{} \t{} \t{}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair, bid_0, ask_0, bid_1, ask_1))

    return True


def exploit_pair(exch_pair, coin_pair, coin_pairs_avail, reverse=False, ):
    """
        launches the exploit thread
    """
    if len(storage.exploit_threads) < MAX_THREADS:
        if not reverse:
            logger.info('launching {} and {} thread for {}'.format(exch_pair[0].name, exch_pair[1].name, coin_pair))
            thread = threading.Thread(target=exploit_thread,
                                      name='arb_' + str(storage.exploit_thread_number) + '_' + exch_pair[0].name + '_' + exch_pair[1].name + '_' + coin_pair,
                                      args=(exch_pair[0], exch_pair[1], coin_pair, coin_pairs_avail))
        else:
            logger.info('launching {} and {} thread for {}'.format(exch_pair[1].name, exch_pair[0].name, coin_pair))
            thread = threading.Thread(target=exploit_thread,
                                      name='arb_' + str(storage.exploit_thread_number) + '_' + exch_pair[1].name + '_' + exch_pair[0].name + '_' + coin_pair,
                                      args=(exch_pair[1], exch_pair[0], coin_pair, coin_pairs_avail))
        # launch the thread
        storage.exploit_thread_number += 1
        storage.exploit_threads.append(thread)
        
        # lock exchs and coin pair        
        lock_string = exch_pair[0].name + ' ' + exch_pair[1].name + ' ' + coin_pair
        storage.exch_locked.append(lock_string)

        # thread.setName
        thread.start()
        return True

    return False


def dumper(balances):
    thread = threading.Thread(target=dump_balances,
                              name='balances dumper',
                              args=(balances,))
    thread.start()


def dump_balances(balances):
    """
    dumps json strings with bot status to output files
    """
    while True:
        with open('output/balance_status.json', 'w') as output:
            bal_list = balances.get_detailed_balance()
            for e in bal_list:
                output.write(str(e).replace('\'', '\"') + '\n')
        
        with open('output/threads_status.json', 'w') as output:
            for e in storage.threads_status:
                output.write(str(e).replace('\'', '\"') + '\n')
        
        time.sleep(5)
    

def main():
    start_time = time.time()
    exchanges = create_exchanges()
    launch_console(exchanges)
    load_markets_(exchanges)
    init_balances(exchanges)
    coins_prices_updater(exchanges)
    dumper(balances)

    storage.initial_balance = balances.get_full_balance()
    exch_pairs = pairs_generator(exchanges)
    pairs_to_cross, all_pairs = cross_exch_pairs(exch_pairs)
    cross_pairs(exch_pairs, pairs_to_cross, all_pairs)


if __name__ == "__main__":
    storage.coins_white_list =  COINS_TO_EXPLOIT.split()

    # releasing the beast! XD
    main()
