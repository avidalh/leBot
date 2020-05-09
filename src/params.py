# parameters
DEMO_MODE = True
DEBUG = True
DEBUG_LEVEL = 0                             # noy used by now

LOG_PROFITS = True

COINS_TO_EXPLOIT = 'BTC BCH ETH LTC EOS XMR XRP ZEC DASH NEO PIVX NANO ADA ETC HT LINK ATOM QTUM BNB BSV OF OKB PAX QC TRX USDC USDT EUR USD'
UPDATE_PRICES_PERIOD = 2                    # hours

TRADING_SIZE = 25  #25                      # $25
TRADING_SIZE_MARGIN = 1.3  
EXPLOIT_THREAD_DELAY = 20  #15              # exploit thread period
RATE_LIMIT_FACTOR = 1.2   #1.05             # exchange rate limit factor to stay in a "requesting rate" safe zone
MAX_THREADS = 50                            # limiting the number of threads

# thresholding
ENTRY_THR_FACTOR   = 1.0  #1.2              # factor times fees to start a thread
OPERATE_THR_FACTOR = 1.0  #2.0              # factor times fees to start a trade/arbitrage
PROFIT_THR_TO_OPEN_POSITIONS =  +0.0030     #+0.000200     # open position threshold (values for testing the bot)
PROFIT_THR_TO_CLOSE_POSITIONS = +0.0000     #-0.000100     # close positions threshold. Close all the positions openend by the arb thread.
NON_DIRECT_TRADE_PROFIT_THR =   +0.0060
NON_DIRECT_CLOSING_PROFIT_THR = +0.0000

MAX_ITER_TO_EXIT = 600                      # 240rep / 4rep/min = 60 min   # number of empty iterations to kill the thread
TRADES_TO_ALLOW_CLOSING = 1                 # how many trades to allow closing positions