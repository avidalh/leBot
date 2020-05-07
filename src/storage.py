
class GlobalStorage:
    """
        general purpose class to store global data
    """

    def __init__(self):
        self.exploit_threads = list()  # list of threads
        self.exploit_thread_number = 0
        self.exch_locked = list()  # list of exchanges/coins already in use (threaded)
        self.timer = {}  # time stampt and exchange delay  TODO: to remove
        self.coins_white_list = list()  # coins allowed to use/cross
        self.initial_balance = 0
        self.current_balance = 0
        self.prices_updated = False
        self.accumProfit = 0

storage = GlobalStorage()