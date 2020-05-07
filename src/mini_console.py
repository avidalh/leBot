import json
import threading

from src.params import TRADING_SIZE, EXPLOIT_THREAD_DELAY, PROFIT_THR_TO_OPEN_POSITIONS 
from src.params import PROFIT_THR_TO_CLOSE_POSITIONS, ENTRY_THR_FACTOR, OPERATE_THR_FACTOR
from src.params import MAX_THREADS, DEMO_MODE, DEBUG, DEBUG_LEVEL, LOG_PROFITS, TRADES_TO_ALLOW_CLOSING
from src.params import NON_DIRECT_TRADE_PROFIT_THR, NON_DIRECT_CLOSING_PROFIT_THR
from src.storage import storage
from src.wallet import balances

def launch_console(exchanges):

    thread = threading.Thread(target=mini_console, name='miniConsole', args=(exchanges,))
    thread.start()


def mini_console(exchanges):
    welcome_message = '''
    this is leBot command console, '?' for help
    '''
    help_msg = '''
    possible commands are:
            - 'se': shows the exchanges list
            - 'ss': shows status config variables
            - 'sb [exchName]': shows balance, or exchName balance
            - 'sbr [exchName]': shows a raw json balance detailed, if exchName present: same information but for the specified exchange
            - 'sbf [exchName]': shows a json formatted detailed balance, if exchName present: same information but for the specified exchange
            - 'sel': show list of exch pairs/coins locked
            - 'sat': show active threads
            tuning params:
            - 'set0 value': -> TRADING_SIZE
            - 'set1 value': -> PROFIT_THR_TO_OPEN_POSITIONS
            - 'set2 value': -> PROFIT_THR_TO_CLOSE_POSITIONS
            - 'set3 value': -> ENTRY_THR_FACTOR
            - 'set4 value': -> OPERATE_THR_FACTOR
            - 'set5 value': -> MAX_THREADS
            - 'set6 value': -> EXPLOIT_THREAD_DELAY
            - 'set7 value': -> NON_DIRECT_TRADE_PROFIT_THR
            - 'set8 value': -> NON_DIRECT_CLOSING_PROFIT_THR

            - '?': shows this message/help
            (new commands soon!)
    '''
    print(welcome_message)
    global TRADING_SIZE
    global EXPLOIT_THREAD_DELAY
    global PROFIT_THR_TO_OPEN_POSITIONS
    global PROFIT_THR_TO_CLOSE_POSITIONS
    global ENTRY_THR_FACTOR
    global OPERATE_THR_FACTOR
    global MAX_THREADS
    global NON_DIRECT_TRADE_PROFIT_THR
    global NON_DIRECT_CLOSING_PROFIT_THR
    while True:
        input_str = input('(leBot) > ')
        input_parsed = parse(input_str)

        if len(input_parsed) == 1:
            input_command = input_parsed[0]
            
            if input_command == 'ss':
                print('DEMO_MODE: ', DEMO_MODE)
                print('DEBUG: ', DEBUG)
                print('DEBUG_LEVEL: ', DEBUG_LEVEL)
                print('LOG_PROFITS: ', LOG_PROFITS)
                print('TRADING_SIZE: ', TRADING_SIZE)
                print('EXPLOIT_THREAD_DELAY: ', EXPLOIT_THREAD_DELAY)
                print('MAX_THREADS: ', MAX_THREADS)
                print('PROFIT_THR_TO_OPEN_POSITIONS: ', PROFIT_THR_TO_OPEN_POSITIONS)
                print('PROFIT_THR_TO_CLOSE_POSITIONS: ', PROFIT_THR_TO_CLOSE_POSITIONS)
                print('NON_DIRECT_TRADE_PROFIT_THR: ', NON_DIRECT_TRADE_PROFIT_THR)
                print('NON_DIRECT_CLOSING_PROFIT_THR: ', NON_DIRECT_CLOSING_PROFIT_THR)

                print('ENTRY_THR_FACTOR: ', ENTRY_THR_FACTOR)
                print('OPERATE_THR_FACTOR: ', OPERATE_THR_FACTOR)

                print('TRADES_TO_ALLOW_CLOSING: ', TRADES_TO_ALLOW_CLOSING)

                print('active threads: ', len(storage.exploit_threads))
                print('accumulated profit: ', storage.accumProfit)
            
            elif input_command == 'se':
                print('exchanges list:')
                print([exchange.name for exchange in exchanges])
            
            elif input_command == 'sb':
                for exchange in exchanges:
                    print('{} balance: {}'.format(exchange.name, balances.get_balance(exchange.name)))
                print('Full balance in USD: ')
                print(balances.get_full_balance())
            
            elif input_command == 'sbr':
                balance = balances.get_detailed_balance()
                print(balance)
            
            elif input_command == 'sbf':
                detail = balances.get_detailed_balance()
                for e in detail:
                    print(json.dumps(e, indent=2))
            
            elif input_command == 'sel':
                print('exchanges pairs locked {}'.format(len(storage.exch_locked)))
                for e in storage.exch_locked:
                    print(e)
            
            elif input_command == 'sat':
                print('active treads {}'.format(len(storage.exploit_threads)))
                for e in storage.exploit_threads:
                    print(e)
            
            elif input_command == '?':
                print(help_msg)
            
            else:
                print('error: command not found')
        
        elif len(input_parsed)  == 2:
            input_command = input_parsed[0]
            arg = input_parsed[1]

            if input_command == 'sbf':
                try:
                    balance = balances.get_detailed_balance(arg)
                    print(json.dumps(balance, indent=2))
                except:
                    print('not present')

            elif input_command == 'sbr':
                try:
                    balance = balances.get_detailed_balance(arg)
                    print(balance)
                except:
                    print('not present')
            
            elif input_command == 'sb':
                try:
                    balance = balances.get_balance(arg)
                    print('{} balance: '.format(arg))
                    print(balance)
                except:
                    print('not present')

            # TODO: check if values are consistent. Not allow enter any value...
            elif input_command == 'set0':
                TRADING_SIZE = float(arg)
            
            elif input_command == 'set1':
                PROFIT_THR_TO_OPEN_POSITIONS = float(arg)
            
            elif input_command == 'set2':
                PROFIT_THR_TO_CLOSE_POSITIONS = float(arg)

            elif input_command == 'set3':
                ENTRY_THR_FACTOR = float(arg)
            
            elif input_command == 'set4':
                OPERATE_THR_FACTOR = float(arg)
            
            elif input_command == 'set5':
                MAX_THREADS = int(arg)
            
            elif input_command == 'set6':
                EXPLOIT_THREAD_DELAY = int(arg)

            elif input_command == 'set7':
                NON_DIRECT_TRADE_PROFIT_THR = float(arg)

            elif input_command == 'set8':
                NON_DIRECT_CLOSING_PROFIT_THR = float(arg)


            else:
                print('error: command not found')
            
            
    return 0


def parse(args):

    'Convert a series of zero or more numbers to an argument tuple'
    return args.split()
