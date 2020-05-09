import pandas as pd
import json

def readFile(fileName):
    '''
    read balances from json file and return two data structures
    '''

    with open('balances.json') as json_file:
        data = json.load(json_file)
        dfItem = pd.DataFrame.from_records(data)
    return dfItem

def main():
    filename = 'balances.json'
    data = readFile(filename)

if __name__ == "__main__":
    main()