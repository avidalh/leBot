import requests




def create_new_request(coin):
    ''' docu '''
    BASE_URL = 'https://min-api.cryptocompare.com/data/price?fsym={}&tsyms=USD'
    APY_KEY = 'b8d053a2337a82e428c46acbcd623be0e5425ddfa2dc706040792b52643bb75a'
    resp = requests.get(BASE_URL.format(coin))
    print(resp.json())
    return resp.json()