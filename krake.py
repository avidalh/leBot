#!/usr/bin/env python3

import time, base64, hashlib, hmac, urllib.request, json

api_nonce = bytes(str(int(time.time()*1000)), "utf-8")
api_request = urllib.request.Request("https://api.kraken.com/0/private/GetWebSocketsToken", b"nonce=%s" % api_nonce)
api_request.add_header("API-Key", "ZCx760KshE/AQBKUvBoxgNtp0NUwGh+Tes32FeFaly5aF")
api_request.add_header("API-Sign", base64.b64encode(hmac.new(base64.b64decode("HrvdSboiiCdki2NErtb+F1t9ZNpEgWoL72dJRFdRN+gXT9zusw4vQUYPLgKT18+w=="), b"/0/private/GetWebSocketsToken" + hashlib.sha256(api_nonce + b"nonce=%s" % api_nonce).digest(), hashlib.sha512).digest()))

print(json.loads(urllib.request.urlopen(api_request).read())['result']['token'])


