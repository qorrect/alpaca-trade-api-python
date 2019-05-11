from alpaca_trade_api.polygon.rest import REST


import os

base_url = os.environ["APCA_API_BASE_URL"]
key_id = os.environ["APCA_API_KEY_ID"]
secret_key = os.environ["APCA_API_SECRET_KEY"]

api = REST(key_id)
a = api.analysts('AAPL')
print(repr(a.analysts))