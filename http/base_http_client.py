import json
import requests
import base64
from config import config_provider


def post(map, reduce, key_delimiter):
    address = config_provider.get_arbiter_address()
    access_token = config_provider.get_access_token()

    url = 'http://' + address

    params = {
        'access_token': access_token,
    }

    payload = {
        "map": base64.encode(map),
        "reduce": base64.encode(reduce),
        "key_delimiter": key_delimiter
    }
    response = requests.post(url, params=params,
                             data=json.dumps(payload))

    response.raise_for_status()

    return response.json()
