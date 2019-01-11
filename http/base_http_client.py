import json
import requests
import base64
from config import config_provider


def post(map, reduce, key_delimiter):
    address = config_provider.ConfigProvider.get_arbiter_address('json/cluster_access.json')
    access_token = config_provider.ConfigProvider.get_access_token('json/cluster_access.json')


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
