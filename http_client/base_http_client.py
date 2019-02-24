import json
import requests
import base64
from config import config_provider


def post(data):
    address = config_provider.ConfigProvider.get_arbiter_address('\\json\\cluster_access.json')
    access_token = config_provider.ConfigProvider.get_access_token('\\json\\cluster_access.json')
    url = 'http://' + address

    params = {
        'access_token': access_token,
    }

    response = requests.post(url, params=params,
                             data=json.dumps(data))

    response.raise_for_status()

    return response.json()


