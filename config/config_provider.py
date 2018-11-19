import json

json_file = 'json/cluster_access.json'
json_data = open(json_file)
data = json.load(json_data)
json_data.close()


def get_arbiter_address():
    return data["arbiter_address"]


def get_access_token():
    return data["access_token"]
