from http import base_http_client


class BaseCommand:

    def __init__(self, map, reduce, key_delimiter):
        self.map = map
        self.reduce = reduce
        self.key_delimiter = key_delimiter

    def send(self):
        base_http_client.post(self.map, self.reduce, self.key_delimiter)
