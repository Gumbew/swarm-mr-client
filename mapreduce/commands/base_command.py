from http_client import base_http_client


class BaseCommand:

    def __init__(self, map, reduce, key_delimiter):
        self.map = map
        self.reduce = reduce
        self.key_delimiter = key_delimiter

    def validate(self):
        if self.map is None:
            raise AttributeError("Mapper is empty!")
        if self.reduce is None:
            raise AttributeError("Reducer is empty!")
        return True

    def send(self):
        self.validate()
        base_http_client.post(self.map, self.reduce, self.key_delimiter)
