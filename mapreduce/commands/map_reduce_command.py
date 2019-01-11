from mapreduce.commands import base_command
import base64


class MapReduceCommand(base_command):

    def __init__(self):
        self._data = {}

    def set_mapper_from_file(self, path):
        file = open(path, 'r')
        file_content = file.read()
        s = base64.encode(file_content)
        self._data["mapper"] = s

    def set_mapper(self, content):
        s = base64.encode(content)
        self._data["mapper"] = s

    def set_reducer_from_file(self, path):
        file = open(path, 'r')
        file_content = file.read()
        s = base64.encode(file_content)
        self._data["reducer"] = s

    def set_reducer(self, path):
        s = base64.encode(path)
        self._data["reducer"] = s

    def set_key_delimiter(self, key_delimiter):
        s = base64.encode(key_delimiter)
        self._data["key_delimiter"] = s

    def validate(self):
        if self._data["mapper"] is None:
            raise AttributeError("Mapper is empty!")
        if self._data["reducer"] is None:
            raise AttributeError("Reducer is empty!")
        return True

    def send(self):
        self.validate()
        base_command.base_http_client.post(self._data["mapper"], self._data["reducer"], self._data["key_delimiter"])
