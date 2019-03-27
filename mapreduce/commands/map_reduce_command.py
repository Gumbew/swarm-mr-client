from mapreduce.commands import base_command
import base64
import os


class MapReduceCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = dict()

    def set_mapper_from_file(self, path):
        file = open(path, 'r')
        file_content = file.read()
        encoded = base64.b64encode(file_content.encode())
        self._data["mapper"] = encoded

    def set_mapper(self, content):
        # encoded = base64.b64encode(content.encode())
        encoded = content
        self._data["mapper"] = encoded

    def set_reducer_from_file(self, path):
        file = open(path, 'r')
        file_content = file.read()
        encoded = base64.b64encode(file_content.encode())
        self._data["reducer"] = encoded

    def set_reducer(self, content):
        # encoded = base64.b64encode(content.encode())
        encoded = content
        self._data["reducer"] = encoded

    # check if method to get (map)_key_delimiter from file is necessary
    def set_key_delimiter(self, key_delimiter):
        # encoded = base64.b64encode(key_delimiter.encode())
        encoded = key_delimiter
        self._data["key_delimiter"] = encoded

    def set_source_file(self, src_file):
        encoded = src_file
        self._data["source_file"] = encoded

    def set_destination_file(self, dest_file):
        encoded = dest_file
        self._data["destination_file"] = encoded

    def validate(self):
        if self._data['mapper'] is None:
            raise AttributeError("Mapper is empty!")
        if self._data['reducer'] is None:
            raise AttributeError("Reducer is empty!")
        if self._data['source_file'] is None:
            raise AttributeError("Source file in not mentioned!")
        if self._data['destination_file'] is None:
            raise AttributeError("Destination file in not mentioned!")
        if os.path.exists(self._data['source_file']) is False:
            raise FileExistsError()

    def send(self):
        self.validate()
        data = dict()
        data['map'] = self._data
        super().__init__(data)
        return super().send()
