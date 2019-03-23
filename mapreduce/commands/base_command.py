from http_client import base_http_client
import os.path


class BaseCommand:

    def __init__(self, map, reduce, key_delimiter, source_file, destination_file):
        self.map = map
        self.reduce = reduce
        self.key_delimiter = key_delimiter
        self.source_file = source_file
        self.destination_file = destination_file

    def validate(self):
        if self.map is None:
            raise AttributeError("Mapper is empty!")
        if self.reduce is None:
            raise AttributeError("Reducer is empty!")
        if self.source_file is None:
            raise AttributeError("Source file in not mentioned!")
        if self.destination_file is None:
            raise AttributeError("Destination file in not mentioned!")
        if os.path.exists(self.source_file) is False:
            raise FileExistsError()
        if os.path.exists(self.destination_file) is False:
            raise FileExistsError()

        return True

    def send(self):
        self.validate()
        data = {
            'map': {"map": self.map,
                    "reduce": self.reduce,
                    "key_delimiter": self.key_delimiter,
                    "src_file": self.source_file,
                    "dest_file": self.destination_file
                    }
        }

        return base_http_client.post(data, )
