from mapreduce.commands import base_command
from filesystem import service
import client
from http_client import base_http_client

class AppendCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}

    def set_data(self, index):
        self._data['data']= client.splitted_file[index]

    def set_destination_file(self, dest_file):
        encoded = dest_file
        self._data["destination_file"] = encoded

    def validate(self):
        pass

    def send(self):
        self.validate()
        base_http_client.post(self._data)
