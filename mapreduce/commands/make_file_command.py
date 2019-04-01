from mapreduce.commands import base_command
from filesystem import service
#import client
from http_client import base_http_client


class MakeFileCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}

    def set_destination_file(self, destination_file):
        self._data['destination_file'] = destination_file



    def validate(self):
        pass

    def send(self):
        self.validate()
        data = dict()
        data['make_file'] = self._data
        super().__init__(data)
        return super().send()
