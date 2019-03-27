from mapreduce.commands import base_command
from filesystem import service
#import client
from http_client import base_http_client


class AppendCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}

    def set_segment(self, segment):
        self._data['segment'] = segment

    def set_file_name(self, file_name):
        self._data["file_name"] = file_name

    def validate(self):
        pass

    def send(self):
        self.validate()
        data = dict()
        data['append'] = self._data
        super().__init__(data)
        return super().send()
