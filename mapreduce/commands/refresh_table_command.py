from mapreduce.commands import base_command
from filesystem import service
#import client
from http_client import base_http_client


class RefreshTableCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}

    def set_file_name(self, file_name):
        self._data['file_name'] = file_name

    def set_ip(self, ip):
        self._data["ip"] = ip

    def set_segment_name(self, segment_name):
        self._data["segment_name"] = segment_name

    def validate(self):
        pass

    def send(self):
        self.validate()
        data = dict()
        data['refresh_table'] = self._data
        super().__init__(data)
        return super().send()
