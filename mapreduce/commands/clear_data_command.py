from mapreduce.commands import base_command
from filesystem import service
#import client
from http_client import base_http_client
import requests
import json


class ClearDataCommand(base_command.BaseCommand):

    def __init__(self):
        self._data = {}

    def set_folder_name(self, folder_name):
        self._data['folder_name'] = folder_name

    def validate(self):
        pass

    def send(self):
        self.validate()
        data = dict()
        data['clear_data'] = self._data
        super().__init__(data)

        return super().send()
