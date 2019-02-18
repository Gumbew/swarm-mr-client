import json
import os.path

class ConfigProvider:

    @staticmethod
    def get_arbiter_address(file_path):
        json_data = open(os.path.dirname(__file__) + file_path)
        data = json.load(json_data)
        json_data.close()
        return data["arbiter_address"]

    @staticmethod
    def get_access_token(file_path):
        json_data = open(os.path.dirname(__file__) + file_path)
        data = json.load(json_data)
        json_data.close()
        return data["access_token"]
