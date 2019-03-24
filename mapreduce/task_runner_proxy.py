from mapreduce.commands import map_reduce_command
from http_client import base_http_client
from filesystem import service
import requests
import json


class TaskRunner:

    @staticmethod
    def map_reduce(is_mapper_in_file, mapper, is_reducer_in_file, reducer, key_delimiter, source_file,
                   destination_file):
        mrc = map_reduce_command.MapReduceCommand()
        if is_mapper_in_file is False:
            mrc.set_mapper(mapper)
        else:
            mrc.set_mapper_from_file(mapper)

        if is_reducer_in_file is False:
            mrc.set_reducer(reducer)
        else:
            mrc.set_reducer_from_file(reducer)

        mrc.set_key_delimiter(key_delimiter)
        mrc.set_source_file(source_file)
        mrc.set_destination_file(destination_file)
        return mrc.send()

    @staticmethod
    # def append(dest_file, distribution):
    #     splitted_file = service.split_file("C:\\Users\\smart\\workspace\\client_data\\text.txt", distribution)
    #     data = {
    #         'file_name':'C:\\Users\\smart\\workspace\\client_data\\out.txt',
    #
    #     }
    #     print(splitted_file)
    def append(file_name, segment):
        data = {
            'append': {
                'file_name': file_name,
                'segment': segment
            }
        }
        # base_http_client.post(data)
        return base_http_client.post(data)['data_node_ip']

    @staticmethod
    def write(file_name, segment, data_node_ip):
        data = {
            'write':
                {
                    'file_name': file_name,
                    'segment': segment
                }
        }


        response = requests.post(data_node_ip,
                                 data=json.dumps(data))

        return response.json()

    @staticmethod
    def main_func(file_name, distribution):
        splitted_file = service.split_file(file_name, distribution)
        counter=1
        for fragment in splitted_file:
            if counter <= distribution:
                counter += 1
            file_name += "\\f" + str(counter)
            print(file_name)
            TaskRunner.write(file_name, fragment, TaskRunner.append(file_name, fragment))