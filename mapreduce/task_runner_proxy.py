from mapreduce.commands import map_reduce_command
from mapreduce.commands import append_command
from mapreduce.commands import write_command
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
    def append(file_name, segment):
        app = append_command.AppendCommand()
        app.set_file_name(file_name)
        app.set_segment(segment)
        return app.send()

    @staticmethod
    def write(file_name, segment, data_node_ip):
        wc = write_command.WriteCommand()
        wc.set_segment(segment)
        wc.set_file_name(file_name)

        wc.set_data_node_ip(data_node_ip['data_node_ip'])

        return wc.send()

    @staticmethod
    def main_func(file_name, distribution, dest):
        splitted_file = service.split_file(file_name, distribution)
        counter = 1
        for fragment in splitted_file:
            if counter >= distribution:
                counter += 1
            dest += "\\f" + str(counter)
            TaskRunner.write(dest, fragment, TaskRunner.append(dest, fragment))
            dest = dest[:-3]
