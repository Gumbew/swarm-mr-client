from filesystem import service
from mapreduce.commands import append_command
from mapreduce.commands import make_file_command
from mapreduce.commands import map_reduce_command
from mapreduce.commands import refresh_table_command
from mapreduce.commands import write_command
from config import config_provider
import os


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
        field_delimiter = config_provider.ConfigProvider.get_field_delimiter(
            os.path.join('..', 'config', 'json', 'client_config.json'))
        mrc.set_field_delimiter(field_delimiter)
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
    def refresh_table(file_name, ip, segment_name):
        rtc = refresh_table_command.RefreshTableCommand()
        rtc.set_file_name(file_name)
        rtc.set_ip(ip['data_node_ip'])
        rtc.set_segment_name(segment_name)

        return rtc.send()

    @staticmethod
    def main_func(file_name, distribution, dest):
        splitted_file = service.split_file(file_name, distribution)
        counter = 0
        for fragment in splitted_file:
            counter += 1
            segment_name = "f" + str(counter)
            ip = TaskRunner.append(dest, fragment)
            TaskRunner.write(dest + os.sep + segment_name, fragment, ip)
            TaskRunner.refresh_table(dest, ip, segment_name)

    @staticmethod
    def make_file(dest_file):
        mfc = make_file_command.MakeFileCommand()
        mfc.set_destination_file(dest_file)
        return mfc.send()

    @staticmethod
    def send_info():
        pass
