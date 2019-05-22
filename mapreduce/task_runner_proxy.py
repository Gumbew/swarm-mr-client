from filesystem import service
from mapreduce.commands import (
	append_command,
	make_file_command,
	map_reduce_command,
	refresh_table_command,
	write_command,
	clear_data_command,
	get_file_command,
	get_result_of_key_command
)
from config import config_provider
import os


class TaskRunner:

	@staticmethod
	def map_reduce(is_mapper_in_file, mapper, is_reducer_in_file, reducer, key_delimiter, is_server_source_file,
				   source_file,
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

		if is_server_source_file is True:
			mrc.set_server_source_file(source_file)
		else:
			mrc.set_source_file(source_file)
		mrc.set_key_delimiter(key_delimiter)
		field_delimiter = config_provider.ConfigProvider.get_field_delimiter(
			os.path.join('..', 'config', 'json', 'client_config.json'))
		mrc.set_field_delimiter(field_delimiter)
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

	@staticmethod
	def get_file(file_name, ip=None):
		gf = get_file_command.GetFileCommand()
		gf.set_file_name(file_name)
		if ip is None:
			return gf.send()
		else:
			return gf.send(ip)

	@staticmethod
	def clear_data(folder_name):
		cdc = clear_data_command.ClearDataCommand()
		cdc.set_folder_name(folder_name.split(',')[0])
		vh =  folder_name.split(",")[1]
		cdc.set_remove_all_data(bool(int(vh)))
		return cdc.send()

	@staticmethod
	def run_map_reduce(is_mapper_in_file, mapper, is_reducer_in_file, reducer, key_delimiter,is_server_source_file, source_file,
					   destination_file):
		if not is_server_source_file:
			print("MAKE_FILE_ON_CLUSTER_FINISHED")
			distribution = TaskRunner.make_file(os.path.join(destination_file))['distribution']
			print("MAKING_FILE_ON_CLUSTER_FINISHED")
			print("APPEND_AND_WRITE_PHASE")
			TaskRunner.main_func(source_file, distribution, destination_file)
			print("APPEND_AND_WRITE_PHASE_FINISHED")
		print("MAP_REDUCE_STARTED")
		TaskRunner.map_reduce(is_mapper_in_file, mapper, is_reducer_in_file, reducer, key_delimiter,is_server_source_file, source_file,
							  destination_file)
		print("MAP_REDUCE_FINISHED")
		print("COMPLETED!")
	@staticmethod
	def push_file_on_cluster(pfc):
		arr = pfc.split(",")
		dist = TaskRunner.make_file(arr[1])
		TaskRunner.main_func(arr[0], dist['distribution'],arr[1])

	@staticmethod
	def get_result_of_key(key, file_name):
		field_delimiter = config_provider.ConfigProvider.get_field_delimiter(
			os.path.join('..', 'config', 'json', 'client_config.json'))
		grk = get_result_of_key_command.GetResultOfKeyCommand()
		grk.set_key(key)
		grk.set_file_name(file_name)
		grk.set_field_delimiter(field_delimiter)
		json_responce = grk.send()
		key_hash = json_responce['hash_key']['key_hash']
		print(json_responce)
		for item in json_responce['key_ranges']:
			if key_hash >= item['hash_keys_range'][0] and key_hash < item['hash_keys_range'][1]:
				data_node_ip = item['data_node_ip']
				break
			elif key_hash > item['hash_keys_range'][0] and key_hash <= item['hash_keys_range'][1]:
				data_node_ip = item['data_node_ip']
				break
		result = grk.send('http://' + data_node_ip)
		service.write_to_file(result['result'], file_name)
