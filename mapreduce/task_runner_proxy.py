from mapreduce.commands import map_reduce_command

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
        mrc.send()
