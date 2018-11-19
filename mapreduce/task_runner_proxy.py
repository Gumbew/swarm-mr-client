from mapreduce.commands import map_reduce_command


class TaskRunner:
    def map_reduce(self, is_file, mapper, reducer, key_delimiter):

        MRC = map_reduce_command.MapReduceCommand()
        if is_file is False:
            MRC.setMapper(mapper)
            MRC.setReducer(reducer)
            MRC.setKeyDelimiter(key_delimiter)
            MRC.send()
        else:
            MRC.set_mapper_from_file(mapper)
            MRC.setReducer(reducer)
            MRC.setKeyDelimiter(key_delimiter)
            MRC.send()
