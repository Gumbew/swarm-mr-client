import argparse
from mapreduce import task_runner_proxy


parser = argparse.ArgumentParser()
parser.add_argument("--m", "--mapper", action="store", help="Set mapper as a content")
parser.add_argument("--mf", "--mapper_from_file", action="store", help="Set mapper as a file path where it is located")
parser.add_argument("--r", "--reducer", action="store", help="Set reducer as a content")
parser.add_argument("--rf", "--reducer_from_file", action="store", help="Set reducer as a file path where it is located")
parser.add_argument("--kd", "--key_delimiter", action="store", help="Set key delimiter")

args = parser.parse_args()


def cli_parser():
    tr = task_runner_proxy.TaskRunner()
    if args.map_f is None:
        if args.reduce_f is None:
            return tr.map_reduce(False, args.map, False, args.reduce, args.key_delimiter)
        else:
            return tr.map_reduce(False, args.map, True, args.reduce, args.key_delimiter)
    elif args.reduce_f is None:
        return tr.map_reduce(True, args.map, False, args.reduce, args.key_delimiter)
    else:
        return tr.map_reduce(True, args.map, True, args.reduce, args.key_delimiter)


cli_parser()
