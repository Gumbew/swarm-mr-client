import argparse
from mapreduce import task_runner_proxy

parser = argparse.ArgumentParser()
parser.add_argument("--map", action="store")
parser.add_argument("--map_f", action="store")
parser.add_argument("--reduce", action="store")
parser.add_argument("--reduce_f", action="store")
parser.add_argument("--key_delimiter", action="store")

args = parser.parse_args()


def cli_parser():
    TR = task_runner_proxy.TaskRunner
    if args.map_f is None:
        if args.reduce_f is None:
            return TR.map_reduce(False, args.map, False, args.reduce, args.key_delimiter)
        TR.map_reduce(False, args.map, True, args.reduce, args.key_delimiter)
    if args.reduce_f is None:
        return TR.map_reduce(True, args.map, False, args.reduce, args.key_delimiter)
    return TR.map_reduce(True, args.map, True, args.reduce, args.key_delimiter)


cli_parser()
