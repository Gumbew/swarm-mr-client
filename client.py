import argparse
from mapreduce import task_runner_proxy

parser = argparse.ArgumentParser()
parser.add_argument("--map", action="store")
parser.add_argument("--map_f", action="store")
parser.add_argument("--reduce", action="store")
parser.add_argument("--key_delimiter", action="store")

args = parser.parse_args()


def cli_parser():
    TR = task_runner_proxy.TaskRunner
    if args.map_f is None:
        return TR.map_reduce(False, args.map, args.reduce, args.key_delimiter)
    else:
        return TR.map_reduce(True, args.map_f, args.reduce, args.key_delimiter)


cli_parser()