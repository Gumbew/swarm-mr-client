import argparse
from mapreduce import task_runner_proxy
from filesystem import service

parser = argparse.ArgumentParser()
parser.add_argument("--m", "--mapper", action="store", help="Set mapper as a content")
parser.add_argument("--mf", "--mapper_from_file", action="store", help="Set mapper as a file path where it is located")
parser.add_argument("--r", "--reducer", action="store", help="Set reducer as a content")
parser.add_argument("--rf", "--reducer_from_file", action="store",
                    help="Set reducer as a file path where it is located")
parser.add_argument("--kd", "--key_delimiter", action="store", help="Set key delimiter")
parser.add_argument("--src", "--source_file", action="store", help="Source file path")
parser.add_argument("--dest", "--destination_file", action="store", help="Destination file path")

args = parser.parse_args()


def cli_parser(tr):

    if args.mf is None:
        if args.rf is None:
            return tr.map_reduce(False, args.m, False, args.r, args.kd, args.src, args.dest)
        else:
            return tr.map_reduce(False, args.m, True, args.r, args.kd, args.src, args.dest)
    elif args.rf is None:
        return tr.map_reduce(True, args.m, False, args.r, args.kd, args.src, args.dest)
    else:
        return tr.map_reduce(True, args.m, True, args.r, args.kd, args.src, args.dest)


tr = task_runner_proxy.TaskRunner()
distribution= tr.map_reduce(False, "MMM", False, "RRR", "KKK", "C:\\Users\\Anchi\\workspace\\client_data\\text.txt",
                  "C:\\Users\\Anchi\\workspace\\client_data\\out.txt")
#tr.append("C:\\Users\\Anchi\\workspace\\client_data\\text.txt",'SOSI PISOS')
tr.main_func("C:\\Users\\Anchi\\workspace\\client_data\\text.txt", distribution['distribution'])

#if __name__ == '__main__':
# cli_parser(tr)
