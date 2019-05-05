import argparse
from mapreduce import task_runner_proxy

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
	print(args)
	if args.mf is None:
		if args.rf is None:
			return tr.run_map_reduce(False, args.m, False, args.r, args.kd, args.src, args.dest)
		else:
			return tr.run_map_reduce(False, args.m, True, args.rf, args.kd, args.src, args.dest)
	elif args.rf is None:
		return tr.run_map_reduce(True, args.mf, False, args.r, args.kd, args.src, args.dest)
	else:
		return tr.run_map_reduce(True, args.mf, True, args.rf, args.kd, args.src, args.dest)


# tr.clear_data('data')
# distribution = tr.make_file(os.path.join(os.path.dirname(__file__), '..', '..', 'client_data','out.txt'))

# tr.run_map_reduce(True, "/home/gumbew/workspace/Kursova/swarm-mr-client/../../client_data/mapper.py", True, "/home/gumbew/workspace/Kursova/swarm-mr-client/../../client_data/reducer.py", "0",
#               os.path.join(os.path.dirname(__file__), '..', '..', 'client_data', 'text.txt'),
#               os.path.join(os.path.dirname(__file__), '..', '..', 'client_data','out.txt'))
if __name__ == '__main__':
	tr = task_runner_proxy.TaskRunner()
	cli_parser(tr)
