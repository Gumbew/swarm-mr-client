import os

path = os.path.join(os.path.expanduser('~'), os.path.dirname(__file__), 'json', 'cluster_access.json')

f = open(path)
print(f)