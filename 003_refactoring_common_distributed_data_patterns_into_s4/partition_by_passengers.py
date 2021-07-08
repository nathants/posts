import sys
import random

cluster_size = int(sys.argv[1])

sys.stdin.readline() # skip the header

files = {}

for line in sys.stdin:
    cols = line.split(',')
    try:
        passengers = int(cols[3])
    except (IndexError, ValueError):
        continue
    else:
        randint = random.randint(0, cluster_size)
        filename = f'passengers_{passengers}_{randint:03d}.csv'
        if filename not in files:
            files[filename] = open(filename, 'w')
        files[filename].write(line)

for name, file in files.items():
    print(name)
    file.close()
