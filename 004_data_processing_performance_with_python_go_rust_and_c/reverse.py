import sys

for line in sys.stdin:
    columns = line.rstrip().split(',')
    print(','.join(reversed(columns)))
