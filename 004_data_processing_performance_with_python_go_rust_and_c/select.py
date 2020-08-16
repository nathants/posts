import sys

for line in sys.stdin:
    columns = line.split(',')
    print(f'{columns[2]},{columns[6]}')
