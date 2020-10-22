import csv
import sys
import collections

sys.stdin.readline() # skip the header

result = collections.defaultdict(int)

for cols in csv.reader(sys.stdin):
    try:
        passengers = cols[3]
    except IndexError:
        continue
    else:
        result[passengers] += 1

for passengers, count in result.items():
    print(f'{passengers},{count}')
