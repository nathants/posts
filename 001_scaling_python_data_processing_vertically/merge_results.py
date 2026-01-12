import sys
import collections

result = collections.defaultdict(int)

for line in sys.stdin:
    passengers, count = line.split(',')
    result[passengers] += int(count)

for passengers, count in result.items():
    print(f'{passengers},{count}')
