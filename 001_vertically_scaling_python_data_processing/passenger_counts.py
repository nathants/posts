import sys
import collections

# count the columns in the header
col_count = len(sys.stdin.readline().split(','))

# setup state
bad_lines = 0
result = collections.defaultdict(int)

# process input row by row
for line in sys.stdin:
    cols = line.split(',')
    if len(cols) != col_count:
        bad_lines += 1
    else:
        passengers = cols[3]
        result[passengers] += 1

# print bad lines count to stderr
print(f'bad lines: {bad_lines}', file=sys.stderr)

# print passenger counts to stdout
for passengers, count in result.items():
    print(f'{passengers},{count}')
