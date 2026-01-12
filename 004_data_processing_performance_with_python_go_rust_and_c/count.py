import sys

count = 0

for line in sys.stdin:
    columns = line.split(',')
    if columns[0][0] == 'f':
        count += 1

print(count)
