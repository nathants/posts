import sys
import collections
import shell

ids = ' '.join(sys.argv[1:])

with shell.tempdir():
    shell.run(f'aws-ec2-rsync --yes :/mnt/results/ ./results/ {ids} 1>&2', stream=True)

    result = collections.defaultdict(int)

    for path in shell.files('results', abspath=True):
        with open(path) as f:
            for line in f:
                passengers, count = line.split(',')
                result[passengers] += int(count)

for passengers, count in result.items():
    print(f'{passengers},{count}')
