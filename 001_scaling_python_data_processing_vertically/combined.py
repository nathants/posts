import os
import shell
import pool.thread

shell.run('mkdir -p /mnt/results')

prefix = "s3://nyc-tlc/trip data"

keys = [x.split()[-1] for x in shell.run(f'aws s3 ls "{prefix}/"').splitlines() if 'yellow' in x]

def process(key):
    shell.run(f'aws s3 cp "{prefix}/{key}" - '
              f'| cut -d, -f1-5'
              f'| pypy3 /mnt/passenger_counts_inlined.py'
              f'> /mnt/results/{key}',
              echo=True)

pool.thread.size = os.cpu_count()

list(pool.thread.map(process, keys))
