import os
import shell
import pool.thread

shell.run('mkdir -p /mnt/data')

prefix = "s3://nyc-tlc/trip data"

keys = [x.split()[-1] for x in shell.run(f'aws s3 ls "{prefix}/"').splitlines() if 'yellow' in x]

def download(key):
    shell.run(f'aws s3 cp "{prefix}/{key}" - | cut -d, -f1-5 > /mnt/data/{key}', echo=True)

pool.thread.size = os.cpu_count()

list(pool.thread.map(download, keys))
