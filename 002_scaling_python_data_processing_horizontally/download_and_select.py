import os
import shell
import pool.thread
import sys

shell.run('mkdir -p /mnt/data')

prefix = "s3://nyc-tlc/trip data"

keys = sys.stdin.read().splitlines()

def download(key):
    shell.run(f'aws s3 cp "{prefix}/{key}" - | cut -d, -f1-5 > /mnt/data/{key}', echo=True)

pool.thread.size = os.cpu_count()

list(pool.thread.map(download, keys))
