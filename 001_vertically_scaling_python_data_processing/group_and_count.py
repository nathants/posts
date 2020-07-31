import os
import shell
import pool.thread

shell.run('mkdir -p /mnt/results')

paths = shell.files('/mnt/data', abspath=True)

def process(path):
    shell.run(f'< {path} pypy3 /mnt/passenger_counts_inlined.py > /mnt/results/{os.path.basename(path)}')

pool.thread.size = os.cpu_count()

list(pool.thread.map(process, paths))
