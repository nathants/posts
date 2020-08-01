import shell
import pool.thread
import util.iter
import sys

ids = sys.argv[1:]

prefix = "s3://nyc-tlc/trip data"

keys = [x.split()[-1] for x in shell.run(f'aws s3 ls "{prefix}/"').splitlines() if 'yellow' in x]

def process(arg):
    id, keys = arg
    keys = '\n'.join(keys)
    shell.run(f'aws-ec2-ssh {id} --yes --cmd "python /mnt/combined.py" --stdin "{keys}" >/dev/null', stream=True)

pool.thread.size = len(ids)

args = zip(ids, util.iter.chunks(keys, num_chunks=len(ids)))

list(pool.thread.map(process, args))
