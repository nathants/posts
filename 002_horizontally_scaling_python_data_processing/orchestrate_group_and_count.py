import shell
import pool.thread
import sys

ids = sys.argv[1:]

def process(id):
    shell.run(f'aws-ec2-ssh {id} --yes --cmd "python /mnt/group_and_count.py" >/dev/null', stream=True)

pool.thread.size = len(ids)

list(pool.thread.map(process, ids))
