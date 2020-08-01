## scaling python data processing horizontally

full source code is available [here](https://github.com/nathants/posts/tree/002/002_scaling_python_data_processing_horizontally).

we scaled an analysis of the nyc taxi dataset [vertically](/posts/scaling-python-data-processing-vertically) on a single machine, now let's scale horizontally on multiple machines. instead of a single i3en.24xlarge we'll use twelve i3en.2xlarge.

we'll be using the same code and aws setup from before, so refer to [that](/posts/scaling-python-data-processing-vertically) for context.

first we're going to need some ec2 instances.

```bash
>> export region=us-east-1

>> aws-ec2-max-spot-price i3en.2xlarge

on demand: 0.904, spot offers 70% savings
us-east-1b 0.271200
us-east-1c 0.271200
us-east-1d 0.271200
us-east-1f 0.272200
us-east-1a 0.288600
```

at about $0.25/hour/instance will be $3/hour.

before we start, let's note the time.

```bash
>> start=$(date +%s)
```

now it's time to spinup our machines. the following may look familiar. it is almost identical to how we instantiated our machine for [vertical scaling](/posts/scaling-python-data-processing-vertically), except that we capture and use multiple ec2 instance `$ids` instead of just one `$id`.

```bash
>> time ids=$(aws-ec2-new --type i3en.2xlarge \
                          --num 12 \
                          --ami arch \
                          --profile s3-readonly \
                          temp-machines)

real    1m57.050s
user    0m3.154s
sys     0m0.744s
```

it takes a moment to format the instance store ssd, so we wait.

```bash
>> aws-ec2-ssh $ids --yes --cmd '
       while true; do
           sleep 1
           df -h | grep /mnt && break
       done
   '
```

we aren't starting from a prebuilt ami, so we need to install some things.

```bash
>> aws-ec2-ssh $ids --yes --cmd '
       sudo pacman -Sy --noconfirm python-pip pypy3 git
       sudo pip install awscli git+https://github.com/nathants/py-{util,shell,pool}
   '
```

then we bump linux limits, reboot, and wait for the machines to come back up.

```bash
>> aws-ec2-ssh $ids --yes --cmd '
       curl -s https://raw.githubusercontent.com/nathants/bootstraps/master/scripts/limits.sh | bash
       sudo reboot
   '

>> aws-ec2-wait-for-ssh $ids --yes
```

baking an [ami](https://github.com/nathants/bootstraps/tree/master/amis) instead of starting from vanilla linux can save some bootstrap time.

our data pipeling is going to look like:

- fetch the dataset
- select the columns we need
- group by and count
- merge results

step 1 will fetch and select passengers. this pipeline will run once per input key, and will run in parallel on all cpus of every machine.

```python
# download_and_select.py
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
```

since we are running on multiple machines now, we'll need to orchestrate the activity. we'll be using a local process and ssh. the local process will divide the keys to process across the machines and monitor their execution.

```python
# orchestrate_download_and_select.py
import shell
import pool.thread
import util.iter
import sys

ids = sys.argv[1:]

prefix = "s3://nyc-tlc/trip data"

keys = [x.split()[-1] for x in shell.run(f'aws s3 ls "{prefix}/"').splitlines() if 'yellow' in x]

def download(arg):
    id, keys = arg
    keys = '\n'.join(keys)
    shell.run(f'aws-ec2-ssh {id} --yes --cmd "python /mnt/download_and_select.py" --stdin "{keys}" >/dev/null', stream=True)

pool.thread.size = len(ids)

args = zip(ids, util.iter.chunks(keys, num_chunks=len(ids)))

list(pool.thread.map(download, args))
```

```bash
>> aws-ec2-scp passenger_counts_inlined.py :/mnt $ids --yes

>> aws-ec2-scp download_and_select.py :/mnt $ids --yes

>> time python orchestrate_download_and_select.py $ids

real    1m23.778s
user    0m4.588s
sys     0m1.950s
```

step 2 will group by passengers and count. this pipeline will run once per input file, and will run in parallel on all cpus of every machine.

```python
# group_and_count.py
import os
import shell
import pool.thread

shell.run('mkdir -p /mnt/results')

paths = shell.files('/mnt/data', abspath=True)

def process(path):
    shell.run(f'< {path} pypy3 /mnt/passenger_counts_inlined.py > /mnt/results/{os.path.basename(path)}', echo=True)

pool.thread.size = os.cpu_count()

list(pool.thread.map(process, paths))
```

again the local machine will orchestrate.

```python
# orchestrate_group_and_count.py
import shell
import pool.thread
import sys

ids = sys.argv[1:]

def process(id):
    shell.run(f'aws-ec2-ssh {id} --yes --cmd "python /mnt/group_and_count.py" >/dev/null', stream=True)

pool.thread.size = len(ids)

list(pool.thread.map(process, ids))
```

```bash
>> aws-ec2-scp group_and_count.py :/mnt $ids --yes

>> time python orchestrate_group_and_count.py $ids

real    0m17.984s
user    0m2.980s
sys     0m0.933s
```

step 3 will merge the results. this pipeline runs locally on a single core and takes all results as input.

```python
# merge_results.py
import sys
import collections
import shell

ids = ' '.join(sys.argv[1:])

with shell.tempdir():
    shell.run(f'aws-ec2-rsync :/mnt/results/ ./results/ --yes {ids} 1>&2', stream=True)

    result = collections.defaultdict(int)

    for path in shell.files('results', abspath=True):
        with open(path) as f:
            for line in f:
                passengers, count = line.split(',')
                result[passengers] += int(count)

    for passengers, count in result.items():
        print(f'{passengers},{count}')
```

```bash
>> time python merge_results.py $ids \
    | tr , " " \
    | sort -nrk 2 \
    | head -n9 \
    | column -t

real    0m2.638s
user    0m0.465s
sys     0m0.095s
```

an optimization we can apply here is to combine steps 1 and 2, which will avoid iowait as a bottleneck since we never touch local disk.

```python
# combined.py
import os
import shell
import pool.thread
import sys

shell.run('mkdir -p /mnt/results')

prefix = "s3://nyc-tlc/trip data"

keys = sys.stdin.read().splitlines()

def process(key):
    shell.run(f'aws s3 cp "{prefix}/{key}" - '
              f'| cut -d, -f1-5'
              f'| pypy3 /mnt/passenger_counts_inlined.py'
              f'> /mnt/results/{key}',
              echo=True)

pool.thread.size = os.cpu_count()

list(pool.thread.map(process, keys))
```

```python
# orchestrate_combined.py
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
```

```bash
>> aws-ec2-scp combined.py :/mnt $ids --yes

>> time python orchestrate_combined.py $ids

real    1m19.735s
user    0m4.867s
sys     0m1.949s
```

since we are paying $3/hour for this instance, let's shut it down. it's a spot instance, and the default behavior is to terminate on shutdown, so we can simply power it off.

```bash
>> aws-ec2-ssh $id -yc 'sudo poweroff'
```

lets see how much money we spent getting this result.

```bash
>> echo job took $(( ($(date +%s) - $start) / 60 )) minutes

job took 8 minutes
```

for less than $1, we analyzed a 250GB dataset with python on a cluster of twelve machines. an individual query took as little as 18 seconds reading from local disk, or 80 seconds reading from s3.

interestingly, this is up from 10 seconds and 60 seconds respectively in the [vertical scaling](/posts/scaling-python-data-processing-vertically) post, suggesting that both network and disk io performance varies with instance size.

we iterated rapidly on local code with a sample of data, and in production with all of the data. we've experimented with several options for a simple data pipeline on large single machines or multiple small machines. we've answered some questions, and discovered more. we did all of this simply, quickly, and for less than the cost of a cup of coffee. most importantly, it was fun.

when analyzing data, it's always good to check the results with an alternate implementation. if they disagree, at least one of them is wrong. you can find alternate implementations of this analysis [here](https://github.com/nathants/s4/tree/master/examples/nyc_taxi_bsv).
