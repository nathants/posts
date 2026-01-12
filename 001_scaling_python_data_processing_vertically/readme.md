## scaling python data processing vertically

full source code is available [here](https://github.com/nathants/posts/tree/master/001_scaling_python_data_processing_vertically).

processing inconveniently large data is a common task these days, and there are many tools and techniques available to help. here we are going to explore how far we can take python on a single machine.

we'll be working with the [nyc taxi](https://registry.opendata.aws/nyc-tlc-trip-records-pds/) dataset in the aws region where it lives, us-east-1. bandwidth between ec2 and s3 is only free within the same region, so make sure you are in us-east-1 if you are following along.

we'll be using some [bash functions](https://gist.github.com/nathants/741b066af9faa15f3ed50ed6cf677d67), [aws tooling](https://github.com/nathants/cli-aws), and the [official aws cli](https://aws.amazon.com/cli/). one could also use other tools without much trouble.

how is the dataset organized?

```bash
>> aws s3 ls 's3://nyc-tlc/trip data/' | head

2016-08-11 07:16:22          0
2016-08-11 07:32:21   85733063 fhv_tripdata_2015-01.csv
2016-08-11 07:33:04   97863482 fhv_tripdata_2015-02.csv
2016-08-11 07:33:40  102220197 fhv_tripdata_2015-03.csv
2016-08-11 07:34:24  121250461 fhv_tripdata_2015-04.csv
2016-08-11 07:35:14  133469666 fhv_tripdata_2015-05.csv
2016-08-11 07:35:48  132209226 fhv_tripdata_2015-06.csv
2016-08-11 07:36:09  137153004 fhv_tripdata_2015-07.csv
2016-08-11 07:36:45  164291700 fhv_tripdata_2015-08.csv
2016-08-11 07:37:37  205607912 fhv_tripdata_2015-09.csv
```

looks like a bunch of csv in a folder. are the prefixes constant?

```bash
>> aws s3 ls 's3://nyc-tlc/trip data/' \
    | awk '{print $NF}' \
    | cut -d_ -f1 \
    | sort \
    | uniq -c

      1 0
     64 fhv
     17 fhvhv
     83 green
    138 yellow
```

nope. we probably want the yellow data. let's check on the sizes first.

```bash
>> aws s3 ls 's3://nyc-tlc/trip data/' \
    | grep yellow \
    | awk '{print $3}' \
    | py '"{:,}".format(sum(int(x) for x in i.splitlines()))'

251,267,607,652
```

looks like about 250GB. what about the others?

```bash
>> aws s3 ls 's3://nyc-tlc/trip data/' \
    | awk '{print $NF}' \
    | cut -d_ -f1 \
    | sort -u \
    | tail -n+2 \
    | while read prefix; do
          echo $prefix $(aws s3 ls "s3://nyc-tlc/trip data/${prefix}_" \
                          | awk '{print $3}' \
                          | py '"{:,}".format(sum(int(x) for x in i.splitlines())).rjust(20, ".")')
      done \
    | column -t

fhv     ......37,567,264,171
fhvhv   ......19,542,027,956
green   ......10,381,632,797
yellow  .....251,267,607,652
```

definitely the yellow dataset then. let's setup some convenience variables.

```bash
>> prefix='s3://nyc-tlc/trip data'
>> keys=$(aws s3 ls "$prefix/" \
    | grep yellow \
    | awk '{print $NF}')
```

let's take a peek at the headers of the first file for each year, selecting the first 10 columns.

```bash
>> (for key in $(echo "$keys" | awk 'NR % 12 == 1'); do
       aws s3 cp "$prefix/$key" - 2>/dev/null \
        | head -n1 \
        | cut -d, -f1-8 &
    done; wait) | column -s, -t

VendorID     tpep_pickup_datetime  tpep_dropoff_datetime  passenger_count   trip_distance   pickup_longitude   pickup_latitude     RateCodeID
VendorID     tpep_pickup_datetime  tpep_dropoff_datetime  passenger_count   trip_distance   RatecodeID         store_and_fwd_flag  PULocationID
vendor_id    pickup_datetime       dropoff_datetime       passenger_count   trip_distance   pickup_longitude   pickup_latitude     rate_code
vendor_id    pickup_datetime       dropoff_datetime       passenger_count   trip_distance   pickup_longitude   pickup_latitude     rate_code
VendorID     tpep_pickup_datetime  tpep_dropoff_datetime  passenger_count   trip_distance   RatecodeID         store_and_fwd_flag  PULocationID
vendor_id    pickup_datetime       dropoff_datetime       passenger_count   trip_distance   pickup_longitude   pickup_latitude     rate_code
VendorID     tpep_pickup_datetime  tpep_dropoff_datetime  passenger_count   trip_distance   pickup_longitude   pickup_latitude     RatecodeID
vendor_id    pickup_datetime       dropoff_datetime       passenger_count   trip_distance   pickup_longitude   pickup_latitude     rate_code
VendorID     tpep_pickup_datetime  tpep_dropoff_datetime  passenger_count   trip_distance   RatecodeID         store_and_fwd_flag  PULocationID
vendor_name  Trip_Pickup_DateTime  Trip_Dropoff_DateTime  Passenger_Count   Trip_Distance   Start_Lon          Start_Lat           Rate_Code
VendorID     tpep_pickup_datetime  tpep_dropoff_datetime  passenger_count   trip_distance   RatecodeID         store_and_fwd_flag  PULocationID
vendor_id    pickup_datetime       dropoff_datetime       passenger_count   trip_distance   pickup_longitude   pickup_latitude     rate_code
```

looks like the first 5 columns are consistent, and then it gets messy. we can punt on data cleanup by just working with those first 5, which contain interesting data like distance, passengers, and date.

before we jump on ec2, let's grab the first million rows of the first file to our local environment and prototype our data scripts.

```bash
>> aws s3 cp "$prefix/$(echo $keys | awk '{print $1}')" - 2>/dev/null \
    | head -n1000000 \
    > /tmp/taxi.csv

>> ls -lh /tmp/taxi.csv | awk '{print $5}'

172M

>> head /tmp/taxi.csv \
    | cut -d, -f1-5 \
    | column -s, -t

vendor_name  Trip_Pickup_DateTime  Trip_Dropoff_DateTime  Passenger_Count  Trip_Distance
VTS          2009-01-04 02:52:00   2009-01-04 03:02:00    1                2.6299999999999999
VTS          2009-01-04 03:31:00   2009-01-04 03:38:00    3                4.5499999999999998
VTS          2009-01-03 15:43:00   2009-01-03 15:57:00    5                10.35
DDS          2009-01-01 20:52:58   2009-01-01 21:14:00    1                5
DDS          2009-01-24 16:18:23   2009-01-24 16:24:56    1                0.40000000000000002
DDS          2009-01-16 22:35:59   2009-01-16 22:43:35    2                1.2
DDS          2009-01-21 08:55:57   2009-01-21 09:05:42    1                0.40000000000000002
VTS          2009-01-04 04:31:00   2009-01-04 04:36:00    1                1.72
```

now that we have data, it's time to ask questions. let's group by passengers and count.

first let's try python's [csv](https://docs.python.org/3/library/csv.html) module.


```python
# passenger_counts_stdlib.py
import csv
import sys
import collections

sys.stdin.readline() # skip the header

result = collections.defaultdict(int)

for cols in csv.reader(sys.stdin):
    try:
        passengers = cols[3]
    except IndexError:
        continue
    else:
        result[passengers] += 1

for passengers, count in result.items():
    print(f'{passengers},{count}')
```

```bash
>> time cat /tmp/taxi.csv \
    | python3 passenger_counts_stdlib.py \
    | sort -nr -k2 -t, \
    | column -s, -t

1  669627
2  166658
5  93718
3  44360
4  20904
6  4685
0  46

real    0m2.316s
user    0m2.259s
sys     0m0.162s
```

let's see how [pandas](https://pandas.pydata.org/) compares.

```python
# passenger_counts_pandas.py
import pandas
import sys

df = pandas.read_csv(sys.stdin)

print(df.iloc[:,3].value_counts())
```

```bash
>> time cat /tmp/taxi.csv | python3 passenger_counts_pandas.py

1    669627
2    166658
5     93718
3     44360
4     20904
6      4685
0        46
Name: Passenger_Count   dtype: int64

real    0m2.164s
user    0m2.085s
sys     0m0.499s
```

about the same.

if we know that our input is well formed, without quotes or escaped delimiters, we can just split on comma. let's try that.

```python
# passenger_counts.py
import sys
import collections

sys.stdin.readline() # skip the header

result = collections.defaultdict(int)

for line in sys.stdin:
    cols = line.split(',')
    try:
        passengers = cols[3]
    except IndexError:
        continue
    else:
        result[passengers] += 1

for passengers, count in result.items():
    print(f'{passengers},{count}')
```

```bash
>> time cat /tmp/taxi.csv \
    | python3 passenger_counts.py \
    | sort -nr -k2 -t, \
    | column -s, -t

1  669627
2  166658
5  93718
3  44360
4  20904
6  4685
0  46

real    0m0.668s
user    0m0.633s
sys     0m0.099s
```

that is a lot faster, about x4. if we can safely assume that the data is well formed, simple split looks like a good idea. after peeking at this dataset for the fields we care about, this is likely ok.

let's run it again with x25 more data by repeating the input over and over. using tail we can skip the header in all but the first input.

```bash
>> time (cat /tmp/taxi.csv; for i in {1..24}; do tail -n+2 /tmp/taxi.csv; done) \
    | python3 passenger_counts.py &>/dev/null

real    0m16.295s
user    0m16.101s
sys     0m2.771s
```

what if we try [pypy](https://pypy.org)?

```bash
>> time (cat /tmp/taxi.csv; for i in {1..24}; do tail -n+2 /tmp/taxi.csv; done) \
    | pypy3 passenger_counts.py &>/dev/null

real    0m17.260s
user    0m16.386s
sys     0m4.011s
```

well that's not ideal. let's see if we can apply performance lessons from compiled languages, which can be summarized as avoid allocations and do as little work as possible. the following file has some [boiler plate](https://github.com/nathants/py-csv) elided, refer to the full [source](https://github.com/nathants/posts/tree/001/001_scaling_python_data_processing_vertically/passenger_counts_inlined.py) for the details.

```python
# passenger_counts_inlined.py
...

result = collections.defaultdict(int)

... # FOR ROW IN STDIN
    ...

    if max >= 3:
        passengers = read_buffer[starts[3]:ends[3]]
        result[passengers] += 1

...

for passengers, count in result.items():
    print(f'{passengers.decode()},{count}')
```

```bash
>> time (cat /tmp/taxi.csv; for i in {1..24}; do tail -n+2 /tmp/taxi.csv; done) \
    | pypy3 passenger_counts_inlined.py &>/dev/null

real    0m10.245s
user    0m8.876s
sys     0m3.108s
```

a x2 improvement on user time, and nearly as much on wall clock. we'll take it. if interested, see further optimizations in [go, rust, and c](https://github.com/nathants/bsv/tree/master/experiments/cut).

a final optimization we can make is to work with less data. since we know we only care about the first 5 columns, we can drop unused data upstream.

```bash
>> time cat /tmp/taxi.csv | cut -d, -f1-5 > /tmp/taxi.csv.slim

real    0m0.409s
user    0m0.359s
sys     0m0.140s
```

```bash
>> time (cat /tmp/taxi.csv.slim; for i in {1..24}; do tail -n+2 /tmp/taxi.csv.slim; done) \
    | pypy3 passenger_counts_inlined.py &>/dev/null

real    0m3.764s
user    0m3.196s
sys     0m1.155s
```

another x2 improvement, we'll take it.

our first significant improvement we got by avoiding allocations, and here we get another one by dropping unused data upstream.

let's take another look at our improvements.

```bash
>> time (cat /tmp/taxi.csv; for i in {1..24}; do tail -n+2 /tmp/taxi.csv; done) \
    | python3 passenger_counts_stdlib.py &>/dev/null

real    0m57.986s
user    0m57.854s
sys     0m3.610s

>> time (cat /tmp/taxi.csv.slim; for i in {1..24}; do tail -n+2 /tmp/taxi.csv.slim; done) \
    | pypy3 passenger_counts_inlined.py &>/dev/null

real    0m3.726s
user    0m3.401s
sys     0m0.907s
```

by doing less work, manually inlining code, avoiding allocations, and reducing the data set, we can get sizeable performance improvements.

just for fun, let's take a look at going even faster. we'll explore this in a later [post](https://nathants.com/posts)

```bash
>> cat /tmp/taxi.csv \
    | tail -n+2 \
    | bsv \
    | bschema *,*,*,*,*,... --filter \
    > /tmp/taxi.bsv.slim

>> time (for i in {1..25}; do cat /tmp/taxi.bsv.slim; done) \
    | bcut 4 \
    | bcounteach-hash >/dev/null

real    0m0.742s
user    0m0.801s
sys     0m0.950s
```

system time as the bottleneck is a really good problem to have.

back to python, it's time to deploy and scale vertically. first we're going to need an ec2 instance. let's use a [i3en.24xlarge](https://aws.amazon.com/ec2/instance-types/i3en/) with [archlinux](https://wiki.archlinux.org/).

```bash
>> export region=us-east-1

>> aws-ec2-max-spot-price i3en.24xlarge

on demand: 10.848, spot offers 70% savings
us-east-1a 3.254400
us-east-1b 3.254400
us-east-1c 3.254400
us-east-1d 3.254400
us-east-1f 3.254400
```

looks like cost will be $3/hour.

our machine is going to need s3 access to get the dataset, so let's make an instance profile.

```bash
>> aws-iam-ensure-instance-profile \
    --policy AmazonS3ReadOnlyAccess \
    s3-readonly
```

we are also going to need a vpc, keypair, and security group access for port 22. if you already have aws setup you're probably fine, otherwise do something like this.

```bash
>> aws-vpc-new adhoc-vpc

>> aws-ec2-authorize-ip $(curl checkip.amazonaws.com) adhoc-vpc --yes

>> aws-ec2-keypair-new $(whoami) ~/.ssh/id_rsa.pub
```

before we start, let's note the time.

```bash
>> start=$(date +%s)
```

now it's time to spin up our machine.

```bash
>> time id=$(aws-ec2-new --type i3en.24xlarge \
                         --ami arch \
                         --profile s3-readonly \
                         test-machine)

real    1m10.673s
user    0m2.510s
sys     0m0.434s
```

it takes a moment to format the instance store ssd, so we wait.

```bash
>> aws-ec2-ssh $id --yes --cmd '
       while true; do
           sleep 1
           df -h | grep /mnt && break
       done
   '
```

now we need to install some things.

```bash
>> aws-ec2-ssh $id --yes --cmd '
       sudo pacman -Sy --noconfirm python-pip pypy3 git
       sudo pip install awscli git+https://github.com/nathants/py-{util,shell,pool}
   '
```

then we bump linux limits, reboot, and wait for the machine to come back up.

```bash
>> aws-ec2-ssh $id --yes --cmd '
       curl -s https://raw.githubusercontent.com/nathants/bootstraps/master/scripts/limits.sh | bash
       sudo reboot
   '

>> aws-ec2-wait-for-ssh $id --yes
```

baking an [ami](https://github.com/nathants/bootstraps/tree/master/amis) instead of starting from vanilla linux can save some bootstrap time.

let's deploy our code.

```bash
>> aws-ec2-scp passenger_counts_inlined.py :/mnt $id --yes
```

our data pipeline is going to look like:
- fetch the dataset
- select the columns we need
- group by and count
- merge results

step 1 will fetch and select passengers. this pipeline will run once per input key, and will run in parallel on all cpus.

```python
# download_and_select.py
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
```

```bash
>> aws-ec2-scp download_and_select.py :/mnt $id --yes

>> time aws-ec2-ssh $id --yes --cmd 'python /mnt/download_and_select.py'

real    1m43.209s
user    0m0.371s
sys     0m0.214s
```

step 2 will group by passengers and count. this pipeline will run once per input file, and will run in parallel on all cpus.

we'll use shell redirection instead of cat for the input since it's more efficient.

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

```bash
>> aws-ec2-scp group_and_count.py :/mnt $id --yes

>> time aws-ec2-ssh $id --yes --cmd 'python /mnt/group_and_count.py'

real    0m11.062s
user    0m0.262s
sys     0m0.018s
```

step 3 will merge the results from step 2. we haven't actually written this code yet, so let's do that now. this pipeline runs on a single core and takes all results as input.

```python
# merge_results.py
import sys
import collections

result = collections.defaultdict(int)

for line in sys.stdin:
    passengers, count = line.split(',')
    result[passengers] += int(count)

for passengers, count in result.items():
    print(f'{passengers},{count}')
```

```bash
>> aws-ec2-scp merge_results.py :/mnt $id --yes

>> time aws-ec2-ssh $id --yes --cmd '
       cat /mnt/results/* \
         | python /mnt/merge_results.py \
         | tr , " " \
         | sort -nrk 2 \
         | head -n9 \
         | column -t
   '

real    0m1.580s
user    0m0.189s
sys     0m0.038s
```

a final optimization we can apply here is to combine steps 1 and 2, which will avoid iowait as a bottleneck since we never touch local disk.

```python
# combined.py
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
```

```bash
>> aws-ec2-scp combined.py :/mnt $id --yes

>> time aws-ec2-ssh $id --yes --cmd 'python /mnt/combined.py'

real    0m53.036s
user    0m0.334s
sys     0m0.069s
```

interesting. reading from the network is faster than writing to disk, and in this case get's us a x2 wall clock improvement.

since we are paying $3/hour for this instance, let's shut it down.

```bash
>> aws-ec2-rm $id --yes
```

let's see how much money we spent getting this result.

```bash
>> echo job took $(( ($(date +%s) - $start) / 60 )) minutes

job took 6 minutes
```

for less than $1, we analyzed a 250GB dataset with python. an individual query took as little as 10 seconds reading from local disk, or 60 seconds reading from s3. vertical scaling with python is a decent technique. now that we've maxed out our instance size, the only way to scale further is to go [horizontal](/posts/scaling-python-data-processing-horizontally).

when analyzing data, it's always good to check the results with an alternate implementation. if they disagree, at least one of them is wrong. you can find alternate implementations of this analysis [here](https://github.com/nathants/s4/tree/go/examples/nyc_taxi_bsv).
