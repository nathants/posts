## vertically scaling python data processing

processing inconveniently large data is a common task these days, and there are many tools and techniques available to help. in this post we are going to see how far we can take python on a single machine.

we will be working with the [nyc taxi](https://registry.opendata.aws/nyc-tlc-trip-records-pds/) dataset in the aws region where it lives, us-east-1. bandwidth between ec2 and s3 is only free within the same region, so make sure you are in us-east-1 if you are working with this dataset from ec2.

we will be using some [bash functions](https://gist.github.com/nathants/741b066af9faa15f3ed50ed6cf677d67), [aws tooling](https://github.com/nathants/cli-aws), and the [official aws cli](https://aws.amazon.com/cli/). you will need these if you want to follow along, but without much fuss one could use other tools.

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

nope. ok, so we probably want the yellow data, since it's biggest. lets check on the sizes first.

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
    | sort \
    | uniq \
    | tail -n+2 \
    | while read prefix; do
       echo $prefix $(aws s3 ls "s3://nyc-tlc/trip data/${prefix}_" \
                       | awk '{print $3}' \
                       | py '"{:,}".format(sum(int(x) for x in i.splitlines())).rjust(20, ".")')
   done | column -t
fhv     ......37,567,264,171
fhvhv   ......19,542,027,956
green   ......10,381,632,797
yellow  .....251,267,607,652
```

definitely the yellow dataset then. let's setup some variables to the dataset for easier bashing.

```bash
>> prefix='s3://nyc-tlc/trip data'
>> keys=$(aws s3 ls "$prefix/" \
    | grep yellow \
    | awk '{print $NF}')
```

let's take a peek at the headers of the first file for each year, selecting the first 10 columns.

```bash
>> (for key in $(echo "$keys" | awk 'NR % 12 == 1'); do
       echo $key $(aws s3 cp "$prefix/$key" - 2>/dev/null | head -n1 | cut -d, -f1-8) &
   done; wait) | column -s, -t
```

looks like the first 5 columns are consistent, and then it gets messy after that. we can punt on data cleanup by just working with those first 5, which contain interesting data like distance, passengers, and date.

before we jump on ec2, lets grab the first million rows of the first file to our local environment, and prototype our data scripts.

```bash
>> aws s3 cp "$prefix/$(echo $keys | awk '{print $1}')" - 2>/dev/null | head -n1000000 > /tmp/taxi.csv

>> ls -lh /tmp/taxi.csv | awk '{print $5}'
172M

>> >> head /tmp/taxi.csv | cut -d, -f1-5 | column -s, -t
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

data has been acquired. it's time to ask some questions. let's group by passengers and count rides.

`passenger_counts.py`

```python
import sys
import collections

# count the columns in the header
col_count = len(sys.stdin.readline().split(','))

# setup state
bad_lines = 0
result = collections.defaultdict(int)

# process input row by row
for line in sys.stdin:
    cols = line.split(',')
    if len(cols) != col_count:
        bad_lines += 1
    else:
        passengers = cols[3]
        result[passengers] += 1

# print bad lines count to stderr
print(f'bad lines: {bad_lines}', file=sys.stderr)

# print passenger counts to stdout
for passengers, count in result.items():
    print(f'{passengers},{count}')
```

now let's push that tmp data though and see what happens.

```bash
>> time cat /tmp/taxi.csv | python3 passenger_counts.py  | sort -nr -k2 -t, | column -s, -t
bad lines: 1
1  669627
2  166658
5  93718
3  44360
4  20904
6  4685
0  46

real    0m0.904s
user    0m0.887s
sys     0m0.088s
```

172MB processed in 1 second, not bad. let's run it again with x25 more data by recyling the input over and over.

```bash
>> time (cat /tmp/taxi.csv; for i in {1..24}; do tail -n+2 /tmp/taxi.csv; done) | python3 passenger_counts.py &>/dev/null

real    0m18.006s
user    0m17.698s
sys     0m2.904s
```

what if we try pypy?

```bash
>> time (cat /tmp/taxi.csv; for i in {1..24}; do tail -n+2 /tmp/taxi.csv; done) | pypy3 passenger_counts.py &>/dev/null

real    0m17.010s
user    0m16.616s
sys     0m3.524s
```

well that's not very impressive. let's see if we can apply performance lessons from compiled languages, which can be summarized as avoid allocations and do as little work as possible. the following file has had some [boiler plate](https://github.com/nathants/py-csv) elided, refer to the [source](https://github.com/nathants/posts) for the rest.

`passenger_counts_inlined.py`

```python
...

result = collections.defaultdict(int)

... FOR ROW IN STDIN
    ...
    passengers = read_buffer[starts[3]:ends[3]]
    result[passengers] += 1
...

for passengers, count in result.items():
    print(f'{passengers.decode()},{count}')
```

```bash
>> time (cat /tmp/taxi.csv; for i in {1..24}; do tail -n+2 /tmp/taxi.csv; done) | pypy3 passenger_counts_inlined.py &>/dev/null

real    0m10.162s
user    0m8.937s
sys     0m2.975s
```

almost a x2 improvement, we'll take it. let's compare it to pandas since that's what most people will be doing.

```bash
>> time (cat /tmp/taxi.csv; for i in {1..24}; do tail -n+2 /tmp/taxi.csv; done) | python3 passenger_counts_pandas.py &>/dev/null

real    0m47.919s
user    0m42.350s
sys     0m7.774s
```

well that's not ideal. also note that pandas used gigabytes of memory here, which isn't necessary when we are doing sequential access and only every working with the current row.

a final optimization we can make is to work with less data. since we know we only care about the first 5 columns, we can slice that out upstream.

```bash
>> time cat /tmp/taxi.csv | cut -d, -f1-5 > /tmp/taxi.csv.slim

real    0m0.409s
user    0m0.359s
sys     0m0.140s
```

```bash
>> time (cat /tmp/taxi.csv.slim; for i in {1..24}; do tail -n+2 /tmp/taxi.csv.slim; done) | pypy3 passenger_counts_inlined.py &>/dev/null

real    0m3.915s
user    0m3.595s
sys     0m0.804s
```

our first x2 improvement we got by avoiding allocations, and here we get another one by dropping unused data upstream.
