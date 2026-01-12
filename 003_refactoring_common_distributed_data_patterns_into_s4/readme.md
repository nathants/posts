## refactoring common distributed data patterns into s4

full source code is available [here](https://github.com/nathants/posts/tree/003/003_refactoring_common_distributed_data_patterns_into_s4).

in [horizontal scaling](/posts/scaling-python-data-processing-horizontally) we manually managed distributed compute across a cluster of machines. we used ssh to execute commands. we created directories and files to hold results. we used rsync to fetch results from multiple machines and merged them locally. we manually managed parallelism in our data scripts.

this wasn't particularly difficult, but neither was it important. let's refactor and build some tooling so next time we can focus more on the data and less on low level details of distributed compute.

our data pipeline looked like:

- fetch the dataset
- select the columns we need
- group by and count
- merge results

let's break that down a bit.

| input | command | output |
| -- | -- | -- |
| files | fetch | files |
| files | select columns | files |
| files | group and count | files |
| files | merge results | file |

it looks like we have two types of things going on.

first we have a 1:1 map of input files to output files through a command. we can imagine it as:

```bash
for file in inputs/*; do
    cat $file | $command > outputs/$(basename $file)
done
```

second we have a n:1 map of input files to output file though a command. we can imagine it as:

```bash
cat inputs/* | $command > output
```

we don't have it in this pipeline, but we can imagine a third type as a 1:n map of input file to output files through a command:

```bash
cat input | $command --outdir=outputs/
```

let's code by wishful thinking. what would our pipeline look like if we had something that helped us do these three types of things? let's imagine something like s3.

first we fetch the dataset. our inputs will be keys, the outputs will be the key data, and the command will be copy. first we need to put the inputs.

```bash
>> prefix='s3://nyc-tlc/trip data'

>> keys=$(aws s3 ls "$prefix/" \
           | grep yellow \
           | awk '{print $NF}')

>> for key in $key; do
      echo "$prefix/$key" | aws s3 cp - s3://inputs/$key
   done

>> aws s3 ls s3://inputs/ | head -n3

yellow_tripdata_2009-01.csv
yellow_tripdata_2009-02.csv
yellow_tripdata_2009-03.csv
```

now that we have our inputs, we can do a 1:1 map.

```bash
>> aws s3 map \
    --in  s3://inputs/ \
    --out s3://step1/ \
    --cmd 'cat > key && aws s3 cp $(cat key) -'
```

next we select the columns with a 1:1 map.


```bash
>> aws s3 map \
    --in  s3://step1/ \
    --out s3://step2/ \
    --cmd "cut -d, -f1-5"
```

next we group and count with a 1:1 map.

```bash
>> aws s3 map \
    --in  s3://step2/ \
    --out s3://step3/ \
    --cmd "pypy3 passenger_counts_inlined.py"
```

finally we merge the results with a n:1 map, and fetch the result.

```bash
>> aws s3 map-from-n \
    --in  s3://step3/ \
    --out s3://result \
    --cmd "python merge_results.py"

>> aws s3 cp s3://result -
```

let's put that all together and see what we've got.

```bash
>> aws s3 map        --in s3://inputs/ --out s3://step1/ --cmd 'cat > key && aws s3 cp $(cat key) -'
>> aws s3 map        --in s3://step1/  --out s3://step2/ --cmd 'cut -d, -f1-5'
>> aws s3 map        --in s3://step2/  --out s3://step3/ --cmd 'pypy3 passenger_counts_inlined.py'
>> aws s3 map-from-n --in s3://step3/  --out s3://result --cmd 'python merge_results.py'
>> aws s3 cp s3://result -
```

now we have a series of steps, mapping immutable inputs to immutable outputs. we have no details of infrastructure, data location, or data transfer. we can imagine taking any of these commands and running them locally to debug or optimize. this feels better than threadpools, rsync, and ssh. it's too bad none of this works.

[s3](https://aws.amazon.com/s3/) is a pinnacle of modern engineering. it scales automatically, is comically durable, quite available, and significantly cheaper than [ebs](https://aws.amazon.com/ebs/). in it's [standard](https://aws.amazon.com/s3/storage-classes/#General_purpose) storage class it replicates across availability zones without bandwidth charges. within the same region, bandwidth between ec2 and s3 is free.

we want to use s3 for durability and scalability. we also want simple distributed compute like we imagined above. let's spin up a system to compliment s3. we'll call it  [s4](https://github.com/nathants/s4).

for a moment, let's think about scope reduction and what we don't want.
- we don't want it to be highly durable or available, because we already have s3 for that.
- we don't want it to use complicated failure handling, because we can retry idempotent commands on immutable data.
- we don't want it to handle security or authentication, because those can be network level concerns.
- we don't want it to allow updates to data unless explicitly deleted, because immutability is a simplifying constraint.

this narrower scope means the system is easier to [use](https://github.com/nathants/s4#api), has [simpler implementation](https://github.com/nathants/s4/tree/go/s4.go), and is more likely to be [correct](https://github.com/nathants/s4/tree/go/tests).

let's give it a try. first we install [s4](https://github.com/nathants/s4#install) and then spin up a [cluster](https://github.com/nathants/s4/tree/go/scripts/new_cluster.sh). we'll size the cluster the same as we did in [horizontal scaling](/posts/scaling-python-data-processing-horizontally).

```bash

>> git clone https://github.com/nathants/s4

>> cd s4

>> python3 -m pip install -r requirements.txt .

>> export region=us-east-1

>> name=s4-cluster

>> time type=i3en.xlarge num=12 bash scripts/new_cluster.sh $name

5m17.052s
```

next we'll [proxy traffic](https://github.com/nathants/s4/tree/go/scripts/connect_to_cluster.sh) through a machine in the cluster. assuming the security group only allows port 22, the machines are only accessible on their internal addresses. since we already have ssh setup, we'll use [sshuttle](https://github.com/sshuttle/sshuttle). run this in a second terminal, and don't forget to set region to us-east-1.

```bash
>> export region=us-east-1

>> name=s4-cluster

>> bash scripts/connect_to_cluster.sh $name
```

let's check the cluster [health](https://github.com/nathants/s4#s4-health).

```bash
>> s4 health

healthy:   10.0.30.103:8080
healthy:   10.0.18.21:8080
healthy:   10.0.29.44:8080
healthy:   10.0.22.60:8080
healthy:   10.0.28.41:8080
healthy:   10.0.29.17:8080
healthy:   10.0.18.163:8080
healthy:   10.0.24.118:8080
healthy:   10.0.22.203:8080
healthy:   10.0.19.10:8080
healthy:   10.0.26.213:8080
healthy:   10.0.28.124:8080
```

we want to be able to place keys on machines. we'll use [consistent hashing](https://github.com/nathants/s4/search?q=%22func+hash%22&type=Code) to automatically place or [numeric prefixes](https://github.com/nathants/s4/search?q=%22func KeyPrefix%22&type=Code) to explicitly place keys around the cluster.


we want to be able to [put](https://github.com/nathants/s4#s4-cp), [get](https://github.com/nathants/s4#s4-cp), and [list](https://github.com/nathants/s4#s4-ls) keys across a cluster of machines. let's try putting some data which is explicitly placed with numeric prefixes.

```bash
>> echo input_a | s4 cp - s4://inputs/000_machine0

>> echo input_b | s4 cp - s4://inputs/001_machine1
```

we want to be able to map [1:1](https://github.com/nathants/s4#s4-map). let's try replacing some text.

```bash
>> s4 map s4://inputs/ s4://mapped/ "sed s/input/output/"

>> for key in $(s4 ls -r s4://mapped/ | awk '{print $NF}'); do
       echo $key '=>' $(s4 eval s4://mapped/$key cat)
   done

000_machine0 => output_a
001_machine1 => output_b
```

we want to be able to map [1:n](https://github.com/nathants/s4#s4-map-to-n) to shuffle data around the cluster. each input key becomes an output directory filled with keys that will be placed around the cluster according to their name. let's try duplicating some content from the first two machines in the cluster to the next two.

```bash
>> s4 map-to-n s4://inputs/ s4://shuffled/ '
       cat > content
       for i in {2..3}; do
           file=$(printf "%03d" $i)_machine$i
           echo -n "$(cat content)$i" > $file
           echo $file
       done
   '

>> for key in $(s4 ls -r s4://shuffled/ | awk '{print $NF}'); do
       echo $key '=>' $(s4 eval s4://shuffled/$key cat)
   done

000_machine0/002_machine2 => input_a2
000_machine0/003_machine3 => input_a3
001_machine1/002_machine2 => input_b2
001_machine1/003_machine3 => input_b3
```

we want to be able to merge shuffled data with [n:1](https://github.com/nathants/s4#s4-map-from-n) maps. let's merge the content we just duplicated.

```bash
>> s4 map-from-n s4://shuffled/ s4://merged/ "xargs cat"

>> for key in $(s4 ls -r s4://merged/ | awk '{print $NF}'); do
       echo $key '=>' $(s4 eval s4://merged/$key cat)
   done

002_machine2 => input_a2 input_b2
003_machine3 => input_a3 input_b3
```

all files with the same name have been merged into a single file with that name.

now that we've seen all of the maps in action, let's summarize their semantics.
- maps take directories as inputs and outputs and operate on the keys in those directories.
- 1:1 map commands take data on stdin and emit data on stdout.
- 1:n map commands take data on stdin, write files to disk, and emit file names on stdout.
- n:1 map commands take file names on stdin, and emit data on stdout.

key names are important, since they define data placement around the cluster.
- 1:1 map operates on keys on a single machine.
- 1:n map will shuffle output keys around the cluster.
- n:1 map operates on keys on a single machine.

now let's try redoing the analysis from [horizontal scaling](/posts/scaling-python-data-processing-horizontally) with [s4](https://github.com/nathants/s4).

we'll be working with the [nyc taxi](https://registry.opendata.aws/nyc-tlc-trip-records-pds/) dataset in the aws region where it lives, us-east-1. bandwidth between ec2 and s3 is only free within the same region, so make sure you are in us-east-1 if you are following along.

we'll be using some [aws tooling](https://github.com/nathants/cli-aws) and the [official aws cli](https://aws.amazon.com/cli/). one could also use other tools without much trouble.

we've already spun up an s4 cluster in us-east-1, but let's delete it and make a new one. clusters spin up fast and should only contain ephemeral data. they spin up even faster when using a prebuilt [ami](https://github.com/nathants/bootstraps/blob/master/amis/s4.sh) instead of live bootstrapping.

```bash
>> export region=us-east-1

>> name=s4-cluster

>> aws-ec2-rm $name --yes

>> time ami=s4 type=i3en.xlarge num=12 bash scripts/new_cluster.sh $name

3m43.205s
```

first we deploy our code to every machine. note that we'll be referring to ec2 instances by name instead of id.

```bash
>> aws-ec2-scp passenger_counts_inlined.py :/mnt $name --yes
```

now we add the s3 keys of the input data to s4 so that we can map over them.

```bash
>> prefix='s3://nyc-tlc/trip data'

>> keys=$(aws s3 ls "$prefix/" \
           | grep yellow \
           | awk '{print $NF}' \
           | while read key; do
                 echo "$prefix/$key"
             done)

>> echo "$keys" | while read key; do
       echo "$key"
       echo "$key" | s4 cp - s4://inputs/$(basename "$key")
   done
```

let's take a peek at the data.

```bash
>> s4 ls s4://inputs/ | head -n3 | awk '{print $NF}'

yellow_tripdata_2009-01.csv
yellow_tripdata_2009-02.csv
yellow_tripdata_2009-03.csv

>> s4 eval s4://inputs/yellow_tripdata_2009-01.csv cat

s3://nyc-tlc/trip data/yellow_tripdata_2009-01.csv
```

now let's run our data pipeline.

```bash
>> time s4 map s4://inputs/ s4://step1/ 'cat > url && aws s3 cp "$(cat url)" -'

1m4.920s

>> time s4 map s4://step1/ s4://step2/ 'cut -d, -f1-5'

0m46.054s

>> time s4 map s4://step2/ s4://step3/ 'pypy3 /mnt/passenger_counts_inlined.py'

0m20.310s
```

we can't merge our results until they are all on one machine, so we need to map 1:n, where n=1, sending all results to the same machine. to do this we are putting all data into keys with the same name, which places them on the same machine.

```bash
>> time s4 map-to-n s4://step3/ s4://step4/ 'cat > results && echo results'

0m1.729s
```

now that all results are on the same machine, we can merge the results with a n:1 map.

```python
# merge_results.py
import sys
import collections
import shell

result = collections.defaultdict(int)

for line in sys.stdin:
    passengers, count = line.split(',')
    result[passengers] += int(count)

for passengers, count in result.items():
    print(f'{passengers},{count}')
```

```bash
>> aws-ec2-scp merge_results.py :/mnt $name --yes

>> time s4 map-from-n s4://step4/ s4://step5/ 'xargs cat | python /mnt/merge_results.py'

0m0.464s
```

finally we fetch the result.

```bash
>> s4 eval s4://step5/results "
       tr , ' ' \
       | sort -nrk2 \
       | head -n9 \
       | column -t
   "

1  1135227331
2  239684017
5  103036920
3  70434390
6  38585794
4  34074806
0  6881330
7  2040
8  1609
```

let's run at the pipeline again. note that keys cannot be updated, so before we can rerun the pipeline we have to delete intermediate results. we'll delete everything except the inputs.

```bash
>> s4 rm -r s4://step

>> time s4 map        s4://inputs/ s4://step1/ 'cat > url && aws s3 cp "$(cat url)" -'

1m5.620s

>> time s4 map        s4://step1/  s4://step2/ 'cut -d, -f1-5'

0m38.109s

>> time s4 map        s4://step2/  s4://step3/ 'pypy3 /mnt/passenger_counts_inlined.py'

0m19.917s

>> time s4 map-to-n   s4://step3/  s4://step4/ 'cat > results && echo results'

0m1.641s

>> time s4 map-from-n s4://step4/  s4://step5/ 'xargs cat | python /mnt/merge_results.py'

0m0.430s
```

we can optimize by merging some of these steps.

```bash
>> s4 rm -r s4://step

>> time s4 map s4://inputs/ s4://step1/ 'cat > url
                                         aws s3 cp "$(cat url)" - \
                                          | cut -d, -f1-5 \
                                          | pypy3 /mnt/passenger_counts_inlined.py'

1m56.197s
```

performance improves, but we can no longer measure steps independently. sometimes we should combine steps, others we should pull them apart.

while we've got the cluster up, let's do one more thing. we haven't really flexed 1:n and n:1 maps properly yet, so let's do that. the taxi dataset is organized into files by date. let's reorganize it by passenger count. this will make it easier to answer questions about the trips for a given passenger count by without scanning the entire dataset.

we're going to need a new data script for our 1:n map. it will partition data by passenger count into separate files. these files will be shuffled around the cluster according to their name. then we'll merge files with the same name into a single file. we're going to further partition each passenger count randomly into multiple files to more evenly spread the data around the cluster. we'll make 12 files per passenger count, the same as cluster size.

```python
# partition_by_passengers.py
import sys
import random

cluster_size = int(sys.argv[1])

sys.stdin.readline() # skip the header

files = {}

for line in sys.stdin:
    cols = line.split(',')
    try:
        passengers = int(cols[3])
    except (IndexError, ValueError):
        continue
    else:
        randint = random.randint(0, cluster_size)
        filename = f'passengers_{passengers}_{randint:03d}.csv'
        if filename not in files:
            files[filename] = open(filename, 'w')
        files[filename].write(line)

for name, file in files.items():
    print(name)
    file.close()
```

```bash
>> aws-ec2-scp partition_by_passengers.py :/mnt $name --yes

>> s4 rm -r s4://step

>> time s4 map        s4://inputs/ s4://step1/ 'cat > url && aws s3 cp "$(cat url)" -'

1m16.529s

>> time s4 map        s4://step1/  s4://step2/ 'cut -d, -f1-5'

0m38.528s

>> time s4 map-to-n   s4://step2/  s4://step3/ 'pypy3 /mnt/partition_by_passengers.py 12'

2m11.914s

>> time s4 map-from-n s4://step3/  s4://step4/ 'xargs cat'

0m25.288s
```

earlier we did a 1:n map, where n=1, sending all results to a single machine. here we did a 1:n map, where n>1, sending results all around the cluster.

earlier we followed that with a n:1 map which ran on a single machine, since only that machine had data. here we followed that with a n:1 map which ran on every machine, since every machine had data, merging the shuffled pieces of data back into single files.

since we partitioned the data in a way that spread it evenly around the cluster, we [could](https://gist.github.com/nathants/fa0044092e4c098763e35326ba704769) [see](https://nathants-public.s3-us-west-2.amazonaws.com/grid.gif) during processing that all machines were busy and then all went idle at the same time. if we hadn't partitioned this way we likely would have seen a few machines staying busy while the rest went idle.

let's take a peak at the data.

```bash
>> s4 ls s4://step4/ \
    | awk '{print $3, $4}' \
    | head -n3 \
    | column -t

29120189  passengers_0_000.csv
29084534  passengers_0_001.csv
29021334  passengers_0_002.csv

>> s4 eval s4://step4/passengers_0_000.csv "head -n1"

DDS,2009-01-06 06:46:08,2009-01-06 07:03:10,0,4.2999999999999998

>> s4 eval s4://step4/passengers_5_000.csv "head -n1"

VTS,2009-01-27 14:41:00,2009-01-27 14:48:00,5,1.1299999999999999

s4 eval s4://step4/passengers_5_000.csv "cut -d, -f2 | grep -Eo '^.{4}' | sort | uniq -c | sort -nr"

1145682 2009
1095902 2010
1065193 2011
 927308 2012
 771382 2013
 713021 2014
 609841 2015
 521383 2016
 414996 2017
 353959 2018
 261314 2019
  43358 2020
      1 2008
```

normally at this point we'd push the results back to s3 to make them durable, but our cluster has read only access, so we won't be doing that.

while we've got a cluster up, let's take a look at performance. what's the biggest and smallest file in the dataset?

```bash
>> aws s3 ls --recursive nyc-tlc/ | sort -nk3 | tail -n1

2016-08-15 08:50:21 2994922424 trip data/yellow_tripdata_2012-03.csv

>> aws s3 ls --recursive nyc-tlc/ | sort -nk3 | head -n3

2016-08-11 07:16:22          0 trip data/
2016-08-17 07:54:39          0 misc/
2016-08-17 07:57:08      12322 misc/taxi _zone_lookup.csv
```

```bash
>> s4 ls -r s4://step1/yellow_tripdata_2012-03.csv | awk '{print $3, $4}'

2994922424 yellow_tripdata_2012-03.csv
```

let's copy the smallest file to s4.

```bash
>> aws s3 cp "s3://nyc-tlc/misc/taxi _zone_lookup.csv" - | s4 cp - s4://small/data.csv
```

let's copy the biggest file from s3 and from s4. we'll run this test on the first machine in the cluster, since the big file doesn't live on that machine.

```bash
>> id=$(aws-ec2-id $name | head -n1)

>> aws-ec2-ssh $id --yes --cmd '
       time aws s3 cp "s3://nyc-tlc/trip data/yellow_tripdata_2012-03.csv" - >/dev/null
   '

0m15.018s

>> aws-ec2-ssh $id --yes --cmd '
       time s4 cp s4://step1/yellow_tripdata_2012-03.csv - >/dev/null
   '

0m3.251s
```

now let's copy the smallest file several times in a loop.

```bash
>> aws-ec2-ssh $id --yes --cmd '
       set -x
       time for i in {1..20}; do
           aws s3 cp "s3://nyc-tlc/misc/taxi _zone_lookup.csv" - >/dev/null
       done
   '

0m7.909s

>> aws-ec2-ssh $id --yes --cmd '
       set -x
       time for i in {1..20}; do
           s4 cp "s4://small/data.csv" - >/dev/null
       done
   '

0m2.193s
```

we're done for now, so let's delete the cluster.

```bash
>> aws-ec2-rm $name --yes
```

clearly s3 and s4 have different performance characteristics, and if we think about their goals, we can understand why.

s3 is durable, elastic, and authenticated. s4 is ephemeral, static, and unauthenticated.

s3 goes slower and almost certainly won't lose data. s4 goes faster and probably won't lose data.

s3 must not fail. s4 may fail and retry strategies must be considered.

these two systems are perfect compliments. we want durability, but we don't need it at every step. we want distributed compute, but we don't want to manually manage the details. we want data shuffle, but we don't want complicated infrastructure or poor performance.

using s4 we can focus more on our data pipelines, and less on low level details of distributed compute. our data pipelines can start, end, and checkpoint to durable data in s3. everywhere in between they can use s4 to map arbitrary commands over ephemeral immutable data in 1:1, 1:n and n:1 operations.

you can find more examples of s4 [here](https://github.com/nathants/s4/tree/go/examples), where further analysis of the nyc taxi dataset is done with python and [bsv](https://github.com/nathants/bsv). to verify results and provide a performance baseline the analysis is repeated with [presto](https://prestodb.io/) on [emr](https://aws.amazon.com/emr/).
