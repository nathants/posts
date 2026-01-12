## performant batch processing with bsv, s4, and presto

full source code is available [here](https://github.com/nathants/posts/tree/master/005_performant_batch_processing_with_bsv_s4_and_presto).

we looked at scaling python batch processing [vertically](/posts/scaling-python-data-processing-vertically) and [horizontally](/posts/scaling-python-data-processing-horizontally). we [refactored](/posts/refactoring-common-distributed-data-patterns-into-s4) the details of distributed compute out of our code. we discovered a [reasonable baseline](/posts/data-processing-performance-with-python-go-rust-and-c) for data processing performance on a single cpu core.

let's build on these experiences and revisit the [nyc taxi](https://registry.opendata.aws/nyc-tlc-trip-records-pds/) dataset. we'll use [presto](https://prestodb.io/) as a performance and correctness baseline to evaluate identical analysis with [bsv](https://github.com/nathants/bsv) on a [s4](https://github.com/nathants/s4) cluster.

we'll be working with the [nyc taxi](https://registry.opendata.aws/nyc-tlc-trip-records-pds/) dataset in the aws region where it lives, us-east-1. bandwidth between ec2 and s3 is only free within the same region, so make sure you are in us-east-1 if you are following along.

we'll be using some [aws tooling](https://github.com/nathants/cli-aws) and the [official aws cli](https://aws.amazon.com/cli/). one could also use other tools without much trouble.

we're going to only use the first 5 columns, since they are consistent across dataset. we'll create two tables so we can transform the data from csv into orc and get decent performance.

```sql
-- schema.hql
CREATE EXTERNAL TABLE IF NOT EXISTS `taxi_csv` (
  `vendor`     string,
  `pickup`     timestamp,
  `dropoff`    timestamp,
  `passengers` integer,
  `distance`   double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/taxi_csv/'
tblproperties("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS `taxi` (
  `vendor`     string,
  `pickup`     timestamp,
  `dropoff`    timestamp,
  `passengers` integer,
  `distance`   double
)
STORED AS ORC
LOCATION '/taxi/';
```

let's spin up an [emr](https://aws.amazon.com/emr/) cluster with [hive](https://hive.apache.org/) and [presto](https://prestodb.io/). we'll size it the same as in [horizontal scaling](/posts/scaling-python-data-processing-horizontally).

if you haven't used [emr](https://aws.amazon.com/emr/) before you may need to create some [default iam roles](https://github.com/nathants/cli-aws/blob/master/aws-iam/aws-iam-ensure-common-roles), then we [spin up](https://github.com/nathants/cli-aws/blob/master/aws-emr/aws-emr-new) the cluster.

```bash
>> export region=us-east-1

>> aws-iam-ensure-common-roles

>> id=$(aws-emr-new --count 12 \
                    --type i3en.2xlarge \
                    --applications hive,presto \
                    test-cluster)

>> time aws-emr-wait-for-state $id --state running

7m37.834s
```

then we fetch the dataset.

```bash
>> time aws-emr-ssh $id --cmd '
       s3-dist-cp --src="s3://nyc-tlc/trip data/" \
                  --srcPattern=".*yellow.*" \
                  --dest=/taxi_csv/
   '

2m52.909s
```

then we create the tables and translate csv to orc.

```bash
>> aws-emr-hive -i $id schema.hql

0m9.091s
```

```sql
-- csv_to_orq.pql
INSERT INTO taxi
SELECT *
FROM taxi_csv;
```

```bash
>> aws-emr-presto -i $id csv_to_orc.pql

2m48.524s
```

now that we have a cluster with data, we can do our analysis. let's ask a few of questions of different types.

grouping and counting.

```sql
-- count_rides_by_passengers.pql
SELECT passengers, count(*) as cnt
FROM taxi
GROUP BY passengers
ORDER BY cnt desc
LIMIT 9;
```

```bash
>> aws-emr-presto -i $id count_rides_by_passengers.pql

          1 | 1135227331
          2 |  239684017
          5 |  103036920
          3 |   70434390
          6 |   38585794
          4 |   34074806
          0 |    6881330
       NULL |     527580
          7 |       2040

0m5.775s
```

more grouping and counting.

```sql
-- count_rides_by_date.pql
SELECT YEAR(pickup), MONTH(pickup), count(*) as cnt
FROM taxi
GROUP BY YEAR(pickup), MONTH(pickup)
ORDER BY cnt desc
LIMIT 9;
```

```bash
>> aws-emr-presto -i $id count_rides_by_date.pql

  2012 |     3 | 16146923
  2011 |     3 | 16066350
  2013 |     3 | 15749228
  2011 |    10 | 15707756
  2009 |    10 | 15604551
  2012 |     5 | 15567525
  2011 |     5 | 15554868
  2010 |     9 | 15540209
  2010 |     5 | 15481351

0m10.556s
```

grouping and accumulating.

```sql
-- sum_distance_by_date.pql
SELECT YEAR(pickup), MONTH(pickup), cast(floor(sum(distance)) as bigint) as dst
FROM taxi
GROUP BY YEAR(pickup), MONTH(pickup)
ORDER BY dst desc
LIMIT 9;
```

```bash
>> aws-emr-presto -i $id sum_distance_by_date.pql

  2013 |     8 | 975457587
  2015 |     4 | 403568758
  2010 |     3 | 372299513
  2015 |    11 | 303443064
  2010 |     2 | 216050426
  2015 |     3 | 210197223
  2015 |     5 | 179394357
  2015 |     1 | 171590254
  2015 |     6 | 145792590

0m9.844s
```

finding large values.

```sql
-- top_n_by_distance.pql
SELECT cast(floor(distance) as bigint)
FROM taxi
ORDER BY distance desc
LIMIT 9;
```

```bash
>> aws-emr-presto -i $id top_n_by_distance.pql

 198623013
  59016609
  19072628
  16201631
  15700000
  15420061
  15420004
  15331800
  15328400

0m5.916s
```

distributed sort.

```sql
-- sort_by_distance.hql
CREATE EXTERNAL TABLE `sorted` (
  `distance` double
)
STORED AS ORC
LOCATION '/sorted/';
```

```sql
-- sort_by_distance.pql
INSERT INTO sorted
SELECT distance
FROM taxi
ORDER BY distance desc;
```

```bash
>> aws-emr-hive   -i $id sort_by_distance.hql

>> aws-emr-presto -i $id sort_by_distance.pql

9m44.334s
```

finally we shutdown the cluster.

```bash
>> aws-emr-rm $id
```

now let's redo the analysis with [bsv](https://github.com/nathants/bsv) and [s4](https://github.com/nathants/s4).

first we need to install [s4](https://github.com/nathants/s4) and [spin up a cluster](https://github.com/nathants/s4/tree/go/scripts/new_cluster.sh). we're going to use an [ami](https://github.com/nathants/bootstraps/blob/master/amis/s4.sh) instead of live bootstrapping to save time.

```bash
>> git clone https://github.com/nathants/s4

>> cd s4

>> python3 -m pip install -r requirements.txt .

>> export region=us-east-1

>> name=s4-cluster

>> time type=i3en.2xlarge ami=s4 num=12 bash scripts/new_cluster.sh $name

3m41.060s
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

healthy:   10.0.3.111:8080
healthy:   10.0.2.192:8080
healthy:   10.0.14.51:8080
healthy:   10.0.9.243:8080
healthy:   10.0.15.97:8080
healthy:   10.0.14.223:8080
healthy:   10.0.15.25:8080
healthy:   10.0.5.197:8080
healthy:   10.0.15.201:8080
healthy:   10.0.7.71:8080
healthy:   10.0.5.249:8080
healthy:   10.0.14.19:8080
```

now we fetch the dataset and convert it to bsv.

```bash
# schema.sh
#!/bin/bash
set -euo pipefail

prefix='s3://nyc-tlc/trip data'

keys=$(aws s3 ls "$prefix/" \
        | grep yellow \
        | awk '{print $NF}' \
        | while read key; do
           echo "$prefix/$key";
          done)

i=0
echo "$keys" | while read key; do
    echo $key
    num=$(printf "%03d" $i)
    yearmonth=$(echo $key | tr -dc 0-9 | tail -c6)
    echo $key | s4 cp - s4://inputs/${num}_${yearmonth}
    i=$((i+1))
done

set -x
time s4 map-to-n s4://inputs/ s4://columns/ '
    cat > url
    aws s3 cp "$(cat url)" - \
     | tail -n+2 \
     | bsv \
     | bschema *,*,*,a:i64,a:f64,... --filter \
     | bunzip $filename
'
```

let's break down what's going on here.

first we find all the s3 keys of the dataset.

```bash
prefix='s3://nyc-tlc/trip data'

keys=$(aws s3 ls "$prefix/" \
        | grep yellow \
        | awk '{print $NF}' \
        | while read key; do
           echo "$prefix/$key";
          done)
```

then we put those keys into s4. since there aren't many keys, we're using numeric prefixes here to ensure the keys are spread evenly across the cluster.

```bash
i=0
echo "$keys" | while read key; do
    echo $key
    num=$(printf "%03d" $i)
    yearmonth=$(echo $key | tr -dc 0-9 | tail -c6)
    echo $key | s4 cp - s4://inputs/${num}_${yearmonth}
    i=$((i+1))
done
```

then we fetch the dataset and convert it to bsv.

```bash
time s4 map-to-n s4://inputs/ s4://columns/ '
    cat > url
    aws s3 cp "$(cat url)" - \
     | tail -n+2 \
     | bsv \
     | bschema *,*,*,a:i64,a:f64,... --filter \
     | bunzip $filename
'
```

let's break that one down a bit more.
- we use [map-to-n](https://github.com/nathants/s4#s4-map-to-n) because our pipeline emits file names instead of data.
- fetch the data.
- skip the csv header.
- [bsv](https://github.com/nathants/bsv#bsv) converts csv to bsv.
- [bschema](https://github.com/nathants/bsv#bschema) filters for rows with at least 5 columns and discards any with less.
- [bschema](https://github.com/nathants/bsv#bschema) keeps the first 5 columns of valid rows.
- [bschema](https://github.com/nathants/bsv#bschema) converts column 4 and 5 from ascii to numerics.
- [bunzip](https://github.com/nathants/bsv#bunzip) splits a single stream of 5 columns into 5 streams of 1 column and emits their file names. the original file name is used as prefix.

let's run it.

```bash
>> bash schema.sh

1m11.860s
```

now that we have a cluster with data, we can do our analysis.

grouping and counting.

```bash
# count_rides_by_passengers.sh

s4 map-to-n s4://columns/*/*_4 s4://tmp/01/ \
            'bcounteach-hash \
             | bpartition 1'

s4 map-from-n s4://tmp/01/ s4://tmp/02/ \
              'xargs cat \
               | bsumeach-hash i64 \
               | bschema i64:a,i64:a \
               | csv'

s4 eval s4://tmp/02/0 \
        'tr , " " \
         | sort -nrk2 \
         | head -n9'
```

let's break that down a bit.
- we [s4 rm](https://github.com/nathants/s4#s4-rm) because we need blank scratch space.
- we [s4 map-to-n](https://github.com/nathants/s4#s4-map-to-n) on a single column, use [bcounteach-hash](https://github.com/nathants/bsv#bcounteach-hash) to count the values, then [bpartition](https://github.com/nathants/bsv#bpartition) by 1 sending all results from around the cluster to a single machine.
- we [s4 map-from-n](https://github.com/nathants/s4#s4-map-from-n) to merge the results. `xargs cat` turns file names into data, [bsumeach-hash](https://github.com/nathants/bsv#bsumeach-hash) merges the counts, then [bschema](https://github.com/nathants/bsv#bschema) converts numerics back to ascii, and [csv](https://github.com/nathants/bsv#csv) converts the result to csv.
- we [s4 eval](https://github.com/nathants/s4#s4-eval) to fetch the result with `tr`, `sort`, and `head` for formatting.

let's run it.

```bash
>> bash count_rides_by_passengers.sh

1 1135227331
2 239684017
5 103036920
3 70434390
6 38585794
4 34074806
0 7408814
7 2040
8 1609

0m2.616s
```

more grouping and counting.

```bash
# count_rides_by_date.sh

s4 map-to-n s4://columns/*/*_2 s4://tmp/01/ \
            'bschema 7* \
             | bcounteach-hash \
             | bpartition 1'

s4 map-from-n s4://tmp/01/ s4://tmp/02/ \
              'xargs cat \
               | bsumeach-hash i64 \
               | bschema *,i64:a \
               | csv'

s4 eval s4://tmp/02/0 \
        'tr , " " \
         | sort -nrk2 \
         | head -n9'
```

let's break that down a bit.
- we [s4 rm](https://github.com/nathants/s4#s4-rm) because we need blank scratch space.
- we [s4 map-to-n](https://github.com/nathants/s4#s4-map-to-n) on a single column, use [bschema](https://github.com/nathants/bsv#bschema) to select the first 7 bytes, use [bcounteach-hash](https://github.com/nathants/bsv#bcounteach-hash) to count the values, then [bpartition](https://github.com/nathants/bsv#bpartition) by 1 sending all results from around the cluster to a single machine.
- we [s4 map-from-n](https://github.com/nathants/s4#s4-map-from-n) to merge the results. `xargs cat` turns file names into data, [bsumeach-hash](https://github.com/nathants/bsv#bsumeach-hash) merges the counts, then [bschema](https://github.com/nathants/bsv#bschema) converts numerics back to ascii, and [csv](https://github.com/nathants/bsv#csv) converts the result to csv.
- we [s4 eval](https://github.com/nathants/s4#s4-eval) to fetch the result with `tr`, `sort`, and `head` for formatting.

let's run it.

```bash
>> bash count_rides_by_date.sh

2012-03 16146923
2011-03 16066350
2013-03 15749228
2011-10 15707756
2009-10 15604551
2012-05 15567525
2011-05 15554868
2010-09 15540209
2010-05 15481351

0m3.399s
```

grouping and accumulating.

```bash
# sum_distance_by_date.sh

s4 map-from-n s4://columns/ s4://tmp/01/ \
              'bzip 2,5 \
               | bschema 7*,8 \
               | bsumeach-hash f64'

s4 map-to-n s4://tmp/01/ s4://tmp/02/ \
            'bpartition 1'

s4 map-from-n s4://tmp/02/ s4://tmp/03/ \
              'xargs cat \
               | bsumeach-hash f64 \
               | bschema 7,f64:a \
               | csv'

s4 eval s4://tmp/03/0 \
        'tr , " " \
         | sort -nrk2 \
         | head -n9'
```

let's break that down a bit.
- we [s4 rm](https://github.com/nathants/s4#s4-rm) because we need blank scratch space.
- we [s4 map-from-n](https://github.com/nathants/s4#s4-map-from-n) to [bzip](https://github.com/nathants/bsv#bzip) together columns 2 and 5, then use [bschema](https://github.com/nathants/bsv#bschema) to select the first 7 bytes of column 1, convert column 2 to numerics, then [bsumeach-hash](https://github.com/nathants/bsv#bsumeach-hash) to sum column 2 by column 1.
- we [s4 map-to-n](https://github.com/nathants/s4#s4-map-to-n) to [bpartition](https://github.com/nathants/bsv#bpartition) by 1 sending all results from around the cluster to a single machine.
- we [s4 map-from-n](https://github.com/nathants/s4#s4-map-from-n) to merge the results. `xargs cat` turns file names into data, [bsumeach-hash](https://github.com/nathants/bsv#bsumeach-hash) merges the sums, then [bschema](https://github.com/nathants/bsv#bschema) converts numerics back to ascii, and [csv](https://github.com/nathants/bsv#csv) converts the result to csv.
- we [s4 eval](https://github.com/nathants/s4#s4-eval) to fetch the result with `tr`, `sort`, and `head` for formatting.

let's run it.

```bash
>> bash sum_distance_by_date.sh

2013-08 975457587.2201815
2015-04 403568758.3299783
2010-03 372299513.2798572
2015-11 303443064.4099629
2010-02 216050426.449974
2015-03 210197223.1599888
2015-05 179394357.3799431
2015-01 171590254.990021
2015-06 145792590.1599617

0m7.130s
```

finding large values.

```bash
# top_n_by_distance.sh

s4 map s4://columns/*/*_5 s4://tmp/01/ \
       'btopn 9 f64'

s4 map-from-n s4://tmp/01/ s4://tmp/02/ \
              'bmerge -r f64'

s4 map-to-n s4://tmp/02/ s4://tmp/03/ \
            'bpartition 1'

s4 map-from-n s4://tmp/03/ s4://tmp/04/ \
              'bmerge -r f64 \
               | bhead 9 \
               | bschema f64:a \
               | csv'

s4 eval s4://tmp/04/0 \
        'cat'
```

let's break that down a bit.
- we [s4 rm](https://github.com/nathants/s4#s4-rm) because we need blank scratch space.
- we [s4 map](https://github.com/nathants/s4#s4-map) to [btopn](https://github.com/nathants/bsv#btopn) over column 5, accumulating the top 9 values.
- we [s4 map-from-n](https://github.com/nathants/s4#s4-map-from-n) to [bmerge](https://github.com/nathants/bsv#bmerge) all results into a single result per machine.
- we [s4 map-to-n](https://github.com/nathants/s4#s4-map-to-n) to [bpartition](https://github.com/nathants/bsv#bpartition) by 1 sending all results from around the cluster to a single machine.
- we [s4 map-from-n](https://github.com/nathants/s4#s4-map-from-n) to merge the results. [bmerge](https://github.com/nathants/bsv#bmerge) combines the results, [bhead](https://github.com/nathants/bsv#bhead) takes the top 9, then [bschema](https://github.com/nathants/bsv#bschema) converts numerics back to ascii, and [csv](https://github.com/nathants/bsv#csv) converts the result to csv.
- we [s4 eval](https://github.com/nathants/s4#s4-eval) to fetch the result with `tr`, `sort`, and `head` for formatting.

let's run it.

```bash
>> bash top_n_by_distance.sh

198623013.6
59016609.3
19072628.8
16201631.4
15700000
15420061
15420004.5
15331800
15328400

0m2.832s
```

distributed sort.

```bash
# sort_by_distance.sh

s4 map s4://columns/*/*_5 s4://tmp/01/ \
      'bsort -r f64'

s4 map-from-n s4://tmp/01/ s4://tmp/02/ \
              'bmerge -r f64'

s4 map-to-n s4://tmp/02/ s4://tmp/03/ \
            'bpartition -l 1'

s4 map-from-n s4://tmp/03/ s4://tmp/04/ \
              'bmerge -lr f64 \
               | blz4'

s4 eval s4://tmp/04/0
        'blz4d \
         | bschema f64:a \
         | csv
         | head -n9'
```

let's break that down a bit.
- we [s4 rm](https://github.com/nathants/s4#s4-rm) because we need blank scratch space.
- we [s4 map](https://github.com/nathants/s4#s4-map) to [bsort](https://github.com/nathants/bsv#bsort) column 5.
- we [s4 map-from-n](https://github.com/nathants/s4#s4-map-from-n) to [bmerge](https://github.com/nathants/bsv#bmerge) all results into a single result per machine.
- we [s4 map-to-n](https://github.com/nathants/s4#s4-map-to-n) to [bpartition](https://github.com/nathants/bsv#bpartition) by 1 sending all results from around the cluster to a single machine.
- we [s4 map-from-n](https://github.com/nathants/s4#s4-map-from-n) to merge the results. [bmerge](https://github.com/nathants/bsv#bmerge) combines the results.
- we [s4 eval](https://github.com/nathants/s4#s4-eval) to fetch the the first few rows with `tr`, `sort`, and `head` for formatting.
- we use lz4 compression at several steps to mitigate iowait.

let's run it.

```bash
>> bash sort_by_distance.sh

2m10.216s
```

we're done for now, so let's delete the cluster.

```bash
>> aws-ec2-rm $name --yes
```

let's put our results in a table.

| query | presto seconds | s4 seconds |
| -- | -- | -- |
| count rides by passengers | [6](https://github.com/nathants/posts/blob/005/005_performant_batch_processing_with_bsv_s4_and_presto/count_rides_by_passengers.pql) | [3](https://github.com/nathants/posts/blob/005/005_performant_batch_processing_with_bsv_s4_and_presto/count_rides_by_passengers.sh) |
| count rides by date | [11](https://github.com/nathants/posts/blob/005/005_performant_batch_processing_with_bsv_s4_and_presto/count_rides_by_date.pql) | [3](https://github.com/nathants/posts/blob/005/005_performant_batch_processing_with_bsv_s4_and_presto/count_rides_by_date.sh) |
| sum distance by date | [10](https://github.com/nathants/posts/blob/005/005_performant_batch_processing_with_bsv_s4_and_presto/sum_distance_by_date.pql) | [7](https://github.com/nathants/posts/blob/005/005_performant_batch_processing_with_bsv_s4_and_presto/sum_distance_by_date.sh) |
| top n by distance | [6](https://github.com/nathants/posts/blob/005/005_performant_batch_processing_with_bsv_s4_and_presto/top_n_by_distance.pql) | [3](https://github.com/nathants/posts/blob/005/005_performant_batch_processing_with_bsv_s4_and_presto/top_n_by_distance.sh) |
| distributed sort | [584](https://github.com/nathants/posts/blob/005/005_performant_batch_processing_with_bsv_s4_and_presto/sort_by_distance.pql) | [130](https://github.com/nathants/posts/blob/005/005_performant_batch_processing_with_bsv_s4_and_presto/sort_by_distance.sh) |

so [s4](https://github.com/nathants/s4) and [bsv](https://github.com/nathants/bsv) exceeds our performance baseline. we could use it for batch processing. should we? it depends.

let's look again at one of the queries.

```sql
-- sort_by_distance.pql
INSERT INTO sorted
SELECT distance
FROM taxi
ORDER BY distance desc;
```

```bash
# sort_by_distance.sh
s4 map        s4://columns/*/*_5 s4://tmp/01/ 'bsort -r f64'
s4 map-from-n s4://tmp/01/       s4://tmp/02/ 'bmerge -r f64'
s4 map-to-n   s4://tmp/02/       s4://tmp/03/ 'bpartition -l 1'
s4 map-from-n s4://tmp/03/       s4://tmp/04/ 'bmerge -lr f64 | blz4'
```

the presto query is high level. it expresses what we want to do, not how to do it.

the s4 query is low level. it expresses how to do it, which if correct, results in what we want.

the presto query will be automatically transformed into executable steps by a query planner.

the s4 query is the executable steps, manually planned.

the presto query is difficult to extend in arbitrary ways.

the s4 query is easy to extend in arbitrary ways. any executable or shell snippet can be inserted into the pipeline of an existing step or as a new step.

the presto query has implicit intermediate results, which are not accessible.

the s4 query has explicit intermediate results, which are accessible.

the presto query has multiple implicit steps which are difficult to analyze and measure independently.

the s4 query has multiple explicit steps which are easy to analyze and measure independently. in fact, we omitted it from the results before, but the s4 query timed each step.

```bash
>> bash sort_by_distance.sh

+ s4 map 's4://columns/*/*_5' s4://tmp/01/ 'bsort -r f64'
ok ok ok ok ok ok ok ok ok ok ok ok
0m21.215s

+ s4 map-from-n s4://tmp/01/ s4://tmp/02/ 'bmerge -r f64'
ok ok ok ok ok ok ok ok ok ok ok ok
0m1.815s

+ s4 map-to-n s4://tmp/02/ s4://tmp/03/ 'bpartition -l 1'
ok ok ok ok ok ok ok ok ok ok ok ok
0m1.432s

+ s4 map-from-n s4://tmp/03/ s4://tmp/04/ 'bmerge -lr f64 | blz4'
ok
1m43.728s

2m10.216s
```

as we might expect, the final merge on a single machine is slow. surprisingly, the merge and shuffle steps were very fast. i wonder how much time shuffle took for presto?

[presto](https://prestodb.io/) is excellent, and significantly faster than the [previous generation](https://hive.apache.org/). it should be used, at a minimum, to check the correctness of your batch processing.

[s4](https://github.com/nathants/s4) and [bsv](https://github.com/nathants/bsv) are primitives for distributed data processing. they are low level, high performance, and flexible. they should be used, at a minimum, to establish a performance baseline.
