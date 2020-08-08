## discovering a baseline for data processing performance

performance is important, and yet our intuition about it is often wrong.

let's try to discover a reasonable baseline for data processing performance and build intuition that can guide our decisions. we'll do this by experimenting with simple transformations of generated data using various formats, techniques, and languages on a single cpu core.

whether we are configuring and using off the shelf software or building bespoke systems, we need the ability to intuit problems and detect low hanging fruit.

we'll say that our data is a sequence of rows, that a row is made of 8 columns, and that a column is a random dictionary word.

we'll generate our dataset as csv with the following [script]().

```python
# gen_csv.py
import sys
import random

words = [
    ...
]

num_rows = int(sys.argv[1])

for _ in range(num_rows):
    row = [random.choice(words) for _ in range(8)]
    print(','.join(row))
```

our first transformation will be selecting a subset of columns.

let's try python.

```python
# select.py
import sys

for line in sys.stdin:
    columns = line.split(',')
    print(f'{columns[2]},{columns[6]}')
```

first we need some data.

```bash
>> pypy3 gen_csv.py 1000000 > /tmp/data.csv

>> ls -lh /tmp/data.csv | awk '{print $5}'

72M
```

we're gonna need more data.

```bash
>> pypy3 gen_csv.py 15000000 > /tmp/data.csv

>> ls -lh /tmp/data.csv | awk '{print $5}'

1.1G
```

that'll do. now let's try our selection. we'll make sure a subset of the result is sane, then we'll check the hash of the entire result using [xxhsum](https://www.archlinux.org/packages/community/x86_64/xxhash/). all other runs we'll discard the output and time execution.

```bash
>> python select.py </tmp/data.csv | head -n3

epigram,Madeleine
strategies,briefed
Doritos,putsch

>> python select.py </tmp/data.csv | xxhsum

12927f314ca6e9eb
```

seems sane. let's time it.

```bash
>> time python select.py </tmp/data.csv >/dev/null

real    0m10.076s
user    0m9.779s
sys     0m0.200s
```

let's try coreutils [cut](https://github.com/coreutils/coreutils/blob/master/src/cut.c).

```bash
>> cut -d, -f3,7 </tmp/data.csv | xxhsum
12927f314ca6e9eb

>> time cut -d, -f3,7 </tmp/data.csv >/dev/null

real    0m3.534s
user    0m3.341s
sys     0m0.159s
```

decently faster. we may need to look at compiled languages for a reasonable baseline.

let's optimize by avoiding allocations and doing as little work as possible. we'll pull rows off a buffered reader, setup columns as offsets into that buffer, and access columns by slicing the row data.

let's try [go]().

```bash
>> go build -o select_go select.go

>> ./select_go </tmp/data.csv | xxhsum

12927f314ca6e9eb

>> time ./select_go </tmp/data.csv >/dev/null

real    0m2.832s
user    0m2.559s
sys     0m0.312s
```

faster than cut. this is progress.

let's try [rust]().

```bash
>> rustc -O -o select_rust select.rs

>> ./select_rust </tmp/data.csv | xxhsum

12927f314ca6e9eb

>> time ./select_rust </tmp/data.csv >/dev/null

real    0m2.602s
user    0m2.491s
sys     0m0.110s
```

pretty much the same. let's try [c](). we'll grab a few header only dependencies from [bsv](https://github.com/nathants/bsv) for [csv parsing](https://github.com/nathants/bsv/blob/master/util/csv.h) and [buffered writing](https://github.com/nathants/bsv/blob/master/util/write_simple.h).

```bash
>> gcc -Iutils -O3 -flto -march=native -mtune=native -o select_c select.c

>> ./select_c </tmp/data.csv | xxhsum

12927f314ca6e9eb

>> time ./select_c </tmp/data.csv >/dev/null

real    0m2.716s
user    0m2.569s
sys     0m0.120s
```

so [rust](), [go](), and [c]() are all basically the same. we may have established a baseline when working with csv.

let's try a similar optimization with [pypy]().

```bash
>> pypy select_inlined.py </tmp/data.csv | xxhsum

12927f314ca6e9eb

>> time pypy select_inlined.py </tmp/data.csv >/dev/null

real    0m4.491s
user    0m4.293s
sys     0m0.170s
```

not bad.

let's try using [protobuf]() and [go]().

```bash
>> (cd psv && protoc -I=row --go_out=row row/row.proto)

>> (cd psv && go build -o psv psv.go)

>> (cd psv && go build -o select select.go)

>> ./psv/psv </tmp/data.csv >/tmp/data.psv

>> ./psv/select </tmp/data.psv | xxhsum

12927f314ca6e9eb  stdin

>> time ./psv/select </tmp/data.psv >/dev/null

real    0m10.424s
user    0m10.465s
sys     0m0.251s
```

interesting. slower than naive python and csv.

is reading and writing data to some format a majority of the work?

let's think about our optimized code from before. our representation of a row is 3 pieces of data. a byte array of content, an array of column start positions, and an array of column sizes. writing a row as csv was easy, but [reading]() was hard.

what if we made it easier? all we want is an array of bytes and two int arrays.

let's let a row written as bytes be:
- the max zero based index
- the column sizes
- the column data

```bash
| u16:max | u16:size | ... | u8[]:column | ... |
```

this should be easy to write, and more importantly easy to read. we can read max, which tells us how many sizes to read. from the sizes we can reconstruct the offsets and the size of the row data. we can then read the row data, and access the columns by offset and size.

our optimized code also buffered reads and writes into large chunks.

let's let a chunk of rows written as bytes be:
- size
- data

```bash
| i32:size | u8[]:row | ... |
```

let's constrain a chunk to only contain complete rows and be smaller than some maximum size.

we'll call this format [bsv](). we'll implement buffered [reading]() and [writing]() of chunks, as well as [loading]() and [dumping]() of [rows]().

let's implement our transformation using bsv in [c]().

```bash
>> gcc -Iutils -O3 -flto -march=native -mtune=native -o bsv/bsv bsv/bsv.c

>> time ./bsv/bsv </tmp/data.csv >/tmp/data.bsv

real    0m3.212s
user    0m2.760s
sys     0m0.450s

>> gcc -Iutils -O3 -flto -march=native -mtune=native -o bsv/select bsv/select.c

>> ./bsv/select </tmp/data.bsv | head -n3

epigram,Madeleine
strategies,briefed
Doritos,putsch

>> ./bsv/select </tmp/data.bsv | xxhsum

12927f314ca6e9eb

>> time ./bsv/select </tmp/data.bsv >/dev/null

real    0m0.479s
user    0m0.339s
sys     0m0.140s
```

we've processed the same data, and system time has been fairly consistent, but user time has varied significantly.

let's try a second transformation where we reverse the columns of every row.

we'll implement it with csv in [python](), [pypy]() and [c](), then with bsv in [c]().

```bash
>> python reverse.py </tmp/data.csv | xxhsum

e221974c95d356f9

>> time python reverse.py </tmp/data.csv >/dev/null

real    0m13.915s
user    0m13.743s
sys     0m0.170s

>> pypy3 reverse_inlined.py </tmp/data.csv | xxhsum

e221974c95d356f9

>> time pypy3 reverse_inlined.py </tmp/data.csv >/dev/null

real    0m6.141s
user    0m5.880s
sys     0m0.220s

>> gcc -Iutils -O3 -flto -march=native -mtune=native -o reverse reverse.c

>> ./reverse </tmp/data.csv | xxhsum

e221974c95d356f9

>> time ./reverse </tmp/data.csv >/dev/null

real    0m2.890s
user    0m2.719s
sys     0m0.170s

>> gcc -Iutils -O3 -flto -march=native -mtune=native -o bsv/reverse bsv/reverse.c

>> ./bsv/reverse </tmp/data.bsv | xxhsum
e221974c95d356f9

>> time ./bsv/reverse </tmp/data.bsv >/dev/null

real    0m1.052s
user    0m0.891s
sys     0m0.161s
```

let's try a third transformation where we count every column where the first character of the first column is "f".

we'll implement it with csv in [python](), [pypy]() and [c](), then with bsv in [c]().

```bash
>> time python count.py </tmp/data.csv
467002

real    0m6.385s
user    0m6.223s
sys     0m0.160s

>> time pypy3 count_inlined.py </tmp/data.csv

467002

real    0m3.147s
user    0m2.938s
sys     0m0.180s

>> gcc -Iutils -O3 -flto -march=native -mtune=native -o count count.c

>> time ./count </tmp/data.csv

467002

real    0m2.367s
user    0m2.245s
sys     0m0.121s

>> gcc -Iutils -O3 -flto -march=native -mtune=native -o bsv/count bsv/count.c

>> time bsv/count </tmp/data.bsv

467002

real    0m0.260s
user    0m0.135s
sys     0m0.125s
```

in transformations 2 and 3 we again see system time constant with variance in user time.

let's put our user time results in a table.

first we have our select transformation, which outputs 25% of its input.

| format | language | user seconds | gigabytes / second |
| -- | -- | -- | -- |
| psv | [go]() | 10.4 | 0.1 |
| csv | [python]() | 9.7 | 0.1 |
| csv | [pypy]() | 4.3 | 0.2 |
| csv | [go]() | 2.6 | 0.4 |
| csv | [c]() | 2.6 | 0.4 |
| csv | [rust]() | 2.5 | 0.4 |
| bsv | [c]() | 0.3 | 3.3 |

second we have our reverse transformation, which outputs 100% of its input.

| format | language | user seconds | gigabytes / second |
| -- | -- | -- | -- |
| csv | [python]() | 13.7 | 0.1 |
| csv | [pypy]() | 5.8 | 0.2 |
| csv | [c]() | 2.7 | 0.4 |
| bsv | [c]() | 0.9 | 1.1 |

third we have our count transformation, which outputs <0.001% of its input.

| format | language | user seconds | gigabytes / second |
| -- | -- | -- | -- |
| csv | [python]() | 6.2 | 0.2 |
| csv | [pypy]() | 2.9 | 0.3 |
| csv | [c]() | 2.2 | 0.5 |
| bsv | [c]() | 0.1 | 10 |

we have some interesting results here. let's take a closer look at the csv and bsv results for c based on the ratio of inputs to outputs.

| inputs / outputs | format | language | user seconds | gigabytes / second |
| -- | -- | -- | -- | -- |
| 1 / 1 | csv | [c]() | 2.7 | 0.4 |
| 4 / 1 | csv | [c]() | 2.6 | 0.4 |
| 1000 / 1 | csv | [c]() | 2.2 | 0.5 |

| inputs / outputs | format | language | user seconds | gigabytes / second |
| -- | -- | -- | -- | -- |
| 1 / 1 | bsv | [c]() | 0.9 | 1.1 |
| 4 / 1 | bsv | [c]() | 0.3 | 3.3 |
| 1000 / 1 | bsv | [c]() | 0.1 | 10 |

now this is interesting. when dealing with csv, the ratio of inputs to outputs has almost no impact on performance. when dealing with bsv the impact is x3 at each step. this suggests that for csv, parsing the input dominates, while for bsv, writing the output dominates. this asks an interesting question, how can we optimize output? for simplicity, the bsv code is outputting csv. it may be worth experimenting with other output formats, but we'll skip that for now.

do we have enough information to establish a baseline? perhaps.

we've seen [python]() process csv and [go]() process protobuf at 100 megabytes / second.

we've seen [c](), [go](), and [rust]() process csv at 400 megabytes / second.

we've seen [c]() process bsv at 1-10 gigabytes / second.

why don't we start with the following baseline. we'll think of it as napkin math.

| category | rate |
| -- | -- |
| slow | &nbsp; <100 megabytes / second / cpu core |
| decent | &nbsp; ~500 megabytes / second / cpu core |
| fast | >1000 megabytes / second / cpu core |

as we do data processing, either by configuring and using off the shelf software, or by building bespoke systems, we can keep these rates in mind.

if you are interested in bsv you can find it [here]().

for futher experimentation like what we've done today, look [here]().

for examples of applying bsv to distributed compute, look [here]().
