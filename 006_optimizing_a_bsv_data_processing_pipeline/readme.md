## optimizing a bsv data processing pipeline

full source code is available [here](https://github.com/nathants/posts/tree/006/006_optimizing_a_bsv_data_processing_pipeline).

in [performant batch processing](/posts/performant-batch-processing-with-bsv-s4-and-presto) we composed [simple tools](https://github.com/nathants/bsv#tools) into data pipelines. there are many benefits to this. simple tools are easier to write, test, and audit. they can even be shell snippets or existing unix utilities. they can be written in any language and rebuilt as needed. simple tools can compose into arbitrarily complex pipelines, and if something is out of reach you can always add another [simple](https://github.com/nathants/bsv#bquantile-sketch) [tool](https://github.com/nathants/bsv#bquantile-merge). simple tools can even be [performant](/posts/data-processing-performance-with-python-go-rust-and-c).

there is a cost to composing simple tools into data pipelines. primarily this cost is serialization and copies. [efficient data formats](https://github.com/nathants/bsv#layout) and [increased pipe sizes](https://github.com/nathants/bsv#install) mitigate this, but don't eliminate it.

let's install [bsv](https://github.com/nathants/bsv#install) then measure the cost.

```bash
>> _gen_bsv 8 12000000 > /tmp/data.bsv

>> ls -lh /tmp/data.bsv | awk '{print $5}'
1.1G

>> time cat /tmp/data.bsv >/dev/null
0m0.124s

>> time cat /tmp/data.bsv | cat >/dev/null
0m0.302s

>> time cat /tmp/data.bsv | cat | cat >/dev/null
0m0.439s

>> time cat /tmp/data.bsv | cat | cat | cat >/dev/null
0m0.537s

>> time bcopy </tmp/data.bsv >/dev/null
0m0.890s

>> time bcopy </tmp/data.bsv | bcopy >/dev/null
0m1.137s

>> time bcopy </tmp/data.bsv | bcopy | bcopy >/dev/null
0m1.228s

>> time bcopy </tmp/data.bsv | bcopy | bcopy | bcopy >/dev/null
0m1.432s
```

so even when we just copy bytes with cat, we can see that as the pipeline grows, time goes up. the effect is even greater when parsing and serialization is performed at each step with [bcopy](https://github.com/nathants/bsv/blob/master/src/bcopy.c).

when we are doing [distributed compute](/posts/refactoring-common-distributed-data-patterns-into-s4) there will be serialization. it's required before data can go over the network. for convenience, we use it between every process in the pipelines we compose to simplify their interface. the benefit is convenience, the cost is performance. this convenience helps us to quickly prototype pipelines and integrate new tools. once our pipelines have stabilized, we can optimize it out.

first we need to install [s4](https://github.com/nathants/s4) and [spin up a cluster](https://github.com/nathants/s4/tree/go/scripts/new_cluster.sh). we're going to use an [ami](https://github.com/nathants/bootstraps/blob/master/amis/s4.sh) instead of live bootstrapping to save time.

```bash
>> git clone https://github.com/nathants/s4

>> cd s4

>> python3 -m pip install -r requirements.txt .

>> export region=us-east-1

>> name=s4-cluster

>> type=i3en.2xlarge ami=s4 num=12 bash scripts/new_cluster.sh $name
```

next we'll [proxy traffic](https://github.com/nathants/s4/tree/go/scripts/connect_to_cluster.sh) through a machine in the cluster. assuming the security group only allows port 22, the machines are only accessible on their internal addresses. since we already have ssh setup, we'll use [sshuttle](https://github.com/sshuttle/sshuttle). run this in a second terminal, and don't forget to set region to us-east-1.

```bash
>> export region=us-east-1

>> name=s4-cluster

>> bash scripts/connect_to_cluster.sh $name
```

let's take a look at one of the queries from [performant batch processing](https://nathants.com/posts/performant-batch-processing-with-bsv-s4-and-presto).

```bash
# sum_distance_by_date.sh
s4 map-from-n s4://columns/ s4://tmp/01/   'bzip 2,5 | bschema 7*,8 | bsumeach-hash f64'
s4 map-to-n   s4://tmp/01/  s4://tmp/02/   'bpartition 1'
s4 map-from-n s4://tmp/02/  s4://tmp/03/   'xargs cat | bsumeach-hash f64 | bschema 7,f64:a | csv'
s4 eval       s4://tmp/03/0                'tr , " " | sort -nrk2 | head -n9'
```

let's run it and see how long each step takes.

```bash
>> bash schema.sh

1m9.272s

>> bash sum_distance_by_date.sh

+ s4 map-from-n s4://columns/ s4://tmp/01/ 'bzip 2,5 | bschema 7*,8 | bsumeach-hash f64'
0m5.718s

+ s4 map-to-n s4://tmp/01/ s4://tmp/02/ 'bpartition 1'
0m1.427s

+ s4 map-from-n s4://tmp/02/ s4://tmp/03/ 'xargs cat | bsumeach-hash f64 | bschema 7,f64:a | csv'
0m0.349s

+ s4 eval s4://tmp/03/0 'tr , " " | sort -nrk2 | head -n9'
0m0.161s

0m7.655s
```

the majority of runtime is in the first step. let's try to replace that pipeline with a single executable. we'll base it off [bzip.c](https://github.com/nathants/bsv/blob/master/src/bzip.c), and then insert functionality from [bschema.c](https://github.com/nathants/bsv/blob/master/src/bschema.c) and [bsumeach_hash.c](https://github.com/nathants/bsv/blob/master/src/bsumeach_hash.c). let's look at the diff of our new [step1.c](https://github.com/nathants/posts/blob/006/006_optimizing_a_bsv_data_processing_pipeline/step1.c) against the original [bzip.c](https://github.com/nathants/bsv/blob/master/src/bzip.c).

```c
diff --git a/~/repos/bsv/src/bzip.c b/step1.c
index d393f10..4e12b7a 100644
--- a/~/repos/bsv/src/bzip.c
+++ b/step1.c
@@ -3,6 +3,7 @@
 #include "load.h"
 #include "array.h"
 #include "dump.h"
+#include "hashmap.h"

 #define DESCRIPTION "combine single column inputs into a multi column output\n\n"
 #define USAGE "ls column_* | bzip [COL1,...COLN] [-l|--lz4]\n\n"
@@ -86,6 +87,13 @@ int main(int argc, char **argv) {
     // setup output
     writebuf_t wbuf = wbuf_init((FILE*[]){stdout}, 1, false);

+    // bsumeach-hash state
+    u8 *key;
+    void* element;
+    struct hashmap_s hashmap;
+    f64 *sum_f64;
+    ASSERT(0 == hashmap_create(2, &hashmap), "fatal: hashmap init\n");
+
     // process input row by row
     while (1) {
         for (i32 i = 0; i < ARRAY_SIZE(selected); i++) {
@@ -99,8 +107,36 @@ int main(int argc, char **argv) {
             ASSERT(memcmp(stops, do_stop, ARRAY_SIZE(selected) * sizeof(i32)) == 0, "fatal: all columns didn't end at the same length\n");
             break;
         }
-        dump(&wbuf, &new, 0);
+
+        // bschema 7*,*
+        new.sizes[0] = 7;
+
+        // bsumeach-hash f64
+        ASSERT(new.max >= 1, "fatal: need at least 2 columns\n");
+        ASSERT(8 == new.sizes[1], "fatal: bad data size\n");
+        if (element = hashmap_get(&hashmap, new.columns[0], new.sizes[0])) {
+            *(f64*)element += *(f64*)new.columns[1];
+        } else {
+            MALLOC(key, new.sizes[0]);
+            strncpy(key, new.columns[0], new.sizes[0]);
+            MALLOC(sum_f64, sizeof(f64)); *sum_f64 = *(f64*)new.columns[1];
+            ASSERT(0 == hashmap_put(&hashmap, key, new.sizes[0], sum_f64), "fatal: hashmap put\n");
+        }
+
     }
+
+    // bsumeach-hash f64 dump results
+    for (i32 i = 0; i < hashmap.table_size; i++) {
+        if (hashmap.data[i].in_use) {
+            row.max = 1;
+            row.columns[0] = hashmap.data[i].key;
+            row.sizes[0] = hashmap.data[i].key_len;
+            row.columns[1] = hashmap.data[i].data;
+            row.sizes[1] = sizeof(f64);
+            dump(&wbuf, &row, 0);
+        }
+    }
+
     dump_flush(&wbuf, 0);

 }
```

let's ship it to the cluster and compile it.

```bash
>> aws-ec2-scp step1.c : s4-cluster --yes

>> aws-ec2-ssh s4-cluster -yc "sudo gcc -Ibsv/util -Ibsv/vendor -flto -O3 -march=native -mtune=native -lm -o /usr/local/bin/step1 step1.c bsv/vendor/lz4.c"
```

our optimized query looks like this.

```bash
# sum_distance_by_date.sh
s4 map-from-n s4://columns/ s4://tmp/01/   'step1 2,5'
s4 map-to-n   s4://tmp/01/  s4://tmp/02/   'bpartition 1'
s4 map-from-n s4://tmp/02/  s4://tmp/03/   'xargs cat | bsumeach-hash f64 | bschema 7,f64:a | csv'
s4 eval       s4://tmp/03/0                'tr , " " | sort -nrk2 | head -n9'
```

let's run it.

```bash
>> bash sum_distance_by_date_optimized.sh

+ s4 map-from-n s4://columns/ s4://tmp/01/ 'step1 2,5'
0m2.034s

+ s4 map-to-n s4://tmp/01/ s4://tmp/02/ 'bpartition 1'
0m1.334s

+ s4 map-from-n s4://tmp/02/ s4://tmp/03/ 'xargs cat | bsumeach-hash f64 | bschema 7,f64:a | csv'
0m0.336s

+ s4 eval s4://tmp/03/0 'tr , " " | sort -nrk2 | head -n9'
real    0m0.161s

0m3.866s
```

looks like step1 went from 6 to 2 seconds and the whole query went from 8 to 4 seconds.

we're done for now, so let's delete the cluster.

```bash
>> aws-ec2-rm $name --yes
```

composing data pipelines from simple tools is an effective way to rapidly prototype.

reusing the same serialization between local and distributed processes we can build and use tools that don't care whether data is coming from or going to a file, a pipe, or a socket.

once our prototypes have stabilized, we can optimize them by collapsing pipelines into a single executable.
