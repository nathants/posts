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
