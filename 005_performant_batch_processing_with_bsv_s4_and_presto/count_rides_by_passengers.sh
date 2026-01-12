#!/bin/bash
set -euo pipefail

s4 rm -r s4://tmp/

time (
    set -x
    time   s4   map-to-n     s4://columns/*/*_4   s4://tmp/01/   'bcounteach-hash | bpartition 1'
    time   s4   map-from-n   s4://tmp/01/         s4://tmp/02/   'xargs cat | bsumeach-hash i64 | bschema i64:a,i64:a | csv'
    time   s4   eval         s4://tmp/02/0                       'tr , " " | sort -nrk2 | head -n9'
)
