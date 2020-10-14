#!/bin/bash

set +x

tests=( "no_shim" "shim_only" "shim_parse" "decor" )

cargo build

for test in "${tests[@]}"
do
    sudo perf record -F 99 -g -a ../target/debug/lobsters --test=$test --nusers=10 --nstories=100 --ncomments=1000 --nthreads=1 --nqueries=3000 #2> /dev/null
    sudo perf script | ./stackcollapse-perf.pl > out.perf-folded
    sudo ./flamegraph.pl out.perf-folded > jerry-$test.svg
done
