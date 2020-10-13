#!/bin/sh

name=$1
testname=$2

sudo perf record -F 99 -g ../target/release/lobsters --test=$testname --ncomments=1000 --nqueries=1000
sudo perf script | ./stackcollapse-perf.pl > out.perf-folded
sudo ./flamegraph.pl out.perf-folded > $name.svg

