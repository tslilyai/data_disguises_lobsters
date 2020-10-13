#!/bin/sh

name=$1

sudo perf record -F 99 -g ../target/release/lobsters --ncomments=1000 --nqueries=1000
sudo perf script | ./stackcollapse-perf.pl > out.perf-folded
sudo ./flamegraph.pl out.perf-folded > $name.svg

