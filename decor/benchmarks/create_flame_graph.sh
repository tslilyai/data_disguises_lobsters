#!/bin/bash

set +x

#tests=( "no_shim" "shim_only" "shim_parse" "decor" )
tests=( "decor" ) #"shim_only" "shim_parse" "decor" )
name=$1

#cargo clean
cargo build --release

for test in "${tests[@]}"
do
	echo $test
	cargo flamegraph -o flamegraphs/$name.svg --bin lobsters-microbenchmarks -- --test=$test --nusers=10 --nstories=500 --ncomments=1000 --nthreads=1 --nqueries=2000
done
