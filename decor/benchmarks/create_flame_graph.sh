#!/bin/bash

set +x

#tests=( "no_shim" "shim_only" "shim_parse" "decor" )
tests=( "decor" ) #"shim_only" "shim_parse" "decor" )

#cargo clean
cargo build --release

for test in "${tests[@]}"
do
	echo $test
	cargo flamegraph -o flamegraphs/$test.svg --bin lobsters-microbenchmarks -- --test=$test --nusers=100 --nstories=500 --ncomments=1000 --nthreads=1 --nqueries=500
done
