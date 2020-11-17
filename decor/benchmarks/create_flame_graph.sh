#!/bin/bash

set +x

#tests=( "no_shim" "shim_only" "shim_parse" "decor" )
tests=( "decor" ) #"shim_only" "shim_parse" "decor" )

#cargo clean
cargo build --release

for test in "${tests[@]}"
do
	echo $test
	cargo flamegraph -o flamegraphs/$test.svg --bin lobsters-microbenchmarks -- --test=$test --nusers=10 --nstories=100 --ncomments=1000 --nthreads=1 --nqueries=1000
done
