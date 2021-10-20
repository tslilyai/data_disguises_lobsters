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
	cargo flamegraph -o flamegraphs/$name.svg --bin hotcrp -- --prime
done
