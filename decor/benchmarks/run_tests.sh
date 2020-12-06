#!/bin/bash

#set -x

trials=1
#tests=( "no_shim" "decor" "shim_only" "shim_parse" )
#tests=( "decor" )

cargo build --release
cargo build 

for trial in `seq $trials`
do
	perflock ../target/debug/lobsters-microbenchmarks \
		--scale=0.001 --nqueries=10000 --prime
done

python3 plot.py
