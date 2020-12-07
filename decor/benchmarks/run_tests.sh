#!/bin/bash

#set -x

trials=1
#tests=( "no_shim" "decor" "shim_only" "shim_parse" )
#tests=( "decor" )

cargo build --release

for trial in `seq $trials`
do
	perflock ../target/release/lobsters-microbenchmarks \
		--scale=0.5 --nqueries=10000 --prime
done

python3 plot.py
