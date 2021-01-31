#!/bin/bash

#set -x
set -e

trials=1
#tests=( "no_shim" "decor" "shim_only" "shim_parse" )
#tests=( "decor" )

#cargo build --release
cargo build

for trial in `seq $trials`
do
	perflock ../target/release/lobsters \
		--scale=0.4 --nqueries=10000 --prop_unsub=0.0 #--prime
done

python3 plot.py
