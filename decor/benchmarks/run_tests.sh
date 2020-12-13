#!/bin/bash

#set -x
set -e

trials=1
#tests=( "no_shim" "decor" "shim_only" "shim_parse" )
#tests=( "decor" )

cargo build --release 

for trial in `seq $trials`
do
	perflock ../target/release/lobsters \
		--scale=0.1 --nqueries=10000 --prime --prop_unsub=0.0
done

python3 plot.py
