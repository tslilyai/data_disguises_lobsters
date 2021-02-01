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
	#perflock ../target/release/lobsters \
	../target/debug/lobsters \
		--scale=0.0005 --nqueries=10 --prop_unsub=1.0 --prime
done

python3 plot.py
