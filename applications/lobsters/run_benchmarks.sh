#!/bin/bash

cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

#get stats first, prime when you do this
# TODO get baselines
#RUST_LOG=error perflock ../../target/release/lobsters \
#	--prime $batch \
#	--stats \
#	--scale 1.5 \
#	&> output/users$batch.out
#echo "Ran stats test for users"

for s in 10000 5000 1000 100 0; do
	RUST_LOG=error perflock ../../target/release/lobsters \
	    --scale 1.5 \
	    --nsleep $s\
    	&> output/users$nsleep.out
	echo "Ran concurrent test for users $nsleep sleep"
done
