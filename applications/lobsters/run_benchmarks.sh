#!/bin/bash

cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

#get stats first, prime when you do this
# get baselines
RUST_LOG=error perflock ../../target/release/lobsters \
	--stats \
	--scale 1.5 \
	&> output/users.out
echo "Ran stats test for users"

RUST_LOG=error perflock ../../target/release/lobsters \
	--prime \
	--stats \
	--scale 1.5 \
	&> output/users.out
echo "Ran stats primed test for users"

for s in 10000 5000 1000 100 0; do
	RUST_LOG=error perflock ../../target/release/lobsters \
	    --scale 1.5 \
	    --nsleep $s\
    	&> output/users$s.out
	echo "Ran concurrent test for users $s sleep"
done
