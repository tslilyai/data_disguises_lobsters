#!/bin/bash

cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

#get stats first, prime when you do this
# TODO get baselines
#RUST_LOG=error perflock ../../target/release/lobsters \
#	--prime \
#	--stats \
#	--scale 0.1 \
#	&> output/users.out
#echo "Ran stats primed test for users"

for u in 0 1 30; do
	RUST_LOG=error perflock ../../target/release/lobsters \
		--scale 1.5 \
		--nsleep 0\
		--nconcurrent $u \
		--filename "${u}users_expensive" \
	&> output/users$s-$u.out
	echo "Ran concurrent test for $u users 0 sleep"
done
