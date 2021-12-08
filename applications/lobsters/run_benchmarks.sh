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

for u in 1 ; do
	RUST_LOG=error perflock ../../target/release/lobsters \
		--scale 1.5 \
		--nsleep 0\
		--nconcurrent $u \
		--filename "${u}users_nodisguising" \
	&> output/users$s-$u-nodisguising.out
	echo "Ran concurrent test for $u users 0 sleep"
done
