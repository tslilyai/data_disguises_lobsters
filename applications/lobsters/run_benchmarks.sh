#!/bin/bash

cargo build --release
rm *txt
set -e

#get stats first, prime when you do this
# TODO get baselines
#RUST_LOG=error perflock ../../target/release/lobsters \
#	--prime \
#	--stats \
#	--scale 1.5 \
#	&> output/users.out
#echo "Ran stats primed test for users"

for d in cheap expensive none ; do
	for u in 1 30 ; do
		RUST_BACKTRACE=1 RUST_LOG=error perflock ../../target/release/lobsters \
			--scale 1.5 \
			--nsleep 0\
			--nconcurrent $u \
			--disguiser $d \
			--filename "${u}users_${d}" \
		&> output/users-$u-${d}.out
		echo "Ran concurrent test for $u users 0 sleep ${d}"
	done
done
