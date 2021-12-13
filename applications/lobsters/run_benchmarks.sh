#!/bin/bash

cargo build --release
rm *txt
set -e

#RUST_LOG=error ../../target/release/lobsters \
#	--storage \
#	--scale 3 \
#	&> output/users.out
#echo "Ran storage test for users"

# TODO get baselines
#RUST_LOG=error ../../target/release/lobsters \
#	--stats \
#	--scale 3 \
#	&> output/users.out
#echo "Ran stats primed test for users"

for d in 'cheap' 'expensive'; do
	for u in 1 10; do
		RUST_LOG=error perflock ../../target/release/lobsters \
			--scale 3 \
			--nsleep 0\
			--nconcurrent $u \
			--disguiser $d \
			--filename "${u}users_${d}" \
		&> output/users-$u-${d}.out
		echo "Ran concurrent test for $u users 0 sleep ${d}"
	done
done
