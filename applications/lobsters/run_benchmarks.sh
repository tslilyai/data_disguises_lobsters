#!/bin/bash

cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

#get stats first, prime when you do this
RUST_LOG=error perflock ../../target/release/lobsters \
	--prime $batch \
	--stats \
	--scale 1.5 \
	&> output/users$batch.out
echo "Ran stats test for users"

#for ndisguising in 0 1 5 10 15 20 25 30 50 100; do
#for ndisguising in 100 50 1 0; do
#        RUST_LOG=error perflock ../../target/release/lobsters \
#            --scale 1.5 \
#            --ndisguising $ndisguising \
#            &> output/users$batch_$ndisguising.out
#        echo "Ran concurrent test for users $ndisguising disguising"
#    done
#done
