#!/bin/bash

cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

# get stats first, prime when you do this
for batch in '--batch' ''; do
    RUST_LOG=error ../../target/release/lobsters \
		--prime $batch \
        --stats \
		--scale 0.01 \
		&> output/users$batch.out
	echo "Ran stats test for users"
done

for ndisguising in 0 1; do #5 10 15 20 25 30; do
    for batch in '--batch' ''; do
        RUST_LOG=error ../../target/release/lobsters \
            $batch \
            --scale 0.01
            #&> output/users$batch_$ndisguising.out
        echo "Ran concurrent test for users $ndisguising disguising"
    done
done
