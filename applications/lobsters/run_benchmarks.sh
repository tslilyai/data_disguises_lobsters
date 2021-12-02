#!/bin/bash

cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

for batch in '--batch' ''; do
	RUST_LOG=error ../../target/release/lobsters \
		--prime \
		--batch \
		--scale 1 \
		&> output/users$batch.out
	echo "Ran test for users"
done
