#!/bin/bash

cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

RUST_LOG=error ../../target/release/lobsters \
	--prime \
	--scale 1 
#	&> output/users.out
echo "Ran test for users"
