#!/bin/bash

RUST_LOG=error
cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

RUST_LOG=warn ../../target/release/hotcrp --prime \
	--nusers_nonpc 2 \
	--nusers_pc 10 \
	--npapers_rej 4 \
	--npapers_acc 2 
#    output/${u}users.out
echo "Ran test for users"
