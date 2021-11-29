#!/bin/bash

RUST_LOG=error
cargo build --release
rm -rf output
rm *txt
mkdir output

RUST_LOG=warn ../../target/release/hotcrp --prime \
	--nusers_nonpc 20 \
	--nusers_pc 20 \
	--npapers_rej 40 \
	--npapers_acc 5 
#    output/${u}users.out
echo "Ran test for users"
