#!/bin/bash

cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

RUST_LOG=error ../../target/release/hotcrp --prime \
	--nusers_nonpc 400 \
	--nusers_pc 50 \
	--npapers_rej 400 \
	--npapers_acc 50 \
	&> output/users.out
echo "Ran test for users"
