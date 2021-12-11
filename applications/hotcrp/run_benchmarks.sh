#!/bin/bash

cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

for baseline in '--baseline' ''; do
	RUST_LOG=warn ../../target/release/hotcrp --prime \
		--nusers_nonpc 400 \
		--nusers_pc 50 \
		--npapers_rej 400 \
		--npapers_acc 50 \
		$baseline \
		&> output/users_$baseline.out
	echo "Ran $baseline test for users"
done
