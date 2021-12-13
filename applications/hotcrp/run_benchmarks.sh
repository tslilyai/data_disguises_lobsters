#!/bin/bash

cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

for baseline in '--baseline' ''; do
	RUST_LOG=warn ../../target/release/hotcrp --prime \
		--nusers_nonpc 30\
		--nusers_pc 20\
		--npapers_rej 50 \
		--npapers_acc 10 \
		$baseline \
		&> output/users_$baseline.out
	echo "Ran $baseline test for users"
done
