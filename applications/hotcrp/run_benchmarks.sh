#!/bin/bash

cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

for baseline in '' '--baseline'; do
	RUST_LOG=error ../../target/release/hotcrp --prime \
		--nusers_nonpc 3000\
		--nusers_pc 80\
		--npapers_rej 500 \
		--npapers_acc 50 \
		$baseline \
		&> output/users_$baseline.out
	echo "Ran $baseline test for users"
done
