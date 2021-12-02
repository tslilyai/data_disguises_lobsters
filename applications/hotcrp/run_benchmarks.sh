#!/bin/bash

cargo build --release
rm -rf output
rm *txt
mkdir output
set -e

for batch in '--batch' ''; do
	for baseline in '--baseline' ''; do
		RUST_LOG=error ../../target/release/hotcrp --prime $batch \
			--nusers_nonpc 400 \
			--nusers_pc 50 \
			--npapers_rej 400 \
			--npapers_acc 50 \
			$baseline \
			&> output/users_$baseline.out
		echo "Ran $baseline test for users"
	done
done
