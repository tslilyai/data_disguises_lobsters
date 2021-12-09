#!/bin/bash

RUST_LOG=error
cargo build --release
rm -rf output
rm *txt
mkdir output

for baseline in false true ; do
	for l in 20 ; do
	    for u in 100 ; do
		RUST_LOG=error perflock ../../../target/release/websubmit-server \
			-i myclass --schema src/schema.sql --config sample-config.toml \
			--benchmark true --prime true --baseline $baseline \
			--nusers $u --nlec $l --nqs 4 &> \
		    output/${l}lec_${u}users_$baseline.out
		echo "Ran test for $l lecture and $u users"
	    done
    done
done
