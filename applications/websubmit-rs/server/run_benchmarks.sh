#!/bin/bash

cargo build --release
rm *txt

set -e

for baseline in false true ; do
	for l in 20; do
	    for u in 100; do
		RUST_LOG=error perflock ../../../target/release/websubmit-server \
			-i myclass --schema src/schema.sql --config sample-config.toml \
			--benchmark true --prime true --baseline $baseline \
			--nusers $u --nlec $l --nqs 1 &> \
		    output/${l}lec_${u}users_isbaseline_$baseline.out
		echo "Ran test for $l lecture and $u users"
	    done
    done
done
