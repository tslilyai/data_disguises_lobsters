#!/bin/bash

RUST_LOG=error
cargo build --release --features flame_it
rm -rf output
mkdir output

for l in 20 40; do
    for u in 10 20 30 50 70 100; do
	RUST_LOG=error perflock ../../../target/release/websubmit-server \
        	-i myclass --schema src/schema.sql --config sample-config.toml \
		--benchmark true --prime true --baseline true \
		--nusers $u --nlec $l --nqs 4 &> \
            output/${l}lec_${u}users_baseline.out
	echo "Ran baseline test for $l lecture and $u users"

	RUST_LOG=error perflock ../../../target/release/websubmit-server \
        	-i myclass --schema src/schema.sql --config sample-config.toml \
		--benchmark true --prime true --baseline false \
		--nusers $u --nlec $l --nqs 4 &> \
            output/${l}lec_${u}users.out
	echo "Ran test for $l lecture and $u users"
    done
    python3 plot_server.py $l
done

