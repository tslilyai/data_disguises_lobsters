#!/bin/bash

RUST_LOG=error
cargo build --release
rm *.txt
rm -rf output
mkdir output

set -e
l=20

for u in 1 30 100; do
	for nd in 0 1; do
	    ps -ef | grep 'websubmit-server' | grep -v grep | awk '{print $2}' | xargs -r kill -9 || true

	    sleep 2

	    echo "Starting server"
	    RUST_LOG=error ../../target/release/websubmit-server \
		-i myclass --schema server/src/schema.sql --config server/sample-config.toml \
		--benchmark false --prime true \
		--nusers 0 --nlec 0 --nqs 0 &> \
		output/server_${l}lec_${u}users_0sleep.out &

	    sleep 5

	    echo "Running client"
	    RUST_LOG=error perflock ../../target/release/websubmit-client \
		--nusers $u --nlec $l --nqs 4 --nsleep 0 --ndisguising $nd \
		--db myclass &> \
		output/client_${l}lec_${u}users_0sleep.out
	    echo "Ran test($t) for $l lecture and $u users, 0 sleep"

	    rm *.txt
	done
done
