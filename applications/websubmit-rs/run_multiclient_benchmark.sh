#!/bin/bash

RUST_LOG=error
cargo build --release
rm *.txt
rm -rf output
mkdir output

set -e

for l in 20; do
    for u in 100; do
	ps -ef | grep 'websubmit-server' | grep -v grep | awk '{print $2}' | xargs -r kill -9 || true

	sleep 8

	echo "Starting server"
	RUST_LOG=error ../../target/release/websubmit-server \
		-i myclass --schema server/src/schema.sql --config server/sample-config.toml \
		--benchmark false --prime true \
		--nusers 0 --nlec 0 --nqs 0 &> \
		output/server_${l}lec_${u}users_normal_disguising.out &

	sleep 15

	echo "Running client"
	RUST_LOG=error ../../target/release/websubmit-client \
		--nusers $u --nlec $l --nqs 4 \
		--test 1 --db myclass &> \
		output/${l}lec_${u}users_normal_disguising.out
	echo "Ran test(1) for $l lecture and $u users"

    	for t in 0 2; do
		for nd in $((u/10)) $((u/8)) $((u/6)) $((u/4)) 20 30; do
		ps -ef | grep 'websubmit-server' | grep -v grep | awk '{print $2}' | xargs -r kill -9 || true

		sleep 8

		echo "Starting server"
		RUST_LOG=error ../../target/release/websubmit-server \
			-i myclass --schema server/src/schema.sql --config server/sample-config.toml \
			--benchmark false --prime true \
			--nusers 0 --nlec 0 --nqs 0 &> \
			output/server_${l}lec_${u}users_${nd}disguisers_$t.out &

		sleep 15

		echo "Running client"
		RUST_LOG=error ../../target/release/websubmit-client \
			--nusers $u --nlec $l --nqs 4 --ndisguising $nd \
			--test $t --db myclass &> \
			output/${l}lec_${u}users_${nd}disguisers_$t.out
		echo "Ran test($t) for $l lecture and $u, $nd users"
	    done
	done
    done
    python3 plot.py $l
done
