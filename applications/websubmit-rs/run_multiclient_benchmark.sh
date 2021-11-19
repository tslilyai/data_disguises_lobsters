#!/bin/bash

RUST_LOG=error
cargo build --release
rm *.csv *.txt
rm -rf output
mkdir output

set -e
	
for l in 20 40; do
    for u in 10 30 50 70 100; do
	    for nd in $((u/10)) $((u / 6)) $((u / 4)) $((u / 2)); do
		for baseline in true false; do
			ps -ef | grep 'websubmit-server' | grep -v grep | awk '{print $2}' | xargs -r kill -9 || true
			
			sleep 8

			echo "Starting server"
			RUST_LOG=error ../../target/release/websubmit-server \
				-i myclass --schema server/src/schema.sql --config server/sample-config.toml \
				--benchmark false --prime true \
				--nusers 0 --nlec 0 --nqs 0 &> \
				output/server.out &
			
			sleep 8

			echo "Running client"
			RUST_LOG=error perflock ../../target/release/websubmit-client \
				--nusers $u --nlec $l --nqs 4 --ndisguising $nd \
				--baseline $baseline --db myclass &> \
				output/${l}lec_${u}users_${nd}disguisers_$baseline.out
			echo "Ran baseline($baseline) test for $l lecture and $u, $nd users"
		done
	done
    done
    python3 plot.py $l
done
