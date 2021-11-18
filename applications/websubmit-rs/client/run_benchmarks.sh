#!/bin/bash

RUST_LOG=error
cargo build --release --features flame_it
rm -f output
mkdir output

for l in 20 40; do
    for u in 10 20 30 50 70 100; do
    	for nd in 2 5 10; do
		RUST_LOG=error perflock ../../../target/release/websubmit-client \
		--nusers $u --nlec $l --nqs 4 --ndisguising $nd --baseline true &> \
		    output/${l}lec_${u}users_${nd}disguisers_baseline.out
		echo "Ran baseline test for $l lecture and $u, $nd users"

		RUST_LOG=error perflock ../../../target/release/websubmit-client \
		--nusers $u --nlec $l --nqs 4 --ndisguising $nd --baseline false &> \
		    output/${l}lec_${u}users_${nd}disguisers.out
		echo "Ran test for $l lecture and $u, $nd users"
	    done
    done
    python3 plot.py $l
done

