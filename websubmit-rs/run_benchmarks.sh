#!/bin/bash

RUST_LOG=error
cargo build --release --features flame_it

for l in 20 40; do
    for u in 10 20 30 50 70 100; do
	RUST_LOG=error ../target/release/websubmit \
        -i myclass --benchmark true --prime true --nusers $u --nlec $l --nqs 4 &> ${l}lec_${u}users_out.txt
	echo "Ran test for $l lecture and $u users"
    done
    python3 plot.py $l
done

