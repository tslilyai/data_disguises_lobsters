#!/bin/bash

RUST_LOG=error
cargo build --release

for (( l = 20; l <= 20; l+=10 )); do
    for (( u = 5; u <= 30; u+=5)); do
	RUST_LOG=error ../target/release/websubmit -i myclass --benchmark true --prime true --nusers $u --nlec $l --nqs 4
    done
done

python3 plot.py
