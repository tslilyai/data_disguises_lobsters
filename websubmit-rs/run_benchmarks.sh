#!/bin/bash

RUST_LOG=error
cargo build --release

for (( u = 1; u < 50 ; u+=2 )); do
    for (( l = 1; l < 20; l+=5 )); do
	RUST_LOG=error ../target/release/websubmit -i myclass --benchmark true --prime true --nusers $(( 2*u )) --nlec $(( 2*l )) --nqs 4
    done
done
