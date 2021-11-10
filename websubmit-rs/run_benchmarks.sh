#!/bin/bash

for (( u = 1; u < 2 ; ++u )); do
    for (( l = 1; l < 2; ++l )); do
        cargo run --release -- -i myclass --benchmark true --prime true --nusers $(( 2*u )) --nlec $(( 2*l )) --nqs 4
    done
done
