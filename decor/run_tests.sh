#!/bin/bash

#set -x

trials=3
tests=( "no_shim" "shim_only" "shim_parse" "decor" )
#tests=( "shim_only" )

cargo build --release

for test in "${tests[@]}"
do
    for i in `seq $trials`
    do
        ./target/release/lobsters --test=$test --nusers=10 --nstories=100 --ncomments=1000 --nthreads=1 --nqueries=3000 #2> /dev/null
    done
done