#!/bin/bash

#set -x

trials=3
tests=( "decor" "shim_parse" "shim_only" "no_shim" )
#tests=( "shim_only" )

cargo build --release

for test in "${tests[@]}"
do
    for i in `seq $trials`
    do
        ./target/release/lobsters --test=$test --nusers=10 --nstories=100 --ncomments=1000 --nthreads=1 --nqueries=2000 #2> /dev/null
    done
done
