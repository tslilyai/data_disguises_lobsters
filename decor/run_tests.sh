#!/bin/bash

#set -x

trials=4
tests=( "no_shim" "shim_only" "shim_parse" "decor" )
testops=( "select" "insert" "update" )

cargo build --release

for test in "${tests[@]}"
do
    for i in `seq $trials`
    do
        echo $test: Trial $i
        for testop in "${testops[@]}"
        do
            ./target/release/lobsters --test=$test --testop=$testop --nusers=10 --nstories=100 --ncomments=1000 --nthreads=1 --nqueries=3000
        done
    done
done
