#!/bin/bash

#set -x

trials=2
tests=( "shim_only" "shim_parse" "decor" )
#tests=( "decor" )

cargo build --release

for test in "${tests[@]}"
do
    for trial in `seq $trials`
    do
        echo $test: Trial $trial
    	perflock ../target/release/lobsters-microbenchmarks \
		--test=$test --testname=$test$trial \
		--nusers=10 --nstories=100 --ncomments=1000 --nthreads=1 --nqueries=6000
    done
done
