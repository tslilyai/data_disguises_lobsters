#!/bin/bash

#set -x

trials=1
#tests=( "shim_only" "shim_parse" "decor" "no_shim" )
tests=( "decor" )

cargo build --release

for test in "${tests[@]}"
do
    for trial in `seq $trials`
    do
        echo $test: Trial $trial
    	#../target/release/lobsters-microbenchmarks \
    	perflock ../target/release/lobsters-microbenchmarks \
		--test=$test --testname=$test$trial \
		--nusers=100 --nstories=500 --ncomments=1000 --nthreads=1 --nqueries=1000
    done
done
