#!/bin/bash

#set -x

trials=1
tests=( "shim_only" "shim_parse" "decor" "no_shim" )
#tests=( "decor" )

cargo build --release

for test in "${tests[@]}"
do
    for trial in `seq $trials`
    do
        echo $test: Trial $trial
    	#../target/release/lobsters-microbenchmarks \
    	perflock ../target/release/lobsters-microbenchmarks \
		--test=$test --testname=$test$trial \
		--nusers=1000 --nstories=5000 --ncomments=10000 --nthreads=1 --nqueries=10
    done
done

for test in "${tests[@]}"
do
    diff $test$trial.out no_shim$trial.out
done

python3 plot.py
