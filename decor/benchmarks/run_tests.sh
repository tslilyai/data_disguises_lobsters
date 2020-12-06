#!/bin/bash

#set -x

trials=1
#tests=( "no_shim" "decor" "shim_only" "shim_parse" )
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
		--scale=0.1 --nqueries=8000 --prime
    done
done
#for test in "${tests[@]}"
#do
#    diff $test$trial.out no_shim$trial.out
#done

python3 plot.py
