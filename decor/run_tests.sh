#!/bin/bash

#set -x

#tests=( "decor" "shim_parse" "shim_only" "no_shim" )
tests=( "shim_only" )

for test in "${tests[@]}"
do
    ./target/debug/lobsters --test=$test --num_users=10 --num_stories=100 --num_comments=1000 --num_threads=1 --num_queries=300 #2> /dev/null
done
