#!/bin/bash

set +x

tests=( "no_shim" "shim_only" "shim_parse" "decor" )

cargo clean
cargo build --release

for test in "${tests[@]}"
do
	echo $test
	cargo flamegraph -o flamegraphs/$test-select.svg --bin lobsters -- --test=$test --nusers=10 --nstories=100 --ncomments=1000 --nthreads=1 --nqueries=3000 --testop=select
	cargo flamegraph -o flamegraphs/$test-insert.svg --bin lobsters -- --test=$test --nusers=10 --nstories=100 --ncomments=1000 --nthreads=1 --nqueries=3000 --testop=insert
	cargo flamegraph -o flamegraphs/$test-update.svg --bin lobsters -- --test=$test --nusers=10 --nstories=100 --ncomments=1000 --nthreads=1 --nqueries=3000 --testop=update
done
