#!/bin/bash

cd websubmit-rs/server
./run_benchmarks.sh
cd ../../hotcrp
./run_benchmarks.sh
cd ../lobsters
./run_benchmarks.sh
cd ../websubmit-rs
./run_multiclient_benchmark.sh
