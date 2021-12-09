#!/bin/bash

RUST_LOG=error
cargo build --release
rm *.txt
rm -rf output
mkdir output

set -e
l=20
u=30
nd=0

for s in 0; do
    ps -ef | grep 'websubmit-server' | grep -v grep | awk '{print $2}' | xargs -r kill -9 || true

    sleep 2

    echo "Starting server"
    RUST_LOG=error ../../target/release/websubmit-server \
        -i myclass --schema server/src/schema.sql --config server/sample-config.toml \
        --benchmark false --prime true \
        --nusers 0 --nlec 0 --nqs 0 &> \
        output/server_${l}lec_${u}users_${s}sleep.out &

    sleep 5

    echo "Running client"
    RUST_LOG=error perflock ../../target/release/websubmit-client \
        --nusers $u --nlec $l --nqs 4 --nsleep $s --ndisguising $nd \
        --db myclass &> \
        output/client_${l}lec_${u}users_${s}sleep.out
    echo "Ran test($t) for $l lecture and $u users, $s sleep"

    rm *.txt
done
