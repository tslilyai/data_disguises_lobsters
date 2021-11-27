#!/bin/bash

RUST_LOG=error
cargo build --release
rm -rf output
rm *txt
mkdir output

RUST_LOG=error perflock ../../../target/release/hotcrp \
    --prime true &> \
    output/${u}users.out
echo "Ran test for $u users"
