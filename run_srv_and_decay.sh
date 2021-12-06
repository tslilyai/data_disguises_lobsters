#curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

source $HOME/.cargo/env

set -e

# kill the current server
ps -ef | grep 'edna-server' | grep -v grep | awk '{print $2}' | xargs -r kill -9 || true

sleep 5

# start a new server
cd edna-srv
cargo run --release -- -h mariadb -d lobsters_development &

echo "Server Running, wait a bit"

sleep 10

# eventually loop decay disguise
echo "Running decay"

cd ../lobsters-decay
RUSTFLAGS=-Ctarget-feature=-crt-static
cargo run --release -- -h mariadb -d lobsters_development
