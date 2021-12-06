#curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

source $HOME/.cargo/env

cd edna-srv
cargo run --release -- -h mariadb -d lobsters_development &

echo "Server Running, wait a bit"

sleep 10

cd ../lobsters_decay
cargo run --release -- -h mariadb -d lobsters_development
