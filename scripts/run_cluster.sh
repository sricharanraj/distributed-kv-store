#!/usr/bin/env bash
# Runs a 3-node local cluster (no Docker required) with replication factor 2.
# Ctrl+C stops all nodes.
set -euo pipefail

cd "$(dirname "$0")/.."
go build -o bin/server ./cmd/server

PIDS=()
cleanup() {
  echo "stopping cluster..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

PEERS="127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003"

./bin/server -http-addr=127.0.0.1:9001 -tcp-addr=127.0.0.1:9101 -node-id=127.0.0.1:9001 \
  -peers="$PEERS" -replicas=2 -data-dir=data-node1 &
PIDS+=($!)

./bin/server -http-addr=127.0.0.1:9002 -tcp-addr=127.0.0.1:9102 -node-id=127.0.0.1:9002 \
  -peers="$PEERS" -replicas=2 -data-dir=data-node2 &
PIDS+=($!)

./bin/server -http-addr=127.0.0.1:9003 -tcp-addr=127.0.0.1:9103 -node-id=127.0.0.1:9003 \
  -peers="$PEERS" -replicas=2 -data-dir=data-node3 &
PIDS+=($!)

echo "cluster up: http 127.0.0.1:9001/9002/9003  tcp 127.0.0.1:9101/9102/9103"
wait
