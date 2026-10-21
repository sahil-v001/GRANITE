#!/usr/bin/env bash
# GRANITE — 3-Node Cluster Startup Script (Bash)

echo "🚀 Starting GRANITE 3-Node Cluster..."

rm -rf ./data/node1 ./data/node2 ./data/node3

go run ./cmd/server --id node1 --addr :5001 --peers node2=127.0.0.1:5002,node3=127.0.0.1:5003 --data ./data/node1 &
go run ./cmd/server --id node2 --addr :5002 --peers node1=127.0.0.1:5001,node3=127.0.0.1:5003 --data ./data/node2 &
go run ./cmd/server --id node3 --addr :5003 --peers node1=127.0.0.1:5001,node2=127.0.0.1:5002 --data ./data/node3 &

echo "✅ Cluster nodes running on ports 5001, 5002, 5003!"
