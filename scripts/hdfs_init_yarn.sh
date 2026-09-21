#!/usr/bin/env bash
set -euo pipefail

echo "[hdfs-init] Waiting for HDFS..."
until hdfs dfs -ls / >/dev/null 2>&1; do
  sleep 2
done

echo "[hdfs-init] Creating base directories..."
hdfs dfs -mkdir -p /data/transport/raw
hdfs dfs -mkdir -p /data/transport/clean
hdfs dfs -mkdir -p /data/transport/curated
hdfs dfs -mkdir -p /data/transport/analytics
hdfs dfs -mkdir -p /data/weather/raw
hdfs dfs -mkdir -p /data/weather/clean
hdfs dfs -mkdir -p /tmp/spark-checkpoints

echo "[hdfs-init] Setting permissive perms (dev/lab)..."
hdfs dfs -chmod -R 777 /data /tmp/spark-checkpoints || true

echo "[hdfs-init] Done."
