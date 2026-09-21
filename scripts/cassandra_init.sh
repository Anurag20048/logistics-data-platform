#!/usr/bin/env bash
# cassandra_init.sh — Waits for Cassandra to accept connections, then creates
# the logistics keyspace and tables.
set -euo pipefail

CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
MAX_RETRIES=60
SLEEP_SECONDS=5

echo "[cassandra-init] Waiting for Cassandra at ${CASSANDRA_HOST}:${CASSANDRA_PORT}..."
for i in $(seq 1 ${MAX_RETRIES}); do
  if cqlsh "${CASSANDRA_HOST}" "${CASSANDRA_PORT}" -e "DESCRIBE KEYSPACES;" > /dev/null 2>&1; then
    echo "[cassandra-init] Cassandra is ready (attempt ${i})"
    break
  fi
  echo "[cassandra-init] Not ready yet, waiting ${SLEEP_SECONDS}s... (${i}/${MAX_RETRIES})"
  sleep "${SLEEP_SECONDS}"
done

echo "[cassandra-init] Applying schema..."
cqlsh "${CASSANDRA_HOST}" "${CASSANDRA_PORT}" -f /scripts/cassandra_init.cql

echo "[cassandra-init] Done."
