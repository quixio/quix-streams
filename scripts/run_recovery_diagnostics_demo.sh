#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/recovery_diagnostics_docker_compose.yml"
KAFKA_PORT="${KAFKA_PORT:-19092}"
BOOTSTRAP_SERVERS="localhost:${KAFKA_PORT}"

cleanup() {
  docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans
}

trap cleanup EXIT

cd "${REPO_ROOT}"

docker compose -f "${COMPOSE_FILE}" up -d

python3 - <<PY
import time
from confluent_kafka.admin import AdminClient

bootstrap = "${BOOTSTRAP_SERVERS}"
deadline = time.monotonic() + 120
last_error = None

while time.monotonic() < deadline:
    try:
        AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
        print(f"Kafka is ready at {bootstrap}")
        break
    except Exception as exc:
        last_error = exc
        time.sleep(1)
else:
    raise SystemExit(f"Timed out waiting for Kafka at {bootstrap}: {last_error}")
PY

KAFKA_BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS}" python3 scripts/recovery_diagnostics_demo.py
