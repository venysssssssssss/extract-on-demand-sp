#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example. Fill SQL Server and credential values before running again." >&2
  exit 2
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

export SAP_API_PORT="${SAP_API_PORT:-18000}"
export SAP_OUTPUT_ROOT="${SAP_OUTPUT_ROOT:-/app/output}"
export SAP_SCHEDULER_POLL_SECONDS="${SAP_SCHEDULER_POLL_SECONDS:-15}"
export SAP_RUNNER_POLL_SECONDS="${SAP_RUNNER_POLL_SECONDS:-5}"
export DB_RUNNER_ID="${DB_RUNNER_ID:-ubuntu-db-runner}"

docker compose up -d --build postgres redis api scheduler db-runner

api_url="http://localhost:${SAP_API_PORT}/health"
echo "Waiting for API at ${api_url}..."
for _ in $(seq 1 60); do
  if curl -fsS "${api_url}" >/dev/null; then
    break
  fi
  sleep 2
done

curl -fsS "${api_url}" >/dev/null

cat <<EOF
Control plane is running.

API: http://${CONTROL_PLANE_HOST:-10.71.202.127}:${SAP_API_PORT}
Daily SM schedule: sm-sala-mercado-diario-1130 at 11:30 America/Bahia

Useful commands:
  docker compose ps
  docker compose logs -f api scheduler db-runner
  curl http://localhost:${SAP_API_PORT}/api/v1/schedules
  curl http://localhost:${SAP_API_PORT}/api/v1/runners
EOF
