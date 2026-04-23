#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo ".env not found. Copy .env.example to .env first." >&2
  exit 2
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

api_base="${CONTROL_PLANE_BASE_URL:-http://${CONTROL_PLANE_HOST:-10.71.202.127}:${SAP_API_PORT:-18000}}"
run_id="${1:-}"
job_id="${2:-}"
tmpdir="$(mktemp -d)"
report_path="${tmpdir}/sm_diagnostics.txt"

cleanup() {
  echo
  echo "Diagnostic report saved to: ${report_path}"
}
trap cleanup EXIT

section() {
  printf '\n===== %s =====\n' "$1" | tee -a "${report_path}"
}

run_cmd() {
  local label="$1"
  shift
  section "${label}"
  {
    printf '$ %s\n' "$*"
    "$@"
  } >>"${report_path}" 2>&1 || true
  cat "${report_path}" | tail -n +1 >/dev/null
}

append_cmd() {
  local label="$1"
  shift
  section "${label}"
  {
    printf '$ %s\n' "$*"
    "$@"
  } >>"${report_path}" 2>&1 || true
}

append_cmd "Timestamp" date -Is
append_cmd "Docker Compose PS" docker compose ps -a
append_cmd "Docker PS" docker ps -a
append_cmd "API Health" curl -sS "${api_base}/health"
append_cmd "Schedules" curl -sS "${api_base}/api/v1/schedules"
append_cmd "Runners" curl -sS "${api_base}/api/v1/runners"

if [[ -n "${run_id}" ]]; then
  append_cmd "Workflow ${run_id}" curl -sS "${api_base}/api/v1/workflows/${run_id}"
  append_cmd "Artifacts ${run_id}" curl -sS "${api_base}/api/v1/artifacts/${run_id}"
  append_cmd "Jobs filtered by ${run_id}" /bin/bash -lc "curl -sS '${api_base}/api/v1/jobs' | python3 - <<'PY'
import json, sys
payload = json.load(sys.stdin)
run_id = '${run_id}'
items = [item for item in payload.get('data', []) if item.get('run_id') == run_id]
print(json.dumps(items, ensure_ascii=False, indent=2))
PY"
fi

if [[ -n "${job_id}" ]]; then
  append_cmd "Job ${job_id}" curl -sS "${api_base}/api/v1/jobs/${job_id}"
fi

append_cmd "API logs" docker compose logs --tail=200 api
append_cmd "DB Runner logs" docker compose logs --tail=200 db-runner
append_cmd "Scheduler logs" docker compose logs --tail=200 scheduler
append_cmd "Postgres logs" docker compose logs --tail=100 postgres
append_cmd "Redis logs" docker compose logs --tail=100 redis

append_cmd "pyodbc drivers in db-runner" docker compose exec -T db-runner python -c "import pyodbc; print(pyodbc.drivers())"
append_cmd "Selected env in db-runner" /bin/bash -lc "docker compose exec -T db-runner env | grep -E '^(ENEL_SQL|DATABASE_URL|REDIS_URL|SAP_|CONTROL_PLANE_)' | sed -E 's/^(ENEL_SQL_PASSWORD|SAP_PASSWORD)=.*/\\1=***REDACTED***/'"

echo
echo "===== BEGIN DIAGNOSTIC REPORT ====="
cat "${report_path}"
echo "===== END DIAGNOSTIC REPORT ====="
