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
reference="${1:-${SM_REFERENCE:-current_month}}"
run_id="${2:-SM_$(date +%Y%m%dT%H%M%S)}"

payload=$(python - "$run_id" "$reference" <<'PY'
import json
import os
import sys

run_id = sys.argv[1]
reference = sys.argv[2]
body = {
    "workflow_type": os.environ.get("SM_WORKFLOW_TYPE", "sm_sala_mercado_daily"),
    "demandante": os.environ.get("SM_DEMANDANTE", "SALA_MERCADO"),
    "run_id": run_id,
    "reference": reference,
    "payload": {
        "distribuidora": os.environ.get("SM_DISTRIBUIDORA", "São Paulo"),
        "output_root": os.environ.get("SAP_OUTPUT_ROOT", "/app/output"),
        "config_path": os.environ.get("SM_CONFIG_PATH", "sap_iw69_batch_config.json"),
        "control_plane_base_url": os.environ.get("CONTROL_PLANE_BASE_URL", ""),
    },
}
print(json.dumps(body, ensure_ascii=False))
PY
)

curl -fsS -X POST "${api_base}/api/v1/workflows" \
  -H "Content-Type: application/json; charset=utf-8" \
  --data-binary "${payload}"

echo
echo "Workflow created: ${run_id}"
echo "Inspect: ${api_base}/api/v1/workflows/${run_id}"
