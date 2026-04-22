param(
    [string]$ControlPlaneHost = $env:CONTROL_PLANE_HOST,
    [string]$ApiPort = $env:SAP_API_PORT,
    [string]$RunnerId = $env:SAP_RUNNER_ID
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ControlPlaneHost)) {
    $ControlPlaneHost = "10.71.202.127"
}
if ([string]::IsNullOrWhiteSpace($ApiPort)) {
    $ApiPort = "18000"
}
if ([string]::IsNullOrWhiteSpace($RunnerId)) {
    $RunnerId = "windows-sap-runner-01"
}

$env:DATABASE_URL = if ($env:DATABASE_URL) { $env:DATABASE_URL } else { "postgresql+psycopg://sap_automation:sap_automation@${ControlPlaneHost}:5432/sap_automation" }
$env:REDIS_URL = if ($env:REDIS_URL) { $env:REDIS_URL } else { "redis://${ControlPlaneHost}:6379/0" }
$env:SAP_QUEUE_NAME = "sap-default"
$env:SAP_RUNNER_ID = $RunnerId
$env:SAP_OUTPUT_ROOT = if ($env:SAP_OUTPUT_ROOT) { $env:SAP_OUTPUT_ROOT } else { "output" }
$env:CONTROL_PLANE_BASE_URL = if ($env:CONTROL_PLANE_BASE_URL) { $env:CONTROL_PLANE_BASE_URL } else { "http://${ControlPlaneHost}:${ApiPort}" }

Write-Host "Checking control plane API $env:CONTROL_PLANE_BASE_URL/health"
Invoke-RestMethod "$env:CONTROL_PLANE_BASE_URL/health" | Out-Null

Write-Host "Checking Postgres ${ControlPlaneHost}:5432"
$pg = Test-NetConnection $ControlPlaneHost -Port 5432
if (-not $pg.TcpTestSucceeded) {
    throw "Cannot reach Postgres at ${ControlPlaneHost}:5432"
}

Write-Host "Checking Redis ${ControlPlaneHost}:6379"
$redis = Test-NetConnection $ControlPlaneHost -Port 6379
if (-not $redis.TcpTestSucceeded) {
    throw "Cannot reach Redis at ${ControlPlaneHost}:6379"
}

Write-Host "Starting SAP runner id=$env:SAP_RUNNER_ID queue=$env:SAP_QUEUE_NAME"
python -m sap_automation.runner
