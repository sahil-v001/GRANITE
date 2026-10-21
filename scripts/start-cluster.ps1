# GRANITE — Start 3-Node Cluster
# Opens each node in its own PowerShell window

$root = Split-Path -Parent $PSScriptRoot

Write-Host "Starting GRANITE 3-node cluster..." -ForegroundColor Cyan

Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command",
    "cd '$root'; Write-Host '=== NODE 1 (port 5001) ===' -ForegroundColor Green; go run ./cmd/server --id node1 --addr :5001 --peers node2=127.0.0.1:5002,node3=127.0.0.1:5003 --data ./data/node1"
)

Start-Sleep -Milliseconds 300

Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command",
    "cd '$root'; Write-Host '=== NODE 2 (port 5002) ===' -ForegroundColor Yellow; go run ./cmd/server --id node2 --addr :5002 --peers node1=127.0.0.1:5001,node3=127.0.0.1:5003 --data ./data/node2"
)

Start-Sleep -Milliseconds 300

Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command",
    "cd '$root'; Write-Host '=== NODE 3 (port 5003) ===' -ForegroundColor Magenta; go run ./cmd/server --id node3 --addr :5003 --peers node1=127.0.0.1:5001,node2=127.0.0.1:5002 --data ./data/node3"
)

Write-Host ""
Write-Host "3 windows opened. Watch for 'ELECTED LEADER' in one of them!" -ForegroundColor Cyan
Write-Host "Kill that window to test failover. Another node will take over within 300ms." -ForegroundColor Yellow
