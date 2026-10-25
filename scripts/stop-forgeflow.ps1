[CmdletBinding()]
param([switch]$StopContainers)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Definition
$RepoRoot   = Split-Path -Parent $ScriptDir
$PidFile    = Join-Path $RepoRoot ".runtime\api.pid.json"

Write-Host "`n==> Stopping ForgeFlow Local Environment" -ForegroundColor Cyan

if (Test-Path $PidFile) {
    try {
        $pidInfo = Get-Content $PidFile -Raw | ConvertFrom-Json
        $trackedPid = [int]$pidInfo.Pid
        if (Get-Process -Id $trackedPid -ErrorAction SilentlyContinue) {
            Write-Host "  Stopping tracked API process (PID: $trackedPid)..."
            taskkill.exe /PID $trackedPid /T /F 2>&1 | Out-Null
            Write-Host "  [OK] Stopped." -ForegroundColor Green
        }
    } catch { }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

Start-Sleep -Seconds 1
$portConn = Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
if ($null -ne $portConn) {
    $ownerPid = $portConn.OwningProcess
    Write-Host "  Stopping Java process on port 8080 (PID: $ownerPid)..."
    taskkill.exe /PID $ownerPid /T /F 2>&1 | Out-Null
    Write-Host "  [OK] Stopped." -ForegroundColor Green
}

if ($StopContainers) {
    Push-Location $RepoRoot
    docker compose stop
    Pop-Location
    Write-Host "  [OK] Docker Compose services stopped." -ForegroundColor Green
}