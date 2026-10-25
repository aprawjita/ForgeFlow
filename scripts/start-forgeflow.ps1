[CmdletBinding()]
param(
    [switch]$SkipApi,
    [int]$InfraTimeoutSeconds = 90,
    [int]$ApiTimeoutSeconds = 120,
    [int]$PrometheusTimeoutSeconds = 60,
    [string]$SpringProfile = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Definition
$RepoRoot    = Split-Path -Parent $ScriptDir
$ApiDir      = Join-Path $RepoRoot "api"
$ComposeFile = Join-Path $RepoRoot "docker-compose.yml"
$AppYml      = Join-Path $ApiDir "src\main\resources\application.yml"
$RuntimeDir  = Join-Path $RepoRoot ".runtime"
$PidFile     = Join-Path $RuntimeDir "api.pid.json"
$StdOutLog   = Join-Path $RuntimeDir "api-stdout.log"
$StdErrLog   = Join-Path $RuntimeDir "api-stderr.log"

function Write-Step([string]$Message) { Write-Host "`n==> $Message" -ForegroundColor Cyan }
function Write-Pass([string]$Message) { Write-Host "  [OK] $Message" -ForegroundColor Green }
function Fail-Startup([string]$Message) { Write-Host "`n  [ERROR] $Message" -ForegroundColor Red; exit 1 }

function Wait-Condition {
    param(
        [Parameter(Mandatory=$true, Position=0)][string]$Description,
        [Parameter(Mandatory=$true, Position=1)][scriptblock]$Condition,
        [Parameter(Position=2)][int]$TimeoutSeconds = 60,
        [Parameter(Position=3)][int]$IntervalSeconds = 2
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    Write-Host "  Waiting for $Description (timeout: ${TimeoutSeconds}s)..." -NoNewline
    while ((Get-Date) -lt $deadline) {
        try {
            if (& $Condition) {
                Write-Host " ready!" -ForegroundColor Green
                return $true
            }
        } catch { }
        Write-Host "." -NoNewline
        Start-Sleep -Seconds $IntervalSeconds
    }
    Write-Host " TIMED OUT!" -ForegroundColor Red
    return $false
}

function Get-YamlPropertyValue([string]$FilePath, [string]$Section, [string]$KeyName) {
    if (-not (Test-Path $FilePath)) { Fail-Startup "Config missing: $FilePath" }
    $lines = Get-Content $FilePath
    $inSection = [string]::IsNullOrEmpty($Section)
    foreach ($line in $lines) {
        if (-not [string]::IsNullOrEmpty($Section) -and $line -match "^([a-zA-Z0-9_-]+)\s*:\s*$") {
            $inSection = ($Matches[1].Trim() -eq $Section)
        }
        if ($inSection -or [string]::IsNullOrEmpty($Section)) {
            if ($line -match "^\s*$KeyName\s*:\s*(.+)$") {
                return ($Matches[1].Trim() -replace '\s+#.*$', '').Trim('"', "'")
            }
        }
    }
    return $null
}

function Get-SanitizedLogTail([string]$LogPath, [int]$Lines = 35) {
    if (-not (Test-Path $LogPath)) { return @() }
    return Get-Content $LogPath -Tail $Lines -ErrorAction SilentlyContinue | ForEach-Object {
        $_ -replace '(?i)(token|password|secret|secret-key)\s*[:=]\s*[^\s,"''\}]+', '$1=[REDACTED]'
    }
}

# --- 1. Audit Prerequisites and Configuration ---
Write-Step "1. Auditing Prerequisites and Configuration"
if (-not (Test-Path $ComposeFile)) { Fail-Startup "Missing docker-compose.yml at $ComposeFile" }
if (-not (Test-Path $AppYml))      { Fail-Startup "Missing application.yml at $AppYml" }

New-Item -Path $RuntimeDir -ItemType Directory -Force | Out-Null

try {
    $null = docker info 2>&1
    if ($LASTEXITCODE -ne 0) { Fail-Startup "Docker daemon is not running. Start Docker Desktop first." }
    Write-Pass "Docker Desktop daemon is running."
} catch {
    Fail-Startup "Docker CLI is not available."
}

$ApiPort      = Get-YamlPropertyValue $AppYml "server" "port"
if (-not $ApiPort) { $ApiPort = "8080" }
$VaultEnabled = Get-YamlPropertyValue $AppYml "forgeflow.vault" "enabled"
if (-not $VaultEnabled) { $VaultEnabled = Get-YamlPropertyValue $AppYml "" "enabled" }
$VaultAddr    = Get-YamlPropertyValue $AppYml "forgeflow.vault" "address"
if (-not $VaultAddr) { $VaultAddr = Get-YamlPropertyValue $AppYml "" "address" }
$VaultToken   = if ($env:VAULT_TOKEN) { $env:VAULT_TOKEN } else { Get-YamlPropertyValue $AppYml "" "token" }
$DbUser       = Get-YamlPropertyValue $AppYml "datasource" "username"
if (-not $DbUser) { $DbUser = Get-YamlPropertyValue $AppYml "" "username" }

Write-Pass "Verified application.yml settings (API Port: $ApiPort, Vault: $VaultAddr, DB User: $DbUser)."

# --- 2. Start Docker Compose Infrastructure ---
Write-Step "2. Starting Local Infrastructure & Observability Services"
Push-Location $RepoRoot
try {
    docker compose up -d postgres redis vault redpanda prometheus grafana
    if ($LASTEXITCODE -ne 0) { Fail-Startup "docker compose up failed with exit code $LASTEXITCODE." }
    Write-Pass "Docker Compose services requested."
} finally {
    Pop-Location
}

# --- 3. Verify Backing Service Readiness ---
Write-Step "3. Verifying Backing Service Readiness"
if (-not (Wait-Condition "PostgreSQL" { (docker inspect --format '{{.State.Health.Status}}' forgeflow-postgres 2>$null) -eq "healthy" } $InfraTimeoutSeconds)) {
    Fail-Startup "PostgreSQL container failed to become healthy."
}
if (-not (Wait-Condition "Redis" { (docker inspect --format '{{.State.Health.Status}}' forgeflow-redis 2>$null) -eq "healthy" } $InfraTimeoutSeconds)) {
    Fail-Startup "Redis container failed to become healthy."
}
if (-not (Wait-Condition "Redpanda Kafka Broker" {
    if ((docker inspect --format '{{.State.Running}}' forgeflow-redpanda 2>$null) -ne "true") { return $false }
    $tcpClient = New-Object System.Net.Sockets.TcpClient
    try {
        $iar = $tcpClient.BeginConnect("127.0.0.1", 9092, $null, $null)
        if ($iar.AsyncWaitHandle.WaitOne(1500, $false) -and $tcpClient.Connected) { return $true }
        return $false
    } finally {
        $tcpClient.Close()
    }
} $InfraTimeoutSeconds)) {
    Fail-Startup "Redpanda Kafka broker on 127.0.0.1:9092 failed to become reachable."
}
if (-not (Wait-Condition "HashiCorp Vault" {
    $h = Invoke-RestMethod "$VaultAddr/v1/sys/health" -TimeoutSec 3
    return ($h.initialized -eq $true -and $h.sealed -eq $false)
} $InfraTimeoutSeconds)) {
    Fail-Startup "HashiCorp Vault at $VaultAddr is unreachable, uninitialized, or sealed."
}

$null = docker exec forgeflow-postgres psql -U $DbUser -d forgeflow_db -c "SELECT 1;" 2>&1
if ($LASTEXITCODE -ne 0) {
    Fail-Startup "Database credential/schema check failed for user '$DbUser' on database 'forgeflow_db'."
}
Write-Pass "PostgreSQL database 'forgeflow_db' accepted connection for user '$DbUser'."

# --- 4. Check & Seed Vault KV-2 JWT Secret ---
Write-Step "4. Checking HashiCorp Vault KV-2 JWT Secret (secret/forgeflow/jwt)"
$vHeaders     = @{ "X-Vault-Token" = $VaultToken }
$secUri       = "$VaultAddr/v1/secret/data/forgeflow/jwt"
$secretExists = $false

try {
    $existing = Invoke-RestMethod -Uri $secUri -Method Get -Headers $vHeaders -TimeoutSec 5
    if ($null -ne $existing.data.data.'secret-key' -and $existing.data.data.'secret-key'.Length -ge 32) {
        $secretExists = $true
        Write-Pass "Required JWT secret already exists in Vault."
    }
} catch {
    $statusCode = 0
    if ($null -ne $_.Exception.Response) {
        $statusCode = [int]$_.Exception.Response.StatusCode
    }
    if ($statusCode -eq 403) {
        Fail-Startup "Vault rejected authentication token (HTTP 403 Forbidden)."
    }
}

if (-not $secretExists) {
    Write-Host "  JWT secret not found in Vault. Seeding KV-2 secret..." -ForegroundColor Yellow
    $jwtSecretB64 = $env:FORGEFLOW_JWT_SECRET_B64
    if ([string]::IsNullOrWhiteSpace($jwtSecretB64)) {
        $b = New-Object byte[] 32
        [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($b)
        $jwtSecretB64 = [Convert]::ToBase64String($b)
    }
    $payload = @{ data = @{ "secret-key" = $jwtSecretB64 } } | ConvertTo-Json -Depth 4
    try {
        $null = Invoke-RestMethod -Uri $secUri -Method Post -Headers $vHeaders -Body $payload -ContentType "application/json" -TimeoutSec 5
        $verifyRead = Invoke-RestMethod -Uri $secUri -Method Get -Headers $vHeaders -TimeoutSec 5
        if ([string]::IsNullOrWhiteSpace($verifyRead.data.data.'secret-key')) {
            Fail-Startup "Vault secret write succeeded, but verification read-back failed."
        }
        Write-Pass "Seeded and verified KV-2 secret 'secret/forgeflow/jwt' (secret value redacted)."
    } catch {
        Fail-Startup "Failed to seed JWT secret into Vault: $($_.Exception.Message)"
    } finally {
        $jwtSecretB64 = $null
        $payload      = $null
    }
}

# --- 5. Launch or Reuse ForgeFlow API ---
$ApiHealthUrl = "http://localhost:$ApiPort/actuator/health"
$ApiPromUrl   = "http://localhost:$ApiPort/actuator/prometheus"
$ApiMode      = "Skipped (-SkipApi)"

if ($SkipApi) {
    Write-Step "5. Skipping API Startup (-SkipApi specified)"
} else {
    Write-Step "5. Checking / Starting ForgeFlow API on port $ApiPort"
    $alreadyUp = $false
    try {
        $healthCheck = Invoke-RestMethod -Uri $ApiHealthUrl -Method Get -TimeoutSec 3
        if ($healthCheck.status -eq "UP") {
            $alreadyUp = $true
        }
    } catch {
        $alreadyUp = $false
    }

    if ($alreadyUp) {
        $ApiMode = "Reused existing healthy instance"
        Write-Pass "API is already running and reporting UP at $ApiHealthUrl. Reusing existing instance."
    } else {
        $portConn = Get-NetTCPConnection -LocalPort ([int]$ApiPort) -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($null -ne $portConn) {
            Fail-Startup "Port $ApiPort is bound by PID $($portConn.OwningProcess), but $ApiHealthUrl is not UP. Run .\scripts\stop-forgeflow.ps1 first."
        }

        if (Test-Path $StdOutLog) { Remove-Item $StdOutLog -Force }
        if (Test-Path $StdErrLog) { Remove-Item $StdErrLog -Force }
        $mvnw = Join-Path $ApiDir "mvnw.cmd"
        if (-not (Test-Path $mvnw)) { Fail-Startup "Maven wrapper missing at $mvnw" }

        $mvnArgs = "spring-boot:run"
        if (-not [string]::IsNullOrWhiteSpace($SpringProfile)) {
            $mvnArgs += " -Dspring-boot.run.profiles=$SpringProfile"
        }

        Write-Host "  Launching API via .\mvnw.cmd $mvnArgs (logs -> .runtime\api-stdout.log)..."
        $proc = Start-Process -FilePath $mvnw `
                              -ArgumentList $mvnArgs `
                              -WorkingDirectory $ApiDir `
                              -RedirectStandardOutput $StdOutLog `
                              -RedirectStandardError $StdErrLog `
                              -WindowStyle Hidden `
                              -PassThru

        $pidJson = @{
            Pid       = $proc.Id
            StartTime = $proc.StartTime.ToString("o")
            Port      = [int]$ApiPort
        } | ConvertTo-Json
        [System.IO.File]::WriteAllText($PidFile, $pidJson, (New-Object System.Text.UTF8Encoding $False))

        $started = Wait-Condition "ForgeFlow API health ($ApiHealthUrl)" {
            if ($proc.HasExited) { throw "ProcessExited" }
            return ((Invoke-RestMethod -Uri $ApiHealthUrl -Method Get -TimeoutSec 2).status -eq "UP")
        } $ApiTimeoutSeconds

        if (-not $started -or $proc.HasExited) {
            Write-Host "`n  --- SANITIZED API LOG TAILS (STDOUT) ---" -ForegroundColor Yellow
            Get-SanitizedLogTail $StdOutLog 30 | ForEach-Object { Write-Host "  $_" }
            Write-Host "  --- SANITIZED API LOG TAILS (STDERR) ---" -ForegroundColor Yellow
            Get-SanitizedLogTail $StdErrLog 20 | ForEach-Object { Write-Host "  $_" }
            if (-not $proc.HasExited) {
                taskkill.exe /PID $proc.Id /T /F 2>&1 | Out-Null
            }
            if (Test-Path $PidFile) { Remove-Item $PidFile -Force -ErrorAction SilentlyContinue }
            Fail-Startup "ForgeFlow API failed to reach UP state within ${ApiTimeoutSeconds}s."
        }
        $ApiMode = "Started new instance (Wrapper PID: $($proc.Id))"
        Write-Pass "ForgeFlow API started ($ApiMode)."
    }
}

# --- 6. Validate Prometheus and Grafana ---
Write-Step "6. Validating Prometheus and Grafana Observability"
if (-not (Wait-Condition "Prometheus readiness" {
    (Invoke-WebRequest -Uri "http://localhost:9090/-/ready" -UseBasicParsing -TimeoutSec 3).StatusCode -eq 200
} 30)) {
    Fail-Startup "Prometheus is not ready on http://localhost:9090."
}

$PromTargetState = "SKIPPED (-SkipApi)"
$UpMetricVal     = "SKIPPED (-SkipApi)"

if (-not $SkipApi) {
    if (-not (Wait-Condition "Prometheus target 'forgeflow-api' UP & up==1" {
        $targets = Invoke-RestMethod -Uri "http://localhost:9090/api/v1/targets" -TimeoutSec 3
        $ffTarget = $targets.data.activeTargets | Where-Object { $_.labels.job -eq "forgeflow-api" } | Select-Object -First 1
        if ($null -eq $ffTarget -or $ffTarget.health -ne "up") { return $false }
        $upRes = Invoke-RestMethod -Uri "http://localhost:9090/api/v1/query?query=up{job=`"forgeflow-api`"}" -TimeoutSec 3
        if ($upRes.data.result.Count -eq 0) { return $false }
        return ($upRes.data.result[0].value[1] -eq "1")
    } $PrometheusTimeoutSeconds)) {
        Fail-Startup "Prometheus target 'forgeflow-api' failed to report UP (up=1)."
    }
    $PromTargetState = "UP"
    $UpMetricVal     = "1"
    Write-Pass "Prometheus target 'forgeflow-api' is UP and up{job=`"forgeflow-api`"} == 1."
}

if (-not (Wait-Condition "Grafana health" {
    (Invoke-RestMethod -Uri "http://localhost:3000/api/health" -TimeoutSec 3).database -eq "ok"
} 30)) {
    Fail-Startup "Grafana is not healthy on http://localhost:3000."
}

$DashFile = Join-Path $RepoRoot "grafana\dashboards\forgeflow.json"
if (-not (Test-Path $DashFile)) {
    Fail-Startup "Provisioned Grafana dashboard file missing at $DashFile"
}
$grafUser = if ($env:GRAFANA_USER) { $env:GRAFANA_USER } else { "admin" }
$grafPass = if ($env:GRAFANA_PASSWORD) { $env:GRAFANA_PASSWORD } else { "secured_local_pass" }
$GrafanaDashStatus = "Provisioned on disk"
try {
    $b64 = [Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("${grafUser}:${grafPass}"))
    $gHeaders = @{ Authorization = "Basic $b64" }
    $dashSearch = Invoke-RestMethod -Uri "http://localhost:3000/api/search?query=ForgeFlow" -Headers $gHeaders -TimeoutSec 5
    $dashMatch = $dashSearch | Where-Object { $_.uid -eq "forgeflow-main" -or $_.title -match "ForgeFlow Operations" } | Select-Object -First 1
    if ($null -eq $dashMatch) {
        Fail-Startup "Grafana is reachable, but dashboard 'ForgeFlow Operations' was not found via /api/search."
    }
    $GrafanaDashStatus = "Verified ('$($dashMatch.title)')"
    Write-Pass "Grafana health is OK and dashboard '$($dashMatch.title)' is discoverable."
} catch {
    Fail-Startup "Failed to verify Grafana dashboard: $($_.Exception.Message)"
}

# --- 7. Summary ---
Write-Host "`n==============================================================" -ForegroundColor Green
Write-Host "  FORGEFLOW LOCAL STACK & OBSERVABILITY READY" -ForegroundColor Green
Write-Host "==============================================================" -ForegroundColor Green
Write-Host ("  {0,-22} : {1}" -f "PostgreSQL", "127.0.0.1:5433 (healthy)")
Write-Host ("  {0,-22} : {1}" -f "Redis", "127.0.0.1:6379 (healthy)")
Write-Host ("  {0,-22} : {1}" -f "Redpanda (Kafka)", "127.0.0.1:9092 (reachable)")
Write-Host ("  {0,-22} : {1}" -f "HashiCorp Vault", "$VaultAddr (unsealed, JWT secret ready)")
Write-Host ("  {0,-22} : {1}" -f "ForgeFlow API", $ApiMode)
if (-not $SkipApi) {
    Write-Host ("  {0,-22} : {1}" -f "API Health URL", $ApiHealthUrl)
    Write-Host ("  {0,-22} : {1}" -f "API Prometheus URL", $ApiPromUrl)
}
Write-Host ("  {0,-22} : {1}" -f "Prometheus UI", "http://localhost:9090 (target: $PromTargetState, up=$UpMetricVal)")
Write-Host ("  {0,-22} : {1}" -f "Grafana UI", "http://localhost:3000 ($GrafanaDashStatus)")
Write-Host ("  {0,-22} : {1}" -f "Stop Command", ".\scripts\stop-forgeflow.ps1")
Write-Host "==============================================================`n" -ForegroundColor Green