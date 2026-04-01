param(
    [Parameter(Mandatory = $true)]
    [string]$SupabaseUrl,
    [Parameter(Mandatory = $true)]
    [string]$SupabaseKey,
    [int]$IntervalSeconds = 60,
    [string]$ServiceName = "InvestmentSyncWorker"
)

$ErrorActionPreference = "Stop"

function Assert-Admin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    if (-not $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Run PowerShell as Administrator."
    }
}

Assert-Admin

$projectDir = $PSScriptRoot
$pythonExe = Join-Path $projectDir ".venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    throw "Python not found in venv: $pythonExe"
}

if ($IntervalSeconds -lt 30) {
    $IntervalSeconds = 30
}

$runner = Join-Path $projectDir "run_sync_worker_service.cmd"
$logFile = Join-Path $projectDir "sync_worker_service.log"

$runnerContent = @"
@echo off
setlocal
set SUPABASE_URL=$SupabaseUrl
set SUPABASE_KEY=$SupabaseKey
set MARKET_SYNC_MIN_SECONDS=180
cd /d "$projectDir"
"$pythonExe" "$projectDir\sync_market_worker.py" --interval $IntervalSeconds >> "$logFile" 2>&1
"@

Set-Content -Path $runner -Value $runnerContent -Encoding ASCII

$existing = sc.exe query $ServiceName 2>$null
if ($LASTEXITCODE -eq 0) {
    sc.exe stop $ServiceName | Out-Null
    Start-Sleep -Seconds 1
    sc.exe delete $ServiceName | Out-Null
    Start-Sleep -Seconds 1
}

$binPath = "cmd.exe /c `"$runner`""
sc.exe create $ServiceName binPath= "$binPath" start= auto DisplayName= "Investment Market Sync Worker" | Out-Null
sc.exe description $ServiceName "Sync market bars to Supabase via sync_market_worker.py" | Out-Null
sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/5000/restart/5000 | Out-Null
sc.exe start $ServiceName | Out-Null

Write-Host "Service installed and started: $ServiceName"
Write-Host "Log file: $logFile"
Write-Host ""
Write-Host "Note: in real sleep (S3), CPU is paused and the service pauses too."
Write-Host "For overnight run, disable sleep or enable wake timers."
