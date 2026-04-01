param(
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
$runner = Join-Path $projectDir "run_sync_worker_service.cmd"

$existing = sc.exe query $ServiceName 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Service not found: $ServiceName"
} else {
    sc.exe stop $ServiceName | Out-Null
    Start-Sleep -Seconds 1
    sc.exe delete $ServiceName | Out-Null
    Write-Host "Service removed: $ServiceName"
}

if (Test-Path $runner) {
    Remove-Item $runner -Force
    Write-Host "Deleted runner script: $runner"
}


