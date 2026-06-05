$ErrorActionPreference = "Stop"

$AppDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = Join-Path $AppDir ".venv\Scripts\python.exe"
$BackendLog = Join-Path $AppDir "backend_startup.log"
$BackendErr = Join-Path $AppDir "backend_startup.err.log"
$FrontendLog = Join-Path $AppDir "frontend_startup.log"
$FrontendErr = Join-Path $AppDir "frontend_startup.err.log"

Set-Location $AppDir

if (!(Test-Path $Python)) {
    throw "Missing Python executable: $Python"
}

$backendListening = Get-NetTCPConnection -LocalPort 8010 -State Listen -ErrorAction SilentlyContinue
if (!$backendListening) {
    Start-Process `
        -FilePath $Python `
        -ArgumentList @("-m", "uvicorn", "backend.main:app", "--host", "127.0.0.1", "--port", "8010") `
        -WorkingDirectory $AppDir `
        -WindowStyle Hidden `
        -RedirectStandardOutput $BackendLog `
        -RedirectStandardError $BackendErr
}

$frontendListening = Get-NetTCPConnection -LocalPort 5173 -State Listen -ErrorAction SilentlyContinue
if (!$frontendListening) {
    Start-Process `
        -FilePath "cmd.exe" `
        -ArgumentList @("/c", "npm.cmd install && npm.cmd run dev -- --host 127.0.0.1 --port 5173") `
        -WorkingDirectory (Join-Path $AppDir "frontend") `
        -WindowStyle Hidden `
        -RedirectStandardOutput $FrontendLog `
        -RedirectStandardError $FrontendErr
}

Write-Host "Backend:  http://127.0.0.1:8010"
Write-Host "Frontend: http://127.0.0.1:5173"
