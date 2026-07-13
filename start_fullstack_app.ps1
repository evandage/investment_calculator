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

function Stop-ServiceOnPort {
    param([int]$Port)

    $connections = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    $processIds = $connections |
        Where-Object { $_.OwningProcess -and $_.OwningProcess -ne 0 } |
        Select-Object -ExpandProperty OwningProcess -Unique

    foreach ($processId in $processIds) {
        $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
        if ($process) {
            Write-Host "Stopping process $($process.ProcessName) (PID $processId) on port $Port..."
            Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
        }
    }

    # Wait briefly until the port is released before starting the replacement.
    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        $stillListening = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
        if (!$stillListening) {
            return
        }
        Start-Sleep -Milliseconds 250
    }

    throw "Port $Port is still in use after stopping the existing process."
}

# Restart both services every time this script is run.
Stop-ServiceOnPort -Port 8010
Stop-ServiceOnPort -Port 5173

Start-Process `
    -FilePath $Python `
    -ArgumentList @("-m", "uvicorn", "backend.main:app", "--host", "127.0.0.1", "--port", "8010") `
    -WorkingDirectory $AppDir `
    -WindowStyle Hidden `
    -RedirectStandardOutput $BackendLog `
    -RedirectStandardError $BackendErr

Start-Process `
    -FilePath "cmd.exe" `
    -ArgumentList @("/c", "npm.cmd install && npm.cmd run dev -- --host 127.0.0.1 --port 5173") `
    -WorkingDirectory (Join-Path $AppDir "frontend") `
    -WindowStyle Hidden `
    -RedirectStandardOutput $FrontendLog `
    -RedirectStandardError $FrontendErr

Write-Host "Backend:  http://127.0.0.1:8010"
Write-Host "Frontend: http://127.0.0.1:5173"
