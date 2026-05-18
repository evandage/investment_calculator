$ErrorActionPreference = "Stop"

$AppDir = "E:\investment_calculator"
$Python = Join-Path $AppDir ".venv\Scripts\python.exe"
$App = Join-Path $AppDir "app.py"
$Log = Join-Path $AppDir "streamlit_lan.log"
$ErrLog = Join-Path $AppDir "streamlit_lan.err.log"
$Port = 8501

Set-Location $AppDir

$lanIps = Get-NetIPAddress -AddressFamily IPv4 |
    Where-Object {
        $_.IPAddress -notlike "127.*" -and
        $_.IPAddress -notlike "169.254.*" -and
        $_.PrefixOrigin -ne "WellKnown"
    } |
    Select-Object -ExpandProperty IPAddress

if (!(Test-Path $Python)) {
    Write-Host "Missing Python executable: $Python"
    exit 1
}

if (!(Test-Path $App)) {
    Write-Host "Missing Streamlit app: $App"
    exit 1
}

$alreadyListening = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($alreadyListening) {
    Write-Host "Port $Port is already listening. If this is your Streamlit app, open one of:"
    foreach ($ip in $lanIps) {
        Write-Host "  http://$ip`:$Port"
    }
    exit 0
}

Write-Host "Starting Streamlit for LAN access on port $Port..."
Write-Host "After it starts, open from another device on the same Wi-Fi/LAN:"
foreach ($ip in $lanIps) {
    Write-Host "  http://$ip`:$Port"
}

Add-Content -Path $Log -Value "$(Get-Date -Format s) Starting Streamlit LAN server on 0.0.0.0:$Port."
Start-Process `
    -FilePath $Python `
    -ArgumentList @("-m", "streamlit", "run", $App, "--server.address", "0.0.0.0", "--server.port", "$Port", "--server.headless", "true") `
    -WorkingDirectory $AppDir `
    -RedirectStandardOutput $Log `
    -RedirectStandardError $ErrLog
