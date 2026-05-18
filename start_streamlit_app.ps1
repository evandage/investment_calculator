$ErrorActionPreference = "Stop"

$AppDir = "E:\investment_calculator"
$Python = Join-Path $AppDir ".venv\Scripts\python.exe"
$App = Join-Path $AppDir "app.py"
$Log = Join-Path $AppDir "streamlit_startup.log"
$ErrLog = Join-Path $AppDir "streamlit_startup.err.log"
$Port = 8501

Set-Location $AppDir

$alreadyListening = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($alreadyListening) {
    Add-Content -Path $Log -Value "$(Get-Date -Format s) Streamlit already listening on port $Port; skip startup."
    exit 0
}

if (!(Test-Path $Python)) {
    Add-Content -Path $Log -Value "$(Get-Date -Format s) Missing Python executable: $Python"
    exit 1
}

if (!(Test-Path $App)) {
    Add-Content -Path $Log -Value "$(Get-Date -Format s) Missing Streamlit app: $App"
    exit 1
}

Add-Content -Path $Log -Value "$(Get-Date -Format s) Starting Streamlit on port $Port."
Start-Process `
    -FilePath $Python `
    -ArgumentList @("-m", "streamlit", "run", $App, "--server.port", "$Port", "--server.headless", "true") `
    -WorkingDirectory $AppDir `
    -WindowStyle Hidden `
    -RedirectStandardOutput $Log `
    -RedirectStandardError $ErrLog
