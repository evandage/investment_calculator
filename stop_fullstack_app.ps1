$ErrorActionPreference = "Stop"

$Services = @(
    @{ Name = "Backend"; Port = 8010 },
    @{ Name = "Frontend"; Port = 5173 }
)

function Stop-ServiceOnPort {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,

        [Parameter(Mandatory = $true)]
        [int]$Port
    )

    $connections = Get-NetTCPConnection `
        -LocalPort $Port `
        -State Listen `
        -ErrorAction SilentlyContinue

    $processIds = @(
        $connections |
            Where-Object { $_.OwningProcess -and $_.OwningProcess -ne 0 } |
            Select-Object -ExpandProperty OwningProcess -Unique
    )

    if ($processIds.Count -eq 0) {
        Write-Host "$Name is already stopped (port $Port is free)."
        return
    }

    foreach ($processId in $processIds) {
        $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
        if (!$process) {
            continue
        }

        Write-Host "Stopping $Name process $($process.ProcessName) (PID $processId) on port $Port..."
        Stop-Process -Id $processId -Force -ErrorAction Stop
    }

    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        $stillListening = Get-NetTCPConnection `
            -LocalPort $Port `
            -State Listen `
            -ErrorAction SilentlyContinue

        if (!$stillListening) {
            Write-Host "$Name stopped."
            return
        }

        Start-Sleep -Milliseconds 250
    }

    throw "$Name did not release port $Port."
}

foreach ($service in $Services) {
    Stop-ServiceOnPort -Name $service.Name -Port $service.Port
}

Write-Host "Investment dashboard services are stopped."
