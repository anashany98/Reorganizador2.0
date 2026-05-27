param(
    [string]$ListenHost = "127.0.0.1",
    [int]$Port = 8000,
    [switch]$AutoPort
)

function Resolve-Python {
    foreach ($candidate in @("python", "py")) {
        $command = Get-Command $candidate -ErrorAction SilentlyContinue
        if ($command) {
            return $candidate
        }
    }

    throw "No se encontro un interprete de Python (python/py)."
}

function Resolve-ListenAddress([string]$HostName) {
    switch ($HostName) {
        "127.0.0.1" { return [System.Net.IPAddress]::Loopback }
        "localhost" { return [System.Net.IPAddress]::Loopback }
        "0.0.0.0" { return [System.Net.IPAddress]::Any }
        default { return [System.Net.IPAddress]::Parse($HostName) }
    }
}

function Test-PortAvailable([string]$HostName, [int]$PortNumber) {
    $address = Resolve-ListenAddress $HostName
    $listener = [System.Net.Sockets.TcpListener]::new($address, $PortNumber)

    try {
        $listener.Start()
        return $true
    }
    catch {
        return $false
    }
    finally {
        $listener.Stop()
    }
}

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Resolve-Python

if (-not $PSBoundParameters.ContainsKey("Port")) {
    $AutoPort = $true
}

if ($AutoPort) {
    $originalPort = $Port
    while (-not (Test-PortAvailable -HostName $ListenHost -PortNumber $Port)) {
        $Port++
    }

    if ($Port -ne $originalPort) {
        Write-Host ("Puerto {0} ocupado. Usando {1}." -f $originalPort, $Port) -ForegroundColor Yellow
    }
}
elseif (-not (Test-PortAvailable -HostName $ListenHost -PortNumber $Port)) {
    throw "El puerto $Port ya esta en uso. Ejecuta el script con -AutoPort o indica otro valor en -Port."
}

$env:PYTHONPATH = $projectRoot
$env:REORGANIZADOR_HOST = $ListenHost
$env:REORGANIZADOR_PORT = "$Port"

Push-Location $projectRoot
try {
    Write-Host ("Lanzando Reorganizador 2.0 en http://{0}:{1}" -f $ListenHost, $Port) -ForegroundColor Cyan
    & $python -B -m uvicorn --app-dir $projectRoot web.server:app --host $ListenHost --port $Port
}
finally {
    Pop-Location
}
