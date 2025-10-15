param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("scan", "watch", "verify")]
    [string]$Command,

    [string]$Source,
    [string]$Dest,
    [string]$OrganizeBy = "type-date",
    [switch]$Move,
    [switch]$DryRun,
    [string]$HashAlgo = "sha256",
    [switch]$NoIncremental,
    [switch]$NoVerify,
    [int]$Threads = 4,
    [int]$Processes = 0,
    [string]$CsvOut = "metadatos.csv",
    [string]$SqliteDb = "metadatos.db",
    [string]$SqlServerConn,
    [int]$BatchSize = 50,
    [string]$LogLevel = "INFO"
)

function Resolve-Python {
    foreach ($candidate in @("python", "py")) {
        $cmd = Get-Command $candidate -ErrorAction SilentlyContinue
        if ($cmd) {
            return $cmd.Path
        }
    }
    throw "No se encontró un intérprete de Python (python/py). Instálalo o agrega su ruta al PATH."
}

$python = Resolve-Python
$arguments = @("-m", "extractor_v2.main", "--log-level", $LogLevel, $Command)

switch ($Command) {
    "scan" {
        if (-not $Source) { throw "Para 'scan' debes indicar --Source"; }
        if (-not (Test-Path $Source)) { throw "La carpeta origen '$Source' no existe."; }
        $arguments += @("--source", $Source)
        if ($Dest) { $arguments += @("--dest", $Dest) }
        $arguments += @("--organize-by", $OrganizeBy, "--hash-algo", $HashAlgo, "--threads", $Threads, "--csv-out", $CsvOut, "--sqlite-db", $SqliteDb, "--batch-size", $BatchSize)
        if ($Processes -gt 0) { $arguments += @("--processes", $Processes) }
        if ($Move) { $arguments += "--move" }
        if ($DryRun) { $arguments += "--dry-run" }
        if ($NoIncremental) { $arguments += "--no-incremental" }
        if ($NoVerify) { $arguments += "--no-verify" }
        if ($SqlServerConn) { $arguments += @("--sqlserver-conn", $SqlServerConn) }
    }
    "watch" {
        if (-not $Source) { throw "Para 'watch' debes indicar --Source"; }
        if (-not (Test-Path $Source)) { throw "La carpeta origen '$Source' no existe."; }
        $arguments += @("--source", $Source, "--organize-by", $OrganizeBy, "--hash-algo", $HashAlgo, "--threads", $Threads, "--csv-out", $CsvOut, "--sqlite-db", $SqliteDb)
        if ($Dest) { $arguments += @("--dest", $Dest) }
        if ($Processes -gt 0) { $arguments += @("--processes", $Processes) }
        if ($Move) { $arguments += "--move" }
        if ($SqlServerConn) { $arguments += @("--sqlserver-conn", $SqlServerConn) }
    }
    "verify" {
        if (-not $SqliteDb -and -not $CsvOut) {
            throw "Para 'verify' indica al menos --SqliteDb o --CsvOut."
        }
        if ($SqliteDb) { $arguments += @("--sqlite-db", $SqliteDb) }
        if ($CsvOut) { $arguments += @("--csv", $CsvOut) }
        if ($HashAlgo) { $arguments += @("--hash-algo", $HashAlgo) }
        $arguments += @("--threads", $Threads)
    }
}

Write-Host "Ejecutando: $python $($arguments -join ' ')" -ForegroundColor Cyan
& $python $arguments

if ($LASTEXITCODE -ne 0) {
    throw "La ejecución terminó con código $LASTEXITCODE."
}