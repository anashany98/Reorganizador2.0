param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("scan", "watch", "preview", "verify")]
    [string]$Command,

    [string]$Source,
    [string]$Dest,
    [ValidateSet("flat", "type", "date", "type-date", "hierarchical-type-ext", "project-type")]
    [string]$OrganizeBy = "type-date",
    [switch]$Move,
    [switch]$DryRun,
    [string]$HashAlgo,
    [switch]$NoIncremental,
    [switch]$NoVerify,
    [int]$Threads,
    [int]$Processes,
    [string]$CsvOut = "metadatos.csv",
    [string]$SqliteDb = "metadatos.db",
    [string]$SqlServerConn,
    [ValidateSet("rename", "overwrite", "skip", "overwrite-if-newer")]
    [string]$Conflict = "rename",
    [switch]$Dedup,
    [string]$Projects,
    [string]$ExcelOut,
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
$cpuCount = [Environment]::ProcessorCount

$autoHash = $false
if (-not $PSBoundParameters.ContainsKey("HashAlgo") -or [string]::IsNullOrWhiteSpace($HashAlgo)) {
    $HashAlgo = "sha256"
    $autoHash = $true
}

$autoThreads = $false
if (-not $PSBoundParameters.ContainsKey("Threads") -or $Threads -le 0) {
    $Threads = [Math]::Max(1, [Math]::Min(8, $cpuCount))
    $autoThreads = $true
}

$autoProcesses = $false
if (-not $PSBoundParameters.ContainsKey("Processes") -or $Processes -lt 0) {
    $Processes = [Math]::Max(1, [Math]::Min(4, $cpuCount))
    $autoProcesses = $true
}

if ($autoHash -or $autoThreads -or $autoProcesses) {
    Write-Host ("Ajustes automáticos -> Hash: {0} | Threads: {1} | Processes: {2} | Núcleos detectados: {3}" -f $HashAlgo, $Threads, $Processes, $cpuCount) -ForegroundColor Yellow
    Write-Host "La verificación de hashes permanece activa; usa -NoVerify solo si quieres deshabilitarla." -ForegroundColor Yellow
}

$arguments = @("-m", "reorganizador_v2.main", "--log-level", $LogLevel, $Command)

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
        if ($Conflict) { $arguments += @("--conflict", $Conflict) }
        if ($Dedup) { $arguments += "--dedup" }
        if ($Projects) { $arguments += @("--projects", $Projects) }
        if ($ExcelOut) { $arguments += @("--excel-out", $ExcelOut) }
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
    "preview" {
        if (-not $Source) { throw "Para 'preview' debes indicar --Source"; }
        if (-not (Test-Path $Source)) { throw "La carpeta origen '$Source' no existe."; }
        $arguments += @("--source", $Source, "--sqlite-db", $SqliteDb)
        if ($Projects) { $arguments += @("--projects", $Projects) }
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
$startTime = Get-Date
$exitCode = 0
try {
    & $python $arguments
    $exitCode = $LASTEXITCODE
}
finally {
    $elapsed = (Get-Date) - $startTime
    Write-Host ("Tiempo total: {0:hh\:mm\:ss}" -f $elapsed) -ForegroundColor Cyan
}

if ($exitCode -ne 0) {
    throw "La ejecucion termino con codigo $exitCode."
}
