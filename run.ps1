<#
.SYNOPSIS
    Build rkv, start the server, and open the desktop GUI.

.DESCRIPTION
    Closing the GUI stops the server this script started. If something is
    already listening on -Addr, the script reuses it and leaves it running.

.EXAMPLE
    .\run.ps1                                   # in-memory server + GUI
    .\run.ps1 -Wal data.wal                     # durable, fsync once per second
    .\run.ps1 -Wal data.wal -Fsync always -Store sharded
    .\run.ps1 -NoGui                            # server only, Ctrl+C to stop
#>
param(
    [string]$Addr = "127.0.0.1:6380",
    [ValidateSet("mutex", "rwlock", "sharded")]
    [string]$Store = "mutex",
    [string]$Wal = "",
    [ValidateSet("always", "every-sec", "never")]
    [string]$Fsync = "every-sec",
    [switch]$NoGui
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# cargo usually isn't on PATH in a fresh shell.
$cargoBin = Join-Path $env:USERPROFILE ".cargo\bin"
if (-not (Get-Command cargo -ErrorAction SilentlyContinue) -and (Test-Path $cargoBin)) {
    $env:PATH = "$cargoBin;$env:PATH"
}

function Invoke-Cargo([string[]]$CargoArgs) {
    & cargo @CargoArgs
    if ($LASTEXITCODE -ne 0) { throw "cargo $($CargoArgs -join ' ') failed" }
}

function Test-Port([string]$Address) {
    $hostName, $port = $Address -split ":(?=\d+$)"
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $client.ConnectAsync($hostName, [int]$port).Wait(200) -and $client.Connected
    } catch {
        $false
    } finally {
        $client.Dispose()
    }
}

# Build everything first, so the wait below never races a compile.
Write-Host "Building server..." -ForegroundColor Cyan
Invoke-Cargo @("build", "--release", "--bin", "rkv")
if (-not $NoGui) {
    Write-Host "Building GUI..." -ForegroundColor Cyan
    Invoke-Cargo @("build", "--release", "--manifest-path", "gui/Cargo.toml")
}

$server = $null
if (Test-Port $Addr) {
    Write-Host "Something is already listening on $Addr; using it." -ForegroundColor Yellow
} else {
    $serverArgs = @("--addr", $Addr, "--store", $Store, "--fsync", $Fsync)
    if ($Wal) { $serverArgs += @("--wal", $Wal) }
    Write-Host "Starting rkv $($serverArgs -join ' ')" -ForegroundColor Cyan
    $server = Start-Process -FilePath "target\release\rkv.exe" -ArgumentList $serverArgs `
        -NoNewWindow -PassThru

    # Bound after WAL replay, so a big log can take a moment.
    $deadline = (Get-Date).AddSeconds(30)
    while (-not (Test-Port $Addr)) {
        if ($server.HasExited) { throw "server exited with code $($server.ExitCode)" }
        if ((Get-Date) -gt $deadline) { throw "server did not start listening on $Addr" }
        Start-Sleep -Milliseconds 100
    }
}

try {
    if ($NoGui) {
        Write-Host "Server running on $Addr. Press Ctrl+C to stop." -ForegroundColor Green
        if ($server) { $server.WaitForExit() }
    } else {
        Write-Host "Opening GUI (close the window to stop)." -ForegroundColor Green
        $env:RKV_ADDR = $Addr
        & "gui\target\release\rkv-gui.exe"
    }
} finally {
    if ($server -and -not $server.HasExited) {
        Write-Host "Stopping server." -ForegroundColor Cyan
        Stop-Process -Id $server.Id
    }
}
