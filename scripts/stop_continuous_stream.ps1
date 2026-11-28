$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$pidFile = Join-Path $repoRoot '.stream_pids'
$stopFile = Join-Path $repoRoot '.stream_stop'
if (-not (Test-Path -LiteralPath $pidFile)) { return }
try {
    $metadata = Get-Content -Raw -LiteralPath $pidFile | ConvertFrom-Json
} catch {
    $legacy = @(Get-Content -LiteralPath $pidFile)
    foreach ($entry in $legacy) {
        if ($entry -notmatch '^([0-9]+),(consumer|producer)$') {
            throw 'Unrecognized PID file. Inspect .stream_pids before starting a new worker.'
        }
        if (Get-Process -Id ([int]$Matches[1]) -ErrorAction SilentlyContinue) {
            throw 'A legacy worker PID is active. Inspect that process before migration.'
        }
    }
    Remove-Item -LiteralPath $pidFile
    Write-Output 'Removed stale legacy PID file.'
    return
}
$process = Get-Process -Id $metadata.pid -ErrorAction SilentlyContinue
if ($process) {
    $sameStart = $process.StartTime.ToUniversalTime().ToString('o') -eq $metadata.start_time
    $samePath = $process.Path -eq $metadata.executable
    if (-not ($sameStart -and $samePath)) {
        throw 'PID belongs to a different process. Worker was not stopped.'
    }
    New-Item -ItemType File -Path $stopFile -Force | Out-Null
    if (-not $process.WaitForExit(45000)) {
        throw 'Worker is still shutting down. Logs and PID file have been retained.'
    }
}
Remove-Item -LiteralPath $pidFile
if (Test-Path -LiteralPath $stopFile) { Remove-Item -LiteralPath $stopFile }
Write-Output 'Streaming worker stopped.'
