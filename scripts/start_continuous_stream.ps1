param (
    [ValidateScript({ $_ -gt 0 -and -not [double]::IsInfinity($_) })]
    [double]$Rate = 15,
    [ValidateRange(1, 2147483647)][int]$BatchSize = 100,
    [ValidateScript({ $_ -gt 0 -and -not [double]::IsInfinity($_) })]
    [double]$FlushInterval = 5
)
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot '.venv/Scripts/python.exe'
$worker = Join-Path $PSScriptRoot 'run_stream_worker.py'
$pidFile = Join-Path $repoRoot '.stream_pids'
$stopFile = Join-Path $repoRoot '.stream_stop'
if (Test-Path -LiteralPath $pidFile) {
    & (Join-Path $PSScriptRoot 'stop_continuous_stream.ps1')
}
if (Test-Path -LiteralPath $stopFile) { Remove-Item -LiteralPath $stopFile }
$workerArgs = @(
    ('"{0}"' -f $worker)
    '--rate', $Rate.ToString([Globalization.CultureInfo]::InvariantCulture)
    '--batch-size', $BatchSize
    '--flush-interval', $FlushInterval.ToString([Globalization.CultureInfo]::InvariantCulture)
    '--stop-file', ('"{0}"' -f $stopFile)
)
$process = Start-Process -FilePath $python -ArgumentList $workerArgs -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $repoRoot 'stream.log') -RedirectStandardError (Join-Path $repoRoot 'stream_error.log')
@{ pid = $process.Id; start_time = $process.StartTime.ToUniversalTime().ToString('o'); executable = $python; worker = $worker } | ConvertTo-Json | Set-Content -LiteralPath $pidFile
Start-Sleep -Seconds 2
$process.Refresh()
if ($process.HasExited) { throw 'Streaming worker exited. Inspect stream_error.log.' }
Write-Output "Streaming worker started with PID $($process.Id)"
