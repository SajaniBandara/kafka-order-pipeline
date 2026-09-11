# Gracefully stops the local Kafka broker. Always use this (or Ctrl+C in the
# start-kafka.ps1 window) instead of closing the window or killing java.exe --
# a hard kill leaves an unclean shutdown marker and can corrupt D:\kafka-logs,
# forcing a full scripts\setup-kafka.ps1 re-format.
$ErrorActionPreference = "Stop"
$root     = Split-Path -Parent $PSScriptRoot
$kafkaDir = Join-Path $root "tools\kafka"

& "$kafkaDir\bin\windows\kafka-server-stop.bat"

Write-Host "Waiting for the broker process to exit..." -ForegroundColor Cyan
$deadline = (Get-Date).AddSeconds(30)
while ((Get-Date) -lt $deadline) {
    $proc = Get-CimInstance Win32_Process -Filter "Name='java.exe'" |
        Where-Object { $_.CommandLine -like "*kraft*" -or $_.CommandLine -like "*Kafka*" }
    if (-not $proc) { Write-Host "Broker stopped cleanly." -ForegroundColor Green; exit 0 }
    Start-Sleep -Seconds 1
}
Write-Host "Broker still running after 30s - check the start-kafka.ps1 window." -ForegroundColor Yellow
