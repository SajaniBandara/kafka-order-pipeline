# Extracts the downloaded Kafka archive and formats a fresh KRaft storage dir.
# Run once. Safe to re-run (it re-formats -> wipes broker data).
$ErrorActionPreference = "Stop"

$root      = Split-Path -Parent $PSScriptRoot
$tools     = Join-Path $root "tools"
$archive   = Join-Path $tools "kafka.tgz"
$kafkaDir  = Join-Path $tools "kafka"
$logDir    = "D:\kafka-logs"   # short path on purpose - avoids Windows 260-char limit

if (-not (Test-Path $archive)) {
    throw "Missing $archive - download kafka_2.13-3.9.1.tgz into tools\kafka.tgz first."
}

if (-not (Test-Path $kafkaDir)) {
    Write-Host "Extracting Kafka..." -ForegroundColor Cyan
    New-Item -ItemType Directory -Force -Path $kafkaDir | Out-Null
    tar -xzf $archive -C $kafkaDir --strip-components=1
}

$props = Join-Path $kafkaDir "config\kraft\server.properties"
(Get-Content $props) `
    -replace '^log.dirs=.*', ("log.dirs=" + ($logDir -replace '\\','/')) `
    | Set-Content $props -Encoding ascii

if (Test-Path $logDir) { Remove-Item -Recurse -Force $logDir }
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$uuid = & "$kafkaDir\bin\windows\kafka-storage.bat" random-uuid
$uuid = $uuid.Trim()
Write-Host "Cluster ID: $uuid" -ForegroundColor Cyan
& "$kafkaDir\bin\windows\kafka-storage.bat" format -t $uuid -c $props

Write-Host "`nDone. Now run:  scripts\start-kafka.ps1" -ForegroundColor Green
