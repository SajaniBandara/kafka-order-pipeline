# Starts the local single-node Kafka broker (KRaft mode). Keep this window open.
$ErrorActionPreference = "Stop"
$root     = Split-Path -Parent $PSScriptRoot
$kafkaDir = Join-Path $root "tools\kafka"
$props    = Join-Path $kafkaDir "config\kraft\server.properties"

if (-not (Test-Path $props)) {
    throw "Kafka not set up yet. Run scripts\setup-kafka.ps1 first."
}

& "$kafkaDir\bin\windows\kafka-server-start.bat" $props
