#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build="$repo_root/build.gradle.kts"

require_literal() {
    local literal="$1"
    if ! rg -Fq -- "$literal" "$build"; then
        echo "missing Phase 4 final Docker-isolation contract '$literal' in build.gradle.kts" >&2
        exit 1
    fi
}

require_literal "DockerIntegrationGateService"
require_literal '"nereusDockerIntegrationGate"'
require_literal "maxParallelUsages.set(1)"
require_literal "usesService(dockerIntegrationGate)"

docker_tasks=(
    phase1IntegrationTest
    cursorS3IntegrationTest
    cursorM2IntegrationTest
    f4M2IntegrationTest
    f4M3IntegrationTest
    oxiaCapabilitySpike
    oxiaIntegrationTest
    f4OxiaIntegrationTest
    s3IntegrationTest
    f4M4IntegrationTest
    phase2PulsarFinalCheck
    phase3M5PulsarFinalCheck
    phase3M6PulsarFinalCheck
    phase4M4PhysicalGcMultiBrokerPulsarCheck
    phase4M5AsyncRetentionMultiBrokerPulsarCheck
    phase4M6TwoBrokerWorkerContentionPulsarCheck
)

for task in "${docker_tasks[@]}"; do
    require_literal "\"$task\""
done

require_literal 'tasks.register<Exec>("checkPhase4FinalDockerIsolation")'
require_literal 'dependsOn("checkPhase4FinalDockerIsolation")'

echo "Phase 4 final Docker-backed local and nested-Pulsar gates share one exclusive build service."
