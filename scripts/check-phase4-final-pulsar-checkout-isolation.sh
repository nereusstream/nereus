#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build="$repo_root/build.gradle.kts"

require_literal() {
    local literal="$1"
    if ! rg -Fq -- "$literal" "$build"; then
        echo "missing Phase 4 final Pulsar-checkout isolation contract '$literal' in build.gradle.kts" >&2
        exit 1
    fi
}

require_literal "PulsarCheckoutGateService"
require_literal '"nereusPulsarCheckoutGate"'
require_literal "maxParallelUsages.set(1)"
require_literal "usesService(pulsarCheckoutGate)"

pulsar_tasks=(
    phase2PulsarCheck
    phase2PulsarFinalCheck
    phase3M4PulsarCheck
    phase3M5PulsarFinalCheck
    phase3M6PulsarFinalCheck
    phase4M4PhysicalGcConfigPulsarCheck
    phase4M4PhysicalDeletionActivationPulsarCheck
    phase4M4ReadinessRolloverPulsarCheck
    phase4M4PhysicalGcMultiBrokerPulsarCheck
    phase4M5GenerationCapabilityPulsarCheck
    phase4M5RegistrationBackfillPulsarCheck
    phase4M5ActivationGuardPulsarCheck
    phase4M5PublicationActivationPulsarCheck
    phase4M5RetentionRuntimePulsarCheck
    phase4M5RetentionPolicyAdminPulsarCheck
    phase4M5AsyncRetentionMultiBrokerPulsarCheck
    phase4M6TwoBrokerWorkerContentionPulsarCheck
)

for task in "${pulsar_tasks[@]}"; do
    require_literal "\"$task\""
done

declared_pulsar_tasks="$({
    LC_ALL=C perl -ne '
        if (/tasks\.register<Exec>\("([^"]+)"\)/) {
            $task = $1;
        }
        if (/workingDir = file\(pulsarCheckoutPath\.get\(\)\)/) {
            print "$task\n";
        }
    ' "$build"
} | LC_ALL=C sort)"
expected_pulsar_tasks="$(printf '%s\n' "${pulsar_tasks[@]}" | LC_ALL=C sort)"

if [[ "$declared_pulsar_tasks" != "$expected_pulsar_tasks" ]]; then
    echo "nested Pulsar Exec task inventory no longer matches pulsarCheckoutExecTasks" >&2
    echo "declared:" >&2
    printf '%s\n' "$declared_pulsar_tasks" >&2
    echo "expected:" >&2
    printf '%s\n' "$expected_pulsar_tasks" >&2
    exit 1
fi

require_literal 'tasks.register<Exec>("checkPhase4FinalPulsarCheckoutIsolation")'
require_literal 'dependsOn("checkPhase4FinalPulsarCheckoutIsolation")'

echo "All nested builds of the locked Pulsar checkout share one exclusive Gradle build service."
