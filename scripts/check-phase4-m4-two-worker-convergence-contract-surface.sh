#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 two-worker convergence contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 two-worker convergence contract '$literal' in $path" >&2
        exit 1
    fi
}

integration="nereus-pulsar-adapter/src/f4M4IntegrationTest/java/com/nereusstream/pulsar/Phase4PhysicalGcOxiaS3IntegrationTest.java"
collector="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/PhysicalObjectGarbageCollector.java"

for path in "$integration" "$collector"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 two-worker convergence artifact: $path" >&2
        exit 1
    fi
done

require_literal "twoIndependentWorkersConvergeConcurrentDeletingIntentAndExactDeletes" "$integration"
require_literal "DeletingRootCasRace" "$integration"
require_literal "TargetDeleteRace" "$integration"
require_literal "PhysicalStoreDecorator.raceDeletingRootCas(" "$integration"
require_literal "ConcurrentTargetDeleteObjectStore" "$integration"
require_literal "deletingRace.bothArrived().get(30, TimeUnit.SECONDS)" "$integration"
require_literal "deleteRace.bothArrived().get(30, TimeUnit.SECONDS)" "$integration"
require_literal "assertThat(deletingRace.attempts()).hasValue(2)" "$integration"
require_literal "assertThat(deletingRace.successes()).hasValue(1)" "$integration"
require_literal "assertThat(deletingRace.failures()).hasValue(1)" "$integration"
require_literal "assertThat(deleteRace.attempts()).hasValue(2)" "$integration"
require_literal "assertThat(deleteRace.completions()).hasValue(2)" "$integration"
require_literal "assertThat(workerB.root(target)).isEqualTo(workerA.root(target))" "$integration"
require_literal "PhysicalObjectLifecycle.DELETED" "$integration"
reject_literal "Thread.sleep(" "$integration"

require_literal "reload physical root after uncertain delete-intent CAS" "$collector"
require_literal "exactReplacement(reloaded.orElseThrow(), replacement)" "$collector"
require_literal "phase4M4TwoWorkerConvergenceCheck" "build.gradle.kts"
require_literal "Checkpoint AV" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4TwoWorkerConvergenceCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 real two-worker DELETING-intent convergence contract surface: PASS"
