#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 post-delete crash-recovery contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 post-delete crash-recovery contract '$literal' in $path" >&2
        exit 1
    fi
}

integration="nereus-pulsar-adapter/src/f4M4IntegrationTest/java/com/nereusstream/pulsar/Phase4PhysicalGcOxiaS3IntegrationTest.java"
module_build="nereus-pulsar-adapter/build.gradle.kts"

for path in "$integration" "$module_build"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 post-delete crash-recovery artifact: $path" >&2
        exit 1
    fi
done

require_literal "processRestartAfterDeleteBeforeDeletedRootCasRecoversDurableDeletingIntent" "$integration"
require_literal "TargetDeleteTrackingObjectStore" "$integration"
require_literal "targetDeleteCompleted.set(true)" "$integration"
require_literal "PhysicalStoreDecorator.failBeforeDeletedRootCas(" "$integration"
require_literal "replacement.lifecycle() == PhysicalObjectLifecycle.DELETED" "$integration"
require_literal "targetDeleteCompleted.get()" "$integration"
require_literal "injected process death after target DELETE before DELETED root CAS" "$integration"
require_literal "crashObserved.get(30, TimeUnit.SECONDS)" "$integration"
require_literal "PhysicalObjectLifecycle.DELETING" "$integration"
require_literal "PhysicalObjectLifecycle.DELETED" "$integration"
require_literal "interrupted.assertObjectAbsent(target)" "$integration"
require_literal "recovered.assertObjectAbsent(target)" "$integration"
reject_literal "Thread.sleep(" "$integration"

require_literal "tasks.register<Test>(\"f4M4IntegrationTest\")" "$module_build"
require_literal "phase4M4PostDeleteCrashRecoveryCheck" "build.gradle.kts"
require_literal "Checkpoint AT" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4PostDeleteCrashRecoveryCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 real post-DELETE/pre-root-CAS process-death recovery contract surface: PASS"
