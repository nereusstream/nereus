#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 DELETED-CAS response-loss contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 DELETED-CAS response-loss contract '$literal' in $path" >&2
        exit 1
    fi
}

integration="nereus-pulsar-adapter/src/f4M4IntegrationTest/java/com/nereusstream/pulsar/Phase4PhysicalGcOxiaS3IntegrationTest.java"
coordinator="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/SourceRetirementCoordinator.java"

for path in "$integration" "$coordinator"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 DELETED-CAS response-loss artifact: $path" >&2
        exit 1
    fi
done

require_literal "lostDeletedRootCasResponseReloadsExactDurableReplacementWithoutRepeatedDelete" "$integration"
require_literal "PhysicalStoreDecorator.loseDeletedRootCasResponse(" "$integration"
require_literal "replacement.lifecycle() == PhysicalObjectLifecycle.DELETED" "$integration"
require_literal "targetDeleteCompleted.get()" "$integration"
require_literal "CompletableFuture<?> applied = (CompletableFuture<?>)" "$integration"
require_literal "invokeDelegate(raw, method, arguments)" "$integration"
require_literal "return applied.thenCompose(ignored ->" "$integration"
require_literal "injected response loss after successful DELETED root CAS" "$integration"
require_literal "exactDeletedReloadObserved.set(true)" "$integration"
require_literal "assertThat(targetDeleteCalls).hasValue(1)" "$integration"
require_literal "assertThat(restartedDeleteCalls).hasValue(0)" "$integration"
reject_literal "Thread.sleep(" "$integration"

require_literal "reload physical root after uncertain DELETED CAS" "$coordinator"
require_literal "exactReplacement(" "$coordinator"
require_literal "phase4M4DeletedCasResponseLossCheck" "build.gradle.kts"
require_literal "Checkpoint AU" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4DeletedCasResponseLossCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 real DELETED-root CAS response-loss convergence contract surface: PASS"
