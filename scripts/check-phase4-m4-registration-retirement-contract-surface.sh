#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 registration-retirement contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 registration-retirement contract '$literal' in $path" >&2
        exit 1
    fi
}

require_ordered() {
    local path="$1"
    shift
    local previous=0
    local literal
    for literal in "$@"; do
        local line
        line="$(rg -n -F -- "$literal" "$repo_root/$path" | head -n 1 | cut -d: -f1)"
        if [[ -z "$line" || "$line" -le "$previous" ]]; then
            echo "Phase 4 M4 registration-retirement ordering is missing or invalid at '$literal' in $path" >&2
            exit 1
        fi
        previous="$line"
    done
}

coordinator="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/StreamRegistrationRetirementCoordinator.java"
result="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/StreamRegistrationRetirementResult.java"
status="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/StreamRegistrationRetirementStatus.java"
authority_reader="nereus-core/src/main/java/com/nereusstream/core/capability/StreamRetirementReferenceAuthorityReader.java"
authority_snapshot="nereus-core/src/main/java/com/nereusstream/core/capability/StreamRetirementReferenceAuthoritySnapshot.java"
managed_reader="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/retention/ManagedLedgerStreamRetirementAuthorityReader.java"
coordinator_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/StreamRegistrationRetirementCoordinatorTest.java"
workflow_test="nereus-materialization/src/test/java/com/nereusstream/materialization/StreamRegistrationRetirementWorkflowTest.java"
recovery_test="nereus-materialization/src/test/java/com/nereusstream/materialization/recovery/RecoveryCheckpointCoordinatorTest.java"
managed_test="nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/retention/ManagedLedgerStreamRetirementAuthorityReaderTest.java"

for path in \
    "$coordinator" \
    "$result" \
    "$status" \
    "$authority_reader" \
    "$authority_snapshot" \
    "$managed_reader" \
    "$coordinator_test" \
    "$workflow_test" \
    "$recovery_test" \
    "$managed_test"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 registration-retirement artifact: $path" >&2
        exit 1
    fi
done

require_literal "CompletableFuture<StreamRetirementReferenceAuthoritySnapshot> capture(" "$authority_reader"
require_literal "referenceFree != (complete && liveReferenceCount == 0)" "$authority_snapshot"
require_literal "a complete stream-retirement snapshot must retain every authority" "$authority_snapshot"

require_literal "ManagedLedgerGenerationProjectionRefV1.from(" "$managed_reader"
require_literal "metadata.getRetention(cluster, exact.streamId())" "$managed_reader"
require_literal "metadata.scanCursors(" "$managed_reader"
require_literal "CursorRetentionLifecycle.ACTIVE" "$managed_reader"
require_literal "CursorRecordLifecycle.DELETED" "$managed_reader"
require_literal "accumulator.authorityCount >= maxAuthorities" "$managed_reader"
require_literal "if (!second.equals(first))" "$managed_reader"

require_literal "if (!config.enabled())" "$coordinator"
require_literal "if (config.dryRun())" "$coordinator"
require_literal "state != StreamState.DELETED" "$coordinator"
require_literal "capture.projection().live()" "$coordinator"
require_literal "!capture.external().complete()" "$coordinator"
require_literal "!capture.external().referenceFree()" "$coordinator"
require_literal "requireEmptyRecoveryTail" "$coordinator"
require_literal "config.maxAuthoritiesPerDomainSnapshot()" "$coordinator"
require_literal "config.maxReferencesPerDomainSnapshot()" "$coordinator"
require_literal "metadataAuditGraceMillis" "$coordinator"
require_literal "generations.deleteIndex(" "$coordinator"
require_literal "generations.deleteTask(" "$coordinator"
require_literal "generations.deleteRecoveryRoot(" "$coordinator"
require_literal "generations.deleteMaterializationCheckpoint(" "$coordinator"
require_literal "generations.deleteRangeRetentionStats(" "$coordinator"
require_literal "generations.deleteSequence(" "$coordinator"
require_literal "generations.deleteStreamRegistration(" "$coordinator"
require_literal '"reload registration after uncertain delete"' "$coordinator"
reject_literal "deleteObject(" "$coordinator"
reject_literal "deleteRoot(" "$coordinator"

require_ordered \
    "$coordinator" \
    "retireProtectionKind(capture, scope, OwnerKind.INDEX, 0)" \
    "retireIndexes(workflow.indexes(), 0)" \
    "retireProtectionKind(capture, scope, OwnerKind.TASK, 0)" \
    "retireTasks(workflow.tasks(), 0)" \
    "retireProtectionKind(capture, scope, OwnerKind.RECOVERY_ROOT, 0)" \
    "deleteRecoveryRoot(recoveryRoot, 0)" \
    "requireAllStreamMetadataEmpty()" \
    "return revalidateCapture(capture).thenCompose" \
    "return deleteRegistration(capture.registration(), 0)"

require_literal "only RETIRED may report registration removal" "$result"
require_literal "RECOVERY_TAIL_PRESENT" "$status"
require_literal "AUDIT_GRACE_PENDING" "$status"
require_literal "LIMIT_EXCEEDED" "$status"
require_literal "VERSION_CHANGED" "$status"

require_literal "lostRegistrationDeleteResponseConvergesFromExactAbsence" "$coordinator_test"
require_literal "emptyRecoveryRootDeleteResponseLossConvergesBeforeRegistrationRetirement" "$coordinator_test"
require_literal "finalExternalAuthorityDriftRetainsRegistration" "$coordinator_test"
require_literal "nonTerminalTaskAndLiveIndexBlockWithoutMutation" "$workflow_test"
require_literal "auditGraceBlocksTerminalTaskBeforeAnyOwnerRetirement" "$workflow_test"
require_literal "retiresPublishedWorkflowOwnersAcrossDeleteResponseLossButKeepsPhysicalRoots" "$workflow_test"
require_literal "registrationRetirementDrainsNonEmptyCheckpointRootAcrossDeleteResponseLoss" "$recovery_test"
require_literal "exactDeletedCursorInventoryIsReferenceFreeAndRecapturable" "$managed_test"
require_literal "authorityLimitAndTransitionalRetentionFailClosed" "$managed_test"

require_literal "phase4M4RegistrationRetirementCheck" "build.gradle.kts"
require_literal "phase4M4RegistrationRetirementCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "Checkpoint AM" "docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M4 registration-retirement authority, ordering, and response-loss surfaces verified."
