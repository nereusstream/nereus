#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 cursor-GC execution contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 cursor-GC execution contract '$literal' in $path" >&2
        exit 1
    fi
}

scanner="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/retention/CursorSnapshotGcScanner.java"
collector="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/PhysicalObjectGarbageCollector.java"
executor="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/CursorSnapshotGcExecutor.java"
assembly="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4GcReferenceDomainAssembly.java"
runtime="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4PhysicalGcRuntime.java"
provider="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/DefaultNereusRuntimeProvider.java"
configuration="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/NereusRuntimeConfiguration.java"
owner="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/NereusManagedLedgerRuntime.java"
scanner_test="nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/retention/CursorSnapshotGcScannerTest.java"
collector_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalObjectGarbageCollectorTest.java"
executor_test="nereus-pulsar-adapter/src/test/java/com/nereusstream/pulsar/CursorSnapshotGcExecutorTest.java"

for path in \
    "$scanner" \
    "$collector" \
    "$executor" \
    "$assembly" \
    "$runtime" \
    "$provider" \
    "$configuration" \
    "$owner" \
    "$scanner_test" \
    "$collector_test" \
    "$executor_test"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 cursor-GC execution artifact: $path" >&2
        exit 1
    fi
done

require_literal "recoverMarked(" "$scanner"
require_literal "activeLifecycleEpoch(" "$scanner"
require_literal "VersionedPhysicalObjectRoot sourceRoot" "$scanner"
require_literal "sameCandidateRoot(" "$scanner"
reject_literal "number(digest, root.metadataVersion());" "$scanner"
reject_literal "number(digest, discoveredAtMillis);" "$scanner"

require_literal "unmarkDrifted(" "$collector"
require_literal "CAS unreconstructable MARKED physical root back to ACTIVE" "$collector"
require_literal "garbageCollector.mark(" "$executor"
require_literal "advanceToDeleteIntent(" "$executor"
require_literal "scanner.revalidate(candidate)" "$executor"
require_literal "sourceRetirement.resume(" "$executor"
require_literal "GcPlan.fromMarkedRoot(" "$executor"
require_literal "recoverDeleting(" "$executor"

require_literal "new AppendRecoveryReferenceDomain(" "$runtime"
require_literal "new CursorSnapshotReferenceDomain(" "$runtime"
require_literal "new FutureCatalogSentinelDomain(" "$runtime"
require_literal "new GenerationReferenceDomain(" "$runtime"
require_literal "new MaterializationReferenceDomain(" "$runtime"
require_literal "Phase4GcReferenceDomainAssembly referenceDomains" "$runtime"
require_literal "exactReferenceDomains.projectionDomain()" "$runtime"
require_literal "exactProjectionReferenceDomain);" "$runtime"
require_literal "new ProjectionGenerationReferenceDomain(" "$assembly"
require_literal "new GenerationZeroIndexRetirementHandler(" "$runtime"
require_literal "new GenerationZeroCommitRetirementHandler(" "$runtime"
require_literal "new GenerationZeroMarkerRetirementHandler(" "$runtime"
require_literal "new HigherGenerationIndexRetirementHandler(" "$runtime"
require_literal "AbandonedAppendIntentPlanBuilder abandonedAppendIntents" "$runtime"
require_literal "OWNERLESS_ORPHAN_CANDIDATE) {" "$runtime"
require_literal "abandonedAppendIntents.reload(candidate, expected)" "$runtime"
require_literal "runtime-managed cursor GC accepts only empty metadata-removal plans" "$runtime"
reject_literal "runtime-managed cursor/ownerless GC accepts only empty metadata-removal plans" "$runtime"
reject_literal ".scheduleAtFixedRate(" "$runtime"
reject_literal ".scheduleWithFixedDelay(" "$runtime"

require_literal "PhysicalGcConfig physicalGc" "$configuration"
require_literal "PhysicalGcConfig.defaults()" "$configuration"
require_literal "physicalGc.validateAgainst(streamStorage, materialization)" "$configuration"
require_literal "new Phase4PhysicalGcRuntime(" "$provider"
require_literal "Phase4GcReferenceDomainAssembly.create(" "$provider"
require_literal "gcReferenceDomains," "$provider"
require_literal "physicalGcRuntime" "$owner"
require_literal "closeOneIfPresent(physicalGcRuntime, failures)" "$owner"

require_literal "reconstructsTheExactCandidateEvidenceFromMarkedRootAfterRestart" "$scanner_test"
require_literal "restartCanConditionallyUnmarkAnUnreconstructableExactRoot" "$collector_test"
require_literal "markedRestartReconstructsThePlanAndCompletesExactDeletion" "$executor_test"
require_literal "changedRestartEvidenceUnmarksWithoutDeletingBytes" "$executor_test"

require_literal "phase4M4CursorGcExecutionCheck" "build.gradle.kts"
require_literal "phase4M4CursorGcExecutionCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "Checkpoint AK" "docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M4 cursor-GC execution contract surface: PASS"
