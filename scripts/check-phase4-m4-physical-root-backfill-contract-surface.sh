#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 physical-root backfill $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 physical-root backfill contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    PhysicalRootBackfillStage.java
    PhysicalRootBackfillFailure.java
    PhysicalRootBackfillRequest.java
    PhysicalRootBackfillReport.java
    PhysicalRootBackfillCoordinator.java
    DefaultPhysicalRootBackfillCoordinator.java
    GenerationProjectionAuthorityReader.java
    GenerationProjectionAuthoritySnapshot.java
    ManagedLedgerGenerationProjectionRefV1.java
    ManagedLedgerGenerationProjectionAuthorityReader.java
)

test_artifacts=(
    PhysicalRootBackfillCoordinatorTest.java
    ManagedLedgerGenerationProjectionRefV1Test.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done
for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

coordinator="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/DefaultPhysicalRootBackfillCoordinator.java"
request="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/PhysicalRootBackfillRequest.java"
report="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/PhysicalRootBackfillReport.java"
projection_ref="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/generation/ManagedLedgerGenerationProjectionRefV1.java"
projection_reader="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/generation/ManagedLedgerGenerationProjectionAuthorityReader.java"
identities="nereus-core/src/main/java/com/nereusstream/core/append/GenerationZeroProtectionIdentities.java"
coordinator_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalRootBackfillCoordinatorTest.java"
projection_test="nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/generation/ManagedLedgerGenerationProjectionRefV1Test.java"

require_literal "maxConcurrentStreams must be in [1, 1024]" "$request"
require_literal "MAX_FAILURE_DETAILS = 100" "$report"
require_literal "scanStreamRegistrations(" "$coordinator"
require_literal "F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS" "$coordinator"
require_literal "readAppendRecoveryTail(" "$coordinator"
require_literal "generations.scanIndex(" "$coordinator"
require_literal "cursors.scanCursors(" "$coordinator"
require_literal "objectStore.headObject(" "$coordinator"
require_literal "protections.acquireOrTransfer(" "$coordinator"
require_literal "ObjectProtectionType.REACHABLE_APPEND" "$coordinator"
require_literal "ObjectProtectionType.VISIBLE_GENERATION" "$coordinator"
require_literal ".CURSOR_SNAPSHOT_ROOT" "$coordinator"
require_literal "finalRevalidate(" "$coordinator"
require_literal "publishProofs(" "$coordinator"
require_literal "streamRegistrationBackfill().complete()" "$coordinator"
require_literal "ACTIVATION_PROOF_CONFLICT" "$coordinator"
require_literal "activationHasProofs(" "$coordinator"

require_literal "public static String reachableAppendReferenceId(" "$identities"
require_literal "public static String visibleGenerationReferenceId(" "$identities"

require_literal 'VALUE_PREFIX = "nereus-ml-v1."' "$projection_ref"
require_literal "CRC32C" "$projection_ref"
require_literal "projectionIdentitySha256()" "$projection_ref"
require_literal "getProjectionByStream" "$projection_reader"
require_literal "ManagedLedgerFacadeState.OPEN" "$projection_reader"
require_literal "ManagedLedgerFacadeState.SEALED" "$projection_reader"

require_literal "emptyStableRegistryPublishesBothProofsAndRecoversLostCasResponse" "$coordinator_test"
require_literal "reachableCommitAndVisibleIndexCreateExactRootAndTwoOwnerProtections" "$coordinator_test"
require_literal "activeCursorSnapshotCreatesExactEtagRootAndPermanentOwnerProtection" "$coordinator_test"
require_literal "incompleteRegistrationCoverageFailsClosedWithoutScanningOrPublishing" "$coordinator_test"
require_literal "strictNpr1RoundTripAndIdentityDigestAreStable" "$projection_test"

require_literal "phase4M4PhysicalRootBackfillCheck" "build.gradle.kts"
require_literal "phase4M4PhysicalRootBackfillCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

if rg -Fq -- "listObjects(" "$repo_root/$coordinator"; then
    echo "live-reference physical-root backfill must not use object listing as coverage evidence" >&2
    exit 1
fi

revalidate_definition="$(rg -n -F 'private CompletableFuture<StreamBackfillResult> finalRevalidate' \
        "$repo_root/$coordinator" | cut -d: -f1)"
publish_definition="$(rg -n -F 'private CompletableFuture<Void> publishProofs' \
        "$repo_root/$coordinator" | cut -d: -f1)"
if [[ -z "$revalidate_definition" || -z "$publish_definition" \
        || "$revalidate_definition" -ge "$publish_definition" ]]; then
    echo "physical-root backfill must define final authority revalidation before activation-proof publication" >&2
    exit 1
fi

echo "Phase 4 M4 physical-root/cursor-root backfill and activation-proof closure verified."
