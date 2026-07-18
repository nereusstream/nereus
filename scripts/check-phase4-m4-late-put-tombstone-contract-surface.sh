#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 late-PUT/tombstone contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 late-PUT/tombstone contract '$literal' in $path" >&2
        exit 1
    fi
}

metadata_api="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaMetadataStore.java"
metadata_impl="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaJavaClientMetadataStore.java"
publisher="nereus-core/src/main/java/com/nereusstream/core/append/DefaultGenerationZeroPhysicalReferencePublisher.java"
coordinator="nereus-core/src/main/java/com/nereusstream/core/append/AppendCoordinator.java"
wal_writer="nereus-object-store/src/main/java/com/nereusstream/objectstore/wal/DefaultWalObjectWriter.java"
wal_test="nereus-core/src/test/java/com/nereusstream/core/append/ObjectWalGuardedUploadTest.java"
tombstone_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalRootTombstoneRetirementTest.java"
late_put_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/LatePutAfterTombstoneTest.java"
inventory="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/ObjectInventoryScanner.java"
integration="nereus-pulsar-adapter/src/f4M4IntegrationTest/java/com/nereusstream/pulsar/Phase4PhysicalGcOxiaS3IntegrationTest.java"

for path in \
    "$metadata_api" \
    "$metadata_impl" \
    "$publisher" \
    "$coordinator" \
    "$wal_writer" \
    "$wal_test" \
    "$tombstone_test" \
    "$late_put_test" \
    "$inventory" \
    "$integration"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 late-PUT/tombstone artifact: $path" >&2
        exit 1
    fi
done

require_literal "CompletableFuture<Void> revalidateAppendSession(" "$metadata_api"
require_literal "AppendSession session" "$metadata_api"
require_literal "current.leaseVersion() < expected.leaseVersion()" "$metadata_impl"
require_literal "metadata.revalidateAppendSession(cluster, expectedSession)" "$publisher"
require_literal "requireUploadRoot(expectedObject, optional)" "$publisher"
require_literal "physicalReferences.authorizeUpload(" "$coordinator"
require_literal "result.objectKey(), source, options, attemptGuard" "$wal_writer"

revalidation_count="$(rg -Fc -- \
    "metadata.revalidateAppendSession(cluster, expectedSession)" \
    "$repo_root/$publisher")"
if [[ "$revalidation_count" -ne 2 ]]; then
    echo "Object-WAL upload authorization must revalidate the append session on both sides of the root read" >&2
    exit 1
fi

require_literal \
    "providerRetryRevalidatesSessionAndRejectsDeletedRootBeforeSecondTransmission" \
    "$wal_test"
require_literal \
    "expiredSessionRejectsFirstAttemptAfterRootTombstoneAndFreshAppendUsesFreshKey" \
    "$wal_test"
require_literal "providerTransmissions()).isEqualTo(1)" "$wal_test"
require_literal "providerTransmissions()).isZero()" "$wal_test"

require_literal "lateExactPutIsDeletedUnderFreshOwnerlessProofAndRestartsTheWindow" "$tombstone_test"
require_literal "mismatchedReappearingBytesAreQuarantinedWithoutDelete" "$tombstone_test"
require_literal "lostLateDeleteResponseConvergesOnExactHeadAbsenceButKeepsTheRoot" "$late_put_test"
require_literal "ownerAppearingBeforeProviderDeleteInvalidatesTheLatePutAttempt" "$late_put_test"
require_literal "exactPutAfterReferenceRetirementIsDeletedBeforeRootRetirement" "$late_put_test"
require_literal "exactPutAfterManifestRetirementIsDeletedBeforeRootRetirement" "$late_put_test"
require_literal "containsExactly(\"references\", \"manifest\")" "$late_put_test"

require_literal "config.orphanGrace().toMillis()" "$inventory"
require_literal "config.maximumClockSkew().toMillis()" "$inventory"
require_literal "register inventory-discovered physical root" "$inventory"
require_literal \
    "externallyReappearingBytesAfterRootRetirementReenterOwnerlessInventoryAndGc" \
    "$integration"
require_literal "recreateExactOwnerlessObject(target)" "$integration"
require_literal "registrationPass.inventory().rootsRegistered()).isEqualTo(1)" "$integration"
require_literal "reentered.value().orphanNotBeforeMillis()" "$integration"
reject_literal "Thread.sleep(" "$wal_test"
reject_literal "Thread.sleep(" "$late_put_test"
reject_literal "Thread.sleep(" "$integration"

require_literal "phase4M4LatePutTombstoneCheck" "build.gradle.kts"
require_literal "Checkpoint BB" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4LatePutTombstoneCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 guarded first/retried PUT, tombstone cuts, and external reappearance contract surface: PASS"
