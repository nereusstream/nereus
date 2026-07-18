#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
object_store="$repo_root/nereus-object-store/src/main/java/com/nereusstream/objectstore/ObjectStore.java"
s3_store="$repo_root/nereus-object-store/src/main/java/com/nereusstream/objectstore/S3CompatibleObjectStore.java"
guard_test="$repo_root/nereus-object-store/src/test/java/com/nereusstream/objectstore/GuardedPutObjectAttemptContractTest.java"
wal_writer="$repo_root/nereus-object-store/src/main/java/com/nereusstream/objectstore/wal/DefaultWalObjectWriter.java"
wal_adapter="$repo_root/nereus-core/src/main/java/com/nereusstream/core/wal/object/ObjectWalAppenderAdapter.java"
append_coordinator="$repo_root/nereus-core/src/main/java/com/nereusstream/core/append/AppendCoordinator.java"
physical_publisher="$repo_root/nereus-core/src/main/java/com/nereusstream/core/append/DefaultGenerationZeroPhysicalReferencePublisher.java"
metadata_store="$repo_root/nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaMetadataStore.java"
wal_guard_test="$repo_root/nereus-core/src/test/java/com/nereusstream/core/append/ObjectWalGuardedUploadTest.java"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing guarded object PUT contract '$literal' in ${path#"$repo_root/"}" >&2
        exit 1
    fi
}

require_literal "PutObjectAttemptGuard attemptGuard" "$object_store"
require_literal "PutObjectAttemptGuard attemptGuard" "$s3_store"
require_literal "attemptGuard.authorize(key, attemptNumber)" "$s3_store"
require_literal "transmitPut(key, source, options, remaining)" "$s3_store"
require_literal "guardRunsImmediatelyBeforeEveryOwnedProviderRetry" "$guard_test"
require_literal "failedRetryGuardSendsNoSecondAttemptAndPreservesFenceFailure" "$guard_test"
require_literal "failedInitialGuardNeverOpensUploadOrCallsProvider" "$guard_test"
require_literal "PutObjectAttemptGuard attemptGuard" "$wal_writer"
require_literal "result.objectKey(), source, options, attemptGuard" "$wal_writer"
require_literal "PutObjectAttemptGuard attemptGuard" "$wal_adapter"
require_literal "writer.upload(prepared.preparedObject(), attemptGuard)" "$wal_adapter"
require_literal "physicalReferences.authorizeUpload(" "$append_coordinator"
require_literal "CompletableFuture<Void> revalidateAppendSession(" "$metadata_store"
require_literal "AppendSession session" "$metadata_store"
require_literal "metadata.revalidateAppendSession(cluster, expectedSession)" "$physical_publisher"
require_literal \
    "providerRetryRevalidatesSessionAndRejectsDeletedRootBeforeSecondTransmission" \
    "$wal_guard_test"
require_literal \
    "expiredSessionRejectsFirstAttemptAfterRootTombstoneAndFreshAppendUsesFreshKey" \
    "$wal_guard_test"

echo "Phase 4 guarded provider PUT ordering, Object-WAL wiring, and negative tests verified."
