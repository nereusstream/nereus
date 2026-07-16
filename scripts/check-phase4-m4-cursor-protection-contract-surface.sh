#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 cursor-protection $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 cursor-protection contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    CursorSnapshotWriteAuthority.java
    CursorSnapshotPublication.java
    CursorSnapshotStore.java
    DefaultCursorSnapshotStore.java
)

test_artifacts=(
    CursorSnapshotStoreTest.java
    CursorStorageOxiaS3IntegrationTest.java
    DefaultCursorSnapshotStoreLocalStackIntegrationTest.java
    NereusManagedLedgerRuntimeTest.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done
for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

snapshot_store="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/cursor/DefaultCursorSnapshotStore.java"
snapshot_api="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/cursor/CursorSnapshotStore.java"
cursor_storage="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/cursor/DefaultCursorStorage.java"
runtime="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/NereusManagedLedgerRuntime.java"
provider="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/DefaultNereusRuntimeProvider.java"
snapshot_test="nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/cursor/CursorSnapshotStoreTest.java"

require_literal "prepareWrite(" "$snapshot_api"
require_literal "completeWrite(" "$snapshot_api"
require_literal "CursorSnapshotWriteAuthority" "$snapshot_api"
require_literal "CursorSnapshotPublication" "$snapshot_api"

require_literal "(key, providerAttempt) -> authorizeUpload" "$snapshot_store"
require_literal "ObjectProtectionType.CURSOR_SNAPSHOT_PENDING" "$snapshot_store"
require_literal "ObjectProtectionType.CURSOR_SNAPSHOT_ROOT" "$snapshot_store"
require_literal "protections.acquireOrTransfer" "$snapshot_store"
require_literal "protections.release(" "$snapshot_store"
require_literal "protectLiveReference(" "$snapshot_store"
require_literal "readPins.acquire(" "$snapshot_store"
require_literal "lease.release()" "$snapshot_store"
require_literal "loadLiveReferenceRoot(" "$snapshot_store"

require_literal "snapshotStore.prepareWrite" "$cursor_storage"
require_literal "metadataStore.compareAndSetCursor" "$cursor_storage"
require_literal "snapshotStore.completeWrite" "$cursor_storage"

require_literal "DefaultObjectReadPinManager" "$provider"
require_literal '"f4-reader/" + streamConfig.processRunId()' "$provider"
require_literal "new DefaultCursorSnapshotStore" "$provider"
require_literal "ObjectReadPinManager objectReadPinManager()" "$runtime"

require_literal "publishesPendingThenPermanentProtectionAndReadsOnlyUnderDurableLease" "$snapshot_test"
require_literal "readReconcilesPermanentProtectionAfterCursorCasResponseLoss" "$snapshot_test"
require_literal "guardedPutRejectsAChangedCursorAuthorityBeforeUploadingBytes" "$snapshot_test"
require_literal "phase4M4CursorProtectionCheck" "build.gradle.kts"
require_literal "phase4M4CursorProtectionCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

prepare_line="$(rg -n -F 'snapshotStore.prepareWrite' "$repo_root/$cursor_storage" | tail -1 | cut -d: -f1)"
complete_line="$(rg -n -F 'snapshotStore.completeWrite' "$repo_root/$cursor_storage" | tail -1 | cut -d: -f1)"
cas_line="$(rg -n -F 'metadataStore.compareAndSetCursor(' "$repo_root/$cursor_storage" \
        | awk -F: -v prepare="$prepare_line" '$1 > prepare { print $1; exit }')"
if [[ -z "$prepare_line" || -z "$cas_line" || -z "$complete_line" \
        || "$prepare_line" -ge "$cas_line" || "$cas_line" -ge "$complete_line" ]]; then
    echo "cursor snapshot publication must visibly prepare, CAS the cursor root, then complete permanent protection" >&2
    exit 1
fi

permanent_line="$(rg -n -F 'protections.acquireOrTransfer(request, revalidator)' \
        "$repo_root/$snapshot_store" | head -1 | cut -d: -f1)"
pending_release_line="$(rg -n -F 'protections.release(' "$repo_root/$snapshot_store" | head -1 | cut -d: -f1)"
if [[ -z "$permanent_line" || -z "$pending_release_line" \
        || "$permanent_line" -ge "$pending_release_line" ]]; then
    echo "cursor snapshot completion must establish permanent protection before pending release" >&2
    exit 1
fi

pin_line="$(rg -n -F 'readPins.acquire(' "$repo_root/$snapshot_store" | head -1 | cut -d: -f1)"
read_line="$(rg -n -F 'return objectStore.readRange(' "$repo_root/$snapshot_store" | head -1 | cut -d: -f1)"
if [[ -z "$pin_line" || -z "$read_line" || "$pin_line" -ge "$read_line" ]]; then
    echo "cursor snapshot bytes must only be read after durable reader-pin admission" >&2
    exit 1
fi

if rg -Fq -- "CompletableFuture<CursorSnapshotReference> write(" "$repo_root/$snapshot_api"; then
    echo "legacy one-phase cursor snapshot write API must not remain authoritative" >&2
    exit 1
fi

echo "Phase 4 M4 guarded cursor snapshot publication, response-loss repair, and durable read pinning verified."
