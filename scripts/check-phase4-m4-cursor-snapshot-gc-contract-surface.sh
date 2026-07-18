#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 cursor-snapshot GC contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 cursor-snapshot GC ownership '$literal' in $path" >&2
        exit 1
    fi
}

scanner="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/retention/CursorSnapshotGcScanner.java"
keys="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/cursor/CursorSnapshotKeys.java"
collector="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/PhysicalObjectGarbageCollector.java"
scanner_test="nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/retention/CursorSnapshotGcScannerTest.java"
collector_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalObjectGarbageCollectorTest.java"

for path in "$scanner" "$keys" "$collector" "$scanner_test" "$collector_test"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 cursor-snapshot GC artifact: $path" >&2
        exit 1
    fi
done

require_literal "MAX_INVENTORY_VALUES = 10_000" "$scanner"
require_literal "CursorSnapshotInventory.classify(" "$scanner"
require_literal "scanCursorRoots(" "$scanner"
require_literal "objectStore.listObjects(" "$scanner"
require_literal "physicalMetadataStore.scanProtections(" "$scanner"
require_literal "physicalMetadataStore.getRoot(" "$scanner"
require_literal "GcReferenceQueryKind.CURSOR_SNAPSHOT_CANDIDATE" "$scanner"
require_literal "PhysicalObjectKind.CURSOR_SNAPSHOT" "$scanner"
require_literal "maximumClockSkew" "$scanner"
require_literal "METADATA_LIMIT_EXCEEDED" "$scanner"
require_literal "public CompletableFuture<Boolean> revalidate(Candidate candidate)" "$scanner"
require_literal "Strict inverse used by F4 inventory" "$keys"
require_literal "FinalCandidateRevalidator" "$collector"
require_literal "revalidate candidate-specific inventory before delete intent" "$collector"
require_literal "PLAN_DRIFT_UNMARKED" "$collector_test"
require_literal "ObjectProtectionType.CURSOR_SNAPSHOT_PENDING" "$scanner_test"
require_literal "scanner.revalidate(candidate).join()" "$scanner_test"
require_literal "ErrorCode.METADATA_LIMIT_EXCEEDED" "$scanner_test"

reject_literal "objectStore.deleteObject(" "$scanner"
reject_literal "physicalMetadataStore.compareAndSetRoot(" "$scanner"
reject_literal "physicalMetadataStore.deleteProtection(" "$scanner"

require_literal "phase4M4CursorSnapshotGcCheck" "build.gradle.kts"
require_literal "phase4M4CursorSnapshotGcCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "Checkpoint AJ" "docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M4 cursor-snapshot GC contract surface: PASS"
