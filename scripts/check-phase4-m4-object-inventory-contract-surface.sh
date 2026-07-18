#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 object-inventory contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 object-inventory contract '$literal' in $path" >&2
        exit 1
    fi
}

scanner="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/ObjectInventoryScanner.java"
key="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/ObjectInventoryKey.java"
result="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/ObjectInventoryScanResult.java"
families="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4ObjectInventoryFamilies.java"
runtime="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4PhysicalGcRuntime.java"
wal_keys="nereus-object-store/src/main/java/com/nereusstream/objectstore/wal/WalObjectKeys.java"
checkpoint="nereus-object-store/src/main/java/com/nereusstream/objectstore/checkpoint/RecoveryCheckpointFormatV1.java"
compacted="nereus-object-store/src/main/java/com/nereusstream/objectstore/compacted/CompactedObjectFormatV1.java"
cursor_keys="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/cursor/CursorSnapshotKeys.java"
scanner_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/ObjectInventoryScannerTest.java"
families_test="nereus-pulsar-adapter/src/test/java/com/nereusstream/pulsar/Phase4ObjectInventoryFamiliesTest.java"

for path in \
    "$scanner" \
    "$key" \
    "$result" \
    "$families" \
    "$runtime" \
    "$wal_keys" \
    "$checkpoint" \
    "$compacted" \
    "$cursor_keys" \
    "$scanner_test" \
    "$families_test"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 object-inventory artifact: $path" >&2
        exit 1
    fi
done

require_literal "objectStore.listObjects(" "$scanner"
require_literal "metadataStore.getRoot(" "$scanner"
require_literal "objectStore.headObject(" "$scanner"
require_literal "metadataStore.createRoot(" "$scanner"
require_literal "config.orphanGrace()" "$scanner"
require_literal "config.maximumClockSkew()" "$scanner"
require_literal "Outcome.WOULD_REGISTER" "$scanner"
require_literal "Outcome.ROOT_CONVERGED" "$scanner"
require_literal "exactDesiredRoot(created, root)" "$scanner"
require_literal "every listed object must have exactly one inventory outcome" "$result"
require_literal "ChecksumType.CRC32C" "$key"
reject_literal "deleteObject(" "$scanner"

require_literal '"wal-object-v1"' "$families"
require_literal '"committed-compacted-v1"' "$families"
require_literal '"topic-compacted-v1"' "$families"
require_literal '"recovery-checkpoint-v1"' "$families"
require_literal '"cursor-snapshot-v1"' "$families"
require_literal "WalObjectKeys.parse(" "$families"
require_literal "CompactedObjectFormatV1.parseObjectKey(" "$families"
require_literal "RecoveryCheckpointFormatV1.parseObjectKey(" "$families"
require_literal "CursorSnapshotKeys.parseOwnerless(" "$families"

require_literal "public static ParsedWalObjectKey parse(" "$wal_keys"
require_literal "public static ParsedRecoveryCheckpointKey parseObjectKey(" "$checkpoint"
require_literal "public static ParsedCompactedObjectKey parseObjectKey(" "$compacted"
require_literal "public static ParsedOwnerlessSnapshotKey parseOwnerless(" "$cursor_keys"

require_literal "new ObjectInventoryScanner(" "$runtime"
require_literal "Phase4ObjectInventoryFamilies.currentV1(" "$runtime"
require_literal "objectInventoryScanner()" "$runtime"
reject_literal ".scheduleAtFixedRate(" "$runtime"
reject_literal ".scheduleWithFixedDelay(" "$runtime"

require_literal "registersOnlyOldExactHeadObjectsAndWaitsAnotherFullGrace" "$scanner_test"
require_literal "disabledAndDryRunPassesReportWithoutCreatingMetadata" "$scanner_test"
require_literal "lostCreateResponseConvergesOnlyThroughTheExactDurableRoot" "$scanner_test"
require_literal "concurrentDifferentRootIsReportedWithoutOverwritingOrAbortingPass" "$scanner_test"
require_literal "currentFamiliesStrictlyInvertEveryInstalledWriterKey" "$families_test"
require_literal "malformedKeysCannotBePromotedByPrefixMembership" "$families_test"

require_literal "phase4M4ObjectInventoryCheck" "build.gradle.kts"
require_literal "phase4M4ObjectInventoryCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "Checkpoint AL" "docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M4 object-inventory contract surface: PASS"
