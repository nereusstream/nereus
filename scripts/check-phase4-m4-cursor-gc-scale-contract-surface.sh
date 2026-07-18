#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 cursor-GC scale contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 cursor-GC scale contract '$literal' in $path" >&2
        exit 1
    fi
}

scanner="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/retention/CursorSnapshotGcScanner.java"
scale_test="nereus-pulsar-adapter/src/test/java/com/nereusstream/pulsar/CursorSnapshotGcScaleTest.java"

for path in "$scanner" "$scale_test"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 cursor-GC scale artifact: $path" >&2
        exit 1
    fi
done

require_literal "for (ObjectKey key : candidates)" "$scanner"
require_literal "private CompletableFuture<Void> visitCandidate(" "$scanner"
reject_literal "index + 1, now, visitor, counts" "$scanner"

require_literal "visitsTenThousandCandidatesSequentiallyWithoutStackGrowthOrDeadlineRetention" "$scale_test"
require_literal "classifiesAndDeletesLiveOldCasLostAndDeletedCursorSnapshotsAtTenThousandRoots" "$scale_test"
require_literal "ROOT_COUNT = 10_000" "$scale_test"
require_literal "PAGE_SIZE = 256" "$scale_test"
require_literal "LIVE_REFERENCE_COUNT = 9_997" "$scale_test"
require_literal "ObjectProtectionType.CURSOR_SNAPSHOT_PENDING" "$scale_test"
require_literal "CursorRecordLifecycle.DELETED" "$scale_test"
require_literal "PhysicalGcAdvanceStatus.WAITING_FOR_GRACE" "$scale_test"
require_literal "PhysicalGcAdvanceStatus.DELETE_INTENT" "$scale_test"
require_literal "PhysicalGcDeletionStatus.DELETED" "$scale_test"
require_literal "assertThat(maximumVisitors).hasValue(1)" "$scale_test"
require_literal "assertThat(context.objectStore.deleteCalls()).isEqualTo(3)" "$scale_test"
require_literal "assertThat(context.scheduler.getQueue()).isEmpty()" "$scale_test"

require_literal "phase4M4CursorGcScaleCheck" "build.gradle.kts"
require_literal "Checkpoint AZ" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4CursorGcScaleCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 10,000-root cursor snapshot classification/deletion and stack-bound contract surface: PASS"
