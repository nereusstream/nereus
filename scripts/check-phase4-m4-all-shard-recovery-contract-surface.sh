#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 all-shard recovery contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 all-shard recovery contract '$literal' in $path" >&2
        exit 1
    fi
}

integration="nereus-pulsar-adapter/src/f4M4IntegrationTest/java/com/nereusstream/pulsar/Phase4PhysicalGcOxiaS3IntegrationTest.java"
scanner="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/ObjectInventoryScanner.java"
scanner_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/ObjectInventoryScannerTest.java"

for path in "$integration" "$scanner" "$scanner_test"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 all-shard recovery artifact: $path" >&2
        exit 1
    fi
done

require_literal "freshProcessRecoversMarkedAndDeletingRootsFromEveryShardWithEmptyInventory" "$integration"
require_literal "createOneOwnerlessCompactedObjectPerShard" "$integration"
require_literal "SHARD_KEYSPACE.physicalObjectShard(hash)" "$integration"
require_literal "for (int shard = 0; shard < 256; shard++)" "$integration"
require_literal "forceDeleting(targets.get(shard))" "$integration"
require_literal "new EmptyInventoryObjectStore(raw, emptyListCalls)" "$integration"
require_literal "assertThat(pass.roots().markedRoots()).isEqualTo(128)" "$integration"
require_literal "assertThat(pass.roots().deletingRoots()).isEqualTo(128)" "$integration"
require_literal "assertThat(pass.roots().totalRoots()).isEqualTo(256)" "$integration"
reject_literal "Thread.sleep(" "$integration"

require_literal "Optional<String> suppliedContinuation" "$scanner"
require_literal "page.continuationToken().equals(suppliedContinuation)" "$scanner"
reject_literal "String lastKey" "$scanner"
reject_literal "compareTo(lastKey)" "$scanner"
require_literal "opaqueContinuationDoesNotImplyCrossPageLogicalOrdering" "$scanner_test"
require_literal "repeatedOpaqueContinuationFailsClosed" "$scanner_test"

require_literal "phase4M4AllShardRecoveryCheck" "build.gradle.kts"
require_literal "Checkpoint AW" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4AllShardRecoveryCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 all-shard metadata recovery and opaque LIST continuation contract surface: PASS"
