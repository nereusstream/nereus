#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M6 registry-scale contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M6 registry-scale contract '$literal' in $path" >&2
        exit 1
    fi
}

scale_test="nereus-materialization/src/test/java/com/nereusstream/materialization/RegisteredMaterializationStreamScannerTest.java"
scanner="nereus-materialization/src/main/java/com/nereusstream/materialization/RegisteredMaterializationStreamScanner.java"

for path in "$scale_test" "$scanner"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M6 registry-scale artifact: $path" >&2
        exit 1
    fi
done

require_literal "scansSixteenThousandFourHundredFortyEightRegistrationsAcrossColdRestarts" "$scale_test"
require_literal "SCALE_STREAMS_PER_SHARD = 257" "$scale_test"
require_literal "SCALE_REGISTRY_PAGE_SIZE = 256" "$scale_test"
require_literal "F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS * SCALE_STREAMS_PER_SHARD" "$scale_test"
require_literal "Map<Integer, List<StreamId>> streamsByShard = streamsPerShard(" "$scale_test"
require_literal "keyspace, SCALE_STREAMS_PER_SHARD" "$scale_test"
require_literal "keyspace.materializationRegistryShard(streamId)" "$scale_test"
require_literal "GenerationMetadataStoreTestFactory.inMemory(clock)" "$scale_test"
require_literal ".createOrVerifyStreamRegistration(" "$scale_test"
require_literal "tracingStore(durable, firstTrace)" "$scale_test"
require_literal "tracingStore(durable, restartedTrace)" "$scale_test"
require_literal "containsExactly(true, false)" "$scale_test"
require_literal "containsExactly(SCALE_REGISTRY_PAGE_SIZE, 1)" "$scale_test"
require_literal ".doesNotHaveDuplicates()" "$scale_test"
require_literal ".containsExactlyElementsOf(expectedKeys)" "$scale_test"
require_literal "l0Store(StreamState.DELETED)" "$scale_test"
reject_literal "Thread.sleep(" "$scale_test"

require_literal "scanRegistryPage(shard, Optional.empty(), accumulator)" "$scanner"
require_literal "generations.scanStreamRegistrations(" "$scanner"
require_literal "page.continuation().isPresent()" "$scanner"

require_literal "phase4M6RegistryScaleCheck" "build.gradle.kts"
require_literal "Checkpoint BH" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M6RegistryScaleCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M6 16,448-stream all-shard pagination and cold-restart registry-scale contract surface: PASS"
