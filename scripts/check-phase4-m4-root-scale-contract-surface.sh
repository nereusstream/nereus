#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 root-scale contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 root-scale contract '$literal' in $path" >&2
        exit 1
    fi
}

integration="nereus-pulsar-adapter/src/f4M4IntegrationTest/java/com/nereusstream/pulsar/Phase4PhysicalGcOxiaS3IntegrationTest.java"

if [[ ! -f "$repo_root/$integration" ]]; then
    echo "missing Phase 4 M4 root-scale integration artifact: $integration" >&2
    exit 1
fi

require_literal "freshProcessPaginatesOneThousandOneRootsInOneShardAndEveryOtherShard" "$integration"
require_literal "createPhysicalRootScaleFixture" "$integration"
require_literal "targets.size() < 1_256" "$integration"
require_literal "shard == 0 ? 1_001 : 1" "$integration"
require_literal "new AtomicIntegerArray(PhysicalObjectRootScanner.ROOT_SHARDS)" "$integration"
require_literal "PhysicalStoreDecorator.auditRootScalePagination(pageCalls)" "$integration"
require_literal "assertThat(result.totalRoots()).isEqualTo(1_256)" "$integration"
require_literal "assertThat(pageCalls.get(0)).isEqualTo(16)" "$integration"
require_literal "assertThat(pageCalls.get(shard)).isEqualTo(1)" "$integration"
require_literal "each shard must start with an empty continuation and advance opaquely" "$integration"
reject_literal "Thread.sleep(" "$integration"

require_literal "phase4M4RootScaleCheck" "build.gradle.kts"
require_literal "Checkpoint AX" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4RootScaleCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 real-Oxia hot-shard pagination and fresh-process root-scale contract surface: PASS"
