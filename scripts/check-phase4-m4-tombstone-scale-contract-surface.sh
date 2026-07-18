#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 tombstone-scale contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 tombstone-scale contract '$literal' in $path" >&2
        exit 1
    fi
}

scale_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalRootTombstoneRetirementScaleTest.java"
materialization_scheduler="nereus-materialization/src/main/java/com/nereusstream/materialization/MaterializationSchedulers.java"
runtime_provider="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/DefaultNereusRuntimeProvider.java"
s3_provider="nereus-object-store/src/main/java/com/nereusstream/objectstore/S3CompatibleObjectStoreProvider.java"

for path in "$scale_test" "$materialization_scheduler" "$runtime_provider" "$s3_provider"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 tombstone-scale artifact: $path" >&2
        exit 1
    fi
done

require_literal "retiresTenThousandDeletedRootsThroughTwoBoundedWindowsWithoutAccumulation" "$scale_test"
require_literal "ROOT_COUNT = 10_000" "$scale_test"
require_literal "PAGE_SIZE = 32" "$scale_test"
require_literal "TombstoneRetirementStatus.NOT_OLD_ENOUGH" "$scale_test"
require_literal "TombstoneRetirementStatus.RETIRED" "$scale_test"
require_literal "assertThat(references).doesNotContainKey(objectId.value())" "$scale_test"
require_literal "assertThat(audits.absent(current.value().objectId())).isTrue()" "$scale_test"
require_literal "assertThat(metadata.rootsDeleted()).isEqualTo(ROOT_COUNT)" "$scale_test"
require_literal "assertThat(scheduler.getQueue()).isEmpty()" "$scale_test"
require_literal "assertThat(objects.deleteCalls()).isZero()" "$scale_test"
require_literal "setRemoveOnCancelPolicy(true)" "$materialization_scheduler"
require_literal "MaterializationSchedulers.newSingleThreadScheduler" "$runtime_provider"
require_literal "setRemoveOnCancelPolicy(true)" "$s3_provider"
reject_literal "Executors.newSingleThreadScheduledExecutor" "$runtime_provider"
reject_literal "Executors.newSingleThreadScheduledExecutor" "$s3_provider"
reject_literal "Thread.sleep(" "$scale_test"

require_literal "phase4M4TombstoneScaleCheck" "build.gradle.kts"
require_literal "Checkpoint AY" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4TombstoneScaleCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 10,000-root dual-window tombstone retirement and bounded-deadline contract surface: PASS"
