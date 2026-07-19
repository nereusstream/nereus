#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_root="${1:?usage: $0 <pulsar-checkout>}"

if [[ ! -d "$pulsar_root" ]]; then
    echo "missing locked Pulsar checkout: $pulsar_root" >&2
    exit 1
fi

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 final contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 final contract '$literal' in $path" >&2
        exit 1
    fi
}

require_pulsar_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$pulsar_root/$path"; then
        echo "missing locked Pulsar Phase 4 M4 final contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_pulsar_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$pulsar_root/$path"; then
        echo "forbidden locked Pulsar Phase 4 M4 final contract '$literal' in $path" >&2
        exit 1
    fi
}

backfill="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/DefaultPhysicalRootBackfillCoordinator.java"
backfill_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalRootBackfillCoordinatorTest.java"
runtime="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4PhysicalGcRuntime.java"
referenced_gc="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/ReferencedObjectGcExecutor.java"
pulsar_fixture="pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusMultiBrokerIntegrationTest.java"
pulsar_test="pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusPhysicalGcMultiBrokerIntegrationTest.java"

for path in "$backfill" "$backfill_test" "$runtime" "$referenced_gc"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 final artifact: $path" >&2
        exit 1
    fi
done

for path in "$pulsar_fixture" "$pulsar_test"; do
    if [[ ! -f "$pulsar_root/$path" ]]; then
        echo "missing locked Pulsar Phase 4 M4 final artifact: $path" >&2
        exit 1
    fi
done

require_literal "sameStreamAuthority(" "$backfill"
require_literal "sameAppendRecoveryAuthority(" "$backfill"
require_literal 'writer.text("stream-authority-v1")' "$backfill"
require_literal "private final int cursorPageSize;" "$backfill"
require_literal "exactCursorConfig.cursorScanPageSize()" "$runtime"
require_literal "sameStreamAuthority(first, refreshed)" "$backfill_test"
require_literal "sameAppendRecoveryAuthority(first, refreshed)" "$backfill_test"
require_literal "class ReferencedObjectGcExecutor" "$referenced_gc"
reject_literal "writer.int64(snapshot.metadataVersion())" "$backfill"
reject_literal "writer.text(snapshot.trim().reason())" "$backfill"
reject_literal "snapshot.trim().updatedAtMillis()" "$backfill"

require_pulsar_literal "new NereusMultiBrokerIntegrationTest(true)" "$pulsar_test"
require_pulsar_literal "deletesMaterializedWalSourcesAndPreservesMessageIdsAcrossOwnershipCuts" "$pulsar_test"
require_pulsar_literal "materializationService()" "$pulsar_test"
require_pulsar_literal "doesNotContainAnyElementsOf(initialWal.get())" "$pulsar_test"
require_pulsar_literal "containsAll(compacted.get())" "$pulsar_test"
require_pulsar_literal ".topics().unload(topic)" "$pulsar_test"
require_pulsar_literal "cluster.stopBroker(stoppedOwner)" "$pulsar_test"
require_pulsar_literal "cluster.startBroker(stoppedOwner)" "$pulsar_test"
require_pulsar_literal "REVERSE_ROLLOVER_RUN" "$pulsar_test"
require_pulsar_literal "bookKeeperTopic" "$pulsar_test"
require_pulsar_literal "exact.getBatchIndex()" "$pulsar_test"
require_pulsar_literal "Duration.ofSeconds(120)" "$pulsar_fixture"
reject_pulsar_literal "Thread.sleep(" "$pulsar_test"

require_literal "phase4M4PhysicalGcMultiBrokerPulsarCheck" "build.gradle.kts"
require_literal '"-PtestRetryCount=0"' "build.gradle.kts"
require_literal "phase4M4FinalCheck" "build.gradle.kts"
require_literal "phase4M4FinalCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "F4-M4 已完成并通过 final gate" \
    "docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M4 stable-authority and real two-broker physical-GC contract surface: PASS"
