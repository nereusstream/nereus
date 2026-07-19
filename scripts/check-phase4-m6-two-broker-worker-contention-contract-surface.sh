#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_root="${1:?usage: $0 <pulsar-checkout>}"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M6 worker-contention artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M6 worker-contention contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$path"; then
        echo "forbidden Phase 4 M6 worker-contention contract '$literal' in $path" >&2
        exit 1
    fi
}

stats="$repo_root/nereus-object-store/src/main/java/com/nereusstream/objectstore/wal/WalSliceReadStats.java"
reader="$repo_root/nereus-core/src/main/java/com/nereusstream/core/read/ParquetCompactedTargetReader.java"
reader_test="$repo_root/nereus-core/src/test/java/com/nereusstream/core/read/ParquetCompactedTargetReaderTest.java"
pulsar_test="$pulsar_root/pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusMaterializationContentionMultiBrokerIntegrationTest.java"

for path in "$stats" "$reader" "$reader_test" "$pulsar_test"; do
    require_file "$path"
done

require_literal "long downloadedPayloadBytes" "$stats"
require_literal "long downloadedEntryIndexBytes" "$stats"
require_literal "public long physicalBytesRead()" "$stats"
require_literal "public long ioDeltaBytes()" "$stats"
require_literal "public long compressionSavingsBytes()" "$stats"
require_literal "result.physicalBytesRead()" "$reader"
require_literal "result.footerBytesRead()" "$reader"
require_literal "readsCompressibleLogicalPayloadLargerThanPhysicalParquetIo" "$reader_test"
require_literal ".isLessThan(stats.returnedPayloadBytes())" "$reader_test"

require_literal "twoBrokerWorkerRuntimesContendOnTheSameStreamsAndConvergeExactReads" "$pulsar_test"
require_literal "new NereusMultiBrokerIntegrationTest(" "$pulsar_test"
require_literal "StorageProfile.OBJECT_WAL_SYNC_OBJECT" "$pulsar_test"
require_literal "CountDownLatch ready = new CountDownLatch(2)" "$pulsar_test"
require_literal "CountDownLatch start = new CountDownLatch(1)" "$pulsar_test"
require_literal ".materializationService()" "$pulsar_test"
require_literal ".scanNow()" "$pulsar_test"
require_literal ".isNotSameAs(runtimes.get(1).runtime())" "$pulsar_test"
require_literal ".isNotEqualTo(runtimes.get(1).processRunId())" "$pulsar_test"
require_literal "assertThat(result.shardsScanned()).isEqualTo(64)" "$pulsar_test"
require_literal "WORKLOAD_ENTRIES = 16" "$pulsar_test"
require_literal "WORKLOAD_PAYLOAD_BYTES = 128 * 1024" "$pulsar_test"
require_literal "assertDirectStorageRead(topic, expected.size())" "$pulsar_test"
require_literal "containsExactly(exact.payload())" "$pulsar_test"
require_literal ".isEqualTo(exact.properties())" "$pulsar_test"
require_literal "exact.assertSamePosition(actual.getMessageId())" "$pulsar_test"
require_literal "assertBookKeeperControl(client)" "$pulsar_test"
reject_literal "Thread.sleep(" "$pulsar_test"

require_literal "phase4M6TwoBrokerWorkerContentionPulsarCheck" "$repo_root/build.gradle.kts"
require_literal '"-PtestRetryCount=0"' "$repo_root/build.gradle.kts"
require_literal "phase4M6TwoBrokerWorkerContentionCheck" "$repo_root/build.gradle.kts"
require_literal "Checkpoint BI" "$repo_root/docs/phase-4-compaction-generation/README.md"
require_literal "phase4M6TwoBrokerWorkerContentionCheck" \
    "$repo_root/docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M6 two-broker/two-worker contention, compressed read, exact MessageId, and BookKeeper coexistence surface: PASS"
