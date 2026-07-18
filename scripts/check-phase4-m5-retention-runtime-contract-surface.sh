#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_root="${1:?usage: check-phase4-m5-retention-runtime-contract-surface.sh PULSAR_CHECKOUT}"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M5 retention-runtime artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M5 retention-runtime contract '$literal' in $path" >&2
        exit 1
    fi
}

managed_main="$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger"
managed_test="$repo_root/nereus-managed-ledger/src/test/java/com/nereusstream/managedledger"
adapter_main="$repo_root/nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar"
adapter_test="$repo_root/nereus-pulsar-adapter/src/test/java/com/nereusstream/pulsar"
pulsar_config="$pulsar_root/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/ServiceConfiguration.java"
pulsar_mapper="$pulsar_root/pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusBrokerStorageConfiguration.java"
pulsar_test="$pulsar_root/pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusBrokerStorageConfigurationTest.java"

lane="$managed_main/retention/NereusRetentionExecutionLane.java"
runtime="$managed_main/retention/NereusRetentionRuntime.java"
ledger="$managed_main/NereusManagedLedger.java"
managed_runtime="$managed_main/NereusManagedLedgerRuntime.java"
policy="$managed_main/retention/RetentionPolicySnapshot.java"
lane_test="$managed_test/retention/NereusRetentionExecutionLaneTest.java"
configuration="$adapter_main/NereusRuntimeConfiguration.java"
provider="$adapter_main/DefaultNereusRuntimeProvider.java"
configuration_test="$adapter_test/NereusRuntimeConfigurationRetentionTest.java"

for path in \
    "$lane" \
    "$runtime" \
    "$ledger" \
    "$managed_runtime" \
    "$policy" \
    "$lane_test" \
    "$configuration" \
    "$provider" \
    "$configuration_test" \
    "$pulsar_config" \
    "$pulsar_mapper" \
    "$pulsar_test"; do
    require_file "$path"
done

require_literal "new ArrayBlockingQueue<>(config.maxQueuedPlans())" "$lane"
require_literal "config.maxConcurrentPlans()" "$lane"
require_literal "flights.putIfAbsent(exactStream, created)" "$lane"
require_literal "source.get(" "$lane"
require_literal "config.operationTimeout().toMillis()" "$lane"
require_literal "executor.awaitTermination(" "$lane"
require_literal "return mirror(existing.result())" "$lane"
require_literal "ErrorCode.BACKPRESSURE_REJECTED" "$lane"

require_literal "new NereusRetentionExecutionLane(" "$runtime"
require_literal "exactService.streamId().equals(exactStream)" "$runtime"
require_literal "return lane.submit(" "$runtime"
require_literal "lane.close()" "$runtime"

require_literal "fromCanonicalMinutesAndMebibytes" "$policy"
require_literal "nereus-retention-policy-v1" "$policy"
require_literal "public void installRetentionPolicy(" "$ledger"
require_literal "public void trimConsumedLedgersInBackground(" "$ledger"
require_literal "runtime.retentionRuntime().trim(" "$ledger"
require_literal "this::snapshotRetentionPolicy" "$ledger"
require_literal "public boolean hasRetentionRuntime()" "$managed_runtime"

require_literal "NereusRetentionConfig retention" "$configuration"
require_literal "retention.maxQueuedPlans() > managedLedger.maxPendingCallbacks()" "$configuration"
require_literal "retentionRuntime = new NereusRetentionRuntime(" "$provider"

require_literal "coalescesOneExecutionPerStreamButReturnsIndependentCompletions" "$lane_test"
require_literal "holdsConcurrencyUntilAsyncCompletionAndRejectsQueueOverflow" "$lane_test"
require_literal "timesOutTheWholeOperationAndRejectsAfterClose" "$lane_test"
require_literal "rejectsCloseAndQueueBeyondManagedLedgerBudgets" "$configuration_test"

require_literal "nereusRetentionStatsScanPageSize = 512" "$pulsar_config"
require_literal "nereusRetentionMaxConcurrentPlans = 4" "$pulsar_config"
require_literal "nereusRetentionMaxQueuedPlans = 1024" "$pulsar_config"
require_literal "nereusRetentionOperationTimeoutSeconds = 60" "$pulsar_config"
require_literal "nereusRetentionCloseTimeoutSeconds = 120" "$pulsar_config"
require_literal "NereusRetentionConfig retention = new NereusRetentionConfig(" "$pulsar_mapper"
require_literal "runtime.retention().maxConcurrentPlans()).isEqualTo(4)" "$pulsar_test"
require_literal "invalidRetentionDeadline" "$pulsar_test"

require_literal "phase4M5RetentionRuntimeCheck" "$repo_root/build.gradle.kts"
require_literal "Checkpoint AH" "$repo_root/docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M5 bounded retention runtime, production ledger wiring, and Pulsar config mapping verified."
