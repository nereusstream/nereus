#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_root="${1:?usage: check-phase4-m4-physical-gc-config-contract-surface.sh PULSAR_CHECKOUT}"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M4 physical-GC configuration artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M4 physical-GC configuration contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$path"; then
        echo "forbidden Phase 4 M4 physical-GC configuration literal '$literal' in $path" >&2
        exit 1
    fi
}

config="$repo_root/nereus-materialization/src/main/java/com/nereusstream/materialization/gc/PhysicalGcConfig.java"
runtime_config="$repo_root/nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/NereusRuntimeConfiguration.java"
provider="$repo_root/nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/DefaultNereusRuntimeProvider.java"
pulsar_config="$pulsar_root/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/ServiceConfiguration.java"
pulsar_mapper="$pulsar_root/pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusBrokerStorageConfiguration.java"
pulsar_test="$pulsar_root/pulsar-broker/src/test/java/org/apache/pulsar/broker/storage/nereus/NereusBrokerStorageConfigurationTest.java"

for path in \
    "$config" \
    "$runtime_config" \
    "$provider" \
    "$pulsar_config" \
    "$pulsar_mapper" \
    "$pulsar_test"; do
    require_file "$path"
done

require_literal "public record PhysicalGcConfig(" "$config"
require_literal "public boolean mutationsAllowed()" "$config"
require_literal "return enabled && !dryRun;" "$config"
require_literal "public PhysicalGcConfig validateAgainst(" "$config"

require_literal "PhysicalGcConfig physicalGc" "$runtime_config"
require_literal "physicalGc.validateAgainst(streamStorage, materialization)" "$runtime_config"
require_literal "var physicalGcConfig = configuration.physicalGc();" "$provider"
require_literal "physicalGcConfig.pendingProtectionDuration()" "$provider"
require_literal "physicalGcConfig.readerLeaseDuration()" "$provider"
require_literal "physicalGcConfig.maximumClockSkew()" "$provider"
require_literal "physicalGcConfig.orphanGrace()" "$provider"
reject_literal "PENDING_PROTECTION_DURATION" "$provider"
reject_literal "READER_LEASE_DURATION" "$provider"
reject_literal "ORPHAN_GRACE" "$provider"

for contract in \
    "nereusPhysicalGcEnabled = false" \
    "nereusPhysicalGcDryRun = true" \
    "nereusReaderLeaseSeconds = 120" \
    "nereusReaderLeaseRenewSeconds = 30" \
    "nereusGcScanIntervalSeconds = 60" \
    "nereusGcMetadataScanPageSize = 1000" \
    "nereusGcObjectListPageSize = 1000" \
    "nereusGcMaxConcurrentDeletes = 4" \
    "nereusGcMaxStreamsPerCandidate = 1024" \
    "nereusGcMaxAuthoritiesPerDomainSnapshot = 100000" \
    "nereusGcMaxReferencesPerDomainSnapshot = 100000" \
    "nereusGcOperationTimeoutSeconds = 60" \
    "nereusGcCloseTimeoutSeconds = 300" \
    "nereusGcDrainGraceSeconds = 300" \
    "nereusPendingProtectionSeconds = 300" \
    "nereusOrphanGraceSeconds = 86400" \
    "nereusGcTombstoneAuditGraceSeconds = 604800"; do
    require_literal "$contract" "$pulsar_config"
done

require_literal "PhysicalGcConfig physicalGc = new PhysicalGcConfig(" "$pulsar_mapper"
for getter in \
    "isNereusPhysicalGcEnabled()" \
    "isNereusPhysicalGcDryRun()" \
    "getNereusGcMetadataScanPageSize()" \
    "getNereusGcObjectListPageSize()" \
    "getNereusGcMaxConcurrentDeletes()" \
    "getNereusGcMaxStreamsPerCandidate()" \
    "getNereusGcMaxAuthoritiesPerDomainSnapshot()" \
    "getNereusGcMaxReferencesPerDomainSnapshot()" \
    "getNereusGcScanIntervalSeconds()" \
    "getNereusReaderLeaseSeconds()" \
    "getNereusReaderLeaseRenewSeconds()" \
    "getNereusMaximumClockSkewSeconds()" \
    "getNereusGcDrainGraceSeconds()" \
    "getNereusPendingProtectionSeconds()" \
    "getNereusOrphanGraceSeconds()" \
    "getNereusGcTombstoneAuditGraceSeconds()" \
    "getNereusGcOperationTimeoutSeconds()" \
    "getNereusGcCloseTimeoutSeconds()"; do
    require_literal "$getter" "$pulsar_mapper"
done
require_literal "retention," "$pulsar_mapper"
require_literal "physicalGc," "$pulsar_mapper"
require_literal "bookKeeper);" "$pulsar_mapper"

require_literal "runtime.physicalGc().enabled()).isFalse()" "$pulsar_test"
require_literal "runtime.physicalGc().dryRun()).isTrue()" "$pulsar_test"
require_literal "runtime.physicalGc().maxReferencesPerDomainSnapshot()).isEqualTo(100000)" "$pulsar_test"
require_literal "physicalGc().mutationsAllowed()" "$pulsar_test"
require_literal "oversizedGcPage" "$pulsar_test"
require_literal "invalidGcLease" "$pulsar_test"

require_literal "phase4M4PhysicalGcConfigCheck" "$repo_root/build.gradle.kts"
require_literal "Checkpoint AO" "$repo_root/docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M4 broker physical-GC configuration and provider-consumption contract verified."
