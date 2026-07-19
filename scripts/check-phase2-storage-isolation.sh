#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:?usage: check-phase2-storage-isolation.sh PULSAR_CHECKOUT}"
object_store_build="$repo_root/nereus-object-store/build.gradle.kts"
runtime_isolation_test="$repo_root/nereus-object-store/src/test/java/com/nereusstream/objectstore/RuntimeDependencyIsolationTest.java"

if rg -n 'LocalFileObjectStore' \
    "$repo_root/nereus-managed-ledger/src/main" \
    "$repo_root/nereus-pulsar-adapter/src/main" \
    "$repo_root/nereus-core/src/main" \
    "$repo_root/nereus-object-store/src/main"; then
    echo "Production source must not select LocalFileObjectStore." >&2
    exit 1
fi

if rg -Pn 'org\.apache\.bookkeeper\.client\.(?!api\.)|ManagedLedgerFactoryImpl' \
    "$repo_root/nereus-managed-ledger/src/main" \
    "$repo_root/nereus-pulsar-adapter/src/main"; then
    echo "Nereus facade/runtime must not call BookKeeper client or stock factory APIs." >&2
    exit 1
fi

reload4j_exclusions="$(rg -Fc 'exclude(group = "org.slf4j", module = "slf4j-reload4j")' \
    "$object_store_build" || true)"
if [[ "$reload4j_exclusions" != "2" ]]; then
    echo "Object-store Hadoop edges must not export a reload4j backend into the broker runtime." >&2
    exit 1
fi
rg -Fq 'objectStoreLibraryDoesNotSelectTheBrokerLoggingBackend' "$runtime_isolation_test"

service_configuration="$pulsar_checkout/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/ServiceConfiguration.java"
hybrid_storage="$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusManagedLedgerStorage.java"

rg -Fq '"com.nereusstream.objectstore.S3CompatibleObjectStoreProvider"' "$service_configuration"
rg -Fq 'nereusClass = new NereusManagedLedgerStorageClass(nereusFactory);' "$hybrid_storage"
rg -Fq 'storageClasses = List.of(bookkeeperClass, nereusClass);' "$hybrid_storage"

echo "Phase 2 BookKeeper/Nereus, object-store, and broker logging-backend isolation verified."
