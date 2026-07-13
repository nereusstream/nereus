#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:?usage: check-phase2-storage-isolation.sh PULSAR_CHECKOUT}"

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

service_configuration="$pulsar_checkout/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/ServiceConfiguration.java"
hybrid_storage="$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/nereus/NereusManagedLedgerStorage.java"

rg -Fq '"com.nereusstream.objectstore.S3CompatibleObjectStoreProvider"' "$service_configuration"
rg -Fq 'nereusClass = new NereusManagedLedgerStorageClass(nereusFactory);' "$hybrid_storage"
rg -Fq 'storageClasses = List.of(bookkeeperClass, nereusClass);' "$hybrid_storage"

echo "Phase 2 BookKeeper/Nereus and production object-store isolation verified."
