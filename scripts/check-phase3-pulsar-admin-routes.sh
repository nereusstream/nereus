#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:?usage: check-phase3-pulsar-admin-routes.sh PULSAR_CHECKOUT}"

broker="$pulsar_checkout/pulsar-broker/src/main/java/org/apache/pulsar/broker"
validator="$broker/storage/nereus/NereusTopicFeatureValidator.java"
storage="$broker/storage/nereus/NereusManagedLedgerStorage.java"
binding_store="$broker/storage/nereus/NereusStorageClassBindingStore.java"
topics_base="$broker/admin/impl/PersistentTopicsBase.java"
topics_v2="$broker/admin/v2/PersistentTopics.java"
namespaces_base="$broker/admin/impl/NamespacesBase.java"
persistent_topic="$broker/service/persistent/PersistentTopic.java"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 3 admin-route guard '$literal' in ${path#"$pulsar_checkout/"}" >&2
        exit 1
    fi
}

for path in \
    "$validator" \
    "$storage" \
    "$binding_store" \
    "$topics_base" \
    "$topics_v2" \
    "$namespaces_base" \
    "$persistent_topic"; do
    if [[ ! -f "$path" ]]; then
        echo "missing Pulsar admin-route source: $path" >&2
        exit 1
    fi
done

operations=(
    TERMINATE_TOPIC
    DELETE_TOPIC
    UNLOAD_TOPIC
    DELETE_DURABLE_SUBSCRIPTION
    ANALYZE_BACKLOG
    CLEAR_BACKLOG
    SKIP_MESSAGES
    EXPIRE_MESSAGES
    RESET_CURSOR
    TRIGGER_COMPACTION
    READ_COMPACTION_STATUS
    TRIGGER_OFFLOAD
    READ_OFFLOAD_STATUS
    TRIM_TOPIC
    TRUNCATE_TOPIC
    SET_SHADOW_TOPICS
    MIGRATE_TOPIC
)

route_sources=("$topics_base" "$topics_v2" "$namespaces_base" "$persistent_topic")
for operation in "${operations[@]}"; do
    require_literal "$operation" "$validator"
    if ! rg -Fq -- "NereusAdminOperation.$operation" "${route_sources[@]}"; then
        echo "Nereus admin operation has no guarded broker route: $operation" >&2
        exit 1
    fi
done

# Routes that can execute without a loaded PersistentTopic must consult the exact
# durable storage-class binding. A deleted binding is intentionally treated as
# absent so same-name recreation can proceed.
require_literal "validateNereusAdminOperationForLoadedOrBoundTopic" "$topics_base"
require_literal "validateUnloadedAdminOperation" "$topics_base"
require_literal "bindingStore.getBinding(persistenceName)" "$storage"
require_literal "StorageClassBindingState.DELETED" "$storage"
require_literal "metadataStore.sync(path)" "$binding_store"
require_literal "thenCompose(ignored -> metadataStore.get(path))" "$binding_store"

# set-shadow has a generic preValidation path that reports a missing topic for an
# unloaded Nereus binding. The storage-class rejection must therefore run first,
# and the internal mutation path repeats the same guard before writing policies.
if ! rg -Uq '(?s)public void setShadowTopics\(.*?validateNereusAdminOperationForLoadedOrBoundTopic.*?preValidation\(authoritative\).*?internalSetShadowTopic' \
    "$topics_v2"; then
    echo "setShadowTopics must validate the loaded-or-bound Nereus topic before generic preValidation" >&2
    exit 1
fi
require_literal "NereusAdminOperation.SET_SHADOW_TOPICS" "$topics_base"

# Namespace fan-out routes must validate each loaded PersistentTopic rather than
# bypassing the topic-level storage-class contract.
require_literal ".validateNereusAdminOperation(NereusAdminOperation.CLEAR_BACKLOG)" "$namespaces_base"
require_literal "NereusAdminOperation.DELETE_DURABLE_SUBSCRIPTION" "$namespaces_base"

# Topic migration is initiated inside PersistentTopic rather than the REST base.
require_literal "validateNereusAdminOperation(NereusAdminOperation.MIGRATE_TOPIC)" "$persistent_topic"

echo "Phase 3 loaded, unloaded, and namespace Pulsar admin routes verified."
