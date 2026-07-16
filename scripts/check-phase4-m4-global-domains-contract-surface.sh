#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 global-domain $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 global-domain contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    GcGlobalReferenceScope.java
    GcGlobalReferenceScopeSnapshot.java
    RegisteredStreamGcGlobalReferenceScope.java
    FutureCatalogSentinelDomain.java
    GenerationProtocolDomainSets.java
    GenerationReferenceDomain.java
    AppendRecoveryReferenceDomain.java
    MaterializationReferenceDomain.java
    ProjectionGenerationReferenceDomain.java
    CursorSnapshotReferenceDomain.java
)

test_artifacts=(
    GcReferenceSnapshotBuilderTest.java
    RegisteredStreamGcGlobalReferenceScopeTest.java
    FutureCatalogSentinelTest.java
    GenerationReferenceDomainTest.java
    AppendRecoveryReferenceDomainTest.java
    MaterializationReferenceDomainTest.java
    ProjectionGenerationReferenceDomainTest.java
    CursorSnapshotReferenceDomainTest.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done
for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

builder="nereus-core/src/main/java/com/nereusstream/core/physical/GcReferenceSnapshotBuilder.java"
scope_api="nereus-core/src/main/java/com/nereusstream/core/physical/GcGlobalReferenceScope.java"
scope="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/RegisteredStreamGcGlobalReferenceScope.java"
sentinel="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/FutureCatalogSentinelDomain.java"
activation_record="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/records/GenerationProtocolActivationRecord.java"

require_literal "markIncomplete" "$builder"
require_literal "GcGlobalReferenceScopeSnapshot.incomplete()" "$scope_api"
require_literal "snapshot.contributeTo(builder)" "$scope_api"
require_literal "query.kind() != GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE" "$scope_api"

require_literal "activationStore.get(cluster)" "$scope"
require_literal "GenerationProtocolDomainSets.deletionReady" "$scope"
require_literal "scanStreamRegistrations" "$scope"
require_literal "F4Keyspace.MATERIALIZATION_REGISTRY_SHARDS" "$scope"
require_literal "global registration scan did not advance" "$scope"
require_literal "!current.orElseThrow().equals(expected)" "$scope"
if rg -Fq -- "getOrCreate" "$repo_root/$scope"; then
    echo "ownerless global scope must never create activation authority while scanning" >&2
    exit 1
fi

require_literal 'DOMAIN_ID = "future-catalog-sentinel-v1"' "$sentinel"
require_literal "activationStore.get(cluster)" "$sentinel"
require_literal "GenerationProtocolDomainSets.exactMatch" "$sentinel"
require_literal "physicalDeleteEnabled" "$sentinel"
require_literal "cursorSnapshotDeleteEnabled" "$sentinel"
require_literal "requiredReferenceDomains must be strictly sorted with unique domain ids" "$activation_record"

for path in \
    nereus-materialization/src/main/java/com/nereusstream/materialization/gc/GenerationReferenceDomain.java \
    nereus-materialization/src/main/java/com/nereusstream/materialization/gc/AppendRecoveryReferenceDomain.java \
    nereus-materialization/src/main/java/com/nereusstream/materialization/gc/MaterializationReferenceDomain.java \
    nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/retention/ProjectionGenerationReferenceDomain.java \
    nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/retention/CursorSnapshotReferenceDomain.java; do
    require_literal "GcGlobalReferenceScope.resolveStreams" "$path"
    if rg -Fq -- "GcReferenceSnapshotBuilder.unsupportedOwnerless" "$repo_root/$path"; then
        echo "global-enabled reference domain still hard-codes unsupported ownerless behavior: $path" >&2
        exit 1
    fi
done

require_literal "deletionReadyActivationPromotesEveryRegistrationShardToGlobalAuthority" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/RegisteredStreamGcGlobalReferenceScopeTest.java"
require_literal "activationChangeAcrossGlobalEnumerationCannotProduceClearScope" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/RegisteredStreamGcGlobalReferenceScopeTest.java"
require_literal "exactDeletionReadyDomainSetClearsAndEveryActivationChangeInvalidatesSnapshot" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/FutureCatalogSentinelTest.java"
require_literal "ownerlessGlobalScopeScansEveryAuthoritativeStreamAndRevalidatesScope" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/GenerationReferenceDomainTest.java"
require_literal "ownerlessGlobalScopeScansRecoveryRootAndLiveTailForEveryRegisteredStream" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/AppendRecoveryReferenceDomainTest.java"
require_literal "ownerlessGlobalScopePagesTheAuthoritativeTaskNamespace" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/MaterializationReferenceDomainTest.java"
require_literal "ownerlessGlobalScopeReusesExactProjectionAuthoritiesWithoutMaterializationDependency" \
    "nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/retention/ProjectionGenerationReferenceDomainTest.java"
require_literal "ownerlessGlobalScopePagesTheExactCursorNamespaceAndDetectsDrift" \
    "nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/retention/CursorSnapshotReferenceDomainTest.java"
require_literal "phase4M4GlobalDomainsCheck" "build.gradle.kts"
require_literal "phase4M4GlobalDomainsCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 activation-gated ownerless global scope and future sentinel verified."
