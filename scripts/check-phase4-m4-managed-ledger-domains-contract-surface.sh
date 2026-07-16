#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 managed-ledger domain $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 managed-ledger domain contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    GcReferenceDomainConfig.java
    GcReferenceSnapshotBuilder.java
    ManagedLedgerProtocolProperties.java
    ManagedLedgerGenerationProtocol.java
    ManagedLedgerStreamProjection.java
    VersionedTopicProjection.java
    VersionedVirtualLedgerProjection.java
    CursorMetadataDigests.java
    ProjectionGenerationReferenceDomain.java
    CursorSnapshotReferenceDomain.java
)

test_artifacts=(
    GcReferenceSnapshotBuilderTest.java
    ManagedLedgerGenerationProtocolTest.java
    ProjectionGenerationReferenceDomainTest.java
    CursorSnapshotReferenceDomainTest.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done
for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

projection_store="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/ManagedLedgerProjectionMetadataStore.java"
projection_core="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/ProjectionMetadataStoreCore.java"
properties="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/ManagedLedgerProtocolProperties.java"
projection_domain="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/retention/ProjectionGenerationReferenceDomain.java"
cursor_domain="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/retention/CursorSnapshotReferenceDomain.java"

require_literal "getProjectionByStream" "$projection_store"
require_literal "activateGenerationProtocol" "$projection_store"
require_literal "readVirtualLedgerProjection" "$projection_core"
require_literal "readTopicAuthority" "$projection_core"
require_literal "sha256(stored.value())" "$projection_core"
require_literal "ManagedLedgerCursorProtocol.PROPERTY" "$properties"
require_literal "ManagedLedgerGenerationProtocol.PROPERTY" "$properties"
require_literal "preserveMarker" "$properties"

require_literal 'DOMAIN_ID = "projection-generation-v1"' "$projection_domain"
require_literal "getProjectionByStream" "$projection_domain"
require_literal "ManagedLedgerGenerationProtocol.isActivated" "$projection_domain"
require_literal "ManagedLedgerFacadeState.DELETED" "$projection_domain"
require_literal "currentIdentity.incarnation() > historical.incarnation()" "$projection_domain"
require_literal "GcGlobalReferenceScope.unsupported()" "$projection_domain"
require_literal "GcGlobalReferenceScope.resolveStreams" "$projection_domain"

require_literal 'DOMAIN_ID = "cursor-snapshot-v1"' "$cursor_domain"
require_literal "getRetention" "$cursor_domain"
require_literal "scanCursors" "$cursor_domain"
require_literal "CursorRetentionLifecycle.ACTIVE" "$cursor_domain"
require_literal '"cursor-snapshot-root"' "$cursor_domain"
require_literal "GcGlobalReferenceScope.unsupported()" "$cursor_domain"
require_literal "GcGlobalReferenceScope.resolveStreams" "$cursor_domain"

require_literal "lostGenerationActivationResponseConvergesFromTheExactTopicAuthority" \
    "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/ManagedLedgerGenerationProtocolTest.java"
require_literal "topicPublishedBeforeDerivedBindingRepairCannotAuthorizeDeletion" \
    "nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/retention/ProjectionGenerationReferenceDomainTest.java"
require_literal "completePagedRootScanVetoesLiveReferenceAndRevalidatesEveryAuthority" \
    "nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/retention/CursorSnapshotReferenceDomainTest.java"
require_literal "phase4M4ManagedLedgerDomainsCheck" "build.gradle.kts"
require_literal "phase4M4ManagedLedgerDomainsCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

if rg -n 'com\.nereusstream\.materialization|scanStreamRegistrations|getStreamRegistration' \
        "$repo_root/$projection_domain" "$repo_root/$cursor_domain"; then
    echo "managed-ledger domains must use exact F2/F3 authorities, not materialization or registration hints" >&2
    exit 1
fi

echo "Phase 4 M4 F2 projection and F3 cursor reference domains verified."
