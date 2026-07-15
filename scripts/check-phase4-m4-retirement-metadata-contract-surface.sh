#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 retirement-metadata $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 retirement-metadata contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    RetirementMetadataClient.java
    RetirementMetadataKey.java
    RetirementMetadataValue.java
    GenerationZeroMarkerIdentity.java
    LegacyCommittedSliceIdentity.java
    GenericCommittedAppendIdentity.java
    VersionedGenerationZeroMarker.java
    SourceRetirementMetadataStore.java
    OxiaJavaSourceRetirementMetadataStore.java
    VersionedObjectManifestAudit.java
    VersionedObjectReferencesAudit.java
    ObjectAuditRetirementStore.java
    OxiaJavaObjectAuditRetirementStore.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done

require_basename "SourceRetirementMetadataStoreContractTest.java" "test"
require_basename "ObjectAuditRetirementStoreContractTest.java" "test"

source_store="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/retirement/OxiaJavaSourceRetirementMetadataStore.java"
audit_store="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/retirement/OxiaJavaObjectAuditRetirementStore.java"

require_literal "getCommittedMarker" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/retirement/SourceRetirementMetadataStore.java"
require_literal "deleteGenerationZeroIndex" "$source_store"
require_literal "deleteCommittedMarker" "$source_store"
require_literal "deleteCommitNode" "$source_store"
require_literal "support.requireExpected" "$source_store"
require_literal "record.metadataVersion() != 0" "$source_store"
require_literal "deleteReferences" "$audit_store"
require_literal "deleteManifest" "$audit_store"
require_literal "support.requireExpected" "$audit_store"
require_literal "record.metadataVersion() != 0" "$audit_store"
require_literal "retirementMetadataClient" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/SharedOxiaClientRuntime.java"
require_literal "OxiaJavaSourceRetirementMetadataStore.usingSharedRuntime" \
    "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/SharedOxiaClientRuntimeTest.java"
require_literal "OxiaJavaObjectAuditRetirementStore.usingSharedRuntime" \
    "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/SharedOxiaClientRuntimeTest.java"
require_literal "conditionallyDeletesBothGenerationZeroIndexEncodingsByExactBytesAndVersion" \
    "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/retirement/SourceRetirementMetadataStoreContractTest.java"
require_literal "capturesAndDeletesBothCommittedMarkerEncodingsWithoutKeyAliasing" \
    "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/retirement/SourceRetirementMetadataStoreContractTest.java"
require_literal "loadsHydratedExactAuditsAndDeletesReferencesBeforeManifest" \
    "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/retirement/ObjectAuditRetirementStoreContractTest.java"
require_literal "absenceAndLostDeleteResponseRequireCoordinatorLevelReproof" \
    "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/retirement/ObjectAuditRetirementStoreContractTest.java"
require_literal "phase4M4RetirementMetadataCheck" "build.gradle.kts"
require_literal "phase4M4RetirementMetadataCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

if rg -q "putIfAbsent|putIfVersion|rangeScan|list\(" \
        "$repo_root/nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/retirement"; then
    echo "retirement metadata package must remain read/delete-only" >&2
    exit 1
fi

if rg -q "public RetirementMetadataKey\(" \
        "$repo_root/nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/retirement/RetirementMetadataKey.java"; then
    echo "retirement exact-key capability must not have a public constructor" >&2
    exit 1
fi

echo "Phase 4 M4 exact source/object-audit retirement metadata surfaces verified."
