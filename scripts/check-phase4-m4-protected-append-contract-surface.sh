#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 protected-append $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 protected-append contract '$literal' in $path" >&2
        exit 1
    fi
}

require_ordered() {
    local path="$1"
    shift
    local previous=0
    local literal
    for literal in "$@"; do
        local line
        line="$(rg -n -F -- "$literal" "$repo_root/$path" | head -n 1 | cut -d: -f1)"
        if [[ -z "$line" || "$line" -le "$previous" ]]; then
            echo "Phase 4 M4 protected-append ordering is missing or invalid at '$literal' in $path" >&2
            exit 1
        fi
        previous="$line"
    done
}

production_artifacts=(
    PreparedStableAppend.java
    MaterializedGenerationZero.java
    ProtectedStableAppend.java
    ProtectedGenerationZero.java
    GenerationZeroPhysicalReferencePublisher.java
    DefaultGenerationZeroPhysicalReferencePublisher.java
    GenerationZeroProtectionIdentities.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done

test_artifacts=(
    Phase15MetadataContractTest.java
    DefaultStreamStorageAppendTest.java
    AppendCoordinatorLaneLifecycleTest.java
    Phase1FinalIntegrationTest.java
    GenerationPublicationOxiaS3IntegrationTest.java
    MaterializationWorkerOxiaS3IntegrationTest.java
)

for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test/integration"
done

require_ordered \
    "nereus-core/src/main/java/com/nereusstream/core/append/AppendCoordinator.java" \
    '"prepare stable append intent"' \
    '"protect stable append before head"' \
    '"commit protected stream head"' \
    '"materialize generation-zero index"' \
    '"protect visible generation-zero index"'

require_literal "prepareStableAppend" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaMetadataStore.java"
require_literal "commitPreparedStableAppend" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaMetadataStore.java"
require_literal "revalidateMaterializedGenerationZero" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaMetadataStore.java"
require_literal "validateStableAppendProtection" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaJavaClientMetadataStore.java"
require_literal "new DefaultGenerationZeroPhysicalReferencePublisher" \
    "nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/DefaultNereusRuntimeProvider.java"
require_literal "phase4M4ProtectedAppendCheck" "build.gradle.kts"
require_literal "phase4M4ProtectedAppendCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

if rg -n "commitStableAppend" "$repo_root" --glob '*.java' >/dev/null; then
    echo "retired combined commitStableAppend remains in Java sources" >&2
    exit 1
fi

raw_core_callers="$(rg -l "commitPreparedStableAppend" \
    "$repo_root/nereus-core/src/main/java" --glob '*.java' | wc -l | tr -d ' ')"
if [[ "$raw_core_callers" != "1" ]]; then
    echo "raw commitPreparedStableAppend must have exactly one nereus-core production caller" >&2
    exit 1
fi
require_literal "commitPreparedStableAppend" \
    "nereus-core/src/main/java/com/nereusstream/core/append/MetadataStableAppendCommitter.java"

echo "Phase 4 M4 protected generation-zero append/recovery surfaces verified."
