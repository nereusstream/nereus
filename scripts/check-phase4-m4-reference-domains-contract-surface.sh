#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 reference-domain $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 reference-domain contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    GcReferenceSnapshotAccumulator.java
    GenerationReferenceDomain.java
    AppendRecoveryReferenceDomain.java
    MaterializationReferenceDomain.java
)

test_artifacts=(
    GenerationReferenceDomainTest.java
    AppendRecoveryReferenceDomainTest.java
    MaterializationReferenceDomainTest.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done
for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

spi="nereus-core/src/main/java/com/nereusstream/core/physical/GcReferenceDomain.java"
registry="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/GcReferenceDomainRegistry.java"
plan_validation="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/GcPlanValidation.java"
generation="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/GenerationReferenceDomain.java"
append="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/AppendRecoveryReferenceDomain.java"
materialization="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/MaterializationReferenceDomain.java"

require_literal "GcReferenceQuery query, GcReferenceSnapshot snapshot" "$spi"
require_literal "registered.domain().stillMatches(query, snapshot)" "$registry"
require_literal "requireEveryReferenceHasExactRemoval" "$plan_validation"
require_literal "every domain reference must have an exact planned metadata removal" "$plan_validation"

require_literal 'DOMAIN_ID = "generation-v1"' "$generation"
require_literal "ReadView.COMMITTED, ReadView.TOPIC_COMPACTED" "$generation"
require_literal "GenerationLifecycle.DRAINING" "$generation"
require_literal "ReferenceDisposition.VETO" "$generation"
require_literal "unsupportedOwnerless" "$generation"

require_literal 'DOMAIN_ID = "append-recovery-v1"' "$append"
require_literal "getRecoveryRoot" "$append"
require_literal "readAppendRecoveryTail" "$append"
require_literal "append-recovery-commit" "$append"
require_literal "recovery-checkpoint-root" "$append"
require_literal "accumulator.veto()" "$append"
require_literal "unsupportedOwnerless" "$append"

require_literal 'DOMAIN_ID = "materialization-v1"' "$materialization"
require_literal "scanTasks" "$materialization"
require_literal "TaskLifecycle.PUBLISHED" "$materialization"
require_literal "materialization-source" "$materialization"
require_literal "materialization-output" "$materialization"
require_literal "accumulator.veto()" "$materialization"
require_literal "unsupportedOwnerless" "$materialization"

require_literal "higherGenerationMustBeDrainingBeforeItsReferenceCanEnterAPlan" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/GenerationReferenceDomainTest.java"
require_literal "liveTailReferenceVetoesAndExactRescanDetectsHeadOrCommitDrift" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/AppendRecoveryReferenceDomainTest.java"
require_literal "scansExactTaskSourcesAndDetectsAuthorityDrift" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/MaterializationReferenceDomainTest.java"
require_literal "everyDomainReferenceRequiresAnExactPlannedRemoval" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/GcPlanTest.java"
require_literal "phase4M4ReferenceDomainsCheck" "build.gradle.kts"
require_literal "phase4M4ReferenceDomainsCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

for domain in "$generation" "$append" "$materialization"; do
    if rg -q "scanStreamRegistrations|getStreamRegistration" "$repo_root/$domain"; then
        echo "affected-stream reference domains must not treat stream registration hints as ownerless proof" >&2
        exit 1
    fi
done

echo "Phase 4 M4 query-bound generation/append/materialization reference domains verified."
