#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 source/protection-cut contract '$literal' in $path" >&2
        exit 1
    fi
}

coordinator="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/SourceRetirementCoordinator.java"
unit_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/SourceRetirementCoordinatorTest.java"
integration_test="nereus-pulsar-adapter/src/f4M4IntegrationTest/java/com/nereusstream/pulsar/Phase4PhysicalGcOxiaS3IntegrationTest.java"

for path in "$coordinator" "$unit_test" "$integration_test"; do
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 source/protection-cut artifact: $path" >&2
        exit 1
    fi
done

require_literal \
    "return retireMetadata(context, journal.plannedMetadataRemovals(), counts, deadline)" \
    "$coordinator"
require_literal "thenCompose(authenticated -> retireProtections(" "$coordinator"
require_literal "thenCompose(authenticated -> deletePhysicalObject(authenticated, deadline)" \
    "$coordinator"
require_literal "conditionally delete journaled object protection" "$coordinator"
require_literal "object protections remain after journaled retirement" "$coordinator"

require_literal \
    "restartAfterMetadataDeleteBeforeProtectionRetirementFinishesExactJournal" \
    "$unit_test"
require_literal \
    "restartAfterProtectionDeleteBeforeObjectDeleteFinishesExactJournal" \
    "$unit_test"
require_literal "lostProtectionDeleteResponseProvesAbsenceBeforeDeletingObject" "$unit_test"
require_literal "injected process loss after metadata deletion" "$unit_test"
require_literal "injected process loss after protection deletion" "$unit_test"
require_literal "injected protection delete response loss" "$unit_test"
require_literal "assertDeletingAndObjectPresent(fixture)" "$unit_test"

require_literal \
    "freshProcessRecoversJournaledMetadataAndProtectionPostDeleteCuts" \
    "$integration_test"
require_literal "seedDeletingPostRetirementCut(metadataCut, true, false)" "$integration_test"
require_literal "seedDeletingPostRetirementCut(protectionCut, false, true)" "$integration_test"
require_literal "PhysicalStoreDecorator.failRootScansWhen" "$integration_test"
require_literal "PhysicalStoreDecorator.loseProtectionDeleteResponse" "$integration_test"
require_literal "injected response loss after successful protection delete" "$integration_test"
require_literal "recovered.assertObjectAbsent(metadataCut)" "$integration_test"
require_literal "recovered.assertObjectAbsent(protectionCut)" "$integration_test"

require_literal "phase4M4SourceProtectionCutCheck" "build.gradle.kts"
require_literal "Checkpoint BA" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4SourceProtectionCutCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 source/protection retirement process-cut and response-loss contract surface: PASS"
