#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M4 retirement-journal $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 retirement-journal contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    GcRetirementManifestRecord.java
    GcRetirementProtectionRecord.java
    GcRetirementRemovalRecord.java
    VersionedGcRetirementManifest.java
    VersionedGcRetirementProtection.java
    VersionedGcRetirementRemoval.java
    GcRetirementProtectionScanPage.java
    GcRetirementRemovalScanPage.java
    GcRetirementJournal.java
    GcRetirementJournalSnapshot.java
    DefaultGcRetirementJournal.java
    PhysicalObjectGarbageCollector.java
)

test_artifacts=(
    GcRetirementJournalMetadataStoreContractTest.java
    DefaultGcRetirementJournalTest.java
    PhysicalObjectGarbageCollectorTest.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done
for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

keyspace="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/F4Keyspace.java"
store="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/PhysicalObjectMetadataStore.java"
journal="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/DefaultGcRetirementJournal.java"
collector="nereus-materialization/src/main/java/com/nereusstream/materialization/gc/PhysicalObjectGarbageCollector.java"
collector_test="nereus-materialization/src/test/java/com/nereusstream/materialization/gc/PhysicalObjectGarbageCollectorTest.java"

require_literal "gcRetirementManifestKey" "$keyspace"
require_literal 'sha256Hex("protection\0"' "$keyspace"
require_literal 'sha256Hex("removal\0"' "$keyspace"
require_literal "createRetirementManifest" "$store"
require_literal "scanRetirementProtections" "$store"
require_literal "scanRetirementRemovals" "$store"

require_literal "writeAndSeal" "$journal"
require_literal "scanExactEntries" "$journal"
require_literal "sealManifest" "$journal"
require_literal "referenceSetSha256" "$journal"
require_literal "existing GC retirement journal conflicts with the exact source plan" "$journal"

require_literal "retirementJournal.prepare" "$collector"
require_literal "prepare sealed GC retirement journal before mark" "$collector"
require_literal "loadExactRetirementJournal" "$collector"
require_literal "requireExactRetirementJournal" "$collector"
require_literal "sealed GC retirement journal is missing for the MARKED root" "$collector"

require_literal "journalPrepareFailureLeavesRootActiveBeforeActivationOrMark" "$collector_test"
require_literal "deleteIntentReloadsExactJournalAtAdmissionAndFinalFence" "$collector_test"
require_literal "journalDisappearanceAtFinalFenceLeavesRootMarked" "$collector_test"
require_literal "convergesWhenEntryAndManifestCreateResponsesAreLostAfterCommit" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/DefaultGcRetirementJournalTest.java"
require_literal "neverSealsManifestWhenAnyJournalEntryIsMissing" \
    "nereus-materialization/src/test/java/com/nereusstream/materialization/gc/DefaultGcRetirementJournalTest.java"
require_literal "phase4M4RetirementJournalCheck" "build.gradle.kts"
require_literal "phase4M4RetirementJournalCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

prepare_line="$(rg -n -F 'retirementJournal.prepare(' "$repo_root/$collector" | head -1 | cut -d: -f1)"
mark_line="$(rg -n -F 'markCas(' "$repo_root/$collector" | head -1 | cut -d: -f1)"
if [[ -z "$prepare_line" || -z "$mark_line" || "$prepare_line" -ge "$mark_line" ]]; then
    echo "collector source must visibly prepare the retirement journal before invoking markCas" >&2
    exit 1
fi

echo "Phase 4 M4 manifest-last retirement journal and PREPARE-before-MARK fence verified."
