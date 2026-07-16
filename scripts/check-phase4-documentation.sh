#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
current_pulsar_lock="c2f7c22fdc562022b992a5c7aecb5fd5c02d318d"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 documentation contract '$literal' in $path" >&2
        exit 1
    fi
}

lock_docs=(
    README.md
    docs/phase-4-compaction-generation/README.md
    docs/phase-4-compaction-generation/01-current-contract-and-source-audit.md
    docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md
)
for path in "${lock_docs[@]}"; do
    require_literal "$current_pulsar_lock" "$path"
done

require_literal "activation schema 的 49 个 envelope vectors" "docs/phase-4-compaction-generation/README.md"
require_literal "GcRetirementManifestRecord" \
    "docs/phase-4-compaction-generation/03-oxia-metadata-and-publication.md"
require_literal "PREPARE RETIREMENT JOURNAL" \
    "docs/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "Checkpoint L's protocol foundation" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "publication id before allocating a generation" \
    "docs/phase-4-compaction-generation/03-oxia-metadata-and-publication.md"
require_literal "GenerationMetadataTransitions" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "F4-M1、F4-M2 and F4-M3 are complete/final-gated" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "F4-M4 implementation checkpoint A" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CheckpointCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ProtectedAppendCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RecoveryRootCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CheckpointReplayCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CheckpointIndexRepairCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RetirementMetadataCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4GcPlanCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RootFenceCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ReferenceDomainsCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ManagedLedgerDomainsCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RetirementJournalCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ActivationMetadataCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4GlobalDomainsCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4TombstoneRetirementCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "Checkpoint U" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "F4-M4 NRC1 object-protocol checkpoint" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "F4-M4 protected generation-zero append checkpoint" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "slash-aware fixed-depth" "docs/phase-4-compaction-generation/03-oxia-metadata-and-publication.md"
require_literal "SDK response succeeds" "docs/phase-4-compaction-generation/02-domain-api-and-object-format.md"
require_literal "HTTP 405 or 501" "docs/phase-4-compaction-generation/02-domain-api-and-object-format.md"

for gate in phase4M1Check phase4M1FinalCheck; do
    require_literal "$gate" "build.gradle.kts"
    require_literal "$gate" "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
done

link_docs=(
    "$repo_root/README.md"
    "$repo_root/docs/phase-4-compaction-generation"
    "$repo_root/docs/design/README.md"
    "$repo_root/docs/design/nereus-design-index.md"
    "$repo_root/docs/design/nereus-future4-compaction-generation.md"
)
while IFS=: read -r source match; do
    target="${match#*](}"
    target="${target%)}"
    target="${target#<}"
    target="${target%>}"
    case "$target" in
        ""|\#*|*://*|mailto:*|app://*) continue ;;
    esac
    target="${target%%#*}"
    target="${target//%20/ }"
    if [[ "$target" = /* ]]; then
        resolved="$target"
    else
        resolved="$(dirname "$source")/$target"
    fi
    if [[ ! -e "$resolved" ]]; then
        echo "broken local Markdown link in ${source#"$repo_root/"}: $target" >&2
        exit 1
    fi
done < <(rg --with-filename --no-heading -o --glob '*.md' '\]\(([^)]+)\)' "${link_docs[@]}")

echo "Phase 4 M1-M3 final status plus current M4 checkpoints/tombstone retirement, source lock, gates, and local links verified."
