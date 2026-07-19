#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
design_dir="$repo_root/docs/phase-bk-bookkeeper-primary-wal"
nereus_audit_lock="35c58c575c3da220633c53e48a581f16756ea047"
pulsar_source_lock="eaf7b9a704890a9265c21f30d9f351e02d00c600"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing BookKeeper primary-WAL documentation contract '$literal' in $path" >&2
        exit 1
    fi
}

required_docs=(
    README.md
    01-current-contract-and-source-audit.md
    02-domain-api-module-and-target-contract.md
    03-oxia-metadata-ledger-lifecycle-and-codecs.md
    04-append-read-recovery-and-fencing.md
    05-retention-materialization-and-completion.md
    06-pulsar-runtime-rollout-and-compatibility.md
    07-implementation-plan-and-gates.md
    08-scenario-evidence-matrix.md
)

for name in "${required_docs[@]}"; do
    if [[ ! -f "$design_dir/$name" ]]; then
        echo "missing BookKeeper primary-WAL design document: $name" >&2
        exit 1
    fi
done

require_literal "$nereus_audit_lock" \
    "docs/phase-bk-bookkeeper-primary-wal/01-current-contract-and-source-audit.md"
require_literal "$nereus_audit_lock" \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
for path in \
    docs/phase-bk-bookkeeper-primary-wal/README.md \
    docs/phase-bk-bookkeeper-primary-wal/01-current-contract-and-source-audit.md \
    docs/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md \
    docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md; do
    require_literal "$pulsar_source_lock" "$path"
done

require_literal "BookKeeper 4.18.0" \
    "docs/phase-bk-bookkeeper-primary-wal/01-current-contract-and-source-audit.md"
require_literal "F1-BK / BookKeeper Primary WAL Delivery" \
    "docs/phase-bk-bookkeeper-primary-wal/README.md"
require_literal "not implementation evidence" \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'ONE_NEREUS_ENTRY_PER_BOOKKEEPER_ENTRY' \
    "docs/phase-bk-bookkeeper-primary-wal/README.md"
require_literal 'CreateAdvBuilder.withLedgerId(long)' \
    "docs/phase-bk-bookkeeper-primary-wal/README.md"
require_literal 'withRecovery(false)' \
    "docs/phase-bk-bookkeeper-primary-wal/04-append-read-recovery-and-fencing.md"
require_literal 'REQUIRED_OBJECT_GENERATION' \
    "docs/phase-bk-bookkeeper-primary-wal/05-retention-materialization-and-completion.md"
require_literal 'No online migration in BK-M0–M6' \
    "docs/phase-bk-bookkeeper-primary-wal/06-pulsar-runtime-rollout-and-compatibility.md"
require_literal 'BK-96' \
    "docs/phase-bk-bookkeeper-primary-wal/08-scenario-evidence-matrix.md"
require_literal 'bookKeeperPrimaryWalDocumentationCheck' \
    "docs/phase-bk-bookkeeper-primary-wal/07-implementation-plan-and-gates.md"
require_literal 'bookKeeperPrimaryWalDocumentationCheck' "build.gradle.kts"

global_links=(
    README.md
    docs/design/README.md
    docs/design/nereus-design-index.md
    docs/design/nereus-futures.md
    docs/design/nereus-future1-core-stream-storage.md
    docs/design/nereus-commit-protocol.md
    docs/design/nereus-overall-architecture.md
    docs/design/nereus-terminology.md
    docs/phase-1.5-core-storage-foundation/README.md
    docs/phase-4-compaction-generation/README.md
)
for path in "${global_links[@]}"; do
    require_literal "phase-bk-bookkeeper-primary-wal" "$path"
done

if rg -n --glob '*.md' \
    'BK-M[1-6][[:space:]:=-]+(complete|final-gated)|BookKeeper primary WAL[[:space:]:=-]+Implemented|Implemented[[:space:]:=-]+BookKeeper primary WAL' \
    "$design_dir"; then
    echo "BookKeeper primary-WAL design incorrectly claims implementation completion" >&2
    exit 1
fi

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
done < <(rg --with-filename --no-heading -o --glob '*.md' '\]\(([^)]+)\)' "$design_dir")

echo "BookKeeper primary-WAL design status, source locks, contracts, links, and scenario range verified."
