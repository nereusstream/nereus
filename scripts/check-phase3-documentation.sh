#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
current_pulsar_lock="f52108468837917234637c514eb7524b9b3fb5f8"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 3 documentation contract '$literal' in $path" >&2
        exit 1
    fi
}

forbid_literal() {
    local literal="$1"
    shift
    if rg -Fn -- "$literal" "$@"; then
        echo "stale Phase 3 documentation status '$literal'" >&2
        exit 1
    fi
}

authoritative_docs=(
    "$repo_root/README.md"
    "$repo_root/docs/phase-3-cursor-subscription/README.md"
    "$repo_root/docs/phase-3-cursor-subscription/01-pulsar-api-and-call-path-audit.md"
    "$repo_root/docs/phase-3-cursor-subscription/05-facade-broker-and-future-compatibility.md"
    "$repo_root/docs/phase-3-cursor-subscription/06-implementation-plan-and-gates.md"
    "$repo_root/docs/design/nereus-design-index.md"
    "$repo_root/docs/design/nereus-future3-cursor-subscription.md"
)

for path in "${authoritative_docs[@]}"; do
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 3 authoritative document: $path" >&2
        exit 1
    fi
    if ! rg -Fq -- "$current_pulsar_lock" "$path"; then
        echo "current Pulsar source lock is missing from ${path#"$repo_root/"}" >&2
        exit 1
    fi
done

require_literal "Implemented / final-gated" "docs/phase-3-cursor-subscription/README.md"
require_literal "F3-M1-M6 implemented/final-gated" "docs/design/nereus-commit-protocol.md"
require_literal "CursorSnapshotInventory" "docs/phase-3-cursor-subscription/03-oxia-metadata-and-snapshot-format.md"
require_literal "read-only F4 snapshot inventory" "docs/phase-3-cursor-subscription/README.md"
require_literal 'terminal `DELETED`' "docs/phase-2-managed-ledger-facade/README.md"
require_literal "immediate same-name open" "docs/phase-2-managed-ledger-facade/06-code-level-interface-contract.md"
require_literal "Cursor snapshot inventory" "docs/design/nereus-terminology.md"
require_literal "never authorizes deletion" "docs/design/nereus-terminology.md"

for gate in phase3M6Check phase3M6FinalCheck phase3Check phase3FinalCheck; do
    require_literal "$gate" "build.gradle.kts"
    require_literal "$gate" "docs/phase-3-cursor-subscription/06-implementation-plan-and-gates.md"
done

for stale in "M6 pending" "M6 next"; do
    forbid_literal \
        "$stale" \
        "$repo_root/docs/phase-3-cursor-subscription" \
        "$repo_root/docs/design/nereus-future3-cursor-subscription.md"
done

for stale in \
    "F3-M6 pending" \
    "F3-M6 next" \
    "Phase 3 M6 pending" \
    "Phase 3 M6 next" \
    "Designed / In progress" \
    "Designed/In progress" \
    "M1-M5 complete/gated" \
    "F3-M1-M5" \
    "F3 cursor protocol 已通过 design-only M0/M0R 并冻结到代码级但尚未实现" \
    "Status: F3-M0/M0R design-gated；not implemented"; do
    forbid_literal "$stale" "$repo_root/README.md" "$repo_root/docs"
done

link_docs=(
    "$repo_root/README.md"
    "$repo_root/docs/phase-3-cursor-subscription"
    "$repo_root/docs/design/README.md"
    "$repo_root/docs/design/nereus-design-index.md"
    "$repo_root/docs/design/nereus-future3-cursor-subscription.md"
    "$repo_root/docs/design/nereus-future4-compaction-generation.md"
    "$repo_root/docs/design/nereus-futures.md"
    "$repo_root/docs/design/nereus-overall-architecture.md"
    "$repo_root/docs/design/nereus-terminology.md"
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

echo "Phase 3 documentation status, source lock, compatibility, and F4 handoff verified."
