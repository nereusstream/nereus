#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
matrix="$repo_root/docs/phase-9-kafka-native-storage/08-scenario-evidence-matrix.md"
manifest="$repo_root/docs/phase-9-kafka-native-storage/f9-scenarios.json"

fail() {
    echo "F9 scenario manifest: $*" >&2
    exit 1
}

[[ -f "$matrix" ]] || fail "missing $matrix"
[[ -f "$manifest" ]] || fail "missing $manifest"
first_nonempty="$(awk 'NF {print; exit}' "$manifest")"
last_nonempty="$(awk 'NF {last = $0} END {print last}' "$manifest")"
[[ "$first_nonempty" == "{" ]] || fail "manifest must start with a JSON object"
[[ "$last_nonempty" == "}" ]] || fail "manifest must end with a JSON object"

markdown_ids="$(rg -o 'KF-[A-Z][A-Z0-9]*-[0-9]{3}' "$matrix" | sort -u)"
manifest_ids="$(rg -o '"id": "KF-[A-Z][A-Z0-9]*-[0-9]{3}"' "$manifest" \
    | sed -E 's/^"id": "([^"]+)"$/\1/' | sort -u)"
markdown_count="$(wc -l <<<"$markdown_ids" | tr -d ' ')"
manifest_count="$(wc -l <<<"$manifest_ids" | tr -d ' ')"

[[ "$markdown_count" -eq 146 ]] || fail "expected 146 Markdown IDs, found $markdown_count"
[[ "$manifest_count" -eq 146 ]] || fail "expected 146 manifest IDs, found $manifest_count"
[[ "$markdown_ids" == "$manifest_ids" ]] || fail "Markdown and manifest ID sets differ"

raw_id_count="$(rg -c '^[[:space:]]+"id": "KF-' "$manifest")"
[[ "$raw_id_count" -eq 146 ]] || fail "manifest contains duplicate or malformed IDs"

for field in milestone task testClass testMethod scenario evidenceTier requiredServices sourceLocks gates status; do
    count="$(rg -c "^[[:space:]]+\"$field\":" "$manifest")"
    [[ "$count" -eq 146 ]] || fail "expected field '$field' exactly 146 times, found $count"
done

status_count="$(rg -c '^[[:space:]]+"status": "(PLANNED|IMPLEMENTED_NOT_RUN|PASSED_CURRENT_SOURCE|FAILED|BLOCKED_ENVIRONMENT)"' "$manifest")"
[[ "$status_count" -eq 146 ]] || fail "manifest contains an invalid status"

method_count="$(rg -o '"testMethod": "scenarioKf[A-Za-z0-9]+"' "$manifest" | sort -u | wc -l | tr -d ' ')"
[[ "$method_count" -eq 146 ]] || fail "planned test methods must be unique and canonical"

echo "F9 scenario manifest: 146/146 planned scenarios synchronized"
