#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
pulsar_root="${1:-${NEREUS_PULSAR_CHECKOUT:-$repo_root/../nereusstream/pulsar}}"
matrix="$repo_root/docs/phase-4-compaction-generation/08-m6-scenario-evidence-matrix.md"
build="$repo_root/build.gradle.kts"

fail() {
    echo "Phase 4 M6 scenario evidence matrix: $*" >&2
    exit 1
}

[[ -f "$matrix" ]] || fail "missing $matrix"
[[ -d "$pulsar_root" ]] || fail "missing Pulsar checkout $pulsar_root"

row_count="$(awk -F'|' '$2 ~ /^[[:space:]]*[0-9][0-9][[:space:]]*$/ {count++} END {print count + 0}' "$matrix")"
[[ "$row_count" -eq 52 ]] || fail "expected exactly 52 scenario rows, found $row_count"

seen=()
while IFS= read -r row; do
    IFS='|' read -r _ raw_id _ raw_evidence raw_gate _ <<<"$row"
    id="$(tr -d '[:space:]' <<<"$raw_id")"
    evidence="$(sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/`//g' -e 's/<br>/;/g' <<<"$raw_evidence")"
    gate="$(tr -d '[:space:]`' <<<"$raw_gate")"

    [[ "$id" =~ ^[0-9]{2}$ ]] || fail "non-canonical scenario id '$id'"
    number=$((10#$id))
    (( number >= 1 && number <= 52 )) || fail "scenario id $id is outside 01..52"
    [[ -z "${seen[$number]:-}" ]] || fail "duplicate scenario id $id"
    seen[$number]=1
    [[ -n "$evidence" ]] || fail "scenario $id has no executable evidence"
    [[ "$gate" =~ ^[A-Za-z0-9]+$ ]] || fail "scenario $id has invalid gate '$gate'"
    rg -q "tasks\.register([^\n]*)?\(\"$gate\"\)" "$build" \
        || fail "scenario $id owns undeclared gate $gate"

    IFS=';' read -ra entries <<<"$evidence"
    for entry in "${entries[@]}"; do
        entry="$(sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' <<<"$entry")"
        [[ "$entry" =~ ^(nereus|pulsar):([^#]+)#([A-Za-z_][A-Za-z0-9_]*)$ ]] \
            || fail "scenario $id has invalid evidence token '$entry'"
        repository="${BASH_REMATCH[1]}"
        relative="${BASH_REMATCH[2]}"
        method="${BASH_REMATCH[3]}"
        if [[ "$repository" == "nereus" ]]; then
            source="$repo_root/$relative"
        else
            source="$pulsar_root/$relative"
        fi
        [[ -f "$source" ]] || fail "scenario $id evidence file is missing: $source"
        awk -v method="$method" '
            { lines[NR] = $0 }
            index($0, method "(") || $0 ~ method "[[:space:]]*\\(" {
                for (line = NR - 8; line < NR; line++) {
                    if (line > 0 && lines[line] ~ /@(Test|ParameterizedTest)/) {
                        found = 1
                    }
                }
            }
            END { exit(found ? 0 : 1) }
        ' "$source" || fail "scenario $id method $method is absent or not test-annotated in $source"
    done
done < <(awk -F'|' '$2 ~ /^[[:space:]]*[0-9][0-9][[:space:]]*$/ {print}' "$matrix")

for number in $(seq 1 52); do
    printf -v id '%02d' "$number"
    [[ -n "${seen[$number]:-}" ]] || fail "missing scenario id $id"
done

echo "Phase 4 M6 executable evidence matrix: 52/52 scenarios traced"
