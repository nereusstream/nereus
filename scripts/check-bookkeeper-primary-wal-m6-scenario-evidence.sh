#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
pulsar_root="${1:-${NEREUS_PULSAR_CHECKOUT:-/Users/liusinan/apps/ideaproject/nereusstream/pulsar}}"
matrix="$repo_root/docs/phase-bk-bookkeeper-primary-wal/09-m6-executable-evidence-matrix.md"
build="$repo_root/build.gradle.kts"

fail() {
    echo "BookKeeper primary-WAL M6 evidence: $*" >&2
    exit 1
}

[[ -f "$matrix" ]] || fail "missing $matrix"
[[ -d "$pulsar_root" ]] || fail "missing locked Pulsar checkout $pulsar_root"

row_count="$(awk -F'|' '$2 ~ /^[[:space:]]*BK-[0-9][0-9][[:space:]]*$/ {count++} END {print count + 0}' "$matrix")"
[[ "$row_count" -eq 10 ]] || fail "expected exactly 10 BK-M6 rows, found $row_count"

seen=()
while IFS= read -r row; do
    IFS='|' read -r _ raw_id raw_evidence raw_gate _ <<<"$row"
    id="$(tr -d '[:space:]' <<<"$raw_id")"
    evidence="$(sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/`//g' -e 's/<br>/;/g' <<<"$raw_evidence")"
    gate="$(tr -d '[:space:]`' <<<"$raw_gate")"

    [[ "$id" =~ ^BK-([0-9][0-9])$ ]] || fail "invalid scenario id '$id'"
    number=$((10#${BASH_REMATCH[1]}))
    (( number >= 87 && number <= 96 )) || fail "$id is outside BK-87..BK-96"
    [[ -z "${seen[$number]:-}" ]] || fail "duplicate scenario $id"
    seen[$number]=1
    [[ "$gate" =~ ^bookKeeperPrimaryWalM6(Scale|Chaos|Compatibility)Check$ ]] \
        || fail "$id has invalid owning gate '$gate'"
    rg -q "tasks\.register([^\n]*)?\(\"$gate\"\)" "$build" \
        || fail "$id owns undeclared gate $gate"

    IFS=';' read -ra entries <<<"$evidence"
    for entry in "${entries[@]}"; do
        entry="$(sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' <<<"$entry")"
        [[ "$entry" =~ ^(nereus|pulsar):([^#]+)#([A-Za-z_][A-Za-z0-9_]*)$ ]] \
            || fail "$id has invalid evidence token '$entry'"
        repository="${BASH_REMATCH[1]}"
        relative="${BASH_REMATCH[2]}"
        method="${BASH_REMATCH[3]}"
        if [[ "$repository" == "nereus" ]]; then
            source="$repo_root/$relative"
        else
            source="$pulsar_root/$relative"
        fi
        [[ -f "$source" ]] || fail "$id evidence file is missing: $source"
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
        ' "$source" || fail "$id method $method is absent or not test-annotated in $source"
    done
done < <(awk -F'|' '$2 ~ /^[[:space:]]*BK-[0-9][0-9][[:space:]]*$/ {print}' "$matrix")

for number in $(seq 87 96); do
    [[ -n "${seen[$number]:-}" ]] || fail "missing BK-$number"
done

required_build_wiring=(
    ':nereus-bookkeeper:bkM6ScaleTest'
    ':nereus-materialization:bkM6MixedSourceScaleTest'
    ':nereus-core:bkM6GenerationScaleTest'
    ':nereus-bookkeeper:bkM6ChaosTest'
    'bookKeeperPrimaryWalM2AllocationAuthorityCheck'
    'bookKeeperPrimaryWalM3ResponseLossCheck'
    'bookKeeperPrimaryWalM5Check'
    'phase4M6TwoBrokerWorkerContentionPulsarCheck'
    'phase4M5AsyncRetentionMultiBrokerPulsarCheck'
)
for literal in "${required_build_wiring[@]}"; do
    rg -Fq -- "$literal" "$build" || fail "missing aggregate wiring '$literal'"
done

echo "BookKeeper primary-WAL M6 executable evidence: BK-87..BK-96 traced"
