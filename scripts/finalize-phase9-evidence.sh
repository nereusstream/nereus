#!/usr/bin/env bash
set -euo pipefail

repo="${1:?repository path is required}"
pre_evidence="${2:?pre-evidence path is required}"
result_directory="${3:?aggregator result directory is required}"
output="${4:?output path is required}"
manifest="$repo/docs/phase-9-kafka-native-storage/f9-scenarios.json"

fail() {
    echo "F9 final evidence: $*" >&2
    exit 1
}

json_quote() {
    printf '%s' "$1" \
        | sed 's/\\/\\\\/g; s/"/\\"/g; s/^/"/; s/$/"/'
}

[[ -f "$pre_evidence" ]] || fail "missing pre-evidence $pre_evidence"
[[ -f "$manifest" ]] || fail "missing manifest $manifest"
[[ -d "$result_directory" ]] \
    || fail "missing aggregator result directory $result_directory"

tests=0
skipped=0
failures=0
errors=0
result_ids_file="$(mktemp)"
manifest_ids_file="$(mktemp)"
trap 'rm -f "$result_ids_file" "$manifest_ids_file"' EXIT

found_xml=false
for xml_file in "$result_directory"/TEST-*.xml; do
    [[ -f "$xml_file" ]] || continue
    found_xml=true
    suite_tests="$(sed -n -E 's/.*<testsuite[^>]* tests="([0-9]+)".*/\1/p' "$xml_file" | head -n 1)"
    suite_skipped="$(sed -n -E 's/.*<testsuite[^>]* skipped="([0-9]+)".*/\1/p' "$xml_file" | head -n 1)"
    suite_failures="$(sed -n -E 's/.*<testsuite[^>]* failures="([0-9]+)".*/\1/p' "$xml_file" | head -n 1)"
    suite_errors="$(sed -n -E 's/.*<testsuite[^>]* errors="([0-9]+)".*/\1/p' "$xml_file" | head -n 1)"
    [[ -n "$suite_tests" && -n "$suite_skipped" && -n "$suite_failures" && -n "$suite_errors" ]] \
        || fail "cannot parse aggregator suite $xml_file"
    tests=$((tests + suite_tests))
    skipped=$((skipped + suite_skipped))
    failures=$((failures + suite_failures))
    errors=$((errors + suite_errors))
    perl -ne '
        while (/<testcase name="([^"]+)" classname="([^"]+)"/g) {
            if ($1 =~ /(KF-[A-Z0-9]+-[0-9]{3})/) {
                print "$1\n";
            } elsif ($1 =~ /scenarioKfScl010/) {
                print "KF-SCL-010\n";
            } else {
                die "testcase lacks F9 ID: $1\n";
            }
        }
    ' "$xml_file" >>"$result_ids_file"
done
[[ "$found_xml" == "true" ]] || fail "missing aggregator JUnit XML"

[[ "$tests" -eq 146 && "$skipped" -eq 0 && "$failures" -eq 0 && "$errors" -eq 0 ]] \
    || fail "aggregator must report 146 passes: tests=$tests skipped=$skipped failures=$failures errors=$errors"
result_count="$(wc -l <"$result_ids_file" | tr -d ' ')"
result_unique="$(sort -u "$result_ids_file" | wc -l | tr -d ' ')"
[[ "$result_count" -eq 146 && "$result_unique" -eq 146 ]] \
    || fail "aggregator IDs missing or duplicated: $result_count/$result_unique"

rg -o '"id": "KF-[A-Z0-9]+-[0-9]{3}"' "$manifest" \
    | sed -E 's/^"id": "([^"]+)"$/\1/' \
    | sort -u >"$manifest_ids_file"
sort -u "$result_ids_file" >"$result_ids_file.sorted"
trap 'rm -f "$result_ids_file" "$result_ids_file.sorted" "$manifest_ids_file"' EXIT
cmp -s "$result_ids_file.sorted" "$manifest_ids_file" \
    || fail "aggregator IDs differ from the manifest"

mkdir -p "$(dirname "$output")"
temporary_output="$output.tmp"
{
    printf '{\n'
    printf '  "schemaVersion":1,\n'
    printf '  "scenarioId":"KF-SCL-010",\n'
    printf '  "status":"PASS",\n'
    printf '  "scenarioCount":146,\n'
    printf '  "aggregator":{"tests":146,"skipped":0,"failures":0,"errors":0},\n'
    printf '  "scenarioIds":[\n'
    first_id=true
    while IFS= read -r scenario_id; do
        if [[ "$first_id" == "false" ]]; then
            printf ',\n'
        fi
        printf '    %s' "$(json_quote "$scenario_id")"
        first_id=false
    done <"$manifest_ids_file"
    printf '\n  ],\n'
    printf '  "preEvidence":'
    sed 's/^/  /' "$pre_evidence"
    printf '}\n'
} >"$temporary_output"
mv "$temporary_output" "$output"

if command -v jq >/dev/null 2>&1; then
    jq -e '
        .scenarioId == "KF-SCL-010"
        and .status == "PASS"
        and .scenarioCount == 146
        and (.scenarioIds | length == 146)
        and .preEvidence.rerunTasks == true
    ' "$output" >/dev/null
fi
echo "F9 final evidence: 146/146 scenario results synchronized and passing"
