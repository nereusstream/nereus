#!/usr/bin/env bash
set -euo pipefail

repo="${1:?repository path is required}"
automq_reference="${2:?AutoMQ reference path is required}"
kafka_fork="${3:?Kafka fork path is required}"
pulsar_fork="${4:?Pulsar fork path is required}"
output="${5:?output path is required}"
rerun_tasks="${6:?rerun flag is required}"

manifest="$repo/docs/v1/phase-9-kafka-native-storage/f9-scenarios.json"
matrix="$repo/docs/v1/phase-9-kafka-native-storage/08-scenario-evidence-matrix.md"
compatibility_report="$repo/nereus-kafka-adapter/build/f9-kafka-client-compatibility-evidence/compatibility-report.json"
performance_report="$repo/nereus-kafka-adapter/build/f9-kafka-performance-evidence/performance-report.json"

fail() {
    echo "F9 final evidence: $*" >&2
    exit 1
}

json_quote() {
    printf '%s' "$1" \
        | sed 's/\\/\\\\/g; s/"/\\"/g; s/^/"/; s/$/"/'
}

sha256_file() {
    shasum -a 256 "$1" | awk '{print $1}'
}

[[ "$rerun_tasks" == "true" ]] \
    || fail "phase9FinalCheck requires --rerun-tasks"
[[ -f "$manifest" ]] || fail "missing manifest $manifest"
[[ -f "$matrix" ]] || fail "missing matrix $matrix"
[[ -f "$compatibility_report" ]] \
    || fail "missing compatibility report $compatibility_report"
[[ -f "$performance_report" ]] \
    || fail "missing performance report $performance_report"

product_branch="$(git -C "$repo" branch --show-current)"
product_commit="$(git -C "$repo" rev-parse HEAD)"
product_status="$(git -C "$repo" status --porcelain)"
automq_branch="$(git -C "$automq_reference" branch --show-current)"
automq_commit="$(git -C "$automq_reference" rev-parse HEAD)"
automq_version="$(git -C "$automq_reference" show HEAD:gradle.properties \
    | sed -n 's/^version=//p' | head -n 1)"
kafka_branch="$(git -C "$kafka_fork" branch --show-current)"
kafka_commit="$(git -C "$kafka_fork" rev-parse HEAD)"
kafka_status="$(git -C "$kafka_fork" status --porcelain)"
pulsar_branch="$(git -C "$pulsar_fork" branch --show-current)"
pulsar_commit="$(git -C "$pulsar_fork" rev-parse HEAD)"
pulsar_status="$(git -C "$pulsar_fork" status --porcelain)"

[[ "$product_branch" == "main" ]] \
    || fail "product branch must be main, found $product_branch"
[[ -z "$product_status" ]] \
    || fail "product tree must be clean: $product_status"
[[ "$automq_branch" == "main" ]] \
    || fail "AutoMQ reference branch mismatch: $automq_branch"
[[ "$automq_commit" == "1c648d84819d5c3fef2af585f02149c397584870" ]] \
    || fail "AutoMQ reference commit mismatch: $automq_commit"
[[ "$automq_version" == "3.9.0-SNAPSHOT" ]] \
    || fail "AutoMQ reference version mismatch: $automq_version"
[[ "$kafka_branch" == "nereus/future9-native-kafka-storage" ]] \
    || fail "Kafka branch mismatch: $kafka_branch"
[[ "$kafka_commit" == "76f62f3b83e882105219b6c7687dbde594a8b8a2" ]] \
    || fail "Kafka commit mismatch: $kafka_commit"
[[ -z "$kafka_status" ]] \
    || fail "Kafka fork tree must be clean: $kafka_status"
[[ "$pulsar_branch" == "5.0.0-M1-nereus" ]] \
    || fail "Pulsar branch mismatch: $pulsar_branch"
[[ "$pulsar_commit" == "50fc70fe4620febcf0fd31d97ff7d2be447af3d4" ]] \
    || fail "Pulsar commit mismatch: $pulsar_commit"
[[ -z "$pulsar_status" ]] \
    || fail "Pulsar fork tree must be clean: $pulsar_status"

scenario_ids="$(rg -o '"id": "KF-[A-Z0-9]+-[0-9]{3}"' "$manifest" \
    | sed -E 's/^"id": "([^"]+)"$/\1/' \
    | sort)"
scenario_count="$(wc -l <<<"$scenario_ids" | tr -d ' ')"
unique_scenario_count="$(sort -u <<<"$scenario_ids" | wc -l | tr -d ' ')"
[[ "$scenario_count" -eq 146 && "$unique_scenario_count" -eq 146 ]] \
    || fail "expected 146 unique scenario IDs, found $scenario_count/$unique_scenario_count"

if rg -n '"status": "(PLANNED|FAILED|BLOCKED_ENVIRONMENT)"' "$manifest"; then
    fail "manifest contains a non-runnable status"
fi
status_count="$(rg -c '"status": "(IMPLEMENTED_NOT_RUN|PASSED_CURRENT_SOURCE)"' "$manifest")"
[[ "$status_count" -eq 146 ]] \
    || fail "expected 146 runnable statuses, found $status_count"

owner_tasks="$(rg -o '"task": "[^"]+"' "$manifest" \
    | sed -E 's/^"task": "([^"]+)"$/\1/' \
    | sort -u \
    | rg -v '^phase9M7FinalCheck$')"
while IFS= read -r owner_task; do
    case "$owner_task" in
        phase9M1FinalCheck \
        | phase9M2FinalCheck \
        | phase9M3FinalCheck \
        | phase9M4FinalCheck \
        | phase9M5FinalCheck \
        | phase9M6FinalCheck \
        | phase9M6KafkaProcessCheck \
        | f9MandatoryInternalTopicNtc2ProfileMatrixProcessIntegrationTest \
        | phase9M5KafkaRetentionOracleCheck \
        | f9DeleteRecordsBoundaryProcessIntegrationTest \
        | phase9M5KafkaCompactionOracleCheck \
        | phase9M5CompactionCoreCheck \
        | phase9M6KafkaFeatureCheck \
        | phase9ScaleCheck \
        | phase9ChaosCheck \
        | phase9CompatibilityCheck \
        | phase9PerformanceCheck)
            ;;
        *)
            fail "manifest owner task is not in the final graph: $owner_task"
            ;;
    esac
done <<<"$owner_tasks"

compatibility_passes="$(rg -o '"status":"PASS"' "$compatibility_report" | wc -l | tr -d ' ')"
performance_passes="$(rg -o '"status":"PASS"' "$performance_report" | wc -l | tr -d ' ')"
[[ "$compatibility_passes" -eq 4 ]] \
    || fail "compatibility report has $compatibility_passes PASS rows"
[[ "$performance_passes" -eq 5 ]] \
    || fail "performance report has $performance_passes PASS rows"
rg -q '"scenarioId":"KF-SCL-008"' "$compatibility_report" \
    || fail "compatibility report scenario mismatch"
rg -q '"scenarioId":"KF-SCL-009"' "$performance_report" \
    || fail "performance report scenario mismatch"
rg -q '"thresholdPolicy":"OBSERVATION_ONLY"' "$performance_report" \
    || fail "performance report threshold policy mismatch"

result_directories=(
    "nereus-api/build/test-results/f9RangedCountLimitTest"
    "nereus-metadata-oxia/build/test-results/f9MetadataTest"
    "nereus-metadata-oxia/build/test-results/f9OxiaIntegrationTest"
    "nereus-metadata-oxia/build/test-results/f9ActivationOxiaIntegrationTest"
    "nereus-metadata-oxia/build/test-results/f9BindingScaleOxiaIntegrationTest"
    "nereus-object-store/build/test-results/kafkaCheckpointTest"
    "nereus-object-store/build/test-results/kafkaCheckpointS3IntegrationTest"
    "nereus-materialization/build/test-results/f9ExactSourceSetTest"
    "nereus-kafka-adapter/build/test-results/f9M2Test"
    "nereus-kafka-adapter/build/test-results/f9M2IntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9M3CodecTest"
    "nereus-kafka-adapter/build/test-results/f9M3ProviderIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9MultiBrokerTakeoverProviderIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9BookKeeperWalOnlyProviderIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9BookKeeperLedgerDeletionProviderIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9ProducerStatePropertyTest"
    "nereus-kafka-adapter/build/test-results/f9RetentionTest"
    "nereus-kafka-adapter/build/test-results/f9CompactionPropertyTest"
    "nereus-kafka-adapter/build/test-results/f9ActivationTest"
    "nereus-kafka-adapter/build/test-results/f9CheckpointQuarantineTest"
    "nereus-kafka-adapter/build/test-results/f9M6KafkaProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9CheckpointTrimRecoveryProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9DeleteRecordsBoundaryProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9TrimResponseLossProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9TrimProfileMatrixProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9MultiBrokerTakeoverProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9CoordinatorMigrationProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9OngoingTransactionMigrationProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9TransactionResolutionCutProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9TransactionResolutionProfileMatrixProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9MandatoryInternalTopicNtc2ProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9MandatoryInternalTopicNtc2ProfileMatrixProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9MultiControllerFailoverProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9ActivationCutFailoverProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9ActivationProofCutFailoverProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9ActivationTransportRecoveryProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9InFlightTakeoverProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9BookKeeperProfileTakeoverProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9BookKeeperInFlightTakeoverProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9BookKeeperWalOnlyProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9BookKeeperWalAsyncObjectProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9BookKeeperWalSyncObjectProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9ObjectWalAsyncObjectProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9LeaderChurnChaosProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9ClientCompatibilityProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9PerformanceProfileProcessIntegrationTest"
    "nereus-kafka-adapter/build/test-results/f9PartitionScaleTest"
    "nereus-kafka-adapter/build/test-results/f9IoConcurrencyStressTest"
    "nereus-kafka-adapter/build/test-results/f9MaterializationScaleTest"
)

canonical_results="$(mktemp)"
trap 'rm -f "$canonical_results"' EXIT
junit_suites=0
junit_tests=0
for relative_directory in "${result_directories[@]}"; do
    result_directory="$repo/$relative_directory"
    [[ -d "$result_directory" ]] \
        || fail "missing JUnit result directory $relative_directory"
    found_xml=false
    for xml_file in "$result_directory"/TEST-*.xml; do
        [[ -f "$xml_file" ]] || continue
        found_xml=true
        tests="$(sed -n -E 's/.*<testsuite[^>]* tests="([0-9]+)".*/\1/p' "$xml_file" | head -n 1)"
        skipped="$(sed -n -E 's/.*<testsuite[^>]* skipped="([0-9]+)".*/\1/p' "$xml_file" | head -n 1)"
        failures="$(sed -n -E 's/.*<testsuite[^>]* failures="([0-9]+)".*/\1/p' "$xml_file" | head -n 1)"
        errors="$(sed -n -E 's/.*<testsuite[^>]* errors="([0-9]+)".*/\1/p' "$xml_file" | head -n 1)"
        [[ -n "$tests" && -n "$skipped" && -n "$failures" && -n "$errors" ]] \
            || fail "cannot parse JUnit suite $xml_file"
        [[ "$tests" -gt 0 ]] || fail "zero-test JUnit suite $xml_file"
        [[ "$skipped" -eq 0 && "$failures" -eq 0 && "$errors" -eq 0 ]] \
            || fail "non-passing JUnit suite $xml_file: tests=$tests skipped=$skipped failures=$failures errors=$errors"
        junit_suites=$((junit_suites + 1))
        junit_tests=$((junit_tests + tests))
        relative_xml="${xml_file#"$repo/"}"
        RELATIVE_XML="$relative_xml" perl -ne '
            while (/<testcase name="([^"]+)" classname="([^"]+)"/g) {
                print "$ENV{RELATIVE_XML}|$2|$1|PASS\n";
            }
        ' "$xml_file" >>"$canonical_results"
    done
    [[ "$found_xml" == "true" ]] \
        || fail "no JUnit XML under $relative_directory"
done
[[ "$junit_suites" -ge 30 && "$junit_tests" -ge 100 ]] \
    || fail "predecessor evidence too small: $junit_suites suites / $junit_tests tests"
junit_sha="$(sort "$canonical_results" | shasum -a 256 | awk '{print $1}')"

mkdir -p "$(dirname "$output")"
temporary_output="$output.tmp"
{
    printf '{\n'
    printf '  "schemaVersion":1,\n'
    printf '  "scenarioId":"KF-SCL-010",\n'
    printf '  "rerunTasks":true,\n'
    printf '  "scenarioCount":146,\n'
    printf '  "product":{"branch":%s,"commit":%s,"clean":true},\n' \
        "$(json_quote "$product_branch")" \
        "$(json_quote "$product_commit")"
    printf '  "autoMqReference":{"branch":%s,"commit":%s,"version":%s},\n' \
        "$(json_quote "$automq_branch")" \
        "$(json_quote "$automq_commit")" \
        "$(json_quote "$automq_version")"
    printf '  "kafkaFork":{"branch":%s,"commit":%s,"clean":true},\n' \
        "$(json_quote "$kafka_branch")" \
        "$(json_quote "$kafka_commit")"
    printf '  "pulsarFork":{"branch":%s,"commit":%s,"clean":true},\n' \
        "$(json_quote "$pulsar_branch")" \
        "$(json_quote "$pulsar_commit")"
    printf '  "services":["kraft","oxia","object-store","bookkeeper"],\n'
    printf '  "passedTasks":[\n'
    first_task=true
    while IFS= read -r owner_task; do
        if [[ "$first_task" == "false" ]]; then
            printf ',\n'
        fi
        printf '    %s' "$(json_quote "$owner_task")"
        first_task=false
    done <<<"$owner_tasks"
    printf '\n  ],\n'
    printf '  "junit":{"suites":%d,"tests":%d,"canonicalSha256":%s},\n' \
        "$junit_suites" \
        "$junit_tests" \
        "$(json_quote "$junit_sha")"
    printf '  "artifacts":[\n'
    artifact_files=(
        "$manifest"
        "$matrix"
        "$compatibility_report"
        "$performance_report"
    )
    for artifact_index in "${!artifact_files[@]}"; do
        artifact="${artifact_files[$artifact_index]}"
        relative_artifact="${artifact#"$repo/"}"
        if [[ "$artifact_index" -gt 0 ]]; then
            printf ',\n'
        fi
        printf '    {"path":%s,"sha256":%s}' \
            "$(json_quote "$relative_artifact")" \
            "$(json_quote "$(sha256_file "$artifact")")"
    done
    printf '\n  ]\n'
    printf '}\n'
} >"$temporary_output"
mv "$temporary_output" "$output"

if command -v jq >/dev/null 2>&1; then
    jq -e '.scenarioCount == 146 and .rerunTasks == true' "$output" >/dev/null
fi
echo "F9 final pre-evidence: $scenario_count scenarios, $junit_suites suites, $junit_tests tests"
