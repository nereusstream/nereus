#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: run-v2-m3-m2-regression.sh --dry-run|--execute \
  --repo-root ABSOLUTE_NEREUS_ROOT --tested-commit COMMIT \
  --kafka-worktree ABSOLUTE_DEDICATED_WORKTREE \
  --pulsar-worktree ABSOLUTE_DEDICATED_WORKTREE \
  --output-dir ABSOLUTE_EXTERNAL_NEW_DIRECTORY

--dry-run performs every source/worktree/image/output preflight and prints the
closed 25-gate command matrix. It writes nothing and starts no test/container.
--execute performs the same preflight, runs the formal current-source profile,
and writes only raw logs/results plus 25 trusted-child JCS files below the
external output directory. It never publishes or rewrites repository evidence.
EOF
    exit 64
}

fail() {
    echo "V2 M3 current-source M2 regression runner: $*" >&2
    exit 1
}

mode=""
repo_root=""
tested_commit=""
kafka_worktree=""
pulsar_worktree=""
output_dir=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run|--execute)
            [[ -z "$mode" ]] || usage
            mode="${1#--}"
            shift
            ;;
        --repo-root|--tested-commit|--kafka-worktree|--pulsar-worktree|--output-dir)
            [[ $# -ge 2 ]] || usage
            case "$1" in
                --repo-root) repo_root="$2" ;;
                --tested-commit) tested_commit="$2" ;;
                --kafka-worktree) kafka_worktree="$2" ;;
                --pulsar-worktree) pulsar_worktree="$2" ;;
                --output-dir) output_dir="$2" ;;
            esac
            shift 2
            ;;
        *) usage ;;
    esac
done
[[ -n "$mode" && -n "$repo_root" && -n "$tested_commit" \
    && -n "$kafka_worktree" && -n "$pulsar_worktree" && -n "$output_dir" ]] || usage
[[ "$tested_commit" =~ ^[0-9a-f]{40}$ ]] || fail "tested commit is not 40 lowercase hexadecimal characters"

canonical_directory() {
    local value="$1"
    [[ "$value" = /* ]] || fail "path must be absolute: $value"
    [[ -d "$value" && ! -L "$value" ]] || fail "directory is absent or symbolic: $value"
    (cd "$value" && pwd -P)
}

repo_root="$(canonical_directory "$repo_root")"
kafka_worktree="$(canonical_directory "$kafka_worktree")"
pulsar_worktree="$(canonical_directory "$pulsar_worktree")"
[[ "$output_dir" = /* ]] || fail "output directory must be absolute"
[[ ! -e "$output_dir" && ! -L "$output_dir" ]] || fail "output directory must not already exist: $output_dir"
output_parent="$(canonical_directory "$(dirname "$output_dir")")"
output_dir="$output_parent/$(basename "$output_dir")"

[[ "$(git -C "$repo_root" rev-parse --show-toplevel)" = "$repo_root" ]] \
    || fail "Nereus root is not the exact Git top-level"
[[ "$(git -C "$repo_root" rev-parse HEAD)" = "$tested_commit" ]] \
    || fail "tested commit differs from exact Nereus HEAD"
[[ -z "$(git -C "$repo_root" status --porcelain=v1 --untracked-files=all)" ]] \
    || fail "Nereus worktree must be clean before preflight"

contract="$repo_root/scripts/check-v2-m3-m2-regression.py"
locks="$repo_root/docs/v2/source-locks.json"
[[ -f "$contract" && ! -L "$contract" ]] || fail "M2 regression contract is absent or symbolic"
[[ -f "$locks" && ! -L "$locks" ]] || fail "source locks are absent or symbolic"

source_values=()
while IFS= read -r value; do
    source_values[${#source_values[@]}]="$value"
done < <(PYTHONDONTWRITEBYTECODE=1 python3 - "$contract" "$locks" <<'PY'
import importlib.util
import json
from pathlib import Path
import re
import sys

contract_path = Path(sys.argv[1])
locks_path = Path(sys.argv[2])
spec = importlib.util.spec_from_file_location("m3_m2_regression_runner_contract", contract_path)
if spec is None or spec.loader is None:
    raise SystemExit("cannot load M2 regression contract")
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
expected_gates = (
    "KAFKA_K0", "KAFKA_K1", "KAFKA_K2", "KAFKA_K3", "KAFKA_K4",
    "KAFKA_K5", "KAFKA_K6", "KAFKA_K7", "KAFKA_K8", "KAFKA_K9",
    "KAFKA_K10", "KAFKA_EXACT", "KAFKA_REAL_BOOKKEEPER",
    "KAFKA_SCALE_10000", "KAFKA_SCALE_100000", "PULSAR_P0", "PULSAR_P1",
    "PULSAR_P2", "PULSAR_P3", "PULSAR_P4", "PULSAR_P5", "PULSAR_P6",
    "PULSAR_NATIVE", "PULSAR_P6_PROVIDER", "PULSAR_FINAL_PARSER_POLICY",
)
if tuple(module.REQUIRED_GATES) != expected_gates:
    raise SystemExit("runner/contract 25-gate inventory differs")
try:
    locks = json.loads(locks_path.read_text())
    kafka = locks["m2KafkaK0InputSourceBinding"]["kafkaInput"]
    pulsar = locks["m2PulsarNativeBinding"]
    bookkeeper = locks["m2KafkaK0InputSourceBinding"]["bookKeeperInput"]
    values = (
        kafka["repository"], kafka["branch"], kafka["forkCommit"],
        kafka["implementationBaseCommit"], pulsar["repository"], pulsar["branch"],
        pulsar["finalForkCommit"], pulsar["implementationBaseCommit"],
        bookkeeper["serverImageReference"], bookkeeper["serverImageConfigDigest"],
    )
except (KeyError, TypeError, json.JSONDecodeError) as error:
    raise SystemExit(f"source-lock schema differs: {error}") from error
for value in values:
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise SystemExit("source-lock value is absent or contains a line break")
for value in (values[2], values[3], values[6], values[7]):
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise SystemExit("source-lock commit is not canonical")
if not re.fullmatch(r"[^@]+@sha256:[0-9a-f]{64}", values[8]):
    raise SystemExit("BookKeeper image reference is not digest pinned")
if not re.fullmatch(r"sha256:[0-9a-f]{64}", values[9]):
    raise SystemExit("BookKeeper image ID is not canonical")
print(*values, sep="\n")
PY
)
[[ ${#source_values[@]} -eq 10 ]] || fail "source-lock extraction did not return the closed tuple"
kafka_repository="${source_values[0]}"
kafka_branch="${source_values[1]}"
kafka_commit="${source_values[2]}"
kafka_base="${source_values[3]}"
pulsar_repository="${source_values[4]}"
pulsar_branch="${source_values[5]}"
pulsar_commit="${source_values[6]}"
pulsar_base="${source_values[7]}"
bookkeeper_image="${source_values[8]}"
bookkeeper_image_id="${source_values[9]}"

localstack_tag="localstack/localstack:4.14.0"
localstack_digest="localstack/localstack@sha256:3ebc37595918b8accb852f8048fef2aff047d465167edd655528065b07bc364a"
localstack_image_id="sha256:ad76d8f93de9cb765653983d32f4b2994ca981b8f6ccfcf7b52b2d1800b18581"
minio_tag="quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
minio_digest="quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
minio_image_id="sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253"

assert_dedicated_worktree() {
    local label="$1"
    local checkout="$2"
    local repository="$3"
    local branch="$4"
    local commit="$5"
    local base="$6"
    [[ -f "$checkout/.git" && ! -L "$checkout/.git" ]] \
        || fail "$label checkout must be a dedicated linked Git worktree (.git file required)"
    [[ "$(git -C "$checkout" remote get-url origin)" = "$repository" ]] \
        || fail "$label origin differs from the source lock"
    [[ "$(git -C "$checkout" rev-parse HEAD)" = "$commit" ]] \
        || fail "$label HEAD differs from the source lock"
    [[ "$(git -C "$checkout" branch --show-current)" = "$branch" ]] \
        || fail "$label worktree branch differs from the source lock"
    [[ "$(git -C "$checkout" rev-parse "refs/remotes/origin/$branch")" = "$commit" ]] \
        || fail "$label remote-tracking branch differs from the source lock"
    git -C "$checkout" merge-base --is-ancestor "$base" "$commit" \
        || fail "$label locked commit does not descend from its implementation base"
    [[ -z "$(git -C "$checkout" status --porcelain=v1 --untracked-files=all)" ]] \
        || fail "$label dedicated worktree is not clean"
}
assert_dedicated_worktree "Kafka" "$kafka_worktree" "$kafka_repository" "$kafka_branch" "$kafka_commit" "$kafka_base"
assert_dedicated_worktree "Pulsar" "$pulsar_worktree" "$pulsar_repository" "$pulsar_branch" "$pulsar_commit" "$pulsar_base"

python3 - "$output_dir" "$repo_root" "$kafka_worktree" "$pulsar_worktree" <<'PY'
from pathlib import Path
import sys

output, *roots = (Path(value).resolve() for value in sys.argv[1:])
for root in roots:
    if output == root or output.is_relative_to(root):
        raise SystemExit(f"output directory is inside a source checkout: {root}")
PY

docker_bin="${NEREUS_M3_DOCKER_BIN:-docker}"
command -v "$docker_bin" >/dev/null || fail "Docker client is unavailable: $docker_bin"
"$docker_bin" compose version >/dev/null || fail "Docker Compose v2 is unavailable"

assert_image() {
    local reference="$1"
    local expected_id="$2"
    local expected_digest="$3"
    local actual_id
    actual_id="$("$docker_bin" image inspect --format '{{.Id}}' "$reference")" \
        || fail "required Docker image is not available: $reference"
    [[ "$actual_id" = "$expected_id" ]] || fail "Docker image ID differs: $reference"
    "$docker_bin" image inspect --format '{{join .RepoDigests "\n"}}' "$reference" \
        | grep -Fxq "$expected_digest" || fail "Docker repo digest differs: $reference"
}
assert_image "$bookkeeper_image" "$bookkeeper_image_id" "$bookkeeper_image"
assert_image "$localstack_tag" "$localstack_image_id" "$localstack_digest"
assert_image "$minio_tag" "$minio_image_id" "$minio_digest"

plan_rows=(
    "KAFKA_K0|local|fresh JUnit|v2M2KafkaK0Module/Provider/Wire/Numeric/Evidence source gates"
    "KAFKA_K1|local|fresh JUnit|scripts/check-v2-m2-kafka-k1.sh"
    "KAFKA_K2|local|fresh JUnit|scripts/check-v2-m2-kafka-k2.sh dedicated Kafka worktree"
    "KAFKA_K3|local|fresh JUnit|scripts/check-v2-m2-kafka-k3.sh"
    "KAFKA_K4|local|fresh JUnit|scripts/check-v2-m2-kafka-k4.sh"
    "KAFKA_K5|local|fresh JUnit|scripts/check-v2-m2-kafka-k5.sh"
    "KAFKA_K6|local|fresh JUnit|scripts/check-v2-m2-kafka-k6.sh"
    "KAFKA_K7|local|fresh JUnit|scripts/check-v2-m2-kafka-k7.sh"
    "KAFKA_K8|local|fresh JUnit|scripts/check-v2-m2-kafka-k8.sh"
    "KAFKA_K9|formal|fresh JUnit plus two raw scale receipts|K9 defaults + real BookKeeper + 10k + 100k"
    "KAFKA_K10|local|fresh JUnit|scripts/check-v2-m2-kafka-k10-policy.sh"
    "KAFKA_EXACT|native|exact command result|Kafka 4.3 K2 exact-source conformance"
    "KAFKA_REAL_BOOKKEEPER|real provider|fresh JUnit|two exact-image realBookKeeperTest suites"
    "KAFKA_SCALE_10000|scale|raw receipt|run-v2-m2-kafka-k9-scale.sh 10000"
    "KAFKA_SCALE_100000|scale|raw receipt|run-v2-m2-kafka-k9-scale.sh 100000"
    "PULSAR_P0|local|fresh JUnit|scripts/check-v2-m2-pulsar-p0.sh"
    "PULSAR_P1|local|fresh JUnit|scripts/check-v2-m2-pulsar-p1.sh"
    "PULSAR_P2|local|fresh JUnit|scripts/check-v2-m2-pulsar-p2.sh"
    "PULSAR_P3|local|fresh JUnit|scripts/check-v2-m2-pulsar-p3.sh"
    "PULSAR_P4|local|fresh JUnit|P4 Object plus dual-source source gates"
    "PULSAR_P5|native|fresh Nereus and Pulsar JUnit|P5 exact-source provider gate"
    "PULSAR_P6|real providers|four fresh JUnit/raw receipts|adapter + candidate + native + MinIO"
    "PULSAR_NATIVE|native|fresh Pulsar JUnit|DualSourceReadHandle plus OffloadLedgerDelete"
    "PULSAR_P6_PROVIDER|real provider|fresh MinIO JUnit/raw receipt|fixed MinIO execution"
    "PULSAR_FINAL_PARSER_POLICY|local|fresh JUnit|scripts/check-v2-m2-pulsar-final-policy.sh"
)
[[ ${#plan_rows[@]} -eq 25 ]] || fail "internal execution plan is not the closed 25-gate inventory"
printf '%s\n' "${plan_rows[@]}"
if [[ "$mode" = "dry-run" ]]; then
    echo "V2 M3 current-source M2 regression preflight PASS: tested=$tested_commit gates=25 writes=0 formalRuns=0"
    exit 0
fi

mkdir -m 700 "$output_dir"
mkdir -m 700 "$output_dir/logs" "$output_dir/raw" "$output_dir/children"

bk_project="nereus-v2-m3-m2-regression-real"
native_container=""
minio_container=""
cleanup() {
    if [[ -n "$native_container" ]]; then
        "$docker_bin" rm -f "$native_container" >/dev/null 2>&1 || true
    fi
    if [[ -n "$minio_container" ]]; then
        "$docker_bin" rm -f "$minio_container" >/dev/null 2>&1 || true
    fi
    "$docker_bin" compose -p "$bk_project" \
        -f "$repo_root/config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml" \
        down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

assert_sources_unchanged() {
    [[ "$(git -C "$repo_root" rev-parse HEAD)" = "$tested_commit" \
        && -z "$(git -C "$repo_root" status --porcelain=v1 --untracked-files=all)" ]] \
        || fail "Nereus source changed during the formal run"
    [[ "$(git -C "$kafka_worktree" rev-parse HEAD)" = "$kafka_commit" \
        && -z "$(git -C "$kafka_worktree" status --porcelain=v1 --untracked-files=all)" ]] \
        || fail "Kafka worktree changed during the formal run"
    [[ "$(git -C "$pulsar_worktree" rev-parse HEAD)" = "$pulsar_commit" \
        && -z "$(git -C "$pulsar_worktree" status --porcelain=v1 --untracked-files=all)" ]] \
        || fail "Pulsar worktree changed during the formal run"
}

run_logged() {
    local label="$1"
    shift
    assert_sources_unchanged
    "$@" 2>&1 | tee "$output_dir/logs/$label.log"
    assert_sources_unchanged
}

gradle=("$repo_root/gradlew" --no-daemon --rerun-tasks \
    "-PkafkaForkCheckout=$kafka_worktree" "-PpulsarCheckout=$pulsar_worktree")
run_logged local-m2-tests "${gradle[@]}" \
    :nereus-storage-api:test :nereus-storage-api:jar :nereus-storage-api:sourcesJar \
    :nereus-storage-api:generatePomFileForMavenJavaPublication \
    :nereus-storage-api:generateMetadataFileForMavenJavaPublication \
    :nereus-storage-bookkeeper:test :nereus-storage-bookkeeper:jar \
    :nereus-storage-bookkeeper:sourcesJar \
    :nereus-storage-bookkeeper:generatePomFileForMavenJavaPublication \
    :nereus-storage-bookkeeper:generateMetadataFileForMavenJavaPublication \
    :nereus-kafka-bookkeeper:test :nereus-kafka-bookkeeper:jar \
    :nereus-kafka-bookkeeper:sourcesJar \
    :nereus-kafka-bookkeeper:generatePomFileForMavenJavaPublication \
    :nereus-kafka-bookkeeper:generateMetadataFileForMavenJavaPublication \
    :nereus-pulsar-offload:test

local_checks=(
    check-v2-m2-kafka-k0-module.sh check-v2-m2-kafka-k0-provider.sh
    check-v2-m2-kafka-k0-wire.sh check-v2-m2-kafka-k0-numeric.sh
    check-v2-m2-kafka-k0-evidence.sh check-v2-m2-kafka-k1.sh
    check-v2-m2-kafka-k3.sh check-v2-m2-kafka-k4.sh check-v2-m2-kafka-k5.sh
    check-v2-m2-kafka-k6.sh check-v2-m2-kafka-k7.sh check-v2-m2-kafka-k8.sh
    check-v2-m2-kafka-k9-plan.sh check-v2-m2-kafka-k9-defaults.sh
    check-v2-m2-kafka-k10-policy.sh check-v2-m2-pulsar-p0.sh
    check-v2-m2-pulsar-p1.sh check-v2-m2-pulsar-p2.sh check-v2-m2-pulsar-p3.sh
    check-v2-m2-pulsar-p4-object.sh check-v2-m2-pulsar-p4-dual.sh
    check-v2-m2-pulsar-final-policy.sh
)
for check in "${local_checks[@]}"; do
    run_logged "${check%.sh}" bash "$repo_root/scripts/$check"
done
run_logged check-v2-m2-kafka-k2 bash "$repo_root/scripts/check-v2-m2-kafka-k2.sh" "$kafka_worktree"

run_logged pulsar-p5-native "${gradle[@]}" v2M2PulsarP5NativeForkTest
run_logged check-v2-m2-pulsar-p5 bash "$repo_root/scripts/check-v2-m2-pulsar-p5.sh" "$pulsar_worktree"

compose="$repo_root/config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml"
"$docker_bin" compose -p "$bk_project" -f "$compose" down -v --remove-orphans >/dev/null 2>&1 || true
run_logged real-bookkeeper-start "$docker_bin" compose -p "$bk_project" -f "$compose" up -d --wait
run_logged real-bookkeeper-tests "${gradle[@]}" \
    :nereus-storage-bookkeeper:realBookKeeperTest :nereus-kafka-bookkeeper:realBookKeeperTest \
    -Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers
run_logged real-bookkeeper-stop "$docker_bin" compose -p "$bk_project" -f "$compose" down -v --remove-orphans

for tier in 10000 100000; do
    tier_root="$output_dir/raw/kafka-k9/tier-$tier"
    mkdir -p "$tier_root"
    run_logged "kafka-scale-$tier" bash "$repo_root/scripts/run-v2-m2-kafka-k9-scale.sh" \
        "$tier" "$tested_commit" "$tier_root"
done

candidate="$output_dir/raw/pulsar-p6/candidate-matrix.json"
native="$output_dir/raw/pulsar-p6/native-baseline.json"
minio="$output_dir/raw/pulsar-p6/minio-provider.json"
mkdir -p "$(dirname "$candidate")"
run_logged pulsar-p6-local "${gradle[@]}" \
    :nereus-pulsar-offload:p6ProviderTest :nereus-pulsar-offload:p6EvidenceTest \
    "-Pv2M2PulsarP6EvidenceOutput=$candidate" \
    "-Pv2M2PulsarP6TestedSourceCommit=$tested_commit" \
    "-Pv2M2PulsarP6PulsarSourceCommit=$pulsar_commit"

native_container="$("$docker_bin" run -d --rm -p 127.0.0.1::4566 -e SERVICES=s3 "$localstack_digest")"
native_port="$("$docker_bin" port "$native_container" 4566/tcp | sed -n 's/.*://p' | tail -1)"
[[ "$native_port" =~ ^[0-9]+$ ]] || fail "cannot resolve the native LocalStack port"
for unused in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:$native_port/_localstack/health" >/dev/null && break
    sleep 1
done
curl -fsS "http://127.0.0.1:$native_port/_localstack/health" >/dev/null \
    || fail "native LocalStack did not become healthy"
run_logged pulsar-p6-native env \
    "NEREUS_P6_LOCALSTACKENDPOINT=http://127.0.0.1:$native_port" \
    "NEREUS_P6_NATIVEEVIDENCEOUTPUT=$native" \
    "NEREUS_P6_PULSARSOURCECOMMIT=$pulsar_commit" \
    "$pulsar_worktree/gradlew" :tiered-storage:jcloud:test --no-daemon --rerun-tasks \
    --tests org.apache.bookkeeper.mledger.offload.jcloud.impl.P6NativeLocalStackEvidenceTest
"$docker_bin" rm -f "$native_container" >/dev/null
native_container=""

minio_container="$("$docker_bin" run -d --rm -p 127.0.0.1::9000 \
    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
    "$minio_digest" server /data --address :9000)"
minio_port="$("$docker_bin" port "$minio_container" 9000/tcp | sed -n 's/.*://p' | tail -1)"
[[ "$minio_port" =~ ^[0-9]+$ ]] || fail "cannot resolve the MinIO port"
for unused in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:$minio_port/minio/health/live" >/dev/null && break
    sleep 1
done
curl -fsS "http://127.0.0.1:$minio_port/minio/health/live" >/dev/null \
    || fail "MinIO did not become healthy"
run_logged pulsar-p6-minio env \
    "NEREUS_P6_MINIO_ENDPOINT=http://127.0.0.1:$minio_port" \
    NEREUS_P6_MINIO_ACCESS_KEY=minioadmin NEREUS_P6_MINIO_SECRET_KEY=minioadmin \
    "${gradle[@]}" :nereus-pulsar-offload:p6RealProviderTest \
    "-Pv2M2PulsarP6RealProviderOutput=$minio" \
    "-Pv2M2PulsarP6TestedSourceCommit=$tested_commit" \
    "-Pv2M2PulsarP6MinioImageReference=$minio_tag" \
    "-Pv2M2PulsarP6MinioImageDigest=${minio_digest#*@}"
"$docker_bin" rm -f "$minio_container" >/dev/null
minio_container=""

export M3_M2_REPO_ROOT="$repo_root"
export M3_M2_TESTED_COMMIT="$tested_commit"
export M3_M2_KAFKA_WORKTREE="$kafka_worktree"
export M3_M2_KAFKA_COMMIT="$kafka_commit"
export M3_M2_PULSAR_WORKTREE="$pulsar_worktree"
export M3_M2_PULSAR_COMMIT="$pulsar_commit"
export M3_M2_OUTPUT="$output_dir"
export M3_M2_MINIO_TAG="$minio_tag"
export M3_M2_MINIO_DIGEST="${minio_digest#*@}"
PYTHONDONTWRITEBYTECODE=1 python3 - <<'PY'
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
import xml.etree.ElementTree as ET

root = Path(os.environ["M3_M2_REPO_ROOT"])
tested = os.environ["M3_M2_TESTED_COMMIT"]
kafka = Path(os.environ["M3_M2_KAFKA_WORKTREE"])
kafka_commit = os.environ["M3_M2_KAFKA_COMMIT"]
pulsar = Path(os.environ["M3_M2_PULSAR_WORKTREE"])
pulsar_commit = os.environ["M3_M2_PULSAR_COMMIT"]
output = Path(os.environ["M3_M2_OUTPUT"])

contract_path = root / "scripts/check-v2-m3-m2-regression.py"
spec = importlib.util.spec_from_file_location("m3_m2_regression_runner_output", contract_path)
if spec is None or spec.loader is None:
    raise SystemExit("cannot load M2 regression output contract")
contract = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = contract
spec.loader.exec_module(contract)

def safe(path: Path) -> bytes:
    mode = path.lstat().st_mode
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise SystemExit(f"evidence input is not a regular non-symlink file: {path}")
    return path.read_bytes()

def junit(path: Path, expected: int | None = None) -> int:
    raw = safe(path)
    suite = ET.fromstring(raw)
    values = {key: int(suite.attrib[key]) for key in ("tests", "failures", "errors", "skipped")}
    if values["tests"] <= 0 or any(values[key] for key in ("failures", "errors", "skipped")):
        raise SystemExit(f"JUnit result is not non-empty zero-failure: {path}: {values}")
    if expected is not None and values["tests"] != expected:
        raise SystemExit(f"JUnit test inventory drifted: {path}: {values['tests']} != {expected}")
    cases = suite.findall("testcase")
    if len(cases) != values["tests"]:
        raise SystemExit(f"JUnit testcase inventory differs from tests: {path}")
    for case in cases:
        if any(case.find(kind) is not None for kind in ("failure", "error", "skipped")):
            raise SystemExit(f"JUnit testcase contains a terminal marker: {path}")
    return values["tests"]

def module_suite(module: str, suffix: str, expected: int) -> int:
    matches = list((root / module / "build/test-results/test").glob(f"TEST-*.{suffix}.xml"))
    if len(matches) != 1:
        raise SystemExit(f"missing unique JUnit suite {module}:{suffix}")
    return junit(matches[0], expected)

def all_module_tests(module: str) -> int:
    paths = sorted((root / module / "build/test-results/test").glob("TEST-*.xml"))
    if not paths:
        raise SystemExit(f"module has no JUnit results: {module}")
    return sum(junit(path) for path in paths)

def load(path: Path) -> dict:
    value = json.loads(safe(path), object_pairs_hook=contract._reject_duplicate_members)
    if not isinstance(value, dict):
        raise SystemExit(f"JSON root is not an object: {path}")
    return value

def raw_receipts() -> tuple[dict[int, dict], dict, dict, dict]:
    scales = {}
    for tier in (10_000, 100_000):
        row = load(output / f"raw/kafka-k9/tier-{tier}/scale-{tier}.json")
        if (row.get("schema"), row.get("result"), row.get("testedSourceCommit"),
                row.get("tierPartitions"), row.get("counts", {}).get("partitions")) != (
                "NEREUS_V2_M2_KAFKA_K9_SCALE_RESULT_V1", "PASS", tested, tier, tier):
            raise SystemExit(f"scale receipt identity/source/result differs: {tier}")
        scales[tier] = row
    candidate = load(output / "raw/pulsar-p6/candidate-matrix.json")
    if (candidate.get("schema"), candidate.get("testedSourceCommit"),
            candidate.get("pulsarSourceCommit"), len(candidate.get("matrix", [])),
            len(candidate.get("boundaryCoverage", []))) != (
            "NEREUS_V2_M2_PULSAR_P6_CANDIDATE_V1", tested, pulsar_commit, 16, 2):
        raise SystemExit("P6 candidate receipt identity/source/inventory differs")
    if candidate.get("selection") != {
        "classes": ["latency-1mib", "balanced-4mib", "scan-8mib"],
        "deploymentDefault": "balanced-4mib", "excludedCandidateBytes": 16 << 20,
    }:
        raise SystemExit("P6 candidate selection differs")
    native = load(output / "raw/pulsar-p6/native-baseline.json")
    if (native.get("schema"), native.get("pulsarSourceCommit"), native.get("entryCount"),
            native.get("entryBytes"), native.get("sourceDeclaredReadBufferBytes")) != (
            "NEREUS_V2_M2_PULSAR_P6_NATIVE_BASELINE_V1", pulsar_commit, 50_000, 100, 1 << 20):
        raise SystemExit("P6 native receipt identity/source/workload differs")
    minio = load(output / "raw/pulsar-p6/minio-provider.json")
    if (minio.get("schema"), minio.get("result"), minio.get("testedSourceCommit"),
            minio.get("imageReference"), minio.get("imageDigest")) != (
            "NEREUS_V2_M2_PULSAR_P6_REAL_PROVIDER_V1", "PASS_MINIO_PROVIDER_ONLY", tested,
            os.environ["M3_M2_MINIO_TAG"], os.environ["M3_M2_MINIO_DIGEST"]):
        raise SystemExit("P6 MinIO receipt identity/source/image differs")
    for field in ("conditionalCreate", "boundedRangeRead", "deleteAndProveAbsent",
                  "multipartCleanupAndRelist", "canonicalNpd1Npo1RoundTrip"):
        if minio.get(field) is not True:
            raise SystemExit(f"P6 MinIO receipt lacks {field}")
    return scales, candidate, native, minio

scales, candidate, native, minio = raw_receipts()
kafka_suites = {
    "KAFKA_K1": {"KafkaPartitionFrontiersV1Test": 6, "KafkaPartitionProtocolStateV1Test": 4,
        "KafkaPartitionPublicationCellV1Test": 6, "KafkaPartitionFenceTransitionV1Test": 6,
        "KafkaPartitionPublicationInterleavingTest": 4},
    "KAFKA_K2": {"KafkaNativeAssignedRecordBatchV1Test": 8,
        "KafkaAssignedRecordBatchGroupAdapterV1Test": 6, "KafkaNbke2AssignedAppendGroupV1Test": 3},
    "KAFKA_K3": {"KafkaBookKeeperEntrySequencerV1Test": 6,
        "KafkaBookKeeperRunLifecycleV1Test": 8, "KafkaBookKeeperRunFailureCutsV1Test": 5},
    "KAFKA_K4": {"KafkaAppendCapacityControllerV1Test": 6,
        "KafkaBookKeeperPipelineAdmissionV1Test": 8, "KafkaBookKeeperOrderedCompletionV1Test": 6},
    "KAFKA_K5": {"KafkaPartitionSpeculativePublicationV1Test": 5,
        "KafkaProducerTransactionStateV1Test": 8, "KafkaCoherentCommitCoordinatorV1Test": 8},
    "KAFKA_K6": {"KafkaPackedLocatorLookupV1Test": 7,
        "KafkaBookKeeperTargetedReaderV1Test": 8, "KafkaBookKeeperSequentialReaderV1Test": 8},
    "KAFKA_K7": {"KafkaProtocolCheckpointCodecV1Test": 6,
        "BookKeeperKafkaProtocolCheckpointStoreV1Test": 5, "KafkaBookKeeperTakeoverRecoveryV1Test": 15},
    "KAFKA_K8": {"KafkaReplicaDescriptorCodecV1Test": 7,
        "KafkaReplicaObservationJournalV1Test": 7, "KafkaReplicaFollowerKernelV1Test": 11},
}
counts = {
    gate: sum(module_suite("nereus-kafka-bookkeeper", suite, count) for suite, count in suites.items())
    for gate, suites in kafka_suites.items()
}
counts["KAFKA_K0"] = sum(all_module_tests(module) for module in (
    "nereus-storage-api", "nereus-storage-bookkeeper", "nereus-kafka-bookkeeper"))
counts["KAFKA_K10"] = module_suite(
    "nereus-kafka-bookkeeper", "KafkaM2FinalResolverV1Test", 9)
exact_log = safe(output / "logs/check-v2-m2-kafka-k2.log").decode("utf-8")
needle = "Kafka 4.3 K2 exact-source conformance: suites=1 tests=13 failures=0 errors=0 skips=0"
if exact_log.splitlines().count(needle) != 1:
    raise SystemExit("Kafka exact-source command result differs")
counts["KAFKA_EXACT"] = 13
real_paths = (
    root / "nereus-storage-bookkeeper/build/test-results/realBookKeeperTest/"
        "TEST-com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1RealTest.xml",
    root / "nereus-kafka-bookkeeper/build/test-results/realBookKeeperTest/"
        "TEST-com.nereusstream.kafka.bookkeeper.RealBookKeeperKafkaEngineV1Test.xml",
)
counts["KAFKA_REAL_BOOKKEEPER"] = junit(real_paths[0], 6) + junit(real_paths[1], 3)
counts["KAFKA_SCALE_10000"] = 1
counts["KAFKA_SCALE_100000"] = 1
counts["KAFKA_K9"] = module_suite(
    "nereus-kafka-bookkeeper", "KafkaBookKeeperOperationalDefaultsV1Test", 6
) + counts["KAFKA_REAL_BOOKKEEPER"] + 2

pulsar_local = {
    "PULSAR_P0": {"PulsarOffloadP0ContractTest": 11},
    "PULSAR_P1": {"npd1.Npd1CodecV1Test": 15},
    "PULSAR_P2": {"npo1.Npo1CodecV1Test": 17},
    "PULSAR_P3": {"PulsarSealedLedgerPublisherV1Test": 9},
    "PULSAR_P4": {"PulsarObjectReadHandleV1Test": 8,
        "PulsarDualSourceReadHandleV1Test": 10, "PulsarBookKeeperDeletionCoordinatorV1Test": 9},
}
for gate, suites in pulsar_local.items():
    counts[gate] = sum(module_suite("nereus-pulsar-offload", suite, count)
                       for suite, count in suites.items())
p5_local = module_suite("nereus-pulsar-offload", "NereusPulsarLedgerOffloaderV1Test", 3) \
    + module_suite("nereus-pulsar-offload", "npd1.Npd1CodecV1Test", 15)
pulsar_test_root = pulsar / "managed-ledger/build/test-results/test"
p5_native = junit(pulsar_test_root /
    "TEST-org.apache.bookkeeper.mledger.impl.DualSourceReadHandleTest.xml", 13) \
    + junit(pulsar_test_root /
    "TEST-org.apache.bookkeeper.mledger.impl.OffloadLedgerDeleteTest.xml", 13)
counts["PULSAR_P5"] = p5_local + p5_native
counts["PULSAR_NATIVE"] = p5_native
p6_provider = junit(root / "nereus-pulsar-offload/build/test-results/p6ProviderTest/"
    "TEST-com.nereusstream.pulsar.offload.S3PulsarOffloadObjectStoreV1Test.xml", 3)
p6_candidate = junit(root / "nereus-pulsar-offload/build/test-results/p6EvidenceTest/"
    "TEST-com.nereusstream.pulsar.offload.PulsarP6CandidateEvidenceTest.xml", 1)
p6_minio = junit(root / "nereus-pulsar-offload/build/test-results/p6RealProviderTest/"
    "TEST-com.nereusstream.pulsar.offload.P6MinioProviderEvidenceTest.xml", 1)
p6_native = junit(pulsar / "tiered-storage/jcloud/build/test-results/test/"
    "TEST-org.apache.bookkeeper.mledger.offload.jcloud.impl.P6NativeLocalStackEvidenceTest.xml", 1)
counts["PULSAR_P6"] = p6_provider + p6_candidate + p6_minio + p6_native
counts["PULSAR_P6_PROVIDER"] = p6_minio
counts["PULSAR_FINAL_PARSER_POLICY"] = module_suite(
    "nereus-pulsar-offload", "evidence.PulsarM2FinalResolverV1Test", 9)

if set(counts) != set(contract.REQUIRED_GATES):
    raise SystemExit(f"derived gate inventory differs: {sorted(set(contract.REQUIRED_GATES) - set(counts))}")
children = []
for gate in contract.REQUIRED_GATES:
    value = {
        "errors": 0, "executionProfile": contract.EXECUTION_PROFILE, "failures": 0,
        "gateId": gate, "result": "PASS", "schema": contract.CHILD_SCHEMA, "skipped": 0,
        "testedNereusCommit": tested, "tests": counts[gate],
    }
    raw = contract.canonical_bytes(value)
    contract.validate_child_result(contract.load_canonical_json(raw, gate), tested)
    path = output / "children" / f"{gate}.json"
    path.write_bytes(raw)
    children.append({"bytes": len(raw), "gateId": gate,
        "path": str(path.relative_to(output)), "sha256": hashlib.sha256(raw).hexdigest(),
        "tests": counts[gate]})

summary = {
    "children": children,
    "executionProfile": contract.EXECUTION_PROFILE,
    "kafkaSource": {"commit": kafka_commit, "repository":
        contract.expected_sources(tested)["kafka"]["repository"]},
    "pulsarSource": {"commit": pulsar_commit, "repository":
        contract.expected_sources(tested)["pulsar"]["repository"]},
    "result": "PASS_CURRENT_SOURCE_M2_REGRESSION_RAW_ONLY",
    "schema": "NEREUS_V2_M3_CURRENT_SOURCE_M2_REGRESSION_RUN_V1",
    "testedNereusCommit": tested,
}
(output / "run-summary.json").write_bytes(contract.canonical_bytes(summary))
print(f"trusted current-source M2 children generated: gates={len(children)} tests={sum(counts.values())}")
PY

assert_sources_unchanged
trap - EXIT INT TERM
cleanup
echo "V2 M3 current-source M2 regression raw run PASS: tested=$tested_commit gates=25 output=$output_dir published=false"
