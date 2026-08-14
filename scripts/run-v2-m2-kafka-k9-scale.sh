#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "usage: $0 <10000|100000> <tested-source-commit> <output-directory>" >&2
    exit 64
fi

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
tier="$1"
tested_source_commit="$2"
output_directory="$3"
case "$tier" in
    10000|100000) ;;
    *) echo "K9 scale tier must be 10000 or 100000" >&2; exit 64 ;;
esac
if [[ ! "$tested_source_commit" =~ ^[0-9a-f]{40}$ ]]; then
    echo "tested source commit must be 40 lowercase hex characters" >&2
    exit 64
fi
if ! git -C "$repo_root" cat-file -e "$tested_source_commit^{commit}"; then
    echo "tested source commit is unavailable" >&2
    exit 65
fi

plan="$repo_root/config/v2/m2/kafka/k9/bookkeeper-scale-plan.properties"
conformance="$repo_root/config/v2/m2/kafka/k9/bookkeeper-conformance.properties"
compose="$repo_root/config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml"
mkdir -p "$output_directory"
output_directory="$(cd "$output_directory" && pwd)"
result="$output_directory/scale-$tier.json"
stats="$output_directory/docker-stats-$tier.jsonl"
environment="$output_directory/environment-$tier.json"
project="nereus-v2-m2-k9-$tier"

cleanup() {
    docker compose -p "$project" -f "$compose" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

docker compose -p "$project" -f "$compose" up -d --wait
services=(metadata-service bookie-0 bookie-1 bookie-2)
container_ids=()
for service in "${services[@]}"; do
    container_id="$(docker compose -p "$project" -f "$compose" ps -q "$service")"
    if [[ -z "$container_id" ]]; then
        echo "K9 scale service has no container: $service" >&2
        exit 66
    fi
    container_ids+=("$container_id")
    image="$(docker inspect --format '{{.Config.Image}}' "$container_id")"
    expected="apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d"
    if [[ "$image" != "$expected" ]]; then
        echo "K9 scale service image differs: $service $image" >&2
        exit 66
    fi
done

: > "$stats"
(
    while true; do
        docker stats --no-stream --format '{{json .}}' "${container_ids[@]}" >> "$stats" || exit 0
        sleep 2
    done
) &
sampler_pid=$!

set +e
(
    cd "$repo_root"
    ./gradlew :nereus-kafka-bookkeeper:v2M2KafkaK9Scale --no-daemon \
        -Pv2M2KafkaK9ScalePlan="$plan" \
        -Pv2M2KafkaK9ConformanceConfig="$conformance" \
        -Pv2M2KafkaK9ScaleTier="$tier" \
        -Pv2M2KafkaK9ScaleOutput="$result" \
        -Pv2M2KafkaK9TestedSourceCommit="$tested_source_commit"
)
gradle_status=$?
set -e
kill "$sampler_pid" >/dev/null 2>&1 || true
wait "$sampler_pid" >/dev/null 2>&1 || true
if [[ $gradle_status -ne 0 ]]; then
    exit "$gradle_status"
fi

export K9_TIER="$tier"
export K9_PROJECT="$project"
export K9_COMPOSE="$compose"
export K9_OUTPUT="$environment"
export K9_STATS="$stats"
python3 - <<'PY'
import json
import os
import subprocess
from pathlib import Path

tier = int(os.environ["K9_TIER"])
project = os.environ["K9_PROJECT"]
compose = os.environ["K9_COMPOSE"]
output = Path(os.environ["K9_OUTPUT"])
stats_path = Path(os.environ["K9_STATS"])
services = ["metadata-service", "bookie-0", "bookie-1", "bookie-2"]

def compose_exec(service: str, command: list[str]) -> str:
    process = subprocess.run(
        ["docker", "compose", "-p", project, "-f", compose, "exec", "-T", service, *command],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return process.stdout.strip()

containers = []
for service in services:
    container_id = subprocess.check_output(
        ["docker", "compose", "-p", project, "-f", compose, "ps", "-q", service], text=True
    ).strip()
    inspect = json.loads(subprocess.check_output(["docker", "inspect", container_id], text=True))[0]
    volume_bytes = int(compose_exec(service, ["du", "-sb", "/opt/bookkeeper/data"]).split()[0])
    logs = subprocess.check_output(["docker", "logs", container_id], text=True, stderr=subprocess.STDOUT)
    log_path = output.parent / f"{service}-{tier}.log"
    log_path.write_text(logs)
    containers.append({
        "service": service,
        "containerId": container_id,
        "imageReference": inspect["Config"]["Image"],
        "imageId": inspect["Image"],
        "platform": inspect["Platform"],
        "volumeBytes": volume_bytes,
        "logFile": log_path.name,
    })

snapshots = []
for line in stats_path.read_text().splitlines():
    if line.strip():
        snapshots.append(json.loads(line))

document = {
    "schema": "NEREUS_V2_M2_KAFKA_K9_SCALE_ENVIRONMENT_V1",
    "tierPartitions": tier,
    "containers": containers,
    "dockerStatsSamples": len(snapshots),
    "dockerStatsFile": stats_path.name,
}
output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
PY

python3 "$repo_root/scripts/check-v2-m2-kafka-k9-scale-result.py" \
    "$plan" "$result" "$environment"
echo "K9 exact-image scale tier $tier completed; cluster volumes will now be removed."
