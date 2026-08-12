#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
    echo "usage: $0 <kafka-checkout> <pulsar-checkout> <oxia-client-checkout> <oxia-server-checkout>" >&2
    exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
kafka_checkout="$1"
pulsar_checkout="$2"
oxia_client_checkout="$3"
oxia_server_checkout="$4"

python3 - "$repo_root" "$kafka_checkout" "$pulsar_checkout" "$oxia_client_checkout" "$oxia_server_checkout" <<'PY'
import hashlib
import json
import pathlib
import subprocess
import sys

root, kafka, pulsar, client, server = map(pathlib.Path, sys.argv[1:])
locks = json.loads((root / "docs/v2/source-locks.json").read_text())

expected = {
    kafka: locks["k1KafkaAuthorityBinding"]["finalForkCommit"],
    pulsar: locks["p1PulsarAuthorityBinding"]["finalForkCommit"],
    client: next(row for row in locks["dependencyForkOutputs"] if row["id"] == "oxia-client-notification-continuity")["finalForkCommit"],
    server: locks["dependencyEvidenceBindings"]["oxiaServerRuntime"]["sourceCommit"],
}
for checkout, commit in expected.items():
    if not (checkout / ".git").exists():
        raise SystemExit(f"V2 M1 exact-source check: missing Git checkout {checkout}")
    head = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
    dirty = subprocess.check_output(
        ["git", "-C", str(checkout), "status", "--porcelain=v1", "--untracked-files=all"], text=True
    )
    if head != commit or dirty:
        raise SystemExit(f"V2 M1 exact-source check: checkout differs: {checkout} head={head} dirty={bool(dirty)}")

if subprocess.check_output(
    ["git", "-C", str(root), "status", "--porcelain=v1", "--untracked-files=all"], text=True
):
    raise SystemExit("V2 M1 exact-source check: Nereus worktree is dirty")

def verify_bundle(binding_name, artifact_key="artifacts"):
    binding = locks[binding_name]
    bundle = root / binding["bundleRoot"]
    for row in binding[artifact_key].values():
        path = bundle / row["relativePath"]
        data = path.read_bytes()
        if len(data) != row["bytes"] or hashlib.sha256(data).hexdigest() != row["sha256"]:
            raise SystemExit(f"V2 M1 exact-source check: locked artifact differs: {path}")
    manifest = binding.get("manifest")
    if manifest:
        path = bundle / manifest["relativePath"]
        data = path.read_bytes()
        if len(data) != manifest["bytes"] or hashlib.sha256(data).hexdigest() != manifest["sha256"]:
            raise SystemExit(f"V2 M1 exact-source check: locked manifest differs: {path}")

verify_bundle("n1ArtifactBinding")
verify_bundle("p1MetadataCapabilityBinding")
client_binding = locks["dependencyEvidenceBindings"]["oxiaClientArtifacts"]
client_bundle = root / client_binding["bundleRoot"]
for row in client_binding["artifacts"].values():
    path = client_bundle / row["relativePath"]
    data = path.read_bytes()
    if len(data) != row["bytes"] or hashlib.sha256(data).hexdigest() != row["sha256"]:
        raise SystemExit(f"V2 M1 exact-source check: locked Oxia client artifact differs: {path}")
client_manifest = client_binding["manifest"]
path = client_bundle / client_manifest["relativePath"]
data = path.read_bytes()
if len(data) != client_manifest["bytes"] or hashlib.sha256(data).hexdigest() != client_manifest["sha256"]:
    raise SystemExit(f"V2 M1 exact-source check: locked Oxia client manifest differs: {path}")

runtime = locks["dependencyEvidenceBindings"]["oxiaServerRuntime"]
image = json.loads(subprocess.check_output(["docker", "image", "inspect", runtime["imageReference"]], text=True))[0]
if image.get("Id") != runtime["imageDigest"]:
    raise SystemExit("V2 M1 exact-source check: Oxia server image digest differs")
if (image.get("Config", {}).get("Labels") or {}).get("org.opencontainers.image.revision") != runtime["sourceCommit"]:
    raise SystemExit("V2 M1 exact-source check: Oxia server image revision differs")

print("V2 M1 exact-source tuple is clean and exact across Kafka, Pulsar, Oxia client/server, N1/P1 artifacts, and image")
PY
