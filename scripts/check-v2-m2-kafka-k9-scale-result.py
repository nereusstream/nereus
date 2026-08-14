#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0.
import json
import sys
from pathlib import Path

if len(sys.argv) != 4:
    raise SystemExit("usage: check-v2-m2-kafka-k9-scale-result.py <plan> <result> <environment>")

def properties(path: Path) -> dict[str, str]:
    result = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            result[key] = value
    return result

plan = properties(Path(sys.argv[1]))
result = json.loads(Path(sys.argv[2]).read_text())
environment = json.loads(Path(sys.argv[3]).read_text())
tier = result.get("tierPartitions")
if result.get("schema") != "NEREUS_V2_M2_KAFKA_K9_SCALE_RESULT_V1" or result.get("result") != "PASS":
    raise SystemExit("K9 scale result schema or result differs")
if tier not in (10000, 100000) or environment.get("tierPartitions") != tier:
    raise SystemExit("K9 result/environment tier differs")
counts = result["counts"]
if counts.get("partitions") != tier:
    raise SystemExit("K9 scale result did not execute the exact partition tier")
if counts.get("ledgersCreated") != tier + int(plan["rolloverSamples"]):
    raise SystemExit("K9 scale ledger/rollover count differs")
if counts.get("maximumOwnedHandles") != int(plan["hotLedgerAdmission"]):
    raise SystemExit("K9 scale handle admission differs")
containers = environment.get("containers", [])
if [item.get("service") for item in containers] != [
    "metadata-service", "bookie-0", "bookie-1", "bookie-2"
]:
    raise SystemExit("K9 scale environment does not bind the exact topology")
expected_image = "apache/bookkeeper@sha256:" + plan["bookKeeperImageManifestSha256"]
for container in containers:
    if container.get("imageReference") != expected_image or container.get("platform") != "linux":
        raise SystemExit("K9 scale container source/platform differs")
    bound = (
        int(plan["maxMetadataVolumeBytes"])
        if container["service"] == "metadata-service"
        else int(plan["maxBookieVolumeBytesEach"])
    )
    if not 0 < container.get("volumeBytes", 0) <= bound:
        raise SystemExit("K9 scale container volume crossed the predeclared bound")
if environment.get("dockerStatsSamples", 0) <= 0:
    raise SystemExit("K9 scale environment has no Docker resource sample")
print(f"K9 scale tier {tier} result and exact-image environment verified")
