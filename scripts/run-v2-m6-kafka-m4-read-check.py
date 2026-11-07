#!/usr/bin/env python3
"""Qualify the current Kafka M4 read path without replaying an historical M5 design freeze."""

import argparse
import importlib.util
from pathlib import Path
import subprocess
import time
import zipfile


ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("m6_bk_process", ROOT / "scripts/run-v2-m6-kafka-bk-process-check.py")
BK = importlib.util.module_from_spec(spec)
spec.loader.exec_module(BK)
M6, JUNIT = BK.M6, BK.JUNIT

READ_CLASSES = {
    "nativePartition": ("nereus-kafka-bookkeeper", "com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperPartitionV1"),
    "nativeReadOwner": ("nereus-kafka-bookkeeper", "com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperM4BrokerReadOwnerV2"),
    "readAsyncExecutor": ("nereus-storage-object", "com.nereusstream.storage.object.read.BindingReadAsyncExecutorV1"),
    "selectorRuntime": ("nereus-storage-object", "com.nereusstream.storage.object.read.control.BindingReadSelectorRuntimeV1"),
    "readControl": ("nereus-storage-object", "com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1"),
    "proofCleanupPlanner": ("nereus-storage-object", "com.nereusstream.storage.object.read.control.M4ProofCleanupPlannerV1"),
    "oxiaReadRoute": ("nereus-metadata-oxia", "com.nereusstream.metadata.oxia.v2.retention.OxiaKafkaBookKeeperM4ReadRouteV2"),
}
SUITES = {
    "v2M5KafkaRunRootsRealTest": (
        "v2M5KafkaRunRootsRealTest", "com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperRunRootsV2RealTest", 16,
        "blockedOxiaSelectorCasDoesNotHoldPartitionFenceOrOldBkReadCompletion()"),
    "v2M5KafkaM4BrokerReadOwnerTest": (
        "test", "com.nereusstream.kafka.bookkeeper.broker.KafkaBookKeeperM4BrokerReadOwnerV2Test", 5, None),
}


def inputs(source):
    modules = ("nereus-domain", *BK.ARTIFACT_MODULES)
    paths = subprocess.check_output([
        "git", "ls-files", "-z", "--", "build.gradle.kts", "settings.gradle.kts", "gradle",
        "gradlew", BK.COMPOSE.relative_to(ROOT), "scripts/run-v2-m6-kafka-m4-read-check.py",
        "scripts/run-v2-m6-kafka-bk-process-check.py", "scripts/v2-m6-execution-inputs.py",
        "scripts/m5-evidence-junit.py", *modules,
    ], cwd=ROOT).decode().split("\0")
    tracked = M6.tree(ROOT, source)
    return {name: M6.file_identity(ROOT / name, tracked[name])["sha256"] for name in paths if name}


def only_jar(path, module):
    matches = sorted(file for file in (path / module).rglob("*.jar")
                     if not file.name.endswith(("-sources.jar", "-javadoc.jar")))
    M6.require(len(matches) == 1, f"missing or ambiguous M4 runtime artifact: {module}")
    return matches[0]


def runtime(repository):
    jars = {module: only_jar(repository / "com/nereusstream", module) for module in BK.ARTIFACT_MODULES}
    classes = {}
    for label, (module, name) in READ_CLASSES.items():
        relative = name.replace(".", "/") + ".class"
        with zipfile.ZipFile(jars[module]) as jar:
            raw = jar.read(relative)
        compiled = ROOT / module / "build/classes/java/main" / relative
        M6.require(M6.file_identity(compiled)["sha256"] == M6.INPUTS.digest(raw),
                   f"tested M4 class differs from the published runtime: {name}")
        classes[label] = M6.INPUTS.digest(raw)
    locked = {name: M6.file_identity(only_jar(BK.OXIA_CLIENT / "io/github/oxia-db", name))["sha256"]
              for name in ("oxia-client", "oxia-client-api")}
    return classes, {name: M6.file_identity(path)["sha256"] for name, path in jars.items()}, locked


def run(out):
    source = M6.clean_commit(ROOT)
    tree = M6.revision(ROOT, f"{source}^{{tree}}")
    M6.require(out == out.resolve() and M6.outside(out, [ROOT]),
               "M4 qualification output overlaps the execution source")
    out.mkdir(parents=True, exist_ok=False)
    source_inputs = inputs(source)
    source_bytes = M6.INPUTS.canonical(source_inputs)
    (out / "tested-inputs.json").write_bytes(source_bytes)
    repository = out / "development-repository"
    publication = BK.publish_development_repository(out, repository)
    repository_inventory = BK.repository_files(repository)
    with BK.bookkeeper_oxia(out) as address:
        command = [
            "./gradlew", "--offline", "--no-daemon", "--no-configuration-cache", "--no-build-cache",
            "--max-workers=2", "--rerun-tasks", "--project-cache-dir", str(out / "m4-gradle-cache"),
            "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers",
            f"-Pv2M5RetentionOxiaServiceAddress={address}",
            ":nereus-kafka-bookkeeper:test", "--tests", SUITES["v2M5KafkaM4BrokerReadOwnerTest"][1],
            ":nereus-kafka-bookkeeper:v2M5KafkaRunRootsRealTest",
        ]
        started = time.time_ns()
        with (out / "m4-gradle.log").open("xb") as log:
            completed = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        suites = []
        for task, (gradle_task, name, count, held_callback) in SUITES.items():
            path = ROOT / "nereus-kafka-bookkeeper/build/test-results" / gradle_task / f"TEST-{name}.xml"
            M6.require(path.is_file() and path.stat().st_mtime_ns >= started, f"missing fresh M4 suite: {task}")
            raw = JUNIT.regular(path)
            (out / f"{task}.xml").write_bytes(raw)
            cases = JUNIT.parse_suite(raw, f":nereus-kafka-bookkeeper:{gradle_task}")["cases"]
            M6.require(len(cases) == count and all(case["class"] == name and case["status"] == "passed" for case in cases)
                       and (held_callback is None or any(case["name"] == held_callback for case in cases)),
                       f"incomplete, failed or skipped M4 suite: {task}")
            suites.append({"task": task, "tests": count, "xmlSha256": M6.INPUTS.digest(raw)})
        M6.require(completed.returncode == 0, f"current M4 read checks failed: {out}")
    classes, artifacts, locked = runtime(repository)
    M6.require(source == M6.clean_commit(ROOT) and source_inputs == inputs(source)
               and repository_inventory == BK.repository_files(repository),
               "M4 qualification source or runtime repository changed")
    (out / "m4-read-result.json").write_bytes(M6.INPUTS.canonical({
        "schema": "NEREUS_V2_M6_KAFKA_M4_READ_QUALIFICATION_V1", "promotionEligible": False,
        "sourceCommit": source, "sourceTree": tree,
        "bookKeeperImageId": BK.BOOKKEEPER_ID, "oxiaImageId": BK.OXIA_ID,
        "testedInputManifestSha256": M6.INPUTS.digest(source_bytes),
        "suites": suites, "currentLoadedM4ReadClassSha256": classes,
        "currentM4RuntimeArtifactSha256": artifacts, "lockedOxiaClientArtifactSha256": locked,
        "publicationCommand": publication, "command": command,
        "backendFiles": {name: M6.file_identity(out / name) for name in ("backend.json", "bookkeeper.log", "oxia.log")},
    }))
    print(f"Current Kafka M4 read qualification passed: {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    run(parser.parse_args().output.absolute())
