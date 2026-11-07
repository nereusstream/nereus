#!/usr/bin/env python3
"""Run focused native Kafka BookKeeper checks against owned real backends."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parent.parent
TASK = ":core:nereusBookKeeperOwnerRelocationRealTest"
XML_DIR = Path("core/build/test-results/nereusBookKeeperOwnerRelocationRealTest")
RUNTIME_INIT = ROOT / "config/v2/m6/capture-kafka-runtime.init.gradle"
COMPOSE = ROOT / "config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml"
BOOKKEEPER = "apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d"
BOOKKEEPER_ID = "sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605"
OXIA_ID = "sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da"
OXIA_CLIENT = ROOT / "gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2"
ARTIFACT_MODULES = (
    "nereus-storage-api", "nereus-metadata-spi", "nereus-storage-object",
    "nereus-storage-bookkeeper", "nereus-kafka-bookkeeper", "nereus-metadata-oxia",
)
CASES = {
    "kafka.server.nereus.KafkaBookKeeperAckRealTest": {
        "bothAckModesWaitForSharedCommitAndAckZeroHasNoResponse(Path)",
        "nativeFetchPreservesCompleteCompressedBatchesAcrossViewChanges(Path)",
        "staleQuorumCallbackCannotPublishAfterNativeOwnerChange(Path)",
    },
    "kafka.server.nsip.KafkaBookKeeperOwnerRelocationRealTest": {
        "liveMetadataDeltaHandoffPreservesGroupAndTransactionCoordinators()",
        "coldOwnerContinuesProduceFetchAndTransactionAcrossTwoNativeRelocations()",
        "smallRunsCheckpointAcrossNativeEpoch256AndRepeatedColdTakeover()",
    },
    "kafka.server.nsip.KafkaBookKeeperProcessRealTest": {
        "productionLauncherRecoversSnapshotAndRelocatesToColdBroker(Path)",
    },
}


def module(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / file)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


JUNIT = module("m6_process_junit", "m5-evidence-junit.py")
M6 = module("m6_process_inputs", "v2-m6-execution-inputs.py")


def output(*args):
    return subprocess.check_output(args, text=True).strip()


def observe_image(reference, expected_id, platform):
    info = json.loads(output("docker", "image", "inspect", reference))[0]
    M6.require(info["Id"] == expected_id and (info["Os"], info["Architecture"]) == platform,
               f"unexpected backend image: {reference}")
    return info["Id"]


def publish_development_repository(out, repository):
    command = [
        "./gradlew", "--offline", "--no-daemon", "--no-configuration-cache", "--no-build-cache",
        "--rerun-tasks", "--max-workers=2", "--project-cache-dir", str(out / "nereus-gradle-cache"),
        f"-PdevelopmentRepository={repository}",
        *[f":{name}:publishMavenJavaPublicationToDevelopmentRepository" for name in ARTIFACT_MODULES],
    ]
    with (out / "nereus-publication.log").open("xb") as log:
        subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
    return command


def repository_files(repository):
    M6.require(repository.is_dir(), "current Nereus development Maven repository is missing")
    result = {}
    for path in sorted(repository.rglob("*")):
        M6.require(not path.is_symlink(), f"redirected Maven repository input: {path}")
        if not path.is_dir():
            result[path.relative_to(repository).as_posix()] = M6.file_identity(path)
    M6.require(all(any(path.startswith(f"com/nereusstream/{module}/") and path.endswith(".jar")
                       and not path.endswith("-sources.jar") for path in result)
                   for module in ARTIFACT_MODULES), "incomplete Nereus Kafka runtime artifact set")
    return result


def process_logs(out, started):
    directory = out / "kafka-process-logs"
    required = {f"node-{node}/{name}" for node in range(5)
                for name in ("server.log", "format.log", "server.properties")}
    required.add("node-2/implicit-format.log")
    files = {}
    for path in sorted(directory.rglob("*")):
        M6.require(not path.is_symlink() and path.resolve() == path, f"redirected Kafka process evidence: {path}")
        if not path.is_dir():
            M6.require(path.stat().st_mtime_ns >= started, f"stale Kafka process evidence: {path}")
            files[path.relative_to(directory).as_posix()] = M6.file_identity(path)
    rejections = {name for name in files if name.endswith("/disabled-restart.log")}
    M6.require(required <= files.keys() and len(rejections) == 1
               and (next(iter(rejections)).removesuffix("disabled-restart.log") + "disabled.properties") in files,
               f"incomplete Kafka process logs: {directory}")
    return files


def run_checks(kafka, java, repository, out):
    root_commit = M6.clean_commit(ROOT)
    kafka_commit = M6.clean_commit(kafka)
    trees = {"nereus": M6.revision(ROOT, f"{root_commit}^{{tree}}"),
             "kafka": M6.revision(kafka, f"{kafka_commit}^{{tree}}")}
    inputs = [ROOT, kafka] + ([] if repository is None else [repository])
    M6.require(out == out.resolve() and M6.outside(out, inputs),
               "process evidence output overlaps inputs or contains a symlink")
    observe_image(BOOKKEEPER, BOOKKEEPER_ID, ("linux", "amd64"))
    observe_image(OXIA_ID, OXIA_ID, ("linux", "arm64"))
    out.mkdir(parents=True, exist_ok=False)
    publication = None
    if repository is None:
        repository = out / "development-repository"
        publication = publish_development_repository(out, repository)
    files = repository_files(repository)
    inventory = M6.INPUTS.canonical({"path": str(repository), "publicationCommand": publication, "files": files})
    (out / "development-repository.json").write_bytes(inventory)
    project = "nereus-m6-kafka-bk-" + uuid.uuid4().hex[:12]
    compose = ("docker", "compose", "-p", project, "-f", str(COMPOSE))
    oxia = None
    try:
        subprocess.run([*compose, "up", "-d", "--pull", "never", "--wait"], check=True)
        bookies = output(*compose, "ps", "-q").splitlines()
        M6.require(len(bookies) == 4, "expected one ZooKeeper process and three bookies")
        oxia = output(
            "docker", "run", "--pull", "never", "--detach", "--name", project + "-oxia",
            "--publish", "127.0.0.1::6648", "--publish", "127.0.0.1::8080",
            OXIA_ID, "oxia", "standalone", "--shards=4",
        )
        facts = json.loads(output("docker", "inspect", *bookies, oxia))
        M6.require([row["Image"] for row in facts] == [BOOKKEEPER_ID] * 4 + [OXIA_ID],
                   "launched backend images differ from the pinned inputs")
        (out / "backend.json").write_bytes(M6.INPUTS.canonical(facts))
        address = output("docker", "port", oxia, "6648/tcp")
        metrics = output("docker", "port", oxia, "8080/tcp")
        for attempt in range(30):
            try:
                with urllib.request.urlopen(f"http://{metrics}/metrics", timeout=2) as response:
                    M6.require(response.status == 200, "Oxia metrics endpoint did not start")
                break
            except OSError:
                if attempt == 29:
                    raise
                time.sleep(1)
        runtime_manifest = out / "kafka-runtime.json"
        command = [
            "./gradlew", "--offline", "--no-daemon", "--no-configuration-cache", "--max-workers=2",
            "--init-script", str(RUNTIME_INIT), f"-Dnereus.m6.kafkaRuntimeCapture={runtime_manifest}",
            f"-PnereusDevelopmentRepository={repository}", "-PnereusDevelopmentVersion=0.2.0-SNAPSHOT",
            f"-PnereusLockedOxiaRepository={OXIA_CLIENT}",
            "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers",
            f"-Pv2M5RetentionOxiaServiceAddress={address}", TASK,
        ]
        env = dict(os.environ, JAVA_HOME=str(java), PATH=str(java / "bin") + os.pathsep + os.environ["PATH"])
        started = time.time_ns()
        with (out / "gradle.log").open("xb") as log:
            completed = subprocess.run(command, cwd=kafka, env=env, stdout=log, stderr=subprocess.STDOUT)
        cases = {}
        attachments = {}
        for name in CASES:
            xml = kafka / XML_DIR / f"TEST-{name}.xml"
            if not xml.is_file() or xml.stat().st_mtime_ns < started:
                continue
            shutil.copyfile(xml, out / xml.name)
            raw = JUNIT.regular(out / xml.name)
            attachments[xml.name] = {"bytes": len(raw), "sha256": M6.INPUTS.digest(raw)}
            suite = JUNIT.parse_suite(raw, TASK)
            cases[name] = {case["name"] for case in suite["cases"] if case["class"] == name
                           and case["status"] == "passed"}
            M6.require(len(suite["cases"]) == len(CASES[name]), f"unexpected case count: {name}")
        M6.require(completed.returncode == 0 and cases == CASES,
                   f"native BK process suite failed, skipped a case or produced incomplete XML: {out}")
        M6.require(runtime_manifest.stat().st_mtime_ns >= started, "stale Kafka runtime capture")
        runtime = json.loads(runtime_manifest.read_bytes())
        M6.require(runtime["schema"] == "NEREUS_V2_M6_KAFKA_RUNTIME_V1"
                   and Path(runtime["java"]).resolve(strict=True) == (java / "bin/java").resolve(strict=True),
                   "Kafka runtime capture did not use the declared Java executable")
        runtime_identity = M6.file_identity(runtime_manifest)
        M6.require(M6.clean_commit(ROOT) == root_commit and M6.clean_commit(kafka) == kafka_commit,
                   "source changed during native process checks")
        M6.require(repository_files(repository) == files, "Nereus runtime Maven repository changed during Kafka checks")
        process = process_logs(out, started)
    finally:
        try:
            if oxia is not None:
                try:
                    with (out / "oxia.log").open("xb") as log:
                        subprocess.run(["docker", "logs", oxia], stdout=log, stderr=subprocess.STDOUT, check=True)
                finally:
                    subprocess.run(["docker", "rm", "-f", oxia], check=True, stdout=subprocess.DEVNULL)
        finally:
            try:
                with (out / "bookkeeper.log").open("xb") as log:
                    subprocess.run([*compose, "logs"], stdout=log, stderr=subprocess.STDOUT)
            finally:
                subprocess.run([*compose, "down", "-v", "--remove-orphans"], check=True)
    (out / "process-result.json").write_bytes(M6.INPUTS.canonical({
        "kind": "FOCUSED_KAFKA_BK_PROCESS_CHECK", "promotionEligible": False,
        "sourceCheckout": {"nereus": root_commit, "kafka": kafka_commit}, "command": command,
        "sourceTrees": trees, "kafkaRuntime": {"path": runtime_manifest.name, **runtime_identity},
        "developmentRepositorySha256": M6.INPUTS.digest(inventory),
        "junit": attachments, "cases": {key: sorted(value) for key, value in cases.items()},
        "processLogs": process,
        "backendFiles": {name: M6.file_identity(out / name) for name in ("backend.json", "bookkeeper.log", "oxia.log")},
    }))
    print(f"{sum(map(len, CASES.values()))} native Kafka BK process checks passed: {out}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-root", type=Path, required=True)
    parser.add_argument("--kafka-java-home", type=Path, required=True)
    parser.add_argument("--development-repository", type=Path,
                        help="reuse an external repository for diagnostics; the default builds from current source")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        repository = args.development_repository.resolve(strict=True) if args.development_repository else None
        run_checks(args.kafka_root.resolve(strict=True), args.kafka_java_home.resolve(strict=True),
                   repository, args.output.absolute())
    except (M6.InputError, JUNIT.JUnitEvidenceError, OSError, subprocess.CalledProcessError, ValueError) as error:
        parser.exit(1, f"Kafka BK process check: {error}\n")


if __name__ == "__main__":
    main()
