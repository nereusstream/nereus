#!/usr/bin/env python3
"""Run the four native Kafka BookKeeper process checks against owned real backends."""

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
COMPOSE = ROOT / "config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml"
BOOKKEEPER = "apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d"
BOOKKEEPER_ID = "sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605"
OXIA_ID = "sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da"
OXIA_CLIENT = ROOT / "gradle/locked-artifacts/oxia-client-java/091a42c2780d92da56e9ec1f02ce1c3d988adc16/m2"
CASES = {
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


def run_checks(kafka, java, repository, out):
    root_commit = M6.clean_commit(ROOT)
    kafka_commit = M6.clean_commit(kafka)
    M6.require(M6.outside(out, [ROOT, kafka, repository]), "process evidence output overlaps inputs")
    M6.require(repository.is_dir(), "current Nereus development Maven repository is missing")
    observe_image(BOOKKEEPER, BOOKKEEPER_ID, ("linux", "amd64"))
    observe_image(OXIA_ID, OXIA_ID, ("linux", "arm64"))
    out.mkdir(parents=True, exist_ok=False)
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
        command = [
            "./gradlew", "--offline", "--no-daemon", "--no-configuration-cache", "--max-workers=2",
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
        M6.require(M6.clean_commit(ROOT) == root_commit and M6.clean_commit(kafka) == kafka_commit,
                   "source changed during native process checks")
    finally:
        try:
            if oxia is not None:
                with (out / "oxia.log").open("xb") as log:
                    subprocess.run(["docker", "logs", oxia], stdout=log, stderr=subprocess.STDOUT, check=True)
                subprocess.run(["docker", "rm", "-f", oxia], check=True, stdout=subprocess.DEVNULL)
        finally:
            with (out / "bookkeeper.log").open("xb") as log:
                subprocess.run([*compose, "logs"], stdout=log, stderr=subprocess.STDOUT)
            subprocess.run([*compose, "down", "-v", "--remove-orphans"], check=True)
    (out / "process-result.json").write_bytes(M6.INPUTS.canonical({
        "kind": "FOCUSED_KAFKA_BK_PROCESS_CHECK", "promotionEligible": False,
        "sourceCheckout": {"nereus": root_commit, "kafka": kafka_commit}, "command": command,
        "junit": attachments, "cases": {key: sorted(value) for key, value in cases.items()},
    }))
    print(f"Four native Kafka BK process checks passed: {out}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-root", type=Path, required=True)
    parser.add_argument("--kafka-java-home", type=Path, required=True)
    parser.add_argument("--development-repository", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        run_checks(args.kafka_root.resolve(strict=True), args.kafka_java_home.resolve(strict=True),
                   args.development_repository.resolve(strict=True), args.output.resolve())
    except (M6.InputError, JUNIT.JUnitEvidenceError, OSError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"Kafka BK process check: {error}\n")


if __name__ == "__main__":
    main()
