#!/usr/bin/env python3
"""Run the focused native Kafka BK-to-Object fixture on real BK, Oxia and MinIO."""

import argparse
from contextlib import contextmanager
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request


ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("m6_m4", ROOT / "scripts/run-v2-m6-kafka-m4-read-check.py")
M4 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M4)
BK, M6, JUNIT = M4.BK, M4.M6, M4.JUNIT
FAULT_PROXY = ROOT / "scripts/m6-kafka-object-fault-proxy.py"
PROXY = BK.module("m6_kafka_object_fault_proxy", FAULT_PROXY)
MINIO_ID = "sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253"
DEVELOPMENT_VERSION = "0.2.0-SNAPSHOT"
N1_REPOSITORY = ROOT / "gradle/locked-artifacts/nereus-n1/330aaec349c51fb2ace52b1085e8a9e5a60b5e3e/m2"
CASES = {
    "nereusBookKeeperObjectMaterializationRealTest": (
        "kafka.server.nereus.KafkaBookKeeperObjectMaterializationRealTest",
        {f'"storesAndVerifiesTheRealNativeSealedPrefix(String, Path).outcome={case}"'
         for case in ("known", "hot", "cold", "automatic", "retirement", "faults", "semantic")},
    ),
    "nereusObjectProviderAdmissionRealTest": (
        "kafka.server.nereus.KafkaObjectProviderAdmissionRealTest",
        {"reopensExactRegisteredScopeAndRejectsForeignBucket(Path)"},
    ),
}


@contextmanager
def minio(out):
    BK.observe_image(MINIO_ID, MINIO_ID, ("linux", "arm64"))
    container = BK.output(
        "docker", "run", "--detach", "--rm", "--pull", "never", "--publish", "127.0.0.1::9000",
        "-e", "MINIO_ROOT_USER=m6kafka", "-e", "MINIO_ROOT_PASSWORD=m6kafka-test-secret",
        MINIO_ID, "server", "/data",
    )
    try:
        image = BK.output("docker", "inspect", "--format", "{{.Image}}", container)
        M6.require(image == MINIO_ID, "the running MinIO image differs from the pinned input")
        endpoint = "http://" + BK.output("docker", "port", container, "9000/tcp")
        (out / "minio.json").write_bytes(M6.INPUTS.canonical(
            {"container": container, "image": image, "endpoint": endpoint}))
        for attempt in range(20):
            try:
                with urllib.request.urlopen(endpoint + "/minio/health/live", timeout=2) as response:
                    M6.require(response.status == 200, "MinIO did not start")
                break
            except OSError:
                if attempt == 19:
                    raise
                time.sleep(1)
        yield endpoint
    finally:
        try:
            with (out / "minio.log").open("xb") as log:
                subprocess.run(["docker", "logs", container], stdout=log, stderr=subprocess.STDOUT, check=True)
        finally:
            subprocess.run(["docker", "stop", container], stdout=subprocess.DEVNULL, check=True)


def read_test(kafka, java, out, started, task, name, expected, artifacts):
    xml = kafka / "core/build/test-results" / task / f"TEST-{name}.xml"
    M6.require(xml.stat().st_mtime_ns >= started, f"stale Kafka async Object XML for {task}")
    raw = JUNIT.regular(xml)
    (out / xml.name).write_bytes(raw)
    cases = JUNIT.parse_suite(raw, ":core:" + task)["cases"]
    M6.require(len(cases) == len(expected)
               and {case["name"] for case in cases if case["class"] == name and case["status"] == "passed"} == expected,
               f"unexpected, skipped or failed Kafka async Object cases for {task}")
    capture = out / f"{task}-runtime.json"
    M6.require(capture.stat().st_mtime_ns >= started, f"stale Kafka runtime capture for {task}")
    runtime = json.loads(capture.read_bytes())
    M6.require(runtime["schema"] == "NEREUS_V2_M6_KAFKA_RUNTIME_V1"
               and not runtime["processClasspath"] and runtime["processLogConfig"] is None
               and Path(runtime["java"]).resolve(strict=True) == (java / "bin/java").resolve(strict=True),
               f"Kafka test did not use its declared TestKit runtime for {task}")
    runtime_jars = [(Path(path).name, runtime["files"][path].get("sha256"))
                    for path in runtime["testClasspath"] if path.endswith(".jar")]
    for filename, expected_sha in artifacts.items():
        M6.require([sha for name, sha in runtime_jars if name == filename] == [expected_sha],
                   f"Kafka test did not load the exact artifact for {filename}: {task}")
    return {"xml": {"name": xml.name, "bytes": len(raw), "sha256": M6.INPUTS.digest(raw)},
            "runtime": {"name": capture.name, **M6.file_identity(capture)}, "cases": sorted(expected)}


def run(kafka, java, out):
    commits = {"nereus": M6.clean_commit(ROOT), "kafka": M6.clean_commit(kafka)}
    trees = {"nereus": M6.revision(ROOT, f"{commits['nereus']}^{{tree}}"),
             "kafka": M6.revision(kafka, f"{commits['kafka']}^{{tree}}")}
    M6.require(out == out.resolve() and M6.outside(out, [ROOT, kafka]), "output overlaps the source inputs")
    tracked = M6.tree(ROOT, commits["nereus"])
    executor_paths = (
        Path(__file__), BK.RUNTIME_INIT, BK.COMPOSE, FAULT_PROXY,
        ROOT / "scripts/run-v2-m6-kafka-m4-read-check.py",
        ROOT / "scripts/run-v2-m6-kafka-bk-process-check.py",
        ROOT / "scripts/v2-m6-execution-inputs.py", ROOT / "scripts/m5-evidence-junit.py",
    )
    executor_inputs = {path.relative_to(ROOT).as_posix():
                       M6.file_identity(path, tracked[path.relative_to(ROOT).as_posix()])
                       for path in executor_paths}
    out.mkdir(parents=True, exist_ok=False)
    m4 = out / "m4"
    M4.run(m4)
    receipt = m4 / "m4-read-result.json"
    receipt_identity = M6.file_identity(receipt)
    repository = m4 / "development-repository"
    repository_inputs = BK.repository_files(repository)
    _, jars, locked = M4.runtime(repository)
    # Gradle exposes the base snapshot name even when Maven publishes a timestamped file.
    artifacts = {f"{name}-{DEVELOPMENT_VERSION}.jar": sha for name, sha in jars.items()}
    artifacts.update({M4.only_jar(BK.OXIA_CLIENT / "io/github/oxia-db", name).name: sha
                      for name, sha in locked.items()})
    domain = M4.only_jar(N1_REPOSITORY / "com/nereusstream", "nereus-domain")
    artifacts[domain.name] = M6.file_identity(domain, tracked[domain.relative_to(ROOT).as_posix()])["sha256"]
    with minio(out) as endpoint, BK.bookkeeper_oxia(out) as oxia, PROXY.fault_proxy(endpoint) as fault:
        relay, control, token = fault
        (out / "minio-fault-relay.json").write_bytes(M6.INPUTS.canonical({
            "relay": relay, "control": control, "upstream": endpoint,
            "controlTokenSha256": M6.INPUTS.digest(token.encode("ascii")),
        }))
        command = [
            "./gradlew", "--offline", "--no-daemon", "--no-configuration-cache", "--max-workers=2",
            "--init-script", str(BK.RUNTIME_INIT), f"-Dnereus.m6.kafkaRuntimeCapture={out / 'runtime.json'}",
            "-Dnereus.m6.kafkaObjectRuntimeCapture=true",
            f"-PnereusDevelopmentRepository={repository}", f"-PnereusDevelopmentVersion={DEVELOPMENT_VERSION}",
            f"-PnereusLockedOxiaRepository={BK.OXIA_CLIENT}",
            f"-Pv2M5KafkaRunSourceRoot={ROOT}", f"-Pv2M5KafkaRunSourceSummary={receipt}",
            "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers",
            f"-Pv2M5RetentionOxiaServiceAddress={oxia}", f"-Pv2M6MinioEndpoint={endpoint}",
            *[":core:" + task for task in CASES],
        ]
        env = dict(os.environ, JAVA_HOME=str(java), PATH=str(java / "bin") + os.pathsep + os.environ["PATH"],
                   NEREUS_M6_TEST_FAULT_PROXY=relay, NEREUS_M6_TEST_FAULT_CONTROL=control,
                   NEREUS_M6_TEST_FAULT_TOKEN=token)
        started = time.time_ns()
        with (out / "gradle.log").open("xb") as log:
            subprocess.run(command, cwd=kafka, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
        tests = {task: read_test(kafka, java, out, started, task, name, expected, artifacts)
                 for task, (name, expected) in CASES.items()}
        M6.require(M6.clean_commit(ROOT) == commits["nereus"] and M6.clean_commit(kafka) == commits["kafka"],
                   "source changed during the native Kafka Object run")
        M6.require(M6.file_identity(receipt) == receipt_identity and BK.repository_files(repository) == repository_inputs,
                   "M4 receipt or development repository changed during the native Kafka Object run")
    (out / "object-fixture-result.json").write_bytes(M6.INPUTS.canonical({
        "kind": "FOCUSED_KAFKA_ASYNC_OBJECT_TESTKIT_CHECK", "promotionEligible": False,
        "sourceCheckout": commits, "sourceTrees": trees, "command": command, "tests": tests,
        "executorInputs": executor_inputs,
        "m4Qualification": {"path": str(receipt.relative_to(out)), **receipt_identity},
        "backendFiles": {name: M6.file_identity(out / name)
                         for name in ("backend.json", "bookkeeper.log", "oxia.log", "minio.json", "minio.log", "minio-fault-relay.json")},
    }))
    print(f"{sum(len(expected) for _, expected in CASES.values())} native Kafka async Object checks passed: {out}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-root", type=Path, required=True)
    parser.add_argument("--kafka-java-home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        run(args.kafka_root.resolve(strict=True), args.kafka_java_home.resolve(strict=True), args.output.absolute())
    except (M6.InputError, JUNIT.JUnitEvidenceError, OSError, subprocess.CalledProcessError, ValueError) as error:
        parser.exit(1, f"Kafka async Object check: {error}\n")


if __name__ == "__main__":
    main()
