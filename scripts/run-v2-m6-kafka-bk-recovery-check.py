#!/usr/bin/env python3
"""Run the current-source Kafka BookKeeper recovery and compaction cuts over owned BK/Oxia."""

from __future__ import annotations

import argparse
import importlib.util
import os
from pathlib import Path
import subprocess
import time


ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("m6_kafka_async_backends",
                                              ROOT / "scripts/run-v2-m6-kafka-async-object-check.py")
ASYNC = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ASYNC)
BK, M4, M6, JUNIT = ASYNC.BK, ASYNC.M4, ASYNC.M6, ASYNC.JUNIT
TASK = "nereusBookKeeperM4AdmissionRealTest"
SUITE = "kafka.server.nsip.KafkaBookKeeperM4AdmissionRealTest"
METHODS = (
    "historicalFetchAcrossRunsAndCachedIndexUsesOnlyLocalControlState",
    "concurrentM4OwnerCloseWaitsForItsActualHistoricalReader",
    "brokerReconcilesLostPersistedOpenResponseBeforeNativeCreate",
    "twoBrokerTakeoversRetainOneCreatedUnselectedLedger",
    "confirmedCreateWithUnresolvedTicketStaysUnselectedUntilSuccessorReconciles",
    "lostRecoveredNativeFactsRetainOldWriterTicketUntilNextBroker",
    "successorActivatesAfterFencingReservedButNeverCreatedLedger",
)
COMPACTION = ("nativeM5BookKeeperOnlyInternalTopicsCompactReclaimAndRecover",)


def run_cases(kafka, java, out, repository, receipt, artifacts, methods):
    out.mkdir()
    runtime_path = out / f"{TASK}-runtime.json"
    with BK.bookkeeper_oxia(out) as oxia:
        command = [
            "./gradlew", "--offline", "--no-daemon", "--no-configuration-cache", "--max-workers=2",
            "--init-script", str(BK.RUNTIME_INIT), f"-Dnereus.m6.kafkaRuntimeCapture={runtime_path}",
            "-Dnereus.m6.kafkaBkRecoveryRuntimeCapture=true",
            f"-PnereusDevelopmentRepository={repository}", f"-PnereusDevelopmentVersion={BK.DEVELOPMENT_VERSION}",
            f"-PnereusLockedOxiaRepository={BK.OXIA_CLIENT}",
            "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers",
            f"-Pv2M5RetentionOxiaServiceAddress={oxia}", f"-Pv2M5KafkaRunSourceRoot={ROOT}",
            f"-Pv2M5KafkaRunSummaryFile={receipt}", "-Pv2M6KafkaReadQualification=true",
            f"-Pv2M5KafkaSealedGcProfile={str(methods == COMPACTION).lower()}", f":core:{TASK}",
        ]
        for method in methods:
            command.extend(("--tests", f"{SUITE}.{method}"))
        env = dict(os.environ, JAVA_HOME=str(java), PATH=str(java / "bin") + os.pathsep + os.environ["PATH"])
        started = time.time_ns()
        with (out / "gradle.log").open("xb") as log:
            completed = subprocess.run(command, cwd=kafka, env=env, stdout=log, stderr=subprocess.STDOUT)
        if completed.returncode != 0:
            xml = kafka / "core/build/test-results" / TASK / f"TEST-{SUITE}.xml"
            if xml.is_file() and xml.stat().st_mtime_ns >= started:
                (out / xml.name).write_bytes(JUNIT.regular(xml))
            completed.check_returncode()
        test = ASYNC.read_test(kafka, java, out, started, TASK, SUITE,
                               {method + "()" for method in methods}, artifacts)
    return {
        **test, "command": command,
        "backendFiles": {name: M6.file_identity(out / name)
                         for name in ("backend.json", "bookkeeper.log", "oxia.log", "gradle.log")},
    }


def run(kafka, java, out):
    roots = {"nereus": ROOT, "kafka": kafka}
    commits = {name: M6.clean_commit(root) for name, root in roots.items()}
    trees = {name: M6.revision(root, f"{commits[name]}^{{tree}}") for name, root in roots.items()}
    M6.require(out == out.resolve() and M6.outside(out, roots.values()), "output overlaps the source inputs")
    out.mkdir(parents=True, exist_ok=False)
    m4 = out / "m4"
    M4.run(m4)
    repository = m4 / "development-repository"
    files = BK.repository_files(repository)
    receipt = m4 / "m4-read-result.json"
    qualified = M6.file_identity(receipt)
    _, jars, locked = M4.runtime(repository)
    artifacts = {f"{name}-{BK.DEVELOPMENT_VERSION}.jar": digest for name, digest in jars.items()}
    artifacts.update({M4.only_jar(BK.OXIA_CLIENT / "io/github/oxia-db", name).name: digest
                      for name, digest in locked.items()})
    domain = M4.only_jar(BK.N1_REPOSITORY / "com/nereusstream", "nereus-domain")
    tracked = M6.tree(ROOT, commits["nereus"])
    artifacts[domain.name] = M6.file_identity(domain, tracked[domain.relative_to(ROOT).as_posix()])["sha256"]
    executor_inputs = {
        path: M6.file_identity(ROOT / path, tracked[path]) for path in (
            "scripts/run-v2-m6-kafka-bk-recovery-check.py", "scripts/run-v2-m6-kafka-bk-process-check.py",
            "scripts/run-v2-m6-kafka-m4-read-check.py", "scripts/run-v2-m6-kafka-async-object-check.py",
            "scripts/v2-m6-execution-inputs.py", "scripts/m5-evidence-junit.py",
            BK.COMPOSE.relative_to(ROOT).as_posix(), BK.RUNTIME_INIT.relative_to(ROOT).as_posix(),
        )
    }
    # Native GC capacity must be installed before the first reservation in its physical namespace.
    executions = {
        "internal-compaction": run_cases(kafka, java, out / "internal-compaction", repository, receipt, artifacts,
                                         COMPACTION),
        "recovery": run_cases(kafka, java, out / "recovery", repository, receipt, artifacts, METHODS),
    }
    M6.require(all(M6.clean_commit(root) == commits[name] for name, root in roots.items())
               and qualified == M6.file_identity(receipt) and files == BK.repository_files(repository),
               "Kafka BK recovery execution inputs changed")
    (out / "bk-recovery-result.json").write_bytes(M6.INPUTS.canonical({
        "kind": "FOCUSED_KAFKA_BK_RECOVERY_CHECK", "promotionEligible": False,
        "sourceCheckout": commits, "sourceTrees": trees,
        "executorInputs": executor_inputs,
        "m4Qualification": {"path": str(receipt.relative_to(out)), **qualified},
        "executions": executions,
    }))
    print(f"{len(METHODS) + len(COMPACTION)} native Kafka BK recovery/compaction checks passed: {out}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-root", type=Path, required=True)
    parser.add_argument("--kafka-java-home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        run(args.kafka_root.resolve(strict=True), args.kafka_java_home.resolve(strict=True), args.output.absolute())
    except (OSError, ValueError, subprocess.CalledProcessError, M6.InputError, M6.INPUTS.ClosureError,
            JUNIT.JUnitEvidenceError) as error:
        parser.exit(1, f"Kafka BK recovery check: {error}\n")


if __name__ == "__main__":
    main()
