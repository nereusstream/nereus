#!/usr/bin/env python3
"""Run the focused Kafka Object WAL adapter and production process checks on exact sources."""

import argparse
from contextlib import contextmanager
import importlib.util
import json
import os
from pathlib import Path
import secrets
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
ASYNC_RUNNER = ROOT / "scripts/run-v2-m6-kafka-async-object-check.py"
SPEC = importlib.util.spec_from_file_location("m6_async_backends", ASYNC_RUNNER)
ASYNC = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ASYNC)
BK, M4, M6, JUNIT = ASYNC.BK, ASYNC.M4, ASYNC.M6, ASYNC.JUNIT
VAULT_REF = "hashicorp/vault@sha256:268bb80aa9c6d13d65fcfa05c0c268caca068952240a8087291a6ce0b66e3a10"
VAULT_ID = "sha256:ba5ade3c7f155978f41dbca62b16e5e08f5331f5d12f9be2d333698b49484457"
PROCESS_TASK = "nereusKafkaObjectWalProcessRealTest"
CASES = {
    "nereusObjectWalAdapterRealTest": (
        "kafka.server.nereus.KafkaObjectWalAdapterRealTest",
        "publishesOnlyAfterTheExactGrantAndReopensTheExactPhysicalRoot(Path)",
    ),
    "nereusKafkaObjectWalAuthorizationRealTest": (
        "kafka.server.nereus.KafkaObjectWalAuthorizationRealTest",
        "nativeObjectWalWaitsForExactAuthorizationAfterLostCasResponse(Path)",
    ),
    PROCESS_TASK: (
        "kafka.server.nereus.KafkaObjectWalProcessRealTest",
        "productionObjectWalPublishesAndRecoversOnColdBroker(Path)",
    ),
}


@contextmanager
def vault(out):
    BK.observe_image(VAULT_REF, VAULT_ID, ("linux", "arm64"))
    token = secrets.token_hex(24)
    token_file = out / "vault-token"
    descriptor = os.open(token_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w", encoding="ascii") as token_output:
        token_output.write(token + "\n")
    process = None
    try:
        process = subprocess.check_output(
            ["docker", "run", "--detach", "--rm", "--pull", "never", "--cap-add", "IPC_LOCK",
             "--env", "VAULT_DEV_ROOT_TOKEN_ID", "--publish", "127.0.0.1::8200", VAULT_ID,
             "server", "-dev", "-dev-listen-address=0.0.0.0:8200"],
            env=dict(os.environ, VAULT_DEV_ROOT_TOKEN_ID=token), text=True).strip()
        image = BK.output("docker", "inspect", "--format", "{{.Image}}", process)
        M6.require(image == VAULT_ID, "running Vault image differs from its pinned input")
        endpoint = "http://" + BK.output("docker", "port", process, "8200/tcp")
        for attempt in range(24):
            try:
                with urllib.request.urlopen(endpoint + "/v1/sys/health", timeout=2) as response:
                    M6.require(response.status == 200, "Vault Transit did not start")
                break
            except OSError:
                if attempt == 23:
                    raise
                time.sleep(0.5)
        cli = ["docker", "exec", "--env", "VAULT_ADDR=http://127.0.0.1:8200", "--env", "VAULT_TOKEN",
               process, "vault"]
        for arguments in (["secrets", "enable", "transit"], ["write", "-f", "transit/keys/nereus-kafka-wal"]):
            subprocess.run([*cli, *arguments], env=dict(os.environ, VAULT_TOKEN=token),
                           stdout=subprocess.DEVNULL, check=True)
        (out / "vault.json").write_bytes(M6.INPUTS.canonical({
            "container": process, "imageReference": VAULT_REF, "imageConfigDigest": image, "endpoint": endpoint,
            "mount": "transit", "key": "nereus-kafka-wal", "tokenFileSha256": M6.file_identity(token_file)["sha256"],
        }))
        yield endpoint, token_file
    finally:
        try:
            if process is not None:
                subprocess.run(["docker", "stop", process], stdout=subprocess.DEVNULL, check=True)
        finally:
            token_file.unlink()


def result_for(task, kafka, java, out, started, artifacts):
    suite, case = CASES[task]
    xml = kafka / "core/build/test-results" / task / f"TEST-{suite}.xml"
    M6.require(xml.stat().st_mtime_ns >= started, f"stale Kafka Object WAL test output: {task}")
    raw = JUNIT.regular(xml)
    cases = JUNIT.parse_suite(raw, ":core:" + task)["cases"]
    M6.require(len(cases) == 1 and cases[0]["class"] == suite and cases[0]["name"] == case
               and cases[0]["status"] == "passed", f"unexpected Kafka Object WAL results: {task}")
    (out / xml.name).write_bytes(raw)
    capture = out / f"{task}-runtime.json"
    M6.require(capture.stat().st_mtime_ns >= started, f"stale Kafka Object WAL runtime capture: {task}")
    runtime = json.loads(capture.read_bytes())
    launches_process = task == PROCESS_TASK
    log = str(kafka / "config/log4j2.yaml") if launches_process else None
    M6.require(runtime["schema"] == "NEREUS_V2_M6_KAFKA_RUNTIME_V1"
               and bool(runtime["processClasspath"]) == launches_process and runtime["processLogConfig"] == log
               and Path(runtime["java"]).resolve(strict=True) == (java / "bin/java").resolve(strict=True),
               f"Kafka Object WAL used a different native runtime: {task}")
    for classpath in ("testClasspath", "processClasspath") if launches_process else ("testClasspath",):
        jars = [(Path(path).name, runtime["files"][path].get("sha256"))
                for path in runtime[classpath] if path.endswith(".jar")]
        for filename, sha in artifacts.items():
            M6.require([digest for name, digest in jars if name == filename] == [sha],
                       f"Kafka Object WAL loaded a different runtime artifact: {task}/{classpath}/{filename}")
    return {
        "case": case, "xml": {"name": xml.name, **M6.file_identity(out / xml.name)},
        "runtime": {"name": capture.name, **M6.file_identity(capture)},
        "processLogs": process_logs(out, started) if launches_process else {},
    }


def process_logs(out, started):
    directory = out / "kafka-process-logs"
    required = {f"node-{node}/{name}" for node in range(3)
                for name in ("server.log", "launcher.log", "format.log", "server.properties")}
    required.update({"node-2/controller.log", "node-1/foreign-object.log", "node-1/foreign-object.properties",
                     "node-1/unauthorized-vault.log", "node-1/unauthorized-vault.properties",
                     "capability.properties", "object-capability"})
    result = {}
    for path in sorted(directory.rglob("*")):
        M6.require(not path.is_symlink() and path.resolve() == path, f"redirected Kafka process evidence: {path}")
        if not path.is_dir():
            M6.require(path.stat().st_mtime_ns >= started, f"stale Kafka process evidence: {path}")
            result[path.relative_to(directory).as_posix()] = M6.file_identity(path)
    M6.require(required <= result.keys(), f"incomplete Kafka Object WAL process logs: {directory}")
    return result


def run_task(task, kafka, java, out, repository, artifacts):
    out.mkdir()
    with (vault(out) as (vault_url, token), ASYNC.minio(out) as (minio, _), BK.bookkeeper_oxia(out) as oxia,
          ASYNC.PROXY.fault_proxy(minio) as (relay, control, fault_token)):
        (out / "minio-fault-relay.json").write_bytes(M6.INPUTS.canonical({
            "relay": relay, "control": control, "upstream": minio,
            "controlTokenSha256": M6.INPUTS.digest(fault_token.encode("ascii")),
        }))
        command = [
            "./gradlew", "--offline", "--no-daemon", "--no-configuration-cache", "--max-workers=2",
            "--init-script", str(BK.RUNTIME_INIT), f"-Dnereus.m6.kafkaRuntimeCapture={out / 'runtime.json'}",
            "-Dnereus.m6.kafkaObjectWalRuntimeCapture=true",
            f"-PnereusDevelopmentRepository={repository}", f"-PnereusDevelopmentVersion={BK.DEVELOPMENT_VERSION}",
            f"-PnereusLockedOxiaRepository={BK.OXIA_CLIENT}",
            "-Pv2M2BookKeeperMetadataServiceUri=zk://127.0.0.1:2181/ledgers",
            f"-Pv2M5RetentionOxiaServiceAddress={oxia}", f"-Pv2M6MinioEndpoint={minio}",
            f"-Pv2M6VaultEndpoint={vault_url}", f"-Pv2M6VaultTokenFile={token}",
            f"-Pv2M6ObjectWalProcessEvidenceDir={out / 'kafka-process-logs'}", ":core:" + task,
        ]
        started = time.time_ns()
        env = dict(os.environ, JAVA_HOME=str(java), PATH=str(java / "bin") + os.pathsep + os.environ["PATH"],
                   NEREUS_M6_TEST_FAULT_PROXY=relay, NEREUS_M6_TEST_FAULT_CONTROL=control,
                   NEREUS_M6_TEST_FAULT_TOKEN=fault_token)
        with (out / "gradle.log").open("xb") as log:
            completed = subprocess.run(command, cwd=kafka, env=env, stdout=log, stderr=subprocess.STDOUT)
        if completed.returncode != 0:
            xml = kafka / "core/build/test-results" / task / f"TEST-{CASES[task][0]}.xml"
            if xml.is_file() and xml.stat().st_mtime_ns >= started:
                (out / xml.name).write_bytes(JUNIT.regular(xml))
            completed.check_returncode()
        result = result_for(task, kafka, java, out, started, artifacts)
    return {
        **result, "command": command,
        "backendFiles": {name: M6.file_identity(out / name)
                         for name in ("backend.json", "bookkeeper.log", "oxia.log", "minio.json",
                                      "minio.log", "minio-fault-relay.json", "vault.json", "gradle.log")},
    }


def run(kafka, java, out):
    commits = {"nereus": M6.clean_commit(ROOT), "kafka": M6.clean_commit(kafka)}
    trees = {"nereus": M6.revision(ROOT, f"{commits['nereus']}^{{tree}}"),
             "kafka": M6.revision(kafka, f"{commits['kafka']}^{{tree}}")}
    M6.require(out == out.resolve() and M6.outside(out, [ROOT, kafka]), "output overlaps the source inputs")
    tracked = M6.tree(ROOT, commits["nereus"])
    executor_paths = (
        Path(__file__), ASYNC_RUNNER, BK.RUNTIME_INIT, BK.COMPOSE, ASYNC.FAULT_PROXY,
        ROOT / "scripts/run-v2-m6-kafka-m4-read-check.py", ROOT / "scripts/run-v2-m6-kafka-bk-process-check.py",
        ROOT / "scripts/v2-m6-execution-inputs.py", ROOT / "scripts/m5-evidence-junit.py", ROOT / "docs/v2/source-locks.json",
    )
    executor_inputs = {path.relative_to(ROOT).as_posix():
                       M6.file_identity(path, tracked[path.relative_to(ROOT).as_posix()])
                       for path in executor_paths}
    out.mkdir(parents=True, exist_ok=False)
    repository = out / "development-repository"
    BK.publish_development_repository(out, repository)
    repository_inputs = BK.repository_files(repository)
    _, jars, locked = M4.runtime(repository)
    artifacts = {f"{name}-{BK.DEVELOPMENT_VERSION}.jar": sha for name, sha in jars.items()}
    artifacts.update({M4.only_jar(BK.OXIA_CLIENT / "io/github/oxia-db", name).name: sha for name, sha in locked.items()})
    domain = M4.only_jar(BK.N1_REPOSITORY / "com/nereusstream", "nereus-domain")
    artifacts[domain.name] = M6.file_identity(domain, tracked[domain.relative_to(ROOT).as_posix()])["sha256"]
    tests = {task: run_task(task, kafka, java, out / task, repository, artifacts) for task in CASES}
    M6.require(BK.repository_files(repository) == repository_inputs
               and all(M6.clean_commit(root) == commits[name] for name, root in (("nereus", ROOT), ("kafka", kafka))),
               "Kafka Object WAL execution inputs changed")
    (out / "object-wal-fixture-result.json").write_bytes(M6.INPUTS.canonical({
        "kind": "FOCUSED_KAFKA_OBJECT_WAL_CHECK", "promotionEligible": False,
        "sourceCheckout": commits, "sourceTrees": trees, "tests": tests,
        "executorInputs": executor_inputs, "repositoryInputs": repository_inputs,
    }))
    print(f"{len(CASES)} native Kafka Object WAL checks passed: {out}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-root", type=Path, required=True)
    parser.add_argument("--kafka-java-home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        run(args.kafka_root.resolve(strict=True), args.kafka_java_home.resolve(strict=True), args.output.absolute())
    except (M6.InputError, JUNIT.JUnitEvidenceError, OSError, subprocess.CalledProcessError, ValueError) as error:
        parser.exit(1, f"Kafka Object WAL check: {error}\n")


if __name__ == "__main__":
    main()
