#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0.
"""Run source-bound native Pulsar BookKeeper profiles (focused, no promotion)."""

import argparse
import contextlib
import hashlib
import importlib.util
import os
from pathlib import Path
import re
import socket
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "config/v2/m6/pulsar"
PROVIDER = "com.nereusstream.pulsar.broker.PulsarP1OxiaMetadataStoreProvider"
STORAGE = "com.nereusstream.pulsar.broker.PulsarM6ManagedLedgerStorage"
STARTER = "org.apache.pulsar.PulsarBrokerStarter"
CLIENT = "M6PulsarBookKeeperSemanticsProbe"


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


BK = module("m6_bk_backends", ROOT / "scripts/run-v2-m6-kafka-bk-process-check.py")
SOURCE = BK.M6


def source_checkout(root):
    SOURCE.require(not SOURCE.git(root, "status", "--porcelain", "--untracked-files=all"),
                   f"source checkout must be clean: {root}")
    return {"commit": SOURCE.git(root, "rev-parse", "HEAD").decode("ascii").strip(),
            "tree": SOURCE.git(root, "rev-parse", "HEAD^{tree}").decode("ascii").strip()}


def capture_runtime(source, output, task, java_home, *extra):
    command = [str(source / "gradlew"), "--no-configuration-cache", "-I",
               str(CONFIG / "capture-runtime.init.gradle"), f"-Dm6.runtime.output={output}", *extra, task]
    with output.with_suffix(".log").open("xb") as log:
        subprocess.run(command, cwd=source, env={**os.environ, "JAVA_HOME": str(java_home)},
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    return [Path(line) for line in output.read_text().splitlines()]


def runtime_paths(pulsar, out, java_home, async_object):
    native = capture_runtime(pulsar, out / "native-resolved.paths",
                             ":pulsar-broker:m6WriteRuntimeClasspath", java_home)
    sdk = capture_runtime(ROOT, out / "sdk-resolved.paths", ":nereus-pulsar-broker:m6WriteRuntimeClasspath",
                          java_home, f"-PpulsarCheckout={pulsar}")
    # Both applications compile against the pinned P1/N1 ABI. The actual Broker runs one current
    # metadata implementation and one SPI; keep the immutable N1 domain from the native graph.
    retained = [p for p in native if ("locked-artifacts/nereus-p1" not in str(p)
                and "locked-artifacts/nereus-n1" not in str(p)) or "/nereus-domain/" in str(p)]
    names = {p.name for p in retained if p.suffix == ".jar"}
    extra = [p for p in sdk if p.suffix == ".jar" and p.name not in names and (
        "/nereus-" in str(p) and "/build/libs/" in str(p)
        or "/software.amazon.awssdk/" in str(p) or "/org.apache.commons/commons-collections4/" in str(p)
        or async_object and any(f"/{group}/" in str(p) for group in (
            "com.github.luben", "org.apache.httpcomponents.client5", "org.apache.httpcomponents.core5")))]
    provider = [p for p in sdk if p.is_dir() and "/nereus-pulsar-broker/build/classes/" in str(p)]
    SOURCE.require(len(provider) == 1, "native provider classes missing from SDK runtime")
    ordered = [p for p in retained + extra + provider if p.exists()]
    for name in ("oxia-client", "oxia-client-api", "nereus-metadata-oxia", "nereus-metadata-spi", "nereus-domain",
                 "nereus-pulsar-offload", "bookkeeper-server"):
        found = [p for p in ordered if re.fullmatch(re.escape(name) + r"-[0-9].*\.jar", p.name)]
        SOURCE.require(len(found) == 1, f"runtime must contain exactly one {name}: {found}")
    if async_object:
        for name in ("httpclient5", "httpcore5", "zstd-jni", "nereus-storage-object-vault"):
            found = [p for p in ordered if re.fullmatch(re.escape(name) + r"-[0-9].*\.jar", p.name)]
            SOURCE.require(len(found) == 1, f"async Object runtime must contain exactly one {name}: {found}")
    return ordered


def path_identity(path):
    if path.is_dir():
        files = [{"path": item.relative_to(path).as_posix(), **BK.M6.file_identity(item)}
                 for item in sorted(path.rglob("*")) if item.is_file()]
        return {"path": str(path), "files": files}
    return {"path": str(path), **BK.M6.file_identity(path)}


def distinct_ports(count):
    with contextlib.ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(count)]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]


def run_to_log(command, log_path, *, timeout=100):
    with log_path.open("xb") as log:
        subprocess.run(command, cwd=log_path.parent, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=timeout)


def broker_config(template, target, overrides):
    config = template.read_text()
    for key, value in overrides.items():
        config = re.sub(r"(?m)^" + re.escape(key) + r"=.*$", "", config)
        config += f"\n{key}={value}\n"
    target.write_text(config)


def start_broker(stack, java, config, log, admin, expect_rejection=False):
    output = stack.enter_context(log.open("xb"))
    proc = subprocess.Popen(java + [STARTER, "--broker-conf", str(config)], stdout=output,
                            stderr=subprocess.STDOUT, cwd=log.parent)

    def stop(strict=False):
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(35)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                if strict:
                    raise
        if strict:
            SOURCE.require("Shut down broker service successfully." in log.read_text(),
                           f"Broker did not complete graceful shutdown: {log}")

    stack.callback(stop)
    if expect_rejection:
        SOURCE.require(proc.wait(35) != 0, "unqualified Broker startup unexpectedly succeeded")
        SOURCE.require("Nereus BookKeeper requires native Oxia ownership with MetadataStore ELM" in log.read_text(),
                       "startup did not fail at the native ownership capability check")
        return stop
    for _ in range(50):
        SOURCE.require(proc.poll() is None, f"Broker exited before health check: {log}")
        try:
            with urllib.request.urlopen(f"http://{admin}/admin/v2/brokers/health", timeout=2) as response:
                if response.status == 200:
                    return stop
        except OSError:
            time.sleep(1)
    raise TimeoutError(f"Broker did not pass its health check: {log}")


def run(pulsar, java_home, out, async_object):
    sources = {"nereus": source_checkout(ROOT), "pulsar": source_checkout(pulsar)}
    SOURCE.require(not out.is_relative_to(ROOT) and not out.is_relative_to(pulsar),
                   "evidence output must be outside both source trees")
    out.mkdir()
    ordered = runtime_paths(pulsar, out, java_home, async_object)
    inputs = [path_identity(p) for p in ordered]
    execution_sources = [
        Path(__file__).resolve(), ROOT / "scripts/run-v2-m6-kafka-bk-process-check.py",
        ROOT / "scripts/v2-m6-execution-inputs.py", ROOT / "scripts/m5-candidate-inputs.py",
        ROOT / "scripts/m5-evidence-junit.py",
        ROOT / "gradlew", pulsar / "gradlew", pulsar / "conf/broker.conf",
        CONFIG / "capture-runtime.init.gradle", CONFIG / "console-log.xml",
        CONFIG / "bookkeeper-cell.properties", CONFIG / (CLIENT + ".java"),
        CONFIG / "M6PulsarCellProvision.java", BK.COMPOSE,
    ]
    if async_object:
        execution_sources.extend(ROOT / "scripts" / name for name in (
            "run-v2-m6-kafka-async-object-check.py", "run-v2-m6-kafka-object-wal-check.py",
            "run-v2-m6-kafka-m4-read-check.py", "v2-m6-kafka-object-fault-proxy.py",
        ))
    source_inputs = [path_identity(path) for path in execution_sources]
    broker_jar, = [p for p in ordered if re.fullmatch(r"bookkeeper-server-[0-9].*\.jar", p.name)]
    template = pulsar / "conf/broker.conf"
    probes = out / "probe-classes"
    probes.mkdir()
    cp = os.pathsep.join(map(str, ordered + [probes]))
    java_exe = (java_home / "bin/java").resolve(strict=True)
    javac_exe = (java_home / "bin/javac").resolve(strict=True)
    java = [str(java_exe), "-Xmx512m", "-XX:MaxDirectMemorySize=256m", "-XX:ActiveProcessorCount=4",
            "--add-exports=java.management/sun.management=ALL-UNNAMED",
            "-Dlog4j.configurationFile=" + str(CONFIG / "console-log.xml"),
            f"-Dpulsar.metadatastore.providers={PROVIDER}", "-cp", cp]
    run_to_log([str(javac_exe), "-proc:none", "-cp", cp, "-d", str(probes),
                str(CONFIG / (CLIENT + ".java")), str(CONFIG / "M6PulsarCellProvision.java")], out / "javac.log")
    ports = distinct_ports(6)
    addresses = [f"127.0.0.1:{port}" for port in ports]
    bk_uri = "zk://127.0.0.1:2181/ledgers"
    with BK.bookkeeper_oxia(out) as oxia, contextlib.ExitStack() as stack:
        object_arguments = []
        if async_object:
            objects = module("m6_real_minio", ROOT / "scripts/run-v2-m6-kafka-async-object-check.py")
            vault = module("m6_real_vault", ROOT / "scripts/run-v2-m6-kafka-object-wal-check.py")
            proxy = module("m6_real_object_faults", ROOT / "scripts/v2-m6-kafka-object-fault-proxy.py")
            minio, _ = stack.enter_context(objects.minio(out))
            kms, token = stack.enter_context(vault.vault(out))
            relay, control, control_token = stack.enter_context(proxy.fault_proxy(minio))
            os.environ.update(AWS_ACCESS_KEY_ID="m6kafka", AWS_SECRET_ACCESS_KEY="m6kafka-test-secret",
                              NEREUS_M6_PULSAR_TEST_VAULT_TOKEN=token.read_text().strip(),
                              NEREUS_M6_PULSAR_FAULT_CONTROL=control, NEREUS_M6_PULSAR_FAULT_TOKEN=control_token,
                              NEREUS_M6_PULSAR_DIRECT_S3=minio)
            object_arguments = [minio, kms, relay]
        origin = f"oxia-nereus://{oxia}"
        run_to_log(java + ["org.apache.pulsar.PulsarClusterMetadataSetup", "--cluster", "m6", "--metadata-store",
                          origin, "--configuration-metadata-store", origin, "--existing-bk-metadata-service-uri",
                          bk_uri, "--web-service-url", f"http://{addresses[0]}", "--broker-service-url",
                          f"pulsar://{addresses[2]}", "--default-namespace-bundle-number", "1"], out / "init.log")
        cell = out / "cell.properties"
        digest = hashlib.sha256(broker_jar.read_bytes()).hexdigest()
        cell.write_text((CONFIG / "bookkeeper-cell.properties").read_text()
                        + f"\nbkClientJar={digest}\nbkMetadataUri={bk_uri}\n")
        run_to_log(java + ["com.nereusstream.pulsar.broker.M6PulsarCellProvision", oxia, str(cell), str(ROOT),
                           *object_arguments],
                   out / "provision.log")
        base = {"clusterName": "m6", "metadataStoreUrl": origin, "configurationMetadataStoreUrl": origin,
                "bookkeeperMetadataServiceUri": bk_uri, "bindAddress": "127.0.0.1",
                "advertisedAddress": "127.0.0.1", "defaultNumberOfNamespaceBundles": "1",
                "loadManagerClassName": "org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl",
                "loadManagerServiceUnitStateTableViewClassName":
                    "org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl",
                "loadBalancerLoadSheddingStrategy":
                    "org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder",
                "managedLedgerStorageClassName": STORAGE, "nereusPulsarCellConfig": str(cell),
                "managedLedgerDefaultEnsembleSize": "3", "managedLedgerDefaultWriteQuorum": "3",
                "managedLedgerDefaultAckQuorum": "2", "managedLedgerMaxEntriesPerLedger": "2",
                "managedLedgerMinLedgerRolloverTimeMinutes": "0", "loadBalancerSheddingEnabled": "false",
                "loadBalancerAutoBundleSplitEnabled": "false", "loadBalancerMultiPhaseBundleUnload": "false",
                "numIOThreads": "4", "numExecutorThreadPoolSize": "4"}
        brokers = []
        for i in range(2):
            config = out / f"broker-{i}.conf"
            broker_config(template, config, {**base, "webServicePort": ports[i], "brokerServicePort": ports[i+2]})
            brokers.append(start_broker(stack, java, config, out / f"broker-{i}.log", addresses[i]))
        state = out / "expected-cold-state.properties"
        args = [CLIENT, origin, *addresses[:4], str(state)]
        run_to_log(java + args + (["async-object", str(cell)] if async_object else []), out / "client.log")
        SOURCE.require("PULSAR_NATIVE_BK_PROTOCOL_SEMANTICS_PASS" in (out / "client.log").read_text(),
                       "native BK protocol checks did not reach their terminal assertions")
        SOURCE.require("PULSAR_NATIVE_MISSING_BINDING_REJECTED" in (out / "client.log").read_text(),
                       "missing Binding was not rejected before native ledger creation")
        if async_object:
            probe = (out / "client.log").read_text()
            SOURCE.require("PULSAR_NATIVE_ENROLLED_NPD1_NPO1_PUBLISHED" in probe
                           and "PULSAR_NATIVE_OBJECT_LOST_REPLY_AND_INDEPENDENT_ROLLOVER" in probe
                           and "PULSAR_NATIVE_ORIGINAL_OFFLOAD_DRAIN_AND_RESUME" in probe,
                           "native enrolled output and independent rollover did not reach terminal assertions")
        SOURCE.require(state.is_file(), "the official client did not record its exact cold restart expectations")
        for stop in reversed(brokers):
            stop(strict=True)
        restarted = start_broker(stack, java, out / "broker-0.conf", out / "restarted.log", addresses[0])
        run_to_log(java + args + ["restart"], out / "restart-client.log")
        SOURCE.require("PULSAR_NATIVE_BK_RESTART_PASS" in (out / "restart-client.log").read_text(),
                       "native BK restart did not reach its terminal assertions")
        restarted(strict=True)
        negative = out / "unqualified.conf"
        broker_config(template, negative, {**base, "webServicePort": ports[4], "brokerServicePort": ports[5],
                                          "metadataStoreUrl": f"oxia://{oxia}"})
        start_broker(stack, java, negative, out / "unqualified.log", addresses[4], expect_rejection=True)
    SOURCE.require(sources == {"nereus": source_checkout(ROOT), "pulsar": source_checkout(pulsar)},
                   "source checkout changed during execution")
    SOURCE.require(inputs == [path_identity(p) for p in ordered], "runtime inputs changed during execution")
    SOURCE.require(source_inputs == [path_identity(path) for path in execution_sources],
                   "fixture build/config/launch inputs changed during execution")
    generated_launch_inputs = {"native-resolved.paths", "sdk-resolved.paths", "cell.properties",
                               "broker-0.conf", "broker-1.conf", "unqualified.conf"}
    artifacts = [p for p in sorted(out.iterdir()) if p.is_file() and p.name not in generated_launch_inputs]
    (out / "process-result.json").write_bytes(SOURCE.INPUTS.canonical({
        "schema": "NEREUS_V2_M6_PULSAR_BK_FOCUSED_PROCESS_V1", "promotionEligible": False, "status": "PASS",
        "profile": "BOOKKEEPER_WAL_ASYNC_OBJECT" if async_object else "BOOKKEEPER_WAL_ONLY",
        "sources": sources, "sourceInputs": source_inputs, "runtime": inputs,
        "launchTools": [path_identity(java_exe), path_identity(javac_exe)],
        "generatedLaunchInputs": {name: BK.M6.file_identity(out / name) for name in sorted(generated_launch_inputs)},
        "compiledProbe": path_identity(probes), "outputs": {p.name: BK.M6.file_identity(p) for p in artifacts},
        "checks": ["official-client", "rollover", "individual-and-cumulative-ack", "native-transfer",
                   "cold-read-and-reset", "native-restart", "missing-binding", "unsupported-ownership-backend",
                   *(["enrolled-npd1-npo1", "original-put-response-lost", "independent-bookkeeper-rollover", "original-output-drain-and-resume"]
                     if async_object else [])],
    }))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pulsar-root", type=Path, required=True)
    parser.add_argument("--java-home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--async-object", action="store_true",
                        help="use the enrolled BookKeeper WAL plus asynchronous Object profile")
    args = parser.parse_args()
    try:
        run(args.pulsar_root.resolve(strict=True), args.java_home.resolve(strict=True), args.output.resolve(), args.async_object)
    except (OSError, ValueError, subprocess.SubprocessError, BK.M6.InputError) as error:
        parser.exit(1, f"Pulsar BookKeeper process check: {error}\n")


if __name__ == "__main__":
    main()
