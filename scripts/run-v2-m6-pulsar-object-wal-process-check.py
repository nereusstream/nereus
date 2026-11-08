#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0.
"""Run the native Pulsar Object WAL profile on real Oxia, BookKeeper, S3, Vault, and official clients."""

import argparse
import base64
import contextlib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
CONFIG = ROOT / "config/v2/m6/pulsar"
SELECTION_WRAPPER = ROOT / (
    "docs/v2/evidence/v2-m3/children/final-source-e5e53e62865c21845621037bea5f18c092bd4259/"
    "10-ALLOCATOR_SELECTION/attachments/00-ALLOCATOR_V5_CAMPAIGN_VERIFICATION.json")
SELECTION_WRAPPER_SHA = "b064a3429414f522ac9ad4114f9885149310227f62991a268861d905bbd5d9f2"
SELECTION_SHA = "92c13584eb2ca08e38b299d4facc6e65cab5cd6e4e6b7bec12aaf83356f8e2fe"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


PUL = load("m6_pulsar_bk", SCRIPTS / "run-v2-m6-pulsar-bk-process-check.py")
OBJECTS = PUL.module("m6_pulsar_minio", SCRIPTS / "run-v2-m6-kafka-async-object-check.py")
FAULTS = PUL.module("m6_pulsar_faults", SCRIPTS / "v2-m6-kafka-object-fault-proxy.py")
VAULT = PUL.module("m6_pulsar_vault", SCRIPTS / "run-v2-m6-kafka-object-wal-check.py")
SOURCE = PUL.SOURCE


def selection(out):
    PUL.run_to_log([sys.executable, str(SCRIPTS / "check-v2-m4-historical-m3.py"), "--repo-root", str(ROOT)],
                   out / "historical-m3.log")
    original = SELECTION_WRAPPER.read_bytes()
    SOURCE.require(hashlib.sha256(original).hexdigest() == SELECTION_WRAPPER_SHA,
                   "M3 Final allocator wrapper differs from the frozen artifact")
    raw = base64.b64decode(json.loads(original)["selectionBase64"], validate=True)
    SOURCE.require(hashlib.sha256(raw).hexdigest() == SELECTION_SHA, "M3 exact NARS5 selection differs")
    path = out / "range64-receipt.bin"
    path.write_bytes(raw)
    return path


def start_broker(stack, java, config, log, endpoint, rejection=None):
    output = stack.enter_context(log.open("xb"))
    process = subprocess.Popen(java + [PUL.STARTER, "--broker-conf", str(config)], stdout=output,
                               stderr=subprocess.STDOUT, cwd=log.parent)

    def stop(graceful=False):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(35)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                if graceful:
                    raise
        if graceful:
            SOURCE.require("Shut down broker service successfully." in log.read_text(),
                           f"Object WAL Broker did not drain: {log}")

    stack.callback(stop)
    if rejection:
        SOURCE.require(process.wait(35) != 0 and rejection in log.read_text(),
                       f"Object WAL startup did not fail at the expected capability check: {log}")
    else:
        for attempt in range(50):
            SOURCE.require(process.poll() is None, f"Object WAL Broker exited before readiness: {log}")
            try:
                with urllib.request.urlopen(f"http://{endpoint}/admin/v2/brokers/health", timeout=2) as response:
                    if response.status == 200:
                        return process, stop
            except OSError:
                if attempt == 49:
                    raise
                time.sleep(1)
    return process, stop


def run(pulsar, java_home, out):
    sources = {"nereus": PUL.source_checkout(ROOT), "pulsar": PUL.source_checkout(pulsar)}
    SOURCE.require(not out.is_relative_to(ROOT) and not out.is_relative_to(pulsar),
                   "Object WAL results must be outside the source trees")
    out.mkdir()
    receipt = selection(out)
    ordered = PUL.runtime_paths(pulsar, out, java_home, True, object_wal=True)
    input_names = [Path(__file__), SCRIPTS / "run-v2-m6-pulsar-bk-process-check.py",
                   SCRIPTS / "run-v2-m6-kafka-async-object-check.py", SCRIPTS / "run-v2-m6-kafka-object-wal-check.py",
                   SCRIPTS / "run-v2-m6-kafka-bk-process-check.py", SCRIPTS / "v2-m6-kafka-object-fault-proxy.py",
                   SCRIPTS / "run-v2-m6-kafka-m4-read-check.py", SCRIPTS / "check-v2-m4-historical-m3.py",
                   SCRIPTS / "check-v2-m3-final.py", SCRIPTS / "check-v2-m3-child.py",
                   SCRIPTS / "v2-m6-execution-inputs.py", SCRIPTS / "m5-candidate-inputs.py",
                   SCRIPTS / "m5-evidence-junit.py", ROOT / "docs/v2/v2-scenarios.json",
                   SELECTION_WRAPPER, ROOT / "gradlew", pulsar / "gradlew", pulsar / "conf/broker.conf",
                   CONFIG / "capture-runtime.init.gradle", CONFIG / "console-log.xml",
                   CONFIG / "bookkeeper-cell.properties", PUL.BK.COMPOSE]
    probes = [CONFIG / name for name in ("M6PulsarObjectWalProvision.java", "M6PulsarBookKeeperSemanticsProbe.java",
                                         "M6PulsarObjectWalProbe.java")]
    input_names.extend(probes)
    source_inputs = [PUL.path_identity(path) for path in input_names]
    runtime = [PUL.path_identity(path) for path in ordered]
    java_exe, javac = ((java_home / "bin" / name).resolve(strict=True) for name in ("java", "javac"))
    executables = [java_exe, javac, Path(sys.executable).resolve(strict=True)]
    for name in ("git", "docker"):
        tool = shutil.which(name)
        SOURCE.require(tool is not None, f"fixture requires {name} on PATH")
        executables.append(Path(tool).resolve(strict=True))
    launch_tools = [PUL.path_identity(path) for path in executables]
    compiled = out / "probe-classes"
    compiled.mkdir()
    cp = os.pathsep.join(map(str, ordered + [compiled]))
    java = [str(java_exe), "-Xmx512m", "-XX:MaxDirectMemorySize=256m", "-XX:ActiveProcessorCount=4",
            "--add-exports=java.management/sun.management=ALL-UNNAMED",
            f"-Dlog4j.configurationFile={CONFIG / 'console-log.xml'}",
            f"-Dpulsar.metadatastore.providers={PUL.PROVIDER}", "-cp", cp]
    PUL.run_to_log([str(javac), "-proc:none", "-cp", cp, "-d", str(compiled), *map(str, probes)], out / "javac.log")
    ports = PUL.distinct_ports(8)
    addresses = [f"127.0.0.1:{port}" for port in ports]
    template = pulsar / "conf/broker.conf"
    bk_uri = "zk://127.0.0.1:2181/ledgers"
    launched = {}
    with PUL.BK.bookkeeper_oxia(out) as oxia, contextlib.ExitStack() as stack:
        minio, _ = stack.enter_context(OBJECTS.minio(out))
        vault, token = stack.enter_context(VAULT.vault(out))
        relay, control, control_token = stack.enter_context(FAULTS.fault_proxy(minio))
        os.environ.update(AWS_ACCESS_KEY_ID="m6kafka", AWS_SECRET_ACCESS_KEY="m6kafka-test-secret",
                          NEREUS_M6_PULSAR_DIRECT_S3=minio, NEREUS_M6_PULSAR_FAULT_CONTROL=control,
                          NEREUS_M6_PULSAR_FAULT_TOKEN=control_token)
        credentials = out / "s3-credentials"
        fd = os.open(credentials, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as secret:
            secret.write("[m6pulsar]\naws_access_key_id = m6kafka\naws_secret_access_key = m6kafka-test-secret\n")
        stack.callback(credentials.unlink)
        origin = f"oxia-nereus://{oxia}"
        PUL.run_to_log(java + ["org.apache.pulsar.PulsarClusterMetadataSetup", "--cluster", "m6", "--metadata-store",
                              origin, "--configuration-metadata-store", origin, "--existing-bk-metadata-service-uri",
                              bk_uri, "--web-service-url", f"http://{addresses[0]}", "--broker-service-url",
                              f"pulsar://{addresses[2]}", "--default-namespace-bundle-number", "1"], out / "init.log")
        cell = out / "cell.properties"
        settings = {"authorityRoot": "/nereus/v2/m6/native-object-wal", "bkMetadataUri": bk_uri,
                    "objectWalBookKeeperZkServers": "127.0.0.1:2181", "objectWalBookKeeperLedgerRoot": "/m6-object-compat",
                    "objectWalRange64Receipt": receipt, "objectWalProviderProfile": out / "object-provider-profile.bin",
                    "objectWalIoThreads": 3, "objectWalPendingAppends": 16, "objectWalPendingBytes": 64 * 1024 * 1024,
                    "s3Endpoint": relay, "s3Bucket": "m6-native-pulsar-wal", "s3Region": "us-east-1",
                    "s3CredentialsFile": credentials, "vaultEndpoint": vault, "vaultMount": "transit",
                    "vaultKey": "nereus-kafka-wal", "vaultTokenFile": token}
        for key, jar in (("bkClientJar", "bookkeeper-server"), ("objectWalDomainJarSha256", "nereus-domain"),
                         ("objectWalMetadataSpiJarSha256", "nereus-metadata-spi"),
                         ("objectWalMetadataOxiaJarSha256", "nereus-metadata-oxia")):
            artifact, = [path for path in ordered if re.fullmatch(re.escape(jar) + r"-[0-9].*\.jar", path.name)]
            settings[key] = hashlib.sha256(artifact.read_bytes()).hexdigest()
        config_text = (CONFIG / "bookkeeper-cell.properties").read_text()
        for key, value in settings.items():
            config_text = re.sub(r"(?m)^" + re.escape(key) + r"=.*$", "", config_text)
            config_text += f"\n{key}={value}\n"
        cell.write_text(config_text + "\nbinding.persistent\\://public/m6-native/physical-run=1\n")
        PUL.run_to_log(java + ["com.nereusstream.pulsar.broker.M6PulsarObjectWalProvision", oxia,
                              str(cell), str(ROOT)], out / "provision.log")
        SOURCE.require("PULSAR_OBJECT_WAL_ACTUAL_NAMESPACE_AND_PROVIDER_PROVISIONED" in (out / "provision.log").read_text(),
                       "the real Object WAL namespace and provider did not finish enrollment")
        base = {"clusterName": "m6", "metadataStoreUrl": origin, "configurationMetadataStoreUrl": origin,
                "bookkeeperMetadataServiceUri": bk_uri, "bindAddress": "127.0.0.1", "advertisedAddress": "127.0.0.1",
                "defaultNumberOfNamespaceBundles": "1",
                "loadManagerClassName": "org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl",
                "loadManagerServiceUnitStateTableViewClassName":
                    "org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl",
                "loadBalancerLoadSheddingStrategy":
                    "org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder",
                "managedLedgerStorageClassName": PUL.STORAGE, "nereusPulsarCellConfig": str(cell),
                "managedLedgerDefaultEnsembleSize": "3", "managedLedgerDefaultWriteQuorum": "3",
                "managedLedgerDefaultAckQuorum": "2", "managedLedgerMaxEntriesPerLedger": "2",
                "managedLedgerMinLedgerRolloverTimeMinutes": "0", "loadBalancerSheddingEnabled": "false",
                "loadBalancerAutoBundleSplitEnabled": "false", "loadBalancerMultiPhaseBundleUnload": "false",
                "numIOThreads": "4", "numExecutorThreadPoolSize": "4"}
        brokers = []
        for i in range(2):
            path = out / f"broker-{i}.conf"
            PUL.broker_config(template, path, {**base, "webServicePort": ports[i], "brokerServicePort": ports[i + 2]})
            brokers.append(start_broker(stack, java, path, out / f"broker-{i}.log", addresses[i]))
        expected = out / "expected-cold-state.properties"
        args = [PUL.CLIENT, origin, *addresses[:4], str(expected)]
        PUL.run_to_log(java + args, out / "client.log")
        SOURCE.require("PULSAR_NATIVE_BK_PROTOCOL_SEMANTICS_PASS" in (out / "client.log").read_text(),
                       "shared official Pulsar protocol probe did not finish")
        for _, stop in reversed(brokers):
            stop(True)
        cold, stop = start_broker(stack, java, out / "broker-0.conf", out / "restarted.log", addresses[0])
        PUL.run_to_log(java + args + ["restart", "record-restart-position"], out / "restart-client.log")
        peer, stop_peer = start_broker(stack, java, out / "broker-1.conf", out / "after-crash-broker.log", addresses[1])
        native_probe = "M6PulsarObjectWalProbe"
        ready = out / "original-put-held"
        with (out / "before-crash.log").open("xb") as probe_log, subprocess.Popen(
            java + [native_probe, "before-crash-with-unknown", origin, addresses[0], addresses[2], str(expected),
                    str(cell), str(ready)], stdout=probe_log, stderr=subprocess.STDOUT, cwd=out,
        ) as original:
            try:
                deadline = time.monotonic() + 35
                while not ready.is_file():
                    SOURCE.require(original.poll() is None and time.monotonic() < deadline,
                                   "official producer did not observe one held applied NWG1 PUT; see before-crash.log")
                    time.sleep(0.05)
                original.kill()
                SOURCE.require(original.wait(10) < 0, "the official producer can still resend")
                cold.kill()
                SOURCE.require(cold.wait(15) < 0, "Object WAL crash cut did not kill the actual Broker")
            finally:
                if original.poll() is None:
                    original.kill()
        request = urllib.request.Request(control, method="PATCH", headers={"X-Nereus-Test-Token": control_token})
        with urllib.request.urlopen(request, timeout=5) as response:
            held = json.load(response)
        SOURCE.require(held["hits"] == 1 and held["appliedVersion"] and held["selectedPath"],
                       "lost response must correspond to one exact applied immutable PUT")
        (out / "original-put-wire.json").write_bytes(SOURCE.INPUTS.canonical(held))
        PUL.run_to_log(java + [native_probe, "after-crash", origin, addresses[1], addresses[3], str(expected), str(cell)],
                       out / "after-crash.log", timeout=120)
        recovered = (out / "after-crash.log").read_text()
        SOURCE.require("PULSAR_OBJECT_WAL_CRASH_TAKEOVER_PASS" in recovered
                       and "PULSAR_OBJECT_WAL_ORIGINAL_UNKNOWN_RECOVERY_PASS" in recovered,
                       "official Pulsar client did not verify takeover and exact original UNKNOWN recovery")
        foreign = out / "foreign"
        foreign.mkdir()
        foreign_cell = foreign / "cell.properties"
        foreign_text = config_text.replace("public/m6-native", "public/m6-foreign").replace("/handoff=1", "/sentinel=1")
        foreign_settings = {
            "authorityRoot": "/nereus/v2/m6/native-object-wal-foreign",
            "deploymentId": "00000000000000010000000000000012",
            "reservationDomainId": "00000000000000030000000000000014",
            "pulsarCellId": "00000000000000050000000000000016",
            "objectWalBookKeeperLedgerRoot": "/m6-object-compat-foreign",
            "objectWalProviderProfile": foreign / "object-provider-profile.bin",
            "s3Bucket": "m6-native-pulsar-wal-foreign",
        }
        for key, value in foreign_settings.items():
            foreign_text = re.sub(r"(?m)^" + re.escape(key) + r"=.*$", "", foreign_text)
            foreign_text += f"\n{key}={value}\n"
        foreign_cell.write_text(foreign_text)
        foreign_web, foreign_service = f"localhost:{ports[6]}", f"localhost:{ports[7]}"
        PUL.run_to_log(java + [native_probe, "configure-foreign", origin, addresses[1], addresses[3],
                              str(expected), str(cell)], foreign / "isolate.log")
        PUL.run_to_log(java + ["com.nereusstream.pulsar.broker.M6PulsarObjectWalProvision", oxia,
                              str(foreign_cell), str(ROOT)], foreign / "provision.log")
        foreign_broker = foreign / "broker.conf"
        PUL.broker_config(template, foreign_broker, {**base, "advertisedAddress": "localhost",
                                                    "webServicePort": ports[6], "brokerServicePort": ports[7],
                                                    "nereusPulsarCellConfig": str(foreign_cell)})
        _, stop_foreign = start_broker(stack, java, foreign_broker, foreign / "broker.log", addresses[6])
        PUL.run_to_log(java + [native_probe, "verify-foreign", origin, foreign_web, foreign_service,
                              str(expected), str(foreign_cell)], foreign / "warm-up.log")
        _, stop_successor = start_broker(stack, java, out / "broker-0.conf", out / "read-drain-successor.log", addresses[0])
        PUL.run_to_log(java + [native_probe, "held-read-unload", origin, addresses[1], addresses[3],
                              str(expected), str(cell), addresses[0], foreign_web, foreign_service],
                       out / "held-read-unload.log", timeout=100)
        SOURCE.require("PULSAR_OBJECT_WAL_HELD_RANGE_CANCEL_AND_UNLOAD_PASS" in
                       (out / "held-read-unload.log").read_text(), "held read or client cancellation released the Cell")
        PUL.run_to_log(java + [native_probe, "live-physical-rollover", origin, addresses[0], addresses[2],
                              str(expected), str(cell)], out / "physical-run-rollover.log", timeout=240)
        SOURCE.require("PULSAR_OBJECT_WAL_LIVE_PHYSICAL_ROLLOVER_PASS 1024" in
                       (out / "physical-run-rollover.log").read_text(), "no native rollover across the full physical Run")
        PUL.run_to_log(java + [native_probe, "held-write-unload", origin, addresses[0], addresses[2],
                              str(expected), str(cell), addresses[1], foreign_web, foreign_service],
                       out / "held-write-unload.log", timeout=110)
        for kind, marker in (("read", "HELD_RANGE"), ("write", "HELD_WRITE")):
            drain = (out / f"held-{kind}-unload.log").read_text()
            SOURCE.require(f"PULSAR_OBJECT_WAL_{marker}_CANCEL_AND_UNLOAD_PASS" in drain
                           and "PULSAR_OBJECT_WAL_INDEPENDENT_FOREIGN_CELL_PASS" in drain,
                           f"native {kind} drain or foreign Cell made no terminal observation")
        PUL.run_to_log(java + [native_probe, "held-read-shutdown", origin, addresses[1], addresses[3],
                              str(expected), str(cell), str(peer.pid), foreign_web, foreign_service],
                       out / "held-read-shutdown.log", timeout=100)
        shutdown = (out / "held-read-shutdown.log").read_text()
        peer.wait(35)
        SOURCE.require("PULSAR_OBJECT_WAL_HELD_RANGE_CANCEL_AND_SHUTDOWN_PASS" in shutdown
                       and "PULSAR_OBJECT_WAL_INDEPENDENT_FOREIGN_CELL_PASS" in shutdown,
                       "native shutdown did not retain and then drain the original Object read")
        stop_peer(True)
        stop_successor(True)
        stop_foreign(True)
        _, stop_after_drain = start_broker(stack, java, out / "broker-0.conf", out / "restarted-after-drain.log",
                                           addresses[0])
        PUL.run_to_log(java + args + ["restart"], out / "cold-after-drain.log")
        SOURCE.require("PULSAR_NATIVE_BK_RESTART_PASS" in (out / "cold-after-drain.log").read_text(),
                       "cold owner did not read exact positions and bytes after held I/O drained")
        stop_after_drain(True)
        negative = out / "unqualified.conf"
        PUL.broker_config(template, negative, {**base, "webServicePort": ports[4], "brokerServicePort": ports[5],
                                               "metadataStoreUrl": f"oxia://{oxia}"})
        start_broker(stack, java, negative, out / "unqualified.log", addresses[4],
                     "requires native Oxia ownership with MetadataStore ELM")
        wrong_receipt = out / "wrong-range64-receipt.bin"
        raw_receipt = receipt.read_bytes()
        wrong_receipt.write_bytes(raw_receipt[:-1] + bytes([raw_receipt[-1] ^ 1]))
        for name, override, reason in (
            ("foreign-namespace", "objectWalBookKeeperLedgerRoot=/ledgers",
             "compatibility namespace must exclude stock BookKeeper writers"),
            ("wrong-allocator-artifact", "objectWalMetadataSpiJarSha256=" + "1" * 64,
             "actual packaged domain/SPI/Oxia artifacts"),
            ("wrong-range64-receipt", f"objectWalRange64Receipt={wrong_receipt}",
             "allocator selection is not the exact M3 Final NARS5"),
        ):
            config = out / f"{name}.properties"
            config.write_text(cell.read_text() + "\n" + override + "\n")
            broker = out / f"{name}.conf"
            PUL.broker_config(template, broker, {**base, "webServicePort": ports[4], "brokerServicePort": ports[5],
                                                  "nereusPulsarCellConfig": str(config)})
            start_broker(stack, java, broker, out / f"{name}.log", addresses[4], reason)
        for path in [receipt, cell, credentials, token, *out.glob("*.conf"), out / "object-provider-profile.bin",
                     *out.glob("wrong-*.properties"), out / "foreign-namespace.properties", wrong_receipt,
                     foreign_cell, foreign_broker, foreign / "object-provider-profile.bin"]:
            launched[path.relative_to(out).as_posix()] = PUL.BK.M6.file_identity(path)

    SOURCE.require(sources == {"nereus": PUL.source_checkout(ROOT), "pulsar": PUL.source_checkout(pulsar)},
                   "native Object WAL source changed during execution")
    SOURCE.require(runtime == [PUL.path_identity(path) for path in ordered]
                   and source_inputs == [PUL.path_identity(path) for path in input_names]
                   and launch_tools == [PUL.path_identity(path) for path in executables],
                   "native Object WAL inputs changed during execution")
    outputs = {path.relative_to(out).as_posix(): PUL.BK.M6.file_identity(path)
               for path in sorted([*out.iterdir(), *foreign.iterdir()])
               if path.is_file() and path.relative_to(out).as_posix() not in launched}
    (out / "process-result.json").write_bytes(SOURCE.INPUTS.canonical({
        "schema": "NEREUS_V2_M6_PULSAR_OBJECT_WAL_FOCUSED_PROCESS_V1", "promotionEligible": False, "status": "PASS",
        "profile": "OBJECT_WAL", "sources": sources, "sourceInputs": source_inputs, "runtime": runtime,
        "historicalAllocatorSelection": {"wrapperSha256": SELECTION_WRAPPER_SHA, "selectionSha256": SELECTION_SHA},
        "launchTools": launch_tools,
        "generatedLaunchInputs": launched, "compiledProbe": PUL.path_identity(compiled), "outputs": outputs,
        "checks": ["real-r1-range64", "official-client", "rollover", "individual-and-cumulative-ack", "seek-reset-backlog",
                   "planned-transfer", "cold-read", "crash-takeover", "original-unknown-key-version",
                   "no-native-ledger-id-reuse", "held-read-cancellation-and-unload", "held-write-cancellation-and-unload",
                   "held-read-cancellation-and-shutdown", "cold-read-after-shutdown-drain",
                   "independent-foreign-cell", "live-physical-run-rollover",
                   "unqualified-ownership-rejected", "foreign-namespace-rejected", "wrong-artifact-rejected",
                   "wrong-range64-receipt-rejected"],
    }))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pulsar-root", type=Path, required=True)
    parser.add_argument("--java-home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        run(args.pulsar_root.resolve(strict=True), args.java_home.resolve(strict=True), args.output.resolve())
    except (OSError, ValueError, subprocess.SubprocessError, SOURCE.InputError) as error:
        parser.exit(1, f"Pulsar Object WAL process check: {error}\n")


if __name__ == "__main__":
    main()
