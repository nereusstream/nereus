
#!/usr/bin/env python3
"""Check one source-bound native Pulsar verified BK replacement (focused, no scenario promotion)."""

import argparse
import contextlib
import hashlib
import importlib.util
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "config/v2/m6/pulsar"
def load(name, script):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

r = load("pulsar_m6_delete_process", "run-v2-m6-pulsar-bk-process-check.py")
proxy = load("pulsar_m6_delete_minio", "v2-m6-kafka-object-fault-proxy.py")
zk_fault = load("pulsar_m6_delete_zk", "v2-m6-pulsar-bk-delete-fault-proxy.py")
vault = load("pulsar_m6_delete_vault", "run-v2-m6-kafka-object-wal-check.py")
bk = r.BK
def run(pulsar, java_home, out):
    root, work = ROOT, CONFIG
    sources = {"nereus": r.source_checkout(root), "pulsar": r.source_checkout(pulsar)}
    r.SOURCE.require(not out.is_relative_to(root) and not out.is_relative_to(pulsar),
                     "evidence must be outside both source trees")
    out.mkdir()
    ordered = r.runtime_paths(pulsar, out, java_home, True)
    runtime_inputs = [r.path_identity(path) for path in ordered]
    actual_cp = out / "qualified-broker.paths"
    actual_cp.write_text("".join(str(path) + "\n" for path in ordered))
    out_cp = out / "probe-classes"
    out_cp.mkdir()
    cp = os.pathsep.join(map(str, ordered + [out_cp]))
    java = [str((java_home / "bin/java").resolve(strict=True)), "-Xmx512m", "-XX:MaxDirectMemorySize=256m", "-XX:ActiveProcessorCount=4",
            "--add-exports=java.management/sun.management=ALL-UNNAMED",
            "-Dlog4j.configurationFile=" + str(r.CONFIG / "console-log.xml"),
            f"-Dpulsar.metadatastore.providers={r.PROVIDER}", "-cp", cp]
    r.run_to_log([str((java_home / "bin/javac").resolve(strict=True)), "-proc:none", "-cp", cp, "-d", str(out_cp),
                  str(work / "M6PulsarBookKeeperReplacementProbe.java"), str(work / "M6PulsarOffloadPublicationFault.java"),
                  str(work / "M6PulsarBookKeeperDeleteHoldProbe.java"),
                  str(r.CONFIG / "M6PulsarCellProvision.java")], out / "javac.log")
    image = "sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253"
    bk.observe_image(image, image, ("linux", "arm64"))
    props = (r.CONFIG / "bookkeeper-cell.properties").read_text()
    provider_scope = re.search(r"(?m)^bkProviderScope=([0-9a-f]+)$", props)[1]
    broker_jar, = [p for p in ordered if re.fullmatch(r"bookkeeper-server-[0-9].*\.jar", p.name)]
    direct_bk_uri = "zk://127.0.0.1:2181/ledgers"
    with bk.bookkeeper_oxia(out) as oxia, contextlib.ExitStack() as stack:
        zk_address, delete_fault = stack.enter_context(zk_fault.delete_fault_proxy())
        bk_uri = "zk://" + zk_address + "/ledgers"
        minio = bk.output("docker", "run", "--detach", "--rm", "--pull", "never", "--env", "MINIO_ROOT_USER=nereus-m5-drain",
                          "--env", "MINIO_ROOT_PASSWORD=nereus-m5-drain-secret", image, "server", "/data")
        stack.callback(lambda: subprocess.run(["docker", "stop", minio], check=True, stdout=subprocess.DEVNULL))
        if bk.output("docker", "inspect", "--format", "{{.Image}}", minio) != image:
            raise RuntimeError("Actual MinIO differs from pinned image")
        endpoint = stack.enter_context(proxy.minio_loopback(minio))
        for attempt in range(20):
            try:
                with urllib.request.urlopen(endpoint + "/minio/health/live", timeout=2) as response:
                    if response.status != 200:
                        raise RuntimeError("MinIO health check did not return success")
                    break
            except OSError:
                if attempt == 19:
                    raise
                time.sleep(1)
        kms, token = stack.enter_context(vault.vault(out))
        os.environ.update(AWS_ACCESS_KEY_ID="nereus-m5-drain", AWS_SECRET_ACCESS_KEY="nereus-m5-drain-secret",
                          NEREUS_M6_PULSAR_TEST_VAULT_TOKEN=token.read_text().strip(), JAVA_HOME=str(java_home))
        runtime = out / "actual-runtime.properties"
        start = time.time_ns()
        names = ["v2M5NativeBkDeleteSdkDrainRealTest", "v2M5S3ProviderDrainRealTest"]
        gradle = [str(root / "gradlew"), f"-PpulsarCheckout={pulsar}", "--no-configuration-cache",
                  "-I", str(work / "qualify-broker-delete-drain.init.gradle"),
                  f"-Dm6.native.qualifiedBrokerClasspath={actual_cp}",
                  f"-Dm6.native.qualifiedBrokerProviderScope={provider_scope}",
                  f"-Pv2M5NativeOffloadEndpoint={endpoint}", f"-Pv2M5S3DrainEndpoint={endpoint}",
                  f"-Pv2M5NativeOffloadZk={zk_address}", f"-Pv2M5NativeOffloadOxia={oxia}",
                  f"-Pv2M5NativeM4RuntimeOutput={runtime}"] + [":nereus-pulsar-offload:" + x for x in names]
        with (out / "m4-current.log").open("xb") as log:
            subprocess.run(gradle, cwd=root, stdout=log, stderr=subprocess.STDOUT, check=True)
        suites = []
        for task in names:
            results = list((root / "nereus-pulsar-offload/build/test-results" / task).glob("TEST-*.xml"))
            if len(results) != 1 or results[0].stat().st_mtime_ns < start:
                raise RuntimeError("Missing fresh exact native M4 test suite")
            report = out / results[0].name
            report.write_bytes(results[0].read_bytes())
            suite = ET.parse(report).getroot()
            if [suite.get(field) for field in ("tests", "failures", "errors", "skipped")] != ["5", "0", "0", "0"]:
                raise RuntimeError("Native M4 drain suite did not pass all cases")
            suites.append(report)
        drain_source = root / "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/PulsarNativeM4DrainAdmissionV1.java"
        suffix = re.findall(r'"/(\w+\.java)"', drain_source.read_text())
        selected = []
        for name in suffix:
            repos = [root, pulsar]
            paths = [repo / file for repo in repos for file in r.SOURCE.git(repo, "ls-files", "*"+name).decode().splitlines()
                     if Path(file).name == name]
            if len(paths) != 1:
                raise RuntimeError(f"Native M4 drain source not unique {name}: {paths}")
            selected.extend(paths)
        capsule = out / "current-broker-m4.properties"
        props_m4 = ["schema=NEREUS_NATIVE_PULSAR_M4_SDK_DRAIN_V1", "authority=ACTUAL_NATIVE_PULSAR_BK_OBJECT_OXIA",
                    "expiryAuthority=false", "wholeM5Authority=false", "physicalDeleteAuthority=false",
                    "bookKeeperImageId=sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605",
                    "minioImageId=" + image,
                    "oxiaImageId=sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da",
                    f"source.count={len(selected)}", f"suite.count={len(suites)}"]
        for kind, paths in (("source", sorted(selected)), ("suite", sorted(suites))):
            for index, path in enumerate(paths):
                props_m4.extend((f"{kind}.{index}.path={path}", f"{kind}.{index}.sha256={hashlib.sha256(path.read_bytes()).hexdigest()}"))
        capsule.write_text("\n".join(props_m4) + "\n" + runtime.read_text())
        digest = hashlib.sha256(capsule.read_bytes()).hexdigest()
        ports = r.distinct_ports(2)
        web, binary = [f"127.0.0.1:{port}" for port in ports]
        origin = f"oxia-nereus://{oxia}"
        r.run_to_log(java + ["org.apache.pulsar.PulsarClusterMetadataSetup", "--cluster", "m6",
                            "--metadata-store", origin, "--configuration-metadata-store", origin,
                            "--existing-bk-metadata-service-uri", bk_uri, "--web-service-url", f"http://{web}",
                            "--broker-service-url", f"pulsar://{binary}", "--default-namespace-bundle-number", "1"], out / "init.log")
        cell = out / "cell.properties"
        cell.write_text(props + f"\nshareOwners=6\nreadMaximumEntries=16\nbkClientJar={hashlib.sha256(broker_jar.read_bytes()).hexdigest()}\n"
                        f"bkMetadataUri={bk_uri}\ns3KeyPrefix=cells/m5-native\nbookKeeperRetention=DELETE_AFTER_VERIFIED\n"
                        f"bookKeeperGcProviderMinimumMillis=0\nbookKeeperGcAuditMinimumMillis=1\nbookKeeperGcGraceMillis=25\n"
                        f"bookKeeperReadDrainAdmission={capsule}\nbookKeeperReadDrainAdmissionSha256={digest}\n")
        r.run_to_log(java + ["com.nereusstream.pulsar.broker.M6PulsarCellProvision", oxia, str(cell), str(root),
                            endpoint, kms, endpoint], out / "provision.log")
        conf = out / "broker.conf"
        template = pulsar / "conf/broker.conf"
        r.broker_config(template, conf, {"clusterName": "m6", "metadataStoreUrl": origin, "configurationMetadataStoreUrl": origin,
                         "bookkeeperMetadataServiceUri": bk_uri, "bindAddress": "127.0.0.1", "advertisedAddress": "127.0.0.1",
                         "defaultNumberOfNamespaceBundles": "1", "webServicePort": ports[0], "brokerServicePort": ports[1],
                         "loadManagerClassName": "org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl",
                         "loadManagerServiceUnitStateTableViewClassName":
                             "org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl",
                         "loadBalancerLoadSheddingStrategy":
                             "org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder",
                         "managedLedgerStorageClassName": r.STORAGE, "nereusPulsarCellConfig": str(cell),
                         "managedLedgerDefaultEnsembleSize": "3", "managedLedgerDefaultWriteQuorum": "3",
                         "managedLedgerDefaultAckQuorum": "2", "managedLedgerMaxEntriesPerLedger": "2",
                         "managedLedgerMinLedgerRolloverTimeMinutes": "0", "loadBalancerSheddingEnabled": "false",
                         "loadBalancerAutoBundleSplitEnabled": "false", "loadBalancerMultiPhaseBundleUnload": "false",
                         "numIOThreads": "4", "numExecutorThreadPoolSize": "4"})
        fault = out / "publication-fault"
        fault.mkdir()
        original_conf = out / "original-broker.conf"
        original_conf.write_text(conf.read_text().replace(origin, "oxia-nereus-m6-offload-fault://" + oxia))
        fault_java = [value.replace(f"pulsar.metadatastore.providers={r.PROVIDER}",
                                    f"pulsar.metadatastore.providers={r.PROVIDER},M6PulsarOffloadPublicationFault")
                      for value in java]
        fault_java.insert(1, f"-Dnereus.m6.publishFaultDirectory={fault}")
        stop = r.start_broker(stack, fault_java, original_conf, out / "broker.log", web)
        expected = out / "expected.properties"
        r.run_to_log(java + ["M6PulsarBookKeeperReplacementProbe", "initial", origin, web, binary,
                            str(expected), bk_uri, str(fault), str(cell)], out / "initial-client.log")
        original = re.search(r"(?m)^ledger=(\d+)$", expected.read_text())[1]
        operator = java + ["com.nereusstream.pulsar.broker.PulsarM6BookKeeperReplacement", str(cell), str(original_conf),
                           "persistent://public/m6-native/handoff", original]
        with (out / "live-owner-rejection.log").open("xb") as log:
            live = subprocess.run(operator, stdout=log, stderr=subprocess.STDOUT, timeout=30)
        r.SOURCE.require(live.returncode != 0
                         and "Native Cell allocator has no confirmed durable drain"
                         in (out / "live-owner-rejection.log").read_text(),
                         "replacement did not reject the serving Broker's active Cell")
        stop(strict=True)
        topic = "persistent://public/m6-native/handoff"
        hold_probe = java + ["com.nereusstream.pulsar.broker.M6PulsarBookKeeperDeleteHoldProbe", str(cell),
                             direct_bk_uri, original, topic]
        delete_fault.arm(int(original))
        with (out / "operator.log").open("xb") as log:
            pending = subprocess.Popen(operator, stdout=log, stderr=subprocess.STDOUT)
            try:
                r.SOURCE.require(delete_fault.held.wait(90), "real original BookKeeper deletion did not reach its fault")
                r.run_to_log(hold_probe + ["held"], out / "physical-delete-held.log", timeout=35)
                r.SOURCE.require(pending.poll() is None, "maintenance exited with its original delete unresolved")
                delete_fault.deliver()
                r.SOURCE.require(pending.wait(90) == 0 and delete_fault.matches == 1,
                                 "original BookKeeper deletion did not reconcile without redispatch")
            finally:
                if pending.poll() is None:
                    pending.terminate()
                    try:
                        pending.wait(10)
                    except subprocess.TimeoutExpired:
                        pending.kill()
                        pending.wait(10)
        r.run_to_log(hold_probe + ["settled"], out / "physical-delete-settled.log", timeout=20)
        (out / "physical-delete-transport.json").write_bytes(r.SOURCE.INPUTS.canonical(
            {"ledger": int(original), "exactRequests": delete_fault.matches,
             "originalResponseSuppressed": True, "actualResponseCode": delete_fault.response}))
        stop = r.start_broker(stack, java, conf, out / "restarted.log", web)
        r.run_to_log(java + ["M6PulsarBookKeeperReplacementProbe", "cold", origin, web, binary, str(expected), bk_uri], out / "cold-client.log")
        stop(strict=True)
        r.SOURCE.require("PULSAR_M6_DELETE_ENROLLED_OUTPUT " + original in (out / "initial-client.log").read_text()
                         and "PULSAR_M6_NATIVE_PUBLICATION_UNKNOWN_SETTLED " + original in (out / "initial-client.log").read_text()
                         and " and INTENT for ledger " + original in (out / "operator.log").read_text()
                         and "PULSAR_M6_NATIVE_DELETE_UNKNOWN_HELD " + original
                         in (out / "physical-delete-held.log").read_text()
                         and "PULSAR_M6_NATIVE_DELETE_UNKNOWN_SETTLED " + original
                         in (out / "physical-delete-settled.log").read_text()
                         and "Native BK replacement reached exact RELEASED and DONE for ledger " + original
                         in (out / "operator.log").read_text()
                         and "PULSAR_M6_DELETE_COLD_BACKLOG_PASS " + original in (out / "cold-client.log").read_text(),
                         "native replacement did not reach its complete real-process assertions")
    r.SOURCE.require(sources == {"nereus": r.source_checkout(root), "pulsar": r.source_checkout(pulsar)}
                     and runtime_inputs == [r.path_identity(path) for path in ordered],
                     "source or actual runtime changed during replacement")
    stage_inputs = {"native-resolved.paths", "sdk-resolved.paths", "qualified-broker.paths",
                    "current-broker-m4.properties", "cell.properties", "broker.conf", "original-broker.conf"}
    (out / "process-result.json").write_bytes(r.SOURCE.INPUTS.canonical({
        "schema": "NEREUS_V2_M6_PULSAR_BK_REPLACEMENT_FOCUSED_PROCESS_V1",
        "promotionEligible": False, "status": "PASS", "sources": sources, "runtime": runtime_inputs,
        "launchTools": [r.path_identity((java_home / "bin" / tool).resolve(strict=True)) for tool in ("java", "javac")],
        "compiledProbe": r.path_identity(out_cp),
        "fixtureInputs": [r.path_identity(path) for path in (
            Path(__file__).resolve(), work / "M6PulsarCellProvision.java", work / "bookkeeper-cell.properties",
            work / "M6PulsarBookKeeperReplacementProbe.java", work / "M6PulsarOffloadPublicationFault.java",
            work / "M6PulsarBookKeeperDeleteHoldProbe.java",
            work / "qualify-broker-delete-drain.init.gradle",
            work / "capture-runtime.init.gradle", work / "console-log.xml", pulsar / "conf/broker.conf",
            root / "scripts/run-v2-m6-pulsar-bk-process-check.py",
            root / "scripts/run-v2-m6-kafka-bk-process-check.py",
            root / "scripts/run-v2-m6-kafka-object-wal-check.py",
            root / "scripts/v2-m6-kafka-object-fault-proxy.py", root / "scripts/v2-m6-pulsar-bk-delete-fault-proxy.py",
            bk.COMPOSE,
        )],
        "generatedLaunchInputs": {name: bk.M6.file_identity(out / name) for name in sorted(stage_inputs)},
        "outputs": {path.name: bk.M6.file_identity(path) for path in sorted(out.iterdir())
                    if path.is_file() and path.name not in stage_inputs},
        "publicationFault": r.path_identity(fault),
        "ledger": int(original),
        "checks": ["current-native-m4-drain", "enrolled-native-offload", "native-publication-unknown-settled",
                   "active-cell-rejected",
                   "actual-fallback-release", "native-bookkeeper-intent-and-done",
                   "original-physical-delete-unknown-settled", "physical-absence",
                   "cold-official-client-backlog-and-positions"],
    }))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pulsar-root", type=Path, required=True)
    parser.add_argument("--java-home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        run(args.pulsar_root.resolve(strict=True), args.java_home.resolve(strict=True), args.output.resolve())
    except (OSError, ValueError, subprocess.SubprocessError, bk.M6.InputError) as failure:
        parser.exit(1, f"Native Pulsar BookKeeper replacement check: {failure}\n")


if __name__ == "__main__":
    main()
