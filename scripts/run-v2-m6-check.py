#!/usr/bin/env python3
"""Execute and verify all six native process profiles on one exact M6 source tuple."""

from __future__ import annotations

import argparse
import importlib.util
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parent.parent
SUITE_PLAN = Path("config/v2/m6/acceptance-suites.json")
CASE = re.compile(r":[\w:-]+\|[\w.$]+\|[^\x00|]+")
CHECK = re.compile(r"[a-z][a-z0-9-]+")
KAFKA_PROCESSES = {
    "kafka-bookkeeper-wal-only": ("bk-process", "FOCUSED_KAFKA_BK_PROCESS_CHECK", "process-result.json"),
    "kafka-bookkeeper-wal-async-object": ("async-object", "FOCUSED_KAFKA_ASYNC_OBJECT_CHECK", "object-fixture-result.json"),
    "kafka-object-wal": ("object-wal", "FOCUSED_KAFKA_OBJECT_WAL_CHECK", "object-wal-fixture-result.json"),
    "kafka-bk-recovery": ("bk-recovery", "FOCUSED_KAFKA_BK_RECOVERY_CHECK", "bk-recovery-result.json"),
}
KAFKA_BK_RECOVERY_TASK = "nereusBookKeeperM4AdmissionRealTest"
KAFKA_BK_RECOVERY_CASE_PREFIX = f":core:{KAFKA_BK_RECOVERY_TASK}|"
PULSAR_PROCESSES = {
    "pulsar-bookkeeper-wal-only": {
        "lifecycle": ("pulsar-bk-process", "BK", "BOOKKEEPER_WAL_ONLY", "--lifecycle"),
    },
    "pulsar-bookkeeper-wal-async-object": {
        "lifecycle": ("pulsar-bk-process", "BK", "BOOKKEEPER_WAL_ASYNC_OBJECT", "--lifecycle", "--async-object"),
        "replacement": ("pulsar-bookkeeper-replacement", "BK_REPLACEMENT", None),
    },
    "pulsar-object-wal": {
        "lifecycle": ("pulsar-object-wal-process", "OBJECT_WAL", "OBJECT_WAL"),
    },
}


def module(root, name):
    spec = importlib.util.spec_from_file_location(name, root / "scripts" / f"{name}.py")
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


SOURCE = module(ROOT, "v2-m6-execution-inputs")
COVERAGE = module(ROOT, "v2-m6-coverage")
BUILD_INPUTS = module(ROOT, "v2-m6-build-inputs")
JUNIT = module(ROOT, "m5-evidence-junit")


def read(path):
    return SOURCE.INPUTS.load(SOURCE.INPUTS.regular(path, SOURCE.INPUTS.MAX_MANIFEST_BYTES))


def write(path, value):
    with path.open("xb") as stream:
        stream.write(SOURCE.INPUTS.canonical(value))


def require_keys(value, expected, label):
    SOURCE.require(isinstance(value, dict) and set(value) == set(expected), f"unexpected {label} fields")


def plan(root):
    COVERAGE.registry(root)
    path = root / SUITE_PLAN
    SOURCE.require(path.is_file(), "M6 acceptance suites are missing from the candidate source")
    value = read(path)
    require_keys(value, ("schema", "profiles"), "suite plan")
    SOURCE.require(value["schema"] == "NEREUS_V2_M6_SUITE_PLAN_V1", "unsupported M6 suite plan")
    profiles = value["profiles"]
    SOURCE.require(isinstance(profiles, dict), "missing M6 native profile plans")
    claims = {}
    for profile, entry in profiles.items():
        if profile in PULSAR_PROCESSES:
            require_keys(entry, ("processes",), f"{profile} plan")
            require_keys(entry["processes"], PULSAR_PROCESSES[profile], f"{profile} process inventory")
            cases = [validate_assignment(item, CHECK, profile) for item in entry["processes"].values()]
        else:
            require_keys(entry, ("cases",), f"{profile} plan")
            cases = [validate_assignment(entry["cases"], CASE, profile)]
            if profile == "kafka-bookkeeper-wal-only":
                SOURCE.require({key.startswith(KAFKA_BK_RECOVERY_CASE_PREFIX) for key in entry["cases"]} == {True, False},
                               "Kafka BK plan requires both production process and qualified recovery cases")
        claims[profile] = sorted(set().union(*cases))
    COVERAGE.validate_profiles(root, claims)
    scripts = {f"kafka-{script}" for script, _, _ in KAFKA_PROCESSES.values()}
    scripts.update(spec[0] for processes in PULSAR_PROCESSES.values() for spec in processes.values())
    for script in scripts:
        SOURCE.require((root / f"scripts/run-v2-m6-{script}-check.py").is_file(),
                       f"missing committed native runner: {script}")
    return profiles


def validate_assignment(cases, pattern, profile):
    SOURCE.require(isinstance(cases, dict) and cases, f"missing M6 native cases for {profile}")
    SOURCE.require(all(pattern.fullmatch(key) and isinstance(rows, list) and rows
                       and all(isinstance(row, str) for row in rows)
                       and len(rows) == len(set(rows)) for key, rows in cases.items()),
                   f"invalid testcase assignment for {profile}")
    return {row for rows in cases.values() for row in rows}


def run(command, cwd, log_path):
    if Path(command[0]).name == "gradlew":
        command = SOURCE.gradle_command(command, log_path)
    with log_path.open("xb") as log:
        subprocess.run(command, cwd=cwd, check=True, stdout=log, stderr=subprocess.STDOUT)


def require_runtime_tools(runtime):
    declared = {path.resolve(strict=True) for path in runtime.values()}
    tools = {"python3": sys.executable, **{name: shutil.which(name) for name in ("bash", "git", "rg", "docker")}}
    java_home = os.environ.get("JAVA_HOME")
    for name in ("java", "javac"):
        tools[name] = str(Path(java_home) / "bin" / name) if java_home else shutil.which(name)
    for name, executable in tools.items():
        SOURCE.require(executable and Path(executable).resolve(strict=True) in declared,
                       f"actual M6 tool is missing from runtime inputs: {name}")


def current_attachment(output, started, name):
    path = output / SOURCE.INPUTS.relative(name)
    SOURCE.require(path.resolve(strict=True) == path and path.stat().st_mtime_ns >= started,
                   f"stale or redirected native attachment: {path}")
    return path


def verify_attachment(output, started, name, identity):
    path = current_attachment(output, started, name)
    SOURCE.require(SOURCE.file_identity(path) == identity, f"native process attachment differs: {path}")
    return path


def verify_attachment_map(output, started, files):
    SOURCE.require(isinstance(files, dict) and files, f"missing native process attachments: {output}")
    for name, identity in files.items():
        verify_attachment(output, started, name, identity)


def verify_runtime_file(path, identity, roots, output, frozen, published):
    SOURCE.require(path.is_absolute(), f"non-absolute native runtime input: {path}")
    actual = SOURCE.file_identity(path)
    SOURCE.require(all(actual.get(key) == value for key, value in identity.items()),
                   f"native runtime input bytes changed: {path}")
    if path.is_relative_to(output):
        return
    for repo, root in roots.items():
        if not path.is_relative_to(root):
            continue
        relative = path.relative_to(root)
        tracked = frozen["sourceInputs"][repo].get(relative.as_posix())
        if repo == "nereus" and tracked is None:
            tracked = frozen["historicalInputs"]["files"].get(relative.as_posix())
        SOURCE.require(tracked == actual or tracked is None and "build" in relative.parts[:-1],
                       f"native input is outside tracked source and generated build outputs: {path}")
        return
    if published.get(path.name) == actual:
        return
    SOURCE.require(any(item["path"] == str(path) and item["sha256"] == actual["sha256"]
                       and item["bytes"] == actual["bytes"] and item["mode"] == actual["mode"]
                       for item in frozen["runtimeInputs"].values()),
                   f"actual external native runtime file was not frozen: {path}")


def verify_pulsar_runtime(output, roots, frozen):
    result = read(output / "process-result.json")
    groups = [result["runtime"], result["launchTools"], [result["compiledProbe"]],
              result.get("sourceInputs", result.get("fixtureInputs", []))]
    if "publicationFault" in result:
        groups.append([result["publicationFault"]])
    for entry in (entry for group in groups for entry in group):
        path = Path(entry["path"])
        if "files" in entry:
            SOURCE.require(path.resolve(strict=True) == path, f"redirected Pulsar runtime directory: {path}")
            rows = entry["files"]
            actual = {file.relative_to(path).as_posix() for file in path.rglob("*") if file.is_file()}
            SOURCE.require(actual == {row["path"] for row in rows}, f"Pulsar runtime directory changed: {path}")
            for row in rows:
                verify_runtime_file(path / SOURCE.INPUTS.relative(row["path"]),
                                    {key: val for key, val in row.items() if key != "path"}, roots, output, frozen, {})
        else:
            verify_runtime_file(path, {key: val for key, val in entry.items() if key != "path"},
                                roots, output, frozen, {})


def kafka_test_runs(result, output):
    if result["kind"] == "FOCUSED_KAFKA_BK_RECOVERY_CHECK":
        executions = result["executions"]
        require_keys(executions, ("internal-compaction", "recovery"), "native BK recovery executions")
        return [(output / name, KAFKA_BK_RECOVERY_TASK, test)
                for name, test in executions.items()]
    nested = result["kind"] == "FOCUSED_KAFKA_OBJECT_WAL_CHECK"
    for task in result["tests"]:
        SOURCE.require(re.fullmatch(r"[a-zA-Z][\w]*", task), f"invalid Kafka evidence task: {task}")
    return [(output / task if nested else output, task, test) for task, test in result["tests"].items()]


def verify_kafka_runtime(result_file, roots, output, frozen):
    result = read(result_file)
    captures = []
    repository = output / "development-repository"
    if "kafkaRuntime" in result:
        captures.append(output / result["kafkaRuntime"]["path"])
    else:
        nested = result["kind"] == "FOCUSED_KAFKA_OBJECT_WAL_CHECK"
        if not nested:
            repository = output / "m4/development-repository"
        captures.extend(directory / test["runtime"]["name"]
                        for directory, _, test in kafka_test_runs(result, output))
    group = repository / "com/nereusstream"
    published = {}
    for jar in sorted(group.rglob("*.jar")):
        if jar.name.endswith(("-sources.jar", "-javadoc.jar")):
            continue
        module = jar.relative_to(group).parts[0]
        filename = f"{module}-0.2.0-SNAPSHOT.jar"
        SOURCE.require(filename not in published, f"ambiguous generated Kafka dependency: {filename}")
        published[filename] = SOURCE.file_identity(jar)
    for capture in captures:
        for name, identity in read(capture)["files"].items():
            path = Path(name)
            if identity == {"absent": True}:
                SOURCE.require(not path.exists() and not path.is_symlink()
                               and path.is_relative_to(roots["kafka"])
                               and "build" in path.relative_to(roots["kafka"]).parts,
                               f"Kafka runtime classpath absence changed: {path}")
            else:
                verify_runtime_file(path, identity, roots, output, frozen, published)


def verify_pulsar_process(output, started, kind, profile, expected, sources):
    result_file = current_attachment(output, started, "process-result.json")
    result = read(result_file)
    SOURCE.require(result.get("schema") == f"NEREUS_V2_M6_PULSAR_{kind}_FOCUSED_PROCESS_V1"
                   and result.get("profile") == profile and result.get("status") == "PASS"
                   and result.get("promotionEligible") is False and result.get("sources") == sources,
                   f"Pulsar process result differs from the launched profile or frozen source: {output}")
    actual = result["checks"]
    SOURCE.require(isinstance(actual, list) and len(actual) == len(expected) and set(actual) == set(expected),
                   f"Pulsar process check inventory differs: {output}")
    temporary_credentials = {"s3-credentials", "vault-token"} if kind == "OBJECT_WAL" else set()
    for group in ("outputs", "generatedLaunchInputs"):
        files = result[group]
        SOURCE.require(isinstance(files, dict) and files, f"missing Pulsar process {group}: {output}")
        ephemeral = temporary_credentials if group == "generatedLaunchInputs" else set()
        SOURCE.require(ephemeral <= files.keys(), f"missing identity for temporary Pulsar credentials: {output}")
        for name, identity in files.items():
            if name in ephemeral:
                require_keys(identity, ("mode", "bytes", "sha256"), f"temporary credential {name}")
                SOURCE.require(identity["mode"] == "100644" and type(identity["bytes"]) is int
                               and identity["bytes"] > 0 and SOURCE.INPUTS.SHA.fullmatch(identity["sha256"])
                               and not (output / name).exists() and not (output / name).is_symlink(),
                               f"invalid identity or retained temporary credential: {name}")
                continue
            verify_attachment(output, started, name, identity)
    SOURCE.require("backend.json" in result["outputs"], f"missing Pulsar backend inventory: {output}")
    if temporary_credentials:
        vault = read(current_attachment(output, started, "vault.json"))
        SOURCE.require(result["generatedLaunchInputs"]["vault-token"]["sha256"] == vault["tokenFileSha256"],
                       f"temporary Vault token identity differs: {output}")
    return {"resultSha256": SOURCE.file_identity(result_file)["sha256"], "checks": len(actual)}


def source_pair(frozen, protocol):
    return {repo: {key: frozen["sourceIdentity"][repo][key] for key in ("commit", "tree")}
            for repo in ("nereus", protocol)}


def java_home():
    return os.environ.get("JAVA_HOME") or str(Path(shutil.which("java")).resolve(strict=True).parent.parent)


def run_pulsar_profile(roots, output, profile, expected, frozen):
    sources = source_pair(frozen, "pulsar")
    output.mkdir()
    results = {}
    for name, (script, kind, storage_profile, *flags) in PULSAR_PROCESSES[profile].items():
        process_output = output / name
        started = time.time_ns()
        run([sys.executable, f"scripts/run-v2-m6-{script}-check.py", "--pulsar-root", str(roots["pulsar"]),
             "--java-home", java_home(), "--output", str(process_output), *flags],
            roots["nereus"], output / f"{name}.log")
        results[name] = verify_pulsar_process(process_output, started, kind, storage_profile, expected[name], sources)
        verify_pulsar_runtime(process_output, roots, frozen)
    return {"processResults": results, "scenarios": sorted({
        row for assignment in expected.values() for rows in assignment.values() for row in rows
    })}


def verify_kafka_result(output, started, profile, expected, sources):
    _, kind, receipt = KAFKA_PROCESSES[profile]
    result_file = current_attachment(output, started, receipt)
    result = read(result_file)
    SOURCE.require(result.get("kind") == kind and result.get("promotionEligible") is False
                   and result.get("sourceCheckout") == {repo: row["commit"] for repo, row in sources.items()}
                   and result.get("sourceTrees") == {repo: row["tree"] for repo, row in sources.items()},
                   f"Kafka profile result has the wrong execution inputs: {profile}")

    def verify_named(directory, attachment, field):
        return verify_attachment(directory, started, attachment[field],
                                 {key: value for key, value in attachment.items() if key != field})

    junit = []
    if profile == "kafka-bookkeeper-wal-only":
        verify_attachment_map(output, started, result["backendFiles"])
        verify_attachment_map(output / "kafka-process-logs", started, result["processLogs"])
        verify_named(output, result["kafkaRuntime"], "path")
        SOURCE.require(SOURCE.file_identity(current_attachment(output, started, "development-repository.json"))
                       ["sha256"] == result["developmentRepositorySha256"], "Kafka BK runtime repository inventory differs")
        junit.extend({"task": ":core:nereusBookKeeperOwnerRelocationRealTest", "path": name, **identity}
                     for name, identity in result["junit"].items())
    else:
        if profile != "kafka-object-wal":
            verify_named(output, result["m4Qualification"], "path")
        if profile == "kafka-bookkeeper-wal-async-object":
            verify_attachment_map(output, started, result["backendFiles"])
            verify_attachment_map(output / "kafka-process-logs", started, result["processLogs"])
        process_attached = profile != "kafka-object-wal"
        for directory, task, test in kafka_test_runs(result, output):
            if directory != output:
                verify_attachment_map(directory, started, test["backendFiles"])
            if test.get("processLogs"):
                verify_attachment_map(directory / "kafka-process-logs", started, test["processLogs"])
                process_attached = True
            verify_named(directory, test["runtime"], "name")
            xml = test["xml"]
            path = (directory / SOURCE.INPUTS.relative(xml["name"])).relative_to(output)
            junit.append({"task": ":core:" + task, "path": path.as_posix(),
                          "bytes": xml["bytes"], "sha256": xml["sha256"]})
        SOURCE.require(process_attached, "Kafka Object WAL did not attach its production process logs")

    suites = JUNIT.validate_suites(junit, lambda name: JUNIT.regular(current_attachment(output, started, name)))
    actual = {"|".join((case["task"], case["class"], case["name"])) for case in suites["cases"]}
    SOURCE.require(actual == set(expected), f"native testcase inventory differs for {profile}")
    return {"profileResultSha256": SOURCE.INPUTS.digest(result_file.read_bytes()), "tests": len(actual),
            "scenarios": sorted({claim for key in actual for claim in expected[key]})}


def run_kafka_process(roots, output, profile, expected, frozen):
    script, _, receipt = KAFKA_PROCESSES[profile]
    started = time.time_ns()
    run([sys.executable, f"scripts/run-v2-m6-kafka-{script}-check.py", "--kafka-root", str(roots["kafka"]),
         "--kafka-java-home", java_home(), "--output", str(output)],
        roots["nereus"], output.with_suffix(".log"))
    result = verify_kafka_result(output, started, profile, expected, source_pair(frozen, "kafka"))
    verify_kafka_runtime(output / receipt, roots, output, frozen)
    return result


def run_kafka_profile(roots, output, profile, expected, frozen):
    if profile != "kafka-bookkeeper-wal-only":
        return run_kafka_process(roots, output, profile, expected, frozen)
    output.mkdir()
    recovery = {key: rows for key, rows in expected.items() if key.startswith(KAFKA_BK_RECOVERY_CASE_PREFIX)}
    process = {key: rows for key, rows in expected.items() if key not in recovery}
    children = {
        "process": run_kafka_process(roots, output / "process", profile, process, frozen),
        "recovery": run_kafka_process(roots, output / "recovery", "kafka-bk-recovery", recovery, frozen),
    }
    return {"checks": children, "scenarios": sorted({
        scenario for child in children.values() for scenario in child["scenarios"]
    })}


def required_build_logs():
    logs = {"nereus-regressions", "pulsar-regressions"}
    bk = "kafka-bookkeeper-wal-only"
    async_object = "kafka-bookkeeper-wal-async-object"
    object_wal = "kafka-object-wal"
    for name in (f"{bk}/process", f"{async_object}/m4", f"{bk}/recovery/m4", object_wal):
        logs.add(f"{name}/nereus-publication")
    for name in (f"{async_object}/m4", f"{bk}/recovery/m4"):
        logs.add(f"{name}/m4-gradle")
    for name in (f"{bk}/process", f"{bk}/recovery/internal-compaction", f"{bk}/recovery/recovery", async_object,
                 *(f"{object_wal}/{task}" for task in ("nereusObjectWalAdapterRealTest",
                     "nereusKafkaObjectWalAuthorizationRealTest", "nereusKafkaObjectWalProcessRealTest"))):
        logs.add(f"{name}/gradle")
    for profile, processes in PULSAR_PROCESSES.items():
        for process in processes:
            logs.update(f"{profile}/{process}/{name}-resolved" for name in ("native", "sdk"))
    logs.add("pulsar-bookkeeper-wal-async-object/replacement/m4-current")
    return logs


def verify_build_inputs(output, roots, frozen):
    traces = []
    repositories = {}
    plans = set(output.rglob("*" + SOURCE.BUILD_PLAN_SUFFIX))
    SOURCE.require(plans == {output / (name + SOURCE.BUILD_PLAN_SUFFIX) for name in required_build_logs()},
                   "Gradle invocation plans differ from the mandatory native suites")
    for plan in plans:
        SOURCE.require(plan.resolve(strict=True) == plan, f"redirected Gradle invocation plan: {plan}")
        capture = read(plan)
        require_keys(capture, ("command", "startedNs"), "Gradle invocation plan")
        relative = plan.relative_to(output).as_posix().removesuffix(SOURCE.BUILD_PLAN_SUFFIX)
        trace = current_attachment(output, capture["startedNs"], relative + SOURCE.BUILD_TRACE_SUFFIX)
        SOURCE.require(f"-Dnereus.m6.buildInputCapture={trace}" in capture["command"],
                       f"Gradle invocation names a different capture: {plan}")
        traces.append((capture["startedNs"], trace))
        property_name = "-PnereusDevelopmentRepository="
        supplied = [Path(arg.removeprefix(property_name)) for arg in capture["command"]
                    if arg.startswith(property_name)]
        if supplied:
            SOURCE.require(len(supplied) == 1 and supplied[0].is_relative_to(output)
                           and supplied[0].resolve(strict=True) == supplied[0],
                           f"Gradle consumed a repository outside the source-built output: {plan}")
            repositories[trace] = supplied[0]
    inventory = set(output.rglob("*" + SOURCE.BUILD_TRACE_SUFFIX))
    SOURCE.require(traces and {trace for _, trace in traces} == inventory, "incomplete Gradle capture inventory")
    return BUILD_INPUTS.verify([trace for _, trace in sorted(traces)], frozen, roots, output,
                               SOURCE.file_identity, SOURCE.require, repositories)


def execute(args):
    roots = {"nereus": args.nereus.resolve(strict=True), "kafka": args.kafka.resolve(strict=True),
             "pulsar": args.pulsar.resolve(strict=True)}
    root = roots["nereus"]
    SOURCE.require(root != ROOT, "execute M6 in a dedicated source worktree separate from the launcher")
    SOURCE.require(SOURCE.clean_commit(root) == SOURCE.clean_commit(ROOT),
                   "launcher and actual Nereus execution worktree differ")
    profiles = plan(root)
    out = args.output.absolute()
    declaration = args.runtime_manifest.resolve(strict=True)
    runtime, hashes = SOURCE.runtime_manifest(declaration)
    require_runtime_tools(runtime)
    SOURCE.require(SOURCE.outside(out, [*roots.values(), *runtime.values()]),
                   "M6 output overlaps an execution input")

    def capture():
        snapshot = SOURCE.capture(roots, out, None, runtime, args.runtime_byte_budget)
        SOURCE.require(all(snapshot["runtimeInputs"][key]["sha256"] == expected
                           for key, expected in hashes.items()), "declared runtime dependency changed")
        return snapshot

    frozen = capture()
    out.mkdir(parents=True, exist_ok=False)
    source = out / "execution-inputs.json"
    write(source, frozen)
    os.environ[SOURCE.BUILD_EVIDENCE_ROOT] = str(out)
    run([
        "./gradlew", "--offline", "--no-configuration-cache", "--no-daemon",
        f"-PpulsarCheckout={roots['pulsar']}", "v2M0Check", "v2M1FastSourceCheck",
        "v2M6ExecutionInputsContractTest", "v2M6CoverageContractTest", "v2M4HistoricalM3DependencyCheck",
        "v2M5HistoricalM4DependencyCheck",
        *[f":{name}:{check}" for name in
          ("nereus-storage-object", "nereus-storage-bookkeeper", "nereus-kafka-bookkeeper",
           "nereus-pulsar-offload", "nereus-pulsar-broker", "nereus-metadata-spi")
          for check in ("test", "spotlessCheck", "checkstyleMain", "checkstyleTest")],
    ], root, out / "nereus-regressions.log")
    run(["./gradlew", "--offline", "--no-configuration-cache", "--no-daemon",
         ":managed-ledger:test", "--tests", "*ManagedLedgerFactoryShutdownTest", "--tests", "*ManagedLedgerErrorsTest",
         ":managed-ledger:checkstyleMain"], roots["pulsar"], out / "pulsar-regressions.log")
    qualified = {}
    for profile in COVERAGE.REQUIRED:
        output = out / profile
        if profile in PULSAR_PROCESSES:
            qualified[profile] = run_pulsar_profile(roots, output, profile, profiles[profile]["processes"], frozen)
        else:
            qualified[profile] = run_kafka_profile(roots, output, profile, profiles[profile]["cases"], frozen)
    scenarios = COVERAGE.validate_profiles(root, {key: row["scenarios"] for key, row in qualified.items()})
    build_inputs = verify_build_inputs(out, roots, frozen)
    SOURCE.require(frozen == capture() and source.read_bytes() == SOURCE.INPUTS.canonical(frozen),
                   "execution source or dependency graph changed")
    write(out / "m6-execution-result.json", {
        "schema": "NEREUS_V2_M6_EXECUTION_RESULT_V1", "inputRootSha256": frozen["inputRootSha256"],
        "profiles": qualified, "scenarios": sorted(scenarios), "buildInputs": build_inputs,
        "regressionLogs": {name: SOURCE.file_identity(out / name)["sha256"]
                           for name in ("nereus-regressions.log", "pulsar-regressions.log")},
        "promotionEligible": False,
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nereus", type=Path)
    parser.add_argument("--kafka", type=Path)
    parser.add_argument("--pulsar", type=Path)
    parser.add_argument("--runtime-manifest", type=Path)
    parser.add_argument("--runtime-byte-budget", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        plan(ROOT)
        if any(getattr(args, item) is None for item in
               ("nereus", "kafka", "pulsar", "runtime_manifest", "runtime_byte_budget", "output")):
            parser.error("all three execution checkouts, runtime manifest, budget and output are required")
        execute(args)
    except (SOURCE.InputError, SOURCE.INPUTS.ClosureError, COVERAGE.CoverageError,
            JUNIT.JUnitEvidenceError, subprocess.CalledProcessError, OSError, ValueError) as error:
        parser.exit(1, f"M6 acceptance: {error}\n")


if __name__ == "__main__":
    main()
