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
        claims[profile] = sorted(set().union(*cases))
    COVERAGE.validate_profiles(root, claims)
    for profile in profiles.keys() - PULSAR_PROCESSES.keys():
        SOURCE.require(driver(root, profile).is_file(), f"missing executable native driver for {profile}")
    return profiles


def validate_assignment(cases, pattern, profile):
    SOURCE.require(isinstance(cases, dict) and cases, f"missing M6 native cases for {profile}")
    SOURCE.require(all(pattern.fullmatch(key) and isinstance(rows, list) and rows
                       and all(isinstance(row, str) for row in rows)
                       and len(rows) == len(set(rows)) for key, rows in cases.items()),
                   f"invalid testcase assignment for {profile}")
    return {row for rows in cases.values() for row in rows}


def driver(root, profile):
    return root / "scripts" / f"run-v2-m6-{profile}-acceptance.py"


def run(command, cwd, log_path):
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
            SOURCE.require(SOURCE.file_identity(current_attachment(output, started, name)) == identity,
                           f"Pulsar process attachment differs: {name}")
    SOURCE.require("backend.json" in result["outputs"], f"missing Pulsar backend inventory: {output}")
    if temporary_credentials:
        vault = read(current_attachment(output, started, "vault.json"))
        SOURCE.require(result["generatedLaunchInputs"]["vault-token"]["sha256"] == vault["tokenFileSha256"],
                       f"temporary Vault token identity differs: {output}")
    return {"resultSha256": SOURCE.file_identity(result_file)["sha256"], "checks": len(actual)}


def run_pulsar_profile(roots, output, profile, expected, frozen):
    sources = {repo: {key: frozen["sourceIdentity"][repo][key] for key in ("commit", "tree")}
               for repo in ("nereus", "pulsar")}
    java = os.environ.get("JAVA_HOME") or str(Path(shutil.which("java")).resolve(strict=True).parent.parent)
    output.mkdir()
    results = {}
    for name, (script, kind, storage_profile, *flags) in PULSAR_PROCESSES[profile].items():
        process_output = output / name
        started = time.time_ns()
        run([sys.executable, f"scripts/run-v2-m6-{script}-check.py", "--pulsar-root", str(roots["pulsar"]),
             "--java-home", java, "--output", str(process_output), *flags], roots["nereus"], output / f"{name}.log")
        results[name] = verify_pulsar_process(process_output, started, kind, storage_profile, expected[name], sources)
    return {"processResults": results, "scenarios": sorted({
        row for assignment in expected.values() for rows in assignment.values() for row in rows
    })}


def verify_native_result(output, started, profile, expected, input_digest):
    result_file = current_attachment(output, started, "profile-result.json")
    result = read(result_file)
    require_keys(result, ("schema", "profile", "inputRootSha256", "junit", "backendFacts"), "profile result")
    SOURCE.require(result["schema"] == "NEREUS_V2_M6_NATIVE_PROFILE_RESULT_V1"
                   and result["profile"] == profile and result["inputRootSha256"] == input_digest,
                   f"profile result has the wrong execution inputs: {profile}")

    def current_file(name):
        return current_attachment(output, started, name)

    # Each source-owned driver must capture its actual process/backend inputs and outputs, and
    # attach them before returning. A raw JUnit file by itself is never a profile result.
    backend = result["backendFacts"]
    require_keys(backend, ("path", "sha256", "bytes"), "native process attachment")
    SOURCE.require(SOURCE.file_identity(current_file(backend["path"])) ==
                   {"mode": "100644", "bytes": backend["bytes"], "sha256": backend["sha256"]},
                   f"native process attachment differs for {profile}")
    suites = JUNIT.validate_suites(result["junit"], lambda name: JUNIT.regular(current_file(name)))
    actual = {"|".join((case["task"], case["class"], case["name"])) for case in suites["cases"]}
    SOURCE.require(actual == set(expected), f"native testcase inventory differs for {profile}")
    return {"profileResultSha256": SOURCE.INPUTS.digest(result_file.read_bytes()), "tests": len(actual),
            "scenarios": sorted({claim for key in actual for claim in expected[key]})}


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
    run([
        "./gradlew", "--offline", "--no-configuration-cache", "--no-daemon",
        f"-PpulsarCheckout={roots['pulsar']}", "v2M0Check", "v2M1FastSourceCheck",
        "v2M6ExecutionInputsContractTest", "v2M6CoverageContractTest", "v2M4HistoricalM3DependencyCheck",
        "v2M5HistoricalM4DependencyCheck",
        *[f":{name}:{check}" for name in
          ("nereus-storage-object", "nereus-storage-bookkeeper", "nereus-kafka-bookkeeper", "nereus-pulsar-offload")
          for check in ("test", "spotlessCheck", "checkstyleMain", "checkstyleTest")],
    ], root, out / "nereus-regressions.log")
    qualified = {}
    for profile in COVERAGE.REQUIRED:
        output = out / profile
        if profile in PULSAR_PROCESSES:
            qualified[profile] = run_pulsar_profile(roots, output, profile, profiles[profile]["processes"], frozen)
            continue
        started = time.time_ns()
        run([sys.executable, str(driver(root, profile)), "--execution-inputs", str(source),
             "--runtime-manifest", str(declaration), "--nereus", str(root),
             "--kafka", str(roots["kafka"]), "--pulsar", str(roots["pulsar"]),
             "--output", str(output)], root, out / f"{profile}.log")
        qualified[profile] = verify_native_result(
            output, started, profile, profiles[profile]["cases"], frozen["inputRootSha256"])
    scenarios = COVERAGE.validate_profiles(root, {key: row["scenarios"] for key, row in qualified.items()})
    SOURCE.require(frozen == capture() and source.read_bytes() == SOURCE.INPUTS.canonical(frozen),
                   "execution source or dependency graph changed")
    write(out / "m6-execution-result.json", {
        "schema": "NEREUS_V2_M6_EXECUTION_RESULT_V1", "inputRootSha256": frozen["inputRootSha256"],
        "profiles": qualified, "scenarios": sorted(scenarios),
        "regressionLogSha256": SOURCE.file_identity(out / "nereus-regressions.log")["sha256"],
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
