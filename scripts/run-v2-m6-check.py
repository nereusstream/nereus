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
        require_keys(entry, ("cases",), f"{profile} plan")
        cases = entry["cases"]
        SOURCE.require(isinstance(cases, dict) and cases, f"missing M6 native cases for {profile}")
        SOURCE.require(all(CASE.fullmatch(key) and isinstance(rows, list) and rows
                           and len(rows) == len(set(rows)) for key, rows in cases.items()),
                       f"invalid testcase assignment for {profile}")
        claims[profile] = sorted({row for rows in cases.values() for row in rows})
    COVERAGE.validate_profiles(root, claims)
    for profile in profiles:
        SOURCE.require(driver(root, profile).is_file(), f"missing executable native driver for {profile}")
    return profiles


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


def verify_native_result(output, started, profile, expected, input_digest):
    result_file = output / "profile-result.json"
    SOURCE.require(result_file.stat().st_mtime_ns >= started, f"stale M6 result for {profile}")
    result = read(result_file)
    require_keys(result, ("schema", "profile", "inputRootSha256", "junit", "backendFacts"), "profile result")
    SOURCE.require(result["schema"] == "NEREUS_V2_M6_NATIVE_PROFILE_RESULT_V1"
                   and result["profile"] == profile and result["inputRootSha256"] == input_digest,
                   f"profile result has the wrong execution inputs: {profile}")

    def current_file(name):
        path = output / SOURCE.INPUTS.relative(name)
        SOURCE.require(path.resolve(strict=True) == path and path.stat().st_mtime_ns >= started,
                       f"stale or redirected native attachment: {path}")
        return path

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
