#!/usr/bin/env python3
"""Publish and verify the source-bound Final for the recorded NSIP-1 M6 execution."""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True


ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("m6_runner", ROOT / "scripts/run-v2-m6-check.py")
RUN = importlib.util.module_from_spec(spec)
spec.loader.exec_module(RUN)
SOURCE = RUN.SOURCE
INPUTS = SOURCE.INPUTS
require = SOURCE.require

COMMITS = {
    "nereus": RUN.COVERAGE.TESTED_NEREUS,
    "kafka": "c2f151b16726b65555d423eb1220170c19034981",
    "pulsar": "7decbf7f9fb540a4a0ec8ada3df5538ab498b0f0",
}
RECEIPT = RUN.COVERAGE.RECEIPT
RECEIPT_SHA = "5e5647c3a9404ccd1006cb0e4e73744079a44b7655074b7b4bc0bee96baec2a1"
RESULT = "m6-execution-result.json"
EXECUTION_INPUTS = "execution-inputs.json"
RESULT_SHA = "521873b2abfd8e293230cbcd1d9f56573aadff5d84ecba515a7a6b20d3813d8d"
INPUTS_SHA = "3976955fbe2fcb654bf62c3f9cb6077e524ca359ffc65c4f486f7e0a803cac79"
INPUT_ROOT = "ba6aa6b7edccaf367b55abe89ce9f3aaf88536fe1030544f6081343fe1230e33"
# The original manifest has 33,285,631 bytes. This bound applies to this M6
# execution manifest only. M0–M5 readers and the historical archive budget are unchanged.
EXECUTION_INPUTS_MAX = 32 * 1024**2

GOVERNANCE = {
    ".github/workflows/build.yml",
    "README.md",
    "docs/v2/03-object-wal.md",
    "docs/v2/06-metadata-backends-and-handoff.md",
    "docs/v2/07-protocol-integrations.md",
    "docs/v2/08-implementation-plan-and-gates.md",
    "docs/v2/09-scenario-evidence-matrix.md",
    "docs/v2/NSIP/nsip-1.md",
    "docs/v2/README.md",
    "docs/v2/architecture.md",
    "docs/v2/detailed_design/m5/m5-current-contracts.md",
    "docs/v2/detailed_design/m5/README.md",
    "docs/v2/open-questions.md",
    "docs/v2/v2-scenarios.json",
    "scripts/check-v2-documentation.sh",
    "scripts/check-v2-m5-evidence.py",
    "scripts/check-v2-m6-execution-inputs-tests.py",
    "scripts/check-v2-m6-final.py",
    "scripts/check-v2-m6-final-tests.py",
    "scripts/run-v2-m6-check.py",
    "scripts/v2-m6-coverage.py",
    "scripts/v2-m6-execution-inputs.py",
}


def verify_delivery_source(root):
    SOURCE.git(root, "merge-base", "--is-ancestor", COMMITS["nereus"], "HEAD")
    # Compare the effective committed source as well as the index and the worktree.
    for args in (("diff", "--name-only", "--no-renames", "-z", COMMITS["nereus"], "HEAD"),
                 ("diff", "--name-only", "--no-renames", "-z", "HEAD"),
                 ("ls-files", "--others", "--exclude-standard", "-z")):
        paths = {path.decode() for path in SOURCE.git(root, *args).split(b"\0") if path}
        require(all(path in GOVERNANCE or Path(path).is_relative_to(RECEIPT.parent) for path in paths),
                "delivery has changes outside the M6 evidence and governance paths")


def execution(raw):
    result_raw = INPUTS.regular(raw / RESULT)
    require(INPUTS.digest(result_raw) == RESULT_SHA, "original M6 execution result differs")
    result = INPUTS.load(result_raw)
    data = INPUTS.regular(raw / EXECUTION_INPUTS, EXECUTION_INPUTS_MAX)
    require(INPUTS.digest(data) == INPUTS_SHA, "original M6 execution inputs differ")
    frozen = json.loads(data, object_pairs_hook=INPUTS.pairs)
    declared = frozen.pop("inputRootSha256")
    require(frozen["schema"] == SOURCE.SCHEMA and declared == INPUT_ROOT
            and result["inputRootSha256"] == declared and INPUTS.digest(INPUTS.canonical(frozen)) == declared,
            "M6 input root differs")
    frozen["inputRootSha256"] = declared
    require(result["schema"] == "NEREUS_V2_M6_EXECUTION_RESULT_V1" and result["promotionEligible"] is False,
            "the original execution verdict must be retained")
    return result, frozen


def verify_sources(roots, frozen):
    identity = frozen["sourceIdentity"]
    require(set(identity) == COMMITS.keys(), "incomplete M6 source tuple")
    for repo, root in roots.items():
        expected = COMMITS[repo]
        binding = identity[repo]
        require(binding["commit"] == expected, f"M6 executed commit differs for {repo}")
        bare = SOURCE.git(root, "rev-parse", "--is-bare-repository").strip() == b"true"
        if repo != "nereus":
            head = SOURCE.revision(root, "HEAD^{commit}") if bare else SOURCE.clean_commit(root)
            require(head == expected, f"M6 {repo} checkout differs from the executed commit")
        require(SOURCE.revision(root, f"{expected}^{{tree}}") == binding["tree"],
                f"M6 executed tree differs for {repo}")
        tracked = SOURCE.tree(root, expected)
        require(tracked == {name: {key: node[key] for key in ("mode", "blob", "bytes")}
                            for name, node in binding["files"].items()},
                f"M6 executed file inventory differs for {repo}")
        if not bare:
            for name, node in frozen["sourceInputs"][repo].items():
                if repo == "nereus" and name in GOVERNANCE:
                    continue
                require(SOURCE.source_identity(root, name, tracked[name], tracked) == node,
                        f"checkout differs from the original M6 source: {repo}/{name}")
    archive = frozen["historicalInputs"]
    require(archive["archiveCommit"] == SOURCE.ARCHIVE_COMMIT
            and SOURCE.tree(roots["nereus"], SOURCE.ARCHIVE_COMMIT, SOURCE.HISTORICAL)
            == SOURCE.tree(roots["nereus"], COMMITS["nereus"], SOURCE.HISTORICAL),
            "M6 historical archive differs from its immutable input")


def original_output(raw):
    plan = RUN.read(raw / "nereus-regressions.build-plan.json")
    prefix = "-Dnereus.m6.buildInputCapture="
    names = [Path(arg.removeprefix(prefix)) for arg in plan["command"] if arg.startswith(prefix)]
    require(len(names) == 1 and names[0].is_absolute()
            and names[0].name == "nereus-regressions.build-inputs.jsonl", "missing original Gradle capture root")
    return names[0].parent


def verify_scenario_contracts(root, scenarios):
    registry = RUN.COVERAGE.REGISTRY

    def contracts(document):
        return {row["id"]: {key: value for key, value in row.items() if key not in ("status", "evidenceReceipt")}
                for row in document["scenarios"] if row["id"] in scenarios}

    baseline = INPUTS.load(SOURCE.git(root, "show", f"{COMMITS['nereus']}:{registry}"))
    require(contracts(RUN.read(root / registry)) == contracts(baseline), "M6 scenario contracts changed after execution")


def verify_native(root, raw, original, result, frozen):
    plan = RUN.plan(root)
    verified = {}
    for name, entry in plan.items():
        directory = raw / name
        if name in RUN.PULSAR_PROCESSES:
            processes = {
                process: RUN.verify_pulsar_process(directory / process, 0, kind, profile,
                                                   entry["processes"][process], RUN.source_pair(frozen, "pulsar"))
                for process, (_, kind, profile, *_) in RUN.PULSAR_PROCESSES[name].items()
            }
            scenarios = {scenario for checks in entry["processes"].values()
                         for assignments in checks.values() for scenario in assignments}
            verified[name] = {"processResults": processes, "scenarios": sorted(scenarios)}
            continue
        sources = RUN.source_pair(frozen, "kafka")
        if name == "kafka-bookkeeper-wal-only":
            children = {
                process: RUN.verify_kafka_result(directory / process, 0, profile, {
                    case: assignments for case, assignments in entry["cases"].items()
                    if case.startswith(RUN.KAFKA_BK_RECOVERY_CASE_PREFIX) == (process == "recovery")
                }, sources)
                for process, profile in (("process", name), ("recovery", "kafka-bk-recovery"))
            }
            verified[name] = {"checks": children, "scenarios": sorted({
                scenario for child in children.values() for scenario in child["scenarios"]
            })}
        else:
            verified[name] = RUN.verify_kafka_result(directory, 0, name, entry["cases"], sources,
                                                     original / name)
    scenarios = RUN.COVERAGE.validate_profiles(root, {key: row["scenarios"] for key, row in verified.items()})
    require(verified == result["profiles"] and sorted(scenarios) == result["scenarios"],
            "native outputs do not reconstruct the original M6 profile/scenario verdict")


def inventory(raw, git_nodes=None):
    require(raw.resolve(strict=True) == raw and raw.is_dir(), f"invalid M6 evidence root: {raw}")
    files = {}
    total = 0
    for path in sorted(raw.rglob("*")):
        require(not path.is_symlink(), f"redirected M6 evidence: {path}")
        if not path.is_dir():
            name = path.relative_to(raw).as_posix()
            total += path.lstat().st_size
            require(len(files) < 2000 and total <= 768 * 1024**2, "M6 output archive exceeds its bound")
            require(git_nodes is None or name in git_nodes, f"uncommitted M6 attachment: {name}")
            files[name] = SOURCE.file_identity(path, None if git_nodes is None else git_nodes[name])
    require(files and (git_nodes is None or files.keys() == git_nodes.keys()), "missing committed M6 attachments")
    return files


def verify_build_records(raw, files, original, result):
    traces = result["buildInputs"]
    suffix = SOURCE.BUILD_TRACE_SUFFIX
    require(set(traces) == {name + suffix for name in RUN.required_build_logs()}
            == {name for name in files if name.endswith(suffix)}, "M6 build input inventory differs")
    jars = set()
    for name, record in traces.items():
        require(files[name] == {key: record[key] for key in ("mode", "bytes", "sha256")},
                f"original Gradle trace differs: {name}")
        plan = RUN.read(raw / (name.removesuffix(suffix) + SOURCE.BUILD_PLAN_SUFFIX))
        require(f"-Dnereus.m6.buildInputCapture={original / name}" in plan["command"]
                and f"-Dnereus.m6.buildArchiveDirectory={original / SOURCE.BUILD_ARCHIVE_DIRECTORY}" in plan["command"],
                f"original Gradle plan differs: {name}")
        tasks, builds = 0, set()
        with (raw / name).open(encoding="utf-8") as stream:
            for line in stream:
                row = json.loads(line)
                if row["event"] == "gradle":
                    builds.add(row["build"])
                else:
                    require(row["event"] == "task", f"invalid Gradle record: {name}")
                    tasks += 1
                    if row["produced"]:
                        jars.update((value["sha256"], value["bytes"]) for key, value in row["outputs"].items()
                                    if key.endswith(".jar") and "sha256" in value)
        require(tasks == record["tasks"] and sorted(builds) == record["builds"],
                f"Gradle task or build inventory differs: {name}")
    for name, node in files.items():
        if name.startswith(SOURCE.BUILD_ARCHIVE_DIRECTORY + "/"):
            require(name == f"{SOURCE.BUILD_ARCHIVE_DIRECTORY}/{node['sha256']}.jar"
                    and (node["sha256"], node["bytes"]) in jars, f"unbound retained Gradle archive: {name}")
    for name, sha in result["regressionLogs"].items():
        require(files[name]["sha256"] == sha, f"original regression log differs: {name}")


def receipt(root, raw, roots, git_nodes=None):
    result, frozen = execution(raw)
    verify_sources(roots, frozen)
    verify_scenario_contracts(root, result["scenarios"])
    files = inventory(raw, git_nodes)
    original = original_output(raw)
    verify_native(root, raw, original, result, frozen)
    verify_build_records(raw, files, original, result)
    return {
        "schema": "NEREUS_V2_M6_FINAL_V1",
        "result": "PASS_V2_M6_FINAL",
        "inputRootSha256": INPUT_ROOT,
        "sourceTuple": {name: {key: row[key] for key in ("commit", "tree")}
                        for name, row in frozen["sourceIdentity"].items()},
        "scenarios": result["scenarios"],
        "attachments": files,
    }


def publish(root, original, roots, execution_nereus):
    verify_delivery_source(root)
    path = root / RECEIPT.parent
    require(not path.exists(), "the M6 Final already exists")
    require(SOURCE.outside(original, [*roots.values(), execution_nereus]),
            "M6 execution output overlaps a source root")
    result, frozen = execution(original)
    runtime = {name: Path(node["path"]) for name, node in frozen["runtimeInputs"].items()}
    original_roots = {**roots, "nereus": execution_nereus}
    require(SOURCE.clean_commit(execution_nereus) == COMMITS["nereus"], "expected the clean executed Nereus checkout")
    require(SOURCE.capture(original_roots, original, None, runtime, frozen["budgets"]["runtimeBytes"]) == frozen,
            "the original M6 source or runtime bytes differ")
    require(RUN.verify_build_inputs(original, original_roots, frozen) == result["buildInputs"],
            "the original M6 build input graph differs")
    for name, processes in RUN.PULSAR_PROCESSES.items():
        for process in processes:
            RUN.verify_pulsar_runtime(original / name / process, original_roots, frozen)
    for name in RUN.KAFKA_PROCESSES:
        if name == "kafka-bk-recovery":
            output = original / "kafka-bookkeeper-wal-only/recovery"
        elif name == "kafka-bookkeeper-wal-only":
            output = original / name / "process"
        else:
            output = original / name
        RUN.verify_kafka_runtime(output / RUN.KAFKA_PROCESSES[name][2], original_roots, output, frozen, original)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=path.parent) as staging:
        staged = Path(staging)
        raw = staged / "raw"
        shutil.copytree(original, raw, ignore=shutil.ignore_patterns("*.lock"), symlinks=True)
        value = receipt(root, raw, roots)
        require(INPUTS.digest(INPUTS.canonical(value)) == RECEIPT_SHA, "M6 Final differs from the reviewed archive")
        RUN.write(staged / RECEIPT.name, value)
        require(not path.exists(), "the M6 Final was published concurrently")
        staged.rename(path)
    return value


def check(root, roots):
    verify_delivery_source(root)
    path = root / RECEIPT
    package = path.parent
    nodes = SOURCE.tree(root, "HEAD", str(RECEIPT.parent))
    require(str(RECEIPT) in nodes, "M6 Final must be committed")
    receipt_identity = SOURCE.file_identity(path, nodes.pop(str(RECEIPT)))
    value = RUN.read(path)
    require(receipt_identity["sha256"] == RECEIPT_SHA == INPUTS.digest(INPUTS.canonical(value)),
            "M6 Final receipt differs from the reviewed archive")
    prefix = RECEIPT.parent / "raw"
    raw_nodes = {Path(name).relative_to(prefix).as_posix(): node for name, node in nodes.items()}
    expected = receipt(root, package / "raw", roots, raw_nodes)
    require(value == expected, "M6 Final receipt or attachment inventory differs")
    require(RUN.COVERAGE.registration(root) == set(value["scenarios"]), "M6 Final scenario registration differs")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--kafka-root", type=Path, required=True)
    parser.add_argument("--pulsar-root", type=Path, required=True)
    parser.add_argument("--publish-from", type=Path)
    parser.add_argument("--execution-nereus", type=Path)
    args = parser.parse_args()
    try:
        root = args.repo_root.resolve(strict=True)
        roots = {"nereus": root, "kafka": args.kafka_root.resolve(strict=True),
                 "pulsar": args.pulsar_root.resolve(strict=True)}
        if args.publish_from is not None:
            if args.execution_nereus is None:
                parser.error("--publish-from requires the clean --execution-nereus checkout")
            value = publish(root, args.publish_from.resolve(strict=True), roots,
                            args.execution_nereus.resolve(strict=True))
        else:
            value = check(root, roots)
        print(f"{value['result']} scenarios={len(value['scenarios'])} "
              f"sha256={INPUTS.digest(INPUTS.canonical(value))}")
    except (SOURCE.InputError, INPUTS.ClosureError, RUN.COVERAGE.CoverageError, RUN.JUNIT.JUnitEvidenceError,
            OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        parser.exit(1, f"M6 Final: {error}\n")


if __name__ == "__main__":
    main()
