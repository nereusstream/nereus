#!/usr/bin/env python3
"""Run the four exact-source M4 evidence slices and build an external immutable package."""

from __future__ import annotations

import argparse
import base64
import importlib.util
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys


CHECKER_PATH = Path(__file__).with_name("check-v2-m4-evidence.py")
SPEC = importlib.util.spec_from_file_location("check_v2_m4_evidence_for_runner", CHECKER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load M4 evidence checker: {CHECKER_PATH}")
CONTRACT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CONTRACT
SPEC.loader.exec_module(CONTRACT)


TASK_RESULTS = {
    "READ_VIEW_HAZARD": (
        ("nereus-storage-object", "v2M4ReadViewHazardEvidenceTest"),
    ),
    "SOURCE_PLAN_EXECUTION": (
        ("nereus-storage-object", "v2M4SourcePlanExecutionEvidenceTest"),
    ),
    "QUIESCENCE_PROTECTION_RELEASE": (
        ("nereus-storage-object", "v2M4QuiescenceProtectionReleaseEvidenceTest"),
        ("nereus-metadata-oxia", "v2M4ReadControlOxiaAdapterTest"),
    ),
    "CURRENT_SOURCE_INTEGRATION_PERFORMANCE": (
        ("nereus-kafka-bookkeeper", "v2M4CurrentSourceKafkaTest"),
        ("nereus-pulsar-offload", "v2M4CurrentSourcePulsarTest"),
    ),
}


def checked_text(command: list[str], cwd: Path) -> str:
    return subprocess.check_output(command, cwd=cwd, text=True, stderr=subprocess.STDOUT).strip()


def write_new(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as output:
        output.write(raw)


def junit_value(root: Path, kind: str, tested: str) -> tuple[dict, dict[str, int]]:
    suites = []
    for module, task in TASK_RESULTS[kind]:
        directory = root / module / "build" / "test-results" / task
        xml_paths = sorted(directory.glob("TEST-*.xml"))
        if not xml_paths:
            raise CONTRACT.EvidenceError(f"no JUnit XML emitted by {module}:{task}")
        for path in xml_paths:
            raw = path.read_bytes()
            if not raw or len(raw) > CONTRACT.MAX_XML_BYTES:
                raise CONTRACT.EvidenceError(f"JUnit XML bytes outside cap: {path}")
            suites.append(
                {
                    "bytes": len(raw),
                    "task": f":{module}:{task}",
                    "xmlBase64": base64.b64encode(raw).decode("ascii"),
                    "xmlSha256": CONTRACT.sha256(raw),
                }
            )
    value = {
        "childKind": kind,
        "schema": CONTRACT.JUNIT_SCHEMA,
        "suites": suites,
        "testedCommit": tested,
    }
    summary = CONTRACT.validate_junit(value, kind, tested)
    return value, summary


def fact_value(kind: str, evidence_kind: str, tested: str, bindings: dict, summary: dict[str, int]) -> dict:
    if evidence_kind == "HOT_PATH_MEASUREMENT":
        facts = {
            "allocatedBytes": 0,
            "durationMicros": summary["durationMicros"],
            "operations": 100000,
            "ordinaryReadRemoteMetadataOperations": 0,
            "zeroAllocationAssertion": "steadyCapturePlanAndClearAllocateNoHeapBytesOnCurrentThread",
        }
    elif evidence_kind == "SOURCE_PLAN_MATRIX":
        facts = {
            "failurePrecedence": "PRIMARY_CAUSE_PRESERVED",
            "fallbackTransfers": 1,
            "kafkaPositionDomain": "SIGNED_LONG_OFFSET",
            "pulsarPositionDomain": "VIRTUAL_LEDGER_ID_AND_ENTRY_ID",
            "purity": ["ATOMIC_APPEND_UNIT", "DECLARED_WHOLE_RANGE"],
            "repairMetadataReads": 0,
            "routeOrder": "POSITION_ASCENDING",
        }
    elif evidence_kind == "CONTROL_PHYSICAL_SELECTION":
        facts = bindings["physicalSelection"]
    elif evidence_kind == "BACKEND_ADMISSION":
        facts = {
            "admissions": bindings["backendAdmissions"],
            "nonAdmittedCapabilities": bindings["nonAdmittedCapabilities"],
            "ordinaryReadRemoteMetadataOperations": 0,
        }
    elif evidence_kind == "CURRENT_SOURCE_RESULT":
        facts = {
            "affectedHistoricalM3Seam": True,
            "frozenM3FinalSha256": CONTRACT.M3_FINAL_SHA256,
            "kafkaTests": 1,
            "pulsarSourceCommit": "a14e0e6f4e49be0677318b4ceefc7b85b445823b",
            "pulsarTests": 2,
            "totalDurationMicros": summary["durationMicros"],
        }
    else:
        raise CONTRACT.EvidenceError(f"unknown M4 fact kind: {evidence_kind}")
    value = {
        "childKind": kind,
        "evidenceKind": evidence_kind,
        "facts": facts,
        "result": "PASS_EVIDENCE_ONLY",
        "schema": CONTRACT.FACT_SCHEMA,
        "testedCommit": tested,
    }
    CONTRACT.validate_fact(value, evidence_kind, kind, tested, bindings, summary)
    return value


def build_package(root: Path, output: Path, tested: str, source_sha: str, bindings: dict) -> None:
    if output.exists() or output.is_symlink():
        raise CONTRACT.EvidenceError(f"output already exists: {output}")
    output.mkdir(parents=True)
    try:
        for ordinal, kind in enumerate(CONTRACT.CHILD_KINDS, start=1):
            child_relative = PurePosixPath(
                f"docs/v2/evidence/v2-m4/children/final-source-{tested}/{ordinal:02d}-{kind}"
            )
            attachment_rows = []
            junit, summary = junit_value(root, kind, tested)
            for attachment_index, attachment_kind in enumerate(CONTRACT.ATTACHMENTS[kind]):
                value = junit if attachment_kind == "JUNIT_SUMMARY" else fact_value(
                    kind, attachment_kind, tested, bindings, summary
                )
                raw = CONTRACT.canonical_bytes(value)
                relative = child_relative / "attachments" / f"{attachment_index:02d}-{attachment_kind}.json"
                write_new(output.joinpath(*relative.parts), raw)
                attachment_rows.append(
                    {
                        "bytes": len(raw),
                        "kind": attachment_kind,
                        "path": str(relative),
                        "sha256": CONTRACT.sha256(raw),
                    }
                )
            receipt = {
                "attachments": attachment_rows,
                "exclusions": CONTRACT.CHILD_EXCLUSIONS,
                "kind": kind,
                "promotionEligible": False,
                "result": CONTRACT.CHILD_RESULTS[kind],
                "schema": CONTRACT.CHILD_SCHEMA,
                "sourceTuple": {"nereusCommit": tested, "sourceLocksSha256": source_sha},
                "testSummary": summary,
            }
            write_new(output.joinpath(*(child_relative / "receipt.json").parts), CONTRACT.canonical_bytes(receipt))
    except Exception:
        shutil.rmtree(output)
        raise


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-tested-commit")
    parser.add_argument("--skip-execution", action="store_true")
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        root = CONTRACT.ensure_root(args.repo_root)
        tested = checked_text(["git", "rev-parse", "HEAD"], root)
        if args.expected_tested_commit and tested != args.expected_tested_commit:
            raise CONTRACT.EvidenceError("HEAD differs from --expected-tested-commit")
        if checked_text(["git", "status", "--porcelain=v1", "--untracked-files=all"], root):
            raise CONTRACT.EvidenceError("formal M4 evidence run requires a clean worktree")
        source_raw = CONTRACT.git_blob(root, tested, CONTRACT.SOURCE_LOCKS_PATH)
        source_sha = CONTRACT.sha256(source_raw)
        bindings = CONTRACT.source_bindings(root, tested, source_sha)
        pulsar = root.parent.parent / "nereusstream" / "pulsar"
        if checked_text(["git", "rev-parse", "HEAD"], pulsar) != "a14e0e6f4e49be0677318b4ceefc7b85b445823b":
            raise CONTRACT.EvidenceError("Pulsar source composite differs from admitted exact source")
        if checked_text(["git", "status", "--porcelain=v1", "--untracked-files=all"], pulsar):
            raise CONTRACT.EvidenceError("Pulsar source composite must be clean for formal M4 evidence")
        if not args.skip_execution:
            subprocess.run(
                ["./gradlew", "--no-daemon", "--rerun-tasks", "v2M4EvidenceExecutionCheck"],
                cwd=root,
                check=True,
            )
        if checked_text(["git", "rev-parse", "HEAD"], root) != tested:
            raise CONTRACT.EvidenceError("Nereus HEAD changed during formal M4 evidence")
        if checked_text(["git", "status", "--porcelain=v1", "--untracked-files=all"], root):
            raise CONTRACT.EvidenceError("Nereus worktree changed during formal M4 evidence")
        if checked_text(["git", "rev-parse", "HEAD"], pulsar) != "a14e0e6f4e49be0677318b4ceefc7b85b445823b":
            raise CONTRACT.EvidenceError("Pulsar source composite changed during formal M4 evidence")
        build_package(root, args.output.resolve(), tested, source_sha, bindings)
    except (CONTRACT.EvidenceError, OSError, subprocess.CalledProcessError) as error:
        print(f"V2 M4 evidence run: {error}", file=sys.stderr)
        return 1
    print(
        f"V2 M4 evidence package built: tested={tested} sourceLocksSha256={source_sha} "
        f"children={len(CONTRACT.CHILD_KINDS)} output={args.output.resolve()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
