#!/usr/bin/env python3
"""Deterministically materialize the closed V2 M1 N3 evidence set from executed JUnit XML."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any


COMMIT = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")
IMAGE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")

HARNESS_REPORTS = ("nereus-domain/build/test-results/m1AllocatorHarnessTest",)
REGISTRY_REPORTS = (
    "nereus-domain/build/test-results/r1RegistryDomainTest",
    "nereus-metadata-oxia/build/test-results/r1MetadataTest",
    "nereus-metadata-oxia/build/test-results/r1OxiaIntegrationTest",
)

CAPACITY = "com.nereusstream.domain.registry.RegistryCapacityEvidenceTest"
ADMISSION = "com.nereusstream.domain.registry.RegistryAdmissionEvidenceV1Test"
REGISTRY_CODEC = "com.nereusstream.domain.registry.Nvr1RegistryCodecV1Test"
TRANSITIONS = "com.nereusstream.domain.registry.PulsarVirtualLedgerRegistryTransitionValidatorV1Test"
AUTHORITY_CODEC = "com.nereusstream.metadata.oxia.v2.codec.Nvr1RegistryAuthorityCodecTest"
AUTHORITY = "com.nereusstream.metadata.oxia.v2.capability.R1RegistryAuthorityTest"
REAL_OXIA = "com.nereusstream.metadata.oxia.v2.R1RegistryOxiaIntegrationTest"
ALLOCATOR_CUTS = "com.nereusstream.domain.registry.allocator.AllocatorEvidenceProtocolHarnessTest"
RANGE_CUTS = "com.nereusstream.domain.registry.allocator.RangeLeasedCandidateHarnessTest"

REGISTRY_SCENARIOS = {
    "V2-POSITION-003": (CAPACITY, ADMISSION, AUTHORITY, REAL_OXIA),
    "V2-POSITION-004": (AUTHORITY_CODEC, AUTHORITY, REAL_OXIA),
    "V2-POSITION-005": (REGISTRY_CODEC, AUTHORITY),
    "V2-POSITION-006": (TRANSITIONS, AUTHORITY),
    "V2-POSITION-007": (CAPACITY, REGISTRY_CODEC),
    "V2-POSITION-008": (TRANSITIONS, AUTHORITY),
    "V2-POSITION-009": (CAPACITY, REGISTRY_CODEC, AUTHORITY),
}
HARNESS_SCENARIOS = {
    "V2-POSITION-010": (ALLOCATOR_CUTS,),
    "V2-POSITION-011": (RANGE_CUTS,),
}


class EvidenceError(RuntimeError):
    """Stable publisher rejection."""


@dataclass(frozen=True)
class Suite:
    suite_id: str
    discovered: int
    executed: int
    passed: int
    failures: int
    errors: int
    skipped: int
    aborted: int

    def report_row(self) -> dict[str, Any]:
        return {
            "aborted": self.aborted,
            "discovered": self.discovered,
            "errors": self.errors,
            "failed": self.failures,
            "passed": self.passed,
            "skipped": self.skipped,
            "suiteId": self.suite_id,
        }

    def receipt_row(self) -> dict[str, Any]:
        return {
            "aborted": self.aborted,
            "discovered": self.discovered,
            "executed": self.executed,
            "failed": self.failures + self.errors,
            "passed": self.passed,
            "skipped": self.skipped,
            "suiteId": self.suite_id,
        }


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def strict_json(path: pathlib.Path) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise EvidenceError(f"duplicate JSON member {key!r} in {path}")
            result[key] = value
        return result

    try:
        value = json.loads(path.read_bytes(), object_pairs_hook=reject_duplicates)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EvidenceError(f"cannot read strict JSON from {path}: {error}") from error
    if not isinstance(value, dict):
        raise EvidenceError(f"JSON root is not an object: {path}")
    return value


def require_string(value: object, label: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise EvidenceError(f"{label} has invalid grammar")
    return value


def source_tuple(root: pathlib.Path, nereus_commit: str) -> dict[str, str]:
    require_string(nereus_commit, "Nereus commit", COMMIT)
    locks_path = root / "docs/v2/source-locks.json"
    locks_bytes = locks_path.read_bytes()
    locks = strict_json(locks_path)
    try:
        client_fork = next(
            row for row in locks["dependencyForkOutputs"] if row["id"] == "oxia-client-notification-continuity"
        )
        client_artifacts = locks["dependencyEvidenceBindings"]["oxiaClientArtifacts"]
        server = locks["dependencyEvidenceBindings"]["oxiaServerRuntime"]
        n1 = locks["n1ArtifactBinding"]
        result = {
            "domainJarSha256": n1["artifacts"]["domainJar"]["sha256"],
            "domainPomSha256": n1["artifacts"]["domainPom"]["sha256"],
            "kafkaCommit": locks["k1KafkaAuthorityBinding"]["finalForkCommit"],
            "nereusCommit": nereus_commit,
            "oxiaClientCommit": client_fork["finalForkCommit"],
            "oxiaClientJarSha256": client_artifacts["artifacts"]["clientJar"]["sha256"],
            "oxiaClientPomSha256": client_artifacts["artifacts"]["clientPom"]["sha256"],
            "oxiaServerCommit": server["sourceCommit"],
            "oxiaServerImageDigest": server["imageDigest"],
            "pulsarCommit": locks["p1PulsarAuthorityBinding"]["finalForkCommit"],
            "sourceLocksSha256": digest(locks_bytes),
        }
    except (KeyError, StopIteration, TypeError) as error:
        raise EvidenceError(f"source-lock authority is incomplete: {error}") from error
    for key, value in result.items():
        pattern = IMAGE_DIGEST if key == "oxiaServerImageDigest" else SHA256 if key.endswith("Sha256") else COMMIT
        require_string(value, f"source tuple {key}", pattern)
    return result


def source_tuple_sha(root: pathlib.Path, nereus_commit: str) -> str:
    return digest(canonical(source_tuple(root, nereus_commit)))


def integer(attributes: dict[str, str], name: str, report: pathlib.Path) -> int:
    raw = attributes.get(name, "0")
    try:
        value = int(raw)
    except ValueError as error:
        raise EvidenceError(f"JUnit {name} is not an integer in {report}") from error
    if value < 0:
        raise EvidenceError(f"JUnit {name} is negative in {report}")
    return value


def read_suites(root: pathlib.Path, report_roots: tuple[str, ...], expected: set[str]) -> dict[str, Suite]:
    suites: dict[str, Suite] = {}
    for relative in report_roots:
        directory = root / relative
        reports = sorted(directory.glob("TEST-*.xml"))
        if not reports:
            raise EvidenceError(f"zero JUnit suites in {relative}")
        for report in reports:
            try:
                node = ET.parse(report).getroot()
            except (OSError, ET.ParseError) as error:
                raise EvidenceError(f"cannot parse JUnit XML {report}: {error}") from error
            name = node.attrib.get("name")
            if not name or name in suites:
                raise EvidenceError(f"missing or duplicate JUnit suite ID in {report}")
            tests = integer(node.attrib, "tests", report)
            failures = integer(node.attrib, "failures", report)
            errors = integer(node.attrib, "errors", report)
            skipped = integer(node.attrib, "skipped", report)
            aborted = integer(node.attrib, "aborted", report)
            executed = tests - skipped
            passed = executed - failures - errors - aborted
            if tests <= 0 or executed <= 0 or passed < 0 or failures or errors or skipped or aborted:
                raise EvidenceError(
                    f"mandatory JUnit suite is not all-pass: {name} tests={tests} failures={failures} "
                    f"errors={errors} skipped={skipped} aborted={aborted}"
                )
            suites[name] = Suite(name, tests, executed, passed, failures, errors, skipped, aborted)
    if set(suites) != expected:
        raise EvidenceError(
            f"JUnit suite inventory differs: missing={sorted(expected - set(suites))} "
            f"extra={sorted(set(suites) - expected)}"
        )
    return suites


def normalized_report(gate_id: str, tuple_sha: str, suites: dict[str, Suite]) -> bytes:
    rows = [suites[name].report_row() for name in sorted(suites)]
    totals = {name: 0 for name in ("aborted", "discovered", "errors", "failed", "passed", "skipped")}
    for row in rows:
        for name in totals:
            totals[name] += int(row[name])
    return canonical(
        {
            "gateId": gate_id,
            "schema": "NEREUS_V2_M1_NORMALIZED_TEST_REPORT_V1",
            "sourceTupleSha": tuple_sha,
            "suiteCount": len(rows),
            "suites": rows,
            "totals": totals,
        }
    )


def receipt(
    kind: str,
    tuple_value: dict[str, str],
    scenarios: dict[str, tuple[str, ...]],
    suites: dict[str, Suite],
    attachment_path: str,
    attachment_bytes: bytes,
) -> bytes:
    scenario_rows = []
    for scenario_id in sorted(scenarios):
        scenario_rows.append(
            {
                "scenarioId": scenario_id,
                "suites": [suites[name].receipt_row() for name in sorted(scenarios[scenario_id])],
            }
        )
    return canonical(
        {
            "attachments": [
                {
                    "attachmentKind": "TEST_REPORT",
                    "length": len(attachment_bytes),
                    "path": attachment_path,
                    "sha256": digest(attachment_bytes),
                }
            ],
            "kind": kind,
            "scenarios": scenario_rows,
            "schema": "NEREUS_VIRTUAL_LEDGER_RECEIPT_V1",
            "sourceTuple": tuple_value,
        }
    )


def verify_gate(path: pathlib.Path, gate_id: str, tuple_sha: str) -> bytes:
    data = path.read_bytes()
    value = strict_json(path)
    expected = {
        "gateId": gate_id,
        "outcome": "PASS",
        "schema": "NEREUS_V2_M1_GATE_RESULT_V1",
        "sourceTupleSha": tuple_sha,
    }
    if value != expected or data != canonical(expected):
        raise EvidenceError(f"gate result is non-canonical or source-mismatched: {path}")
    return data


def reference(kind: str, path: str, data: bytes) -> dict[str, Any]:
    return {"kind": kind, "length": len(data), "path": path, "sha256": digest(data)}


def gate_reference(gate_id: str, path: str, data: bytes) -> dict[str, Any]:
    return {"gateId": gate_id, "length": len(data), "path": path, "sha256": digest(data)}


def generate(
    root: pathlib.Path,
    nereus_commit: str,
    fast_gate_path: pathlib.Path,
    exact_gate_path: pathlib.Path,
    output: pathlib.Path,
) -> None:
    tuple_value = source_tuple(root, nereus_commit)
    tuple_sha = digest(canonical(tuple_value))
    harness_expected = {name for names in HARNESS_SCENARIOS.values() for name in names}
    registry_expected = {name for names in REGISTRY_SCENARIOS.values() for name in names}
    harness_suites = read_suites(root, HARNESS_REPORTS, harness_expected)
    registry_suites = read_suites(root, REGISTRY_REPORTS, registry_expected)
    fast_gate = verify_gate(fast_gate_path, "V2_M1_FAST", tuple_sha)
    exact_gate = verify_gate(exact_gate_path, "V2_M1_EXACT_SOURCE", tuple_sha)

    harness_report = normalized_report("V2_M1_FAST", tuple_sha, harness_suites)
    registry_report = normalized_report("V2_M1_EXACT_SOURCE", tuple_sha, registry_suites)
    harness_receipt = receipt(
        "HARNESS_CONFORMANCE_ONLY",
        tuple_value,
        HARNESS_SCENARIOS,
        harness_suites,
        "attachments/harness-test-report.json",
        harness_report,
    )
    registry_receipt = receipt(
        "REGISTRY_CONFORMANCE",
        tuple_value,
        REGISTRY_SCENARIOS,
        registry_suites,
        "attachments/registry-test-report.json",
        registry_report,
    )
    final_index = canonical(
        {
            "receiptRefs": [
                reference("HARNESS_CONFORMANCE_ONLY", "harness-conformance.json", harness_receipt),
                reference("REGISTRY_CONFORMANCE", "registry-conformance.json", registry_receipt),
            ],
            "requiredGateRefs": [
                gate_reference("V2_M1_EXACT_SOURCE", "gates/exact-source.json", exact_gate),
                gate_reference("V2_M1_FAST", "gates/fast.json", fast_gate),
            ],
            "schema": "NEREUS_V2_M1_FINAL_INDEX_V1",
            "sourceTupleSha": tuple_sha,
        }
    )

    if output.exists() and (not output.is_dir() or output.is_symlink()):
        raise EvidenceError(f"output is not a safe directory: {output}")
    output.mkdir(parents=True, exist_ok=True)
    expected_paths = {
        "attachments/harness-test-report.json": harness_report,
        "attachments/registry-test-report.json": registry_report,
        "gates/exact-source.json": exact_gate,
        "gates/fast.json": fast_gate,
        "harness-conformance.json": harness_receipt,
        "registry-conformance.json": registry_receipt,
        "final-index.json": final_index,
    }
    existing = {str(path.relative_to(output)) for path in output.rglob("*") if path.is_file() or path.is_symlink()}
    unknown = sorted(existing - set(expected_paths))
    if unknown:
        raise EvidenceError(f"output contains unknown files: {unknown}")
    for relative, data in expected_paths.items():
        path = output / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink():
            raise EvidenceError(f"output path is a symlink: {path}")
        path.write_bytes(data)
    print(
        f"V2 M1 N3 evidence generated: files={len(expected_paths)} sourceTupleSha={tuple_sha} "
        f"harnessSuites={len(harness_suites)} registrySuites={len(registry_suites)}"
    )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    subcommands = result.add_subparsers(dest="command", required=True)
    sha = subcommands.add_parser("source-tuple-sha")
    sha.add_argument("--repo-root", type=pathlib.Path, required=True)
    sha.add_argument("--nereus-commit", required=True)
    publish = subcommands.add_parser("generate")
    publish.add_argument("--repo-root", type=pathlib.Path, required=True)
    publish.add_argument("--nereus-commit", required=True)
    publish.add_argument("--fast-gate", type=pathlib.Path, required=True)
    publish.add_argument("--exact-gate", type=pathlib.Path, required=True)
    publish.add_argument("--output", type=pathlib.Path, required=True)
    return result


def main(argv: list[str]) -> int:
    args = parser().parse_args(argv[1:])
    try:
        root = args.repo_root.resolve(strict=True)
        if args.command == "source-tuple-sha":
            print(source_tuple_sha(root, args.nereus_commit))
        else:
            generate(
                root,
                args.nereus_commit,
                args.fast_gate.resolve(strict=True),
                args.exact_gate.resolve(strict=True),
                args.output.resolve(),
            )
    except (EvidenceError, OSError) as error:
        print(f"V2 M1 N3 evidence publisher: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
