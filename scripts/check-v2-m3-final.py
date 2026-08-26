#!/usr/bin/env python3
"""Fail-closed validator for the exact-source Nereus V2 M3 Final receipt."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
from typing import Any


SCHEMA = "NEREUS_V2_M3_FINAL_V1"
KIND = "V2_M3_FINAL"
RESULT = "PASS_V2_M3_FINAL"
MAX_CANONICAL_BYTES = 131_072
MAX_SAFE_INTEGER = 9_007_199_254_740_991
MAX_ATTACHMENTS_PER_CHILD = 32
MAX_SINGLE_EVIDENCE_BYTES = 67_108_864
MAX_TOTAL_EVIDENCE_BYTES = 268_435_456
EVIDENCE_PREFIX = PurePosixPath("docs/v2/evidence/v2-m3")
FINAL_PREFIX = EVIDENCE_PREFIX / "final"
SOURCE_LOCKS_PATH = PurePosixPath("docs/v2/source-locks.json")
SCENARIO_PATH = PurePosixPath("docs/v2/v2-scenarios.json")
SOURCE_BINDING_SCHEMA = "NEREUS_V2_M3_EVIDENCE_SOURCE_LOCKS_V2"

CHILD_RESULTS = {
    "W1_CURRENT_SOURCE_M2_REGRESSION": "PASS_CURRENT_SOURCE_M2_REGRESSION_ONLY",
    "AB_NWG1_WIRE": "PASS_NWG1_WIRE_ONLY",
    "C_OBJECT_WAL_STATE_TRACE": "PASS_OBJECT_WAL_STATE_TRACE_ONLY",
    "D_LOCAL_CAP": "PASS_LOCAL_CAP_ONLY",
    "C1_REAL_PROVIDER_KMS": "PASS_REAL_PROVIDER_KMS_ONLY",
    "C2_SEGMENTED_PREFIX": "PASS_C2_SEGMENTED_PREFIX_EVIDENCE_ONLY",
    "R_CONTROL_RECOVERY": "PASS_CONTROL_RECOVERY_ONLY",
    "K_NWKCP1": "PASS_NWKCP1_ONLY",
    "U_KAFKA_OBJECT_WAL": "PASS_KAFKA_OBJECT_WAL_ONLY",
    "P_PULSAR_OBJECT_WAL": "PASS_PULSAR_OBJECT_WAL_ONLY",
    "ALLOCATOR_SELECTION": "PASS_ALLOCATOR_SELECTION_ONLY",
}
CHILD_KINDS = tuple(CHILD_RESULTS)

ATTACHMENT_KINDS = {
    "ALLOCATOR_FAULT_SUMMARY",
    "ALLOCATOR_NATIVE_RELATIVE_SUMMARY",
    "ALLOCATOR_RAW_VERIFICATION",
    "ALLOCATOR_SCALE_100000_SUMMARY",
    "ALLOCATOR_SCALE_10000_SUMMARY",
    "CURRENT_SOURCE_M2_GATE_RESULT",
    "JUNIT_SUMMARY",
    "KMS_REAL_RECEIPT",
    "LOCAL_CAP_RESULT",
    "MUTATION_MANIFEST",
    "NATIVE_RESULT",
    "NWG1_VECTOR_MANIFEST",
    "PROTOCOL_FIXTURE",
    "PROVIDER_REAL_RECEIPT",
    "RECOVERY_MANIFEST",
    "SOURCE_LOCK_SNAPSHOT",
    "TRACE_MANIFEST",
    "WIRE_ARTIFACT",
    "ZSTD_INTEROPERABILITY_FIXTURE",
}
EXCLUSIONS = {
    "C1_EVIDENCE_SUBSTITUTE",
    "M3_FINAL_AGGREGATE",
    "M6_PROCESS_ACTIVATION",
    "M8_NATIVE_PARITY",
    "PRODUCTION_ALLOWLIST",
    "REAL_KMS",
    "REAL_PROVIDER",
    "SCENARIO_PROMOTION",
}
FINAL_EXCLUSIONS = ["M6_PROCESS_ACTIVATION", "M8_NATIVE_PARITY"]
ALLOCATOR_MODES = {"RANGE", "STRICT"}
REQUIRED_TYPED_ATTACHMENTS = {
    "W1_CURRENT_SOURCE_M2_REGRESSION": {"CURRENT_SOURCE_M2_GATE_RESULT"},
    "AB_NWG1_WIRE": {
        "MUTATION_MANIFEST",
        "NWG1_VECTOR_MANIFEST",
        "WIRE_ARTIFACT",
        "ZSTD_INTEROPERABILITY_FIXTURE",
    },
    "C_OBJECT_WAL_STATE_TRACE": {"TRACE_MANIFEST"},
    "D_LOCAL_CAP": {"LOCAL_CAP_RESULT"},
    "C1_REAL_PROVIDER_KMS": {"KMS_REAL_RECEIPT", "PROVIDER_REAL_RECEIPT"},
    "R_CONTROL_RECOVERY": {"RECOVERY_MANIFEST"},
    "K_NWKCP1": {"PROTOCOL_FIXTURE"},
    "U_KAFKA_OBJECT_WAL": {"NATIVE_RESULT"},
    "P_PULSAR_OBJECT_WAL": {"NATIVE_RESULT"},
    "ALLOCATOR_SELECTION": {
        "ALLOCATOR_FAULT_SUMMARY",
        "ALLOCATOR_NATIVE_RELATIVE_SUMMARY",
        "ALLOCATOR_RAW_VERIFICATION",
        "ALLOCATOR_SCALE_10000_SUMMARY",
        "ALLOCATOR_SCALE_100000_SUMMARY",
    },
}
REQUIRED_SCENARIOS = (
    "V2-FABRIC-002",
    "V2-OBJ-001",
    "V2-OBJ-002",
    "V2-OBJ-003",
    "V2-OBJ-004",
    "V2-OBJ-005",
    "V2-OBJ-006",
    "V2-OBJ-007",
    "V2-OBJ-008",
    "V2-OBJ-009",
    "V2-OBJ-010",
    "V2-OBJ-012",
    "V2-OBJ-013",
    "V2-OBJ-016",
    "V2-OBJ-017",
    "V2-OBJ-019",
    "V2-OBJ-021",
    "V2-OBJ-023",
    "V2-OBJ-024",
    "V2-POSITION-012",
    "V2-POSITION-013",
    "V2-POSITION-014",
    "V2-POSITION-015",
    "V2-POSITION-016",
    "V2-POSITION-017",
    "V2-POSITION-018",
)

M2_REGRESSION_SCHEMA = "NEREUS_V2_M3_CURRENT_SOURCE_M2_REGRESSION_V1"
M2_REGRESSION_RESULT = "PASS_CURRENT_SOURCE_M2_REGRESSION_ONLY"
M2_REQUIRED_GATES = (
    "KAFKA_K0",
    "KAFKA_K1",
    "KAFKA_K2",
    "KAFKA_K3",
    "KAFKA_K4",
    "KAFKA_K5",
    "KAFKA_K6",
    "KAFKA_K7",
    "KAFKA_K8",
    "KAFKA_K9",
    "KAFKA_K10",
    "KAFKA_EXACT",
    "KAFKA_REAL_BOOKKEEPER",
    "KAFKA_SCALE_10000",
    "KAFKA_SCALE_100000",
    "PULSAR_P0",
    "PULSAR_P1",
    "PULSAR_P2",
    "PULSAR_P3",
    "PULSAR_P4",
    "PULSAR_P5",
    "PULSAR_P6",
    "PULSAR_NATIVE",
    "PULSAR_P6_PROVIDER",
    "PULSAR_FINAL_PARSER_POLICY",
)

EVIDENCE_ONLY_EXACT = {
    PurePosixPath("docs/v2/08-implementation-plan-and-gates.md"),
    PurePosixPath("docs/v2/09-scenario-evidence-matrix.md"),
    PurePosixPath("docs/v2/README.md"),
    PurePosixPath("docs/v2/detailed_design/m3/README.md"),
    SCENARIO_PATH,
}
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
SOURCE_ID_PATTERN = re.compile(r"[A-Z0-9][A-Z0-9_.:-]{0,127}")
_CHILD_CONTRACT = None


class FinalError(RuntimeError):
    """Stable fail-closed M3 Final rejection."""


def _child_contract():
    global _CHILD_CONTRACT
    if _CHILD_CONTRACT is None:
        path = Path(__file__).with_name("check-v2-m3-child.py")
        spec = importlib.util.spec_from_file_location("check_v2_m3_child_for_final", path)
        if spec is None or spec.loader is None:
            raise FinalError(f"cannot load M3 child contract: {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        _CHILD_CONTRACT = module
    return _CHILD_CONTRACT


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _reject_duplicate_members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise FinalError(f"duplicate JSON member: {key}")
        result[key] = value
    return result


def _validate_jcs_domain(value: Any, label: str) -> None:
    if value is None or type(value) is bool:
        return
    if type(value) is int:
        if abs(value) > MAX_SAFE_INTEGER:
            raise FinalError(f"integer exceeds the closed JCS safe domain in {label}")
        return
    if isinstance(value, str):
        if not value.isascii():
            raise FinalError(f"non-ASCII string is outside the closed JCS schema in {label}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_jcs_domain(item, f"{label}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key.isascii():
                raise FinalError(f"non-ASCII object key is outside the closed JCS schema in {label}")
            _validate_jcs_domain(item, f"{label}.{key}")
        return
    raise FinalError(f"unsupported JCS value type in {label}: {type(value).__name__}")


def canonical_bytes(value: Any) -> bytes:
    _validate_jcs_domain(value, "root")
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def load_canonical_json(raw: bytes, label: str) -> dict[str, Any]:
    if not raw or len(raw) > MAX_CANONICAL_BYTES:
        raise FinalError(f"canonical JSON bytes outside cap: {label}")
    try:
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_members)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FinalError(f"cannot parse JSON {label}: {error}") from error
    if not isinstance(value, dict):
        raise FinalError(f"JSON root is not an object: {label}")
    if canonical_bytes(value) != raw:
        raise FinalError(f"JSON is not exact closed-domain JCS: {label}")
    return value


def safe_relative(value: object, label: str, prefix: PurePosixPath = EVIDENCE_PREFIX) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise FinalError(f"{label} is not a safe POSIX-relative path")
    parts = value.split("/")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in ("", ".", "..") for part in parts):
        raise FinalError(f"{label} is not a safe POSIX-relative path")
    if not is_under(path, prefix):
        raise FinalError(f"{label} is outside {prefix}: {path}")
    return path


def is_under(path: PurePosixPath, parent: PurePosixPath) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def read_safe_file(root: Path, relative: PurePosixPath, maximum: int = MAX_SINGLE_EVIDENCE_BYTES) -> bytes:
    current = root
    mode = 0
    for part in relative.parts:
        current /= part
        try:
            mode = current.lstat().st_mode
        except OSError as error:
            raise FinalError(f"evidence file is missing: {relative}") from error
        if stat.S_ISLNK(mode):
            raise FinalError(f"evidence path has a symlink component: {relative}")
    if not stat.S_ISREG(mode):
        raise FinalError(f"evidence path is not a regular file: {relative}")
    if current.stat().st_size <= 0 or current.stat().st_size > maximum:
        raise FinalError(f"evidence file bytes outside cap: {relative}")
    return current.read_bytes()


def _exact_members(value: object, members: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != members:
        actual = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise FinalError(f"{label} member set differs: {actual}")
    return value


def _positive(value: object, label: str) -> int:
    if type(value) is not int or value <= 0 or value > MAX_SAFE_INTEGER:
        raise FinalError(f"{label} must be a positive JCS-safe integer")
    return value


def _zero(value: object, label: str) -> int:
    if type(value) is not int or value != 0:
        raise FinalError(f"{label} must be zero")
    return value


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256_PATTERN.fullmatch(value):
        raise FinalError(f"{label} is not a canonical lowercase SHA-256")
    return value


def _commit(value: object, label: str) -> str:
    if not isinstance(value, str) or not COMMIT_PATTERN.fullmatch(value):
        raise FinalError(f"{label} is not a canonical lowercase commit")
    return value


def _validate_attachment(value: object, label: str) -> dict[str, Any]:
    row = _exact_members(value, {"bytes", "kind", "path", "sha256"}, label)
    _positive(row["bytes"], f"{label}.bytes")
    if row["kind"] not in ATTACHMENT_KINDS:
        raise FinalError(f"{label}.kind is outside the closed inventory")
    safe_relative(row["path"], f"{label}.path")
    _sha(row["sha256"], f"{label}.sha256")
    return row


def _validate_child_shape(value: object, expected_kind: str, final_commit: str) -> dict[str, Any]:
    members = {
        "attachments",
        "bytes",
        "errors",
        "exclusions",
        "failures",
        "kind",
        "path",
        "promotionEligible",
        "result",
        "sha256",
        "skipped",
        "sourceTuple",
        "tests",
    }
    child = _exact_members(value, members, f"child {expected_kind}")
    if child["kind"] != expected_kind or child["result"] != CHILD_RESULTS[expected_kind]:
        raise FinalError(f"child identity/result differs: {expected_kind}")
    if child["promotionEligible"] is not False:
        raise FinalError(f"focused child is promotable: {expected_kind}")
    _positive(child["bytes"], f"{expected_kind}.bytes")
    _positive(child["tests"], f"{expected_kind}.tests")
    _zero(child["failures"], f"{expected_kind}.failures")
    _zero(child["errors"], f"{expected_kind}.errors")
    _zero(child["skipped"], f"{expected_kind}.skipped")
    safe_relative(child["path"], f"{expected_kind}.path")
    _sha(child["sha256"], f"{expected_kind}.sha256")
    source = _exact_members(
        child["sourceTuple"],
        {"nereusCommit", "sourceTupleId", "sourceTupleSha256"},
        f"{expected_kind}.sourceTuple",
    )
    if _commit(source["nereusCommit"], f"{expected_kind}.sourceTuple.nereusCommit") != final_commit:
        raise FinalError(f"child tested Nereus source differs from Final source: {expected_kind}")
    if not isinstance(source["sourceTupleId"], str) or not SOURCE_ID_PATTERN.fullmatch(source["sourceTupleId"]):
        raise FinalError(f"child source tuple ID is invalid: {expected_kind}")
    _sha(source["sourceTupleSha256"], f"{expected_kind}.sourceTuple.sourceTupleSha256")
    exclusions = child["exclusions"]
    if (
        not isinstance(exclusions, list)
        or not exclusions
        or exclusions != sorted(set(exclusions))
        or not set(exclusions).issubset(EXCLUSIONS)
    ):
        raise FinalError(f"child exclusions are empty, open, duplicated, or unsorted: {expected_kind}")
    if not {"M3_FINAL_AGGREGATE", "SCENARIO_PROMOTION"}.issubset(exclusions):
        raise FinalError(f"child does not exclude aggregate/scenario promotion: {expected_kind}")
    if expected_kind == "D_LOCAL_CAP" and not {"REAL_KMS", "REAL_PROVIDER"}.issubset(exclusions):
        raise FinalError("D local child does not exclude real Provider/KMS claims")
    if expected_kind == "C2_SEGMENTED_PREFIX" and not {
        "C1_EVIDENCE_SUBSTITUTE",
        "PRODUCTION_ALLOWLIST",
    }.issubset(exclusions):
        raise FinalError("C2 child can substitute for C1 or enter the production allowlist")
    attachments = child["attachments"]
    if not isinstance(attachments, list) or not 1 <= len(attachments) <= MAX_ATTACHMENTS_PER_CHILD:
        raise FinalError(f"child attachment count outside cap: {expected_kind}")
    validated = [
        _validate_attachment(row, f"{expected_kind}.attachments[{index}]")
        for index, row in enumerate(attachments)
    ]
    paths = [row["path"] for row in validated]
    if paths != sorted(set(paths)):
        raise FinalError(f"child attachment paths are duplicated or unsorted: {expected_kind}")
    missing_kinds = REQUIRED_TYPED_ATTACHMENTS.get(expected_kind, set()) - {
        row["kind"] for row in validated
    }
    if missing_kinds:
        raise FinalError(f"child mandatory typed attachments are absent: {expected_kind} {sorted(missing_kinds)}")
    return child


def _identity(value: dict[str, Any]) -> tuple[str, int, str]:
    return (value["path"], value["bytes"], value["sha256"])


def _validate_child_receipt_semantics(
    child: dict[str, Any], child_receipt: dict[str, Any], final_commit: str
) -> None:
    kind = child["kind"]
    if child_receipt.get("result") != child["result"] or child_receipt.get("promotionEligible") is not False:
        raise FinalError(f"child receipt result/promotion boundary differs: {kind}")
    if kind == "W1_CURRENT_SOURCE_M2_REGRESSION":
        _validate_m2_regression_child(child, child_receipt, final_commit)
        source_value = child_receipt.get("sources")
        source_id = "SOURCES"
    else:
        source_value = child_receipt.get("sourceTuple")
        source_id = "SOURCE_TUPLE"
        if not isinstance(source_value, dict):
            raise FinalError(f"child receipt has no closed sourceTuple: {kind}")
        if source_value.get("nereusCommit") != final_commit:
            raise FinalError(f"child receipt sourceTuple Nereus commit differs: {kind}")
        summary = _exact_members(
            child_receipt.get("testSummary"),
            {"errors", "failures", "skipped", "tests"},
            f"{kind}.testSummary",
        )
        if (
            summary["tests"] != child["tests"]
            or summary["failures"] != child["failures"]
            or summary["errors"] != child["errors"]
            or summary["skipped"] != child["skipped"]
        ):
            raise FinalError(f"child receipt and Final counters differ: {kind}")
        receipt_attachments = child_receipt.get("attachments")
        if not isinstance(receipt_attachments, list):
            raise FinalError(f"child receipt has no explicit attachments: {kind}")
        receipt_identities = {
            _identity(_validate_attachment_identity(row, f"{kind}.receipt.attachments"))
            for row in receipt_attachments
        }
        if receipt_identities != {_identity(row) for row in child["attachments"]}:
            raise FinalError(f"child receipt attachment inventory differs from Final: {kind}")
        child_exclusions = child_receipt.get("exclusions")
        if child_exclusions != child["exclusions"]:
            raise FinalError(f"child receipt exclusions differ from Final: {kind}")
    source = child["sourceTuple"]
    if source["sourceTupleId"] != source_id or source["sourceTupleSha256"] != sha256(canonical_bytes(source_value)):
        raise FinalError(f"child source tuple digest binding differs: {kind}")


def _validate_attachment_identity(value: object, label: str) -> dict[str, Any]:
    if isinstance(value, dict) and set(value) == {"bytes", "kind", "path", "sha256"}:
        row = value
        if row["kind"] not in ATTACHMENT_KINDS:
            raise FinalError(f"{label}.kind is outside the closed inventory")
    else:
        row = _exact_members(value, {"bytes", "path", "sha256"}, label)
    _positive(row["bytes"], f"{label}.bytes")
    safe_relative(row["path"], f"{label}.path")
    _sha(row["sha256"], f"{label}.sha256")
    return row


def _validate_m2_regression_child(
    child: dict[str, Any], receipt: dict[str, Any], final_commit: str
) -> None:
    _exact_members(
        receipt,
        {
            "childGates",
            "evidenceClass",
            "exclusions",
            "historicalFinal",
            "kind",
            "m2AmendmentLineage",
            "promotionEligible",
            "result",
            "scenarioPromotion",
            "schema",
            "sources",
            "testedNereusCommit",
        },
        "W1 current-source M2 regression",
    )
    if (
        receipt.get("schema") != M2_REGRESSION_SCHEMA
        or receipt.get("kind") != "CURRENT_SOURCE_M2_REGRESSION"
        or receipt.get("result") != M2_REGRESSION_RESULT
        or receipt.get("evidenceClass") != "TRUSTED_FULL_CURRENT_SOURCE_M2"
        or receipt.get("testedNereusCommit") != final_commit
        or receipt.get("promotionEligible") is not False
        or receipt.get("scenarioPromotion") is not False
        or receipt.get("m2AmendmentLineage") != []
        or receipt.get("exclusions")
        != [
            "M3_IMPLEMENTATION_AND_FINAL",
            "M6_PROCESS_ACTIVATION",
            "M8_NATIVE_PARITY",
            "SCENARIO_PROMOTION",
        ]
    ):
        raise FinalError("W1 current-source M2 regression identity/source differs")
    sources = receipt.get("sources")
    if (
        not isinstance(sources, dict)
        or not isinstance(sources.get("providerAdapter"), dict)
        or sources["providerAdapter"].get("nereusCommit") != final_commit
    ):
        raise FinalError("W1 provider-adapter current-source binding differs")
    gates = receipt.get("childGates")
    if not isinstance(gates, list) or tuple(row.get("gateId") for row in gates) != M2_REQUIRED_GATES:
        raise FinalError("W1 current-source M2 regression gate inventory differs")
    tests = failures = errors = skipped = 0
    attachment_identities: set[tuple[str, int, str]] = set()
    child_parent = PurePosixPath(child["path"]).parent
    for gate_index, gate in enumerate(gates):
        _exact_members(
            gate,
            {"attachment", "errors", "failures", "gateId", "result", "skipped", "tests"},
            f"W1.childGates[{gate_index}]",
        )
        if gate.get("result") != "PASS":
            raise FinalError(f"W1 child gate result is not PASS: {gate.get('gateId')}")
        tests += _positive(gate.get("tests"), f"W1.{gate.get('gateId')}.tests")
        failures += _zero(gate.get("failures"), f"W1.{gate.get('gateId')}.failures")
        errors += _zero(gate.get("errors"), f"W1.{gate.get('gateId')}.errors")
        skipped += _zero(gate.get("skipped"), f"W1.{gate.get('gateId')}.skipped")
        attachment = _validate_attachment_identity(gate.get("attachment"), "W1 gate attachment")
        expected_path = child_parent / "attachments" / f"{gate.get('gateId')}.json"
        if attachment["path"] != str(expected_path):
            raise FinalError(f"W1 child gate attachment path differs: {gate.get('gateId')}")
        attachment_identities.add(_identity(attachment))
    if (tests, failures, errors, skipped) != (
        child["tests"],
        child["failures"],
        child["errors"],
        child["skipped"],
    ):
        raise FinalError("W1 current-source M2 regression counters differ from Final")
    if attachment_identities != {_identity(row) for row in child["attachments"]}:
        raise FinalError("W1 current-source M2 regression attachment inventory differs from Final")


def _verify_typed_child_attachments(
    root: Path,
    child: dict[str, Any],
    tested: str,
    bindings: dict[tuple[str, str], dict[str, str]],
) -> None:
    contract = _child_contract()
    for attachment in child["attachments"]:
        kind = attachment["kind"]
        if kind not in contract.NORMALIZED_TYPED_KINDS:
            continue
        path = safe_relative(attachment["path"], f"{child['kind']}.{kind}.path")
        maximum = contract.SEALED_NATIVE_MAX_BYTES if kind == "NATIVE_RESULT" else contract.MAX_CANONICAL_BYTES
        value = contract.load_canonical_json(read_safe_file(root, path), str(path), maximum)
        try:
            if kind == "NATIVE_RESULT":
                contract.validate_native_result(
                    value,
                    root,
                    child["kind"],
                    tested,
                    bindings[(child["kind"], kind)],
                )
            else:
                contract.validate_typed_evidence(
                    value,
                    kind,
                    child["kind"],
                    tested,
                    bindings[(child["kind"], kind)],
                )
        except contract.ChildError as error:
            raise FinalError(str(error)) from error
        if kind in contract.REAL_RECEIPT_KINDS or kind == "NATIVE_RESULT":
            continue
        binding = bindings[(child["kind"], kind)]
        if (
            value["backend"] != binding["backend"]
            or value["executionClass"] != binding["executionClass"]
            or value["sourceIdentity"] != binding["sourceIdentity"]
        ):
            raise FinalError(f"typed attachment is not the exact locked backend/source: {child['kind']}/{kind}")


def validate_receipt_value(root: Path, value: object, expected_tested_commit: str | None = None) -> str:
    members = {
        "allocatorSelection",
        "childReceipts",
        "exclusions",
        "kind",
        "promotionEligible",
        "providerEvidence",
        "result",
        "scenarios",
        "schema",
        "sourceTuple",
    }
    receipt = _exact_members(value, members, "M3 Final")
    if (
        receipt["schema"] != SCHEMA
        or receipt["kind"] != KIND
        or receipt["result"] != RESULT
        or receipt["promotionEligible"] is not True
    ):
        raise FinalError("M3 Final schema/kind/result/promotion identity differs")
    source = _exact_members(
        receipt["sourceTuple"], {"nereusCommit", "sourceLocksSha256"}, "M3 Final sourceTuple"
    )
    tested = _commit(source["nereusCommit"], "M3 Final sourceTuple.nereusCommit")
    if expected_tested_commit is not None and tested != expected_tested_commit:
        raise FinalError(f"M3 Final tested commit differs from expected: {tested} != {expected_tested_commit}")
    _sha(source["sourceLocksSha256"], "M3 Final sourceTuple.sourceLocksSha256")
    if receipt["exclusions"] != FINAL_EXCLUSIONS:
        raise FinalError("M3 Final must retain exact M6 and M8 exclusions")
    if tuple(receipt["scenarios"]) != REQUIRED_SCENARIOS:
        raise FinalError("M3 Final scenario set differs from the exact evidence-owned allowlist")
    provider = _exact_members(
        receipt["providerEvidence"],
        {"c2PromotionEligible", "realKms", "realProvider"},
        "M3 Final providerEvidence",
    )
    if provider != {"c2PromotionEligible": False, "realKms": True, "realProvider": True}:
        raise FinalError("M3 Final requires real Provider/KMS and keeps C2 non-promotable")
    allocator = _exact_members(
        receipt["allocatorSelection"],
        {"faultEvidence", "mode", "nativeRelativeEvidence", "scale10000", "scale100000"},
        "M3 Final allocatorSelection",
    )
    if (
        allocator["mode"] not in ALLOCATOR_MODES
        or allocator["faultEvidence"] is not True
        or allocator["nativeRelativeEvidence"] is not True
        or allocator["scale10000"] is not True
        or allocator["scale100000"] is not True
    ):
        raise FinalError("allocator selection lacks one mode or fault/native/10k/100k evidence")
    source_locks = git_blob(root, tested, SOURCE_LOCKS_PATH)
    if sha256(source_locks) != source["sourceLocksSha256"]:
        raise FinalError("M3 Final source-lock SHA differs from the exact tested production tree")
    try:
        locked_mode, bindings = _child_contract().source_bindings(
            root, tested, allocator["mode"]
        )
    except _child_contract().ChildError as error:
        raise FinalError(str(error)) from error
    if locked_mode != allocator["mode"]:
        raise FinalError("Final allocator mode is not derived from exact tested source locks")
    children = receipt["childReceipts"]
    if not isinstance(children, list) or len(children) != len(CHILD_KINDS):
        raise FinalError("M3 Final child count differs from the closed inventory")
    all_paths: set[str] = set()
    total_bytes = 0
    for expected_kind, raw_child in zip(CHILD_KINDS, children, strict=True):
        child = _validate_child_shape(raw_child, expected_kind, tested)
        paths = [child["path"], *(row["path"] for row in child["attachments"])]
        if all_paths.intersection(paths):
            raise FinalError(f"M3 Final evidence path is duplicated: {expected_kind}")
        all_paths.update(paths)
        child_path = safe_relative(child["path"], f"{expected_kind}.path")
        child_raw = read_safe_file(root, child_path)
        if len(child_raw) != child["bytes"] or sha256(child_raw) != child["sha256"]:
            raise FinalError(f"child receipt bytes/SHA differ: {expected_kind}")
        try:
            derived = _child_contract().build_final_child_identity(
                root, child_path, expected_kind, tested
            )
        except _child_contract().ChildError as error:
            raise FinalError(f"child contract rejected {expected_kind}: {error}") from error
        if derived != child:
            raise FinalError(f"Final child row is not exactly derived from the closed child receipt: {expected_kind}")
        _verify_typed_child_attachments(root, child, tested, bindings)
        total_bytes += len(child_raw)
        for attachment in child["attachments"]:
            attachment_path = safe_relative(attachment["path"], f"{expected_kind}.attachment.path")
            raw = read_safe_file(root, attachment_path)
            if len(raw) != attachment["bytes"] or sha256(raw) != attachment["sha256"]:
                raise FinalError(f"child attachment bytes/SHA differ: {attachment_path}")
            total_bytes += len(raw)
        if total_bytes > MAX_TOTAL_EVIDENCE_BYTES:
            raise FinalError("M3 Final total evidence bytes exceed cap")
    return tested


def git(root: Path, *args: str, text: bool = False) -> bytes | str:
    try:
        return subprocess.check_output(
            ["git", "-C", os.fspath(root), *args],
            stderr=subprocess.STDOUT,
            text=text,
        )
    except subprocess.CalledProcessError as error:
        output = error.output if isinstance(error.output, str) else error.output.decode("utf-8", "replace")
        raise FinalError(f"git {' '.join(args)} failed: {output.strip()}") from error


def git_blob(root: Path, commit: str, path: PurePosixPath) -> bytes:
    raw = git(root, "show", f"{commit}:{path}")
    assert isinstance(raw, bytes)
    return raw


def ensure_root(root: Path) -> Path:
    try:
        root = root.resolve(strict=True)
        top = Path(str(git(root, "rev-parse", "--show-toplevel", text=True)).strip()).resolve(strict=True)
    except OSError as error:
        raise FinalError(f"cannot resolve repository root: {root}") from error
    if top != root:
        raise FinalError(f"repository root differs from Git top-level: {root}")
    return root


def current_head(root: Path) -> str:
    return _commit(str(git(root, "rev-parse", "HEAD", text=True)).strip(), "current HEAD")


def _evidence_only(path: PurePosixPath) -> bool:
    return is_under(path, EVIDENCE_PREFIX) or path in EVIDENCE_ONLY_EXACT


def validate_descendants(root: Path, tested: str) -> tuple[str, int]:
    head = current_head(root)
    result = subprocess.run(
        ["git", "-C", os.fspath(root), "merge-base", "--is-ancestor", tested, head],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        raise FinalError(f"M3 Final tested source is not an ancestor of HEAD: {tested}")
    commits_raw = str(git(root, "rev-list", "--reverse", f"{tested}..{head}", text=True)).strip()
    commits = commits_raw.splitlines() if commits_raw else []
    for commit in commits:
        ancestry = str(git(root, "rev-list", "--parents", "-n", "1", commit, text=True)).split()
        if len(ancestry) != 2:
            raise FinalError(f"evidence-only descendant contains a merge commit: {commit}")
        changed_raw = git(root, "diff-tree", "--no-commit-id", "--name-only", "-r", "-z", commit)
        assert isinstance(changed_raw, bytes)
        changed = [PurePosixPath(item.decode("utf-8")) for item in changed_raw.split(b"\0") if item]
        if not changed:
            raise FinalError(f"evidence-only descendant contains an empty commit: {commit}")
        invalid = sorted(str(path) for path in changed if not _evidence_only(path))
        if invalid:
            raise FinalError(f"non-evidence path changed after tested production source at {commit}: {invalid}")
    count = len(commits)
    return head, count


def dirty_paths(root: Path) -> set[PurePosixPath]:
    paths: set[PurePosixPath] = set()
    for args in (
        ("diff", "--name-only", "-z"),
        ("diff", "--cached", "--name-only", "-z"),
        ("ls-files", "--others", "--exclude-standard", "-z"),
    ):
        raw = git(root, *args)
        assert isinstance(raw, bytes)
        paths.update(PurePosixPath(item.decode("utf-8")) for item in raw.split(b"\0") if item)
    return paths


def validate_scenario_sync(root: Path, receipt_path: PurePosixPath) -> None:
    try:
        document = json.loads(read_safe_file(root, SCENARIO_PATH, 2_097_152))
    except json.JSONDecodeError as error:
        raise FinalError(f"cannot parse scenario manifest: {error}") from error
    rows = document.get("scenarios") if isinstance(document, dict) else None
    if not isinstance(rows, list):
        raise FinalError("scenario manifest has no scenario rows")
    scenario_ids = [row.get("id") for row in rows if isinstance(row, dict)]
    if len(scenario_ids) != len(rows) or any(not isinstance(value, str) for value in scenario_ids):
        raise FinalError("scenario manifest contains a non-object row or invalid ID")
    if len(scenario_ids) != len(set(scenario_ids)):
        raise FinalError("scenario manifest contains duplicate scenario IDs")
    by_id = {row["id"]: row for row in rows}
    borrowed = sorted(
        row["id"]
        for row in rows
        if row["id"] not in REQUIRED_SCENARIOS
        and row.get("evidenceReceipt") == str(receipt_path)
    )
    if borrowed:
        raise FinalError(
            f"scenario manifest lends the M3 Final receipt outside its exact allowlist: {borrowed}"
        )
    for scenario in REQUIRED_SCENARIOS:
        row = by_id.get(scenario)
        if (
            row is None
            or row.get("status") != "PASSED_CURRENT_SOURCE"
            or row.get("evidenceReceipt") != str(receipt_path)
        ):
            raise FinalError(f"M3 scenario status/receipt is not synchronized: {scenario}")


def validate_receipt(
    root: Path,
    receipt_path: PurePosixPath,
    expected_tested_commit: str | None = None,
    require_scenario_sync: bool = True,
) -> tuple[str, str, int]:
    root = ensure_root(root)
    receipt_path = safe_relative(str(receipt_path), "receipt", FINAL_PREFIX)
    raw = read_safe_file(root, receipt_path, MAX_CANONICAL_BYTES)
    receipt = load_canonical_json(raw, str(receipt_path))
    tested = validate_receipt_value(root, receipt, expected_tested_commit)
    head, descendants = validate_descendants(root, tested)
    invalid_dirty = sorted(str(path) for path in dirty_paths(root) if not _evidence_only(path))
    if invalid_dirty:
        raise FinalError(f"non-evidence dirty path exists during Final validation: {invalid_dirty}")
    if require_scenario_sync:
        validate_scenario_sync(root, receipt_path)
    return tested, head, descendants


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument(
        "--receipt",
        default="docs/v2/evidence/v2-m3/final/m3-final.json",
    )
    parser.add_argument("--expected-tested-commit")
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        receipt = safe_relative(args.receipt, "--receipt", FINAL_PREFIX)
        tested, head, descendants = validate_receipt(
            args.repo_root,
            receipt,
            args.expected_tested_commit,
        )
    except (FinalError, OSError) as error:
        print(f"V2 M3 Final check: {error}", file=sys.stderr)
        return 1
    print(
        "V2 M3 Final PASS: "
        f"tested={tested} head={head} evidenceOnlyDescendants={descendants} "
        f"children={len(CHILD_KINDS)} scenarios={len(REQUIRED_SCENARIOS)} "
        "realProvider=true realKms=true c2PromotionEligible=false exclusions=M6,M8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
