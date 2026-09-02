#!/usr/bin/env python3
"""Fail-closed validator for exact-source Nereus V2 M4 child and Final evidence."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
from typing import Any
import xml.etree.ElementTree as ET


CHILD_SCHEMA = "NEREUS_V2_M4_CHILD_EVIDENCE_V1"
FINAL_SCHEMA = "NEREUS_V2_M4_FINAL_V1"
FINAL_KIND = "V2_M4_FINAL"
FINAL_RESULT = "PASS_V2_M4_FINAL"
JUNIT_SCHEMA = "NEREUS_V2_M4_GOVERNED_JUNIT_V1"
FACT_SCHEMA = "NEREUS_V2_M4_EVIDENCE_FACTS_V1"
SOURCE_BINDING_SCHEMA = "NEREUS_V2_M4_EVIDENCE_SOURCE_LOCKS_V1"
MAX_CANONICAL_BYTES = 2_097_152
MAX_XML_BYTES = 1_048_576
MAX_TOTAL_BYTES = 16_777_216
MAX_SAFE_INTEGER = 9_007_199_254_740_991
EVIDENCE_PREFIX = PurePosixPath("docs/v2/evidence/v2-m4")
CHILD_PREFIX = EVIDENCE_PREFIX / "children"
FINAL_PREFIX = EVIDENCE_PREFIX / "final"
SOURCE_LOCKS_PATH = PurePosixPath("docs/v2/source-locks.json")
SCENARIO_PATH = PurePosixPath("docs/v2/v2-scenarios.json")
M3_FINAL_PATH = PurePosixPath(
    "docs/v2/evidence/v2-m3/final/e5e53e62865c21845621037bea5f18c092bd4259/m3-final.json"
)
M3_TESTED_COMMIT = "e5e53e62865c21845621037bea5f18c092bd4259"
M3_CLOSURE_COMMIT = "efab430aed37b3f7c32d09b88ae935c1aea1c902"
M3_FINAL_SHA256 = "81c7004a923e5b96cab0a3c8b4f1fa26d71606a2208bbabe779f0d872f84f84a"
M3_SOURCE_LOCKS_SHA256 = "2a46f31c90912f3f3f10d2365b9f9ffcc8070a847c4c7e202177ea903cdc240b"

CHILD_RESULTS = {
    "READ_VIEW_HAZARD": "PASS_READ_VIEW_HAZARD_ONLY",
    "SOURCE_PLAN_EXECUTION": "PASS_SOURCE_PLAN_EXECUTION_ONLY",
    "QUIESCENCE_PROTECTION_RELEASE": "PASS_QUIESCENCE_PROTECTION_RELEASE_ONLY",
    "CURRENT_SOURCE_INTEGRATION_PERFORMANCE": "PASS_CURRENT_SOURCE_INTEGRATION_PERFORMANCE_ONLY",
}
CHILD_KINDS = tuple(CHILD_RESULTS)
ATTACHMENTS = {
    "READ_VIEW_HAZARD": ("JUNIT_SUMMARY", "HOT_PATH_MEASUREMENT"),
    "SOURCE_PLAN_EXECUTION": ("JUNIT_SUMMARY", "SOURCE_PLAN_MATRIX"),
    "QUIESCENCE_PROTECTION_RELEASE": (
        "JUNIT_SUMMARY",
        "CONTROL_PHYSICAL_SELECTION",
        "BACKEND_ADMISSION",
    ),
    "CURRENT_SOURCE_INTEGRATION_PERFORMANCE": (
        "JUNIT_SUMMARY",
        "CURRENT_SOURCE_RESULT",
    ),
}
CHILD_EXCLUSIONS = [
    "M4_FINAL_AGGREGATE",
    "M5_PHYSICAL_DELETION",
    "M6_PROCESS_ACTIVATION",
    "M8_NATIVE_PARITY",
    "PRODUCTION_DEPLOYMENT_AUTHORITY",
    "SCENARIO_PROMOTION",
]
FINAL_EXCLUSIONS = [
    "M5_PHYSICAL_DELETION",
    "M6_PROCESS_ACTIVATION",
    "M8_NATIVE_PARITY",
    "PRODUCTION_DEPLOYMENT_AUTHORITY",
]
PROMOTED_SCENARIOS = (
    "V2-READ-001",
    "V2-READ-003",
    "V2-READ-004",
    "V2-READ-005",
    "V2-READ-007",
)
SHARED_PREDICATES = (
    "V2-READ-006",
    "V2-READ-008",
    "V2-READ-009",
    "V2-READ-010",
    "V2-READ-011",
    "V2-READ-012",
    "V2-READ-013",
    "V2-READ-014",
    "V2-READ-015",
)
PHYSICAL_SELECTION = {
    "emergencyStoppedReserveBytes": 2048,
    "maxActiveBatches": 8,
    "maxPendingAnchors": 8,
    "maxProofFolds": 64,
    "maxProofIntervalEpochs": 4096,
    "maxSourcesPerBatch": 64,
    "ordinaryReadRemoteMetadataOperations": 0,
    "proofFoldEntries": 32,
    "proofWindowEntries": 64,
    "selectorMaxBytes": 32768,
    "warmedCapturePlanClearAllocatedBytes": 0,
    "warmedCapturePlanClearOperations": 100000,
}
EXPECTED_TESTS = {
    "READ_VIEW_HAZARD": {
        "capturedGenerationRemainsPinnedThroughProviderAndBufferDrain",
        "authoritySwitchRejectsOldAdmissionAndNewCaptureUsesSuccessorGeneration",
        "stoppedAuthorityAndExhaustedPoolFailBeforeSourceUse",
        "multiBindingReservationReleasesEveryPartialLease",
        "lifecycleRejectsCrossThreadMutation",
        "plannedPoolCloseRejectsNewAdmissionWithoutForceClearingLiveLease",
        "leaseGenerationWrapRetiresSlotInsteadOfReusingAbaIdentity",
        "scanTreatsClaimedLeaseWithUnpublishedPayloadAsInconclusive",
        "asyncCancellationClosesGateButRetainsLeaseUntilRealProviderCompletion",
        "asyncSuccessReleasesExactLeaseBeforeMakingHeapOwnedResultObservable",
        "steadyCapturePlanAndClearAllocateNoHeapBytesOnCurrentThread",
    },
    "SOURCE_PLAN_EXECUTION": {
        "deterministicPlanRejectsGapsAndCapacityWithoutRepairReads",
        "pulsarPlannerPreservesTypedVirtualLedgerAndNeverFlattensLargePositions",
        "fallbackIsSingleTransferPreObservabilityAndPreservesPrimaryCause",
        "completedEarlierIntervalDoesNotCloseBatchOrForbidLaterIntervalFallback",
        "observabilityForbidsFallbackAndUnprovedTerminationQuarantinesLease",
        "sourceRouteRejectsNotEligibleFallbackAndSemanticMismatch",
    },
    "QUIESCENCE_PROTECTION_RELEASE": {
        "codecsRoundTripAllClosedRecordsAndRejectNonCanonicalBytes",
        "fusedClosureAtomicallyFreezesBatchClosesEpochAndGrantsPreferredOnlySuccessor",
        "takeoverFallbackIntroductionAndMembershipNeutralUpdateShareExactSelectorCas",
        "anchorCapacityClosesAdmissionIntoReservedStoppedEnvelopeAndFreshEpochCanResumeAfterPrune",
        "terminalCreateAdoptsDifferentValidVariantAndQuarantinesInvalidOccupant",
        "exactProofAndHazardDrainGateIrreversibleProtectionReleaseAndResponseLoss",
        "localAdmissionClosesBeforeUnknownSelectorResponseAndOldCapturedGenerationSurvives",
        "proofIntervalUsesEachHistoricalCapabilityAndRevocationFailsSafe",
        "proofHeadFoldsBoundedContiguousEntriesAndRejectsAGap",
        "acceptsOnlyTheClosedCellShardControlFamilies",
        "putIfAbsentConvergesExactResponseLossAndClosesConflicts",
        "compareAndSetMapsExactExpectedBytesToTheOxiaVersionFence",
        "compareAndSetResponseLossUsesOnlyTheExactSameKeyReread",
        "exactAbsenceCasUsesConditionalCreateAndNeverOverwrites",
        "readFailureIsNeverReportedAsAbsenceAndMutationRereadFailureIsUnknown",
        "mapsTheVersionReturnedByTheExactPreReadRatherThanGuessingARevision",
        "identicalBytesCannotHideAnOxiaVersionAbaBetweenPreReadAndCas",
    },
    "CURRENT_SOURCE_INTEGRATION_PERFORMANCE": {
        "m4CurrentSourceReadPinsExactM3LocatorUntilProviderAndOuterLeaseDrain",
        "m4CapturedActiveSourceSurvivesManifestSwitchAndNewReadUsesManifest",
        "m4OuterHazardAloneClosesCaptureToP4InnerPinRetirementRace",
    },
}
EXPECTED_SUITES = {
    "READ_VIEW_HAZARD": {
        ":nereus-storage-object:v2M4ReadViewHazardEvidenceTest":
            "com.nereusstream.storage.object.read.BindingReadM4KernelV1Test",
    },
    "SOURCE_PLAN_EXECUTION": {
        ":nereus-storage-object:v2M4SourcePlanExecutionEvidenceTest":
            "com.nereusstream.storage.object.read.BindingReadM4KernelV1Test",
    },
    "QUIESCENCE_PROTECTION_RELEASE": {
        ":nereus-storage-object:v2M4QuiescenceProtectionReleaseEvidenceTest":
            "com.nereusstream.storage.object.read.control.M4ReadControlCoordinatorV1Test",
        ":nereus-metadata-oxia:v2M4ReadControlOxiaAdapterTest":
            "com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStoreTest",
    },
    "CURRENT_SOURCE_INTEGRATION_PERFORMANCE": {
        ":nereus-kafka-bookkeeper:v2M4CurrentSourceKafkaTest":
            "com.nereusstream.kafka.bookkeeper.object.publication.KafkaObjectPublicationBridgeV1Test",
        ":nereus-pulsar-offload:v2M4CurrentSourcePulsarTest":
            "com.nereusstream.pulsar.offload.objectwal.PulsarObjectWalBridgeV1Test",
    },
}
EXPECTED_SUITE_TESTS = {
    "READ_VIEW_HAZARD": {
        ":nereus-storage-object:v2M4ReadViewHazardEvidenceTest": EXPECTED_TESTS["READ_VIEW_HAZARD"],
    },
    "SOURCE_PLAN_EXECUTION": {
        ":nereus-storage-object:v2M4SourcePlanExecutionEvidenceTest": EXPECTED_TESTS["SOURCE_PLAN_EXECUTION"],
    },
    "QUIESCENCE_PROTECTION_RELEASE": {
        ":nereus-storage-object:v2M4QuiescenceProtectionReleaseEvidenceTest": {
            "codecsRoundTripAllClosedRecordsAndRejectNonCanonicalBytes",
            "fusedClosureAtomicallyFreezesBatchClosesEpochAndGrantsPreferredOnlySuccessor",
            "takeoverFallbackIntroductionAndMembershipNeutralUpdateShareExactSelectorCas",
            "anchorCapacityClosesAdmissionIntoReservedStoppedEnvelopeAndFreshEpochCanResumeAfterPrune",
            "terminalCreateAdoptsDifferentValidVariantAndQuarantinesInvalidOccupant",
            "exactProofAndHazardDrainGateIrreversibleProtectionReleaseAndResponseLoss",
            "localAdmissionClosesBeforeUnknownSelectorResponseAndOldCapturedGenerationSurvives",
            "proofIntervalUsesEachHistoricalCapabilityAndRevocationFailsSafe",
            "proofHeadFoldsBoundedContiguousEntriesAndRejectsAGap",
        },
        ":nereus-metadata-oxia:v2M4ReadControlOxiaAdapterTest": {
            "acceptsOnlyTheClosedCellShardControlFamilies",
            "putIfAbsentConvergesExactResponseLossAndClosesConflicts",
            "compareAndSetMapsExactExpectedBytesToTheOxiaVersionFence",
            "compareAndSetResponseLossUsesOnlyTheExactSameKeyReread",
            "exactAbsenceCasUsesConditionalCreateAndNeverOverwrites",
            "readFailureIsNeverReportedAsAbsenceAndMutationRereadFailureIsUnknown",
            "mapsTheVersionReturnedByTheExactPreReadRatherThanGuessingARevision",
            "identicalBytesCannotHideAnOxiaVersionAbaBetweenPreReadAndCas",
        },
    },
    "CURRENT_SOURCE_INTEGRATION_PERFORMANCE": {
        ":nereus-kafka-bookkeeper:v2M4CurrentSourceKafkaTest": {
            "m4CurrentSourceReadPinsExactM3LocatorUntilProviderAndOuterLeaseDrain",
        },
        ":nereus-pulsar-offload:v2M4CurrentSourcePulsarTest": {
            "m4CapturedActiveSourceSurvivesManifestSwitchAndNewReadUsesManifest",
            "m4OuterHazardAloneClosesCaptureToP4InnerPinRetirementRace",
        },
    },
}
COMMIT_RE = re.compile(r"[0-9a-f]{40}")
SHA_RE = re.compile(r"[0-9a-f]{64}")


class EvidenceError(RuntimeError):
    """Stable fail-closed M4 evidence rejection."""


def canonical_bytes(value: Any) -> bytes:
    validate_jcs_domain(value, "root")
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()


def validate_jcs_domain(value: Any, label: str) -> None:
    if value is None or type(value) is bool:
        return
    if type(value) is int:
        if abs(value) > MAX_SAFE_INTEGER:
            raise EvidenceError(f"integer outside JCS-safe domain: {label}")
        return
    if isinstance(value, str):
        if not value.isascii():
            raise EvidenceError(f"non-ASCII string outside closed domain: {label}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            validate_jcs_domain(item, f"{label}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key.isascii():
                raise EvidenceError(f"non-ASCII key outside closed domain: {label}")
            validate_jcs_domain(item, f"{label}.{key}")
        return
    raise EvidenceError(f"unsupported closed-domain value: {label}")


def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceError(f"duplicate JSON member: {key}")
        result[key] = value
    return result


def load_canonical(raw: bytes, label: str, maximum: int = MAX_CANONICAL_BYTES) -> dict[str, Any]:
    if not raw or len(raw) > maximum:
        raise EvidenceError(f"canonical JSON bytes outside cap: {label}")
    try:
        value = json.loads(raw, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EvidenceError(f"cannot parse JSON {label}: {error}") from error
    if not isinstance(value, dict) or canonical_bytes(value) != raw:
        raise EvidenceError(f"JSON is not an exact canonical object: {label}")
    return value


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def exact(value: object, members: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != members:
        actual = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise EvidenceError(f"{label} member set differs: {actual}")
    return value


def positive(value: object, label: str) -> int:
    if type(value) is not int or value <= 0 or value > MAX_SAFE_INTEGER:
        raise EvidenceError(f"{label} must be a positive safe integer")
    return value


def zero(value: object, label: str) -> int:
    if type(value) is not int or value != 0:
        raise EvidenceError(f"{label} must be zero")
    return value


def commit(value: object, label: str) -> str:
    if not isinstance(value, str) or not COMMIT_RE.fullmatch(value):
        raise EvidenceError(f"{label} is not a canonical commit")
    return value


def digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA_RE.fullmatch(value):
        raise EvidenceError(f"{label} is not a canonical SHA-256")
    return value


def is_under(path: PurePosixPath, parent: PurePosixPath) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def safe_relative(value: object, label: str, prefix: PurePosixPath = EVIDENCE_PREFIX) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise EvidenceError(f"{label} is not a safe POSIX-relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in ("", ".", "..") for part in value.split("/")) or not is_under(path, prefix):
        raise EvidenceError(f"{label} is outside {prefix}")
    return path


def read_safe(root: Path, relative: PurePosixPath, maximum: int = MAX_CANONICAL_BYTES) -> bytes:
    current = root
    mode = 0
    for part in relative.parts:
        current /= part
        try:
            mode = current.lstat().st_mode
        except OSError as error:
            raise EvidenceError(f"evidence file is missing: {relative}") from error
        if stat.S_ISLNK(mode):
            raise EvidenceError(f"evidence path contains a symlink: {relative}")
    if not stat.S_ISREG(mode) or not 0 < current.stat().st_size <= maximum:
        raise EvidenceError(f"evidence file type/bytes invalid: {relative}")
    return current.read_bytes()


def git(root: Path, *args: str, text: bool = False) -> bytes | str:
    try:
        return subprocess.check_output(["git", "-C", os.fspath(root), *args], text=text, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as error:
        output = error.output.strip() if isinstance(error.output, str) else error.output.decode(errors="replace").strip()
        raise EvidenceError(f"git {' '.join(args)} failed: {output}") from error


def ensure_root(root: Path) -> Path:
    resolved = root.resolve()
    if str(git(resolved, "rev-parse", "--show-toplevel", text=True)).strip() != os.fspath(resolved):
        raise EvidenceError("--repo-root is not the Git worktree root")
    return resolved


def git_blob(root: Path, source: str, path: PurePosixPath) -> bytes:
    value = git(root, "show", f"{source}:{path}")
    assert isinstance(value, bytes)
    return value


def validate_frozen_m3_ancestry(root: Path, tested: str) -> None:
    for ancestor in (M3_TESTED_COMMIT, M3_CLOSURE_COMMIT):
        if subprocess.run(
            ["git", "-C", os.fspath(root), "merge-base", "--is-ancestor", ancestor, tested],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode != 0:
            raise EvidenceError("M4 tested source does not descend from the frozen M3 closure")


def source_bindings(root: Path, tested: str, expected_sha: str) -> dict[str, Any]:
    raw = git_blob(root, tested, SOURCE_LOCKS_PATH)
    if sha256(raw) != expected_sha:
        raise EvidenceError("source-lock SHA differs from exact tested source")
    try:
        locks = json.loads(raw, object_pairs_hook=reject_duplicates)
    except json.JSONDecodeError as error:
        raise EvidenceError(f"tested source locks cannot be parsed: {error}") from error
    value = exact(
        locks.get("m4EvidenceBindings"),
        {"backendAdmissions", "exclusions", "nonAdmittedCapabilities", "physicalSelection", "receiptSchema", "schema"},
        "m4EvidenceBindings",
    )
    if value["schema"] != SOURCE_BINDING_SCHEMA or value["receiptSchema"] != CHILD_SCHEMA:
        raise EvidenceError("M4 source binding schema differs")
    if value["physicalSelection"] != PHYSICAL_SELECTION:
        raise EvidenceError("M4 physical selection differs from the closed evidence contract")
    admissions = value["backendAdmissions"]
    if not isinstance(admissions, list) or len(admissions) != 3:
        raise EvidenceError("M4 backend admission inventory differs")
    names = [row.get("backend") for row in admissions if isinstance(row, dict)]
    if names != ["KAFKA_BOOKKEEPER_OBJECT_WAL", "PULSAR_OBJECT_WAL", "OXIA_CANONICAL_CONTROL_METADATA"]:
        raise EvidenceError("M4 backend admission order/identity differs")
    expected_identities = [
        "NEREUS_TESTED_COMMIT|kafkaClients=org.apache.kafka:kafka-clients:3.9.0|bookkeeper=org.apache.bookkeeper:bookkeeper-server:4.18.0",
        "PULSAR_SOURCE_COMPOSITE|repository=https://github.com/nereusstream/pulsar.git|branch=nereus/v2-m2-pulsar-native-offload|commit=a14e0e6f4e49be0677318b4ceefc7b85b445823b",
        "OXIA_CLIENT_LOCKED_ARTIFACT|commit=091a42c2780d92da56e9ec1f02ce1c3d988adc16|jarSha256=0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5",
    ]
    for index, row in enumerate(admissions):
        exact(row, {"admissionGeneration", "backend", "capabilityKind", "productionDeploymentAuthority", "sourceIdentity"}, f"backendAdmissions[{index}]")
        positive(row["admissionGeneration"], f"backendAdmissions[{index}].admissionGeneration")
        if row["capabilityKind"] != "DURABLE_DRAIN_ONLY_V1" or row["productionDeploymentAuthority"] is not False:
            raise EvidenceError("backend admission overclaims capability/deployment authority")
        if row["sourceIdentity"] != expected_identities[index]:
            raise EvidenceError("backend admission source identity differs")
    if value["nonAdmittedCapabilities"] != ["AUTHORITY_EXPIRY_V1"]:
        raise EvidenceError("M4 non-admitted capability inventory differs")
    if value["exclusions"] != ["M5_PHYSICAL_DELETION", "M6_PROCESS_ACTIVATION", "M8_NATIVE_PARITY", "PRODUCTION_DEPLOYMENT_AUTHORITY"]:
        raise EvidenceError("M4 source binding exclusions differ")
    return value


def validate_junit(value: object, kind: str, tested: str) -> dict[str, int]:
    doc = exact(value, {"childKind", "schema", "suites", "testedCommit"}, "JUNIT_SUMMARY")
    if doc["schema"] != JUNIT_SCHEMA or doc["childKind"] != kind or doc["testedCommit"] != tested:
        raise EvidenceError(f"JUNIT_SUMMARY identity differs: {kind}")
    suites = doc["suites"]
    if not isinstance(suites, list) or not suites:
        raise EvidenceError(f"JUNIT_SUMMARY has no suites: {kind}")
    names: set[str] = set()
    tests = failures = errors = skipped = duration = 0
    tasks: list[str] = []
    expected_suites = EXPECTED_SUITES[kind]
    for index, raw_suite in enumerate(suites):
        suite = exact(raw_suite, {"bytes", "task", "xmlBase64", "xmlSha256"}, f"suite[{index}]")
        positive(suite["bytes"], f"suite[{index}].bytes")
        digest(suite["xmlSha256"], f"suite[{index}].xmlSha256")
        if not isinstance(suite["task"], str) or not suite["task"].isascii() or suite["task"] in tasks:
            raise EvidenceError(f"suite task invalid or duplicate: {kind}")
        tasks.append(suite["task"])
        try:
            xml_raw = base64.b64decode(suite["xmlBase64"], validate=True)
        except (ValueError, TypeError) as error:
            raise EvidenceError(f"suite XML base64 invalid: {kind}") from error
        if len(xml_raw) != suite["bytes"] or len(xml_raw) > MAX_XML_BYTES or sha256(xml_raw) != suite["xmlSha256"]:
            raise EvidenceError(f"suite XML bytes/SHA differ: {kind}")
        try:
            root = ET.fromstring(xml_raw)
        except ET.ParseError as error:
            raise EvidenceError(f"suite XML invalid: {kind}: {error}") from error
        if root.tag != "testsuite":
            raise EvidenceError(f"suite XML root differs: {kind}")
        expected_class = expected_suites.get(suite["task"])
        if expected_class is None or root.attrib.get("name") != expected_class:
            raise EvidenceError(f"suite task/class identity differs: {kind}/{suite['task']}")
        try:
            suite_tests = int(root.attrib["tests"])
            suite_failures = int(root.attrib["failures"])
            suite_errors = int(root.attrib["errors"])
            suite_skipped = int(root.attrib["skipped"])
        except (KeyError, ValueError) as error:
            raise EvidenceError(f"suite counters invalid: {kind}") from error
        if min(suite_tests, suite_failures, suite_errors, suite_skipped) < 0:
            raise EvidenceError(f"suite counters negative: {kind}")
        suite_names: set[str] = set()
        for case in root.findall("testcase"):
            name = case.attrib.get("name", "").removesuffix("()")
            if not name or name in names or name in suite_names or case.attrib.get("classname") != expected_class:
                raise EvidenceError(f"test case missing or duplicated: {kind}/{name}")
            suite_names.add(name)
            names.add(name)
            try:
                duration += max(0, int(float(case.attrib.get("time", "0")) * 1_000_000))
            except ValueError as error:
                raise EvidenceError(f"test duration invalid: {kind}/{name}") from error
        tests += suite_tests
        failures += suite_failures
        errors += suite_errors
        skipped += suite_skipped
        if suite_names != EXPECTED_SUITE_TESTS[kind][suite["task"]]:
            raise EvidenceError(f"suite exact test inventory differs: {kind}/{suite['task']}")
    if set(tasks) != set(expected_suites) or names != EXPECTED_TESTS[kind] or tests != len(names):
        raise EvidenceError(f"JUNIT_SUMMARY exact test inventory differs: {kind}")
    if failures or errors or skipped:
        raise EvidenceError(f"JUNIT_SUMMARY is not a zero-failure/zero-skip run: {kind}")
    return {"durationMicros": duration, "errors": errors, "failures": failures, "skipped": skipped, "tests": tests}


def validate_fact(value: object, attachment_kind: str, child_kind: str, tested: str, bindings: dict[str, Any], summary: dict[str, int]) -> None:
    doc = exact(value, {"childKind", "evidenceKind", "facts", "result", "schema", "testedCommit"}, attachment_kind)
    if (
        doc["schema"] != FACT_SCHEMA
        or doc["childKind"] != child_kind
        or doc["evidenceKind"] != attachment_kind
        or doc["testedCommit"] != tested
        or doc["result"] != "PASS_EVIDENCE_ONLY"
    ):
        raise EvidenceError(f"fact attachment identity differs: {child_kind}/{attachment_kind}")
    facts = doc["facts"]
    if attachment_kind == "HOT_PATH_MEASUREMENT":
        expected = {
            "allocatedBytes": 0,
            "durationMicros": summary["durationMicros"],
            "operations": 100000,
            "ordinaryReadRemoteMetadataOperations": 0,
            "zeroAllocationAssertion": "steadyCapturePlanAndClearAllocateNoHeapBytesOnCurrentThread",
        }
    elif attachment_kind == "SOURCE_PLAN_MATRIX":
        expected = {
            "failurePrecedence": "PRIMARY_CAUSE_PRESERVED",
            "fallbackTransfers": 1,
            "kafkaPositionDomain": "SIGNED_LONG_OFFSET",
            "pulsarPositionDomain": "VIRTUAL_LEDGER_ID_AND_ENTRY_ID",
            "purity": ["ATOMIC_APPEND_UNIT", "DECLARED_WHOLE_RANGE"],
            "repairMetadataReads": 0,
            "routeOrder": "POSITION_ASCENDING",
        }
    elif attachment_kind == "CONTROL_PHYSICAL_SELECTION":
        expected = bindings["physicalSelection"]
    elif attachment_kind == "BACKEND_ADMISSION":
        expected = {
            "admissions": bindings["backendAdmissions"],
            "nonAdmittedCapabilities": bindings["nonAdmittedCapabilities"],
            "ordinaryReadRemoteMetadataOperations": 0,
        }
    elif attachment_kind == "CURRENT_SOURCE_RESULT":
        expected = {
            "affectedHistoricalM3Seam": True,
            "frozenM3FinalSha256": M3_FINAL_SHA256,
            "kafkaTests": 1,
            "pulsarSourceCommit": "a14e0e6f4e49be0677318b4ceefc7b85b445823b",
            "pulsarTests": 2,
            "totalDurationMicros": summary["durationMicros"],
        }
    else:
        raise EvidenceError(f"unknown fact attachment: {attachment_kind}")
    if facts != expected:
        raise EvidenceError(f"fact attachment values differ: {child_kind}/{attachment_kind}")


def validate_child_value(root: Path, receipt: object, expected_kind: str | None = None, expected_tested: str | None = None) -> tuple[str, str, dict[str, Any]]:
    doc = exact(receipt, {"attachments", "exclusions", "kind", "promotionEligible", "result", "schema", "sourceTuple", "testSummary"}, "M4 child")
    kind = doc["kind"]
    if kind not in CHILD_RESULTS or (expected_kind is not None and kind != expected_kind):
        raise EvidenceError(f"M4 child kind differs: {kind}")
    if doc["schema"] != CHILD_SCHEMA or doc["result"] != CHILD_RESULTS[kind] or doc["promotionEligible"] is not False:
        raise EvidenceError(f"M4 child schema/result/promotion differs: {kind}")
    if doc["exclusions"] != CHILD_EXCLUSIONS:
        raise EvidenceError(f"M4 child exclusions differ: {kind}")
    source = exact(doc["sourceTuple"], {"nereusCommit", "sourceLocksSha256"}, f"{kind}.sourceTuple")
    tested = commit(source["nereusCommit"], f"{kind}.nereusCommit")
    if expected_tested is not None and tested != expected_tested:
        raise EvidenceError(f"M4 child tested commit differs: {kind}")
    source_sha = digest(source["sourceLocksSha256"], f"{kind}.sourceLocksSha256")
    bindings = source_bindings(root, tested, source_sha)
    attachments = doc["attachments"]
    expected_attachments = ATTACHMENTS[kind]
    if not isinstance(attachments, list) or tuple(row.get("kind") for row in attachments if isinstance(row, dict)) != expected_attachments:
        raise EvidenceError(f"M4 child attachment inventory differs: {kind}")
    seen: set[str] = set()
    junit_summary: dict[str, int] | None = None
    facts: list[tuple[str, dict[str, Any]]] = []
    total = 0
    ordinal = CHILD_KINDS.index(kind) + 1
    expected_parent = CHILD_PREFIX / f"final-source-{tested}" / f"{ordinal:02d}-{kind}" / "attachments"
    for index, raw_row in enumerate(attachments):
        row = exact(raw_row, {"bytes", "kind", "path", "sha256"}, f"{kind}.attachments[{index}]")
        positive(row["bytes"], f"{kind}.attachments[{index}].bytes")
        digest(row["sha256"], f"{kind}.attachments[{index}].sha256")
        path = safe_relative(row["path"], f"{kind}.attachments[{index}].path", CHILD_PREFIX)
        expected_path = expected_parent / f"{index:02d}-{row['kind']}.json"
        if path != expected_path:
            raise EvidenceError(f"M4 child attachment path differs: {kind}/{row['kind']}")
        if str(path) in seen:
            raise EvidenceError(f"M4 child attachment path duplicated: {kind}")
        seen.add(str(path))
        raw = read_safe(root, path)
        if len(raw) != row["bytes"] or sha256(raw) != row["sha256"]:
            raise EvidenceError(f"M4 child attachment bytes/SHA differ: {path}")
        value = load_canonical(raw, str(path))
        if row["kind"] == "JUNIT_SUMMARY":
            junit_summary = validate_junit(value, kind, tested)
        else:
            facts.append((row["kind"], value))
        total += len(raw)
    if junit_summary is None:
        raise EvidenceError(f"M4 child lacks governed JUnit summary: {kind}")
    summary = exact(doc["testSummary"], {"durationMicros", "errors", "failures", "skipped", "tests"}, f"{kind}.testSummary")
    if summary != junit_summary:
        raise EvidenceError(f"M4 child test summary differs from governed JUnit: {kind}")
    for attachment_kind, value in facts:
        validate_fact(value, attachment_kind, kind, tested, bindings, summary)
    if total > MAX_TOTAL_BYTES:
        raise EvidenceError(f"M4 child evidence exceeds total cap: {kind}")
    return kind, tested, bindings


def validate_child(root: Path, path: PurePosixPath, expected_kind: str | None = None, expected_tested: str | None = None) -> tuple[str, str, dict[str, Any]]:
    root = ensure_root(root)
    path = safe_relative(str(path), "child receipt", CHILD_PREFIX)
    return validate_child_value(root, load_canonical(read_safe(root, path), str(path)), expected_kind, expected_tested)


def child_identity(root: Path, path: PurePosixPath, expected_kind: str, tested: str) -> dict[str, Any]:
    raw = read_safe(root, path)
    value = load_canonical(raw, str(path))
    kind, _, _ = validate_child_value(root, value, expected_kind, tested)
    summary = value["testSummary"]
    return {
        "bytes": len(raw),
        "durationMicros": summary["durationMicros"],
        "kind": kind,
        "path": str(path),
        "result": value["result"],
        "sha256": sha256(raw),
        "tests": summary["tests"],
    }


def validate_final_value(root: Path, value: object, expected_tested: str | None = None) -> str:
    doc = exact(
        value,
        {"backendAdmissions", "childReceipts", "exclusions", "frozenM3", "kind", "physicalSelection", "promotionEligible", "result", "scenarios", "schema", "sharedPredicates", "sourceTuple"},
        "M4 Final",
    )
    if doc["schema"] != FINAL_SCHEMA or doc["kind"] != FINAL_KIND or doc["result"] != FINAL_RESULT or doc["promotionEligible"] is not True:
        raise EvidenceError("M4 Final schema/kind/result/promotion differs")
    source = exact(doc["sourceTuple"], {"nereusCommit", "sourceLocksSha256"}, "M4 Final sourceTuple")
    tested = commit(source["nereusCommit"], "M4 Final tested commit")
    if expected_tested is not None and tested != expected_tested:
        raise EvidenceError("M4 Final tested commit differs from expected")
    validate_frozen_m3_ancestry(root, tested)
    bindings = source_bindings(root, tested, digest(source["sourceLocksSha256"], "M4 Final source lock SHA"))
    frozen = {
        "closureCommit": M3_CLOSURE_COMMIT,
        "finalPath": str(M3_FINAL_PATH),
        "finalSha256": M3_FINAL_SHA256,
        "sourceLocksSha256": M3_SOURCE_LOCKS_SHA256,
        "testedCommit": M3_TESTED_COMMIT,
    }
    if doc["frozenM3"] != frozen:
        raise EvidenceError("M4 Final frozen M3 identity differs")
    if sha256(read_safe(root, M3_FINAL_PATH)) != M3_FINAL_SHA256:
        raise EvidenceError("M4 Final historical M3 bytes differ")
    if doc["physicalSelection"] != bindings["physicalSelection"] or doc["backendAdmissions"] != bindings["backendAdmissions"]:
        raise EvidenceError("M4 Final physical/backend selection differs from tested source locks")
    if tuple(doc["scenarios"]) != PROMOTED_SCENARIOS or tuple(doc["sharedPredicates"]) != SHARED_PREDICATES:
        raise EvidenceError("M4 Final scenario or shared-predicate inventory differs")
    if doc["exclusions"] != FINAL_EXCLUSIONS:
        raise EvidenceError("M4 Final exclusions differ")
    children = doc["childReceipts"]
    if not isinstance(children, list) or len(children) != len(CHILD_KINDS):
        raise EvidenceError("M4 Final child count differs")
    paths: set[str] = set()
    for expected_kind, raw_row in zip(CHILD_KINDS, children, strict=True):
        row = exact(raw_row, {"bytes", "durationMicros", "kind", "path", "result", "sha256", "tests"}, f"Final child {expected_kind}")
        if row["kind"] != expected_kind or row["result"] != CHILD_RESULTS[expected_kind]:
            raise EvidenceError(f"M4 Final child identity differs: {expected_kind}")
        positive(row["bytes"], f"{expected_kind}.bytes")
        positive(row["tests"], f"{expected_kind}.tests")
        if type(row["durationMicros"]) is not int or row["durationMicros"] < 0:
            raise EvidenceError(f"{expected_kind}.durationMicros invalid")
        digest(row["sha256"], f"{expected_kind}.sha256")
        path = safe_relative(row["path"], f"{expected_kind}.path", CHILD_PREFIX)
        if str(path) in paths:
            raise EvidenceError("M4 Final child path duplicated")
        paths.add(str(path))
        derived = child_identity(root, path, expected_kind, tested)
        if row != derived:
            raise EvidenceError(f"M4 Final child row is not exactly derived: {expected_kind}")
    return tested


def evidence_only(path: PurePosixPath) -> bool:
    return is_under(path, EVIDENCE_PREFIX) or path in {
        PurePosixPath("docs/v2/08-implementation-plan-and-gates.md"),
        PurePosixPath("docs/v2/09-scenario-evidence-matrix.md"),
        PurePosixPath("docs/v2/README.md"),
        PurePosixPath("docs/v2/detailed_design/m4/README.md"),
        PurePosixPath("docs/v2/detailed_design/m4/m4-implementation-log.md"),
        PurePosixPath("docs/v2/open-questions.md"),
        SCENARIO_PATH,
    }


def validate_descendants(root: Path, tested: str) -> tuple[str, int]:
    head = str(git(root, "rev-parse", "HEAD", text=True)).strip()
    ancestry = subprocess.run(["git", "-C", os.fspath(root), "merge-base", "--is-ancestor", tested, head])
    if ancestry.returncode != 0:
        raise EvidenceError("M4 tested commit is not an ancestor of HEAD")
    commits_raw = str(git(root, "rev-list", "--reverse", f"{tested}..{head}", text=True)).strip()
    commits = commits_raw.splitlines() if commits_raw else []
    for descendant in commits:
        parents = str(git(root, "rev-list", "--parents", "-n", "1", descendant, text=True)).split()
        if len(parents) != 2:
            raise EvidenceError(f"M4 evidence descendant is a merge commit: {descendant}")
        raw = git(root, "diff-tree", "--no-commit-id", "--name-only", "-r", "-z", descendant)
        assert isinstance(raw, bytes)
        changed = [PurePosixPath(item.decode()) for item in raw.split(b"\0") if item]
        invalid = sorted(str(path) for path in changed if not evidence_only(path))
        if not changed or invalid:
            raise EvidenceError(f"non-evidence change after M4 tested source at {descendant}: {invalid}")
    return head, len(commits)


def dirty_paths(root: Path) -> set[PurePosixPath]:
    result: set[PurePosixPath] = set()
    for args in (("diff", "--name-only", "-z"), ("diff", "--cached", "--name-only", "-z"), ("ls-files", "--others", "--exclude-standard", "-z")):
        raw = git(root, *args)
        assert isinstance(raw, bytes)
        result.update(PurePosixPath(item.decode()) for item in raw.split(b"\0") if item)
    return result


def validate_scenarios(root: Path, final_path: PurePosixPath) -> None:
    try:
        doc = json.loads((root / SCENARIO_PATH).read_bytes(), object_pairs_hook=reject_duplicates)
    except json.JSONDecodeError as error:
        raise EvidenceError(f"scenario registry cannot be parsed: {error}") from error
    rows = doc.get("scenarios") if isinstance(doc, dict) else None
    if not isinstance(rows, list):
        raise EvidenceError("scenario registry has no rows")
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str) or row["id"] in by_id:
            raise EvidenceError("scenario registry contains malformed/duplicate row")
        by_id[row["id"]] = row
    borrowed = sorted(row["id"] for row in rows if row.get("evidenceReceipt") == str(final_path) and row["id"] not in PROMOTED_SCENARIOS)
    if borrowed:
        raise EvidenceError(f"M4 Final receipt borrowed outside allowlist: {borrowed}")
    for scenario in PROMOTED_SCENARIOS:
        row = by_id.get(scenario)
        if row is None or row.get("status") != "PASSED_CURRENT_SOURCE" or row.get("evidenceReceipt") != str(final_path):
            raise EvidenceError(f"M4 promoted scenario is not synchronized: {scenario}")
    for scenario in ("V2-READ-002", *SHARED_PREDICATES):
        row = by_id.get(scenario)
        if row is None or row.get("status") != "PLANNED" or row.get("evidenceReceipt") is not None:
            raise EvidenceError(f"M4 non-promotable scenario was promoted: {scenario}")


def validate_final(root: Path, path: PurePosixPath, expected_tested: str | None = None, require_scenarios: bool = True) -> tuple[str, str, int]:
    root = ensure_root(root)
    path = safe_relative(str(path), "Final receipt", FINAL_PREFIX)
    value = load_canonical(read_safe(root, path), str(path))
    tested = validate_final_value(root, value, expected_tested)
    head, descendants = validate_descendants(root, tested)
    invalid_dirty = sorted(str(path) for path in dirty_paths(root) if not evidence_only(path))
    if invalid_dirty:
        raise EvidenceError(f"non-evidence dirty path exists during M4 Final validation: {invalid_dirty}")
    if require_scenarios:
        validate_scenarios(root, path)
    return tested, head, descendants


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--kind", choices=("child", "final"), default="final")
    parser.add_argument("--expected-child-kind", choices=CHILD_KINDS)
    parser.add_argument("--expected-tested-commit")
    parser.add_argument("--no-scenario-sync", action="store_true")
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        if args.kind == "child":
            kind, tested, _ = validate_child(args.repo_root, PurePosixPath(args.receipt), args.expected_child_kind, args.expected_tested_commit)
            print(f"V2 M4 child PASS: kind={kind} tested={tested}")
        else:
            tested, head, descendants = validate_final(
                args.repo_root,
                PurePosixPath(args.receipt),
                args.expected_tested_commit,
                not args.no_scenario_sync,
            )
            print(
                f"V2 M4 Final PASS: tested={tested} head={head} evidenceOnlyDescendants={descendants} "
                f"children={len(CHILD_KINDS)} scenarios={len(PROMOTED_SCENARIOS)} sharedPredicates={len(SHARED_PREDICATES)}"
            )
    except (EvidenceError, OSError) as error:
        print(f"V2 M4 evidence check: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
