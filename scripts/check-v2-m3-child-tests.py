#!/usr/bin/env python3
"""Positive and negative tests for the closed M3 child receipt validator."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import struct
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET


CHECKER_PATH = Path(__file__).with_name("check-v2-m3-child.py")
SOURCE_ROOT = Path(__file__).resolve().parent.parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


CONTRACT = load_module("m3_child_checker_test", CHECKER_PATH)
ALLOCATOR_FIXTURE_CLIENT_BYTES = b"governed allocator fixture Oxia client JAR\n"


def source_lock_fixture(
    allocator_mode: str = "STRICT",
) -> tuple[dict, dict[tuple[str, str], dict[str, str]]]:
    value = json.loads((SOURCE_ROOT / CONTRACT.FINAL.SOURCE_LOCKS_PATH).read_text())
    kafka = value["m3KafkaNativeBinding"]
    pulsar = value["m3PulsarNativeBinding"]
    allocator = value["m3AllocatorEvidenceBinding"]
    dependencies = value["dependencyEvidenceBindings"]
    client = dependencies["oxiaClientArtifacts"]["artifacts"]["clientJar"]
    client["bytes"] = len(ALLOCATOR_FIXTURE_CLIENT_BYTES)
    client["sha256"] = CONTRACT.sha256(ALLOCATOR_FIXTURE_CLIENT_BYTES)
    allocator["oxiaClientJarSha256"] = client["sha256"]
    identities = {
        "provider": (
            "MINIO_S3_COMPATIBLE|artifactReference=quay.io/minio/minio@sha256:"
            "14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
            "|artifactConfigDigest=sha256:"
            "8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253"
        ),
        "kms": (
            "VAULT_TRANSIT|artifactReference=hashicorp/vault@sha256:"
            "268bb80aa9c6d13d65fcfa05c0c268caca068952240a8087291a6ce0b66e3a10"
            "|artifactConfigDigest=sha256:"
            "ba5ade3c7f155978f41dbca62b16e5e08f5331f5d12f9be2d333698b49484457"
        ),
        "kafka": f"KAFKA_NATIVE|repository=apache/kafka|commit={kafka['sourceCommit']}",
        "pulsar": f"PULSAR_NATIVE|repository=apache/pulsar|commit={pulsar['sourceCommit']}",
        "allocator": (
            f"OXIA_AND_PULSAR|pulsarCommit={allocator['pulsarSourceCommit']}"
            f"|oxiaClientCommit={allocator['oxiaClientSourceCommit']}"
            f"|oxiaClientJarSha256={allocator['oxiaClientJarSha256']}"
            f"|oxiaServerCommit={allocator['oxiaServerSourceCommit']}"
            f"|oxiaServerImageDigest={allocator['oxiaServerImageDigest']}"
        ),
    }
    specs = {
        ("ALLOCATOR_SELECTION", "ALLOCATOR_FAULT_SUMMARY"): (
            "OXIA_AND_PULSAR", "FAULT_INJECTION", identities["allocator"]),
        ("ALLOCATOR_SELECTION", "ALLOCATOR_NATIVE_RELATIVE_SUMMARY"): (
            "PULSAR_NATIVE", "NATIVE_REFERENCE_EXECUTION", identities["pulsar"]),
        ("ALLOCATOR_SELECTION", "ALLOCATOR_SCALE_10000_SUMMARY"): (
            "OXIA_AND_PULSAR", "SCALE_EXECUTION", identities["allocator"]),
        ("ALLOCATOR_SELECTION", "ALLOCATOR_SCALE_100000_SUMMARY"): (
            "OXIA_AND_PULSAR", "SCALE_EXECUTION", identities["allocator"]),
        ("C1_REAL_PROVIDER_KMS", "KMS_REAL_RECEIPT"): (
            "VAULT_TRANSIT", "REAL_EXTERNAL_PROCESS", identities["kms"]),
        ("C1_REAL_PROVIDER_KMS", "PROVIDER_REAL_RECEIPT"): (
            "MINIO_S3_COMPATIBLE", "REAL_EXTERNAL_PROCESS", identities["provider"]),
        ("P_PULSAR_OBJECT_WAL", "NATIVE_RESULT"): (
            "PULSAR_NATIVE", "NATIVE_REFERENCE_EXECUTION", identities["pulsar"]),
        ("U_KAFKA_OBJECT_WAL", "NATIVE_RESULT"): (
            "KAFKA_NATIVE", "NATIVE_REFERENCE_EXECUTION", identities["kafka"]),
    }
    bindings: dict[tuple[str, str], dict[str, str]] = {}
    for key, (backend, execution, identity) in specs.items():
        bindings[key] = {
            "backend": backend,
            "childKind": key[0],
            "evidenceKind": key[1],
            "executionClass": execution,
            "sourceIdentity": identity,
            "sourceProvenance": CONTRACT._expected_locked_provenance(
                value, key[1], backend, identity
            ),
        }
    value["m3EvidenceBindings"] = {
        "allocatorMode": allocator_mode,
        "bindings": [bindings[key] for key in sorted(bindings)],
        "schema": CONTRACT.SOURCE_BINDING_SCHEMA,
    }
    return value, bindings


def provider_raw_receipt_fixture(tested_commit: str) -> dict:
    return {
        "absenceListPlusExactGetNotFound": True,
        "actualFrameRangeBytes": 1_048_576,
        "actualPrefixRangeBytes": 4_194_304,
        "actualStreamingFullGetSha256Bytes": 67_108_864,
        "actualStreamingPutBytes": 67_108_864,
        "adapterVersion": "nereus-s3-c1-v1/aws-sdk-s3-2.47.5",
        "c2PromotionEligible": False,
        "c2Tested": False,
        "candidateRootAdmissionContractSha256": "c6a68cb5d2f9a9783538fa3e36df0bc7881fd6a5b1d50a45bef99b0b4fbafe8c",
        "containerAutoRemove": True,
        "containerId": "0123456789ab",
        "coreC1ObjectProviderSession": True,
        "errors": 0,
        "etagUsedAsContentProof": False,
        "failures": 0,
        "forcedPaginationObjects": 5,
        "forcedPaginationPageKeys": 2,
        "headCalls": 0,
        "imageConfigDigest": "sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253",
        "imageReference": (
            "quay.io/minio/minio@sha256:"
            "14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
        ),
        "inFlightAtTerminal": 0,
        "networkBinding": "127.0.0.1:RANDOM",
        "nereusCommit": tested_commit,
        "promotionEligible": False,
        "providerProduct": "MinIO",
        "providerProofMode": "NONE",
        "providerVersion": "RELEASE.2025-09-07T16-13-09Z",
        "realProvider": True,
        "responseUnknownCuts": ["PRESENT", "ABSENT", "UNKNOWN"],
        "responseUnknownFaultInjection": "DETERMINISTIC_CLIENT_BOUNDARY",
        "result": "PASS_REAL_PROVIDER_C1_ONLY",
        "rootBodyCapBytes": 67_108_864,
        "samePrefixImmediateList": True,
        "schema": "NEREUS_V2_M3_EXACT_PROVIDER_CAPACITY_EVIDENCE_V1",
        "skipped": 0,
        "spoolResourcesAtTerminal": 0,
        "strategy": "C1_SINGLE_OBJECT_V1",
        "terminalOutcomes": [
            "APPLIED_EXACT",
            "EXISTING_EXACT",
            "DEFINITIVELY_NOT_APPLIED",
            "DEFINITIVE_CONFLICT",
        ],
        "testClass": "com.nereusstream.storage.object.s3.MinioC1RealProviderEvidenceTest",
        "testMethod": "provesC1AtTheAdmitted64MiBRootCap()",
        "testTask": "realProviderTest",
        "tests": 1,
        "unexpectedErrors": 0,
        "userMetadataUsedAsContentProof": False,
    }


def kms_raw_receipt_fixture(tested_commit: str) -> dict:
    return {
        "adapterVersion": "nereus-vault-transit-v1/jdk-http",
        "applicationOwnedPlaintextKeyArraysZeroized": True,
        "containerAutoRemove": True,
        "containerId": "abcdef012345",
        "contractSha256": "c668d3a20b3f99fbcd6b7c0a252e19452d193983c8c16188257694f025615754",
        "coreKmsTransportSpi": True,
        "derivedKeyContext": False,
        "devMode": True,
        "envelopeAlgorithmId": "AES256_GCM96",
        "envelopeProviderId": "HASHICORP_VAULT_TRANSIT",
        "errors": 0,
        "failures": 0,
        "imageConfigDigest": "sha256:ba5ade3c7f155978f41dbca62b16e5e08f5331f5d12f9be2d333698b49484457",
        "imageReference": "hashicorp/vault@sha256:268bb80aa9c6d13d65fcfa05c0c268caca068952240a8087291a6ce0b66e3a10",
        "jdkHttpTlsInternalBufferZeroizationProven": False,
        "kmsContextAuthority": "NONE",
        "networkBinding": "127.0.0.1:RANDOM",
        "nereusCommit": tested_commit,
        "oldVersionDecryptAfterRotation": True,
        "product": "HashiCorp Vault Transit",
        "productVersion": "1.20.4",
        "productionDeploymentProven": False,
        "promotionEligible": False,
        "realKms": True,
        "result": "PASS_REAL_VAULT_TRANSIT_KMS_ONLY",
        "runKeyBytes": 32,
        "schema": "NEREUS_V2_M3_REAL_KMS_EVIDENCE_V1",
        "skipped": 0,
        "testClass": "com.nereusstream.storage.object.vault.VaultTransitRealKmsEvidenceTest",
        "testMethod": "provesRealTransitWrapUnwrapVersionRotationAndLifecycle()",
        "testTask": "realKmsTest",
        "tests": 1,
        "tokenStringZeroizationProven": False,
        "versionsProven": [1, 2],
        "walRunTerminalClosureProof": True,
        "wrappingKeyIdentity": "vault-transit://transit/keys/nereus-m3-run-key",
    }


def real_junit_xml_fixture(attachment_kind: str) -> bytes:
    _, test_class, test_method = CONTRACT.REAL_EXECUTION_PROFILES[attachment_kind]
    method = test_method.removesuffix("()")
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<testsuite name="{test_class}" tests="1" skipped="0" failures="0" errors="0">\n'
        f'  <testcase name="{method}" classname="{test_class}" time="1.0"/>\n'
        '</testsuite>\n'
    ).encode()


def governed_junit_receipt_fixture(
    root: Path,
    tested_commit: str,
    child_kind: str,
    *,
    tests: int = 3,
    skipped: int = 0,
) -> bytes:
    required = sorted(CONTRACT.GOVERNED_JUNIT_REQUIRED_TESTS.get(child_kind, set()))
    if not required:
        required = [(f"com.nereusstream.m3.evidence.{child_kind}Test", "evidenceCase0")]
    tests = max(tests, len(required))
    cases = list(required)
    cases.extend(
        (required[0][0], f"fixtureEvidenceCase{index}")
        for index in range(tests - len(required))
    )
    by_class: dict[str, list[tuple[str, bool]]] = {}
    for index, (test_class, method) in enumerate(cases):
        by_class.setdefault(test_class, []).append((method, index < skipped))
    junit_inputs: list[tuple[str, bytes]] = []
    for test_class in sorted(by_class):
        class_cases = by_class[test_class]
        skipped_in_class = sum(1 for _, is_skipped in class_cases if is_skipped)
        case_xml = "\n".join(
            f'  <testcase name="{method}" classname="{test_class}" time="0.1">'
            f'{"<skipped/>" if is_skipped else ""}</testcase>'
            for method, is_skipped in class_cases
        )
        raw_xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<testsuite name="{test_class}" tests="{len(class_cases)}" '
            f'skipped="{skipped_in_class}" failures="0" errors="0">\n'
            + case_xml
            + "\n</testsuite>\n"
        ).encode()
        relative = (
            f"nereus-m3-evidence/build/test-results/{child_kind.lower()}/"
            f"TEST-{test_class}.xml"
        )
        junit_inputs.append((relative, raw_xml))
    if skipped == 0:
        return CONTRACT.canonical_bytes(
            CONTRACT.seal_junit_execution_receipt(
                root, junit_inputs, child_kind, tested_commit
            )
        )
    rows = [
        {
            "bytes": len(raw_xml),
            "path": relative,
            "sha256": CONTRACT.sha256(raw_xml),
            "xmlBase64": base64.b64encode(raw_xml).decode("ascii"),
        }
        for relative, raw_xml in junit_inputs
    ]
    wrapper = {
        "childKind": child_kind,
        "junitXml": rows,
        "receiptSha256": "0" * 64,
        "schema": CONTRACT.JUNIT_SCHEMA,
        "testedCommit": tested_commit,
    }
    wrapper["receiptSha256"] = CONTRACT.sha256(CONTRACT.canonical_bytes(wrapper))
    return CONTRACT.canonical_bytes(wrapper)


def provider_receipt_fixture(tested_commit: str) -> dict:
    raw = CONTRACT.canonical_bytes(provider_raw_receipt_fixture(tested_commit))
    return CONTRACT.seal_real_execution_receipt(
        raw,
        real_junit_xml_fixture("PROVIDER_REAL_RECEIPT"),
        "PROVIDER_REAL_RECEIPT",
        tested_commit,
    )


def kms_receipt_fixture(tested_commit: str) -> dict:
    raw = CONTRACT.canonical_bytes(kms_raw_receipt_fixture(tested_commit))
    return CONTRACT.seal_real_execution_receipt(
        raw,
        real_junit_xml_fixture("KMS_REAL_RECEIPT"),
        "KMS_REAL_RECEIPT",
        tested_commit,
    )


def allocator_verifier_junit_fixture() -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<testsuite name="{CONTRACT.ALLOCATOR_VERIFIER_TEST_CLASS}" tests="1" '
        'skipped="0" failures="0" errors="0">\n'
        f'  <testcase name="{CONTRACT.ALLOCATOR_VERIFIER_TEST_CASE}" '
        f'classname="{CONTRACT.ALLOCATOR_VERIFIER_TEST_CLASS}" time="1.0"/>\n'
        '</testsuite>\n'
    ).encode()


def _allocator_tuple_bytes(source: dict[str, str]) -> bytes:
    return b"".join(
        bytes.fromhex(source[name])
        for name in (
            "nereusCommit",
            "pulsarCommit",
            "oxiaClientCommit",
            "oxiaServerCommit",
            "oxiaClientJarSha256",
            "testedEvidenceArtifactSha256",
            "runtimeDomainArtifactSha256",
            "runtimeMetadataSpiArtifactSha256",
            "runtimeMetadataOxiaArtifactSha256",
            "sourceLocksSha256",
            "executorManifestSha256",
        )
    )


def java_allocator_verification_fixture(
    root: Path, tested_commit: str
) -> tuple[bytes, bytes, list[tuple[str, str]]]:
    locks_raw = CONTRACT.git(root, "show", f"{tested_commit}:{CONTRACT.FINAL.SOURCE_LOCKS_PATH}")
    assert isinstance(locks_raw, bytes)
    locks = json.loads(locks_raw)
    pulsar = locks["m2PulsarNativeBinding"]
    oxia = next(
        row for row in locks["dependencyForkOutputs"]
        if row["id"] == "oxia-client-notification-continuity"
    )
    dependencies = locks["dependencyEvidenceBindings"]
    client_jar = dependencies["oxiaClientArtifacts"]["artifacts"]["clientJar"]
    server = dependencies["oxiaServerRuntime"]
    source_locks_sha = CONTRACT.sha256(locks_raw)
    artifact_contents = {
        name: (f"governed allocator fixture artifact: {name}\n").encode()
        for name in (
            "executorManifest",
            "runtimeDomainArtifact",
            "runtimeMetadataOxiaArtifact",
            "runtimeMetadataSpiArtifact",
            "testedEvidenceArtifact",
        )
    }
    digests = {name: CONTRACT.sha256(raw) for name, raw in artifact_contents.items()}
    selected_mode = (
        "STRICT_SERIALIZED"
        if locks["m3EvidenceBindings"]["allocatorMode"] == "STRICT"
        else "RANGE_LEASED"
    )
    selected_range = 1 if selected_mode == "STRICT_SERIALIZED" else 64
    formal = root / CONTRACT.ALLOCATOR_FORMAL_PREFIX / tested_commit
    formal.mkdir(parents=True, exist_ok=True)
    artifact_paths = {
        "executorManifest": (
            root
            / "nereus-metadata-oxia/build/m3-allocator-evidence/executor"
            / f"{tested_commit}.json"
        ),
        "oxiaClientJar": root / client_jar["relativePath"],
        "runtimeDomainArtifact": formal / "artifacts/nereus-domain.jar",
        "runtimeMetadataOxiaArtifact": formal / "artifacts/nereus-metadata-oxia.jar",
        "runtimeMetadataSpiArtifact": formal / "artifacts/nereus-metadata-spi.jar",
        "sourceLocks": root / CONTRACT.FINAL.SOURCE_LOCKS_PATH,
        "testedEvidenceArtifact": formal / "artifacts/nereus-metadata-oxia-evidence.jar",
    }
    for name, raw in artifact_contents.items():
        artifact_paths[name].parent.mkdir(parents=True, exist_ok=True)
        artifact_paths[name].write_bytes(raw)
    source_artifacts = {
        "oxiaClientJar": {
            "basename": PurePosixPath(client_jar["relativePath"]).name,
            "bytes": client_jar["bytes"],
            "sha256": client_jar["sha256"],
        },
        "testedEvidenceArtifact": {
            "basename": "nereus-metadata-oxia-evidence.jar",
            "bytes": len(artifact_contents["testedEvidenceArtifact"]),
            "sha256": digests["testedEvidenceArtifact"],
        },
        "runtimeDomainArtifact": {
            "basename": "nereus-domain.jar",
            "bytes": len(artifact_contents["runtimeDomainArtifact"]),
            "sha256": digests["runtimeDomainArtifact"],
        },
        "runtimeMetadataSpiArtifact": {
            "basename": "nereus-metadata-spi.jar",
            "bytes": len(artifact_contents["runtimeMetadataSpiArtifact"]),
            "sha256": digests["runtimeMetadataSpiArtifact"],
        },
        "runtimeMetadataOxiaArtifact": {
            "basename": "nereus-metadata-oxia.jar",
            "bytes": len(artifact_contents["runtimeMetadataOxiaArtifact"]),
            "sha256": digests["runtimeMetadataOxiaArtifact"],
        },
        "sourceLocks": {
            "basename": CONTRACT.FINAL.SOURCE_LOCKS_PATH.name,
            "bytes": len(locks_raw),
            "sha256": source_locks_sha,
        },
        "executorManifest": {
            "basename": f"{tested_commit}.json",
            "bytes": len(artifact_contents["executorManifest"]),
            "sha256": digests["executorManifest"],
        },
    }
    source = {
        "nereusCommit": tested_commit,
        "pulsarCommit": pulsar["finalForkCommit"],
        "oxiaClientCommit": oxia["finalForkCommit"],
        "oxiaServerCommit": server["sourceCommit"],
        "oxiaClientJarSha256": client_jar["sha256"],
        "testedEvidenceArtifactSha256": digests["testedEvidenceArtifact"],
        "runtimeDomainArtifactSha256": digests["runtimeDomainArtifact"],
        "runtimeMetadataSpiArtifactSha256": digests["runtimeMetadataSpiArtifact"],
        "runtimeMetadataOxiaArtifactSha256": digests["runtimeMetadataOxiaArtifact"],
        "sourceLocksSha256": source_locks_sha,
        "executorManifestSha256": digests["executorManifest"],
    }
    tuple_bytes = _allocator_tuple_bytes(source)
    naea_names = ("test.naea", "native.naea", "fault.naea", "scale-10000.naea", "scale-100000.naea")
    attachments: dict[str, dict[str, int | str]] = {}
    naea_paths: dict[str, Path] = {}
    for code, name in enumerate(naea_names, start=1):
        payload = f"allocator fixture payload: {name}\n".encode()
        envelope = (
            struct.pack(">4sHH", b"NAEA", 1, code)
            + tuple_bytes
            + bytes.fromhex(CONTRACT.sha256(payload))
            + struct.pack(">q", len(payload))
            + bytes(8)
            + payload
        )
        path = formal / name
        path.write_bytes(envelope)
        naea_paths[name] = path
        attachments[name] = {"bytes": len(envelope), "envelopeSha256": CONTRACT.sha256(envelope)}
    mode_code = 1 if selected_mode == "STRICT_SERIALIZED" else 2
    selection = (
        struct.pack(">4sHHIIqIHH", b"NARS", 1, mode_code, 1, 7, selected_range, 4, 9, 9)
        + tuple_bytes
        + b"".join(bytes.fromhex(attachments[name]["envelopeSha256"]) for name in naea_names)
        + struct.pack(">I", 8)
        + bytes(8 * 224)
        + bytes(36)
    )
    assert len(selection) == 2328
    selection_path = formal / "selection.nars"
    selection_path.write_bytes(selection)
    recomputation = {
        "schema": CONTRACT.JAVA_ALLOCATOR_RAW_SCHEMA,
        "selfSha256": "0" * 64,
        "selfHashRule": CONTRACT.ALLOCATOR_VERIFICATION_SELF_HASH_RULE,
        "authority": False,
        "status": "PASS_RAW_RECOMPUTED",
        "selectionEligible": True,
        "testedCommit": tested_commit,
        "sourceLocksSha256": source_locks_sha,
        "selectedMode": selected_mode,
        "selectedRangeSize": selected_range,
        "selection": {
            "basename": "selection.nars",
            "bytes": 2328,
            "sha256": CONTRACT.sha256(selection),
        },
        "derived": {"intervals": 288, "faultCutKinds": 9, "selectedRows": 8},
        "junit": {"tests": 1, "failures": 0, "errors": 0, "skips": 0},
        "source": source,
        "attachments": attachments,
        "sourceArtifacts": source_artifacts,
    }
    raw_recomputation = json.dumps(
        recomputation, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=False
    ).encode() + b"\n"
    recomputation["selfSha256"] = CONTRACT.sha256(raw_recomputation)
    raw_recomputation = json.dumps(
        recomputation, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=False
    ).encode() + b"\n"
    junit_xml = allocator_verifier_junit_fixture()
    outer = {
        "schema": CONTRACT.JAVA_ALLOCATOR_VERIFICATION_SCHEMA,
        "selfSha256": "0" * 64,
        "selfHashRule": CONTRACT.ALLOCATOR_VERIFICATION_SELF_HASH_RULE,
        "rawVerification": recomputation,
        "rawVerificationBytes": len(raw_recomputation),
        "rawVerificationSha256": CONTRACT.sha256(raw_recomputation),
        "verifierJUnit": {
            "basename": f"TEST-{CONTRACT.ALLOCATOR_VERIFIER_TEST_CLASS}.xml",
            "bytes": len(junit_xml),
            "sha256": CONTRACT.sha256(junit_xml),
            "tests": 1,
            "failures": 0,
            "errors": 0,
            "skips": 0,
            "testClass": CONTRACT.ALLOCATOR_VERIFIER_TEST_CLASS,
            "testCase": CONTRACT.ALLOCATOR_VERIFIER_TEST_CASE,
        },
    }
    zeroed = json.dumps(
        outer, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=False
    ).encode() + b"\n"
    outer["selfSha256"] = CONTRACT.sha256(zeroed)
    java_verification = json.dumps(
        outer, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=False
    ).encode() + b"\n"
    external_paths = [
        ("selection.nars", str(selection_path.relative_to(root))),
        *((name, str(naea_paths[name].relative_to(root))) for name in naea_names),
        *((name, str(artifact_paths[name].relative_to(root))) for name in CONTRACT.ALLOCATOR_EXTERNAL_NAMES[6:]),
    ]
    return java_verification, junit_xml, external_paths


def allocator_receipt_fixture(root: Path, tested_commit: str) -> bytes:
    java_verification, junit_xml, external_paths = java_allocator_verification_fixture(root, tested_commit)
    return CONTRACT.canonical_bytes(
        CONTRACT.seal_allocator_verification_receipt(
            root,
            java_verification,
            str(CONTRACT.ALLOCATOR_FORMAL_PREFIX / tested_commit / "raw-verification.json"),
            junit_xml,
            str(CONTRACT.ALLOCATOR_VERIFIER_JUNIT_PATH),
            tested_commit,
            external_paths,
        )
    )


def local_cap_result_fixture(root: Path, tested_commit: str) -> bytes:
    harness_source = CONTRACT.git(
        root, "show", f"{tested_commit}:{CONTRACT.LOCAL_CAP_HARNESS_SOURCE}"
    )
    harness_test_source = CONTRACT.git(
        root, "show", f"{tested_commit}:{CONTRACT.LOCAL_CAP_HARNESS_TEST_SOURCE}"
    )
    assert isinstance(harness_source, bytes) and isinstance(harness_test_source, bytes)
    value = {
        "allocationFreeAnalyticalOnly": True,
        "childKind": "D_LOCAL_CAP",
        "errors": 0,
        "failures": 0,
        "harnessSourceSha256": CONTRACT.sha256(harness_source),
        "harnessTestSourceSha256": CONTRACT.sha256(harness_test_source),
        "nereusCommit": tested_commit,
        "promotionEligible": False,
        "providerTransferClaimed": False,
        "receiptSha256": "0" * 64,
        "records": CONTRACT._expected_local_cap_records(root, tested_commit),
        "result": CONTRACT.LOCAL_CAP_RESULT,
        "schema": CONTRACT.LOCAL_CAP_SCHEMA,
        "skipped": 0,
        "tests": 6,
    }
    value["receiptSha256"] = CONTRACT.sha256(CONTRACT.canonical_bytes(value))
    return CONTRACT.canonical_bytes(value)


def native_junit_xml_fixtures(child_kind: str) -> list[tuple[str, bytes]]:
    profile = CONTRACT.NATIVE_PROFILES[child_kind]
    result: list[tuple[str, bytes]] = []
    suite_counts = profile.get("suiteTestCounts")
    for index, suite in enumerate(profile["suites"]):
        methods = [
            test_id.removeprefix(suite + "#")
            for test_id in profile["requiredTests"]
            if test_id.startswith(suite + "#")
        ]
        if suite_counts is not None:
            methods.extend(
                f"fixtureFiller{ordinal}"
                for ordinal in range(suite_counts[index] - len(methods))
            )
        cases = "\n".join(
            f'  <testcase name="{method}" classname="{suite}" time="0.1"/>'
            for method in methods
        )
        raw = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<testsuite name="{suite}" tests="{len(methods)}" skipped="0" '
            f'failures="0" errors="0">\n{cases}\n</testsuite>\n'
        ).encode()
        result.append((f"{profile['junitRoot']}/TEST-{suite}.xml", raw))
    return result


def _native_artifacts(root: Path, tested_commit: str, child_kind: str) -> tuple[list[dict], str]:
    profile = CONTRACT.NATIVE_PROFILES[child_kind]
    listed = str(
        CONTRACT.git(
            root,
            "ls-tree",
            "-r",
            "--name-only",
            tested_commit,
            "--",
            *profile["sourceRoots"],
            text=True,
        )
    ).splitlines()
    paths = sorted(
        path for path in listed
        if path.endswith(".java") or path in profile["exactNonJavaSources"]
    )
    artifacts: list[dict] = []
    tree = hashlib.sha256()
    for path in paths:
        blob = CONTRACT.git(root, "show", f"{tested_commit}:{path}")
        assert isinstance(blob, bytes)
        path_bytes = path.encode()
        tree.update(struct.pack(">I", len(path_bytes)))
        tree.update(path_bytes)
        tree.update(struct.pack(">Q", len(blob)))
        tree.update(blob)
        artifacts.append({"path": path, "sha256": CONTRACT.sha256(blob), "bytes": len(blob)})
    return artifacts, tree.hexdigest()


def native_raw_result_fixture(
    root: Path,
    tested_commit: str,
    child_kind: str,
    binding: dict[str, str],
) -> tuple[bytes, list[tuple[str, bytes]]]:
    profile = CONTRACT.NATIVE_PROFILES[child_kind]
    xml_inputs = native_junit_xml_fixtures(child_kind)
    junit_files: list[dict] = []
    total_tests = 0
    for path, raw in xml_inputs:
        tests = raw.count(b"<testcase ")
        total_tests += tests
        junit_files.append(
            {
                "path": path,
                "sha256": CONTRACT.sha256(raw),
                "tests": tests,
                "failures": 0,
                "errors": 0,
                "skipped": 0,
            }
        )
    artifacts, tree_sha = _native_artifacts(root, tested_commit, child_kind)
    identity = binding["sourceIdentity"]
    external = identity.split("|repository=", 1)[1]
    repository, commit = external.rsplit("|commit=", 1)
    row = {
        "schema": CONTRACT.NATIVE_RESULT_SCHEMA,
        "componentKind": child_kind,
        "status": "PASS",
        "testedSource": {
            "repository": "nereus",
            "commit": tested_commit,
            "treeSha256": tree_sha,
        },
        "externalSources": [{"repository": repository, "commit": commit}],
        "execution": {
            "command": profile["command"],
            "startedAtUtc": "2026-08-24T00:00:00Z",
            "finishedAtUtc": "2026-08-24T00:00:01Z",
            "jvm": "test-jvm",
            "os": "test-os",
        },
        "junit": {
            "xmlRoot": profile["junitRoot"],
            "xmlFiles": junit_files,
            "totals": {
                "suites": len(junit_files),
                "tests": total_tests,
                "failures": 0,
                "errors": 0,
                "skipped": 0,
            },
        },
        "requiredTests": list(profile["requiredTests"]),
        "counters": dict(profile["counters"]),
        "artifacts": artifacts,
        "exclusions": list(CONTRACT.NATIVE_RESULT_EXCLUSIONS),
        "receiptSha256": "0" * 64,
    }
    counter_keys = tuple(profile["counters"])
    row["receiptSha256"] = CONTRACT.sha256(
        CONTRACT._native_canonical_bytes(row, counter_keys, "0" * 64)
    )
    raw_result = CONTRACT._native_canonical_bytes(row, counter_keys)
    return raw_result, xml_inputs


def native_receipt_fixture(
    root: Path,
    tested_commit: str,
    child_kind: str,
    binding: dict[str, str],
) -> bytes:
    raw_result, xml_inputs = native_raw_result_fixture(
        root, tested_commit, child_kind, binding
    )
    sealed = CONTRACT.seal_native_execution_receipt(
        root, raw_result, xml_inputs, child_kind, tested_commit
    )
    return CONTRACT.canonical_bytes(sealed)


def write_native_source_fixture(root: Path) -> None:
    paths: set[str] = set()
    exact_non_java = {
        path
        for profile in CONTRACT.NATIVE_PROFILES.values()
        for path in profile["exactNonJavaSources"]
    }
    for profile in CONTRACT.NATIVE_PROFILES.values():
        for source in profile["sourceRoots"]:
            path = (
                source
                if source.endswith(".java") or source in exact_non_java
                else source + "/FixtureSource.java"
            )
            paths.add(path)
    for relative in sorted(paths):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"// fixture source: {relative}\n")
    kafka = CONTRACT.NATIVE_PROFILES["U_KAFKA_OBJECT_WAL"]
    listed = set(paths) | {
        path
        for path in CONTRACT.GOVERNED_JUNIT_SOURCE_PATHS.values()
        if path.startswith("nereus-kafka-bookkeeper/")
    }
    missing = kafka["sourceArtifactCount"] - sum(
        1
        for path in listed
        if path.startswith("nereus-kafka-bookkeeper/")
    )
    filler_root = "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object"
    for index in range(missing):
        relative = f"{filler_root}/FixtureFiller{index:02d}.java"
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"// fixture source: {relative}\n")


def write_local_cap_source_fixture(root: Path) -> None:
    for relative in (
        CONTRACT.LOCAL_CAP_HARNESS_SOURCE,
        CONTRACT.LOCAL_CAP_HARNESS_TEST_SOURCE,
        *(record[3] for record in CONTRACT.LOCAL_CAP_RECORDS),
    ):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"// fixture D1 source: {relative}\n")


def write_governed_junit_source_fixture(root: Path) -> None:
    methods_by_class: dict[str, set[str]] = {}
    for required in CONTRACT.GOVERNED_JUNIT_REQUIRED_TESTS.values():
        for test_class, method in required:
            methods_by_class.setdefault(test_class, set()).add(method)
    for test_class, methods in sorted(methods_by_class.items()):
        relative = CONTRACT.GOVERNED_JUNIT_SOURCE_PATHS[test_class]
        package, simple_name = test_class.rsplit(".", 1)
        source = [f"package {package};", "", "import org.junit.jupiter.api.Test;", "", f"class {simple_name} {{"]
        for method in sorted(methods):
            source.extend(("    @Test", f"    void {method}() {{}}", ""))
        source.append("}")
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(source) + "\n")


def recovery_manifest_fixture() -> bytes:
    rows = ["\t".join(CONTRACT.RECOVERY_MANIFEST_HEADER)]
    rows.extend("\t".join(row) for row in CONTRACT.RECOVERY_MANIFEST_ROWS)
    return ("\n".join(rows) + "\n").encode()


def protocol_fixture_fixture() -> bytes:
    root_sha = bytes([12]) * 32
    object_body = bytearray(324)
    object_body[:8] = CONTRACT.NWKCP1_MAGIC
    object_body[8:12] = (64).to_bytes(4, "big")
    object_body[16:24] = len(object_body).to_bytes(8, "big")
    object_body[24:56] = root_sha
    object_body[56:60] = (1).to_bytes(4, "big")
    object_body[64:68] = (224).to_bytes(4, "big")
    object_sha = CONTRACT.sha256(bytes(object_body))
    object_key = (
        f"{CONTRACT.PROTOCOL_FIXTURE_ROOT}/protocol/kafka/nwkcp1-v1/objects/"
        f"sha256-v1-{object_sha}.nwkcp1"
    )
    head_key = f"{CONTRACT.PROTOCOL_FIXTURE_ROOT}/protocol/kafka/nwkcp1-v1/head"

    def head(state: int) -> bytes:
        value = bytearray(434)
        value[:8] = CONTRACT.NWKCP1_HEAD_MAGIC
        value[8:12] = len(value).to_bytes(4, "big")
        value[12:44] = root_sha
        value[44:52] = (9).to_bytes(8, "big")
        value[52] = state
        return bytes(value)

    rows = ["\t".join(CONTRACT.PROTOCOL_FIXTURE_HEADER)]
    for artifact_id, state, key, body in (
        ("NWKCP1_OBJECT", "IMMUTABLE", object_key, bytes(object_body)),
        ("KAFKA_PROTOCOL_CHECKPOINT_HEAD_OPEN", "OPEN", head_key, head(0)),
        ("KAFKA_PROTOCOL_CHECKPOINT_HEAD_TERMINAL", "TERMINAL", head_key, head(1)),
    ):
        rows.append(
            "\t".join(
                (artifact_id, state, key, str(len(body)), CONTRACT.sha256(body), body.hex())
            )
        )
    return ("\n".join(rows) + "\n").encode()


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *args], text=True, stderr=subprocess.STDOUT
    ).strip()


def allocator_v2_campaign_fixture(root: Path, tested_commit: str) -> dict:
    fixture_root = root / "nereus-metadata-oxia/build/m3-allocator-evidence/v2-fixture"
    attachment_root = fixture_root / "attachments"
    attachment_root.mkdir(parents=True, exist_ok=True)
    executor = fixture_root / "executor.jar"
    workload = fixture_root / "plan.txt"
    executor.write_bytes(b"allocator V2 governed fixture executor\n")
    workload.write_bytes(b"logicalPerformanceCells=288\nplannerVersion=2\n")
    locks = CONTRACT._source_locks(root, tested_commit)
    allocator_mode = locks["m3EvidenceBindings"]["allocatorMode"]
    if allocator_mode not in {"STRICT", "RANGE"}:
        raise AssertionError("allocator V2 selection fixture requires a selected source-lock mode")
    image = locks["m3AllocatorEvidenceBinding"]["oxiaServerImageDigest"].removeprefix("sha256:")
    source_locks_raw = CONTRACT.git(
        root, "show", f"{tested_commit}:{CONTRACT.FINAL.SOURCE_LOCKS_PATH}"
    )
    assert isinstance(source_locks_raw, bytes)
    source = {
        "nereusCommit": tested_commit,
        "oxiaImageDigest": image,
        "dependencyLockDigest": CONTRACT.sha256(source_locks_raw),
        "executorDigest": CONTRACT.sha256(executor.read_bytes()),
        "workloadDigest": CONTRACT.sha256(workload.read_bytes()),
    }

    def source_bytes() -> bytes:
        return tested_commit.encode("ascii") + b"".join(
            bytes.fromhex(source[name])
            for name in (
                "oxiaImageDigest",
                "dependencyLockDigest",
                "executorDigest",
                "workloadDigest",
            )
        )

    planner = CONTRACT._AllocatorV2Planner()
    observations: list[dict] = []
    for _ in range(400):
        action = planner.next_action()
        if action is None:
            break
        if action[0] == "interval":
            cell = action[1]
            offered = cell["rate"] * 30
            values = [offered, offered, 0, offered, 0, 0, offered, 0, 0, 0, 0, 0]
            values.extend(
                [
                    100_000,
                    100_000,
                    100_000,
                    cell["rate"],
                    100_000,
                    100_000,
                    0,
                    0,
                    0,
                ]
            )
            relative_failure = (
                allocator_mode == "STRICT" and cell["candidate"] >= 2
            ) or (allocator_mode == "RANGE" and cell["candidate"] == 1)
            if relative_failure:
                values[17] = 400_001
            observation = {"cell": cell, "tag": "interval", "values": values}
        else:
            observation = {
                "cuts": 0x1FF,
                "row": action[1],
                "tag": "fault",
                "values": [0, 0, 0, 0, 0, 0, 0, 0, 1, 1_000_000],
            }
        observations.append(observation)
        planner.accept(observation)
    else:
        raise AssertionError("allocator V2 governed fixture planner did not terminate")
    recomputed = CONTRACT._allocator_v2_recompute(observations)
    expected_qualified = [1] if allocator_mode == "STRICT" else [2]
    expected_executed = 20 if allocator_mode == "STRICT" else 17
    if not recomputed["completed"] or (
        recomputed["qualifiedCandidates"] != expected_qualified
        or recomputed["executedPerformanceCells"] != expected_executed
    ):
        raise AssertionError("allocator V2 governed fixture selection differs")

    execution_paths: list[Path] = []
    attachment_digests: list[str] = []
    for index in range(len(observations)):
        path = attachment_root / f"{index:03d}.bin"
        path.write_bytes(f"allocator V2 execution attachment {index}\n".encode())
        execution_paths.append(path)
        attachment_digests.append(CONTRACT.sha256(path.read_bytes()))

    contexts = CONTRACT._allocator_v2_context_ids()
    checkpoint = bytearray(b"NACP2\0\0\0")
    checkpoint.extend(struct.pack(">HBBQ", 2, 1, 0, 0))
    checkpoint.extend(source_bytes())
    checkpoint.extend(bytes.fromhex(CONTRACT._allocator_v2_campaign_id(source, contexts)))
    checkpoint.extend(bytes(32))
    checkpoint.extend(struct.pack(">7Q", 900, 5_400, 7_200, 5_400, 11_520, 1_440, 600))
    checkpoint.extend(struct.pack(">I", len(contexts)))
    checkpoint.extend(b"".join(struct.pack(">I", context) for context in contexts))
    checkpoint.extend(struct.pack(">I", len(observations)))
    for index, observation in enumerate(observations):
        if observation["tag"] == "interval":
            checkpoint.extend(
                struct.pack(
                    ">BI21Q",
                    1,
                    observation["cell"]["contextId"],
                    *observation["values"],
                )
            )
        else:
            candidate, population, latency = observation["row"]
            checkpoint.extend(
                struct.pack(
                    ">BBIII10Q",
                    2,
                    candidate,
                    population,
                    latency,
                    observation["cuts"],
                    *observation["values"],
                )
            )
        checkpoint.extend(bytes.fromhex(attachment_digests[index]))
    dispositions = recomputed["dispositions"]
    checkpoint.extend(struct.pack(">I", len(dispositions)))
    for context, kind, dependencies in dispositions:
        checkpoint.extend(struct.pack(">IBH", context, kind, len(dependencies)))
        checkpoint.extend(b"".join(struct.pack(">I", value) for value in dependencies))
    checkpoint_raw = bytes(checkpoint)
    checkpoint_sha = CONTRACT.sha256(checkpoint_raw)
    attachment_root_sha = CONTRACT._allocator_v2_attachment_root(attachment_digests)

    evaluation_status = 0 if allocator_mode == "STRICT" else 1
    selected_candidate = 1 if allocator_mode == "STRICT" else 2
    selected_name = "STRICT" if allocator_mode == "STRICT" else "RANGE_16"
    evaluation_raw = (
        b"NAEV2\0\0\0"
        + struct.pack(
            ">HBBII",
            2,
            evaluation_status,
            selected_candidate,
            recomputed["executedPerformanceCells"],
            len(dispositions),
        )
        + source_bytes()
        + bytes.fromhex(CONTRACT._allocator_v2_campaign_id(source, contexts))
        + bytes.fromhex(checkpoint_sha)
        + bytes.fromhex(attachment_root_sha)
    )
    diagnostic_junit = (
        '<testsuite name="M3V2RealOxiaDiagnosticTest" tests="4" failures="0" errors="0" skipped="0">\n'
        + "".join(
            f'  <testcase classname="M3V2RealOxiaDiagnosticTest" name="{name}"/>\n'
            for name in sorted(CONTRACT.ALLOCATOR_V2_DIAGNOSTIC_TESTS)
        )
        + "</testsuite>\n"
    ).encode()
    formal_junit = (
        '<testsuite name="M3V2AllocatorFormalCampaignTest" tests="1" failures="0" errors="0" skipped="0">\n'
        '  <testcase classname="M3V2AllocatorFormalCampaignTest" name="runsValidatorProofAdaptiveCampaign()"/>\n'
        "</testsuite>\n"
    ).encode()
    diagnostic_raw = (
        b"NADV2\0\0\0"
        + struct.pack(">HBB", 2, 0x0F, 0)
        + source_bytes()
        + bytes.fromhex(CONTRACT.sha256(diagnostic_junit))
    )
    decision = CONTRACT.canonical_bytes(
        {
            "checkpointSha256": checkpoint_sha,
            "diagnosticJUnitSha256": CONTRACT.sha256(diagnostic_junit),
            "diagnosticSha256": CONTRACT.sha256(diagnostic_raw),
            "evaluationSha256": CONTRACT.sha256(evaluation_raw),
            "formalJUnitSha256": CONTRACT.sha256(formal_junit),
            "schema": CONTRACT.ALLOCATOR_V2_PROMOTION_SCHEMA,
            "selectedCandidate": selected_name,
            "status": "PROMOTABLE",
        }
    )
    paths = {
        "checkpoint": fixture_root / "campaign.nacp",
        "evaluation": fixture_root / "evaluation.naev",
        "diagnostic": fixture_root / "diagnostic.nadv",
        "diagnosticJunit": fixture_root / "diagnostic-junit.xml",
        "formalJunit": fixture_root / "formal-junit.xml",
        "promotionDecision": fixture_root / "promotion.json",
        "executor": executor,
        "workload": workload,
    }
    for name, raw in (
        ("checkpoint", checkpoint_raw),
        ("evaluation", evaluation_raw),
        ("diagnostic", diagnostic_raw),
        ("diagnosticJunit", diagnostic_junit),
        ("formalJunit", formal_junit),
        ("promotionDecision", decision),
    ):
        paths[name].write_bytes(raw)

    def relative(path: Path) -> str:
        return path.relative_to(root).as_posix()

    receipt = CONTRACT.seal_allocator_v2_campaign_verification_receipt(
        root,
        tested_commit,
        checkpoint_raw,
        evaluation_raw,
        diagnostic_raw,
        diagnostic_junit,
        formal_junit,
        decision,
        [
            ("executorArtifact", relative(executor)),
            ("workloadPlan", relative(workload)),
        ],
        [relative(path) for path in execution_paths],
    )
    return {
        "executionPaths": execution_paths,
        "paths": paths,
        "receipt": CONTRACT.canonical_bytes(receipt),
    }


class Fixture:
    def __init__(self, allocator_mode: str = "STRICT") -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="nereus-m3-child-checker-")
        self.root = Path(self.temporary.name) / "repo"
        self.root.mkdir()
        git(self.root, "init", "-b", "main")
        git(self.root, "config", "user.name", "M3 Child Test")
        git(self.root, "config", "user.email", "m3-child@example.invalid")
        locks = self.root / CONTRACT.FINAL.SOURCE_LOCKS_PATH
        locks.parent.mkdir(parents=True)
        source_locks, self.bindings = source_lock_fixture(allocator_mode)
        locks.write_text(json.dumps(source_locks, indent=2) + "\n")
        client = source_locks["dependencyEvidenceBindings"]["oxiaClientArtifacts"]["artifacts"]["clientJar"]
        client_path = self.root / client["relativePath"]
        client_path.parent.mkdir(parents=True, exist_ok=True)
        client_path.write_bytes(ALLOCATOR_FIXTURE_CLIENT_BYTES)
        write_native_source_fixture(self.root)
        write_local_cap_source_fixture(self.root)
        write_governed_junit_source_fixture(self.root)
        (self.root / "production.txt").write_text("production source\n")
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "tested source")
        self.tested = git(self.root, "rev-parse", "HEAD")
        (self.root / ".git/info/exclude").write_text("nereus-metadata-oxia/build/\n")

    def cleanup(self) -> None:
        self.temporary.cleanup()

    def canonical(self, value: dict) -> bytes:
        return CONTRACT.canonical_bytes(value)

    def junit(self, child_kind: str, *, skipped: int = 0, tests: int = 3) -> bytes:
        return governed_junit_receipt_fixture(
            self.root, self.tested, child_kind, skipped=skipped, tests=tests
        )

    def typed(self, attachment_kind: str, child_kind: str, *, fake: bool = False) -> bytes:
        _, _, subjects = CONTRACT._expected_typed_profile(
            attachment_kind, child_kind
        )
        binding = self.bindings[(child_kind, attachment_kind)]
        if attachment_kind == "PROVIDER_REAL_RECEIPT":
            raw_value = provider_raw_receipt_fixture(self.tested)
            if fake:
                raw_value["imageReference"] = "quay.io/minio/minio@sha256:" + "b" * 64
            value = CONTRACT.seal_real_execution_receipt(
                self.canonical(raw_value),
                real_junit_xml_fixture(attachment_kind),
                attachment_kind,
                self.tested,
            )
            return self.canonical(value)
        if attachment_kind == "KMS_REAL_RECEIPT":
            raw_value = kms_raw_receipt_fixture(self.tested)
            if fake:
                raw_value["imageConfigDigest"] = "sha256:" + "b" * 64
            value = CONTRACT.seal_real_execution_receipt(
                self.canonical(raw_value),
                real_junit_xml_fixture(attachment_kind),
                attachment_kind,
                self.tested,
            )
            return self.canonical(value)
        if attachment_kind == "NATIVE_RESULT":
            return native_receipt_fixture(
                self.root, self.tested, child_kind, binding
            )
        identity = binding["sourceIdentity"]
        if fake:
            identity = "sha256:" + "b" * 64
        tests = 1 if attachment_kind in CONTRACT.ALLOCATOR_DERIVED_KINDS else 2
        return self.canonical(
            {
                "backend": binding["backend"],
                "errors": 0,
                "evidenceKind": attachment_kind,
                "executionClass": binding["executionClass"],
                "failures": 0,
                "nereusCommit": self.tested,
                "result": "PASS_EVIDENCE_ONLY",
                "schema": CONTRACT.TYPED_SCHEMA,
                "skipped": 0,
                "sourceIdentity": identity,
                "subjectCount": subjects,
                "tests": tests,
            }
        )

    def build(
        self,
        kind: str,
        *,
        omit: set[str] | None = None,
        overrides: dict[str, bytes] | None = None,
    ) -> tuple[PurePosixPath, dict]:
        omit = omit or set()
        overrides = overrides or {}
        required_kinds = set(CONTRACT.REQUIRED_ATTACHMENTS[kind])
        if kind == "ALLOCATOR_SELECTION":
            required_kinds.update(CONTRACT.ALLOCATOR_V1_AUTHORITY_ATTACHMENTS)
        required = sorted(required_kinds - omit)
        output = PurePosixPath(f"docs/v2/evidence/v2-m3/children/{kind}.json")
        rows: list[dict] = []
        normalized_summaries: list[dict[str, int]] = []
        allocator_raw: bytes | None = None
        allocator_authority: dict | None = None
        if kind == "ALLOCATOR_SELECTION":
            allocator_raw = allocator_receipt_fixture(self.root, self.tested)
            allocator_authority = CONTRACT.validate_allocator_verification(
                CONTRACT.load_canonical_json(
                    allocator_raw,
                    "allocator governed raw verification fixture",
                    CONTRACT.ALLOCATOR_VERIFICATION_MAX_BYTES,
                ),
                self.root,
                self.tested,
            )
        for index, attachment_kind in enumerate(required):
            if attachment_kind in overrides:
                raw = overrides[attachment_kind]
            elif attachment_kind == "JUNIT_SUMMARY":
                raw = self.junit(kind)
            elif attachment_kind == "MUTATION_MANIFEST":
                raw = (SOURCE_ROOT / "docs/v2/wire/nwg1-v1-golden-manifest.json").read_bytes()
            elif attachment_kind == "ALLOCATOR_RAW_VERIFICATION":
                assert allocator_raw is not None
                raw = allocator_raw
            elif attachment_kind == "LOCAL_CAP_RESULT":
                raw = local_cap_result_fixture(self.root, self.tested)
            elif attachment_kind == "RECOVERY_MANIFEST":
                raw = recovery_manifest_fixture()
            elif attachment_kind == "PROTOCOL_FIXTURE":
                raw = protocol_fixture_fixture()
            elif attachment_kind in CONTRACT.NORMALIZED_TYPED_KINDS:
                raw = self.typed(attachment_kind, kind)
            else:
                raw = f"artifact:{kind}:{attachment_kind}".encode()
            relative = output.parent / "attachments" / f"{index:02d}-{attachment_kind}.bin"
            path = self.root.joinpath(*relative.parts)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            rows.append(
                {
                    "bytes": len(raw),
                    "kind": attachment_kind,
                    "path": str(relative),
                    "sha256": CONTRACT.sha256(raw),
                }
            )
            if attachment_kind == "JUNIT_SUMMARY":
                normalized_summaries.append(
                    CONTRACT.validate_junit(
                        CONTRACT.load_canonical_json(
                            raw, str(relative), CONTRACT.SEALED_JUNIT_MAX_BYTES
                        ),
                        self.root,
                        self.tested,
                        kind,
                    )
                )
            elif attachment_kind == "ALLOCATOR_RAW_VERIFICATION":
                assert allocator_authority is not None
                normalized_summaries.extend(
                    [
                        allocator_authority["rawSummary"],
                        allocator_authority["verifierSummary"],
                    ]
                )
            elif attachment_kind == "LOCAL_CAP_RESULT":
                normalized_summaries.append(
                    CONTRACT.validate_local_cap_result(
                        CONTRACT.load_canonical_json(raw, str(relative)),
                        self.root,
                        self.tested,
                    )
                )
            elif attachment_kind in CONTRACT.ALLOCATOR_DERIVED_KINDS:
                assert allocator_authority is not None
                CONTRACT.validate_allocator_derived_evidence(
                    CONTRACT.load_canonical_json(raw, str(relative)),
                    attachment_kind,
                    self.tested,
                    self.bindings[(kind, attachment_kind)],
                    allocator_authority,
                )
            elif attachment_kind in CONTRACT.NORMALIZED_TYPED_KINDS:
                normalized_summaries.append(
                    CONTRACT.validate_native_result(
                        CONTRACT.load_canonical_json(
                            raw, str(relative), CONTRACT.SEALED_NATIVE_MAX_BYTES
                        ),
                        self.root,
                        kind,
                        self.tested,
                        self.bindings[(kind, attachment_kind)],
                    )
                    if attachment_kind == "NATIVE_RESULT"
                    else CONTRACT.validate_typed_evidence(
                        CONTRACT.load_canonical_json(raw, str(relative)),
                        attachment_kind,
                        kind,
                        self.tested,
                        self.bindings[(kind, attachment_kind)],
                    )
                )
        receipt = {
            "attachments": rows,
            "exclusions": CONTRACT.expected_exclusions(kind),
            "kind": kind,
            "promotionEligible": False,
            "result": CONTRACT.CHILD_RESULTS[kind],
            "schema": CONTRACT.SCHEMA,
            "sourceTuple": CONTRACT.expected_source_tuple(self.root, self.tested),
            "testSummary": CONTRACT._sum_summaries(normalized_summaries),
        }
        destination = self.root.joinpath(*output.parts)
        destination.write_bytes(self.canonical(receipt))
        return output, receipt

    def build_allocator_v2(self) -> tuple[PurePosixPath, dict]:
        output = PurePosixPath("docs/v2/evidence/v2-m3/children/ALLOCATOR_SELECTION_V2.json")
        campaign = allocator_v2_campaign_fixture(self.root, self.tested)
        raw_by_kind = {
            "ALLOCATOR_V2_CAMPAIGN_VERIFICATION": campaign["receipt"],
            "JUNIT_SUMMARY": self.junit("ALLOCATOR_SELECTION"),
        }
        rows: list[dict] = []
        summaries: list[dict[str, int]] = []
        for index, attachment_kind in enumerate(sorted(raw_by_kind)):
            raw = raw_by_kind[attachment_kind]
            relative = output.parent / "attachments-v2" / f"{index:02d}-{attachment_kind}.json"
            path = self.root.joinpath(*relative.parts)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            rows.append(
                {
                    "bytes": len(raw),
                    "kind": attachment_kind,
                    "path": str(relative),
                    "sha256": CONTRACT.sha256(raw),
                }
            )
            if attachment_kind == "JUNIT_SUMMARY":
                summaries.append(
                    CONTRACT.validate_junit(
                        CONTRACT.load_canonical_json(raw, str(relative)),
                        self.root,
                        self.tested,
                        "ALLOCATOR_SELECTION",
                    )
                )
            else:
                authority = CONTRACT.validate_allocator_v2_campaign_verification(
                    CONTRACT.load_canonical_json(
                        raw, str(relative), CONTRACT.ALLOCATOR_V2_VERIFICATION_MAX_BYTES
                    ),
                    self.root,
                    self.tested,
                )
                summaries.append(authority["summary"])
        receipt = {
            "attachments": rows,
            "exclusions": CONTRACT.expected_exclusions("ALLOCATOR_SELECTION"),
            "kind": "ALLOCATOR_SELECTION",
            "promotionEligible": False,
            "result": CONTRACT.CHILD_RESULTS["ALLOCATOR_SELECTION"],
            "schema": CONTRACT.SCHEMA,
            "sourceTuple": CONTRACT.expected_source_tuple(self.root, self.tested),
            "testSummary": CONTRACT._sum_summaries(summaries),
        }
        destination = self.root.joinpath(*output.parts)
        destination.write_bytes(self.canonical(receipt))
        return output, receipt

    def rewrite(self, output: PurePosixPath, value: dict) -> None:
        self.root.joinpath(*output.parts).write_bytes(self.canonical(value))

    def rebind_attachment(self, output: PurePosixPath, receipt: dict, kind: str, raw: bytes) -> None:
        row = next(item for item in receipt["attachments"] if item["kind"] == kind)
        (self.root / row["path"]).write_bytes(raw)
        row["bytes"] = len(raw)
        row["sha256"] = CONTRACT.sha256(raw)
        self.rewrite(output, receipt)


class M3ChildCheckerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = Fixture()

    def tearDown(self) -> None:
        self.fixture.cleanup()

    def test_closed_inventory_and_every_generic_kind_pass(self) -> None:
        self.assertEqual(11, len(CONTRACT.CHILD_KINDS))
        self.assertEqual(10, len(CONTRACT.GENERIC_CHILD_KINDS))
        for kind in CONTRACT.GENERIC_CHILD_KINDS:
            with self.subTest(kind=kind):
                # Each child needs a fresh output path, but evidence-only dirt is admitted.
                output, _ = self.fixture.build(kind)
                identity = CONTRACT.build_final_child_identity(
                    self.fixture.root, output, kind, self.fixture.tested
                )
                self.assertEqual(kind, identity["kind"])
                self.assertGreater(identity["tests"], 0)
                self.assertEqual(0, identity["skipped"])
                self.assertFalse(identity["promotionEligible"])

    def test_preselection_source_locks_allow_non_allocator_children_only(self) -> None:
        fixture = Fixture("UNSELECTED")
        try:
            output, _ = fixture.build("C1_REAL_PROVIDER_KMS")
            identity = CONTRACT.build_final_child_identity(
                fixture.root, output, "C1_REAL_PROVIDER_KMS", fixture.tested
            )
            self.assertEqual("C1_REAL_PROVIDER_KMS", identity["kind"])
            with self.assertRaisesRegex(
                CONTRACT.ChildError, "requires a selected source-lock mode"
            ):
                fixture.build("ALLOCATOR_SELECTION")
        finally:
            fixture.cleanup()

    def test_accepts_exact_allocator_v2_campaign_profile(self) -> None:
        output, _ = self.fixture.build_allocator_v2()
        identity = CONTRACT.build_final_child_identity(
            self.fixture.root, output, "ALLOCATOR_SELECTION", self.fixture.tested
        )
        self.assertEqual("ALLOCATOR_SELECTION", identity["kind"])
        self.assertEqual(8, identity["tests"])
        self.assertEqual(0, identity["skipped"])

    def test_accepts_exact_allocator_v2_range_campaign_profile(self) -> None:
        fixture = Fixture("RANGE")
        try:
            output, _ = fixture.build_allocator_v2()
            identity = CONTRACT.build_final_child_identity(
                fixture.root, output, "ALLOCATOR_SELECTION", fixture.tested
            )
            self.assertEqual("ALLOCATOR_SELECTION", identity["kind"])
            self.assertEqual(8, identity["tests"])
            self.assertEqual(0, identity["skipped"])
        finally:
            fixture.cleanup()

    def test_rejects_mixed_allocator_v1_v2_authority_profiles(self) -> None:
        output, receipt = self.fixture.build_allocator_v2()
        raw = allocator_receipt_fixture(self.fixture.root, self.fixture.tested)
        relative = output.parent / "attachments-v2" / "00-ALLOCATOR_RAW_VERIFICATION.json"
        path = self.fixture.root.joinpath(*relative.parts)
        path.write_bytes(raw)
        receipt["attachments"].append(
            {
                "bytes": len(raw),
                "kind": "ALLOCATOR_RAW_VERIFICATION",
                "path": str(relative),
                "sha256": CONTRACT.sha256(raw),
            }
        )
        receipt["attachments"].sort(key=lambda row: row["kind"])
        self.fixture.rewrite(output, receipt)
        with self.assertRaisesRegex(CONTRACT.ChildError, "mixes or incompletely"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "ALLOCATOR_SELECTION", self.fixture.tested
            )

    def test_rejects_allocator_v2_external_attachment_tamper(self) -> None:
        output, _ = self.fixture.build_allocator_v2()
        external = (
            self.fixture.root
            / "nereus-metadata-oxia/build/m3-allocator-evidence/v2-fixture/attachments/000.bin"
        )
        external.write_bytes(external.read_bytes() + b"tamper")
        with self.assertRaisesRegex(CONTRACT.ChildError, "bytes/SHA differ"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "ALLOCATOR_SELECTION", self.fixture.tested
            )

    def test_rejects_fully_rehashed_allocator_v2_caller_disposition_forgery(self) -> None:
        output, receipt = self.fixture.build_allocator_v2()
        row = next(
            item
            for item in receipt["attachments"]
            if item["kind"] == "ALLOCATOR_V2_CAMPAIGN_VERIFICATION"
        )
        wrapper = json.loads((self.fixture.root / row["path"]).read_bytes())
        checkpoint = bytearray(base64.b64decode(wrapper["checkpointBase64"]))
        offset = 8 + 2 + 1 + 1 + 8 + 40 + 32 * 6 + 7 * 8 + 4 + 288 * 4
        record_count = struct.unpack_from(">I", checkpoint, offset)[0]
        offset += 4
        for _ in range(record_count):
            tag = checkpoint[offset]
            if tag == 1:
                offset += 1 + 4 + 21 * 8 + 32
            elif tag == 2:
                offset += 1 + 1 + 4 + 4 + 4 + 10 * 8 + 32
            else:
                raise AssertionError("allocator V2 fixture observation tag differs")
        disposition_count = struct.unpack_from(">I", checkpoint, offset)[0]
        self.assertGreater(disposition_count, 0)
        first_kind_offset = offset + 4 + 4
        checkpoint[first_kind_offset] = (checkpoint[first_kind_offset] + 1) % 6
        checkpoint_raw = bytes(checkpoint)
        checkpoint_sha = CONTRACT.sha256(checkpoint_raw)

        evaluation = bytearray(base64.b64decode(wrapper["evaluationBase64"]))
        evaluation[220:252] = bytes.fromhex(checkpoint_sha)
        evaluation_raw = bytes(evaluation)
        decision = json.loads(base64.b64decode(wrapper["promotionDecisionBase64"]))
        decision["checkpointSha256"] = checkpoint_sha
        decision["evaluationSha256"] = CONTRACT.sha256(evaluation_raw)
        decision_raw = CONTRACT.canonical_bytes(decision)

        for prefix, raw in (
            ("checkpoint", checkpoint_raw),
            ("evaluation", evaluation_raw),
            ("promotionDecision", decision_raw),
        ):
            wrapper[f"{prefix}Base64"] = base64.b64encode(raw).decode("ascii")
            wrapper[f"{prefix}Sha256"] = CONTRACT.sha256(raw)
        wrapper["receiptSha256"] = "0" * 64
        wrapper["receiptSha256"] = CONTRACT.sha256(CONTRACT.canonical_bytes(wrapper))
        self.fixture.rebind_attachment(
            output,
            receipt,
            "ALLOCATOR_V2_CAMPAIGN_VERIFICATION",
            CONTRACT.canonical_bytes(wrapper),
        )
        with self.assertRaisesRegex(CONTRACT.ChildError, "caller dispositions differ"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "ALLOCATOR_SELECTION", self.fixture.tested
            )

    def test_rejects_allocator_v2_non_promotable_decision_as_selection_child(self) -> None:
        output, receipt = self.fixture.build_allocator_v2()
        row = next(
            item
            for item in receipt["attachments"]
            if item["kind"] == "ALLOCATOR_V2_CAMPAIGN_VERIFICATION"
        )
        wrapper = json.loads((self.fixture.root / row["path"]).read_bytes())
        decision = json.loads(base64.b64decode(wrapper["promotionDecisionBase64"]))
        decision["status"] = "NON_PROMOTABLE_EVALUATION"
        decision["selectedCandidate"] = "NONE"
        decision_raw = CONTRACT.canonical_bytes(decision)
        wrapper["promotionDecisionBase64"] = base64.b64encode(decision_raw).decode("ascii")
        wrapper["promotionDecisionSha256"] = CONTRACT.sha256(decision_raw)
        wrapper["receiptSha256"] = "0" * 64
        wrapper["receiptSha256"] = CONTRACT.sha256(CONTRACT.canonical_bytes(wrapper))
        self.fixture.rebind_attachment(
            output,
            receipt,
            "ALLOCATOR_V2_CAMPAIGN_VERIFICATION",
            CONTRACT.canonical_bytes(wrapper),
        )
        with self.assertRaisesRegex(CONTRACT.ChildError, "promotion decision"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "ALLOCATOR_SELECTION", self.fixture.tested
            )

    def test_rejects_attachment_tamper(self) -> None:
        output, value = self.fixture.build("AB_NWG1_WIRE")
        path = self.fixture.root / value["attachments"][0]["path"]
        path.write_bytes(path.read_bytes() + b"tamper")
        with self.assertRaisesRegex(CONTRACT.ChildError, "bytes/SHA differ"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "AB_NWG1_WIRE", self.fixture.tested
            )

    def test_rejects_fully_rehashed_recovery_manifest_substitution(self) -> None:
        output, value = self.fixture.build("R_CONTROL_RECOVERY")
        raw = recovery_manifest_fixture().replace(
            b"strict canonical Root Pointer Seal checkpoint round trip",
            b"caller substituted claim",
        )
        self.fixture.rebind_attachment(output, value, "RECOVERY_MANIFEST", raw)
        with self.assertRaisesRegex(CONTRACT.ChildError, "closed row inventory differs"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "R_CONTROL_RECOVERY", self.fixture.tested
            )

    def test_rejects_fully_rehashed_protocol_fixture_state_substitution(self) -> None:
        output, value = self.fixture.build("K_NWKCP1")
        lines = protocol_fixture_fixture().decode().splitlines()
        fields = lines[2].split("\t")
        body = bytearray.fromhex(fields[5])
        body[52] = 1
        fields[4] = CONTRACT.sha256(bytes(body))
        fields[5] = body.hex()
        lines[2] = "\t".join(fields)
        raw = ("\n".join(lines) + "\n").encode()
        self.fixture.rebind_attachment(output, value, "PROTOCOL_FIXTURE", raw)
        with self.assertRaisesRegex(CONTRACT.ChildError, "Head wire/context/state differs"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "K_NWKCP1", self.fixture.tested
            )

    def test_parses_closed_large_mutation_manifest_under_dedicated_cap(self) -> None:
        output, value = self.fixture.build("AB_NWG1_WIRE")
        identity = CONTRACT.build_final_child_identity(
            self.fixture.root, output, "AB_NWG1_WIRE", self.fixture.tested
        )
        mutation = next(row for row in identity["attachments"] if row["kind"] == "MUTATION_MANIFEST")
        self.assertGreater(mutation["bytes"], CONTRACT.MAX_CANONICAL_BYTES)
        self.assertLessEqual(mutation["bytes"], CONTRACT.MUTATION_MANIFEST_MAX_BYTES)

        manifest = json.loads((self.fixture.root / mutation["path"]).read_bytes())
        manifest["mutations"][0]["callerClaimedPass"] = True
        self.fixture.rebind_attachment(
            output, value, "MUTATION_MANIFEST", CONTRACT.canonical_bytes(manifest)
        )
        with self.assertRaisesRegex(CONTRACT.ChildError, "member set differs"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "AB_NWG1_WIRE", self.fixture.tested
            )

    def test_rejects_mutation_manifest_above_one_mib_cap(self) -> None:
        output, value = self.fixture.build("AB_NWG1_WIRE")
        raw = b"{" + b" " * CONTRACT.MUTATION_MANIFEST_MAX_BYTES + b"}"
        self.fixture.rebind_attachment(output, value, "MUTATION_MANIFEST", raw)
        with self.assertRaisesRegex(CONTRACT.ChildError, "canonical JSON bytes outside cap"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "AB_NWG1_WIRE", self.fixture.tested
            )

    def test_rejects_skip_even_when_receipt_counters_are_rebound(self) -> None:
        output, value = self.fixture.build("D_LOCAL_CAP")
        attachment = value["attachments"][0]
        raw = self.fixture.junit("D_LOCAL_CAP", skipped=1)
        path = self.fixture.root / attachment["path"]
        path.write_bytes(raw)
        attachment["bytes"] = len(raw)
        attachment["sha256"] = CONTRACT.sha256(raw)
        value["testSummary"]["skipped"] = 1
        self.fixture.rewrite(output, value)
        with self.assertRaisesRegex(CONTRACT.ChildError, "must be zero"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "D_LOCAL_CAP", self.fixture.tested
            )

    def test_rejects_legacy_caller_supplied_junit_counter_object(self) -> None:
        output, receipt = self.fixture.build("D_LOCAL_CAP")
        legacy = self.fixture.canonical(
            {
                "errors": 0,
                "failures": 0,
                "nereusCommit": self.fixture.tested,
                "schema": "NEREUS_V2_M3_NORMALIZED_JUNIT_V1",
                "skipped": 0,
                "tests": 3,
            }
        )
        self.fixture.rebind_attachment(output, receipt, "JUNIT_SUMMARY", legacy)
        with self.assertRaisesRegex(CONTRACT.ChildError, "member set differs"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "D_LOCAL_CAP", self.fixture.tested
            )

    def test_d1_requires_exact_six_executed_source_bound_records_and_no_provider_claim(self) -> None:
        output, receipt = self.fixture.build("D_LOCAL_CAP")
        identity = CONTRACT.build_final_child_identity(
            self.fixture.root, output, "D_LOCAL_CAP", self.fixture.tested
        )
        self.assertEqual(12, identity["tests"])
        attachment = next(
            row for row in receipt["attachments"] if row["kind"] == "LOCAL_CAP_RESULT"
        )
        value = json.loads((self.fixture.root / attachment["path"]).read_bytes())
        self.assertEqual(
            [record[0] for record in CONTRACT.LOCAL_CAP_RECORDS],
            [record["name"] for record in value["records"]],
        )
        self.assertFalse(value["providerTransferClaimed"])
        value["providerTransferClaimed"] = True
        value["receiptSha256"] = "0" * 64
        value["receiptSha256"] = CONTRACT.sha256(CONTRACT.canonical_bytes(value))
        self.fixture.rebind_attachment(
            output, receipt, "LOCAL_CAP_RESULT", CONTRACT.canonical_bytes(value)
        )
        with self.assertRaisesRegex(CONTRACT.ChildError, "six-record source-bound"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "D_LOCAL_CAP", self.fixture.tested
            )

    def test_d1_rejects_fully_rehashed_junit_without_one_required_component_test(self) -> None:
        output, receipt = self.fixture.build("D_LOCAL_CAP")
        attachment = next(
            row for row in receipt["attachments"] if row["kind"] == "JUNIT_SUMMARY"
        )
        wrapper = json.loads((self.fixture.root / attachment["path"]).read_bytes())
        xml_row = wrapper["junitXml"][0]
        suite = ET.fromstring(base64.b64decode(xml_row["xmlBase64"], validate=True))
        removed = next(
            case
            for case in suite.findall("testcase")
            if case.attrib["name"]
            == "localParserRecordExercisesLengthsFirstEnvelopeParser"
        )
        suite.remove(removed)
        suite.attrib["tests"] = str(int(suite.attrib["tests"]) - 1)
        raw_xml = ET.tostring(suite, encoding="utf-8")
        xml_row.update(
            {
                "bytes": len(raw_xml),
                "sha256": CONTRACT.sha256(raw_xml),
                "xmlBase64": base64.b64encode(raw_xml).decode("ascii"),
            }
        )
        wrapper["receiptSha256"] = "0" * 64
        wrapper["receiptSha256"] = CONTRACT.sha256(CONTRACT.canonical_bytes(wrapper))
        receipt["testSummary"]["tests"] -= 1
        self.fixture.rebind_attachment(
            output, receipt, "JUNIT_SUMMARY", CONTRACT.canonical_bytes(wrapper)
        )
        with self.assertRaisesRegex(CONTRACT.ChildError, "lacks required child tests"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "D_LOCAL_CAP", self.fixture.tested
            )

    def test_every_ordinary_layer_has_exact_source_bound_junit_allowlist(self) -> None:
        self.assertEqual(
            {
                "AB_NWG1_WIRE",
                "ALLOCATOR_SELECTION",
                "C1_REAL_PROVIDER_KMS",
                "C2_SEGMENTED_PREFIX",
                "C_OBJECT_WAL_STATE_TRACE",
                "D_LOCAL_CAP",
                "K_NWKCP1",
                "R_CONTROL_RECOVERY",
            },
            set(CONTRACT.GOVERNED_JUNIT_REQUIRED_TESTS),
        )
        for kind, required in CONTRACT.GOVERNED_JUNIT_REQUIRED_TESTS.items():
            with self.subTest(kind=kind):
                self.assertGreater(len(required), 0)
                for test_class, method in required:
                    self.assertTrue(test_class)
                    self.assertTrue(method)
                    self.assertIn(test_class, CONTRACT.GOVERNED_JUNIT_SOURCE_PATHS)

        output, receipt = self.fixture.build("AB_NWG1_WIRE")
        attachment = next(
            row for row in receipt["attachments"] if row["kind"] == "JUNIT_SUMMARY"
        )
        wrapper = json.loads((self.fixture.root / attachment["path"]).read_bytes())
        target_method = "allEightyFourRecordsAreExplicitAndExecuteTwoHundredFortyPaths"
        xml_row = next(
            row
            for row in wrapper["junitXml"]
            if target_method in base64.b64decode(row["xmlBase64"]).decode()
        )
        suite = ET.fromstring(base64.b64decode(xml_row["xmlBase64"], validate=True))
        suite.remove(next(case for case in suite.findall("testcase") if case.attrib["name"] == target_method))
        suite.attrib["tests"] = str(int(suite.attrib["tests"]) - 1)
        raw_xml = ET.tostring(suite, encoding="utf-8")
        xml_row.update(
            {
                "bytes": len(raw_xml),
                "sha256": CONTRACT.sha256(raw_xml),
                "xmlBase64": base64.b64encode(raw_xml).decode("ascii"),
            }
        )
        wrapper["receiptSha256"] = "0" * 64
        wrapper["receiptSha256"] = CONTRACT.sha256(CONTRACT.canonical_bytes(wrapper))
        receipt["testSummary"]["tests"] -= 1
        self.fixture.rebind_attachment(
            output, receipt, "JUNIT_SUMMARY", CONTRACT.canonical_bytes(wrapper)
        )
        with self.assertRaisesRegex(CONTRACT.ChildError, "lacks required child tests"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "AB_NWG1_WIRE", self.fixture.tested
            )

    def test_rejects_fake_real_provider_and_kms(self) -> None:
        for attachment_kind in ("PROVIDER_REAL_RECEIPT", "KMS_REAL_RECEIPT"):
            with self.subTest(attachment_kind=attachment_kind):
                fixture = Fixture()
                try:
                    raw = fixture.typed(
                        attachment_kind, "C1_REAL_PROVIDER_KMS", fake=True
                    )
                    # Build with valid bytes, then replace and rebind to reach source authenticity checks.
                    output, value = fixture.build("C1_REAL_PROVIDER_KMS")
                    row = next(item for item in value["attachments"] if item["kind"] == attachment_kind)
                    (fixture.root / row["path"]).write_bytes(raw)
                    row["bytes"] = len(raw)
                    row["sha256"] = CONTRACT.sha256(raw)
                    fixture.rewrite(output, value)
                    with self.assertRaisesRegex(CONTRACT.ChildError, "receipt schema/result/backend"):
                        CONTRACT.build_final_child_identity(
                            fixture.root, output, "C1_REAL_PROVIDER_KMS", fixture.tested
                        )
                finally:
                    fixture.cleanup()

    def test_rejects_stale_real_receipt_source_and_forged_contract_digest(self) -> None:
        cases = (
            (
                "PROVIDER_REAL_RECEIPT",
                provider_raw_receipt_fixture,
                "candidateRootAdmissionContractSha256",
            ),
            ("KMS_REAL_RECEIPT", kms_raw_receipt_fixture, "contractSha256"),
        )
        for attachment_kind, factory, digest_field in cases:
            for mutation in ("stale-source", "forged-contract"):
                with self.subTest(attachment_kind=attachment_kind, mutation=mutation):
                    fixture = Fixture()
                    try:
                        output, receipt = fixture.build("C1_REAL_PROVIDER_KMS")
                        value = factory(fixture.tested)
                        if mutation == "stale-source":
                            value["nereusCommit"] = "0" * 40
                            expected = "sealed real execution receipt schema/source"
                        else:
                            value[digest_field] = "0" * 64
                            expected = "contract digest differs"
                        sealed = CONTRACT.seal_real_execution_receipt(
                            CONTRACT.canonical_bytes(value),
                            real_junit_xml_fixture(attachment_kind),
                            attachment_kind,
                            value["nereusCommit"] if mutation == "stale-source" else fixture.tested,
                        )
                        fixture.rebind_attachment(
                            output,
                            receipt,
                            attachment_kind,
                            CONTRACT.canonical_bytes(sealed),
                        )
                        with self.assertRaisesRegex(CONTRACT.ChildError, expected):
                            CONTRACT.build_final_child_identity(
                                fixture.root,
                                output,
                                "C1_REAL_PROVIDER_KMS",
                                fixture.tested,
                            )
                    finally:
                        fixture.cleanup()

    def test_rejects_missing_native_attachment(self) -> None:
        for kind in ("U_KAFKA_OBJECT_WAL", "P_PULSAR_OBJECT_WAL"):
            with self.subTest(kind=kind):
                output, _ = self.fixture.build(kind, omit={"NATIVE_RESULT"})
                with self.assertRaisesRegex(CONTRACT.ChildError, "mandatory typed attachments"):
                    CONTRACT.build_final_child_identity(
                        self.fixture.root, output, kind, self.fixture.tested
                    )

    def test_rejects_native_self_report_after_raw_wrapper_and_xml_hashes_are_resealed(self) -> None:
        for kind in ("U_KAFKA_OBJECT_WAL", "P_PULSAR_OBJECT_WAL"):
            with self.subTest(kind=kind):
                output, receipt = self.fixture.build(kind)
                attachment = next(
                    row for row in receipt["attachments"] if row["kind"] == "NATIVE_RESULT"
                )
                wrapper = json.loads((self.fixture.root / attachment["path"]).read_bytes())
                raw_result = json.loads(base64.b64decode(wrapper["rawEvidenceBase64"]))
                xml = base64.b64decode(wrapper["junitXml"][0]["xmlBase64"])
                suite = CONTRACT.NATIVE_PROFILES[kind]["suites"][0]
                required = next(
                    test_id.removeprefix(suite + "#")
                    for test_id in CONTRACT.NATIVE_PROFILES[kind]["requiredTests"]
                    if test_id.startswith(suite + "#")
                )
                xml = xml.replace(required.encode(), b"forgedSelfReportedPass", 1)
                xml_sha = CONTRACT.sha256(xml)
                raw_result["junit"]["xmlFiles"][0]["sha256"] = xml_sha
                counter_keys = tuple(CONTRACT.NATIVE_PROFILES[kind]["counters"])
                raw_result["receiptSha256"] = CONTRACT.sha256(
                    CONTRACT._native_canonical_bytes(raw_result, counter_keys, "0" * 64)
                )
                raw = CONTRACT._native_canonical_bytes(raw_result, counter_keys)
                wrapper["rawEvidenceBase64"] = base64.b64encode(raw).decode("ascii")
                wrapper["rawEvidenceSha256"] = CONTRACT.sha256(raw)
                wrapper["junitXml"][0]["bytes"] = len(xml)
                wrapper["junitXml"][0]["sha256"] = xml_sha
                wrapper["junitXml"][0]["xmlBase64"] = base64.b64encode(xml).decode("ascii")
                wrapper["receiptSha256"] = "0" * 64
                wrapper["receiptSha256"] = CONTRACT.sha256(CONTRACT.canonical_bytes(wrapper))
                self.fixture.rebind_attachment(
                    output, receipt, "NATIVE_RESULT", CONTRACT.canonical_bytes(wrapper)
                )
                with self.assertRaisesRegex(CONTRACT.ChildError, "lacks required tests"):
                    CONTRACT.build_final_child_identity(
                        self.fixture.root, output, kind, self.fixture.tested
                    )

    def test_rejects_incomplete_allocator_evidence(self) -> None:
        output, _ = self.fixture.build(
            "ALLOCATOR_SELECTION", omit={"ALLOCATOR_SCALE_100000_SUMMARY"}
        )
        with self.assertRaisesRegex(CONTRACT.ChildError, "complete V1 or V2 authority"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "ALLOCATOR_SELECTION", self.fixture.tested
            )

    def test_rejects_fully_rehashed_allocator_raw_self_report(self) -> None:
        output, receipt = self.fixture.build("ALLOCATOR_SELECTION")
        attachment = next(
            row
            for row in receipt["attachments"]
            if row["kind"] == "ALLOCATOR_RAW_VERIFICATION"
        )
        wrapper = json.loads((self.fixture.root / attachment["path"]).read_bytes())
        java = json.loads(base64.b64decode(wrapper["allocatorVerificationBase64"]))
        raw = java["rawVerification"]
        raw["derived"]["intervals"] = 289
        raw["selfSha256"] = "0" * 64
        raw_zeroed = json.dumps(
            raw,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=False,
        ).encode() + b"\n"
        raw["selfSha256"] = CONTRACT.sha256(raw_zeroed)
        raw_bytes = json.dumps(
            raw,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=False,
        ).encode() + b"\n"
        java["rawVerificationBytes"] = len(raw_bytes)
        java["rawVerificationSha256"] = CONTRACT.sha256(raw_bytes)
        java["selfSha256"] = "0" * 64
        java_zeroed = json.dumps(
            java,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=False,
        ).encode() + b"\n"
        java["selfSha256"] = CONTRACT.sha256(java_zeroed)
        java_bytes = json.dumps(
            java,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=False,
        ).encode() + b"\n"
        wrapper["allocatorVerificationBase64"] = base64.b64encode(java_bytes).decode("ascii")
        wrapper["allocatorVerificationSha256"] = CONTRACT.sha256(java_bytes)
        wrapper["receiptSha256"] = "0" * 64
        wrapper["receiptSha256"] = CONTRACT.sha256(CONTRACT.canonical_bytes(wrapper))
        self.fixture.rebind_attachment(
            output,
            receipt,
            "ALLOCATOR_RAW_VERIFICATION",
            CONTRACT.canonical_bytes(wrapper),
        )
        with self.assertRaisesRegex(CONTRACT.ChildError, "288/9/8"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "ALLOCATOR_SELECTION", self.fixture.tested
            )

    def test_rejects_allocator_external_raw_tamper_and_symlink(self) -> None:
        output, receipt = self.fixture.build("ALLOCATOR_SELECTION")
        attachment = next(
            row
            for row in receipt["attachments"]
            if row["kind"] == "ALLOCATOR_RAW_VERIFICATION"
        )
        wrapper = json.loads((self.fixture.root / attachment["path"]).read_bytes())
        native = next(row for row in wrapper["externalFiles"] if row["name"] == "native.naea")
        native_path = self.fixture.root / native["path"]
        original = native_path.read_bytes()
        native_path.write_bytes(original + b"tamper")
        with self.assertRaisesRegex(CONTRACT.ChildError, "bytes/SHA differ"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "ALLOCATOR_SELECTION", self.fixture.tested
            )
        native_path.write_bytes(original)
        alternate = native_path.with_name("native-alternate.naea")
        alternate.write_bytes(original)
        native_path.unlink()
        native_path.symlink_to(alternate.name)
        with self.assertRaisesRegex(CONTRACT.ChildError, "symlink component"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "ALLOCATOR_SELECTION", self.fixture.tested
            )

    def test_rejects_c2_promotion_or_allowlist_authority(self) -> None:
        output, value = self.fixture.build("C2_SEGMENTED_PREFIX")
        value["promotionEligible"] = True
        self.fixture.rewrite(output, value)
        with self.assertRaisesRegex(CONTRACT.ChildError, "promotion boundary"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "C2_SEGMENTED_PREFIX", self.fixture.tested
            )
        value["promotionEligible"] = False
        value["exclusions"].remove("PRODUCTION_ALLOWLIST")
        self.fixture.rewrite(output, value)
        with self.assertRaisesRegex(CONTRACT.ChildError, "exclusions differ"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "C2_SEGMENTED_PREFIX", self.fixture.tested
            )

    def test_rejects_dirty_production_source(self) -> None:
        output, _ = self.fixture.build("D_LOCAL_CAP")
        (self.fixture.root / "production.txt").write_text("dirty production source\n")
        with self.assertRaisesRegex(CONTRACT.ChildError, "non-evidence dirty path"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "D_LOCAL_CAP", self.fixture.tested
            )

    def test_rejects_stale_source_after_production_commit(self) -> None:
        output, _ = self.fixture.build("D_LOCAL_CAP")
        (self.fixture.root / "production.txt").write_text("later production source\n")
        git(self.fixture.root, "add", "production.txt")
        git(self.fixture.root, "commit", "-m", "production changed")
        with self.assertRaisesRegex(CONTRACT.ChildError, "non-evidence path changed"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "D_LOCAL_CAP", self.fixture.tested
            )

    def test_rejects_open_schema_or_duplicate_attachment_kind(self) -> None:
        output, value = self.fixture.build("D_LOCAL_CAP")
        value["scenarioPass"] = True
        self.fixture.rewrite(output, value)
        with self.assertRaisesRegex(CONTRACT.ChildError, "member set differs"):
            CONTRACT.build_final_child_identity(
                self.fixture.root, output, "D_LOCAL_CAP", self.fixture.tested
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
