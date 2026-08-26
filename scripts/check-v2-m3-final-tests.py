#!/usr/bin/env python3
"""Positive and fail-closed tests for the M3 Final validator contract."""

from __future__ import annotations

import base64
import importlib.util
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tempfile
import unittest


CHECKER_PATH = Path(__file__).with_name("check-v2-m3-final.py")
PUBLISHER_PATH = Path(__file__).with_name("publish-v2-m3-final.py")
CHILD_CHECKER_PATH = Path(__file__).with_name("check-v2-m3-child.py")
CHILD_TEST_SUPPORT_PATH = Path(__file__).with_name("check-v2-m3-child-tests.py")
M2_PUBLISHER_PATH = Path(__file__).with_name("publish-v2-m3-m2-regression.py")
SOURCE_ROOT = Path(__file__).resolve().parent.parent
OUTPUT = PurePosixPath("docs/v2/evidence/v2-m3/final/m3-final.json")
W1_OUTPUT = PurePosixPath("docs/v2/evidence/v2-m3/w1/m2-regression/receipt.json")


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


CONTRACT = load_module("m3_final_contract_check_test", CHECKER_PATH)
PUBLISHER = load_module("m3_final_publisher_check_test", PUBLISHER_PATH)
CHILD = load_module("m3_child_contract_final_test", CHILD_CHECKER_PATH)
CHILD_TEST_SUPPORT = load_module("m3_child_contract_final_test_support", CHILD_TEST_SUPPORT_PATH)
M2_PUBLISHER = load_module("m3_m2_publisher_final_test", M2_PUBLISHER_PATH)


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *args], text=True, stderr=subprocess.STDOUT
    ).strip()


class FixtureBuilder:
    def __init__(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="nereus-m3-final-checker-")
        self.base = Path(self.temporary.name)
        self.root = self.base / "repo"
        self.root.mkdir()
        git(self.root, "init", "-b", "main")
        git(self.root, "config", "user.name", "M3 Final Test")
        git(self.root, "config", "user.email", "m3-final@example.invalid")
        source_locks = self.root / CONTRACT.SOURCE_LOCKS_PATH
        source_locks.parent.mkdir(parents=True)
        source_lock_value = json.loads((SOURCE_ROOT / CONTRACT.SOURCE_LOCKS_PATH).read_text())
        client = source_lock_value["dependencyEvidenceBindings"]["oxiaClientArtifacts"]["artifacts"]["clientJar"]
        client["bytes"] = len(CHILD_TEST_SUPPORT.ALLOCATOR_FIXTURE_CLIENT_BYTES)
        client["sha256"] = CHILD.sha256(CHILD_TEST_SUPPORT.ALLOCATOR_FIXTURE_CLIENT_BYTES)
        source_lock_value["m3AllocatorEvidenceBinding"]["oxiaClientJarBytes"] = client["bytes"]
        source_lock_value["m3AllocatorEvidenceBinding"]["oxiaClientJarSha256"] = client["sha256"]
        self.bindings = self._source_bindings(source_lock_value)
        source_lock_value["m3EvidenceBindings"] = {
            "allocatorMode": "STRICT",
            "bindings": [self.bindings[key] for key in sorted(self.bindings)],
            "schema": CONTRACT.SOURCE_BINDING_SCHEMA,
        }
        source_locks.write_text(json.dumps(source_lock_value, indent=2) + "\n")
        client_path = self.root / client["relativePath"]
        client_path.parent.mkdir(parents=True, exist_ok=True)
        client_path.write_bytes(CHILD_TEST_SUPPORT.ALLOCATOR_FIXTURE_CLIENT_BYTES)
        scenarios = self.root / CONTRACT.SCENARIO_PATH
        scenarios.write_text(
            json.dumps(
                {
                    "scenarios": [
                        {
                            "evidenceReceipt": None,
                            "id": scenario,
                            "milestone": "M3",
                            "status": "PLANNED",
                        }
                        for scenario in CONTRACT.REQUIRED_SCENARIOS
                    ],
                    "schemaVersion": 1,
                },
                indent=2,
            )
            + "\n"
        )
        historical = self.root / CHILD.M2.HISTORICAL_FINAL_PATH
        historical.parent.mkdir(parents=True, exist_ok=True)
        historical.write_bytes((SOURCE_ROOT / CHILD.M2.HISTORICAL_FINAL_PATH).read_bytes())
        CHILD_TEST_SUPPORT.write_native_source_fixture(self.root)
        CHILD_TEST_SUPPORT.write_local_cap_source_fixture(self.root)
        CHILD_TEST_SUPPORT.write_governed_junit_source_fixture(self.root)
        (self.root / "production.txt").write_text("tested M3 production source\n")
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "tested production source")
        self.tested = git(self.root, "rev-parse", "HEAD")
        (self.root / ".git/info/exclude").write_text("nereus-metadata-oxia/build/\n")
        self.w1_identity = self._publish_w1()
        self.children = self._write_children()
        git(self.root, "add", str(CONTRACT.EVIDENCE_PREFIX))
        git(self.root, "commit", "-m", "publish M3 child evidence")
        self.candidate = self.base / "candidate.json"
        self.candidate.write_bytes(CONTRACT.canonical_bytes(self.receipt_value()))

    def cleanup(self) -> None:
        self.temporary.cleanup()

    def _write(self, relative: PurePosixPath, raw: bytes) -> None:
        path = self.root.joinpath(*relative.parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

    @staticmethod
    def _source_bindings(source_locks: dict) -> dict[tuple[str, str], dict[str, str]]:
        native = source_locks["m3NativeEvidenceBindings"]
        kafka = native["kafka"]
        pulsar = native["pulsar"]
        allocator = source_locks["m3AllocatorEvidenceBinding"]
        identities = {
            "provider": (
                "MINIO_S3_COMPATIBLE|artifactReference=quay.io/minio/minio@sha256:"
                + "14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
                + "|artifactConfigDigest=sha256:"
                + "8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253"
            ),
            "kms": (
                "VAULT_TRANSIT|artifactReference=hashicorp/vault@sha256:"
                + "268bb80aa9c6d13d65fcfa05c0c268caca068952240a8087291a6ce0b66e3a10"
                + "|artifactConfigDigest=sha256:"
                + "ba5ade3c7f155978f41dbca62b16e5e08f5331f5d12f9be2d333698b49484457"
            ),
            "kafka": (
                f"KAFKA_NATIVE|repository={kafka['logicalRepository']}|commit={kafka['sourceCommit']}"
            ),
            "pulsar": (
                f"PULSAR_NATIVE|repository={pulsar['logicalRepository']}|commit={pulsar['sourceCommit']}"
            ),
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
                "OXIA_AND_PULSAR", "FAULT_INJECTION", identities["allocator"]
            ),
            ("ALLOCATOR_SELECTION", "ALLOCATOR_NATIVE_RELATIVE_SUMMARY"): (
                "PULSAR_NATIVE", "NATIVE_REFERENCE_EXECUTION", identities["pulsar"]
            ),
            ("ALLOCATOR_SELECTION", "ALLOCATOR_SCALE_10000_SUMMARY"): (
                "OXIA_AND_PULSAR", "SCALE_EXECUTION", identities["allocator"]
            ),
            ("ALLOCATOR_SELECTION", "ALLOCATOR_SCALE_100000_SUMMARY"): (
                "OXIA_AND_PULSAR", "SCALE_EXECUTION", identities["allocator"]
            ),
            ("C1_REAL_PROVIDER_KMS", "KMS_REAL_RECEIPT"): (
                "VAULT_TRANSIT", "REAL_EXTERNAL_PROCESS", identities["kms"]
            ),
            ("C1_REAL_PROVIDER_KMS", "PROVIDER_REAL_RECEIPT"): (
                "MINIO_S3_COMPATIBLE", "REAL_EXTERNAL_PROCESS", identities["provider"]
            ),
            ("P_PULSAR_OBJECT_WAL", "NATIVE_RESULT"): (
                "PULSAR_NATIVE", "NATIVE_REFERENCE_EXECUTION", identities["pulsar"]
            ),
            ("U_KAFKA_OBJECT_WAL", "NATIVE_RESULT"): (
                "KAFKA_NATIVE", "NATIVE_REFERENCE_EXECUTION", identities["kafka"]
            ),
        }
        result: dict[tuple[str, str], dict[str, str]] = {}
        for key, (backend, execution, identity) in specs.items():
            result[key] = {
                "backend": backend,
                "childKind": key[0],
                "evidenceKind": key[1],
                "executionClass": execution,
                "sourceIdentity": identity,
                "sourceProvenance": CHILD._expected_locked_provenance(
                    source_locks, key[1], backend, identity
                ),
            }
        return result

    def _publish_w1(self) -> dict:
        child_paths: list[Path] = []
        for index, gate in enumerate(CHILD.M2.REQUIRED_GATES):
            path = self.base / "w1-inputs" / f"{gate}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(CHILD.M2.canonical_bytes({
                "errors": 0,
                "executionProfile": CHILD.M2.EXECUTION_PROFILE,
                "failures": 0,
                "gateId": gate,
                "result": "PASS",
                "schema": CHILD.M2.CHILD_SCHEMA,
                "skipped": 0,
                "testedNereusCommit": self.tested,
                "tests": index + 1,
            }))
            child_paths.append(path)
        M2_PUBLISHER.publish(self.root, self.tested, W1_OUTPUT, child_paths)
        return CHILD.build_final_child_identity(
            self.root, W1_OUTPUT, "W1_CURRENT_SOURCE_M2_REGRESSION", self.tested
        )

    def _attachment(
        self, child_index: int, attachment_index: int, child_kind: str, kind: str
    ) -> tuple[dict, dict[str, int] | None]:
        path = PurePosixPath(
            f"docs/v2/evidence/v2-m3/children/{child_index:02d}-{child_kind}/"
            f"attachments/{attachment_index:02d}-{kind}.json"
        )
        summary = None
        if kind == "JUNIT_SUMMARY":
            raw = CHILD_TEST_SUPPORT.governed_junit_receipt_fixture(
                self.root, self.tested, child_kind, tests=child_index + 1
            )
            summary = CHILD.validate_junit(
                CHILD.load_canonical_json(
                    raw, str(path), CHILD.SEALED_JUNIT_MAX_BYTES
                ),
                self.root,
                self.tested,
                child_kind,
            )
        elif kind == "PROVIDER_REAL_RECEIPT":
            value = CHILD_TEST_SUPPORT.provider_receipt_fixture(self.tested)
            summary = {"errors": 0, "failures": 0, "skipped": 0, "tests": 1}
            raw = CHILD.canonical_bytes(value)
        elif kind == "KMS_REAL_RECEIPT":
            value = CHILD_TEST_SUPPORT.kms_receipt_fixture(self.tested)
            summary = {"errors": 0, "failures": 0, "skipped": 0, "tests": 1}
            raw = CHILD.canonical_bytes(value)
        elif kind == "NATIVE_RESULT":
            raw = CHILD_TEST_SUPPORT.native_receipt_fixture(
                self.root,
                self.tested,
                child_kind,
                self.bindings[(child_kind, kind)],
            )
            summary = CHILD.validate_native_result(
                CHILD.load_canonical_json(raw, str(path), CHILD.SEALED_NATIVE_MAX_BYTES),
                self.root,
                child_kind,
                self.tested,
                self.bindings[(child_kind, kind)],
            )
        elif kind == "LOCAL_CAP_RESULT":
            raw = CHILD_TEST_SUPPORT.local_cap_result_fixture(self.root, self.tested)
            summary = {"errors": 0, "failures": 0, "skipped": 0, "tests": 6}
        elif kind == "ALLOCATOR_RAW_VERIFICATION":
            raw = CHILD_TEST_SUPPORT.allocator_receipt_fixture(self.root, self.tested)
            authority = CHILD.validate_allocator_verification(
                CHILD.load_canonical_json(
                    raw, str(path), CHILD.ALLOCATOR_VERIFICATION_MAX_BYTES
                ),
                self.root,
                self.tested,
            )
            summary = CHILD._sum_summaries(
                [authority["rawSummary"], authority["verifierSummary"]]
            )
        elif kind in CHILD.ALLOCATOR_DERIVED_KINDS:
            binding = self.bindings[(child_kind, kind)]
            _, _, subject_count = CHILD._expected_typed_profile(kind, child_kind)
            value = {
                "backend": binding["backend"],
                "errors": 0,
                "evidenceKind": kind,
                "executionClass": binding["executionClass"],
                "failures": 0,
                "nereusCommit": self.tested,
                "result": "PASS_EVIDENCE_ONLY",
                "schema": CHILD.TYPED_SCHEMA,
                "skipped": 0,
                "sourceIdentity": binding["sourceIdentity"],
                "subjectCount": subject_count,
                "tests": 1,
            }
            raw = CHILD.canonical_bytes(value)
        elif kind in CHILD.NORMALIZED_TYPED_KINDS:
            binding = self.bindings[(child_kind, kind)]
            _, _, subject_count = CHILD._expected_typed_profile(kind, child_kind)
            summary = {"errors": 0, "failures": 0, "skipped": 0, "tests": attachment_index + 1}
            value = {
                "backend": binding["backend"],
                **summary,
                "evidenceKind": kind,
                "executionClass": binding["executionClass"],
                "nereusCommit": self.tested,
                "result": "PASS_EVIDENCE_ONLY",
                "schema": CHILD.TYPED_SCHEMA,
                "sourceIdentity": binding["sourceIdentity"],
                "subjectCount": subject_count,
            }
            raw = CHILD.canonical_bytes(value)
        elif kind == "MUTATION_MANIFEST":
            raw = (SOURCE_ROOT / "docs/v2/wire/nwg1-v1-golden-manifest.json").read_bytes()
        else:
            raw = f"attachment:{child_index}:{child_kind}:{kind}".encode()
        self._write(path, raw)
        return (
            {"bytes": len(raw), "kind": kind, "path": str(path), "sha256": CONTRACT.sha256(raw)},
            summary,
        )

    def _write_children(self) -> list[dict]:
        children: list[dict] = [self.w1_identity]
        for index, kind in enumerate(CONTRACT.CHILD_KINDS):
            if kind == "W1_CURRENT_SOURCE_M2_REGRESSION":
                continue
            attachment_kinds = sorted(CHILD.REQUIRED_ATTACHMENTS[kind])
            built = [
                self._attachment(index, attachment_index, kind, attachment_kind)
                for attachment_index, attachment_kind in enumerate(attachment_kinds)
            ]
            attachments = [row for row, _ in built]
            summaries = [summary for _, summary in built if summary is not None]
            summary = {
                key: sum(row[key] for row in summaries)
                for key in ("errors", "failures", "skipped", "tests")
            }
            source_value = CHILD.expected_source_tuple(self.root, self.tested)
            child_receipt = {
                "attachments": attachments,
                "exclusions": CHILD.expected_exclusions(kind),
                "kind": kind,
                "promotionEligible": False,
                "result": CONTRACT.CHILD_RESULTS[kind],
                "schema": CHILD.SCHEMA,
                "sourceTuple": source_value,
                "testSummary": summary,
            }
            child_path = PurePosixPath(f"docs/v2/evidence/v2-m3/children/{index:02d}-{kind}.json")
            child_raw = CONTRACT.canonical_bytes(child_receipt)
            self._write(child_path, child_raw)
            children.append(
                CHILD.build_final_child_identity(self.root, child_path, kind, self.tested)
            )
        return children

    def receipt_value(self) -> dict:
        locks = CONTRACT.git_blob(self.root, self.tested, CONTRACT.SOURCE_LOCKS_PATH)
        return {
            "allocatorSelection": {
                "faultEvidence": True,
                "mode": "STRICT",
                "nativeRelativeEvidence": True,
                "scale10000": True,
                "scale100000": True,
            },
            "childReceipts": self.children,
            "exclusions": CONTRACT.FINAL_EXCLUSIONS,
            "kind": CONTRACT.KIND,
            "promotionEligible": True,
            "providerEvidence": {
                "c2PromotionEligible": False,
                "realKms": True,
                "realProvider": True,
            },
            "result": CONTRACT.RESULT,
            "scenarios": list(CONTRACT.REQUIRED_SCENARIOS),
            "schema": CONTRACT.SCHEMA,
            "sourceTuple": {
                "nereusCommit": self.tested,
                "sourceLocksSha256": CONTRACT.sha256(locks),
            },
        }

    def publish(self) -> None:
        PUBLISHER.publish(self.root, self.candidate, OUTPUT)

    def commit_final_and_sync_scenarios(self) -> None:
        git(self.root, "add", str(OUTPUT))
        git(self.root, "commit", "-m", "publish M3 Final")
        path = self.root / CONTRACT.SCENARIO_PATH
        document = json.loads(path.read_text())
        for row in document["scenarios"]:
            row["status"] = "PASSED_CURRENT_SOURCE"
            row["evidenceReceipt"] = str(OUTPUT)
        path.write_text(json.dumps(document, indent=2) + "\n")
        git(self.root, "add", str(CONTRACT.SCENARIO_PATH))
        git(self.root, "commit", "-m", "synchronize M3 scenarios")


class CheckerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = FixtureBuilder()
        self.fixture.publish()
        self.fixture.commit_final_and_sync_scenarios()

    def tearDown(self) -> None:
        self.fixture.cleanup()

    def receipt_path(self) -> Path:
        return self.fixture.root.joinpath(*OUTPUT.parts)

    def rewrite_receipt(self, value: dict) -> None:
        self.receipt_path().write_bytes(CONTRACT.canonical_bytes(value))

    def rebind_generic_attachment(self, child_kind: str, attachment_kind: str, raw: bytes) -> None:
        value = self.fixture.receipt_value()
        child = next(row for row in value["childReceipts"] if row["kind"] == child_kind)
        attachment = next(row for row in child["attachments"] if row["kind"] == attachment_kind)
        path = self.fixture.root / attachment["path"]
        path.write_bytes(raw)
        attachment["bytes"] = len(raw)
        attachment["sha256"] = CONTRACT.sha256(raw)
        child_receipt_path = self.fixture.root / child["path"]
        child_receipt = json.loads(child_receipt_path.read_bytes())
        child_attachment = next(
            row for row in child_receipt["attachments"] if row["kind"] == attachment_kind
        )
        child_attachment["bytes"] = len(raw)
        child_attachment["sha256"] = CONTRACT.sha256(raw)
        child_raw = CONTRACT.canonical_bytes(child_receipt)
        child_receipt_path.write_bytes(child_raw)
        child["bytes"] = len(child_raw)
        child["sha256"] = CONTRACT.sha256(child_raw)
        self.rewrite_receipt(value)

    def test_accepts_exact_complete_final_and_evidence_only_descendants(self) -> None:
        self.assertEqual(11, len(CONTRACT.CHILD_KINDS))
        self.assertEqual(26, len(CONTRACT.REQUIRED_SCENARIOS))
        self.assertEqual(8, len(self.fixture.bindings))
        tested, head, descendants = CONTRACT.validate_receipt(
            self.fixture.root, OUTPUT, self.fixture.tested
        )
        self.assertEqual(self.fixture.tested, tested)
        self.assertEqual(git(self.fixture.root, "rev-parse", "HEAD"), head)
        self.assertGreaterEqual(descendants, 3)

    def test_rejects_noncanonical_receipt(self) -> None:
        self.receipt_path().write_bytes(self.receipt_path().read_bytes() + b"\n")
        with self.assertRaisesRegex(CONTRACT.FinalError, "not exact closed-domain JCS"):
            CONTRACT.validate_receipt(self.fixture.root, OUTPUT)

    def test_rejects_wrong_or_borrowed_scenario(self) -> None:
        value = self.fixture.receipt_value()
        value["scenarios"][0] = "V2-POLICY-001"
        self.rewrite_receipt(value)
        with self.assertRaisesRegex(CONTRACT.FinalError, "exact evidence-owned allowlist"):
            CONTRACT.validate_receipt(self.fixture.root, OUTPUT)

    def test_rejects_child_source_mismatch(self) -> None:
        value = self.fixture.receipt_value()
        value["childReceipts"][0]["sourceTuple"]["nereusCommit"] = "f" * 40
        self.rewrite_receipt(value)
        with self.assertRaisesRegex(CONTRACT.FinalError, "differs from Final source"):
            CONTRACT.validate_receipt(self.fixture.root, OUTPUT)

    def test_rejects_zero_tests_or_nonzero_failure_error_skip(self) -> None:
        for field, bad in (("tests", 0), ("failures", 1), ("errors", 1), ("skipped", 1)):
            with self.subTest(field=field):
                value = self.fixture.receipt_value()
                value["childReceipts"][1][field] = bad
                self.rewrite_receipt(value)
                with self.assertRaises(CONTRACT.FinalError):
                    CONTRACT.validate_receipt(self.fixture.root, OUTPUT)

    def test_rejects_fake_provider_kms_or_promotable_c2(self) -> None:
        for field, bad in (("realProvider", False), ("realKms", False), ("c2PromotionEligible", True)):
            with self.subTest(field=field):
                value = self.fixture.receipt_value()
                value["providerEvidence"][field] = bad
                self.rewrite_receipt(value)
                with self.assertRaisesRegex(CONTRACT.FinalError, "real Provider/KMS"):
                    CONTRACT.validate_receipt(self.fixture.root, OUTPUT)

    def test_rejects_old_fake_text_rebound_as_real_provider_receipt(self) -> None:
        self.rebind_generic_attachment(
            "C1_REAL_PROVIDER_KMS", "PROVIDER_REAL_RECEIPT", b"trusted-real-provider=true"
        )
        with self.assertRaisesRegex(CONTRACT.FinalError, "cannot parse JSON"):
            CONTRACT.validate_receipt(self.fixture.root, OUTPUT)

    def test_rejects_exact_looking_unlocked_native_and_allocator_identities(self) -> None:
        for child_kind, attachment_kind in (
            ("U_KAFKA_OBJECT_WAL", "NATIVE_RESULT"),
            ("P_PULSAR_OBJECT_WAL", "NATIVE_RESULT"),
            ("ALLOCATOR_SELECTION", "ALLOCATOR_FAULT_SUMMARY"),
            ("ALLOCATOR_SELECTION", "ALLOCATOR_NATIVE_RELATIVE_SUMMARY"),
            ("ALLOCATOR_SELECTION", "ALLOCATOR_SCALE_10000_SUMMARY"),
            ("ALLOCATOR_SELECTION", "ALLOCATOR_SCALE_100000_SUMMARY"),
        ):
            with self.subTest(child=child_kind, attachment=attachment_kind):
                fixture = FixtureBuilder()
                try:
                    fixture.publish()
                    fixture.commit_final_and_sync_scenarios()
                    child = next(row for row in fixture.children if row["kind"] == child_kind)
                    attachment = next(
                        row for row in child["attachments"] if row["kind"] == attachment_kind
                    )
                    typed = json.loads((fixture.root / attachment["path"]).read_bytes())
                    if attachment_kind == "NATIVE_RESULT":
                        raw_result = json.loads(base64.b64decode(typed["rawEvidenceBase64"]))
                        raw_result["externalSources"][0]["commit"] = "b" * 40
                        profile = CHILD.NATIVE_PROFILES[child_kind]
                        counter_keys = tuple(profile["counters"])
                        raw_result["receiptSha256"] = CHILD.sha256(
                            CHILD._native_canonical_bytes(raw_result, counter_keys, "0" * 64)
                        )
                        raw = CHILD._native_canonical_bytes(raw_result, counter_keys)
                        typed["rawEvidenceBase64"] = base64.b64encode(raw).decode("ascii")
                        typed["rawEvidenceSha256"] = CHILD.sha256(raw)
                        typed["receiptSha256"] = "0" * 64
                        typed["receiptSha256"] = CHILD.sha256(CHILD.canonical_bytes(typed))
                        expected = "external locked source differs"
                    else:
                        typed["sourceIdentity"] = "sha256:" + "b" * 64
                        expected = "exact tested source locks"
                    self.fixture.cleanup()
                    self.fixture = fixture
                    self.rebind_generic_attachment(
                        child_kind, attachment_kind, CONTRACT.canonical_bytes(typed)
                    )
                    with self.assertRaisesRegex(CONTRACT.FinalError, expected):
                        CONTRACT.validate_receipt(fixture.root, OUTPUT)
                finally:
                    if fixture is not self.fixture:
                        fixture.cleanup()

    def test_rejects_incomplete_allocator_evidence(self) -> None:
        value = self.fixture.receipt_value()
        value["allocatorSelection"]["nativeRelativeEvidence"] = False
        self.rewrite_receipt(value)
        with self.assertRaisesRegex(CONTRACT.FinalError, "fault/native/10k/100k"):
            CONTRACT.validate_receipt(self.fixture.root, OUTPUT)

    def test_rejects_child_or_attachment_tamper(self) -> None:
        child = self.fixture.children[1]
        path = self.fixture.root / child["path"]
        path.write_bytes(path.read_bytes() + b"\n")
        with self.assertRaisesRegex(CONTRACT.FinalError, "child receipt bytes/SHA differ"):
            CONTRACT.validate_receipt(self.fixture.root, OUTPUT)

    def test_rejects_non_evidence_descendant(self) -> None:
        path = self.fixture.root / "production.txt"
        path.write_text("changed after tested production\n")
        git(self.fixture.root, "add", "production.txt")
        git(self.fixture.root, "commit", "-m", "invalid production descendant")
        with self.assertRaisesRegex(CONTRACT.FinalError, "non-evidence path changed"):
            CONTRACT.validate_receipt(self.fixture.root, OUTPUT)

    def test_rejects_non_evidence_descendant_even_when_later_reverted(self) -> None:
        path = self.fixture.root / "production.txt"
        original = path.read_text()
        path.write_text("temporary invalid production change\n")
        git(self.fixture.root, "add", "production.txt")
        git(self.fixture.root, "commit", "-m", "invalid production descendant")
        path.write_text(original)
        git(self.fixture.root, "add", "production.txt")
        git(self.fixture.root, "commit", "-m", "hide invalid production descendant")
        with self.assertRaisesRegex(CONTRACT.FinalError, "non-evidence path changed"):
            CONTRACT.validate_receipt(self.fixture.root, OUTPUT)

    def test_rejects_scenario_status_or_receipt_drift(self) -> None:
        path = self.fixture.root / CONTRACT.SCENARIO_PATH
        document = json.loads(path.read_text())
        document["scenarios"][0]["status"] = "PLANNED"
        path.write_text(json.dumps(document, indent=2) + "\n")
        with self.assertRaisesRegex(CONTRACT.FinalError, "not synchronized"):
            CONTRACT.validate_receipt(self.fixture.root, OUTPUT)

    def test_rejects_duplicate_scenario_id_even_when_the_last_copy_passes(self) -> None:
        path = self.fixture.root / CONTRACT.SCENARIO_PATH
        document = json.loads(path.read_text())
        duplicate = dict(document["scenarios"][0])
        document["scenarios"].insert(0, duplicate)
        path.write_text(json.dumps(document, indent=2) + "\n")
        with self.assertRaisesRegex(CONTRACT.FinalError, "duplicate scenario IDs"):
            CONTRACT.validate_receipt(self.fixture.root, OUTPUT)

    def test_rejects_final_receipt_borrowed_by_scenario_outside_allowlist(self) -> None:
        path = self.fixture.root / CONTRACT.SCENARIO_PATH
        document = json.loads(path.read_text())
        document["scenarios"].append(
            {
                "evidenceReceipt": str(OUTPUT),
                "id": "V2-POLICY-001",
                "milestone": "M3",
                "status": "PASSED_CURRENT_SOURCE",
            }
        )
        path.write_text(json.dumps(document, indent=2) + "\n")
        with self.assertRaisesRegex(CONTRACT.FinalError, "outside its exact allowlist"):
            CONTRACT.validate_receipt(self.fixture.root, OUTPUT)


if __name__ == "__main__":
    unittest.main(verbosity=2)
