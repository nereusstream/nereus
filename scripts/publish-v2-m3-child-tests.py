#!/usr/bin/env python3
"""Tests for exact-input M3 child receipt publication."""

from __future__ import annotations

import base64
import importlib.util
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tempfile
import unittest


PUBLISHER_PATH = Path(__file__).with_name("publish-v2-m3-child.py")
CHECK_TEST_SUPPORT_PATH = Path(__file__).with_name("check-v2-m3-child-tests.py")


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PUBLISHER = load_module("m3_child_publisher_test", PUBLISHER_PATH)
CONTRACT = PUBLISHER.CONTRACT
SUPPORT = load_module("m3_child_publisher_test_support", CHECK_TEST_SUPPORT_PATH)


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *args], text=True, stderr=subprocess.STDOUT
    ).strip()


class Fixture:
    def __init__(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="nereus-m3-child-publisher-")
        self.base = Path(self.temporary.name)
        self.root = self.base / "repo"
        self.inputs = self.base / "inputs"
        self.root.mkdir()
        self.inputs.mkdir()
        git(self.root, "init", "-b", "main")
        git(self.root, "config", "user.name", "M3 Child Publisher Test")
        git(self.root, "config", "user.email", "m3-child-publisher@example.invalid")
        locks = self.root / CONTRACT.FINAL.SOURCE_LOCKS_PATH
        locks.parent.mkdir(parents=True)
        source_locks, self.bindings = SUPPORT.source_lock_fixture()
        locks.write_text(json.dumps(source_locks, indent=2) + "\n")
        client = source_locks["dependencyEvidenceBindings"]["oxiaClientArtifacts"]["artifacts"]["clientJar"]
        client_path = self.root / client["relativePath"]
        client_path.parent.mkdir(parents=True, exist_ok=True)
        client_path.write_bytes(SUPPORT.ALLOCATOR_FIXTURE_CLIENT_BYTES)
        SUPPORT.write_native_source_fixture(self.root)
        SUPPORT.write_local_cap_source_fixture(self.root)
        SUPPORT.write_governed_junit_source_fixture(self.root)
        (self.root / "production.txt").write_text("production source\n")
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "tested source")
        self.tested = git(self.root, "rev-parse", "HEAD")
        (self.root / ".git/info/exclude").write_text("nereus-metadata-oxia/build/\n")

    def cleanup(self) -> None:
        self.temporary.cleanup()

    def write(self, name: str, value: dict | bytes) -> Path:
        path = self.inputs / name
        raw = value if isinstance(value, bytes) else CONTRACT.canonical_bytes(value)
        path.write_bytes(raw)
        return path

    def junit(self, child_kind: str, *, skipped: int = 0, tests: int = 3) -> Path:
        return self.write(
            "junit.json",
            SUPPORT.governed_junit_receipt_fixture(
                self.root, self.tested, child_kind, skipped=skipped, tests=tests
            ),
        )

    def typed(self, attachment_kind: str, child_kind: str, index: int) -> Path:
        _, _, subjects = CONTRACT._expected_typed_profile(
            attachment_kind, child_kind
        )
        binding = self.bindings[(child_kind, attachment_kind)]
        if attachment_kind == "PROVIDER_REAL_RECEIPT":
            return self.write(
                f"typed-{index}.json",
                SUPPORT.provider_receipt_fixture(self.tested),
            )
        if attachment_kind == "KMS_REAL_RECEIPT":
            return self.write(
                f"typed-{index}.json",
                SUPPORT.kms_receipt_fixture(self.tested),
            )
        if attachment_kind == "NATIVE_RESULT":
            return self.write(
                f"typed-{index}.json",
                SUPPORT.native_receipt_fixture(
                    self.root, self.tested, child_kind, binding
                ),
            )
        return self.write(
            f"typed-{index}.json",
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
                "sourceIdentity": binding["sourceIdentity"],
                "subjectCount": subjects,
                "tests": 1 if attachment_kind in CONTRACT.ALLOCATOR_DERIVED_KINDS else 2,
            },
        )

    def inputs_for(self, kind: str) -> list[tuple[str, Path]]:
        result: list[tuple[str, Path]] = []
        required_kinds = set(CONTRACT.REQUIRED_ATTACHMENTS[kind])
        if kind == "ALLOCATOR_SELECTION":
            required_kinds.update(CONTRACT.ALLOCATOR_V1_AUTHORITY_ATTACHMENTS)
        for index, attachment_kind in enumerate(sorted(required_kinds)):
            if (
                kind == "ALLOCATOR_SELECTION"
                and attachment_kind in CONTRACT.ALLOCATOR_DERIVED_KINDS
            ):
                continue
            if attachment_kind == "JUNIT_SUMMARY":
                path = self.junit(kind)
            elif attachment_kind == "LOCAL_CAP_RESULT":
                path = self.write(
                    "d1-local-cap-result.json",
                    SUPPORT.local_cap_result_fixture(self.root, self.tested),
                )
            elif attachment_kind == "ALLOCATOR_RAW_VERIFICATION":
                path = self.write(
                    "allocator-raw-verification.json",
                    SUPPORT.allocator_receipt_fixture(self.root, self.tested),
                )
            elif attachment_kind in CONTRACT.NORMALIZED_TYPED_KINDS:
                path = self.typed(attachment_kind, kind, index)
            else:
                path = self.write(f"artifact-{index}.bin", f"artifact:{attachment_kind}".encode())
            result.append((attachment_kind, path))
        return result


class M3ChildPublisherTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = Fixture()

    def tearDown(self) -> None:
        self.fixture.cleanup()

    def test_publishes_exact_c1_inputs_and_derives_counts(self) -> None:
        kind = "C1_REAL_PROVIDER_KMS"
        output = PurePosixPath("docs/v2/evidence/v2-m3/children/c1.json")
        raw = PUBLISHER.publish_generic(
            self.fixture.root,
            kind,
            self.fixture.tested,
            output,
            self.fixture.inputs_for(kind),
        )
        receipt = CONTRACT.load_canonical_json(raw, "published child")
        self.assertEqual({"errors": 0, "failures": 0, "skipped": 0, "tests": 5}, receipt["testSummary"])
        self.assertEqual(
            ["JUNIT_SUMMARY", "KMS_REAL_RECEIPT", "PROVIDER_REAL_RECEIPT"],
            [row["kind"] for row in receipt["attachments"]],
        )
        identity = CONTRACT.build_final_child_identity(
            self.fixture.root, output, kind, self.fixture.tested
        )
        self.assertEqual(5, identity["tests"])
        self.assertEqual(CONTRACT.CHILD_RESULTS[kind], identity["result"])
        # Exercise the exact private contract consumed by M3 Final, not merely
        # this tool's own identity builder.
        CONTRACT.FINAL._validate_child_shape(identity, kind, self.fixture.tested)
        CONTRACT.FINAL._validate_child_receipt_semantics(
            identity, receipt, self.fixture.tested
        )

    def test_publishes_d1_only_from_java_execution_payload_and_six_required_tests(self) -> None:
        kind = "D_LOCAL_CAP"
        inputs = self.fixture.inputs_for(kind)
        output = PurePosixPath("docs/v2/evidence/v2-m3/children/d1.json")
        raw = PUBLISHER.publish_generic(
            self.fixture.root, kind, self.fixture.tested, output, inputs
        )
        receipt = CONTRACT.load_canonical_json(raw, "published D1 child")
        self.assertEqual(
            ["JUNIT_SUMMARY", "LOCAL_CAP_RESULT"],
            [row["kind"] for row in receipt["attachments"]],
        )
        self.assertEqual(
            {"errors": 0, "failures": 0, "skipped": 0, "tests": 12},
            receipt["testSummary"],
        )
        CONTRACT.build_final_child_identity(
            self.fixture.root, output, kind, self.fixture.tested
        )

        missing_output = PurePosixPath("docs/v2/evidence/v2-m3/children/d1-missing.json")
        with self.assertRaisesRegex(CONTRACT.ChildError, "LOCAL_CAP_RESULT"):
            PUBLISHER.build_generic_receipt(
                self.fixture.root,
                kind,
                self.fixture.tested,
                missing_output,
                [item for item in inputs if item[0] != "LOCAL_CAP_RESULT"],
            )

    def test_normalizes_real_provider_and_kms_receipts_from_evidence_fields(self) -> None:
        kind = "C1_REAL_PROVIDER_KMS"
        inputs = self.fixture.inputs_for(kind)
        real_paths = {}
        for attachment_kind, value in (
            ("PROVIDER_REAL_RECEIPT", SUPPORT.provider_receipt_fixture(self.fixture.tested)),
            ("KMS_REAL_RECEIPT", SUPPORT.kms_receipt_fixture(self.fixture.tested)),
        ):
            path = self.fixture.inputs / f"pretty-{attachment_kind}.json"
            path.write_text(json.dumps(value, indent=2) + "\n")
            real_paths[attachment_kind] = path
        inputs = [(attachment_kind, real_paths.get(attachment_kind, path)) for attachment_kind, path in inputs]
        output = PurePosixPath("docs/v2/evidence/v2-m3/children/c1-real.json")
        raw = PUBLISHER.publish_generic(
            self.fixture.root, kind, self.fixture.tested, output, inputs
        )
        receipt = CONTRACT.load_canonical_json(raw, "published child")
        for attachment_kind, source_path in real_paths.items():
            row = next(item for item in receipt["attachments"] if item["kind"] == attachment_kind)
            copied = (self.fixture.root / row["path"]).read_bytes()
            self.assertEqual(
                CONTRACT.canonical_bytes(CONTRACT.load_external_json(source_path.read_bytes(), str(source_path))),
                copied,
            )
            self.assertNotEqual(source_path.read_bytes(), copied)

    def test_rejects_real_receipt_from_a_different_nereus_commit(self) -> None:
        kind = "C1_REAL_PROVIDER_KMS"
        inputs = self.fixture.inputs_for(kind)
        attachment_kind, source_path = next(
            item for item in inputs if item[0] == "PROVIDER_REAL_RECEIPT"
        )
        value = SUPPORT.provider_receipt_fixture("0" * 40)
        source_path.write_bytes(CONTRACT.canonical_bytes(value))
        output = PurePosixPath("docs/v2/evidence/v2-m3/children/c1-stale.json")
        with self.assertRaisesRegex(CONTRACT.ChildError, "receipt schema/source/test identity"):
            PUBLISHER.publish_generic(
                self.fixture.root,
                kind,
                self.fixture.tested,
                output,
                inputs,
            )
        self.assertEqual("PROVIDER_REAL_RECEIPT", attachment_kind)
        self.assertFalse(self.fixture.root.joinpath(*output.parts).exists())

    def test_post_test_sealer_binds_raw_provider_json_and_exact_junit_xml(self) -> None:
        raw_path = self.fixture.write(
            "provider-raw.json",
            SUPPORT.provider_raw_receipt_fixture(self.fixture.tested),
        )
        xml_path = self.fixture.inputs / "provider.xml"
        xml_path.write_bytes(SUPPORT.real_junit_xml_fixture("PROVIDER_REAL_RECEIPT"))
        sealed_path = self.fixture.inputs / "provider-sealed.json"
        raw = PUBLISHER.seal_real_execution(
            "PROVIDER_REAL_RECEIPT",
            self.fixture.tested,
            raw_path,
            xml_path,
            sealed_path,
        )
        self.assertEqual(raw, sealed_path.read_bytes())
        value = CONTRACT.load_canonical_json(raw, str(sealed_path))
        summary = CONTRACT.validate_typed_evidence(
            value,
            "PROVIDER_REAL_RECEIPT",
            "C1_REAL_PROVIDER_KMS",
            self.fixture.tested,
            self.fixture.bindings[("C1_REAL_PROVIDER_KMS", "PROVIDER_REAL_RECEIPT")],
        )
        self.assertEqual({"errors": 0, "failures": 0, "skipped": 0, "tests": 1}, summary)

        invalid_xml = xml_path.read_bytes().replace(
            b"provesC1AtTheAdmitted64MiBRootCap",
            b"handWrittenDifferentMethod",
        )
        xml_path.write_bytes(invalid_xml)
        with self.assertRaisesRegex(CONTRACT.ChildError, "testcase identity"):
            PUBLISHER.seal_real_execution(
                "PROVIDER_REAL_RECEIPT",
                self.fixture.tested,
                raw_path,
                xml_path,
                self.fixture.inputs / "invalid-sealed.json",
            )

        xml_path.write_bytes(SUPPORT.real_junit_xml_fixture("PROVIDER_REAL_RECEIPT"))
        with self.assertRaisesRegex(CONTRACT.ChildError, "refuses to overwrite"):
            PUBLISHER.seal_real_execution(
                "PROVIDER_REAL_RECEIPT",
                self.fixture.tested,
                raw_path,
                self.fixture.inputs / "provider.xml",
                sealed_path,
            )

    def test_governed_junit_sealer_binds_actual_xml_and_rejects_legacy_counters(self) -> None:
        kind = "D_LOCAL_CAP"
        fixture_wrapper = CONTRACT.load_canonical_json(
            SUPPORT.governed_junit_receipt_fixture(
                self.fixture.root, self.fixture.tested, kind
            ),
            "governed JUnit fixture",
            CONTRACT.SEALED_JUNIT_MAX_BYTES,
        )
        sealed_xml = fixture_wrapper["junitXml"][0]
        xml_path = self.fixture.write(
            "actual-child-junit.xml",
            base64.b64decode(sealed_xml["xmlBase64"]),
        )
        output = self.fixture.inputs / "governed-child-junit.json"
        raw = PUBLISHER.seal_junit_execution(
            self.fixture.root,
            kind,
            self.fixture.tested,
            [(sealed_xml["path"], xml_path)],
            output,
        )
        summary = CONTRACT.validate_junit(
            CONTRACT.load_canonical_json(raw, str(output), CONTRACT.SEALED_JUNIT_MAX_BYTES),
            self.fixture.root,
            self.fixture.tested,
            kind,
        )
        self.assertEqual(
            {"errors": 0, "failures": 0, "skipped": 0, "tests": 6}, summary
        )
        with self.assertRaisesRegex(CONTRACT.ChildError, "refuses to overwrite"):
            PUBLISHER.seal_junit_execution(
                self.fixture.root,
                kind,
                self.fixture.tested,
                [(sealed_xml["path"], xml_path)],
                output,
            )
        with self.assertRaisesRegex(CONTRACT.ChildError, "duplicated"):
            PUBLISHER.seal_junit_execution(
                self.fixture.root,
                kind,
                self.fixture.tested,
                [(sealed_xml["path"], xml_path), (sealed_xml["path"], xml_path)],
                self.fixture.inputs / "duplicate-junit.json",
            )
        symlink = self.fixture.inputs / "symlink-junit.xml"
        symlink.symlink_to(xml_path)
        with self.assertRaisesRegex(CONTRACT.ChildError, "non-symlink"):
            PUBLISHER.seal_junit_execution(
                self.fixture.root,
                kind,
                self.fixture.tested,
                [(sealed_xml["path"], symlink)],
                self.fixture.inputs / "symlink-junit.json",
            )
        with self.assertRaisesRegex(CONTRACT.ChildError, "safe relative path"):
            PUBLISHER.seal_junit_execution(
                self.fixture.root,
                kind,
                self.fixture.tested,
                [("../build/test-results/test/TEST-escape.xml", xml_path)],
                self.fixture.inputs / "escape-junit.json",
            )
        empty_xml = self.fixture.inputs / "empty.xml"
        empty_xml.write_bytes(b"")
        with self.assertRaisesRegex(CONTRACT.ChildError, "bytes outside cap"):
            PUBLISHER.seal_junit_execution(
                self.fixture.root,
                kind,
                self.fixture.tested,
                [(sealed_xml["path"], empty_xml)],
                self.fixture.inputs / "empty-junit.json",
            )

        legacy = self.fixture.write(
            "legacy-junit.json",
            {
                "errors": 0,
                "failures": 0,
                "nereusCommit": self.fixture.tested,
                "schema": "NEREUS_V2_M3_NORMALIZED_JUNIT_V1",
                "skipped": 0,
                "tests": 3,
            },
        )
        legacy_inputs = self.fixture.inputs_for(kind)
        legacy_inputs = [
            (attachment_kind, legacy if attachment_kind == "JUNIT_SUMMARY" else path)
            for attachment_kind, path in legacy_inputs
        ]
        with self.assertRaisesRegex(CONTRACT.ChildError, "member set differs"):
            PUBLISHER.build_generic_receipt(
                self.fixture.root,
                kind,
                self.fixture.tested,
                PurePosixPath("docs/v2/evidence/v2-m3/children/legacy-junit.json"),
                legacy_inputs,
            )

    def test_native_sealer_binds_raw_self_hash_and_exact_junit_xml_bytes(self) -> None:
        for child_kind in ("U_KAFKA_OBJECT_WAL", "P_PULSAR_OBJECT_WAL"):
            with self.subTest(child_kind=child_kind):
                raw_result, xml_inputs = SUPPORT.native_raw_result_fixture(
                    self.fixture.root,
                    self.fixture.tested,
                    child_kind,
                    self.fixture.bindings[(child_kind, "NATIVE_RESULT")],
                )
                raw_path = self.fixture.write(f"{child_kind}-raw.json", raw_result)
                path_inputs: list[tuple[str, Path]] = []
                for index, (relative, raw_xml) in enumerate(xml_inputs):
                    path_inputs.append(
                        (relative, self.fixture.write(f"{child_kind}-{index}.xml", raw_xml))
                    )
                sealed_path = self.fixture.inputs / f"{child_kind}-sealed.json"
                raw = PUBLISHER.seal_native_execution(
                    self.fixture.root,
                    child_kind,
                    self.fixture.tested,
                    raw_path,
                    path_inputs,
                    sealed_path,
                )
                self.assertEqual(raw, sealed_path.read_bytes())
                summary = CONTRACT.validate_native_result(
                    CONTRACT.load_canonical_json(
                        raw, str(sealed_path), CONTRACT.SEALED_NATIVE_MAX_BYTES
                    ),
                    self.fixture.root,
                    child_kind,
                    self.fixture.tested,
                    self.fixture.bindings[(child_kind, "NATIVE_RESULT")],
                )
                self.assertGreater(summary["tests"], 0)
                self.assertEqual(0, summary["skipped"])
                with self.assertRaisesRegex(CONTRACT.ChildError, "refuses to overwrite"):
                    PUBLISHER.seal_native_execution(
                        self.fixture.root,
                        child_kind,
                        self.fixture.tested,
                        raw_path,
                        path_inputs,
                        sealed_path,
                    )

    def test_allocator_sealer_and_publisher_derive_summaries_only_from_raw_authority(self) -> None:
        java_raw, junit_xml, external_paths = SUPPORT.java_allocator_verification_fixture(
            self.fixture.root, self.fixture.tested
        )
        external_inputs = [(name, Path(path)) for name, path in external_paths]
        tested_index = next(
            index
            for index, (name, _) in enumerate(external_inputs)
            if name == "testedEvidenceArtifact"
        )
        original_tested_artifact = self.fixture.root / external_inputs[tested_index][1]
        outside_tested_artifact = self.fixture.inputs / original_tested_artifact.name
        outside_tested_artifact.write_bytes(original_tested_artifact.read_bytes())
        external_inputs[tested_index] = ("testedEvidenceArtifact", outside_tested_artifact)
        java_path = (
            self.fixture.root
            / CONTRACT.ALLOCATOR_FORMAL_PREFIX
            / self.fixture.tested
            / "raw-verification.json"
        )
        java_path.write_bytes(java_raw)
        junit_path = self.fixture.root / CONTRACT.ALLOCATOR_VERIFIER_JUNIT_PATH
        junit_path.parent.mkdir(parents=True, exist_ok=True)
        junit_path.write_bytes(junit_xml)
        sealed_path = self.fixture.inputs / "allocator-governed.json"
        sealed = PUBLISHER.seal_allocator_verification(
            self.fixture.root,
            self.fixture.tested,
            java_path,
            junit_path,
            external_inputs,
            sealed_path,
        )
        authority = CONTRACT.validate_allocator_verification(
            CONTRACT.load_canonical_json(
                sealed,
                str(sealed_path),
                CONTRACT.ALLOCATOR_VERIFICATION_MAX_BYTES,
            ),
            self.fixture.root,
            self.fixture.tested,
        )
        self.assertEqual(1, authority["rawSummary"]["tests"])
        with self.assertRaisesRegex(CONTRACT.ChildError, "refuses to overwrite"):
            PUBLISHER.seal_allocator_verification(
                self.fixture.root,
                self.fixture.tested,
                java_path,
                junit_path,
                external_inputs,
                sealed_path,
            )

        inputs = self.fixture.inputs_for("ALLOCATOR_SELECTION")
        raw_index = next(
            index
            for index, (kind, _) in enumerate(inputs)
            if kind == "ALLOCATOR_RAW_VERIFICATION"
        )
        inputs[raw_index] = ("ALLOCATOR_RAW_VERIFICATION", sealed_path)
        inputs.sort(key=lambda item: item[0])
        output = PurePosixPath("docs/v2/evidence/v2-m3/children/allocator.json")
        receipt_raw = PUBLISHER.publish_generic(
            self.fixture.root,
            "ALLOCATOR_SELECTION",
            self.fixture.tested,
            output,
            inputs,
        )
        receipt = CONTRACT.load_canonical_json(receipt_raw, "published allocator child")
        self.assertEqual(
            [
                "ALLOCATOR_FAULT_SUMMARY",
                "ALLOCATOR_NATIVE_RELATIVE_SUMMARY",
                "ALLOCATOR_RAW_VERIFICATION",
                "ALLOCATOR_SCALE_100000_SUMMARY",
                "ALLOCATOR_SCALE_10000_SUMMARY",
                "JUNIT_SUMMARY",
            ],
            [row["kind"] for row in receipt["attachments"]],
        )
        self.assertEqual(
            {"errors": 0, "failures": 0, "skipped": 0, "tests": 5},
            receipt["testSummary"],
        )
        CONTRACT.build_final_child_identity(
            self.fixture.root, output, "ALLOCATOR_SELECTION", self.fixture.tested
        )

        caller_derived = self.fixture.typed(
            "ALLOCATOR_FAULT_SUMMARY", "ALLOCATOR_SELECTION", 99
        )
        forged_inputs = sorted(
            inputs + [("ALLOCATOR_FAULT_SUMMARY", caller_derived)],
            key=lambda item: item[0],
        )
        with self.assertRaisesRegex(CONTRACT.ChildError, "publisher-owned"):
            PUBLISHER.build_generic_receipt(
                self.fixture.root,
                "ALLOCATOR_SELECTION",
                self.fixture.tested,
                PurePosixPath("docs/v2/evidence/v2-m3/children/allocator-forged.json"),
                forged_inputs,
            )

    def test_allocator_v2_sealer_and_publisher_bind_campaign_authority(self) -> None:
        campaign = SUPPORT.allocator_v2_campaign_fixture(
            self.fixture.root, self.fixture.tested
        )
        paths = campaign["paths"]
        sealed_path = self.fixture.inputs / "allocator-v2-governed.json"
        sealed = PUBLISHER.seal_allocator_v2_verification(
            self.fixture.root,
            self.fixture.tested,
            paths["checkpoint"],
            paths["evaluation"],
            paths["diagnostic"],
            paths["diagnosticJunit"],
            paths["formalJunit"],
            paths["promotionDecision"],
            paths["executor"],
            paths["workload"],
            campaign["executionPaths"],
            sealed_path,
        )
        authority = CONTRACT.validate_allocator_v2_campaign_verification(
            CONTRACT.load_canonical_json(
                sealed,
                str(sealed_path),
                CONTRACT.ALLOCATOR_V2_VERIFICATION_MAX_BYTES,
            ),
            self.fixture.root,
            self.fixture.tested,
        )
        self.assertEqual("STRICT", authority["mode"])
        self.assertEqual(1, authority["selectedRangeSize"])
        self.assertEqual(
            {"errors": 0, "failures": 0, "skipped": 0, "tests": 5},
            authority["summary"],
        )
        with self.assertRaisesRegex(CONTRACT.ChildError, "refuses to overwrite"):
            PUBLISHER.seal_allocator_v2_verification(
                self.fixture.root,
                self.fixture.tested,
                paths["checkpoint"],
                paths["evaluation"],
                paths["diagnostic"],
                paths["diagnosticJunit"],
                paths["formalJunit"],
                paths["promotionDecision"],
                paths["executor"],
                paths["workload"],
                campaign["executionPaths"],
                sealed_path,
            )

        inputs = sorted(
            [
                ("ALLOCATOR_V2_CAMPAIGN_VERIFICATION", sealed_path),
                ("JUNIT_SUMMARY", self.fixture.junit("ALLOCATOR_SELECTION")),
            ],
            key=lambda item: item[0],
        )
        output = PurePosixPath("docs/v2/evidence/v2-m3/children/allocator-v2.json")
        receipt_raw = PUBLISHER.publish_generic(
            self.fixture.root,
            "ALLOCATOR_SELECTION",
            self.fixture.tested,
            output,
            inputs,
        )
        receipt = CONTRACT.load_canonical_json(receipt_raw, "published allocator V2 child")
        self.assertEqual(
            ["ALLOCATOR_V2_CAMPAIGN_VERIFICATION", "JUNIT_SUMMARY"],
            [row["kind"] for row in receipt["attachments"]],
        )
        self.assertEqual(
            {"errors": 0, "failures": 0, "skipped": 0, "tests": 8},
            receipt["testSummary"],
        )
        CONTRACT.build_final_child_identity(
            self.fixture.root, output, "ALLOCATOR_SELECTION", self.fixture.tested
        )

    def test_refuses_overwrite(self) -> None:
        output = PurePosixPath("docs/v2/evidence/v2-m3/children/existing.json")
        path = self.fixture.root.joinpath(*output.parts)
        path.parent.mkdir(parents=True)
        path.write_bytes(b"existing")
        git(self.fixture.root, "add", ".")
        git(self.fixture.root, "commit", "-m", "reserve output")
        self.fixture.tested = git(self.fixture.root, "rev-parse", "HEAD")
        with self.assertRaisesRegex(CONTRACT.ChildError, "refuses to overwrite"):
            PUBLISHER.publish_generic(
                self.fixture.root,
                "D_LOCAL_CAP",
                self.fixture.tested,
                output,
                self.fixture.inputs_for("D_LOCAL_CAP"),
            )

    def test_rejects_dirty_or_stale_source_before_writing(self) -> None:
        output = PurePosixPath("docs/v2/evidence/v2-m3/children/d.json")
        inputs = self.fixture.inputs_for("D_LOCAL_CAP")
        (self.fixture.root / "production.txt").write_text("dirty\n")
        with self.assertRaisesRegex(CONTRACT.ChildError, "clean exact HEAD"):
            PUBLISHER.publish_generic(
                self.fixture.root, "D_LOCAL_CAP", self.fixture.tested, output, inputs
            )
        git(self.fixture.root, "restore", "production.txt")
        old = self.fixture.tested
        (self.fixture.root / "production.txt").write_text("new source\n")
        git(self.fixture.root, "add", "production.txt")
        git(self.fixture.root, "commit", "-m", "new source")
        with self.assertRaisesRegex(CONTRACT.ChildError, "equal exact HEAD"):
            PUBLISHER.publish_generic(
                self.fixture.root, "D_LOCAL_CAP", old, output, inputs
            )

    def test_rejects_skipped_normalized_input_without_creating_output(self) -> None:
        output = PurePosixPath("docs/v2/evidence/v2-m3/children/d.json")
        inputs = self.fixture.inputs_for("D_LOCAL_CAP")
        skipped = self.fixture.junit("D_LOCAL_CAP", skipped=1)
        inputs = [
            (attachment_kind, skipped if attachment_kind == "JUNIT_SUMMARY" else path)
            for attachment_kind, path in inputs
        ]
        with self.assertRaisesRegex(CONTRACT.ChildError, "must be zero"):
            PUBLISHER.publish_generic(
                self.fixture.root,
                "D_LOCAL_CAP",
                self.fixture.tested,
                output,
                inputs,
            )
        self.assertFalse(self.fixture.root.joinpath(*output.parts).exists())

    def test_rejects_unsorted_or_missing_explicit_attachment_inventory(self) -> None:
        kind = "C1_REAL_PROVIDER_KMS"
        inputs = self.fixture.inputs_for(kind)
        output = PurePosixPath("docs/v2/evidence/v2-m3/children/c1.json")
        with self.assertRaisesRegex(CONTRACT.ChildError, "sorted and unique"):
            PUBLISHER.publish_generic(
                self.fixture.root, kind, self.fixture.tested, output, list(reversed(inputs))
            )
        with self.assertRaisesRegex(CONTRACT.ChildError, "mandatory typed attachments"):
            PUBLISHER.publish_generic(
                self.fixture.root, kind, self.fixture.tested, output, inputs[:-1]
            )

    def test_w1_requires_the_full_explicit_m2_child_inventory(self) -> None:
        output = PurePosixPath("docs/v2/evidence/v2-m3/w1/m2-regression/receipt.json")
        with self.assertRaisesRegex(CONTRACT.ChildError, "exactly 25 explicit trusted child results"):
            PUBLISHER.publish_w1(
                self.fixture.root, self.fixture.tested, output, []
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
