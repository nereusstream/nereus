#!/usr/bin/env python3
"""Contract tests for raw M5 JUnit attachment integrity, counts and testcase ownership."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET


SPEC = importlib.util.spec_from_file_location("m5_evidence_junit_contract", Path(__file__).with_name("m5-evidence-junit.py"))
CONTRACT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTRACT)
TASK = ":module:realTest"


def xml(status=None, name="case()", **attributes):
    root = ET.Element("testsuite", name="example.Suite", tests="1", failures="0", errors="0", skipped="0")
    case = ET.SubElement(root, "testcase", name=name, classname="example.Suite", time="0.123456")
    if status:
        ET.SubElement(case, status)
        root.set({"failure": "failures", "error": "errors", "skipped": "skipped"}[status], "1")
    root.attrib.update(attributes)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def row(raw, path="suite.xml", task=TASK):
    return {"path": path, "task": task, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


class JUnitEvidenceContractTest(unittest.TestCase):
    def test_exact_raw_cases_and_duration_never_claim_execution_or_promotion(self):
        raw = xml()
        value = CONTRACT.validate_suites([row(raw)], lambda _: raw)
        self.assertEqual({"tests": 1, "failures": 0, "errors": 0, "skipped": 0}, value["testSummary"])
        self.assertEqual(123456, value["durationMicros"])
        self.assertEqual("case()", value["cases"][0]["name"])
        self.assertFalse(value["promotionEligible"])
        self.assertFalse(value["executionEvidence"])
        self.assertFalse(value["sourceQualified"])

    def test_actual_failure_error_and_skip_reject_even_with_matching_counters(self):
        for status in ("failure", "error", "skipped"):
            with self.subTest(status=status):
                raw = xml(status)
                self.assertEqual(1, CONTRACT.parse_suite(raw, TASK)["summary"][{"failure": "failures", "error": "errors", "skipped": "skipped"}[status]])
                with self.assertRaisesRegex(CONTRACT.JUnitEvidenceError, "cannot qualify"):
                    CONTRACT.validate_suites([row(raw)], lambda _: raw)

    def test_declared_zero_cannot_hide_failure_error_or_skip(self):
        for status in ("failure", "error", "skipped"):
            field = {"failure": "failures", "error": "errors", "skipped": "skipped"}[status]
            with self.subTest(status=status), self.assertRaisesRegex(CONTRACT.JUnitEvidenceError, "counters differ"):
                CONTRACT.parse_suite(xml(status, **{field: "0"}), TASK)

    def test_empty_inventory_zero_suite_and_inflated_test_count_reject(self):
        with self.assertRaises(CONTRACT.JUnitEvidenceError):
            CONTRACT.validate_suites([], lambda _: b"")
        for raw in (xml(tests="2"), b'<testsuite name="empty" tests="0" failures="0" errors="0" skipped="0"/>'):
            with self.subTest(raw=raw), self.assertRaises(CONTRACT.JUnitEvidenceError):
                CONTRACT.parse_suite(raw, TASK)

    def test_attachment_byte_length_digest_and_unknown_fields_reject(self):
        raw = xml()
        for field, value in (("bytes", len(raw) + 1), ("bytes", True), ("sha256", "0" * 64), ("claimedPassed", True)):
            changed = row(raw)
            changed[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(CONTRACT.JUnitEvidenceError):
                CONTRACT.validate_suites([changed], lambda _: raw)

    def test_duplicate_attachment_and_repeated_test_identity_reject(self):
        raw = xml()
        for rows in ([row(raw), row(raw)], [row(raw), row(raw, "copy.xml")]):
            with self.subTest(rows=rows), self.assertRaisesRegex(CONTRACT.JUnitEvidenceError, "duplicate|more than once"):
                CONTRACT.validate_suites(rows, lambda _: raw)
        renamed = raw.replace(b'name="example.Suite"', b'name="renamed.Suite"', 1)
        with self.assertRaisesRegex(CONTRACT.JUnitEvidenceError, "more than once"):
            CONTRACT.validate_suites([row(raw), row(renamed, "renamed.xml")],
                                     lambda name: raw if name == "suite.xml" else renamed)
        root = ET.fromstring(raw)
        root.append(copy.deepcopy(root.find("testcase")))
        root.set("tests", "2")
        with self.assertRaisesRegex(CONTRACT.JUnitEvidenceError, "duplicate JUnit testcase"):
            CONTRACT.parse_suite(ET.tostring(root), TASK)

    def test_parameter_variants_and_same_test_in_independent_tasks_remain_distinct(self):
        first, second = xml(name="case[1]"), xml(name="case[2]")
        blobs = {"first.xml": first, "second.xml": second, "independent.xml": first}
        rows = [row(first, "first.xml"), row(second, "second.xml"), row(first, "independent.xml", ":module:otherTest")]
        self.assertEqual(3, CONTRACT.validate_suites(rows, blobs.__getitem__)["testSummary"]["tests"])

    def test_ambiguous_status_nested_suite_missing_identity_and_bad_task_reject(self):
        root = ET.fromstring(xml("failure"))
        ET.SubElement(root.find("testcase"), "skipped")
        malformed = [ET.tostring(root), xml().replace(b'classname="example.Suite"', b''),
                     b'<testsuite name="nested"><testsuite/></testsuite>']
        for raw in malformed:
            with self.subTest(raw=raw), self.assertRaises(CONTRACT.JUnitEvidenceError):
                CONTRACT.parse_suite(raw, TASK)
        for task in ("realTest", ":module", ":module:test --skip"):
            with self.subTest(task=task), self.assertRaises(CONTRACT.JUnitEvidenceError):
                CONTRACT.parse_suite(xml(), task)

    def test_dtd_entities_utf16_invalid_duration_and_noncanonical_counts_reject(self):
        malformed = [b'<!DOCTYPE testsuite [<!ENTITY x "expanded">]>' + xml(), xml().decode().encode("utf-16"),
                     xml(tests="01"), xml(tests="-1"), xml(time="NaN"), xml(time="1e999999"), xml(time="-1")]
        for raw in malformed:
            with self.subTest(raw=raw[:80]), self.assertRaises(CONTRACT.JUnitEvidenceError):
                CONTRACT.parse_suite(raw, TASK)
        for value in ("Infinity", "-0.1", "1e999999", "bad"):
            with self.subTest(value=value), self.assertRaises(CONTRACT.JUnitEvidenceError):
                CONTRACT.duration(value)

    def test_regular_reader_rejects_linked_file_and_linked_parent(self):
        with tempfile.TemporaryDirectory(prefix="m5-junit-attachments-") as temporary:
            root = Path(temporary).resolve()
            folder = root / "actual"
            folder.mkdir()
            path = folder / "suite.xml"
            path.write_bytes(xml())
            self.assertEqual(xml(), CONTRACT.regular(path))
            (root / "file-link").symlink_to(path)
            (root / "folder-link").symlink_to(folder, target_is_directory=True)
            for supplied in (root / "file-link", root / "folder-link/suite.xml", folder):
                with self.subTest(path=supplied), self.assertRaises(CONTRACT.JUnitEvidenceError):
                    CONTRACT.regular(supplied)


if __name__ == "__main__":
    unittest.main()
