#!/usr/bin/env python3
"""Positive and fail-closed tests for the M4 child/Final evidence contract."""

from __future__ import annotations

import base64
import copy
import importlib.util
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import xml.etree.ElementTree as ET


CHECKER_PATH = Path(__file__).with_name("check-v2-m4-evidence.py")
RUNNER_PATH = Path(__file__).with_name("run-v2-m4-evidence.py")
SOURCE_ROOT = Path(__file__).resolve().parent.parent


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


CONTRACT = load_module("m4_evidence_contract_test", CHECKER_PATH)
RUNNER = load_module("m4_evidence_runner_test", RUNNER_PATH)


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True, stderr=subprocess.STDOUT).strip()


class Fixture:
    def __init__(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="nereus-m4-evidence-")
        self.root = Path(self.temporary.name) / "repo"
        self.root.mkdir()
        git(self.root, "init", "-b", "main")
        git(self.root, "config", "user.name", "M4 Evidence Test")
        git(self.root, "config", "user.email", "m4-evidence@example.invalid")
        locks = json.loads((SOURCE_ROOT / CONTRACT.SOURCE_LOCKS_PATH).read_text())
        path = self.root / CONTRACT.SOURCE_LOCKS_PATH
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"m4EvidenceBindings": locks["m4EvidenceBindings"]}, indent=2) + "\n")
        m3 = self.root / CONTRACT.M3_FINAL_PATH
        m3.parent.mkdir(parents=True)
        m3.write_bytes((SOURCE_ROOT / CONTRACT.M3_FINAL_PATH).read_bytes())
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "tested M4 source")
        self.tested = git(self.root, "rev-parse", "HEAD")
        source_raw = CONTRACT.git_blob(self.root, self.tested, CONTRACT.SOURCE_LOCKS_PATH)
        self.source_sha = CONTRACT.sha256(source_raw)
        self.bindings = CONTRACT.source_bindings(self.root, self.tested, self.source_sha)
        self.child_paths = self._write_children()
        git(self.root, "add", str(CONTRACT.EVIDENCE_PREFIX))
        git(self.root, "commit", "-m", "publish M4 children")
        self.final_path = CONTRACT.FINAL_PREFIX / self.tested / "m4-final.json"
        self.final_value = self._final_value()
        destination = self.root / self.final_path
        destination.parent.mkdir(parents=True)
        destination.write_bytes(CONTRACT.canonical_bytes(self.final_value))
        self._write_scenarios()
        git(self.root, "add", str(CONTRACT.EVIDENCE_PREFIX), str(CONTRACT.SCENARIO_PATH))
        git(self.root, "commit", "-m", "publish M4 Final")

    def cleanup(self) -> None:
        self.temporary.cleanup()

    def _xml(self, class_name: str, names: set[str], task: str) -> bytes:
        root = ET.Element(
            "testsuite",
            name=class_name,
            tests=str(len(names)),
            failures="0",
            errors="0",
            skipped="0",
        )
        for name in sorted(names):
            ET.SubElement(root, "testcase", name=f"{name}()", classname=class_name, time="0.000001")
        metric_lines = self._metric_lines(task)
        if metric_lines:
            ET.SubElement(root, "system-out").text = "\n".join(metric_lines) + "\n"
        return ET.tostring(root, encoding="utf-8", xml_declaration=True)

    @staticmethod
    def _metric_lines(task: str) -> list[str]:
        if task == ":nereus-storage-object:v2M4ReadViewHazardEvidenceTest":
            return [
                "M4_METRIC HOT_PATH operations=100000 allocatedBytes=0 elapsedNanos=100000000 "
                "p50Nanos=100 p99Nanos=200 maxNanos=500 throughputOpsPerSecond=1000000 "
                "atomicCasPerOperation=2 fullFencesPerOperation=1 authorityAcquireLoadsPerOperation=2 "
                "perCallbackSlotCas=0 ordinaryReadRemoteMetadataOperations=0",
                "M4_METRIC CONCURRENT_HAZARD threads=4 operations=80000 elapsedNanos=80000000 "
                "p50Nanos=100 p99Nanos=300 maxNanos=600 throughputOpsPerSecond=1000000 scans=100 "
                "pinnedScans=50 cleanScans=40 inconclusiveScans=10 poolCapacity=16",
            ]
        if task == ":nereus-storage-object:v2M4QuiescenceProtectionReleaseEvidenceTest":
            return [
                "M4_METRIC CONTROL_CAPACITY attemptedProofs=2113 admittedProofs=2112 folds=64 "
                "windowEntries=64 stoppedEpoch=2113 pendingProofRows=1 elapsedNanos=2113000000 "
                "p99Nanos=1000 throughputOpsPerSecond=1000 metadataMutations=4226 proofIntervalEpochCap=4096",
                "M4_METRIC CONTROL_CLEANUP plannedRows=32 terminalRowsRetirable=32 proofRowsRetirable=32 "
                "removedFolds=1 successorWindowEntries=33 blockedReferenceKinds=6 "
                "referenceGenerationBound=1 publicationFenceShaBound=1 cleanupBatchCap=32",
            ]
        if task == ":nereus-kafka-bookkeeper:v2M4CurrentSourceKafkaTest":
            return [
                "M4_METRIC KAFKA_CURRENT_SOURCE operations=1000 elapsedNanos=100000000 p50Nanos=1000 "
                "p99Nanos=2000 maxNanos=3000 throughputOpsPerSecond=10000 callerAllocatedBytes=1000000 "
                "callerAllocatedBytesPerOperation=1000 ownerAllocatedBytes=2000000 "
                "ownerAllocatedBytesPerOperation=2000 measuredAllocatedBytes=3000000 "
                "measuredAllocatedBytesPerOperation=3000 hazardCapacity=8 peakInFlight=8 rejectedAtCapacity=1 "
                "outerHazardCasPerRead=2 perCallbackSlotCas=0 reusablePlanCapacity=256"
            ]
        if task == ":nereus-pulsar-offload:v2M4CurrentSourcePulsarTest":
            return [
                "M4_METRIC PULSAR_CURRENT_SOURCE operations=1000 elapsedNanos=125000000 p50Nanos=1000 "
                "p99Nanos=2000 maxNanos=3000 throughputOpsPerSecond=8000 callerAllocatedBytes=900000 "
                "callerAllocatedBytesPerOperation=900 ownerAllocatedBytes=1800000 "
                "ownerAllocatedBytesPerOperation=1800 measuredAllocatedBytes=2700000 "
                "measuredAllocatedBytesPerOperation=2700 hazardCapacity=8 outerHazardCasPerRead=2 "
                "perCallbackSlotCas=0 reusablePlanCapacity=1"
            ]
        return []

    def _write_children(self) -> list[PurePosixPath]:
        paths = []
        child_root = CONTRACT.CHILD_PREFIX / f"final-source-{self.tested}"
        for ordinal, kind in enumerate(CONTRACT.CHILD_KINDS, start=1):
            suites = []
            suite_items = list(CONTRACT.EXPECTED_SUITES[kind].items())
            for task, class_name in suite_items:
                names = CONTRACT.EXPECTED_SUITE_TESTS[kind][task]
                xml = self._xml(class_name, names, task)
                suites.append({
                    "bytes": len(xml),
                    "task": task,
                    "xmlBase64": base64.b64encode(xml).decode(),
                    "xmlSha256": CONTRACT.sha256(xml),
                })
            junit = {
                "childKind": kind,
                "schema": CONTRACT.JUNIT_SCHEMA,
                "suites": suites,
                "testedCommit": self.tested,
            }
            summary = CONTRACT.validate_junit(junit, kind, self.tested)
            metrics = CONTRACT.junit_metrics(junit, kind)
            attachments = []
            for index, attachment_kind in enumerate(CONTRACT.ATTACHMENTS[kind]):
                value = junit if attachment_kind == "JUNIT_SUMMARY" else RUNNER.fact_value(
                    kind, attachment_kind, self.tested, self.bindings, summary, metrics
                )
                raw = CONTRACT.canonical_bytes(value)
                relative = child_root / f"{ordinal:02d}-{kind}" / "attachments" / f"{index:02d}-{attachment_kind}.json"
                output = self.root / relative
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(raw)
                attachments.append({"bytes": len(raw), "kind": attachment_kind, "path": str(relative), "sha256": CONTRACT.sha256(raw)})
            receipt = {
                "attachments": attachments,
                "exclusions": CONTRACT.CHILD_EXCLUSIONS,
                "kind": kind,
                "promotionEligible": False,
                "result": CONTRACT.CHILD_RESULTS[kind],
                "schema": CONTRACT.CHILD_SCHEMA,
                "sourceTuple": {"nereusCommit": self.tested, "sourceLocksSha256": self.source_sha},
                "testSummary": summary,
            }
            path = child_root / f"{ordinal:02d}-{kind}" / "receipt.json"
            (self.root / path).write_bytes(CONTRACT.canonical_bytes(receipt))
            paths.append(path)
        return paths

    def _final_value(self) -> dict:
        children = [
            CONTRACT.child_identity(self.root, path, kind, self.tested)
            for path, kind in zip(self.child_paths, CONTRACT.CHILD_KINDS, strict=True)
        ]
        return {
            "backendAdmissions": self.bindings["backendAdmissions"],
            "childReceipts": children,
            "exclusions": CONTRACT.FINAL_EXCLUSIONS,
            "frozenM3": {
                "closureCommit": CONTRACT.M3_CLOSURE_COMMIT,
                "finalPath": str(CONTRACT.M3_FINAL_PATH),
                "finalSha256": CONTRACT.M3_FINAL_SHA256,
                "sourceLocksSha256": CONTRACT.M3_SOURCE_LOCKS_SHA256,
                "testedCommit": CONTRACT.M3_TESTED_COMMIT,
            },
            "kind": CONTRACT.FINAL_KIND,
            "physicalSelection": self.bindings["physicalSelection"],
            "promotionEligible": True,
            "result": CONTRACT.FINAL_RESULT,
            "scenarios": list(CONTRACT.PROMOTED_SCENARIOS),
            "schema": CONTRACT.FINAL_SCHEMA,
            "sharedPredicates": list(CONTRACT.SHARED_PREDICATES),
            "sourceTuple": {"nereusCommit": self.tested, "sourceLocksSha256": self.source_sha},
        }

    def _write_scenarios(self) -> None:
        rows = []
        for scenario in CONTRACT.PROMOTED_SCENARIOS:
            rows.append({"evidenceReceipt": str(self.final_path), "id": scenario, "status": "PASSED_CURRENT_SOURCE"})
        for scenario in ("V2-READ-002", *CONTRACT.SHARED_PREDICATES):
            rows.append({"evidenceReceipt": None, "id": scenario, "status": "PLANNED"})
        path = self.root / CONTRACT.SCENARIO_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"scenarios": rows}) + "\n")


class M4EvidenceContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ancestry = mock.patch.object(CONTRACT, "validate_frozen_m3_ancestry")
        self.ancestry.start()
        self.fixture = Fixture()

    def tearDown(self) -> None:
        self.fixture.cleanup()
        self.ancestry.stop()

    def test_accepts_exact_four_child_final_and_scenario_allowlist(self) -> None:
        tested, _, descendants = CONTRACT.validate_final(
            self.fixture.root, self.fixture.final_path, self.fixture.tested
        )
        self.assertEqual(self.fixture.tested, tested)
        self.assertEqual(2, descendants)

    def test_rejects_missing_child(self) -> None:
        value = copy.deepcopy(self.fixture.final_value)
        value["childReceipts"].pop()
        with self.assertRaisesRegex(CONTRACT.EvidenceError, "child count"):
            CONTRACT.validate_final_value(self.fixture.root, value, self.fixture.tested)

    def test_rejects_extra_child(self) -> None:
        value = copy.deepcopy(self.fixture.final_value)
        value["childReceipts"].append(copy.deepcopy(value["childReceipts"][0]))
        with self.assertRaisesRegex(CONTRACT.EvidenceError, "child count"):
            CONTRACT.validate_final_value(self.fixture.root, value, self.fixture.tested)

    def test_rejects_wrong_tested_source(self) -> None:
        value = copy.deepcopy(self.fixture.final_value)
        value["sourceTuple"]["nereusCommit"] = "0" * 40
        with self.assertRaisesRegex(CONTRACT.EvidenceError, "tested commit differs"):
            CONTRACT.validate_final_value(self.fixture.root, value, self.fixture.tested)

    def test_rejects_stale_attachment_digest(self) -> None:
        receipt = json.loads((self.fixture.root / self.fixture.child_paths[0]).read_text())
        receipt["attachments"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(CONTRACT.EvidenceError, "bytes/SHA differ"):
            CONTRACT.validate_child_value(self.fixture.root, receipt, CONTRACT.CHILD_KINDS[0], self.fixture.tested)

    def test_rejects_duplicate_json_member(self) -> None:
        with self.assertRaisesRegex(CONTRACT.EvidenceError, "duplicate JSON member"):
            CONTRACT.load_canonical(b'{"kind":"A","kind":"B"}', "duplicate")

    def test_rejects_borrowed_scenario(self) -> None:
        path = self.fixture.root / CONTRACT.SCENARIO_PATH
        doc = json.loads(path.read_text())
        doc["scenarios"].append({"evidenceReceipt": str(self.fixture.final_path), "id": "V2-READ-999", "status": "PASSED_CURRENT_SOURCE"})
        path.write_text(json.dumps(doc))
        with self.assertRaisesRegex(CONTRACT.EvidenceError, "borrowed outside allowlist"):
            CONTRACT.validate_scenarios(self.fixture.root, self.fixture.final_path)

    def test_rejects_partial_shared_predicate_promotion(self) -> None:
        path = self.fixture.root / CONTRACT.SCENARIO_PATH
        doc = json.loads(path.read_text())
        row = next(row for row in doc["scenarios"] if row["id"] == "V2-READ-006")
        row["status"] = "PASSED_CURRENT_SOURCE"
        row["evidenceReceipt"] = str(self.fixture.final_path)
        path.write_text(json.dumps(doc))
        with self.assertRaisesRegex(CONTRACT.EvidenceError, "borrowed outside allowlist|non-promotable"):
            CONTRACT.validate_scenarios(self.fixture.root, self.fixture.final_path)

    def test_rejects_behavior_change_after_tested_source(self) -> None:
        path = self.fixture.root / "production.txt"
        path.write_text("behavior change\n")
        git(self.fixture.root, "add", "production.txt")
        git(self.fixture.root, "commit", "-m", "invalid behavior descendant")
        with self.assertRaisesRegex(CONTRACT.EvidenceError, "non-evidence change"):
            CONTRACT.validate_descendants(self.fixture.root, self.fixture.tested)

    def test_rejects_tampered_governed_performance_metric(self) -> None:
        receipt = json.loads((self.fixture.root / self.fixture.child_paths[0]).read_text())
        junit_path = self.fixture.root / receipt["attachments"][0]["path"]
        junit = json.loads(junit_path.read_text())
        suite = junit["suites"][0]
        xml = base64.b64decode(suite["xmlBase64"])
        xml = xml.replace(b"p99Nanos=200", b"p99Nanos=5000001", 1)
        suite["bytes"] = len(xml)
        suite["xmlBase64"] = base64.b64encode(xml).decode()
        suite["xmlSha256"] = CONTRACT.sha256(xml)
        with self.assertRaisesRegex(CONTRACT.EvidenceError, "latency percentiles|hot-path metric"):
            CONTRACT.junit_metrics(junit, "READ_VIEW_HAZARD")


if __name__ == "__main__":
    unittest.main(verbosity=2)
