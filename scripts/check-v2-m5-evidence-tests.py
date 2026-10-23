#!/usr/bin/env python3
"""Synthetic validator contracts only; these tests do not produce M5 execution evidence."""

from __future__ import annotations

import ast
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import tempfile
import textwrap
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET
from zipfile import ZipFile


SPEC = importlib.util.spec_from_file_location("m5_execution_contract", Path(__file__).with_name("check-v2-m5-evidence.py"))
C = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(C)
INPUT_TESTS = C.module("m5_commit_fixture", "check-v2-m5-candidate-inputs-tests.py")
RUNNER = C.module("m5_runner_fixture", "run-v2-m5-evidence.py")
PREPARATION = C.module("m5_fresh_preparation_fixture", "m5-native-build-preparation.py")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def suite(cases):
    root = ET.Element("testsuite", name="CONTRACT_FIXTURE_ONLY", tests=str(len(cases)),
                      failures="0", errors="0", skipped="0", time="0.001")
    for cls, method in cases:
        ET.SubElement(root, "testcase", classname=cls, name=method if "(" in method else method + "()", time="0.001")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def pulsar_quota_fixture():
    """Synthetic stored-wire observations for reader contracts; never backend evidence."""
    def signed(raw):
        return raw + hashlib.sha256(raw).digest()
    def text(raw):
        return struct.pack('>i', len(raw)) + raw
    def layout(ordinal):
        service, container = b'CONTRACT_SERVICE', b'CONTRACT_CONTAINER'
        root = '/nereus/m5-native-authorities-v2/' + sha(
            struct.pack('>i', ordinal + 1) + text(service) + text(container))
        member = b'v2/physical-members-m5-v2/' + b'0' * 64 + b'/inventory-v2'
        scope = hashlib.sha256(struct.pack('>ii', 0x4d354d51, 8554) + text(member)
            + struct.pack('>i', ordinal) + text(root.encode()) + text(service) + text(container)).digest()
        return root, scope, service, container
    def head(ordinal, revision, reserved, settled=0, done_bytes=0, births=()):
        root, scope, _, _ = layout(ordinal)
        capacity = 500_000_000 if ordinal else 64 * 1024 * 1024
        raw = b'M5GH' + struct.pack('>i', 3 if births else 2) + scope \
            + struct.pack('>5q', revision, capacity, reserved, settled, done_bytes) + bytes(160)
        if births:
            raw += struct.pack('>3q', *births) + bytes(8196)
        reservation = len(root + '/v2/physical-delete-quota-v2/resources/' + '0' * 64) + 156 \
            + len(root + '/v2/physical-delete-m5-v2/' + '0' * 64 + '/authority-v2') + 1048576 \
            + len(root + '/v2/physical-members-m5-v2/' + '0' * 64 + '/inventory-v2') + 8554
        charge = len(root + '/v2/physical-delete-quota-v2/head') + 272 \
            + reserved * reservation + settled * (reservation - 1048576) + done_bytes
        if births:
            charge += 8220 + births[0] * (len(root + '/v3/object-birth-credits/' + '0' * 64) + 8192) \
                + (births[0] - births[1] - births[2]) * reservation
        return signed(raw), charge, capacity
    resource = b'CONTRACT_RESOURCE_CANONICAL_BYTES'
    def identity(value):
        digest = hashlib.sha256(text(b'NEREUS_V2_M5_PHYSICAL_RESOURCE_V2') + text(value)).digest()
        done = signed(b'M5DC' + struct.pack('>ii', 2, len(value)) + value + bytes(356))
        return digest, done
    _, done = identity(resource)
    def entry(settled, ordinal, value):
        digest, done = identity(value)
        return signed(b'M5GQ' + struct.pack('>i', 2) + layout(ordinal)[1] + digest + struct.pack('>qi', 2, int(settled))
                      + (hashlib.sha256(done).digest() if settled else bytes(32))
                      + struct.pack('>q', len(done) if settled else 0))
    def sample(phase, ordinal, values, settled=None, value=resource):
        raw, charged, capacity = values
        root, _, service, container = layout(ordinal)
        digest, done = identity(value)
        b64 = lambda value: C.base64.b64encode(value).decode()
        return 'NATIVE_PULSAR_QUOTA ' + ' '.join(f'{key}={value}' for key, value in {
            'phase': phase, 'nativeKey': root + '/v2/physical-delete-quota-v2/head',
            'version': struct.unpack_from('>q', raw, 40)[0],
            'provider': 'BOOKKEEPER' if ordinal else 'OBJECT_PROVIDER',
            'service': b64(service), 'container': b64(container), 'head': b64(raw),
            'charged': charged, 'available': capacity - charged,
            'resource': 'none' if settled is None else digest.hex(),
            'entry': 'none' if settled is None else b64(entry(settled, ordinal, value)),
            'done': b64(done) if settled else 'none'}.items())
    gc_after = head(0, 4, 1, 1, len(done), (2, 2, 0))
    _, _, service, container = layout(1)
    bk1 = b'M5RI\x00\x02\x02\x02' + text(service) + text(container) + struct.pack('>q', 101)
    bk2 = b'M5RI\x00\x02\x02\x02' + text(service) + text(container) + struct.pack('>q', 102)
    bk1_bytes, bk2_bytes = len(identity(bk1)[1]), len(identity(bk2)[1])
    final = head(1, 20, 9, 2, bk1_bytes + bk2_bytes)
    owner = [sample('bk-initial', 1, head(1, 1, 0)),
             sample('gc-before', 0, head(0, 3, 2, births=(2, 2, 0)), False),
             sample('gc-after', 0, gc_after, True), sample('readmission-before', 0, gc_after),
             sample('readmission-after', 0, head(0, 5, 2, 1, len(done), (3, 3, 0))),
             sample('bk-gc-before', 1, head(1, 2, 4), False, bk1),
             sample('bk-gc-after', 1, head(1, 3, 3, 1, bk1_bytes), True, bk1),
             sample('bk-gc-before', 1, head(1, 10, 10, 1, bk1_bytes), False, bk2),
             sample('bk-gc-after', 1, head(1, 11, 9, 2, bk1_bytes + bk2_bytes), True, bk2),
             sample('bk-final', 1, final)]
    return '\n'.join(owner) + '\n', sample('bk-sdk-completion', 1, final) + '\n'


class Fixture:
    def __init__(self, stage="COMPONENTS"):
        self.temp = tempfile.TemporaryDirectory(prefix="m5-validator-contract-")
        self.root = Path(self.temp.name)
        self.stage = stage
        self.inputs = {"inputRootSha256": sha(b"CONTRACT_FIXTURE_ONLY"), "extraInputs": {}, "repositories": {
            name: {"baseCommit": "1" * 40, "sourceRoot": "/contract/" + name, "files": {}}
            for name in C.INPUTS.REPOSITORIES}}
        for path, raw in [(C.STAGES[stage][0], b"contract driver"), ("docs/v2/source-locks.json", b"contract locks")]:
            self.inputs["repositories"]["nereus"]["files"][path] = {"mode": "100644", "bytes": len(raw), "sha256": sha(raw)}
        self.record = {
            "schema": "NEREUS_V2_M5_EXECUTION_V1", "stage": stage,
            "inputRootSha256": self.inputs["inputRootSha256"],
            "sourceLocksSha256": sha(b"contract locks"), "startedAtMicros": 1000, "finishedAtMicros": 2000,
            "exitCode": 0, "command": ["python3", C.STAGES[stage][0]],
            "driver": {"repository": "nereus", "path": C.STAGES[stage][0], "bytes": len(b"contract driver"),
                       "sha256": sha(b"contract driver")}, "workingDirectory": "/fixture/workspace/GITHUB/nereus",
            "inputBeforeSha256": self.inputs["inputRootSha256"], "inputAfterSha256": self.inputs["inputRootSha256"],
            "log": self.write(f"executions/{stage}/run.log", f"M5_STAGE_EXIT stage={stage} code=0\n".encode()),
            "junit": [], "runtimeInputs": [], "boundaries": list(C.STAGES[stage][1]), "attachments": [],
            "backendSelections": [], "cleanup": {"ownedAbsent": True, "originalUnchanged": True},
        }
        self.record["attachments"].append(self.record["log"])
        xml = self.write(f"executions/{stage}/raw.xml", suite([("ContractOnly", "passes")]))
        self.record["attachments"].append(xml)
        self.record["junit"].append(xml | {"task": ":contract:fixture", "producedAtMicros": 1500})
        if self.record["boundaries"]:
            self.inputs["extraInputs"]["/contract/dependency.jar"] = {"bytes": 3, "sha256": sha(b"jar")}
            self.record["runtimeInputs"] = [{"sourcePath": "/contract/dependency.jar", "bytes": 3, "sha256": sha(b"jar")}]
            for index, boundary in enumerate(self.record["boundaries"]):
                if boundary not in C.IMAGES:
                    name = "kafka" if boundary == "NATIVE_KAFKA" else "pulsar"
                    source = "/contract/" + name + "/build/native.class"
                    self.inputs["extraInputs"][source] = {"bytes": 6, "sha256": sha(b"native")}
                    self.record["runtimeInputs"].append({"sourcePath": source, "bytes": 6, "sha256": sha(b"native")})
                    loaded = {source: sha(b"native")}
                    observed = self.write(f"executions/{stage}/{boundary}-loaded.json", C.canonical(loaded))
                    facts = {"schema": "NEREUS_V2_M5_BACKEND_SELECTION_V1", "boundary": boundary,
                             "implementation": "ACTUAL_NATIVE_SDK", "identity": {"repository": name,
                             "baseCommit": "1" * 40, "loadedInputs": loaded}, "rawEvidence": observed}
                    self.record["attachments"].append(observed)
                    fact = self.write(f"executions/{stage}/{boundary}.json", C.canonical(facts))
                    self.record["attachments"].append(fact)
                    self.record["backendSelections"].append({"boundary": boundary, "facts": fact})
                    continue
                image, image_id = C.IMAGES[boundary]
                container = str(index + 1) * 64
                observed = self.write(f"executions/{stage}/{boundary}-inspect.json", json.dumps([
                    {"Id": container, "Image": image_id, "Config": {"Image": image}}]).encode())
                self.record["attachments"].append(observed)
                facts = {"schema": "NEREUS_V2_M5_BACKEND_SELECTION_V1", "boundary": boundary,
                         "implementation": "PINNED_CONTAINER", "identity": {
                             "image": image, "imageId": image_id, "containerId": container}, "rawEvidence": observed}
                fact = self.write(f"executions/{stage}/{boundary}.json", C.canonical(facts))
                self.record["attachments"].append(fact)
                self.record["backendSelections"].append({"boundary": boundary, "facts": fact})

    def write(self, path, raw):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        return {"path": path, "bytes": len(raw), "sha256": sha(raw)}

    def validate(self):
        return C.validate_execution(self.root, self.record, self.inputs)


class ExecutionContracts(unittest.TestCase):
    def fixture(self, stage="COMPONENTS"):
        f = Fixture(stage)
        self.addCleanup(f.temp.cleanup)
        return f

    def test_component_and_actual_boundary_record_shapes_parse_without_claiming_live_services(self):
        for stage in ("COMPONENTS", "OBJECT"):
            f = self.fixture(stage)
            self.assertEqual(1, f.validate()["testSummary"]["tests"])
            self.assertFalse(f.validate()["executionEvidence"])

    def test_source_drift_stale_xml_and_failed_execution_reject(self):
        mutations = [("inputAfterSha256", "0" * 64), ("exitCode", 1), ("exitCode", False),
                     ("finishedAtMicros", 999), ("workingDirectory", "/shared/GITHUB/nereus")]
        for key, value in mutations:
            f = self.fixture()
            f.record[key] = value
            with self.assertRaises(C.EvidenceError):
                f.validate()
        f = self.fixture()
        f.record["junit"][0]["producedAtMicros"] = 999
        with self.assertRaisesRegex(C.EvidenceError, "freshly produced"):
            f.validate()

    def test_unregistered_changed_runtime_and_foreign_driver_reject(self):
        for key in ("sha256", "bytes"):
            f = self.fixture("OBJECT")
            f.record["runtimeInputs"][0][key] = "0" * 64 if key == "sha256" else 4
            with self.assertRaisesRegex(C.EvidenceError, "runtime input differs"):
                f.validate()
        f = self.fixture()
        f.record["driver"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(C.EvidenceError, "frozen executed source"):
            f.validate()

    def test_missing_raw_bytes_duplicate_or_unreferenced_attachments_reject(self):
        f = self.fixture()
        (f.root / f.record["junit"][0]["path"]).write_bytes(b"different")
        with self.assertRaisesRegex(C.EvidenceError, "bytes or digest differ"):
            f.validate()
        f = self.fixture()
        f.record["attachments"].append(f.record["attachments"][0])
        with self.assertRaisesRegex(C.EvidenceError, "duplicate execution attachment"):
            f.validate()
        f = self.fixture()
        f.write("executions/COMPONENTS/unreferenced.txt", b"unregistered")
        with self.assertRaisesRegex(C.EvidenceError, "unreferenced files"):
            f.validate()

    def test_aliases_and_symlinks_cannot_replace_raw_attachments(self):
        f = self.fixture()
        original = f.root / f.record["log"]["path"]
        raw = original.read_bytes()
        original.unlink()
        outside = f.root / "outside.log"
        outside.write_bytes(raw)
        original.symlink_to(outside)
        with self.assertRaisesRegex(C.EvidenceError, "symlink"):
            f.validate()
        with self.assertRaises((C.EvidenceError, C.INPUTS.ClosureError)):
            C.read(f.root, "executions/COMPONENTS/../../outside.log")

    def test_wrong_image_and_failed_cleanup_cannot_qualify_a_real_boundary(self):
        f = self.fixture("OBJECT")
        f.record["cleanup"]["ownedAbsent"] = False
        with self.assertRaisesRegex(C.EvidenceError, "cleanup differs"):
            f.validate()
        f = self.fixture("OBJECT")
        selection = f.record["backendSelections"][0]
        facts = C.load((f.root / selection["facts"]["path"]).read_bytes())
        facts["identity"]["imageId"] = "sha256:" + "0" * 64
        old = selection["facts"]
        replacement = f.write(old["path"], C.canonical(facts))
        selection["facts"] = replacement
        f.record["attachments"] = [replacement if row == old else row for row in f.record["attachments"]]
        with self.assertRaisesRegex(C.EvidenceError, "image identity differs"):
            f.validate()

    def test_skipped_or_failed_xml_never_becomes_success_by_changing_declared_counts(self):
        f = self.fixture()
        row = f.record["junit"][0]
        xml = ET.fromstring((f.root / row["path"]).read_bytes())
        ET.SubElement(xml.find("testcase"), "skipped")
        xml.set("skipped", "1")
        old = {key: row[key] for key in ("path", "bytes", "sha256")}
        new = f.write(row["path"], ET.tostring(xml))
        f.record["attachments"] = [new if item == old else item for item in f.record["attachments"]]
        f.record["junit"][0] = new | {"task": row["task"], "producedAtMicros": 1500}
        with self.assertRaisesRegex(C.JUNIT.JUnitEvidenceError, "skipped JUnit"):
            f.validate()


class ChildContracts(unittest.TestCase):
    def fixture(self):
        f = Fixture()
        self.addCleanup(f.temp.cleanup)
        kind = C.KINDS[4]
        methods = set(C.CORE_ANCHORS[kind])
        for identity, owner in C.OWNERS.items():
            if owner == kind:
                methods.update(C.ANCHORS[identity])
        by_stage = {}
        for cls, method in sorted(methods):
            by_stage.setdefault(C.CLASS_STAGES[cls], []).append((cls, method))
        executions, attachments, identities = {}, [], {}
        for stage, cases in by_stage.items():
            raw = suite(cases)
            parsed = C.JUNIT.parse_suite(raw, ":contract:raw")
            for case in parsed["cases"]:
                case["executionStage"] = stage
                case["xmlSha256"] = sha(raw)
                identities[(case["class"], case["name"].removesuffix("()"))] = C.case_key(stage, case)
            executions[stage] = parsed | {"inputRootSha256": f.inputs['inputRootSha256'],
                "suites": [{"task": ":contract:raw", "sha256": sha(raw)}]}
            attachments.append(f.write(f"children/{kind}/{stage}.xml", raw))
        obligations = {identity: [identities[anchor] for anchor in C.ANCHORS[identity]]
                       for identity, owner in C.OWNERS.items() if owner == kind}
        value = {"schema": "NEREUS_V2_M5_CHILD_EVIDENCE_V1", "kind": kind,
                 "result": "PASS_V2_M5_" + kind, "promotionEligible": False, "sourceTuple": None,
                 "inputRootSha256": f.inputs["inputRootSha256"], "attachments": attachments,
                 "executionInputRoots": {stage: f.inputs['inputRootSha256'] for stage in executions},
                 "obligations": obligations, "coreCases": [identities[anchor] for anchor in C.CORE_ANCHORS[kind]],
                 "summary": {"tests": len(methods), "failures": 0, "errors": 0, "skipped": 0},
                 "exclusions": list(C.EXCLUSIONS)}
        f.write(f"children/{kind}/receipt.json", C.canonical(value))
        return f, value, executions

    def test_one_exclusive_child_binds_exact_executed_cases_and_raw_corpus(self):
        f, value, executions = self.fixture()
        checked = C.validate_child(f.root, value, executions, f.inputs)
        self.assertGreater(len(checked["promotionCases"]), 0)
        self.assertEqual(value["summary"], checked["summary"])

    def test_wrong_owner_missing_execution_or_unbound_source_cannot_promote(self):
        f, value, executions = self.fixture()
        value["obligations"]["BK_ONLY_COMPACTION"] = value["coreCases"]
        with self.assertRaisesRegex(C.EvidenceError, "semantic ownership differs"):
            C.validate_child(f.root, value, executions, f.inputs)
        f, value, executions = self.fixture()
        value["obligations"]["QUARANTINE_EXIT"] = ["unexecuted assertion"]
        with self.assertRaisesRegex(C.EvidenceError, "unexecuted assertion"):
            C.validate_child(f.root, value, executions, f.inputs)
        f, value, executions = self.fixture()
        value["sourceTuple"] = {"commits": {"nereus": "0" * 40}}
        with self.assertRaisesRegex(C.EvidenceError, "source binding differs"):
            C.validate_child(f.root, value, executions, f.inputs)

    def test_same_task_different_xml_does_not_prove_the_claimed_assertion(self):
        f, value, executions = self.fixture()
        original = value["attachments"][0]
        replacement = f.write(original["path"], suite([("Unrelated", "passes")]))
        value["attachments"][0] = replacement
        with self.assertRaisesRegex(C.EvidenceError, "exact raw attachment"):
            C.validate_child(f.root, value, executions, f.inputs)

    def test_alias_archive_and_false_child_counts_reject(self):
        f, value, executions = self.fixture()
        value["summary"]["tests"] += 1
        with self.assertRaisesRegex(C.EvidenceError, "summary differs"):
            C.validate_child(f.root, value, executions, f.inputs)
        f, value, executions = self.fixture()
        row = value["attachments"][0]
        other = f.root / "children/foreign/alias.xml"
        other.parent.mkdir(parents=True)
        other.write_bytes((f.root / row["path"]).read_bytes())
        row["path"] = "children/foreign/alias.xml"
        with self.assertRaisesRegex(C.EvidenceError, "exclusive owner"):
            C.validate_child(f.root, value, executions, f.inputs)


class BindingAndScenarioContracts(unittest.TestCase):
    def test_final_source_tree_and_dependency_bytes_are_checked_independently(self):
        f = INPUT_TESTS.Fixture()
        self.addCleanup(f.temporary.cleanup)
        inputs = f.prepare()
        commits = f.commits()
        package = f.base / "final-package"
        for record in inputs["extraInputs"].values():
            target = package / "input-objects" / record["sha256"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((f.output / "objects" / record["sha256"]).read_bytes())
        # Only external/generated inputs are needed; committed source bytes come from actual Git blobs.
        versioned = {str(Path(repo["sourceRoot"]) / path) for repo in inputs["repositories"].values()
                     for path, record in repo["files"].items() if record != {"deleted": True}}
        for source, record in inputs["extraInputs"].items():
            if source in versioned:
                target = package / "input-objects" / record["sha256"]
                if target.exists():
                    target.unlink()
        C.validate_committed_inputs(package, inputs, f.roots, commits)
        source = f.roots["kafka"]
        (source / "source.txt").write_text("untested committed source\n")
        INPUT_TESTS.git(source, "add", "--", "source.txt")
        INPUT_TESTS.git(source, "commit", "-qm", "contract incompatible source")
        commits["kafka"] = INPUT_TESTS.git(source, "rev-parse", "HEAD")
        with self.assertRaisesRegex(C.EvidenceError, "differs from.*executed complete tree"):
            C.validate_committed_inputs(package, inputs, f.roots, commits)

    def test_m6_or_focused_child_receipts_cannot_replace_the_complete_final(self):
        with tempfile.TemporaryDirectory(prefix="m5-scenario-contract-") as directory:
            root = Path(directory)
            target = root / C.SCENARIOS
            target.parent.mkdir(parents=True)
            final = C.EVIDENCE_PREFIX / "final/final-source-test/m5-final.json"
            rows = [{"id": identity, "status": "PASSED_CURRENT_SOURCE", "evidenceReceipt": str(final)}
                    for identity in C.PROMOTED]
            rows += [{"id": identity, "status": "PLANNED", "evidenceReceipt": None} for identity in C.DEFERRED]
            target.write_text(json.dumps({"scenarios": rows}))
            C.validate_scenarios(root, final)
            rows[-1]["status"] = "PASSED_CURRENT_SOURCE"
            rows[-1]["evidenceReceipt"] = str(final)
            target.write_text(json.dumps({"scenarios": rows}))
            with self.assertRaisesRegex(C.EvidenceError, "M6-deferred"):
                C.validate_scenarios(root, final)
            rows[-1]["status"], rows[-1]["evidenceReceipt"] = "PLANNED", None
            rows[0]["evidenceReceipt"] = "docs/v2/evidence/v2-m5/children/focused/receipt.json"
            target.write_text(json.dumps({"scenarios": rows}))
            with self.assertRaises(C.EvidenceError):
                C.validate_scenarios(root, final)


class CiPulsarSelectionContracts(unittest.TestCase):
    def fixture(self):
        f = INPUT_TESTS.Fixture()
        self.addCleanup(f.temporary.cleanup)
        root = f.roots['nereus']
        locks = root / 'docs/v2/source-locks.json'
        locks.parent.mkdir(parents=True)
        locks.write_bytes(C.canonical({'contract': 'CI selection only'}))
        inputs = f.prepare()
        commits = f.commits()
        INPUT_TESTS.git(root, 'add', '--', 'docs/v2/source-locks.json')
        INPUT_TESTS.git(root, 'commit', '-qm', 'fixture complete source')
        commits['nereus'] = INPUT_TESTS.git(root, 'rev-parse', 'HEAD')
        path = C.EVIDENCE_PREFIX / 'final' / ('final-source-' + commits['nereus']) / 'm5-final.json'
        package = root / path.parent
        package.mkdir(parents=True)
        value = {'schema': 'NEREUS_V2_M5_FINAL_V1', 'kind': 'V2_M5_FINAL', 'result': 'PASS_V2_M5_FINAL',
            'promotionEligible': True, 'executionEvidence': True,
            'inputRootSha256': inputs['inputRootSha256'], 'sourceLocksSha256': sha(locks.read_bytes()),
            'sourceTuple': {'commits': commits, 'sourceLocksSha256': sha(locks.read_bytes())}}
        (package / 'input-closure.json').write_bytes(C.canonical(inputs))
        (package / 'commit-binding.json').write_bytes(C.canonical({
            'schema': 'NEREUS_V2_M5_CANDIDATE_COMMIT_EQUIVALENCE_V1', 'kind': 'INPUT_EQUIVALENCE_ONLY',
            'promotionEligible': False, 'executionEvidence': False,
            'inputRootSha256': inputs['inputRootSha256'], 'commits': commits}))
        (root / path).write_bytes(C.canonical(value))
        rows = [{'id': identity, 'status': 'PASSED_CURRENT_SOURCE', 'evidenceReceipt': str(path)}
                for identity in C.PROMOTED]
        rows += [{'id': identity, 'status': 'PLANNED', 'evidenceReceipt': None} for identity in C.DEFERRED]
        (root / C.SCENARIOS).write_text(json.dumps({'scenarios': rows}))
        return f, path, value

    def test_final_metadata_selects_exact_sdk_and_propagates_archive_rejection(self):
        f, path, value = self.fixture()
        # This fixture supplies only selector metadata. Existing aggregate contracts cover the real
        # archive reader; the C5 smoke check must use the complete published archive without a stub.
        with patch.object(C, 'validate_package') as archive:
            self.assertEqual(value['sourceTuple']['commits']['pulsar'], C.ci_pulsar_commit(f.roots['nereus']))
            self.assertEqual(f.roots['nereus'] / path.parent, archive.call_args.args[0])
            with patch('sys.stdout', new_callable=io.StringIO) as output:
                self.assertEqual(0, C.main(['ci-pulsar-commit', '--repo-root', str(f.roots['nereus'])]))
                self.assertEqual(value['sourceTuple']['commits']['pulsar'] + '\n', output.getvalue())
        with patch.object(C, 'validate_package', side_effect=C.EvidenceError('incomplete archive')):
            with self.assertRaisesRegex(C.EvidenceError, 'incomplete archive'):
                C.ci_pulsar_commit(f.roots['nereus'])

    def test_missing_nonfinal_malformed_or_unbound_selection_fails(self):
        f, path, value = self.fixture()
        root = f.roots['nereus']
        target = root / path
        original = target.read_bytes()
        target.unlink()
        with self.assertRaises(OSError):
            C.ci_pulsar_commit(root)
        for key, replacement in [('schema', 'NEREUS_V2_M5_COMPLETE_CANDIDATE_V1'),
                ('kind', 'V2_M5_COMPLETE_CANDIDATE'), ('result', 'PENDING'),
                ('promotionEligible', False), ('executionEvidence', False),
                ('sourceLocksSha256', '0' * 64)]:
            with self.subTest(field=key):
                altered = copy.deepcopy(value)
                altered[key] = replacement
                target.write_bytes(C.canonical(altered))
                with self.assertRaises(C.EvidenceError):
                    C.ci_pulsar_commit(root)
        altered = copy.deepcopy(value)
        altered['sourceTuple']['commits']['pulsar'] = 'short-sha'
        target.write_bytes(C.canonical(altered))
        with self.assertRaisesRegex(C.EvidenceError, 'full commit IDs'):
            C.ci_pulsar_commit(root)
        target.write_bytes(original)
        binding = target.parent / 'commit-binding.json'
        binding.unlink()
        with self.assertRaises(OSError):
            C.ci_pulsar_commit(root)

    def test_workflow_checks_out_the_selected_commit_after_the_sdk_branch_advances(self):
        f, _, value = self.fixture()
        selected = value['sourceTuple']['commits']['pulsar']
        sdk = f.roots['pulsar']
        (sdk / 'source.txt').write_text('later SDK branch state\n')
        INPUT_TESTS.git(sdk, 'add', '--', 'source.txt')
        INPUT_TESTS.git(sdk, 'commit', '-qm', 'fixture later SDK tip')
        self.assertNotEqual(selected, INPUT_TESTS.git(sdk, 'rev-parse', 'HEAD'))
        workflow = (Path(__file__).resolve().parent.parent / '.github/workflows/build.yml').read_text()
        block = workflow.split('      - name: Check out source-locked Pulsar composite for current modules\n', 1)[1]
        block = textwrap.dedent(block.split('        run: |\n', 1)[1].split('      - name:', 1)[0])
        block = block.replace('pulsar_commit="$(python3 scripts/check-v2-m5-evidence.py ci-pulsar-commit)"',
                              'pulsar_commit=' + selected)
        runner = f.base / 'runner'
        runner.mkdir()
        env = dict(os.environ, RUNNER_TEMP=str(runner), GIT_CONFIG_COUNT='1',
            GIT_CONFIG_KEY_0='url.' + sdk.as_uri() + '.insteadOf',
            GIT_CONFIG_VALUE_0='https://github.com/nereusstream/pulsar.git')
        subprocess.run(['bash', '-eu', '-o', 'pipefail', '-c', block], cwd=f.roots['nereus'], env=env,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        self.assertEqual(selected, INPUT_TESTS.git(runner / 'nsip-pulsar-source', 'rev-parse', 'HEAD'))


class AggregateContracts(unittest.TestCase):
    def test_final_current_source_allows_only_evidence_successors(self):
        f = INPUT_TESTS.Fixture()
        self.addCleanup(f.temporary.cleanup)
        commits = f.commits()
        root = f.roots["nereus"]
        artifact = root / "docs/v2/evidence/v2-m5/final/contract-only.txt"
        artifact.parent.mkdir(parents=True)
        artifact.write_text("source-gate contract only\n")
        INPUT_TESTS.git(root, "add", "--", str(artifact.relative_to(root)))
        INPUT_TESTS.git(root, "commit", "-qm", "fixture evidence successor")
        C.validate_descendants(root, commits["nereus"])
        untested = root / "untested.java"
        untested.write_text("contract untested runtime\n")
        with self.assertRaisesRegex(C.EvidenceError, "untested non-evidence"):
            C.validate_descendants(root, commits["nereus"])
        untested.unlink()
        (root / "source.txt").write_text("contract changed runtime\n")
        INPUT_TESTS.git(root, "add", "--", "source.txt")
        INPUT_TESTS.git(root, "commit", "-qm", "fixture untested runtime successor")
        with self.assertRaisesRegex(C.EvidenceError, "runtime/source changed"):
            C.validate_descendants(root, commits["nereus"])

    def fixture(self):
        temporary = tempfile.TemporaryDirectory(prefix="m5-aggregate-contract-")
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        anchors = set(anchor for group in (*C.ANCHORS.values(), *C.CORE_ANCHORS.values()) for anchor in group)
        executions, inputs = {}, None
        for stage in C.STAGES:
            f = Fixture(stage)
            self.addCleanup(f.temp.cleanup)
            cases = [(cls, method) for cls, method in anchors if C.CLASS_STAGES[cls] == stage]
            if stage == "NATIVE_KAFKA":
                methods = {row[0] for row in C.EXPECTED_KAFKA57}
                cases = [(cls, method) for cls, method in cases if method not in methods]
                indices = {}
                for method, mode in C.EXPECTED_KAFKA57:
                    indices[method] = indices.get(method, 0) + 1
                    cases.append((C.KAFKA, f"{method}(String)[{indices[method]}]{mode}"))
            if not cases:
                cases = [("ContractOnly", "passes")]
            if stage == 'NATIVE_PULSAR':
                cases = [(C.PULSAR, name) for name in C.EXPECTED_PULSAR81]
            raw = ET.fromstring(suite(sorted(cases)))
            if stage == "NATIVE_KAFKA":
                ET.SubElement(raw, "system-out").text = "M5_CANONICAL_CAPACITY_BYTES 500000000\n"
            if stage == 'NATIVE_PULSAR':
                owner_quota, final_quota = pulsar_quota_fixture()
                ET.SubElement(raw, 'system-out').text = owner_quota
            row = f.write(f"executions/{stage}/raw.xml", ET.tostring(raw))
            old = {key: f.record["junit"][0][key] for key in ("path", "bytes", "sha256")}
            f.record["attachments"] = [row if value == old else value for value in f.record["attachments"]]
            f.record["junit"] = [row | {"task": ":contract:fixture", "producedAtMicros": 1500}]
            if stage == 'NATIVE_PULSAR':
                f.record['junit'][0]['task'] = ':nereus-pulsar-offload:v2M5NativeM4OwnerRealTest'
                negative = ET.fromstring(suite([(C.PULSAR, 'nativeBindingCapturesExactP1OwnerAndObjectNamespace')]))
                ET.SubElement(negative, 'system-out').text = final_quota
                negative_row = f.write(f'executions/{stage}/namespace.xml', ET.tostring(negative))
                f.record['attachments'].append(negative_row)
                f.record['junit'].append(negative_row | {
                    'task': ':nereus-pulsar-offload:v2M5NativeOffloadRealTest', 'producedAtMicros': 1500})
            f.write(f"executions/{stage}/record.json", C.canonical(f.record))
            shutil.copytree(f.root / "executions" / stage, root / "executions" / stage)
            if inputs is None:
                inputs = copy.deepcopy(f.inputs)
            else:
                inputs["extraInputs"].update(f.inputs["extraInputs"])
                inputs["repositories"]["nereus"]["files"].update(f.inputs["repositories"]["nereus"]["files"])
        for stage in C.STAGES:
            executions[stage] = C.validate_execution(root, C.load((root / f"executions/{stage}/record.json").read_bytes()), inputs)
        original_records = {stage: (root / f'executions/{stage}/record.json').read_bytes() for stage in C.STAGES}
        RUNNER.assemble(root, inputs, executions)
        self.assertEqual(original_records, {stage: (root / f'executions/{stage}/record.json').read_bytes()
                                           for stage in C.STAGES})
        return root, inputs, C.load((root / "candidate.json").read_bytes())

    def validate(self, root, inputs, value):
        # Shared M4 predicates/metrics have their own existing exact parser contracts. The fixture
        # here deliberately supplies no M4 execution evidence and exercises only the M5 aggregator.
        with patch.object(C, "validate_current_m4"):
            return C.validate_package(root, value, inputs)

    def test_actual_assembler_and_complete_five_child_validator_agree(self):
        root, inputs, value = self.fixture()
        result = self.validate(root, inputs, value)
        self.assertEqual(set(C.KINDS), set(result["children"]))
        self.assertEqual(17, len(value["obligations"]))
        claimed = [case for child in result["children"].values() for case in child["promotionCases"]]
        self.assertEqual(len(claimed), len(set(claimed)))

    def test_missing_child_or_execution_and_false_final_authority_reject(self):
        root, inputs, value = self.fixture()
        for key in ("childReceipts", "executionReceipts"):
            altered = copy.deepcopy(value)
            altered[key].pop()
            with self.assertRaises(C.EvidenceError):
                self.validate(root, inputs, altered)
        value["promotionEligible"] = True
        with self.assertRaisesRegex(C.EvidenceError, "authority differs"):
            self.validate(root, inputs, value)

    def test_fixed_capacity_must_be_observed_in_exact_executed_raw_xml(self):
        root, inputs, value = self.fixture()
        facts = C.load((root / "physical-capacity.json").read_bytes())
        execution = C.validate_execution(root,
            C.load((root / "executions/NATIVE_KAFKA/record.json").read_bytes()), inputs)
        C.validate_capacity(root, "NATIVE_KAFKA", facts, execution)
        facts["canonicalCapacityBytes"] += 1
        with self.assertRaisesRegex(C.EvidenceError, "canonical capacity differs"):
            C.validate_capacity(root, "NATIVE_KAFKA", facts, execution)
        facts["canonicalCapacityBytes"] -= 1
        facts["rawEvidence"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(C.EvidenceError, "exact actual raw suite"):
            C.validate_capacity(root, "NATIVE_KAFKA", facts, execution)

    def test_pulsar_budget_refund_readmission_and_final_observations_are_independent_requirements(self):
        root, inputs, _ = self.fixture()
        original_facts = C.load((root / 'physical-capacity-pulsar.json').read_bytes())
        original_execution = C.validate_execution(root,
            C.load((root / 'executions/NATIVE_PULSAR/record.json').read_bytes()), inputs)
        C.validate_capacity(root, 'NATIVE_PULSAR', original_facts, original_execution)
        row = original_facts['rawEvidence'][0]
        path = root / row['path']
        original = path.read_bytes()
        for phase, mutation, error in [
                ('gc-after', 'charge', 'canonical charge'),
                ('gc-after', 'entry', 'wire/checksum'),
                ('gc-after', 'unframed-resource', 'actual GC exact entry/DONE settlement'),
                ('bk-gc-after', 'charge', 'canonical charge'),
                ('bk-gc-after', 'entry', 'wire/checksum'),
                ('bk-gc-before', 'remove', 'exact GC resource observation pairs differ'),
                ('readmission-before', 'remove', 'lifecycle observations incomplete'),
                ('bk-final', 'capacity', 'fixed budget'),
                ('bk-final', 'bound', 'lifetime resource bound')]:
            with self.subTest(phase=phase, mutation=mutation):
                tree = ET.fromstring(original)
                output = tree.find('system-out')
                lines = output.text.splitlines()
                index = next(i for i, line in enumerate(lines) if 'phase=' + phase + ' ' in line)
                sample = dict(field.split('=', 1) for field in lines[index].split()[1:])
                if mutation == 'remove':
                    lines.pop(index)
                else:
                    if mutation == 'charge':
                        sample['charged'] = str(int(sample['charged']) + 1)
                    elif mutation == 'entry':
                        raw = bytearray(C.base64.b64decode(sample['entry']))
                        raw[84] ^= 1
                        sample['entry'] = C.base64.b64encode(raw).decode()
                    elif mutation == 'unframed-resource':
                        done = C.base64.b64decode(sample['done'])
                        wrong_digest = hashlib.sha256(done[12:12 + int.from_bytes(done[8:12], 'big')]).digest()
                        before_index = next(i for i, line in enumerate(lines) if 'phase=gc-before ' in line)
                        before = dict(field.split('=', 1) for field in lines[before_index].split()[1:])
                        for pair in (before, sample):
                            pair['resource'] = wrong_digest.hex()
                            raw = bytearray(C.base64.b64decode(pair['entry']))
                            raw[40:72] = wrong_digest
                            raw[-32:] = hashlib.sha256(raw[:-32]).digest()
                            pair['entry'] = C.base64.b64encode(raw).decode()
                        lines[before_index] = 'NATIVE_PULSAR_QUOTA ' + ' '.join(
                            f'{key}={value}' for key, value in before.items())
                    else:
                        raw = bytearray(C.base64.b64decode(sample['head']))
                        if mutation == 'capacity':
                            struct.pack_into('>q', raw, 48, 64 * 1024 * 1024)
                            sample['available'] = str(64 * 1024 * 1024 - int(sample['charged']))
                        else:
                            struct.pack_into('>q', raw, 56, 445)
                            extra = (445 - 9) * 1057887
                            sample['charged'] = str(int(sample['charged']) + extra)
                            sample['available'] = str(500_000_000 - int(sample['charged']))
                        raw[-32:] = hashlib.sha256(raw[:-32]).digest()
                        sample['head'] = C.base64.b64encode(raw).decode()
                    lines[index] = 'NATIVE_PULSAR_QUOTA ' + ' '.join(f'{key}={value}' for key, value in sample.items())
                output.text = '\n'.join(lines) + '\n'
                raw = ET.tostring(tree)
                path.write_bytes(raw)
                replacement = {'path': row['path'], 'bytes': len(raw), 'sha256': sha(raw)}
                facts, execution = copy.deepcopy(original_facts), copy.deepcopy(original_execution)
                facts['rawEvidence'] = [replacement if item['path'] == row['path'] else item
                                        for item in facts['rawEvidence']]
                execution['suites'] = [item | replacement if item['path'] == row['path'] else item
                                       for item in execution['suites']]
                with self.assertRaisesRegex(C.EvidenceError, error):
                    C.validate_capacity(root, 'NATIVE_PULSAR', facts, execution)
        path.write_bytes(original)
        facts = copy.deepcopy(original_facts)
        facts['rawEvidence'].pop()
        with self.assertRaisesRegex(C.EvidenceError, 'final namespace observations missing'):
            C.validate_capacity(root, 'NATIVE_PULSAR', facts, original_execution)

    def test_same_size_pulsar_corpus_cannot_replace_a_required_native_lifecycle_method(self):
        root, inputs, _ = self.fixture()
        facts = C.load((root / 'physical-capacity-pulsar.json').read_bytes())
        execution = C.validate_execution(root,
            C.load((root / 'executions/NATIVE_PULSAR/record.json').read_bytes()), inputs)
        row = facts['rawEvidence'][0]
        path = root / row['path']
        tree = ET.fromstring(path.read_bytes())
        case = next(case for case in tree.findall('testcase') if case.get('name')
                    == 'nativeBookKeeperReplacementRequiresExactReleaseAndReconcilesUnknown()')
        case.set('name', 'unrelatedSameCountCase()')
        raw = ET.tostring(tree)
        path.write_bytes(raw)
        replacement = {'path': row['path'], 'bytes': len(raw), 'sha256': sha(raw)}
        facts['rawEvidence'] = [replacement if item['path'] == row['path'] else item
                                for item in facts['rawEvidence']]
        execution['suites'] = [item | replacement if item['path'] == row['path'] else item
                               for item in execution['suites']]
        with self.assertRaisesRegex(C.EvidenceError, 'complete fixed Pulsar81 corpus differs'):
            C.validate_capacity(root, 'NATIVE_PULSAR', facts, execution)

    def test_missing_shared_kafka_mode_and_unreferenced_aggregate_file_reject(self):
        root, inputs, value = self.fixture()
        record = C.load((root / "executions/NATIVE_KAFKA/record.json").read_bytes())
        row = record["junit"][0]
        tree = ET.fromstring((root / row["path"]).read_bytes())
        case = next(node for node in tree.findall("testcase") if "(String)[" in node.get("name"))
        tree.remove(case)
        tree.set("tests", str(int(tree.get("tests")) - 1))
        raw = ET.tostring(tree)
        (root / row["path"]).write_bytes(raw)
        replacement = {"path": row["path"], "bytes": len(raw), "sha256": sha(raw)}
        record["attachments"] = [replacement if node["path"] == row["path"] else node for node in record["attachments"]]
        record["junit"][0].update(replacement)
        with self.assertRaisesRegex(C.EvidenceError, "Kafka57 corpus"):
            C.validate_execution(root, record, inputs)
        root, inputs, value = self.fixture()
        (root / "undeclared.txt").write_text("contract extra")
        with self.assertRaisesRegex(C.EvidenceError, "unreferenced files"):
            self.validate(root, inputs, value)

    def test_shared_kafka_modes_require_explicit_mode_and_exact_array_index(self):
        root, inputs, _ = self.fixture()
        original = C.load((root / "executions/NATIVE_KAFKA/record.json").read_bytes())
        row = original["junit"][0]
        xml = (root / row["path"]).read_bytes()
        for name in ("nativeM5PublishesItsActualTaskAndFetchesSelectedBytes(String)[1]",
                     "nativeM5PublishesItsActualTaskAndFetchesSelectedBytes(String)[2]TASK_OUTPUT_PUBLISH",
                     "nativeM5PublishesItsActualTaskAndFetchesSelectedBytes(String).mode=TASK_OUTPUT_PUBLISH"):
            with self.subTest(name=name):
                record = copy.deepcopy(original)
                tree = ET.fromstring(xml)
                case = next(node for node in tree.findall("testcase")
                            if node.get("name") == "nativeM5PublishesItsActualTaskAndFetchesSelectedBytes(String)[1]TASK_OUTPUT_PUBLISH")
                case.set("name", name)
                raw = ET.tostring(tree)
                (root / row["path"]).write_bytes(raw)
                replacement = {"path": row["path"], "bytes": len(raw), "sha256": sha(raw)}
                record["attachments"] = [replacement if node["path"] == row["path"] else node
                                         for node in record["attachments"]]
                record["junit"][0].update(replacement)
                with self.assertRaisesRegex(C.EvidenceError, "shared Kafka scenario"):
                    C.validate_execution(root, record, inputs)


class CurrentM4InventoryContracts(unittest.TestCase):
    """Inventory contracts only; measurements retain the existing M4 parser and raw XML."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='m5-current-m4-inventory-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.inputs = {'repositories': {'nereus': {'baseCommit': '0' * 40}}}
        self.rows = []
        for kind, tasks in C.M4_TASKS.items():
            for task in tasks:
                names = C.M4.EXPECTED_SUITE_TESTS[kind][task] | C.CURRENT_M4_ADDITIONAL_TESTS.get(task, set())
                row = {'task': task, 'path': sha(task.encode()) + '.xml'}
                self.write_suite(row, C.M4.EXPECTED_SUITES[kind][task], names)
                self.rows.append(row)

    def write_suite(self, row, classname, names):
        tree = ET.fromstring(suite([(classname, name) for name in sorted(names)]))
        tree.set('name', classname)
        raw = ET.tostring(tree)
        (self.root / row['path']).write_bytes(raw)
        row.update(bytes=len(raw), sha256=sha(raw))

    def validate(self):
        return C.validate_current_m4(self.root, {'COMPONENTS': {'suites': self.rows}}, self.inputs)

    def test_exact_current_inventory_preserves_raw_xml_and_historical_reader(self):
        original = copy.deepcopy(C.M4.EXPECTED_SUITE_TESTS)
        with patch.object(C.M4, 'junit_metrics') as metrics:
            self.validate()
        self.assertEqual(len(C.M4_TASKS), metrics.call_count)
        for call in metrics.call_args_list:
            payload, kind = call.args
            for row in payload['suites']:
                source = next(item for item in self.rows if item['task'] == row['task'])
                self.assertEqual((self.root / source['path']).read_bytes(), C.base64.b64decode(row['xmlBase64']))
            if kind == 'QUIESCENCE_PROTECTION_RELEASE':
                with self.assertRaisesRegex(C.M4.EvidenceError, 'suite exact test inventory differs'):
                    C.M4.validate_junit(payload, kind, '0' * 40)
        self.assertEqual(original, C.M4.EXPECTED_SUITE_TESTS)

    def test_missing_original_or_added_method_and_unknown_extra_method_are_rejected(self):
        kind = 'QUIESCENCE_PROTECTION_RELEASE'
        task = ':nereus-metadata-oxia:v2M4ReadControlOxiaAdapterTest'
        row = next(item for item in self.rows if item['task'] == task)
        original = C.M4.EXPECTED_SUITE_TESTS[kind][task]
        added = C.CURRENT_M4_ADDITIONAL_TESTS[task]
        names = original | added
        for changed in (names - {sorted(original)[0]}, names - {sorted(added)[0]},
                        names | {'unregisteredCurrentMethod'}):
            with self.subTest(names=sorted(changed)):
                self.write_suite(row, C.M4.EXPECTED_SUITES[kind][task], changed)
                with patch.object(C.M4, 'junit_metrics'), \
                        self.assertRaisesRegex(C.EvidenceError, 'current shared M4 exact test inventory differs'):
                    self.validate()


class ReaderOnlyEquivalenceContracts(unittest.TestCase):
    def setUp(self):
        self.reader = C.module('m5_reader_only_contract', 'm5-reader-only-evidence.py')

    def test_one_hop_rejects_runtime_native_source_and_unreviewed_reader_changes(self):
        self.assertEqual('eb378e578695a923a2fb0bfe070dcde5c6e7636e653e48a1fcbdd4f9b5d19d9e',
                         self.reader.PRIOR_ROOT)
        fixture = INPUT_TESTS.Fixture()
        self.addCleanup(fixture.temporary.cleanup)
        prior = fixture.prepare()
        final = copy.deepcopy(prior)
        path = 'new-source.txt'
        before = prior['repositories']['nereus']['files'][path]
        final['repositories']['nereus']['files'][path]['sha256'] = 'a' * 64
        raw = Path(self.reader.__file__).read_bytes()
        final['repositories']['nereus']['files'][self.reader.SCRIPT] = {
            'mode': '100644', 'bytes': len(raw), 'sha256': sha(raw)}

        def freeze(value):
            value['inputRootSha256'] = sha(C.canonical({key: value[key] for key in
                ('repositories', 'extraInputs', 'inputManifests')}))
            return value

        final = freeze(final)
        with patch.object(self.reader, 'PRIOR_ROOT', prior['inputRootSha256']), \
                patch.object(self.reader, 'SOURCE_HASHES', {path: (before['sha256'], 'a' * 64)}):
            self.assertEqual(2, len(self.reader.check_inputs(prior, final, C)))
            for mutation in ('dependency', 'native-source', 'reader-source', 'new-entry'):
                with self.subTest(mutation=mutation):
                    changed = copy.deepcopy(final)
                    if mutation == 'dependency':
                        changed['extraInputs'][str(fixture.jar)]['sha256'] = 'b' * 64
                    else:
                        repo, name = ('kafka', 'source.txt') if mutation == 'native-source' else (
                            'nereus', path if mutation == 'reader-source' else self.reader.SCRIPT)
                        changed['repositories'][repo]['files'][name]['sha256'] = 'b' * 64
                    with self.assertRaises(C.EvidenceError):
                        self.reader.check_inputs(prior, freeze(changed), C)

    def test_original_record_and_loaded_input_cannot_be_relabeled(self):
        stage = 'NATIVE_PULSAR'
        size, digest = self.reader.RECORDS[stage]
        reference = {'path': f'executions/{stage}/record.json', 'bytes': size, 'sha256': digest}
        self.reader.check_record_identity(stage, reference, C)
        with self.assertRaisesRegex(C.EvidenceError, 'original record identity'):
            self.reader.check_record_identity(stage, reference | {'sha256': '0' * 64}, C)
        expected = {'bytes': 3, 'sha256': sha(b'jar')}
        inputs = {'extraInputs': {'/contract/runtime.jar': expected}, 'repositories': {}}
        record = {'runtimeInputs': [{'sourcePath': '/contract/runtime.jar', **expected}]}
        self.reader.check_runtime(record, inputs, C)
        record['runtimeInputs'][0]['sha256'] = sha(b'different')
        with self.assertRaisesRegex(C.EvidenceError, 'actually loaded runtime input'):
            self.reader.check_runtime(record, inputs, C)

    def test_reader_filename_does_not_allow_execution_boundary_changes(self):
        reader = b'def validate_current_m4(): pass\ndef validate_capacity(): pass\ndef validate_execution(): return 1\n'
        tests = b'def pulsar_quota_fixture(): pass\n'
        continuation = b'def prepare_postprocessing_source(): pass\ndef main(): return 1\n'
        dispatcher = b'def validate(root, reference, final, C):\n    C.require(True, "contract")\n    proof = {}\n    return proof\n'
        before = dict(zip(('scripts/check-v2-m5-evidence.py', 'scripts/check-v2-m5-evidence-tests.py',
                           'scripts/continue-v2-m5-evidence.py', 'scripts/m5-stage-input-equivalence.py'),
                          (reader, tests, continuation, dispatcher)))
        after = copy.deepcopy(before)
        after['scripts/m5-stage-input-equivalence.py'] = dispatcher.replace(b'    return proof', (
            "    if proof.get('schema') == '" + self.reader.SCHEMA + "':\n"
            "        return C.module('m5_eb_reader_equivalence', 'm5-reader-only-evidence.py').validate(root, reference, final, C)\n"
            "    return proof").encode())
        self.assertEqual(4, len(self.reader.check_boundaries(before, after, C)))
        after['scripts/check-v2-m5-evidence.py'] = reader.replace(b'return 1', b'return 2')
        with self.assertRaisesRegex(C.EvidenceError, 'execution/collection boundary'):
            self.reader.check_boundaries(before, after, C)


class StageEquivalenceContracts(unittest.TestCase):
    def test_exact_pulsar_fixture_allowance_keeps_corpus_and_rejects_actual_prior_usage(self):
        before = b'\n'.join(f'@Test void case{i}() throws Exception {{ nativeBindingCapture({i}); }}'.encode()
                            for i in range(82))
        after = before + b'\n// observed fixed budget\n'
        with patch.object(C.EQUIVALENCE, 'PULSAR_FIXTURE_BEFORE', sha(before)), \
                patch.object(C.EQUIVALENCE, 'PULSAR_FIXTURE_AFTER', sha(after)):
            C.EQUIVALENCE.check_pulsar_fixture(before, after, C)
            changed = after.replace(b'nativeBindingCapture(81)', b'nativeBindingCapture(80)')
            with patch.object(C.EQUIVALENCE, 'PULSAR_FIXTURE_AFTER', sha(changed)):
                with self.assertRaisesRegex(C.EvidenceError, 'test/mode/order inventory'):
                    C.EQUIVALENCE.check_pulsar_fixture(before, changed, C)
        f = Fixture('OBJECT')
        self.addCleanup(f.temp.cleanup)
        C.EQUIVALENCE.check_pulsar_fixture_unused(f.root, f.record, C)
        altered = copy.deepcopy(f.record)
        altered['runtimeInputs'].append({'sourcePath': '/contract/M5NativePulsarOffloadRealTest$Observer.class'})
        with self.assertRaisesRegex(C.EvidenceError, 'actually loaded'):
            C.EQUIVALENCE.check_pulsar_fixture_unused(f.root, altered, C)
        row = f.write('executions/OBJECT/pulsar-compile.log',
                      b'> Task :nereus-pulsar-offload:compileP6ProviderTestJava\n')
        altered = copy.deepcopy(f.record)
        altered['attachments'].append(row)
        with self.assertRaisesRegex(C.EvidenceError, 'actually compiled'):
            C.EQUIVALENCE.check_pulsar_fixture_unused(f.root, altered, C)

    def test_postprocessing_boundary_keeps_dispatch_and_import_configuration(self):
        before = Path(RUNNER.__file__).read_bytes()
        tree = ast.parse(before)
        assembly = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'assemble')
        assembly.body = [ast.Pass()]
        changed = ast.unparse(ast.fix_missing_locations(tree)).encode()
        C.EQUIVALENCE.check_function_boundary(before, changed, {'assemble'}, C)
        dispatch = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'execute_stage')
        dispatch.body = [ast.Pass()]
        with self.assertRaisesRegex(C.EvidenceError, 'dispatch/import/configuration'):
            C.EQUIVALENCE.check_function_boundary(before, ast.unparse(ast.fix_missing_locations(tree)).encode(),
                                                   {'assemble'}, C)

    def test_native_to_final_is_direct_and_forbids_runtime_dependency_or_source_changes(self):
        _, prior = self.fixture()
        path = 'scripts/check-v2-m5-evidence.py'
        prior['repositories']['nereus']['files'][path] = {'mode': '100644', 'bytes': 3, 'sha256': sha(b'old')}
        self.rehash(prior)
        final = copy.deepcopy(prior)
        final['repositories']['nereus']['files'][path] = {'mode': '100644', 'bytes': 3, 'sha256': sha(b'new')}
        self.rehash(final)
        with patch.object(C.EQUIVALENCE, 'NATIVE_ROOT', prior['inputRootSha256']):
            self.assertEqual([path], [row['path'] for row in C.EQUIVALENCE.check_native_postprocessing(prior, final, C)])
            for repository, source in [('nereus', C.EQUIVALENCE.NATIVE_DRIVER),
                                       ('nereus', 'config/v2/m5/evidence-native.init.gradle'),
                                       ('pulsar', 'managed-ledger/src/main/Runtime.java')]:
                altered = copy.deepcopy(final)
                altered['repositories'][repository]['files'][source] = {
                    'mode': '100644', 'bytes': 3, 'sha256': sha(b'bad')}
                self.rehash(altered)
                with self.assertRaisesRegex(C.EvidenceError, 'actual product/test/build/call-path'):
                    C.EQUIVALENCE.check_native_postprocessing(prior, altered, C)
            altered = copy.deepcopy(final)
            next(iter(altered['extraInputs'].values()))['sha256'] = '0' * 64
            self.rehash(altered)
            with self.assertRaisesRegex(C.EvidenceError, 'dependency or generated input'):
                C.EQUIVALENCE.check_native_postprocessing(prior, altered, C)
        with self.assertRaisesRegex(C.EvidenceError, 'original successful v11'):
            C.EQUIVALENCE.check_native_postprocessing(prior, final, C)

    def test_parent_observer_delta_cannot_change_actual_dispatch(self):
        before = Path(RUNNER.__file__).read_bytes()
        tree = ast.parse(before)
        observer = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'docker_inventory')
        observer.body = [ast.Return(value=ast.Dict(keys=[], values=[]))]
        after = ast.unparse(ast.fix_missing_locations(tree))
        proof = C.EQUIVALENCE.check_observer_change(before, after, C)
        self.assertNotEqual(proof['beforeAstSha256'], proof['afterAstSha256'])
        dispatch = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'execute_stage')
        dispatch.body.append(ast.Return(value=ast.Constant(value=None)))
        with self.assertRaisesRegex(C.EvidenceError, 'outside the approved read-only observer'):
            C.EQUIVALENCE.check_observer_change(before, ast.unparse(ast.fix_missing_locations(tree)), C)

    def test_container_teardown_between_list_and_inspect_keeps_exact_successful_observations(self):
        present, removed = "1" * 64, "2" * 64
        row = {"Id": present, "Name": "/owned-live"}
        result = SimpleNamespace(returncode=1, stdout=json.dumps([row]), stderr="error: no such object: " + removed + "\n")
        with patch.object(RUNNER.subprocess, "check_output", return_value=present + "\n" + removed), \
                patch.object(RUNNER.subprocess, "run", return_value=result):
            self.assertEqual({present: row}, RUNNER.docker_inventory())

    def test_container_observer_does_not_hide_engine_errors_or_unexplained_absence(self):
        identity = "1" * 64
        for result in (SimpleNamespace(returncode=1, stdout="[]", stderr="Docker engine unavailable\n"),
                       SimpleNamespace(returncode=0, stdout="[]", stderr="")):
            with patch.object(RUNNER.subprocess, "check_output", return_value=identity), \
                    patch.object(RUNNER.subprocess, "run", return_value=result):
                with self.assertRaises(RUNNER.C.EvidenceError):
                    RUNNER.docker_inventory()

    def fixture(self):
        f = INPUT_TESTS.Fixture()
        self.addCleanup(f.temporary.cleanup)
        prior = f.prepare()
        node = {"mode": "100644", "bytes": 3, "sha256": sha(b"src")}
        for path in C.EQUIVALENCE.CALL_PATHS:
            prior["repositories"]["nereus"]["files"][path] = dict(node)
        prior["repositories"]["nereus"]["files"][C.EQUIVALENCE.NATIVE_DRIVER] = {
            "mode": "100644", "bytes": 12353, "sha256": C.EQUIVALENCE.OLD_NATIVE_SHA}
        self.rehash(prior)
        final = copy.deepcopy(prior)
        final["repositories"]["nereus"]["files"][C.EQUIVALENCE.NATIVE_DRIVER] = {
            "mode": "100644", "bytes": 13062, "sha256": C.EQUIVALENCE.NEW_NATIVE_SHA}
        final["repositories"]["nereus"]["files"]["scripts/m5-stage-input-equivalence.py"] = dict(node)
        self.rehash(final)
        self.enterContext(patch.object(C.EQUIVALENCE, "PRIOR_ROOT", prior["inputRootSha256"]))
        return prior, final

    def rehash(self, value):
        value["inputRootSha256"] = C.INPUTS.digest(C.canonical({key: value[key] for key in
            ("repositories", "extraInputs", "inputManifests")}))

    def test_complete_inventory_identifies_every_approved_input_difference(self):
        prior, final = self.fixture()
        delta = C.EQUIVALENCE.check_inputs(prior, final, C)
        self.assertEqual({C.EQUIVALENCE.NATIVE_DRIVER, "scripts/m5-stage-input-equivalence.py"},
                         {row["path"] for row in delta})
        with self.assertRaisesRegex(C.EvidenceError, "omits or misstates"):
            C.EQUIVALENCE.check_reported_differences(prior, final, delta[:-1], C)
        wrong = copy.deepcopy(delta)
        wrong[0]["after"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(C.EvidenceError, "omits or misstates"):
            C.EQUIVALENCE.check_reported_differences(prior, final, wrong, C)
        final["repositories"]["nereus"]["files"]["docs/v2/NSIP/nsip-1.md"] = {
            "mode": "100644", "bytes": 3, "sha256": sha(b"doc")}
        self.rehash(final)
        self.assertEqual(3, len(C.EQUIVALENCE.check_inputs(prior, final, C)))

    def test_runtime_test_build_lock_or_foreign_source_change_invalidates_reuse(self):
        for path in ("src/main/Runtime.java", "src/test/Contract.java", "build.gradle.kts", "docs/v2/source-locks.json"):
            prior, final = self.fixture()
            final["repositories"]["nereus"]["files"][path] = {"mode": "100644", "bytes": 4, "sha256": sha(b"edit")}
            self.rehash(final)
            with self.assertRaisesRegex(C.EvidenceError, "actual product/test/build/call-path"):
                C.EQUIVALENCE.check_inputs(prior, final, C)
        prior, final = self.fixture()
        final["repositories"]["kafka"]["files"].pop("source.txt")
        self.rehash(final)
        with self.assertRaises(C.EvidenceError):
            C.EQUIVALENCE.check_inputs(prior, final, C)

    def test_dependency_bytes_and_unapproved_native_driver_cannot_be_rebound(self):
        prior, final = self.fixture()
        path = next(iter(final["extraInputs"]))
        final["extraInputs"][path]["sha256"] = "0" * 64
        self.rehash(final)
        with self.assertRaisesRegex(C.EvidenceError, "runtime input"):
            C.EQUIVALENCE.check_inputs(prior, final, C)
        prior, final = self.fixture()
        final["repositories"]["nereus"]["files"][C.EQUIVALENCE.NATIVE_DRIVER]["sha256"] = "0" * 64
        self.rehash(final)
        with self.assertRaisesRegex(C.EvidenceError, "approved create-only"):
            C.EQUIVALENCE.check_inputs(prior, final, C)

    def test_original_receipt_digest_and_stage_identity_are_not_rewritable(self):
        for stage, (size, digest) in C.EQUIVALENCE.ORIGINAL_RECORDS.items():
            reference = {"path": f"executions/{stage}/record.json", "bytes": size, "sha256": digest}
            C.EQUIVALENCE.check_original_reference(stage, reference, C)
            reference["sha256"] = "0" * 64
            with self.assertRaisesRegex(C.EvidenceError, "receipt was changed"):
                C.EQUIVALENCE.check_original_reference(stage, reference, C)
        with self.assertRaisesRegex(C.EvidenceError, "stage path differs"):
            C.EQUIVALENCE.check_original_reference("NATIVE_KAFKA", reference, C)


class NativeInputEquivalenceContracts(unittest.TestCase):
    def test_pulsar_source_exception_is_only_the_original_virtual_read_dispatch(self):
        before = b'class Fixture {\n        asyncReadEntry(ledgerId, ledger, firstEntry, lastEntry, opReadEntry, opReadEntry.ctx);\n}'
        fixed = (b'        if (ledger != null) {\n'
                 b'            asyncReadEntry(ledger, firstEntry, lastEntry, opReadEntry, opReadEntry.ctx);\n'
                 b'        } else {\n'
                 b'            asyncReadEntry(ledgerId, null, firstEntry, lastEntry, opReadEntry, opReadEntry.ctx);\n'
                 b'        }')
        after = before.replace(before.splitlines()[1], fixed)
        with patch.object(C.EQUIVALENCE, 'PULSAR_SOURCE_BEFORE', sha(before)), \
                patch.object(C.EQUIVALENCE, 'PULSAR_SOURCE_AFTER', sha(after)):
            C.EQUIVALENCE.check_pulsar_source(before, after, C)
            with self.assertRaisesRegex(C.EvidenceError, 'exact virtual-read'):
                C.EQUIVALENCE.check_pulsar_source(before, after.replace(b'ledgerId, null', b'ledgerId, fakeHandle'), C)

    def test_pulsar_preparation_covers_actual_component_and_sdk_test_dependencies(self):
        tree = ast.parse(Path(RUNNER.__file__).read_bytes())
        component = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'components')
        tasks = next(ast.literal_eval(node.value) for node in component.body if isinstance(node, ast.Assign)
                     and any(isinstance(target, ast.Name) and target.id == 'tasks' for target in node.targets))
        needed = {task for task in tasks if task.startswith(':')}
        needed.update(task for group in C.M4_TASKS.values() for task in group)
        self.assertLessEqual(needed, set(C.NATIVE_BUILD.PULSAR_TESTS))
        self.assertEqual(len(C.NATIVE_BUILD.PULSAR_TESTS), len(set(C.NATIVE_BUILD.PULSAR_TESTS)))
        root = Path('/prepared')
        self.assertTrue(C.NATIVE_BUILD.allowed_pulsar_missing(
            root / 'workspace/nereusstream/pulsar/managed-ledger/src/test/resources', root))
        self.assertFalse(C.NATIVE_BUILD.allowed_pulsar_missing(Path('/foreign/cache/missing.jar'), root))
        self.assertFalse(C.NATIVE_BUILD.pulsar_generated(
            root / 'workspace/nereusstream/pulsar/managed-ledger/src/main/java/example/build/Runtime.java', root))
        optional = root / 'workspace/nereusstream/pulsar/config/checkstyle'
        self.assertTrue(C.NATIVE_BUILD.allowed_pulsar_missing(optional, root, ':managed-ledger:checkstyleMain'))
        self.assertFalse(C.NATIVE_BUILD.allowed_pulsar_missing(optional, root, ':managed-ledger:compileJava'))

    @staticmethod
    def class_bytes(marker, source='NereusKafkaNativeInputV2.java'):
        # A bounded class-file fixture for the provenance reader, never execution evidence.
        def utf(value):
            raw = value.encode()
            return b'\x01' + struct.pack('>H', len(raw)) + raw
        pool = (utf('kafka/server/nereus/NereusKafkaNativeInputV2') + b'\x07\x00\x01'
                + utf('java/lang/Object') + b'\x07\x00\x03' + utf('SourceFile') + utf(source) + utf(marker))
        return (b'\xca\xfe\xba\xbe\x00\x00\x00\x3d\x00\x08' + pool
                + struct.pack('>HHHHHHHHIH', 0x21, 2, 4, 0, 0, 0, 1, 5, 2, 6))

    def fixture(self, foreign_jar_change=False, omit_manifest_edge=False):
        temporary = tempfile.TemporaryDirectory(prefix='m5-native-input-contract-')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        base = '/contract/native-kafka'
        binary = 'kafka/server/nereus/NereusKafkaNativeInputV2.class'
        class_path = base + '/core/build/classes/scala/main/' + binary
        jar_path = base + '/core/build/libs/kafka_2.13-4.3.0-SNAPSHOT.jar'
        manifest_path = '/contract/nereus/build/runtime-inputs.json'
        old_class, new_class = self.class_bytes('old'), self.class_bytes('new')
        def archive(raw, shared):
            output = io.BytesIO()
            with ZipFile(output, 'w') as jar:
                jar.writestr(binary, raw)
                jar.writestr('com/nereusstream/kafka/Shared.class', shared)
            return output.getvalue()
        old = {class_path: old_class, jar_path: archive(old_class, b'shared')}
        new = {class_path: new_class, jar_path: archive(new_class, b'changed' if foreign_jar_change else b'shared')}
        old[manifest_path] = C.canonical({path: sha(raw) for path, raw in old.items()})
        new[manifest_path] = C.canonical({path: (sha(old[path]) if omit_manifest_edge and path == class_path else sha(raw))
                                         for path, raw in new.items()})
        def cohort(files):
            return {'repositories': {'kafka': {'sourceRoot': base}, 'nereus': {'sourceRoot': '/contract/nereus'}},
                    'inputManifests': [{'path': manifest_path}],
                    'extraInputs': {path: {'bytes': len(raw), 'sha256': sha(raw)} for path, raw in files.items()}}
        prior, final = cohort(old), cohort(new)
        delta = C.NATIVE_INPUTS.check_inventory(prior, final, C)
        rows = []
        for change in delta:
            row = {'path': change['path']}
            for key, files in (('before', old), ('after', new)):
                raw = files[change['path']]
                path = 'source-continuation/native-inputs/' + sha(raw)
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)
                row[key] = {'path': path, 'bytes': len(raw), 'sha256': sha(raw)}
            rows.append(row)
        return root, rows, delta, prior, final, class_path

    def test_only_exact_unused_native_artifacts_are_reusable(self):
        root, rows, delta, prior, final, path = self.fixture()
        self.assertTrue(C.NATIVE_INPUTS.validate_objects(root, rows, delta, prior, final, C))
        C.NATIVE_INPUTS.check_stage_usage(delta, {'runtimeInputs': [{'sourcePath': '/contract/root/shared.jar'}]}, C)
        with self.assertRaisesRegex(C.EvidenceError, 'actually loaded a changed input'):
            C.NATIVE_INPUTS.check_stage_usage(delta, {'runtimeInputs': [{'sourcePath': path}]}, C)
        final['extraInputs']['/contract/root/shared.jar'] = {'bytes': 1, 'sha256': sha(b'x')}
        with self.assertRaisesRegex(C.EvidenceError, 'runtime input'):
            C.NATIVE_INPUTS.check_inventory(prior, final, C)

    def test_native_jar_cannot_hide_shared_class_or_manifest_changes(self):
        for foreign, omitted, message in ((True, False, 'another source entry'),
                                          (False, True, 'other edges or omitted')):
            root, rows, delta, prior, final, _ = self.fixture(foreign, omitted)
            with self.assertRaisesRegex(C.EvidenceError, message):
                C.NATIVE_INPUTS.validate_objects(root, rows, delta, prior, final, C)

    def test_native_class_requires_exact_declared_name_and_source(self):
        binary = 'kafka/server/nereus/NereusKafkaNativeInputV2.class'
        C.NATIVE_INPUTS.require_class(self.class_bytes('valid'), binary, C.NATIVE_INPUTS.MAIN, C)
        for raw, path in ((self.class_bytes('wrong', 'Other.java'), binary),
                          (self.class_bytes('wrong'), 'com/nereusstream/kafka/Shared.class'),
                          (self.class_bytes('truncated')[:-1], binary)):
            with self.assertRaises(C.EvidenceError):
                C.NATIVE_INPUTS.require_class(raw, path, C.NATIVE_INPUTS.MAIN, C)

    def test_prebuilt_native_class_requires_its_exact_producer_and_frozen_bytes(self):
        raw = self.class_bytes('compiled')
        directory = '/prepared/workspace/nereusstream/kafka/core/build/classes/scala/main'
        physical = directory + '/kafka/server/nereus/NereusKafkaNativeInputV2.class'
        logical = '/original/kafka/core/build/classes/scala/main/kafka/server/nereus/NereusKafkaNativeInputV2.class'
        producer = {'task': ':core:compileScala', 'classDirectory': directory,
                    'sources': [{'path': '/prepared/workspace/nereusstream/kafka/' + C.NATIVE_INPUTS.MAIN}]}
        item = {'physicalPath': physical, 'producer': producer['task'], 'bytes': len(raw), 'sha256': sha(raw)}
        preparation = {'provenance': {logical: item}, 'outputs': {physical: (producer, item)}}
        C.NATIVE_BUILD.validate_artifact(logical, raw, preparation, C)
        with self.assertRaisesRegex(C.EvidenceError, 'exact artifact'):
            C.NATIVE_BUILD.validate_artifact(logical, self.class_bytes('unregistered'), preparation, C)
        producer['sources'] = [{'path': '/prepared/unrelated/Other.java'}]
        with self.assertRaisesRegex(C.EvidenceError, 'actual source'):
            C.NATIVE_BUILD.validate_artifact(logical, raw, preparation, C)

    def test_class_source_reader_does_not_decode_unrelated_modified_utf8_constants(self):
        raw = self.class_bytes('a' + '\x00' + 'b')
        raw = raw.replace(b'\x01\x00\x03a\x00b', b'\x01\x00\x04a\xc0\x80b')
        self.assertEqual(('NereusKafkaNativeInputV2.java', 'kafka/server/nereus/NereusKafkaNativeInputV2'),
                         C.NATIVE_INPUTS.class_source(raw, C))

    def test_compact_producer_graph_rejects_missing_or_ambiguous_file_references(self):
        node = {'path': '/prepared/core/Native.class', 'bytes': 3, 'sha256': sha(b'raw')}
        graph = {'schema': 'NEREUS_V2_M5_NATIVE_PROJECT_BUILD_V2', 'project': ':core', 'files': [node],
                 'producers': [{'task': ':core:compileScala', 'inputs': [0], 'outputs': [0]}], 'runtime': [0]}
        expanded = C.NATIVE_BUILD.expand_project(copy.deepcopy(graph), C.require)
        self.assertEqual([node], expanded['runtime'])
        graph['runtime'] = [1]
        with self.assertRaisesRegex(C.EvidenceError, 'file reference'):
            C.NATIVE_BUILD.expand_project(copy.deepcopy(graph), C.require)
        graph['runtime'] = [0]
        graph['files'].append(dict(node))
        with self.assertRaisesRegex(C.EvidenceError, 'file table'):
            C.NATIVE_BUILD.expand_project(graph, C.require)

    def test_root_source_classifier_is_checked_against_source_instead_of_runtime_jar(self):
        source = b'package contract; class Input {}'
        output = io.BytesIO()
        with ZipFile(output, 'w') as archive:
            archive.writestr('contract/Input.java', source)
        raw = output.getvalue()
        path = ('/original/nereus/build/m5-evidence-development-repository/com/nereusstream/'
                'nereus-storage-api/0.2.0-SNAPSHOT/nereus-storage-api-test-sources.jar')
        preparation = {'provenance': {path: {'physicalPath': path, 'producer': 'ROOT_VERIFIED_PUBLICATION',
                       'bytes': len(raw), 'sha256': sha(raw)}}, 'outputs': {},
                       'rootSources': {'nereus-storage-api/src/main/java/contract/Input.java': {
                           'bytes': len(source), 'sha256': sha(source)}}}
        C.NATIVE_BUILD.validate_artifact(path, raw, preparation, C)
        preparation['rootSources']['nereus-storage-api/src/main/java/contract/Input.java']['sha256'] = '0' * 64
        with self.assertRaisesRegex(C.EvidenceError, 'differs from the frozen source'):
            C.NATIVE_BUILD.validate_artifact(path, raw, preparation, C)

    def test_native_delta_reuses_only_registered_after_build_objects(self):
        temporary = tempfile.TemporaryDirectory(prefix='m5-shared-native-object-')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        binary = 'kafka/server/nereus/NereusKafkaNativeInputV2.class'
        directory = '/prepared/kafka/core/build/classes/scala/main'
        physical = directory + '/' + binary
        logical = '/original/kafka/core/build/classes/scala/main/' + binary
        before, after = self.class_bytes('before'), self.class_bytes('after')
        def write(path, raw):
            target = root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
            return {'path': path, 'bytes': len(raw), 'sha256': sha(raw)}
        old_ref = write('source-continuation/native-inputs/' + sha(before), before)
        new_ref = write(C.NATIVE_BUILD.PREFIX + '/preparation/raw-objects/' + sha(after), after)
        producer = {'task': ':core:compileScala', 'classDirectory': directory,
                    'sources': [{'path': '/prepared/' + C.NATIVE_INPUTS.MAIN}]}
        origin = {'physicalPath': physical, 'producer': producer['task'], 'bytes': len(after), 'sha256': sha(after)}
        context = {'paths': {new_ref['path']}, 'provenance': {logical: origin}, 'outputs': {physical: (producer, origin)}}
        prior = {'repositories': {'kafka': {'sourceRoot': '/original/kafka'}, 'nereus': {'sourceRoot': '/original/nereus'}},
                 'inputManifests': []}
        rows = [{'path': logical, 'before': old_ref, 'after': new_ref}]
        delta = [{'path': logical, 'before': {key: old_ref[key] for key in ('bytes', 'sha256')},
                  'after': {key: new_ref[key] for key in ('bytes', 'sha256')}}]
        self.assertEqual({old_ref['path'], new_ref['path']},
                         C.NATIVE_INPUTS.validate_objects(root, rows, delta, prior, {}, C, context))
        context['paths'] = set()
        with self.assertRaises(C.EvidenceError):
            C.NATIVE_INPUTS.validate_objects(root, rows, delta, prior, {}, C, context)

    def test_two_build_origins_bind_external_dependency_to_its_actual_inventory(self):
        temporary = tempfile.TemporaryDirectory(prefix='m5-two-build-origin-')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        name, raw = '/external/actual-kafka-compiler.jar', b'contract dependency only'
        path = C.NATIVE_BUILD.PREFIX + '/preparation/raw-objects/' + sha(raw)
        (root / path).parent.mkdir(parents=True)
        (root / path).write_bytes(raw)
        reference = {'path': path, 'bytes': len(raw), 'sha256': sha(raw)}
        node = {key: reference[key] for key in ('bytes', 'sha256')}
        actual = {'physicalPath': name, 'producer': None, **node}
        native = {'paths': {path}, 'provenance': {name: actual}, 'outputs': {}}
        pulsar = {'paths': set(), 'provenance': {}, 'outputs': {}}
        prior = {'repositories': {repo: {'sourceRoot': '/original/' + repo} for repo in C.INPUTS.REPOSITORIES},
                 'extraInputs': {}, 'inputManifests': []}
        rows = [{'path': name, 'before': None, 'after': reference}]
        delta = [{'path': name, 'before': None, 'after': node}]
        self.assertEqual({path}, C.NATIVE_INPUTS.validate_objects(root, rows, delta, prior,
                         {'extraInputs': {name: node}}, C, native, pulsar))
        actual['sha256'] = '0' * 64
        with self.assertRaisesRegex(C.EvidenceError, 'exact artifact'):
            C.NATIVE_INPUTS.validate_objects(root, rows, delta, prior,
                                           {'extraInputs': {name: node}}, C, native, pulsar)


class FreshPreparationContracts(unittest.TestCase):
    """Offline input/CLI contracts. Synthetic producer records prove no actual Gradle preparation."""

    def setUp(self):
        self.fixture = INPUT_TESTS.Fixture()
        self.addCleanup(self.fixture.temporary.cleanup)
        f = self.fixture
        self.arguments = ['--candidate', str(f.output), '--java-home', str(f.base / 'jdk21'),
            '--kafka-java-home', str(f.base / 'jdk17'), '--python-executable', sys.executable,
            '--maven-repository', str(f.base / 'ant'), '--sdk-testing-repository', str(f.base / 'sdk')]
        for name, root in f.roots.items():
            self.arguments += ['--' + name + '-root', str(root)]

    def test_default_fresh_cli_has_no_historical_material_dependency(self):
        args = PREPARATION.parse_args(self.arguments)
        self.assertEqual('fresh', args.mode)
        self.assertIsNone(args.prior_input_closure)
        self.assertIsNone(args.prior_package)
        self.assertIsNone(args.pulsar_from_v11)
        with patch('sys.stderr', new=io.StringIO()), self.assertRaises(SystemExit):
            PREPARATION.parse_args(self.arguments + ['--prior-package', '/deleted/v5'])
        with patch('sys.stderr', new=io.StringIO()), self.assertRaises(SystemExit):
            PREPARATION.parse_args(self.arguments + ['--mode', 'historical-kafka'])

    def test_fresh_requires_explicit_tools_and_dependency_repositories(self):
        cut = self.arguments.index('--python-executable')
        args = self.arguments[:cut] + self.arguments[self.arguments.index('--nereus-root'):]
        with patch('sys.stderr', new=io.StringIO()), self.assertRaises(SystemExit):
            PREPARATION.parse_args(args)
        args = PREPARATION.parse_args(self.arguments)
        args.candidate = self.fixture.base / 'build/candidate'
        with self.assertRaisesRegex(PREPARATION.I.ClosureError, '/build/ ancestor'):
            PREPARATION.fresh_preflight(args)
        self.assertFalse(args.candidate.exists())

    def test_wrong_tool_and_missing_external_input_reject(self):
        f = self.fixture
        with patch.object(PREPARATION.subprocess, 'check_output', return_value='javac 21.0.10'):
            with self.assertRaisesRegex(PREPARATION.I.ClosureError, 'wrong tool version'):
                PREPARATION.actual_tool(Path(sys.executable), '17', True)
        root = f.output / 'workspace' / PREPARATION.I.LAYOUT['nereus']
        graph = [{'producers': []}]
        before = [{'producers': [{'task': ':compileJava', 'inputs': [],
                                 'missingInputs': [str(f.base / 'missing-external.jar')]}]}]
        with self.assertRaisesRegex(PREPARATION.I.ClosureError, 'missing external input'):
            PREPARATION.fresh_records(graph, before, f.output)
        with self.assertRaises(FileNotFoundError):
            PREPARATION.copy_dependencies(f.base / 'deleted-dependencies', root / 'build/dependencies')
        dependency = f.base / 'symlink-dependency'
        dependency.mkdir()
        (dependency / 'alias.jar').symlink_to(f.jar)
        with self.assertRaisesRegex(PREPARATION.I.ClosureError, 'symlink'):
            PREPARATION.copy_dependencies(dependency, root / 'build/dependencies')

    def test_incomplete_dependency_repository_rejects_before_preparation(self):
        args = PREPARATION.parse_args(self.arguments)
        args.maven_repository.mkdir()
        (args.maven_repository / 'only.pom').write_bytes(b'<project/>')
        with patch.object(PREPARATION, 'actual_tool', return_value={}), \
             self.assertRaisesRegex(PREPARATION.I.ClosureError, 'actual Maven POM and JAR'):
            PREPARATION.fresh_preflight(args)
        self.assertFalse(self.fixture.output.exists())

    def compile_state_graph(self):
        owner = self.fixture.output / 'workspace' / PREPARATION.I.LAYOUT['nereus'] / 'module'
        temporary = owner / 'build/tmp/compileJava'
        temporary.mkdir(parents=True)
        state = temporary / 'previous-compilation-data.bin'
        state.write_bytes(b'synthetic mutable compiler state; not execution evidence')
        node = {'path': str(state), 'bytes': state.stat().st_size, 'sha256': sha(state.read_bytes())}
        return {'project': ':module', 'build': 'nereus', 'runtime': [], 'producers': [{
            'task': ':module:compileJava', 'type': 'org.gradle.api.tasks.compile.JavaCompile_Decorated',
            'projectDirectory': str(owner), 'temporaryDirectory': str(temporary),
            'inputs': [], 'outputs': [node]}]}, node

    def test_build_audit_requires_exact_java_compile_project_task_and_temporary_path(self):
        graph, node = self.compile_state_graph()
        self.assertEqual({node['path']}, PREPARATION.java_compile_state_audits([graph], self.fixture.output))
        for field, value in (
                ('type', 'org.gradle.api.tasks.scala.ScalaCompile_Decorated'),
                ('task', ':other:compileJava'),
                ('temporaryDirectory', str(Path(node['path']).parent.parent / 'compileOtherJava'))):
            with self.subTest(field=field):
                changed = copy.deepcopy(graph)
                changed['producers'][0][field] = value
                self.assertEqual(set(), PREPARATION.java_compile_state_audits([changed], self.fixture.output))

    def test_cross_project_and_cross_build_consumers_keep_compiler_state_immutable(self):
        graph, node = self.compile_state_graph()
        for field in ('inputs', 'sources', 'classpath', 'compilerClasspath'):
            with self.subTest(field=field):
                consumer = {'project': ':other', 'build': 'nereus', 'producers': [
                    {'task': ':other:consume', field: [node]}]}
                self.assertEqual(set(), PREPARATION.java_compile_state_audits(
                    [graph, consumer], self.fixture.output))
        consumer = {'project': ':core', 'build': 'kafka', 'producers': [], 'runtime': [node]}
        self.assertEqual(set(), PREPARATION.java_compile_state_audits([graph, consumer], self.fixture.output))
        before = {'project': ':other', 'producers': [
            {'task': ':other:consume', 'missingInputs': [node['path']]}]}
        self.assertEqual(set(), PREPARATION.java_compile_state_audits([before, graph], self.fixture.output))

    def test_current_source_freezes_without_prior_packages_or_flat_inventory(self):
        f = self.fixture
        args = PREPARATION.parse_args(self.arguments)
        for repository in (args.maven_repository, args.sdk_testing_repository):
            repository.mkdir()
            (repository / 'fixture.pom').write_bytes(b'<project>offline contract fixture only</project>')
            (repository / 'fixture.jar').write_bytes(b'offline contract dependency, not an actual JAR')
        def tool(path):
            path = path.resolve(strict=True)
            return {'path': str(path), 'bytes': path.stat().st_size, 'sha256': sha(path.read_bytes()), 'version': 'fixture only'}
        tools = {'python': tool(Path(sys.executable)), 'root': {}, 'kafka': {}}
        args.java_home = f.base / 'tool-root'
        args.kafka_java_home = f.base / 'tool-kafka'
        for label in ('root', 'kafka'):
            for name in ('java', 'javac'):
                path = f.base / ('tool-' + label) / 'bin' / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b'offline fixture tool, never executed')
                tools[label][name] = tool(path)
        events = []
        def publication(command, root, env, log):
            self.assertIn('publishMavenJavaPublicationToDevelopmentRepository', ' '.join(command))
            self.assertNotIn('--offline', command)
            log.write_text('OFFLINE_CONTRACT_FIXTURE_ONLY\n')
            for module in PREPARATION.ROOT_MODULES:
                path = root / module / 'build/libs' / (module + '-0.2.0-SNAPSHOT.jar')
                path.parent.mkdir(parents=True); path.write_bytes(module.encode())
            (root / 'build/m5-evidence-development-repository').mkdir()
            return {'command': command, 'cwd': str(root), 'exitCode': 0}
        def capture(command, root, env, directory, prepared, bootstrap):
            events.append(directory.name)
            self.assertFalse(any('RunSummary' in part for part in command))
            self.assertEqual(directory.name.startswith('stability-'), '--offline' in command)
            directory.mkdir()
            source = root / 'source.txt'
            generated = root / 'build/classes/java/main/Fixture.class'
            generated.parent.mkdir(parents=True, exist_ok=True)
            generated.write_bytes(b'offline contract class fixture only')
            def node(path): return {'path': str(path), 'bytes': path.stat().st_size, 'sha256': sha(path.read_bytes())}
            state = root / 'build/tmp/compileJava/previous-compilation-data.bin'
            consumed_state = root / 'build/tmp/compileConsumedJava/previous-compilation-data.bin'
            for path in (state, consumed_state):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b'offline contract incremental state only')
            def producer(task, outputs):
                return {'task': ':' + task, 'type': 'org.gradle.api.tasks.compile.JavaCompile_Decorated',
                        'projectDirectory': str(root), 'temporaryDirectory': str(root / 'build/tmp' / task),
                        'inputs': [node(source)], 'outputs': [node(path) for path in outputs]}
            graph = [{'project': ':', 'build': 'nereus' if root.name == 'nereus' else 'kafka',
                      'producers': [producer('compileJava', [generated, state]),
                                    producer('compileConsumedJava', [consumed_state])],
                      'runtime': [node(generated), node(consumed_state)]}]
            before = [{'producers': [{'task': ':compileJava', 'inputs': [node(source)], 'missingInputs': []}]}]
            (directory / 'project.json').write_text(json.dumps(graph))
            return {'command': command, 'cwd': str(root), 'exitCode': 0}, graph, before
        with patch.object(PREPARATION, 'fresh_preflight', return_value=tools), \
             patch.object(PREPARATION, 'execute', side_effect=publication), \
             patch.object(PREPARATION, 'capture_build', side_effect=capture), \
             patch.object(PREPARATION.shutil, 'which', return_value=sys.executable):
            inputs = PREPARATION.prepare_fresh(args)
        self.assertEqual(['build-root', 'build-kafka', 'stability-root', 'stability-kafka'], events)
        self.assertFalse(inputs['executionEvidence'])
        self.assertFalse(inputs['promotionEligible'])
        root = f.output / 'workspace' / PREPARATION.I.LAYOUT['nereus']
        self.assertFalse((root / 'build/m5-kafka-run-source').exists())
        self.assertFalse((root / 'build/m5-bookkeeper-task-terminal').exists())
        self.assertIn(str(f.roots['nereus'] / PREPARATION.RECORD), inputs['extraInputs'])
        self.assertIn(str(f.roots['nereus'] / PREPARATION.FRESH_RECORD), inputs['extraInputs'])
        state = root / 'build/tmp/compileJava/previous-compilation-data.bin'
        original = state.read_bytes()
        self.assertNotIn(str(f.roots['nereus'] / state.relative_to(root)), inputs['extraInputs'])
        self.assertEqual(original, (f.output / 'objects' / sha(original)).read_bytes())
        state.write_bytes(b'later compiler mutable state')
        self.assertEqual(inputs, PREPARATION.I.verify(f.output))
        for path in (root / 'build/tmp/compileConsumedJava/previous-compilation-data.bin',
                     root / 'build/classes/java/main/Fixture.class'):
            prior = path.read_bytes()
            path.write_bytes(b'changed actual runtime dependency')
            with self.assertRaisesRegex(PREPARATION.I.ClosureError, 'materialized dependency input differs'):
                PREPARATION.I.verify(f.output)
            path.write_bytes(prior)
        for original in f.roots.values():
            shutil.rmtree(original)
        self.assertEqual(inputs, PREPARATION.I.verify(f.output))


if __name__ == "__main__":
    unittest.main()
