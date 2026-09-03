#!/usr/bin/env python3
"""Validate the focused non-promotable M5-D BookKeeper deletion adapter."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_BOOKKEEPER_DELETE_ADAPTER_NON_PROMOTABLE"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-d-bookkeeper-delete-projection.json"
IMPLEMENTATION_ANCESTOR = "e864d2aa339de04d9e479058f8fd0bd3af49da9e"
BK_SOURCE = "cd06340851d6d657b7c7546df01df365c18980de"
BK_JAR_SHA = "8e64f2b7436bb814705f611eb0ac48d64d90de7a50d295905c459d89bc3f9d8f"
BK_IMAGE = (
    "apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d"
)
BK_IMAGE_ID = "sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605"
REQUIRED_SOURCES = (
    "nereus-storage-bookkeeper/src/main/java/com/nereusstream/storage/bookkeeper/M5BookKeeperDeleteAdapterV1.java",
    "nereus-storage-bookkeeper/src/test/java/com/nereusstream/storage/bookkeeper/M5BookKeeperDeleteAdapterV1Test.java",
    "nereus-storage-bookkeeper/src/realBookKeeperTest/java/com/nereusstream/storage/bookkeeper/RealBookKeeperCellSessionV1RealTest.java",
    "nereus-storage-bookkeeper/build.gradle.kts",
    "config/v2/m2/kafka/k9/bookkeeper-conformance.compose.yml",
    "scripts/run-v2-m5-bookkeeper-delete-check.sh",
)


class BookKeeperDeleteError(RuntimeError):
    """Stable fail-closed M5-D BookKeeper-slice rejection."""


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise BookKeeperDeleteError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise BookKeeperDeleteError(f"{label} lacks exact deletion contract: {missing}")


def validate_projection_value(value: object) -> None:
    if not isinstance(value, dict) or set(value) != {
        "schema",
        "status",
        "implementationAncestor",
        "bookKeeperBoundary",
        "realBookKeeperTest",
        "staleTargetRejectedBeforeDispatch",
        "deleteResponseLossReconciled",
        "repeatAbsenceIdempotent",
        "dispatchAuthorityApiPresent",
        "fullM5DGatePresent",
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    }:
        raise BookKeeperDeleteError("M5-D BookKeeper projection members differ")
    if value.get("schema") != "NEREUS_V2_M5_D_BOOKKEEPER_DELETE_PROJECTION_V1" or value.get(
        "status"
    ) != "BOOKKEEPER_DELETE_ADAPTER_IMPLEMENTED_NON_PROMOTABLE":
        raise BookKeeperDeleteError("M5-D BookKeeper projection schema/status differs")
    if value.get("implementationAncestor") != IMPLEMENTATION_ANCESTOR:
        raise BookKeeperDeleteError("M5-D BookKeeper projection ancestor differs")
    if value.get("bookKeeperBoundary") != {
        "sourceCommit": BK_SOURCE,
        "coordinate": "org.apache.bookkeeper:bookkeeper-server:4.18.0",
        "clientJarSha256": BK_JAR_SHA,
        "serverImageReference": BK_IMAGE,
        "serverImageId": BK_IMAGE_ID,
        "platform": "linux/amd64",
        "sealedMetadataFingerprintRequired": True,
        "deleteResponseNeverSufficient": True,
        "authoritativeMetadataReconciliationRequired": True,
    }:
        raise BookKeeperDeleteError("M5-D exact BookKeeper boundary differs")
    if value.get("realBookKeeperTest") != {
        "task": ":nereus-storage-bookkeeper:realBookKeeperTest",
        "class": "com.nereusstream.storage.bookkeeper.RealBookKeeperCellSessionV1RealTest",
        "focusedMethod": "m5DeleteAdapterDeletesOnlyTheExactSealedLedgerAndReconcilesAbsence",
        "suiteTests": 7,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }:
        raise BookKeeperDeleteError("M5-D real BookKeeper test binding differs")
    for field in (
        "staleTargetRejectedBeforeDispatch",
        "deleteResponseLossReconciled",
        "repeatAbsenceIdempotent",
    ):
        if value.get(field) is not True:
            raise BookKeeperDeleteError(f"M5-D BookKeeper projection lacks {field}")
    for field in (
        "dispatchAuthorityApiPresent",
        "fullM5DGatePresent",
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    ):
        if value.get(field) is not False:
            raise BookKeeperDeleteError(f"M5-D BookKeeper projection overstates {field}")


def validate_source_locks(value: object) -> None:
    if not isinstance(value, dict):
        raise BookKeeperDeleteError("source locks are not an object")
    bookkeeper = value.get("m2KafkaK0InputSourceBinding", {}).get("bookKeeperInput", {})
    expected = {
        "sourceCommit": BK_SOURCE,
        "mavenCoordinate": "org.apache.bookkeeper:bookkeeper-server:4.18.0",
        "jarSha256": BK_JAR_SHA,
        "serverImageReference": BK_IMAGE,
        "serverImageConfigDigest": BK_IMAGE_ID,
        "serverImagePlatform": "linux/amd64",
    }
    if not isinstance(bookkeeper, dict) or any(bookkeeper.get(key) != item for key, item in expected.items()):
        raise BookKeeperDeleteError("source locks lack the exact BookKeeper client/server tuple")


def validate_sources(root: Path) -> None:
    missing = [path for path in REQUIRED_SOURCES if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise BookKeeperDeleteError(f"M5-D BookKeeper sources are missing/empty: {missing}")
    adapter = (root / REQUIRED_SOURCES[0]).read_text(encoding="utf-8")
    unit = (root / REQUIRED_SOURCES[1]).read_text(encoding="utf-8")
    real = (root / REQUIRED_SOURCES[2]).read_text(encoding="utf-8")
    module = (root / REQUIRED_SOURCES[3]).read_text(encoding="utf-8")
    compose = (root / REQUIRED_SOURCES[4]).read_text(encoding="utf-8")
    runner = (root / REQUIRED_SOURCES[5]).read_text(encoding="utf-8")
    require_literals(
        adapter,
        (
            "BookKeeperDeleteTargetV1",
            "captureExactTarget",
            "deleteAndReconcile",
            "metadataSha256",
            "BKException.Code.NoSuchLedgerExistsException",
            "BKException.Code.NoSuchLedgerExistsOnMetadataServerException",
            "DeleteOutcome.EXACT_LEDGER_REMAINS",
            "DeleteOutcome.DIFFERENT_LEDGER_OR_METADATA",
            "thenCompose(ignored -> read(target.handle()))",
        ),
        "BookKeeper delete adapter",
    )
    require_literals(
        unit,
        (
            "lostDeleteResponseCanOnlyResolveThroughAuthoritativeAbsence",
            "exactLedgerRemainingIsRetryableButChangedMetadataIsAConflict",
            "ambiguousReadsStayUnknownBeforeAndAfterDispatch",
        ),
        "BookKeeper delete unit test",
    )
    require_literals(
        real,
        (
            "m5DeleteAdapterDeletesOnlyTheExactSealedLedgerAndReconcilesAbsence",
            "DeleteOutcome.DIFFERENT_LEDGER_OR_METADATA",
            "DeleteOutcome.AUTHORITATIVELY_ABSENT",
            "CaptureOutcome.DEFINITIVELY_ABSENT",
        ),
        "real BookKeeper delete test",
    )
    require_literals(
        module,
        (
            'tasks.register<Test>("realBookKeeperTest")',
            'doFirst {',
            'providers.gradleProperty("v2M2BookKeeperMetadataServiceUri").orNull',
        ),
        "BookKeeper module build",
    )
    if 'providers.gradleProperty("v2M2BookKeeperMetadataServiceUri").get()' in module:
        raise BookKeeperDeleteError("real BookKeeper property is still read eagerly during task realization")
    require_literals(compose, (BK_IMAGE, 'platform: linux/amd64', 'BK_metadataServiceUri:'), "BookKeeper compose")
    require_literals(
        runner,
        (
            BK_IMAGE,
            BK_IMAGE_ID,
            "v2M5BookKeeperDeleteCheck",
            "PASS_V2_M5_BOOKKEEPER_DELETE_REAL",
        ),
        "BookKeeper delete runner",
    )


def validate_tasks(root: Path) -> None:
    build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    for task in (
        "v2M5BookKeeperDeleteContractTest",
        "v2M5BookKeeperDeleteSourceCheck",
        "v2M5BookKeeperDeleteCheck",
    ):
        if re.search(rf'tasks\.register(?:<[^>]+>)?\("{task}"\)', build) is None:
            raise BookKeeperDeleteError(f"root build lacks {task}")
    task = re.search(
        r'tasks\.register\("v2M5BookKeeperDeleteCheck"\) \{(?P<body>.*?)\n\}',
        build,
        re.DOTALL,
    )
    if task is None or not all(
        required in task.group("body")
        for required in (
            '"v2M5VersionMatchDeleteCheck"',
            '":nereus-storage-bookkeeper:realBookKeeperTest"',
            '"v2M5BookKeeperDeleteSourceCheck"',
        )
    ):
        raise BookKeeperDeleteError("M5-D BookKeeper gate omits predecessor, real test, or source check")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    prior = load_module(root / "scripts/check-v2-m5-version-match-delete.py", "nereus_m5_d_object_for_bk")
    prior.validate(root)
    if subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", IMPLEMENTATION_ANCESTOR, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode != 0:
        raise BookKeeperDeleteError("required Object-delete implementation ancestor is missing")
    validate_projection_value(json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8")))
    validate_source_locks(json.loads((root / "docs/v2/source-locks.json").read_text(encoding="utf-8")))
    validate_sources(root)
    validate_tasks(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-D BookKeeper delete: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
