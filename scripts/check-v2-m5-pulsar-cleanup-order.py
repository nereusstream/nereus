#!/usr/bin/env python3
"""Validate the focused non-promotable M5-D Pulsar cleanup ordering core."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_PULSAR_CLEANUP_ORDER_NON_PROMOTABLE"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-d-pulsar-cleanup-order-projection.json"
IMPLEMENTATION_ANCESTOR = "ee5bd41252dede5e0d567162ace67d15d03f95fa"
REQUIRED_SOURCES = (
    "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/M5PulsarObjectCleanupOrderV1.java",
    "nereus-pulsar-offload/src/test/java/com/nereusstream/pulsar/offload/M5PulsarObjectCleanupOrderV1Test.java",
    "settings.gradle.kts",
)


class PulsarCleanupOrderError(RuntimeError):
    """Stable fail-closed M5-D Pulsar cleanup-order rejection."""


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise PulsarCleanupOrderError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise PulsarCleanupOrderError(f"{label} lacks root-before-data contract: {missing}")


def validate_projection_value(value: object) -> None:
    if not isinstance(value, dict) or set(value) != {
        "schema",
        "status",
        "implementationAncestor",
        "exactTargetBindings",
        "cleanupSequence",
        "typedReconciliation",
        "orderViolationAdvances",
        "responseUnknownAdvances",
        "differentIdentityQuarantines",
        "retainBookKeeperAttemptRejected",
        "focusedTests",
        "externalMutationApiPresent",
        "intentMutationApiPresent",
        "fullM5DGatePresent",
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    }:
        raise PulsarCleanupOrderError("M5-D Pulsar cleanup projection members differ")
    if value.get("schema") != "NEREUS_V2_M5_D_PULSAR_CLEANUP_ORDER_PROJECTION_V1" or value.get(
        "status"
    ) != "PULSAR_ROOT_DATA_MULTIPART_ORDER_IMPLEMENTED_NON_PROMOTABLE":
        raise PulsarCleanupOrderError("M5-D Pulsar cleanup projection schema/status differs")
    if value.get("implementationAncestor") != IMPLEMENTATION_ANCESTOR:
        raise PulsarCleanupOrderError("M5-D Pulsar cleanup projection ancestor differs")
    if value.get("exactTargetBindings") != [
        "sealed_ledger_attempt_and_uuid",
        "npo1_root_key_length_body_sha256_and_immutable_version",
        "npd1_data_key_length_body_sha256_and_immutable_version",
        "persisted_intent_binding_root",
        "m4_released_proof_root",
        "reference_free_proof_root",
        "multipart_inventory_root",
        "provider_admission_root",
    ]:
        raise PulsarCleanupOrderError("Pulsar cleanup target bindings differ")
    if value.get("cleanupSequence") != [
        "NPO1_ROOT_AUTHORITATIVELY_ABSENT",
        "NPD1_DATA_AUTHORITATIVELY_ABSENT",
        "OWNED_MULTIPART_RESIDUE_AUTHORITATIVELY_ABSENT",
        "AUTHORITATIVELY_ABSENT",
    ]:
        raise PulsarCleanupOrderError("Pulsar root/data/multipart order differs")
    if value.get("typedReconciliation") != [
        "AUTHORITATIVELY_ABSENT",
        "EXACT_OLD_IDENTITY_REMAINS",
        "DIFFERENT_OR_FOREIGN_IDENTITY",
        "OUTCOME_UNKNOWN",
    ]:
        raise PulsarCleanupOrderError("Pulsar reconciliation inventory differs")
    for field in ("differentIdentityQuarantines", "retainBookKeeperAttemptRejected"):
        if value.get(field) is not True:
            raise PulsarCleanupOrderError(f"Pulsar cleanup projection lacks {field}")
    for field in (
        "orderViolationAdvances",
        "responseUnknownAdvances",
        "externalMutationApiPresent",
        "intentMutationApiPresent",
        "fullM5DGatePresent",
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    ):
        if value.get(field) is not False:
            raise PulsarCleanupOrderError(f"M5-D Pulsar cleanup projection overstates {field}")
    if value.get("focusedTests") != {
        "class": "com.nereusstream.pulsar.offload.M5PulsarObjectCleanupOrderV1Test",
        "tests": 6,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }:
        raise PulsarCleanupOrderError("focused Pulsar cleanup test binding differs")


def validate_sources(root: Path) -> None:
    missing = [path for path in REQUIRED_SOURCES if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise PulsarCleanupOrderError(f"M5-D Pulsar cleanup sources are missing/empty: {missing}")
    implementation = (root / REQUIRED_SOURCES[0]).read_text(encoding="utf-8")
    tests = (root / REQUIRED_SOURCES[1]).read_text(encoding="utf-8")
    settings = (root / REQUIRED_SOURCES[2]).read_text(encoding="utf-8")
    require_literals(
        implementation,
        (
            "NPO1_ROOT",
            "NPD1_DATA",
            "OWNED_MULTIPART_RESIDUE",
            "ROOT_ABSENCE_REQUIRED",
            "DATA_ABSENCE_REQUIRED",
            "MULTIPART_ABSENCE_REQUIRED",
            "AUTHORITATIVELY_ABSENT",
            "EXACT_OLD_IDENTITY_REMAINS",
            "DIFFERENT_OR_FOREIGN_IDENTITY",
            "OUTCOME_UNKNOWN",
            "persistedIntentBindingRoot",
            "m4ReleasedProofRoot",
            "referenceFreeProofRoot",
            "multipartInventoryRoot",
            "providerAdmissionRoot",
            "RetentionClass.DELETE_AFTER_VERIFIED",
        ),
        "Pulsar cleanup ordering core",
    )
    require_literals(
        tests,
        (
            "rootMustBeAuthoritativelyAbsentBeforeDataAndMultipartCanAdvance",
            "responseUnknownAndExactOldIdentityRemainingNeverAdvance",
            "differentOrForeignIdentityQuarantinesPermanently",
            "pureOrderingCoreExposesNoExternalDeleteMethod",
        ),
        "Pulsar cleanup ordering tests",
    )
    require_literals(
        settings,
        ('task == "v2M5PulsarCleanupOrderCheck"', "includeBuild(pulsarCheckout)"),
        "Pulsar composite selection",
    )


def validate_tasks(root: Path) -> None:
    build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    for task in (
        "v2M5PulsarCleanupOrderContractTest",
        "v2M5PulsarCleanupOrderSourceCheck",
        "v2M5PulsarCleanupOrderCheck",
    ):
        if re.search(rf'tasks\.register(?:<[^>]+>)?\("{task}"\)', build) is None:
            raise PulsarCleanupOrderError(f"root build lacks {task}")
    task = re.search(
        r'tasks\.register\("v2M5PulsarCleanupOrderCheck"\) \{(?P<body>.*?)\n\}', build, re.DOTALL
    )
    if task is None or not all(
        required in task.group("body")
        for required in (
            '"v2M5OrphanAdmissionSourceCheck"',
            '"v2M5PulsarCleanupOrderSourceCheck"',
            '":nereus-pulsar-offload:test"',
        )
    ):
        raise PulsarCleanupOrderError("Pulsar cleanup gate omits predecessor, tests, or source check")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    prior = load_module(root / "scripts/check-v2-m5-orphan-admission.py", "nereus_m5_d_orphan_for_pulsar")
    prior.validate(root)
    if subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", IMPLEMENTATION_ANCESTOR, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode != 0:
        raise PulsarCleanupOrderError("required orphan/admission implementation ancestor is missing")
    validate_projection_value(json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8")))
    validate_sources(root)
    validate_tasks(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-D Pulsar cleanup order: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
