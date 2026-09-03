#!/usr/bin/env python3
"""Validate the focused M5-C closed writer registry and mutation guard slice."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import sys


RESULT = "PASS_V2_M5_CLOSED_WRITER_INTEGRATION_NON_PROMOTABLE"
AMENDMENT_MANIFEST_SHA256 = "1f769b44740d9bfb2fa5d5d29fe2207d1cc72cb9e1ea5a8384223a6da143b452"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-c-closed-writer-integration-projection.json"
REQUIRED_SOURCES = (
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5ClosedWriterRegistryV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5ReferenceMutationGuardV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5BindingRetirementCoordinatorV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5PulsarAggregateRetirementCoordinatorV1.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/retention/M5RetentionRetirementV1Test.java",
)


class ClosedWriterIntegrationError(RuntimeError):
    """Fail-closed M5-C writer integration rejection."""


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ClosedWriterIntegrationError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise ClosedWriterIntegrationError(f"{label} lacks closed-writer contract: {missing}")


def validate_required_sources(root: Path, paths: tuple[str, ...] = REQUIRED_SOURCES) -> None:
    missing = [path for path in paths if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise ClosedWriterIntegrationError(f"M5-C closed-writer sources are missing/empty: {missing}")


def source(root: Path, name: str) -> str:
    matches = [path for path in REQUIRED_SOURCES if path.endswith("/" + name)]
    if len(matches) != 1:
        raise ClosedWriterIntegrationError(f"source name is not unique: {name}")
    return (root / matches[0]).read_text(encoding="utf-8")


def validate_runtime_contract(root: Path) -> None:
    registry = source(root, "M5ClosedWriterRegistryV1.java")
    guard = source(root, "M5ReferenceMutationGuardV1.java")
    binding = source(root, "M5BindingRetirementCoordinatorV1.java")
    pulsar = source(root, "M5PulsarAggregateRetirementCoordinatorV1.java")
    tests = source(root, "M5RetentionRetirementV1Test.java")
    require_literals(
        registry,
        (
            "MAX_WRITER_CLASSES = 64",
            "MAX_WRITER_CLASS_BYTES = 1_024",
            "WriterDeclarationV1",
            "RegisteredReferenceWriterV1",
            "implementationSourceSha256",
            "does not exactly cover the closed proof inventory",
            "proof-bound class has more than one writer owner",
            "registryRootSha256",
            "ReferenceWriterEnrollmentV1",
        ),
        "closed writer registry",
    )
    require_literals(
        guard,
        (
            "forBindingBatch",
            "forPulsarAggregate",
            "containsTicket",
            "externalMutation.dispatch",
            "RETAINED_AMBIGUOUS_EXTERNAL_MUTATION",
            "RETAINED_TICKET_STATE_UNKNOWN",
            "authoritativeKeySetRootSha256",
            "registry.requireRegistered",
        ),
        "reference mutation guard",
    )
    if "conditionalTransaction" in guard or "supportsAtomicMultiKeyTransactions" in guard:
        raise ClosedWriterIntegrationError("closed writer guard references the rejected multi-key path")
    require_literals(binding, ("acquireTicket", "clearTicket", "metadata.compareAndSet"), "Binding coordinator")
    require_literals(pulsar, ("acquireTicket", "clearTicket", "metadata.compareAndSet"), "Pulsar coordinator")
    require_literals(
        tests,
        (
            "closedWriterRegistryRejectsEveryMissingFloorOwner",
            "closedWriterRegistryRejectsEveryMissingReferenceOwner",
            "closedWriterRegistryRejectsMalformedOrConflictingDeclarations",
            "guardedBindingMutationDispatchesOnlyWithVisibleRegisteredTicket",
            "ambiguousGuardedMutationRetainsTicketAndVetoesFence",
            "winningFencePreventsGuardedExternalMutationDispatch",
            "pulsarGuardUsesTheSameClosedWriterTicketProtocol",
            "casResponseUnknownAfterApply",
        ),
        "closed writer integration tests",
    )


def validate_projection_value(value: object, floor_classes: tuple[str, ...], reference_kinds: tuple[str, ...]) -> None:
    if not isinstance(value, dict):
        raise ClosedWriterIntegrationError("closed writer projection is not an object")
    if value.get("schema") != "NEREUS_V2_M5_C_CLOSED_WRITER_INTEGRATION_PROJECTION_V1" or value.get(
        "status"
    ) != "CLOSED_WRITER_GUARD_IMPLEMENTED_NON_PROMOTABLE":
        raise ClosedWriterIntegrationError("closed writer projection schema/status differs")
    if value.get("amendmentManifestSha256") != AMENDMENT_MANIFEST_SHA256:
        raise ClosedWriterIntegrationError("closed writer amendment manifest SHA differs")
    registry = value.get("registry", {})
    if registry.get("canonical") is not True or registry.get("maximumWriterClasses") != 64 or registry.get(
        "maximumWriterClassBytes"
    ) != 1_024:
        raise ClosedWriterIntegrationError("closed writer registry caps/canonical flag differ")
    if registry.get("floorClasses") != list(floor_classes) or registry.get("referenceKinds") != list(reference_kinds):
        raise ClosedWriterIntegrationError("closed writer registry inventory differs")
    for field, expected in {
        "duplicateOwnership": "FORBIDDEN",
        "mixedCapability": "FORBIDDEN",
        "missingClass": "FAIL_CLOSED",
        "implementationSourceSha256Required": True,
    }.items():
        if registry.get(field) != expected:
            raise ClosedWriterIntegrationError(f"closed writer registry {field} differs")
    guard = value.get("guardProtocol", {})
    if guard.get("supportedAuthorityFamilies") != ["M5R1_BINDING_BATCH", "M5PA_PULSAR_AGGREGATE"]:
        raise ClosedWriterIntegrationError("guard authority families differ")
    if guard.get("order") != [
        "READ_EXACT_ENROLLED_OPEN_AUTHORITY",
        "CAS_DURABLE_TICKET",
        "REREAD_VISIBLE_EXACT_TICKET",
        "DISPATCH_EXTERNAL_MUTATION",
        "REREAD_AUTHORITATIVE_EXTERNAL_RESULT",
        "CAS_CLEAR_EXACT_TICKET",
    ]:
        raise ClosedWriterIntegrationError("guard mutation order differs")
    for field, expected in {
        "ticketResponseLoss": "SAME_AUTHORITY_KEY_REREAD",
        "ambiguousExternalMutation": "KEEP_TICKET_AND_RETAIN_TARGET",
        "failedTicketClear": "KEEP_OR_TREAT_TICKET_STATE_AS_UNKNOWN_AND_RETAIN_TARGET",
        "fenceWins": "DO_NOT_DISPATCH_EXTERNAL_MUTATION",
        "externalAuthorityRootMismatch": "KEEP_TICKET_AND_RETAIN_TARGET",
        "timeoutOrLocalCompletionClear": "FORBIDDEN",
    }.items():
        if guard.get(field) != expected:
            raise ClosedWriterIntegrationError(f"guard protocol {field} differs")
    if value.get("closedWriterIntegrationGatePresent") is not True:
        raise ClosedWriterIntegrationError("closed writer focused gate is not present")
    if value.get("fullRetirementGatePresent") is not True:
        raise ClosedWriterIntegrationError("closed writer projection lacks the completed full retirement gate")
    for field in (
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    ):
        if value.get(field) is not False:
            raise ClosedWriterIntegrationError(f"closed writer projection overstates {field}")


def validate_tasks(root: Path) -> None:
    root_build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    module_build = (root / "nereus-storage-object/build.gradle.kts").read_text(encoding="utf-8")
    require_literals(
        root_build,
        (
            "v2M5PulsarAggregateAuthorityCheck",
            "v2M5ClosedWriterIntegrationContractTest",
            "v2M5ClosedWriterIntegrationSourceCheck",
            "v2M5ClosedWriterIntegrationCheck",
        ),
        "root build",
    )
    if 'tasks.register<Test>("v2M5ClosedWriterIntegrationTest")' not in module_build:
        raise ClosedWriterIntegrationError("storage-object build lacks v2M5ClosedWriterIntegrationTest")
    if re.search(r'tasks\.register(?:<[^>]+>)?\("v2M5RetentionRetirementCheck"\)', root_build) is None:
        raise ClosedWriterIntegrationError("root build lacks the completed full M5-C retirement gate")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    binding = load_module(
        root / "scripts/check-v2-m5-binding-authority.py", "nereus_m5_binding_for_closed_writers"
    )
    pulsar = load_module(
        root / "scripts/check-v2-m5-pulsar-aggregate-authority.py", "nereus_m5_pulsar_for_closed_writers"
    )
    binding.validate(root)
    pulsar.validate(root)
    validate_required_sources(root)
    validate_runtime_contract(root)
    validate_projection_value(
        json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8")),
        binding.FLOOR_CLASSES,
        binding.REFERENCE_KINDS,
    )
    validate_tasks(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-C closed writer integration: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
