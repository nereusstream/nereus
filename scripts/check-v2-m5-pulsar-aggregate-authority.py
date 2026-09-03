#!/usr/bin/env python3
"""Validate the focused M5-C Pulsar aggregate single-key authority slice."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_PULSAR_AGGREGATE_AUTHORITY_IMPLEMENTATION_NON_PROMOTABLE"
AMENDMENT_COMMIT = "3d641ab6c1a709155c40774e3304f7ffd86115bd"
AMENDMENT_MANIFEST_SHA256 = "1f769b44740d9bfb2fa5d5d29fe2207d1cc72cb9e1ea5a8384223a6da143b452"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-c-pulsar-aggregate-authority-projection.json"
REQUIRED_SOURCES = (
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5PulsarAggregateAuthorityRecordsV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5PulsarAggregateAuthorityCodecV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5PulsarAggregateRetirementCoordinatorV1.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/retention/M5RetentionRetirementV1Test.java",
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/v2/codec/AggregateAuthorityCodec.java",
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/v2/codec/Nta1AggregateAuthorityCodec.java",
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/v2/capability/OxiaTopicBindingAggregatePublisher.java",
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/v2/capability/OxiaTopicBindingAggregateReader.java",
    "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/v2/codec/Nta1AggregateAuthorityCodecTest.java",
    "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/v2/capability/OxiaCapabilityAdaptersTest.java",
)


class PulsarAggregateAuthorityError(RuntimeError):
    """Fail-closed M5-C Pulsar aggregate authority rejection."""


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise PulsarAggregateAuthorityError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise PulsarAggregateAuthorityError(f"{label} lacks Pulsar aggregate authority contract: {missing}")


def require_ancestor(root: Path) -> None:
    result = subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", AMENDMENT_COMMIT, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        raise PulsarAggregateAuthorityError("accepted M5-C amendment is not an ancestor of HEAD")


def validate_required_sources(root: Path, paths: tuple[str, ...] = REQUIRED_SOURCES) -> None:
    missing = [path for path in paths if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise PulsarAggregateAuthorityError(f"M5-C Pulsar aggregate sources are missing/empty: {missing}")


def source(root: Path, name: str) -> str:
    matches = [path for path in REQUIRED_SOURCES if path.endswith("/" + name)]
    if len(matches) != 1:
        raise PulsarAggregateAuthorityError(f"source name is not unique: {name}")
    return (root / matches[0]).read_text(encoding="utf-8")


def validate_runtime_contract(root: Path) -> None:
    records = source(root, "M5PulsarAggregateAuthorityRecordsV1.java")
    codec = source(root, "M5PulsarAggregateAuthorityCodecV1.java")
    coordinator = source(root, "M5PulsarAggregateRetirementCoordinatorV1.java")
    codec_port = source(root, "AggregateAuthorityCodec.java")
    nta1 = source(root, "Nta1AggregateAuthorityCodec.java")
    publisher = source(root, "OxiaTopicBindingAggregatePublisher.java")
    reader = source(root, "OxiaTopicBindingAggregateReader.java")
    retention_tests = source(root, "M5RetentionRetirementV1Test.java")
    codec_tests = source(root, "Nta1AggregateAuthorityCodecTest.java")
    adapter_tests = source(root, "OxiaCapabilityAdaptersTest.java")
    require_literals(
        records,
        (
            "PulsarAggregateRetirementAuthorityV1",
            "REFERENCE_SCAN_FENCED_V1",
            "PulsarAggregateScanFenceV1",
            "ReferenceMutationTicketV1",
            "ReferenceWriterEnrollmentV1",
            "MAX_AUTHORITY_BYTES = 1_048_576",
            "MAX_REFERENCE_MUTATION_TICKETS = 256",
            "canonical NTA1 aggregate",
        ),
        "Pulsar authority records",
    )
    require_literals(
        codec,
        (
            "0x4d355041",
            "migrateLegacy",
            "projectAggregate",
            "Nta1CodecV1.decode",
            "Pulsar aggregate authority canonical SHA-256 differs",
        ),
        "Pulsar authority codec",
    )
    require_literals(
        coordinator,
        (
            "enrollWriters",
            "acquireTicket",
            "clearTicket",
            "validateDeletedSelector",
            "REFERENCE_SCAN_FENCED_V1",
            "PhysicalCleanupSummaryV1",
            "M5RetentionCodecV1.finalizeRetiredPulsar",
            "metadata.compareAndSet",
        ),
        "Pulsar single-key coordinator",
    )
    if "conditionalTransaction" in coordinator or "supportsAtomicMultiKeyTransactions" in coordinator:
        raise PulsarAggregateAuthorityError("Pulsar coordinator references the rejected multi-key path")
    require_literals(codec_port, ("projectStoredBytes", "return storedBytes"), "aggregate codec port")
    require_literals(
        nta1,
        ("M5PulsarAggregateAuthorityCodecV1.projectAggregate", "projectStoredBytes"),
        "production NTA1 codec",
    )
    require_literals(publisher, ("codec.projectStoredBytes",), "aggregate publisher")
    require_literals(reader, ("codec.projectStoredBytes",), "aggregate reader")
    require_literals(
        retention_tests,
        (
            "pulsarAggregateMigrationPreservesExactNta1Projection",
            "pulsarTicketAndFenceRaceHasOnlyOneWinningAuthorityCas",
            "pulsarFenceRejectsSameNameGenerationAba",
            "fencedPulsarAggregateRetiresInOneSingleKeyCasAfterCleanup",
            "transactionCalls).isZero",
        ),
        "Pulsar authority retention tests",
    )
    require_literals(codec_tests, ("decodesM5PulsarAuthorityThroughItsExactNta1Projection",), "NTA1 tests")
    require_literals(adapter_tests, ("productionAggregateAdaptersProjectOneM5AuthorityEnvelope",), "adapter tests")


def validate_projection_value(value: object, floor_classes: tuple[str, ...], reference_kinds: tuple[str, ...]) -> None:
    if not isinstance(value, dict):
        raise PulsarAggregateAuthorityError("Pulsar aggregate projection is not an object")
    if value.get("schema") != "NEREUS_V2_M5_C_PULSAR_AGGREGATE_AUTHORITY_PROJECTION_V1" or value.get(
        "status"
    ) != "PULSAR_AGGREGATE_AUTHORITY_IMPLEMENTED_NON_PROMOTABLE":
        raise PulsarAggregateAuthorityError("Pulsar aggregate projection schema/status differs")
    if value.get("amendmentManifestSha256") != AMENDMENT_MANIFEST_SHA256:
        raise PulsarAggregateAuthorityError("Pulsar aggregate amendment manifest SHA differs")
    expected = {
        "authorityKey": "EXISTING_PULSAR_INCARNATION_AGGREGATE_KEY",
        "authorityWireMagic": "M5PA",
        "legacyAggregateWireMagic": "NTA1",
        "legacyMigration": "ONE_EXACT_AGGREGATE_KEY_CAS",
        "aggregateReaderProjection": "EXACT_ORIGINAL_CANONICAL_NTA1_BYTES",
        "selectorRequirement": "EXACT_PERMANENT_NPS1_DELETED_SAME_PERSISTENCE_NAME_BINDING_GENERATION_BINDING_ID_AND_NTA1_SHA256",
        "referenceMutationProtocol": "DURABLE_TICKET_THEN_EXTERNAL_MUTATION_THEN_EXACT_REREAD_THEN_TICKET_CLEAR",
        "scanFence": "ZERO_TICKETS_THEN_REFERENCE_SCAN_FENCED_V1_BOUND_TO_EXACT_DELETED_SELECTOR",
        "physicalCleanupRequirement": "EVERY_REQUIRED_SOURCE_DELETE_DONE_OR_AUTHORITATIVELY_ABSENT_WITH_ZERO_PENDING",
        "retirementTransition": "FENCED_M5PA_TO_PERMANENT_M5PR_ONE_EXACT_AGGREGATE_KEY_CAS",
    }
    for field, expected_value in expected.items():
        if value.get(field) != expected_value:
            raise PulsarAggregateAuthorityError(f"Pulsar aggregate projection {field} differs")
    if value.get("hardCaps") != {
        "authorityBytes": 1_048_576,
        "referenceMutationTickets": 256,
        "concurrentScanFences": 1,
    }:
        raise PulsarAggregateAuthorityError("Pulsar aggregate hard caps differ")
    enrollment = value.get("writerEnrollment", {})
    if enrollment.get("required") is not True or enrollment.get("floorClasses") != list(
        floor_classes
    ) or enrollment.get("referenceKinds") != list(reference_kinds):
        raise PulsarAggregateAuthorityError("Pulsar aggregate writer enrollment differs")
    backend = value.get("selectedMetadataBackend", {})
    if backend != {
        "adapter": "Oxia09ExactMetadataTransactionStoreV1",
        "clientArtifact": "io.github.oxia-db:oxia-client:0.9.4",
        "operation": "EXACT_SINGLE_KEY_COMPARE_AND_SET",
        "supportsSelectedOperation": True,
        "atomicMultiKeyTransactionRequired": False,
        "internalWriteBatchAuthority": False,
        "sequentialCasFallback": "FORBIDDEN",
    }:
        raise PulsarAggregateAuthorityError("Pulsar aggregate backend capability projection differs")
    if value.get("pulsarAuthorityGatePresent") is not True:
        raise PulsarAggregateAuthorityError("Pulsar aggregate focused gate is not present")
    for field in (
        "fullRetirementGatePresent",
        "m5DImplementationPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    ):
        if value.get(field) is not False:
            raise PulsarAggregateAuthorityError(f"Pulsar aggregate projection overstates {field}")


def validate_tasks(root: Path) -> None:
    root_build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    oxia_build = (root / "nereus-metadata-oxia/build.gradle.kts").read_text(encoding="utf-8")
    require_literals(
        root_build,
        (
            "v2M5BindingAuthorityCheck",
            "v2M5PulsarAggregateAuthorityContractTest",
            "v2M5PulsarAggregateAuthoritySourceCheck",
            "v2M5PulsarAggregateAuthorityCheck",
        ),
        "root build",
    )
    if 'tasks.register<Test>("v2M5PulsarAggregateAuthorityTest")' not in oxia_build:
        raise PulsarAggregateAuthorityError("metadata-oxia build lacks v2M5PulsarAggregateAuthorityTest")
    if re.search(r'tasks\.register(?:<[^>]+>)?\("v2M5RetentionRetirementCheck"\)', root_build):
        raise PulsarAggregateAuthorityError("full M5-C retirement gate exists before writer integration closure")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    amendment = load_module(
        root / "scripts/check-v2-m5-design-amendment.py", "nereus_m5_amendment_for_pulsar_authority"
    )
    binding = load_module(
        root / "scripts/check-v2-m5-binding-authority.py", "nereus_m5_binding_for_pulsar_authority"
    )
    amendment.validate(root)
    binding.validate(root)
    require_ancestor(root)
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
        print(f"M5-C Pulsar aggregate authority: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
