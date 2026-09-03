#!/usr/bin/env python3
"""Validate the non-promotable M5-B Kafka compaction implementation and its frozen dependencies."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_KAFKA_COMPACTION_IMPLEMENTATION_NON_PROMOTABLE"
M5_A_COMMIT = "580257daff9c09f8c5f36976dc342d94d1a79403"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-b-wire-projection.json"
REQUIRED_SOURCES = (
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaCompactionAdmissionV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaCompactionCanonicalV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaCompactionIndexV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaCompactionPublicationFenceV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaCompactionRecordsV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaCompactionSemanticValidatorV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaCompactionSuppressionV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaM5CompactionBridgeV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaRecordBatchCodecV1.java",
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaSemanticCompactorV1.java",
    "nereus-kafka-bookkeeper/src/test/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaSemanticCompactorV1Test.java",
)


class KafkaCompactionError(RuntimeError):
    """Fail-closed M5-B implementation rejection."""


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise KafkaCompactionError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_ancestor(root: Path, commit: str = M5_A_COMMIT) -> None:
    result = subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", commit, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        raise KafkaCompactionError("accepted M5-A implementation is not an ancestor of HEAD")


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise KafkaCompactionError(f"{label} lacks required M5-B contract: {missing}")


def validate_required_sources(root: Path, paths: tuple[str, ...] = REQUIRED_SOURCES) -> None:
    missing = [path for path in paths if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise KafkaCompactionError(f"M5-B required sources are missing/empty: {missing}")


def source(root: Path, name: str) -> str:
    path = next(path for path in REQUIRED_SOURCES if path.endswith("/" + name))
    return (root / path).read_text(encoding="utf-8")


def validate_runtime_contract(root: Path) -> None:
    records = source(root, "KafkaCompactionRecordsV1.java")
    codec = source(root, "KafkaRecordBatchCodecV1.java")
    compactor = source(root, "KafkaSemanticCompactorV1.java")
    indexes = source(root, "KafkaCompactionIndexV1.java")
    validator = source(root, "KafkaCompactionSemanticValidatorV1.java")
    suppression = source(root, "KafkaCompactionSuppressionV1.java")
    admission = source(root, "KafkaCompactionAdmissionV1.java")
    bridge = source(root, "KafkaM5CompactionBridgeV1.java")
    fence = source(root, "KafkaCompactionPublicationFenceV1.java")
    tests = source(root, "KafkaSemanticCompactorV1Test.java")
    require_literals(
        records,
        (
            "DROP_SUPERSEDED_VALUE",
            "DROP_EXPIRED_TOMBSTONE",
            "RETAIN_UNKNOWN",
            "maximumDistinctKeys",
            "completeKeyDomainSha256",
            "candidate crosses an unsafe captured frontier",
            "candidate touches an open transaction",
        ),
        "M5-B records",
    )
    require_literals(
        codec,
        (
            "MemoryRecords",
            "MAGIC_VALUE_V2",
            "appendWithOffset",
            "incrementSequence",
            "ControlRecordType",
            "ensureValid",
        ),
        "M5-B native RecordBatch codec",
    )
    require_literals(
        compactor,
        (
            "KAFKA_SEMANTIC_COMPACTED_V1",
            "REQUIRED_INDEXES",
            "KEEP_TRANSACTION_OR_CONTROL",
            "KEEP_TOMBSTONE_WITHIN_RETENTION",
            "ExtentRow",
            "compactionTaskId",
        ),
        "M5-B compactor",
    )
    require_literals(indexes, ("FLAG_GAP", "listOffset", "lookup", "trailing bytes"), "M5-B indexes")
    require_literals(
        validator,
        (
            "expectedDispositions",
            "requireOutputEquivalence",
            "requireIndexes",
            "semanticValidationRoot",
            "complete eight-index set",
        ),
        "M5-B semantic validator",
    )
    require_literals(
        suppression,
        ("allowFromFallback", "filterFallback", "suppressionRoot"),
        "M5-B fallback suppression",
    )
    require_literals(
        admission,
        (
            "maximumSpillBytes",
            "maximumProviderOperations",
            "maximumKmsOperations",
            "maximumMetadataOperations",
            "maximumResponseUnknowns",
            "compareAndSet",
        ),
        "M5-B Cell admission",
    )
    require_literals(bridge, ("validateSemantic", "semantic proof changed"), "M5-B M5-A bridge")
    require_literals(fence, ("policy/root/frontier fence is stale", "readCurrent"), "M5-B publication fence")
    require_literals(
        tests,
        (
            "compactsAcrossBatchesWithSparseAndNoDataCoverageAtExactTombstoneBoundary",
            "preservesIdempotentProducerSequencesAcrossPartialSparseRewrite",
            "retainsTransactionalDataAndExactControlMarkerWithAbortedAndLeaderEpochIndexes",
            "acceptsKafkaProducedEmptyBatchAndEmitsAuthoritativeNoDataGap",
            "semanticProofFeedsM5PublicationEnvelopeAndBindsSuppressionRoot",
            "persistedCellAdmissionReservesAllCompactionResourcesBeforeDispatch",
            "failsClosedForOpenTransactionsCapsAndMutatedRebuiltIndexes",
            "outputAndAllRootsAreDeterministic",
        ),
        "M5-B tests",
    )


def validate_m5_a_extension(root: Path) -> None:
    nms1 = (root / "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/Nms1ObjectV1.java").read_text(encoding="utf-8")
    codec = (root / "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/Nms1CodecV1.java").read_text(encoding="utf-8")
    validator = (root / "nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationValidatorV1.java").read_text(encoding="utf-8")
    require_literals(nms1, ("KAFKA_SEMANTIC_COMPACTED_V1", "gap extent"), "M5-A NMS1 gap extension")
    require_literals(codec, ("payloadBytes() == 0", "KAFKA_SEMANTIC_COMPACTED_V1"), "M5-A NMS1 codec")
    require_literals(
        validator,
        (
            "SemanticValidationProof",
            "validateSemantic",
            "compactionSuppressionRootSha256",
            "semanticPayloadBodiesRoot",
            "semanticIndexBodiesRoot",
        ),
        "M5-A semantic publication extension",
    )


def validate_projection(root: Path) -> None:
    value = json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8"))
    if value.get("schema") != "NEREUS_V2_M5_B_WIRE_PROJECTION_V1":
        raise KafkaCompactionError("M5-B wire projection schema differs")
    expected_indexes = [
        "OFFSET_OR_POSITION",
        "PAYLOAD_LOCATOR",
        "TIMESTAMP",
        "PRODUCER_RECOVERY",
        "TRANSACTION",
        "ABORTED_TRANSACTION",
        "LEADER_EPOCH",
        "CHECKSUM_COVERAGE",
    ]
    if value.get("rebuiltIndexes") != expected_indexes:
        raise KafkaCompactionError("M5-B complete rebuilt index set differs")
    if value.get("nativeKafkaLibrary") != "org.apache.kafka:kafka-clients:3.9.0":
        raise KafkaCompactionError("M5-B native Kafka library lock differs")
    if value.get("lookupRule") != "FLOOR_COVERAGE_THEN_SUCCESSOR":
        raise KafkaCompactionError("M5-B lookup rule differs")
    if value.get("maximumRecords") != 1_048_576 or value.get("maximumBatchBytes") != 67_108_864:
        raise KafkaCompactionError("M5-B wire caps differ")


def validate_tasks_and_dependency(root: Path) -> None:
    root_build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    module_build = (root / "nereus-kafka-bookkeeper/build.gradle.kts").read_text(encoding="utf-8")
    if "implementation(libs.kafka.clients)" not in module_build:
        raise KafkaCompactionError("M5-B module lacks the locked Kafka client implementation dependency")
    if re.search(r'tasks\.register<Test>\("v2M5KafkaCompactionTest"\)', module_build) is None:
        raise KafkaCompactionError("Kafka module lacks v2M5KafkaCompactionTest")
    if re.search(r'tasks\.register\("v2M5KafkaCompactionCheck"\)', root_build) is None:
        raise KafkaCompactionError("root build lacks v2M5KafkaCompactionCheck")
    require_literals(root_build, ("v2M5MaterializationCheck", "v2M5KafkaCompactionSourceCheck"), "M5-B aggregate")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    m5_a = load(root / "scripts/check-v2-m5-materialization.py", "nereus_m5_a_for_m5_b")
    m5_a.validate(root)
    require_ancestor(root)
    validate_required_sources(root)
    validate_runtime_contract(root)
    validate_m5_a_extension(root)
    validate_projection(root)
    validate_tasks_and_dependency(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-B implementation: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
