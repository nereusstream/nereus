#!/usr/bin/env python3
"""Verify shared Kafka semantics while keeping sealed-BK publication and real evidence open."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECTION = "docs/v2/detailed_design/m5/m5-kafka-semantic-core-projection.json"
EXPECTED = {
  "schema": "NEREUS_V2_M5_KAFKA_SEMANTIC_CORE_PROJECTION_V2",
  "status": "SHARED_SEMANTICS_IMPLEMENTED_NON_PROMOTABLE",
  "semanticOutputIdentityDomain": "NEREUS_V2_M5_KAFKA_SEMANTIC_OUTPUT_V2",
  "compiledOutput": [
    "EXACT_RETAINED_RECORD_BATCHES",
    "DETERMINISTIC_DISPOSITIONS",
    "COMPLETE_GAPS",
    "COMPLETE_EIGHT_INDEX_SET",
    "SEMANTIC_VALIDATION_PROOF"
  ],
  "indexKinds": [
    "OFFSET_OR_POSITION",
    "PAYLOAD_LOCATOR",
    "TIMESTAMP",
    "PRODUCER_RECOVERY",
    "TRANSACTION",
    "ABORTED_TRANSACTION",
    "LEADER_EPOCH",
    "CHECKSUM_COVERAGE"
  ],
  "coreCreatesObjectCandidates": False,
  "coreUsesObjectSession": False,
  "coreAllocatesBookKeeperLedger": False,
  "sharedRecordSelectionAndRewrite": True,
  "sharedIndependentRecordAndIndexValidation": True,
  "independentBatchBytesReparse": True,
  "allIndexKindsRequireCompleteExpectedRows": True,
  "allLocatorAndProtocolFieldsValidated": True,
  "existingObjectBridgeRetained": True,
  "focusedTests": {
    "kafkaCompactionTests": 14,
    "newSemanticCoreTests": 6,
    "failures": 0,
    "errors": 0,
    "skipped": 0
  },
  "sealedBookKeeperCarrierImplemented": False,
  "realBookKeeperCompactionEvidencePresent": False,
  "internalTopicObjectDisabledLifecycleEvidencePresent": False,
  "sourceBoundReceiptPresent": False,
  "physicalDeleteAuthority": False,
  "productionAuthority": False,
  "abortedIndexMatchesProducerAndTransactionalBatch": True,
  "planIdentityDomain": "NEREUS_V2_M5_B_COMPACTION_PLAN_V2",
  "legacyPlanOutputsReused": False
}

def validate_projection(value):
    if json.dumps(value, sort_keys=True) != json.dumps(EXPECTED, sort_keys=True):
        raise ValueError("shared Kafka semantic projection differs or overclaims BK lifecycle")

def validate(root):
    validate_projection(json.loads((root / PROJECTION).read_text()))
    base = root / "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction"
    compiler = (base / "KafkaSemanticCompactorV1.java").read_text()
    validator = (base / "KafkaCompactionSemanticValidatorV1.java").read_text()
    for start, end in (
        ("public KafkaCompactionSemanticOutputV2 compileSemantic", "static Sha256Digest semanticOutputIdentity"),
        ("private static CompiledRecords compileRecords", "private static MaterializationPlan materializationPlan"),
        ("private static List<KafkaCompactionIndexV1> indexes", "private static CandidateGeneration generation"),
    ):
        body = compiler[compiler.index(start):compiler.index(end)]
        if any(token in body for token in (
            "new MaterializationPlan", "materializationPlan(plan)", "generation(", "new Nms1ObjectV1",
            "new GenerationObject", "new Candidate", "new ObjectIdentity", "M5MaterializationObjectSessionV1")):
            raise ValueError("semantic core constructs an Object materialization")
    for source, literals in (
        (compiler, ["compileRecords(plan)", "requireIndexBudget(plan, indexes)", "validateSemantic(plan, output)",
                    "NEREUS_V2_M5_KAFKA_SEMANTIC_OUTPUT_V2", "transaction.producerId() == batch.producerId()",
                    "boolean aborted = batch.transactional()"]),
        (validator, ["requireCompleteIndexRows", "expectedDispositions", "expectedGaps",
                     "KafkaRecordBatchCodecV1.parse(output.outputBody().orElseThrow())",
                     "row.outputBatchOrdinal() != output.outputBatchOrdinal()",
                     "row.byteOffset() != output.payloadOffset()", "case ABORTED_TRANSACTION",
                     "M5-B index omits or invents required semantic rows",
                     "transaction.producerId() == batch.producerId()", "return batch.transactional()"]),
    ):
        compact = "".join(source.split())
        if any("".join(literal.split()) not in compact for literal in literals):
            raise ValueError("shared semantic core omits a required predicate")
    canonical = (base / "KafkaCompactionCanonicalV1.java").read_text()
    if EXPECTED["planIdentityDomain"] not in canonical or "NEREUS_V2_M5_B_COMPACTION_PLAN_V1" in canonical:
        raise ValueError("semantic algorithm revision reuses a historical immutable output identity")
    print("PASS_V2_M5_KAFKA_SEMANTIC_CORE_NON_PROMOTABLE")

if __name__ == "__main__":
    validate(ROOT)
