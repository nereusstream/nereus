#!/usr/bin/env python3
"""Validate descriptor-only BK recovery and exact M4 publication while retaining all native lifecycle boundaries."""
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
PROJECTION = "docs/v2/detailed_design/m5/m5-bookkeeper-descriptor-projection.json"
EXPECTED = json.loads(r'''{
  "schema": "NEREUS_V2_M5_BK_DESCRIPTOR_PROJECTION_V2",
  "status": "SEALED_DESCRIPTOR_PUBLICATION_AND_RECOVERY_NON_PROMOTABLE",
  "predecessorSource": "b0b885e0160006a46ac891f986a108f8cdc592e7",
  "descriptorKind": "SEALED_BK_COMPACTED_RUN_V2",
  "descriptorWire": "KBSD2",
  "maximumDescriptorBytes": 1048576,
  "physicalIndexLocators": 8,
  "locatorEncoding": "FIRST_PART_FIRST_ENTRY_CONTIGUOUS_CHUNKS",
  "exactTaskCutProfilePredecessorBound": true,
  "fullNativeSealedFingerprintsBound": true,
  "completeSemanticAndGapRootsBound": true,
  "descriptorOnlyRecovery": true,
  "readonlyNativeRecovery": true,
  "completeEntriesAndArtifactsVerified": true,
  "exactIndexLocatorsReconstructed": true,
  "producerTransactionAbortedIndexesRecovered": true,
  "obsoleteGapFallbackSuppressed": true,
  "exactM4SelectorCasDelegated": true,
  "m5SelectorEnvelopePreserved": true,
  "immutableTaskCandidateBound": true,
  "unknownSelectorResponseReconciled": true,
  "concurrentOwnerCasPreserved": true,
  "requiresExplicitRecoveryByteBound": true,
  "focusedSuites": {
    "descriptorUnitTests": 10,
    "realDescriptorTests": 4,
    "failures": 0,
    "errors": 0,
    "skipped": 0
  },
  "sourceAndControlAuthority": "EXPLICIT_SYNTHETIC_FIXTURE",
  "nativeKafkaEvidenceScope": "CLIENT_ENCODED_RECORD_BATCHES_AND_REBUILT_INDEXES",
  "usesObjectProvider": false,
  "needsExpectedOutputBodiesForRecovery": false,
  "readsOldSourceBodiesDuringRecovery": false,
  "nativeNamespaceAdmissionIntegrated": false,
  "nativeTaskWriterFencingIntegrated": false,
  "nativeM4ReadSourcePlanAdmissionIntegrated": false,
  "cellCapacityAdmissionIntegrated": false,
  "realOxiaControlEvidencePresent": false,
  "internalTopicLifecycleEvidencePresent": false,
  "unpublishedArtifactCleanupIntegrated": false,
  "oldInputReplacementDeleteIntegrated": false,
  "sourceBoundM5ReceiptPresent": false,
  "m5FinalAuthority": false,
  "physicalDeleteAuthority": false,
  "productionAuthority": false
}''')

def validate_projection(value):
    if json.dumps(value, sort_keys=True) != json.dumps(EXPECTED, sort_keys=True):
        raise ValueError("BK descriptor projection differs or overclaims complete native lifecycle")

def validate(root):
    validate_projection(json.loads((root / PROJECTION).read_text()))
    base = root / "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction"
    names = ("KafkaSealedBookKeeperDescriptorV2.java", "KafkaSealedBookKeeperDescriptorCodecV2.java",
             "KafkaBookKeeperArtifactAssemblerV2.java", "KafkaBookKeeperReadViewV2.java",
             "KafkaSealedBookKeeperReaderV2.java", "KafkaBookKeeperCompactionPublicationV2.java")
    sources = {name: (base / name).read_text() for name in names}
    for source in sources.values():
        if any(value in source for value in ("new MaterializationPlan", "new ObjectIdentity", "new GenerationObject",
                "M5MaterializationObjectSessionV1", "new Nms1ObjectV1", "compareAndSet(")):
            raise ValueError("BK descriptor path reaches Object construction or bypasses M4 selection")
    reader = sources["KafkaSealedBookKeeperReaderV2.java"]
    if any(value in reader for value in ("fenceAndRecoverRunLedger(", "createRunLedger(", "createReservedRunLedger(",
            "appendExplicitEntry(", "reserveLedgerIdentity(", "deleteAndReconcile(")):
        raise ValueError("selected descriptor recovery performs a native mutation")
    required = {
        "KafkaSealedBookKeeperDescriptorV2.java": ["SEALED_BK_COMPACTED_RUN_V2", "MAX_ENCODED_BYTES = 1_048_576",
            "indexes.size() != 8", "descriptorSha256()", "requireBodies(semantic, artifacts)",
            "seal.sealedLastEntryId() != plan.entryCount() - 1L"],
        "KafkaSealedBookKeeperDescriptorCodecV2.java": ["input.readUnsignedShort() != 2",
            "!encode(descriptor).equals(bytes)", "input.available() != 0", "input.readInt() != 8"],
        "KafkaBookKeeperArtifactAssemblerV2.java": ["chunk.chunkOrdinal() != nextChunk", "chunk.chunkCount() != first.chunkCount()",
            "!Sha256Digest.hash(value).equals(first.artifactSha256())", "indexes.size() != 8"],
        "KafkaBookKeeperReadViewV2.java": ["KafkaRecordBatchCodecV1::parse", "requireProof()", "requireIndexes(records)",
            "allowsPredecessorOffset", "case ABORTED_TRANSACTION", "!actual.equals(expected)",
            "row.byteOffset() != located.byteOffset()", "artifacts.indexLocators().equals(descriptor.indexes())"],
        "KafkaSealedBookKeeperReaderV2.java": ["required > maximumEncodedRecoveryBytes", "requireMetadata(expected)",
            "!result.exactTarget().orElseThrow().equals(expected)", "!body.plan().equals(plan)",
            "observedBytes[0] > plan.length()"],
        "KafkaBookKeeperCompactionPublicationV2.java": ["validateSemantic(plan, semantic)", "reader.recover(descriptor)",
            "requireFallbackProtections(plan.sourceCut(), sources)", "requireCurrent(plan, currentCompactionState)",
            "selecting.introduceFallback(", "selecting.updateMembershipNeutralView(", "createExact(candidateKey(",
            "recoverSelected(BindingReadSelector admittedSelector)", "selected BK descriptor digest differs from its selector"],
    }
    for name, literals in required.items():
        compact = "".join(sources[name].split())
        if any("".join(value.split()) not in compact for value in literals):
            raise ValueError("BK descriptor omits a required identity, recovery or publication predicate")
    print("PASS_V2_M5_BK_DESCRIPTOR_NON_PROMOTABLE")

if __name__ == "__main__":
    validate(ROOT)
