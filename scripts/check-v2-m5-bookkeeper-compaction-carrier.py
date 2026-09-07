#!/usr/bin/env python3
"""Check the bounded BK carrier slice without promoting it to selected-read or delete authority."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECTION = "docs/v2/detailed_design/m5/m5-bookkeeper-compaction-carrier-projection.json"
EXPECTED = json.loads(r'''{
  "schema": "NEREUS_V2_M5_BK_COMPACTION_CARRIER_PROJECTION_V2",
  "status": "INVENTORIED_WRITES_AND_SEALED_PART_VERIFICATION_NON_PROMOTABLE",
  "predecessorSource": "806d8c0298fbf3ff8497b0c62fd43938fe9ebcf5",
  "inventoryWire": "KBIV2",
  "entryWire": "KBCE2",
  "profile": "BOOKKEEPER_WAL_ONLY",
  "bounds": {
    "maximumParts": 256,
    "maximumTaskBytes": 1048576,
    "maximumPartBytes": 67108864,
    "maximumPartEntries": 65536,
    "entryEnvelopeBytes": 91,
    "maximumArtifactBytes": 67108864
  },
  "nativeReservationBeforeInventory": true,
  "exactInventoryBeforeCreate": true,
  "concurrentLoserBurnsIdOnly": true,
  "createRetryUsesSameLedgerId": true,
  "retainedBatchBytesAndEightIndexesOnly": true,
  "emptyOutputHasNoDataLedger": true,
  "nativeFenceBeforeFullPartValidation": true,
  "exactRunReadBeforeNativeFencing": true,
  "partialSealedPartNeverRewritten": true,
  "allEntriesAndNativeMetadataVerified": true,
  "bookKeeperSource": "cd06340851d6d657b7c7546df01df365c18980de",
  "bookKeeperClientJarSha256": "8e64f2b7436bb814705f611eb0ac48d64d90de7a50d295905c459d89bc3f9d8f",
  "bookKeeperImage": "apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d",
  "bookKeeperImageId": "sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605",
  "focusedSuites": {
    "inventoryTests": 8,
    "realCarrierTests": 7,
    "realCellSessionTests": 8
  },
  "sourceAndControlAuthority": "EXPLICIT_SYNTHETIC_FIXTURE",
  "responseLossInjection": "APPLICATION_OBSERVER_AFTER_REAL_PROVIDER_COMPLETION",
  "usesObjectProvider": false,
  "sealedReadDescriptorImplemented": false,
  "selectorPublicationIntegrated": false,
  "recoveryWithoutExpectedOutputBodies": false,
  "nativeNamespaceAdmissionIntegrated": false,
  "nativeTaskWriterFencingIntegrated": false,
  "cellCompactionAdmissionIntegrated": false,
  "unpublishedArtifactCleanupIntegrated": false,
  "oldInputReplacementDeleteIntegrated": false,
  "realOxiaEvidencePresent": false,
  "internalTopicLifecycleEvidencePresent": false,
  "sourceBoundReceiptPresent": false,
  "m5FinalAuthority": false,
  "physicalDeleteAuthority": false,
  "productionAuthority": false
}''')

def validate_projection(value):
    if json.dumps(value, sort_keys=True) != json.dumps(EXPECTED, sort_keys=True):
        raise ValueError("BK carrier projection differs or overclaims complete lifecycle")

def validate(root):
    validate_projection(json.loads((root / PROJECTION).read_text()))
    base = root / "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction"
    sources = {name: (base / name).read_text() for name in (
        "KafkaBookKeeperInventoryV2.java", "KafkaBookKeeperInventoryCodecV2.java",
        "KafkaBookKeeperCompactionLayoutV2.java", "KafkaBookKeeperCompactionWriterV2.java")}
    for source in sources.values():
        if any(token in source for token in (
            "new MaterializationPlan", "new ObjectIdentity", "new GenerationObject",
            "M5MaterializationObjectSessionV1", "deleteAndReconcile(", "compareAndSet(",
            "new Nms1ObjectV1", "new CandidateGeneration")):
            raise ValueError("BK carrier invokes Object, selection or deletion authority")
    required = {
        "KafkaBookKeeperInventoryV2.java": [
            "session.reserveLedgerIdentity()", "metadata.putIfAbsent(partKey(",
            "readPart(task, part.ordinal()).equals(Optional.of(part))", "session.createReservedRunLedger(",
            "EXISTING_EXACT_REQUIRES_RECOVERY", "different immutable BK part occupies its inventory slot"],
        "KafkaBookKeeperCompactionLayoutV2.java": [
            "validateSemantic(plan, semantic)", "semantic.batchOutputs()", "semantic.indexBodies()",
            "entriesRoot(entries)", "MAX_PARTS", "maximumEntryBytes > capability.maximumAddPayloadBytes()"],
        "KafkaBookKeeperCompactionWriterV2.java": [
            "inventory.register(layout.task())", "inventory.reservePart(", "inventory.createPart(",
            "session.openRunLedger(part.handle())", "inventory.requireRegisteredTask(layout.task(), session)",
            "session.fenceAndRecoverRunLedger(", "metadataReader.capture(", "!before.equals(after)",
            "session.readExactEntry(", "!exact.payload().equals(body.entries().get(entryId))",
            "payload.release()", "List::copyOf"],
        "KafkaBookKeeperInventoryCodecV2.java": [
            "BOOKKEEPER_WAL_ONLY", "requireCanonical(bytes, encodeTask(task))",
            "requireCanonical(bytes, encodePart(part))", "input.available() != 0", "input.readFully(run)"],
    }
    for name, literals in required.items():
        compact = "".join(sources[name].split())
        if any("".join(value.split()) not in compact for value in literals):
            raise ValueError("BK carrier omits an inventory or exact verification predicate")
    native = (root / "nereus-storage-bookkeeper/src/main/java/com/nereusstream/storage/bookkeeper/RealBookKeeperCellSessionV1.java").read_text()
    for value in ("newLedgerIdGenerator()", "generator.generateLedgerId(", "create.withLedgerId(", "generator.close()"):
        if value not in native:
            raise ValueError("BK carrier lacks the native fixed-ID creation primitive")
    print("PASS_V2_M5_BK_COMPACTION_CARRIER_NON_PROMOTABLE")

if __name__ == "__main__":
    validate(ROOT)
