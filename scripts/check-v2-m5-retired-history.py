#!/usr/bin/env python3
"""Supplementary retirement-history shape/scope checks; behavior is verified by actual Java integration tests."""
import json
import hashlib
import subprocess
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
PROJECTION = "docs/v2/detailed_design/m5/m5-retired-history-projection.json"
EXPECTED = json.loads(r'''{
  "schema": "NEREUS_V2_M5_RETIRED_HISTORY_PROJECTION_V2",
  "status": "SELECTOR_HISTORY_FOLDING_AND_ADMISSION_NON_PROMOTABLE",
  "predecessorSource": "b17c0a03ad0543171bb50856e10ef87045521aca",
  "authorityWire": "M5R1_VERSION_2",
  "historicalVersionOneBytesPreserved": true,
  "historyNodeWire": "M5H2_VERSION_2",
  "proofDepth": 256,
  "maximumNodeBytes": 4096,
  "maximumNodesPerFold": 257,
  "maximumResidentSlots": 1024,
  "maximumSelectorBytes": 1048576,
  "historyRootCountAndRevisionBoundToSelector": true,
  "monotonicActivationOrdinalsPreserveActiveHoles": true,
  "prewritesHaveNoSelectionAuthority": true,
  "foldUsesOneSelectorCas": true,
  "m4AdmissionChecksExactCurrentHistoryProof": true,
  "ticketAdmissionChecksExactCurrentHistoryProof": true,
  "historicalBatchIdRejectedWithDifferentPayload": true,
  "staleProofRejectedAfterConcurrentRootChange": true,
  "exactTerminalReconciledAfterFurtherFolds": true,
  "unknownPrewriteReservationRetainedForRetry": true,
  "prewriteQuotaHeldUntilNativeTermination": true,
  "boundedLocalPrewriteBytesAndUnresolvedFolds": true,
  "immutableHistoryNodesRetained": true,
  "focusedSuites": {
    "historyTests": 15,
    "failures": 0,
    "errors": 0,
    "skipped": 0,
    "continuousRetirements": 1026,
    "retainedActiveHoles": 1,
    "residentSelectorBytesStrictlyBelow": 2048
  },
  "sourceAndControlAuthority": "EXPLICIT_SYNTHETIC_FIXTURE",
  "nativeNamespaceQuotaAndRestartHeadroomIntegrated": false,
  "nativePerCellIoSchedulingAndMetricsIntegrated": false,
  "realOxiaHistoryEvidencePresent": false,
  "nativeProtocolWriterMatrixComplete": false,
  "physicalDoneCacheLifecycleComplete": false,
  "sourceBoundM5ReceiptPresent": false,
  "historyPhysicalGcAuthority": false,
  "m5FinalAuthority": false,
  "physicalDeleteAuthority": false,
  "productionAuthority": false
}''')

def validate_projection(value):
    if json.dumps(value, sort_keys=True) != json.dumps(EXPECTED, sort_keys=True):
        raise ValueError("retired history projection differs or overclaims native lifecycle closure")

def validate(root):
    validate_projection(json.loads((root / PROJECTION).read_text()))
    fixtures = root / "nereus-storage-object/src/test/resources/retention/m5-history"
    golden = (fixtures / "legacy-m5r1-v1.bin").read_bytes()
    provenance = json.loads((fixtures / "legacy-m5r1-v1.json").read_text())
    expected_sha = "5cd63ab14744698e182697640e1ab296b3412047cb31614976e7bfaef086ab02"
    if (hashlib.sha256(golden).hexdigest() != expected_sha
            or provenance["fixtureSha256"] != expected_sha or provenance["fixtureBytes"] != len(golden)
            or provenance["encoderSourceCommit"] != EXPECTED["predecessorSource"]
            or provenance["m5Receipt"] is not False or provenance["productionAuthority"] is not False):
        raise ValueError("historical M5R1 fixture bytes or provenance differ")
    for name in ("M5BindingAuthorityRecordsV1.java", "M5BindingAuthorityCodecV1.java"):
        path = "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/" + name
        original = subprocess.check_output(["git", "-C", str(root), "show", EXPECTED["predecessorSource"] + ":" + path])
        if hashlib.sha256(original).hexdigest() != provenance["historicalSourceSha256"][name]:
            raise ValueError("M5R1 fixture does not lock its actual predecessor encoder")
    base = root / "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention"
    required = {
        "M5RetiredBatchHistoryV2.java": ("DEPTH = 256", "MAX_NODE_BYTES = 4_096", "MAX_INSERT_NODES = DEPTH + 1",
            "expectedBatchId", "siblings.size() != DEPTH", "!node.root().equals(expected)",
            "!current.equals(expected)", "Math.addExact(left.count(), right.count())", "retired history node is missing"),
        "M5BindingAuthorityRecordsV1.java": ("lastActivationOrdinal", "retiredHistory", "MAX_BATCH_SLOTS = 1_024"),
        "M5BindingAuthorityCodecV1.java": ("VERSION = 2", "wireVersion != 1", "current.lastActivationOrdinal()",
            "historyProof.apply(batch.batchIdSha256())", "selector successor reuses a historical BatchId",
            "only a history fold may remove a slot", "new BatchId admission must verify the exact current history proof"),
        "M5BindingAuthorityControlMetadataStoreV1.java": ("history.requireHead(", "history.readProof(current.retiredHistory(), id, delegate::get)"),
        "M5BindingRetirementCoordinatorV1.java": ("withHistoryAdmission", "reconcileRetired", "Outcome.EXISTING_TERMINAL",
            "proof.tombstone().isPresent()"),
        "M5RetiredBatchHistoryCoordinatorV2.java": ("metadata.compareAndSet(Optional.of(predecessor), predecessor.key(), candidate)",
            "metadata.compareAndSet(Optional.empty(), key, node.bytes())", "requireExactNode", "reconcileTerminal", "tryBegin()",
            "endAttempt()", "budget.reserve(attempt, insertion)"),
        "M5RetiredHistoryWriteBudgetV2.java": ("maximumUnresolvedFolds", "maximumReservedBytes", "beforeNodeDispatch",
            "addAdmittedHeadroom", "unchargedReservedBytes()", "chargedDurableBytes"),
    }
    for name, literals in required.items():
        source = (base / name).read_text()
        compact = "".join(source.split())
        if any("".join(value.split()) not in compact for value in literals):
            raise ValueError("retired history source lacks a required invariant: " + name)
        if name == "M5RetiredBatchHistoryCoordinatorV2.java" and "conditionalTransaction" in source:
            raise ValueError("history folding uses a multi-key transaction")
    print("PASS_V2_M5_RETIRED_HISTORY_NON_PROMOTABLE")

if __name__ == "__main__":
    validate(ROOT)
