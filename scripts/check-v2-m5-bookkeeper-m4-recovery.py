#!/usr/bin/env python3
"""Supplementary scope checks for M4-protected BK recovery; native lifecycle admission remains separate."""
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
PROJECTION = "docs/v2/detailed_design/m5/m5-bookkeeper-m4-recovery-projection.json"
EXPECTED = json.loads(r'''{
  "schema": "NEREUS_V2_M5_BK_M4_RECOVERY_PROJECTION_V2",
  "status": "M4_KERNEL_RECOVERY_BRIDGE_NON_PROMOTABLE",
  "predecessorSource": "cbc1e6d5264617a5be78e667b3d83c8fe2d4bf14",
  "nativeM4HazardKernelUsed": true,
  "nativeM4SourcePlannerUsed": true,
  "completeDescriptorRouteBound": true,
  "noObsoleteFallbackRoute": true,
  "leaseHeldThroughNativeTermination": true,
  "observerCancellationCannotClearLease": true,
  "unknownClosureStopsNewAdmission": true,
  "oldLeaseSurvivesGenerationChange": true,
  "recoveredCachesCarryCapturedAuthority": true,
  "recoveryControlMetadataIo": false,
  "intendedUse": "LOW_FREQUENCY_SELECTED_DESCRIPTOR_RECOVERY",
  "focusedSuites": {
    "unitTests": 7,
    "realBookKeeperTests": 2,
    "failures": 0,
    "errors": 0,
    "skipped": 0
  },
  "sourceAndControlAuthority": "EXPLICIT_SYNTHETIC_FIXTURE",
  "nativeProtocolOwnerAdmissionIntegrated": false,
  "nativeNamespaceAdmissionIntegrated": false,
  "nativeTaskWriterFencingIntegrated": false,
  "cellCapacityAdmissionIntegrated": false,
  "realOxiaControlEvidencePresent": false,
  "ordinaryProtocolReadIntegrationPresent": false,
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
        raise ValueError("BK M4 recovery projection differs or overclaims native lifecycle authority")

def validate(root):
    validate_projection(json.loads((root / PROJECTION).read_text()))
    source = (root / "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaBookKeeperM4RecoveryV2.java").read_text()
    forbidden = ("CanonicalControlMetadataStore", "putIfAbsent(", "compareAndSet(", "new MaterializationPlan",
                 "new ObjectIdentity", "fenceAndRecoverRunLedger(", "deleteAndReconcile(", "reader.recover(descriptor).toCompletableFuture().cancel")
    if any(value in source for value in forbidden):
        raise ValueError("BK M4 recovery mutates authority or bypasses async source ownership")
    compact = "".join(source.split())
    required = ("new BindingReadAsyncExecutorV1(ownerEventLoop, hazards.capacity())", "executor.execute(current, hazards",
                "BindingReadPlannerV1.plan(", "!plan.route(0).equals(expectedRoute)", "plan.size() != 1",
                "!captured.selectedViewSha256().equals(digest)", "!selected.equals(descriptor)",
                "reader.recover(descriptor).thenApply(view -> new RecoveryResult(captured, view))",
                "selector.admissionState() == AdmissionState.ADMITTING", "new BindingReadPlanBufferV1(1)",
                "source, null, 0, BindingReadRouteV1.SourcePurity.KAFKA_APPEND_UNIT")
    if any("".join(value.split()) not in compact for value in required):
        raise ValueError("BK M4 recovery omits exact capture, route, closed-admission or terminal ownership predicate")
    print("PASS_V2_M5_BK_M4_RECOVERY_NON_PROMOTABLE")

if __name__ == "__main__":
    validate(ROOT)
