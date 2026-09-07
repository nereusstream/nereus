#!/usr/bin/env python3
"""Supplementary real-Oxia history scope checks; the runner/JUnit files supply execution evidence separately."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECTION = "docs/v2/detailed_design/m5/m5-retired-history-oxia-projection.json"
EXPECTED = json.loads(r'''{
  "schema": "NEREUS_V2_M5_RETIRED_HISTORY_OXIA_PROJECTION_V1",
  "status": "REAL_OXIA_HISTORY_FOCUSED_NON_PROMOTABLE",
  "predecessorSource": "47fa3f1daa6b456c05d1e1eff69deb33803912f5",
  "nativeMetadataAdapter": "Oxia09ExactMetadataTransactionStoreV1",
  "clientSource": "091a42c2780d92da56e9ec1f02ce1c3d988adc16",
  "clientJarSha256": "0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5",
  "loadedClientJarSha256Verified": true,
  "serverSource": "37a17bef17202d5fd6e23282da5fd26d94865484",
  "serverImageId": "sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da",
  "serverPlatform": "linux/arm64",
  "nativeSingleKeyCasAndImmutablePrewrites": true,
  "nativeRetirementCoordinatorBeforeEveryFold": true,
  "continuousRetirements": 1026,
  "retainedActiveHoles": 1,
  "residentSelectorBytesStrictlyBelow": 2048,
  "selectorMeasurementPoint": "AFTER_EACH_FOLD",
  "serverRestartFixtureRetiredCount": 2,
  "clientReconnectRejectsHistoricalBatchAndTicket": true,
  "sameNativeServerRestartPreservesSelectorAndHistory": true,
  "lostPrewriteResponseRetainsBudgetForExactRetry": true,
  "lostSelectorResponseReconcilesAfterFurtherFold": true,
  "nativeRootChangeRejectsStaleAdmission": true,
  "missingOrCorruptNativeHistoryFailsClosed": true,
  "sourceAndReferenceAuthority": "SYNTHETIC_PROOF_FACTS_WITH_REAL_NATIVE_METADATA",
  "m4ControlBridge": "TEST_ONLY_SYNCHRONOUS_EXACT_ADAPTER",
  "responseFaults": "POST_NATIVE_MUTATION_CLIENT_DELIVERY_INJECTION",
  "focusedSuites": {
    "historyIntegrationTests": 5,
    "serverRestartPhaseTests": 2,
    "priorBindingAndPulsarIntegrationTests": 2,
    "failures": 0,
    "errors": 0,
    "skipped": 0
  },
  "nativeNamespaceQuotaAndRestartHeadroomIntegrated": false,
  "nativePerCellIoSchedulingAndMetricsIntegrated": false,
  "nativeProtocolWriterMatrixComplete": false,
  "physicalDoneCacheLifecycleComplete": false,
  "crossModulePhysicalDeleteCompositionComplete": false,
  "sourceBoundM5ReceiptPresent": false,
  "historyPhysicalGcAuthority": false,
  "m5FinalAuthority": false,
  "physicalDeleteAuthority": false,
  "productionAuthority": false
}''')


def validate_projection(value):
    if json.dumps(value, sort_keys=True) != json.dumps(EXPECTED, sort_keys=True):
        raise ValueError("real Oxia history projection differs or overclaims native lifecycle closure")


def validate(root):
    validate_projection(json.loads((root / PROJECTION).read_text()))
    required = {
        "nereus-metadata-oxia/src/oxiaIntegrationTest/java/com/nereusstream/metadata/oxia/v2/retention/"
        "M5RetiredHistoryOxiaIntegrationTest.java": (
            "id <= 1_027", "fixture.retire(id)", "fixture.fold(id)", "isLessThan(2_048)",
            "getProtectionDomain()", EXPECTED["clientJarSha256"], "fixture.reconnect()",
            "nativeClient.createIfAbsent(key, value)", "nativeClient.compareAndSet(key, value, version)",
            "failedReads.set(2)", "heldResponse.completeExceptionally", "fixture.faults.beforeSelectorCas",
            "fixture.client.delete(nativeKey)", "OxiaBindingLifecycleMetadataStoreV2",
        ),
        "nereus-metadata-oxia/src/oxiaIntegrationTest/java/com/nereusstream/metadata/oxia/v2/retention/"
        "M5RetiredHistoryOxiaRestartTest.java": (
            "writeBeforeServerRestart", "readAfterServerRestart", "Files.write(checkpoint()",
            "Files.readAllLines(checkpoint())", "fixture.requireRejected(2)", "fixture.requireRejected(3)",
        ),
        "scripts/run-v2-m5-retired-history-oxia-check.sh": (
            EXPECTED["serverSource"], EXPECTED["serverImageId"], EXPECTED["clientJarSha256"],
            "--no-configuration-cache", "--rerun-tasks", 'docker restart "$m5_container"',
            'test "$m5_started_before" != "$m5_started_after"', "v2M5RetiredHistoryOxiaRestartReadTest",
            '"m5Receipt": False', '"productionAuthority": False', "xmlSha256", "run-summary.json",
        ),
        "build.gradle.kts": ("v2M5RetiredHistoryOxiaCheck", "v2M5RetiredHistoryOxiaSourceCheck",
                               "v2M5RetiredHistoryOxiaContractTest", "v2M5LifecycleDesignCheck"),
    }
    for name, literals in required.items():
        compact = "".join((root / name).read_text().split())
        if any("".join(value.split()) not in compact for value in literals):
            raise ValueError("real Oxia history verification lacks its expected boundary: " + name)
    print("PASS_V2_M5_RETIRED_HISTORY_OXIA_SCOPE_NON_PROMOTABLE")


if __name__ == "__main__":
    validate(ROOT)
