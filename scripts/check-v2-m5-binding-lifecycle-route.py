#!/usr/bin/env python3
"""Supplementary Binding route shape/scope checks, separate from real execution evidence."""
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
PROJECTION = "docs/v2/detailed_design/m5/m5-binding-lifecycle-route-projection.json"
EXPECTED = json.loads(r'''{
  "schema": "NEREUS_V2_M5_BINDING_LIFECYCLE_ROUTE_PROJECTION_V1",
  "status": "NATIVE_BINDING_ROUTE_FOCUSED_NON_PROMOTABLE",
  "predecessorSource": "93d3e36db9161233dd7d98b5e84d0c62eb471498",
  "adapter": "OxiaBindingLifecycleMetadataStoreV2",
  "sameNativeM4SelectorKeyAsM3": true,
  "canonicalRelativeKeysAtPublicBoundary": true,
  "oneConfiguredCellRootShardAndBinding": true,
  "nativeKeyBytesMaximum": 512,
  "typedBindingIncarnationEpochAndKeyValidation": true,
  "historyNodeCodecSharedWithProofReader": true,
  "historyWritesAreCanonicalImmutableCreates": true,
  "m4ViewPreservesHistoryEnvelope": true,
  "versionTokenWire": "M5O2_ROUTE_DIGEST_AND_EXACT_NATIVE_VERSION",
  "versionTokenBytes": 44,
  "differentCellVersionReuseRejected": true,
  "legacyDowngradeAndHistoryRollbackRejected": true,
  "nativeVersionAbaRejected": true,
  "realM4HistoryControlBridge": "PRODUCTION_ADAPTER",
  "externalSourceAndReferenceAuthority": "SYNTHETIC_FACT_DISPATCHER_ONLY",
  "sourceLockedOxiaContinuationAndRestart": true,
  "continuousRetirements": 1026,
  "retainedActiveHoles": 1,
  "postFoldSelectorBytes": 982,
  "serverRestartFixtureRetiredCount": 2,
  "focusedSuites": {
    "routeUnitTests": 8,
    "priorM3ControlAdapterUnitTests": 8,
    "historyUnitTests": 15,
    "historyIntegrationTests": 5,
    "serverRestartPhaseTests": 2,
    "priorBindingAndPulsarIntegrationTests": 2,
    "failures": 0,
    "errors": 0,
    "skipped": 0
  },
  "nativeUniqueNamespaceAndBindingAssignmentAdmissionComplete": false,
  "nativeNamespaceQuotaAndRestartHeadroomIntegrated": false,
  "nativePerCellIoSchedulingAndMetricsIntegrated": false,
  "nativeProtocolWriterMatrixComplete": false,
  "physicalDoneCacheLifecycleComplete": false,
  "bookKeeperCompactionMetadataRouteComplete": false,
  "sourceBoundM5ReceiptPresent": false,
  "historyPhysicalGcAuthority": false,
  "m5FinalAuthority": false,
  "physicalDeleteAuthority": false,
  "productionAuthority": false
}''')


def validate_projection(value):
    if json.dumps(value, sort_keys=True) != json.dumps(EXPECTED, sort_keys=True):
        raise ValueError("Binding lifecycle route projection differs or overclaims native admission")


def validate(root):
    validate_projection(json.loads((root / PROJECTION).read_text()))
    required = {
        "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/v2/retention/"
        "OxiaBindingLifecycleMetadataStoreV2.java": (
            "MAX_NATIVE_KEY_BYTES = 512", "VERSION_TOKEN_MAGIC = 0x4d354f32", "routeDigest",
            "M5BindingAuthorityControlMetadataStoreV1", "M4ReadControlKeysV1", "nativeKey(key)",
            "history.verifyNode", "value.versionId()", "scopedVersion", "nativeVersion",
            "history cannot roll back", "create-only", "TransactionOutcome.UNSUPPORTED",
        ),
        "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5RetiredBatchHistoryV2.java": (
            "public NodeWrite verifyNode", "private Node decodeNode", "!node.root().equals(expected)",
        ),
        "nereus-metadata-oxia/src/oxiaIntegrationTest/java/com/nereusstream/metadata/oxia/v2/retention/"
        "M5RetiredHistoryOxiaIntegrationTest.java": (
            "new OxiaCanonicalControlMetadataStore", "facade = route.controlMetadata()", "proofFixtureKey",
            "fixture.route.compareAndSet", "history cannot roll back", "id <= 1_027",
        ),
        "scripts/run-v2-m5-binding-lifecycle-route-check.sh": (
            "v2M5BindingLifecycleRouteCheck", "--no-configuration-cache", "--rerun-tasks",
            "OxiaBindingLifecycleMetadataStoreV2Test", "OxiaCanonicalControlMetadataStoreTest",
            'docker restart "$m5_container"', '"m5FinalAuthority": False',
        ),
    }
    for path, literals in required.items():
        compact = "".join((root / path).read_text().split())
        if any("".join(value.split()) not in compact for value in literals):
            raise ValueError("Binding lifecycle route lacks a required boundary: " + path)
    print("PASS_V2_M5_BINDING_LIFECYCLE_ROUTE_SCOPE_NON_PROMOTABLE")


if __name__ == "__main__":
    validate(ROOT)
