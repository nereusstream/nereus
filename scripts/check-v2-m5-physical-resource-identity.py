#!/usr/bin/env python3
"""Bind the M5 stable resource projection; native identity admission remains unproven."""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
PROJECTION = "docs/v2/detailed_design/m5/m5-physical-resource-identity-projection.json"
EXPECTED = {
    "schema": "NEREUS_V2_M5_PHYSICAL_RESOURCE_IDENTITY_PROJECTION_V2",
    "status": "IMPLEMENTED_IDENTITY_ONLY_NON_PROMOTABLE",
    "wire": {
        "magic": "M5RI", "version": 2, "maximumComponentBytes": 8192, "maximumEncodedBytes": 65536,
        "resourceTags": {"OBJECT_VERSION": 1, "BOOKKEEPER_LEDGER": 2, "MULTIPART_UPLOAD": 3},
        "providerTags": {"OBJECT_PROVIDER": 1, "BOOKKEEPER": 2},
        "objectIdentityTags": {"IMMUTABLE_VERSION": 1, "IMMUTABLE_CREATE": 2},
        "hashDomain": "NEREUS_V2_M5_PHYSICAL_RESOURCE_V2",
    },
    "authorityKey": "v2/physical-delete-m5-v2/<PhysicalResourceIdSha256>/authority-v2",
    "authorityWireVersion": 3,
    "mutableContextInKey": False,
    "objectFormatRoleInKey": False,
    "legacyOpaqueAuthorityAdmitted": False,
    "nativeNamespaceAndAuthorityRouteAdmissionImplemented": False,
    "typedEligibilitySnapshotImplemented": True,
    "realDependencyUniquenessEvidencePresent": False,
    "sourceBoundReceiptPresent": False,
    "physicalDeleteAuthority": False,
    "productionAuthority": False,
}


def validate(root: Path) -> None:
    value = json.loads((root / PROJECTION).read_text())
    if json.dumps(value, sort_keys=True) != json.dumps(EXPECTED, sort_keys=True):
        raise ValueError("physical resource projection differs or overclaims runtime authority")
    identity = (root / "nereus-storage-api/src/main/java/com/nereusstream/storage/api/lifecycle/PhysicalResourceIdV2.java").read_text()
    codec = (root / "nereus-storage-api/src/main/java/com/nereusstream/storage/api/lifecycle/PhysicalResourceIdCodecV2.java").read_text()
    records = (root / "nereus-storage-object/src/main/java/com/nereusstream/storage/object/gc/M5TargetDeleteAuthorityRecordsV1.java").read_text()
    authority_codec = (root / "nereus-storage-object/src/main/java/com/nereusstream/storage/object/gc/M5TargetDeleteAuthorityCodecV1.java").read_text()
    for text, literals in (
        (identity, ("sealed interface PhysicalResourceIdV2", "MAX_COMPONENT_BYTES = 8192", "MAX_ENCODED_BYTES = 65536",
                    "record ObjectVersion", "record BookKeeperLedger", "record MultipartUpload", "Arrays.compareUnsigned",
                    '"v2/physical-delete-m5-v2/"', '"/authority-v2"')),
        (codec, ("0x4d355249", "VERSION = 2", EXPECTED["wire"]["hashDomain"], "CanonicalUtf8.fromBytes",
                 "input.available() != 0", "length > input.available()")),
        (records, ("PhysicalDeleteTargetV1(PhysicalResourceIdV2 resourceId", "resourceId.sha256()")),
        (authority_codec, ("VERSION = 3", "PhysicalResourceIdCodecV2.decode(targetBytes)", "resourceId().canonicalBytes()")),
    ):
        if any(literal not in text for literal in literals):
            raise ValueError("stable resource implementation no longer matches its wire projection")
    target = records[records.index("public record PhysicalDeleteTargetV1"):records.index("/** Exact closed inventory")]
    for forbidden in ("CellProviderScopeId", "exactTargetIdentity", "ownerFence", "proofSnapshot", "BindingId"):
        if forbidden in target:
            raise ValueError(f"mutable or opaque target identity returned: {forbidden}")
    print("PASS_V2_M5_PHYSICAL_RESOURCE_IDENTITY_NON_PROMOTABLE")


if __name__ == "__main__":
    validate(ROOT)
