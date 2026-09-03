#!/usr/bin/env python3
"""Validate the focused non-promotable M5-D exact owned multipart cleanup adapter."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_MULTIPART_CLEANUP_NON_PROMOTABLE"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-d-multipart-cleanup-projection.json"
IMPLEMENTATION_ANCESTOR = "40b0006bf41682534ef33a2da01fb4cfd798c34b"
REQUIRED_SOURCES = (
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/provider/ObjectProviderTransport.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/provider/M5MultipartCleanupSessionV1.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/provider/M5MultipartCleanupSessionV1Test.java",
    "nereus-storage-object-s3/src/main/java/com/nereusstream/storage/object/s3/S3C1ObjectProviderTransport.java",
    "nereus-storage-object-s3/src/realProviderTest/java/com/nereusstream/storage/object/s3/M5MinioMultipartCleanupTest.java",
    "nereus-storage-object-s3/build.gradle.kts",
)


class MultipartCleanupError(RuntimeError):
    """Stable fail-closed M5-D multipart-slice rejection."""


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise MultipartCleanupError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise MultipartCleanupError(f"{label} lacks exact multipart contract: {missing}")


def validate_projection_value(value: object) -> None:
    expected_members = {
        "schema",
        "status",
        "implementationAncestor",
        "exactBindings",
        "typedReconciliation",
        "provider",
        "responseUnknownAdvancesWithoutEmptyRelist",
        "foreignSameKeyUploadCanBeAborted",
        "emptyOwnedInventoryAdmitted",
        "focusedTests",
        "externalMutationAdapterPresent",
        "intentMutationApiPresent",
        "dispatchAuthority",
        "fullM5DGatePresent",
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    }
    if not isinstance(value, dict) or set(value) != expected_members:
        raise MultipartCleanupError("M5-D multipart projection members differ")
    if value.get("schema") != "NEREUS_V2_M5_D_MULTIPART_CLEANUP_PROJECTION_V1" or value.get(
        "status"
    ) != "EXACT_OWNED_MULTIPART_CLEANUP_IMPLEMENTED_NON_PROMOTABLE":
        raise MultipartCleanupError("M5-D multipart projection schema/status differs")
    if value.get("implementationAncestor") != IMPLEMENTATION_ANCESTOR:
        raise MultipartCleanupError("M5-D multipart projection ancestor differs")
    if value.get("exactBindings") != [
        "cell_provider_scope_and_exclusive_namespace",
        "provider_identity_and_exact_upload_id_abort_capability",
        "persisted_owned_inventory_root",
        "exact_object_key_and_upload_id",
        "complete_bounded_exact_key_listing",
        "key_marker_and_upload_id_marker_continuation",
        "complete_relist_after_every_abort_result",
    ]:
        raise MultipartCleanupError("multipart exact bindings differ")
    if value.get("typedReconciliation") != [
        "AUTHORITATIVELY_ABSENT",
        "EXACT_OWNED_RESIDUE_REMAINS",
        "DIFFERENT_OR_FOREIGN_IDENTITY",
        "OUTCOME_UNKNOWN",
    ]:
        raise MultipartCleanupError("multipart reconciliation inventory differs")
    provider = value.get("provider")
    if provider != {
        "adapter": "nereus-s3-c1-v1/aws-sdk-s3-2.47.5",
        "productIdentity": "minio/RELEASE.2025-09-07T16-13-09Z",
        "imageReference": "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e",
        "imageConfigDigest": "sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253",
        "platform": "linux/arm64",
        "mechanism": "EXACT_UPLOAD_ID_ABORT_V1",
        "maximumUploadIdBytes": 2048,
        "maximumContinuationTokenBytes": 4096,
        "maximumListPageUploads": 1000,
        "directoryPrefixListingAdmitted": False,
        "exactObjectKeyListingAdmitted": True,
    }:
        raise MultipartCleanupError("multipart exact Provider boundary differs")
    if value.get("externalMutationAdapterPresent") is not True:
        raise MultipartCleanupError("multipart adapter execution fact differs")
    for field in (
        "responseUnknownAdvancesWithoutEmptyRelist",
        "foreignSameKeyUploadCanBeAborted",
        "emptyOwnedInventoryAdmitted",
        "intentMutationApiPresent",
        "dispatchAuthority",
        "fullM5DGatePresent",
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    ):
        if value.get(field) is not False:
            raise MultipartCleanupError(f"M5-D multipart projection overstates {field}")
    if value.get("focusedTests") != {
        "unitClass": "com.nereusstream.storage.object.provider.M5MultipartCleanupSessionV1Test",
        "unitTests": 7,
        "realClass": "com.nereusstream.storage.object.s3.M5MinioMultipartCleanupTest",
        "realTests": 1,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }:
        raise MultipartCleanupError("focused multipart test binding differs")


def validate_sources(root: Path) -> None:
    missing = [path for path in REQUIRED_SOURCES if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise MultipartCleanupError(f"M5-D multipart sources are missing/empty: {missing}")
    transport = (root / REQUIRED_SOURCES[0]).read_text(encoding="utf-8")
    session = (root / REQUIRED_SOURCES[1]).read_text(encoding="utf-8")
    unit = (root / REQUIRED_SOURCES[2]).read_text(encoding="utf-8")
    s3 = (root / REQUIRED_SOURCES[3]).read_text(encoding="utf-8")
    real = (root / REQUIRED_SOURCES[4]).read_text(encoding="utf-8")
    s3_build = (root / REQUIRED_SOURCES[5]).read_text(encoding="utf-8")
    require_literals(
        transport,
        (
            "MultipartCleanupCapabilities.unsupported",
            "EXACT_UPLOAD_ID_ABORT_V1",
            "listMultipartUploads",
            "abortMultipartUploadExact",
            "RESPONSE_UNKNOWN",
        ),
        "shared default-unsupported transport",
    )
    require_literals(
        session,
        (
            "persistedInventoryRoot",
            "inventoryRoot",
            "completeInventoryList",
            "completeListForExactKey",
            "AUTHORITATIVELY_ABSENT",
            "EXACT_OWNED_RESIDUE_REMAINS",
            "DIFFERENT_OR_FOREIGN_IDENTITY",
            "OUTCOME_UNKNOWN",
            "completeInventoryList(owned, counters)",
            "must be non-empty",
        ),
        "Cell-scoped multipart session",
    )
    if re.search(r"public\s+[^\n]+\s+(?:createIntent|publishDone|dispatchAuthorized)\s*\(", session):
        raise MultipartCleanupError("multipart session exposes intent/done/dispatch authority")
    require_literals(
        unit,
        (
            "defaultUnsupportedTransportFailsBeforeMultipartIo",
            "foreignUploadVetoesAllAbortBeforeMutation",
            "lostAbortResponseAdvancesOnlyAfterCompleteEmptyRelist",
            "exactOwnedResidueRemainingIsRetryableAndNeverCalledAbsent",
            "repeatedContinuationTokenFailsClosedBeforeAbort",
        ),
        "multipart unit tests",
    )
    require_literals(
        s3,
        (
            "admitMultipartCleanupV1",
            "ListMultipartUploadsRequest",
            "AbortMultipartUploadRequest",
            "MULTIPART_TOKEN_MAGIC",
            "nextKeyMarker",
            "nextUploadIdMarker",
            "NoSuchUpload",
            "MAXIMUM_MULTIPART_UPLOAD_ID_BYTES = 2_048",
            "MAXIMUM_MULTIPART_CONTINUATION_TOKEN_BYTES = 4_096",
        ),
        "S3 exact multipart adapter",
    )
    require_literals(
        real,
        (
            "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e",
            "sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253",
            "sameKeyFirst",
            "LostAbortResponseTransport",
            "DIFFERENT_OR_FOREIGN_IDENTITY",
            "UploadPartRequest",
        ),
        "fixed-digest MinIO multipart test",
    )
    if 'include("**/M5MinioMultipartCleanupTest.class")' not in s3_build:
        raise MultipartCleanupError("real multipart task is not isolated to its exact test class")


def validate_tasks(root: Path) -> None:
    build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    for task in (
        "v2M5MultipartCleanupContractTest",
        "v2M5MultipartCleanupSourceCheck",
        "v2M5MultipartCleanupCheck",
    ):
        if re.search(rf'tasks\.register(?:<[^>]+>)?\("{task}"\)', build) is None:
            raise MultipartCleanupError(f"root build lacks {task}")
    task = re.search(
        r'tasks\.register\("v2M5MultipartCleanupCheck"\) \{(?P<body>.*?)\n\}', build, re.DOTALL
    )
    if task is None or not all(
        required in task.group("body")
        for required in (
            '"v2M5PulsarCleanupOrderSourceCheck"',
            '"v2M5MultipartCleanupSourceCheck"',
            '":nereus-storage-object:test"',
            '":nereus-storage-object-s3:v2M5MultipartCleanupRealProviderTest"',
        )
    ):
        raise MultipartCleanupError("multipart gate omits predecessor, tests, real Provider, or source check")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    prior = load_module(root / "scripts/check-v2-m5-pulsar-cleanup-order.py", "nereus_m5_d_order_for_multipart")
    prior.validate(root)
    if subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", IMPLEMENTATION_ANCESTOR, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode != 0:
        raise MultipartCleanupError("required Pulsar cleanup-order implementation ancestor is missing")
    validate_projection_value(json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8")))
    validate_sources(root)
    validate_tasks(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-D multipart cleanup: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
