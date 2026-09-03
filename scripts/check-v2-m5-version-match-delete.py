#!/usr/bin/env python3
"""Validate the focused non-promotable M5-D version-match Provider slice."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_VERSION_MATCH_DELETE_PROVIDER_NON_PROMOTABLE"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-d-version-match-delete-projection.json"
IMPLEMENTATION_ANCESTOR = "237e902c26a852df757c14952645a6cacace3c8f"
IMAGE_REFERENCE = (
    "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
)
IMAGE_ID = "sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253"
REQUIRED_SOURCES = (
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/provider/ObjectProviderTransport.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/provider/M5ObjectDeleteSessionV1.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/provider/M5ObjectDeleteSessionV1Test.java",
    "nereus-storage-object-s3/src/main/java/com/nereusstream/storage/object/s3/S3C1ObjectProviderTransport.java",
    "nereus-storage-object-s3/src/realProviderTest/java/com/nereusstream/storage/object/s3/M5MinioVersionMatchDeleteTest.java",
)


class VersionMatchDeleteError(RuntimeError):
    """Stable fail-closed M5-D Provider-slice rejection."""


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise VersionMatchDeleteError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise VersionMatchDeleteError(f"{label} lacks version-match delete contract: {missing}")


def validate_projection_value(value: object) -> None:
    if not isinstance(value, dict) or set(value) != {
        "schema",
        "status",
        "implementationAncestor",
        "providerBoundary",
        "realProviderTest",
        "defaultTransportUnsupported",
        "sameKeyRecreationProtected",
        "fullM5DGatePresent",
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    }:
        raise VersionMatchDeleteError("M5-D Provider projection members differ")
    if value.get("schema") != "NEREUS_V2_M5_D_VERSION_MATCH_DELETE_PROJECTION_V1" or value.get(
        "status"
    ) != "VERSION_MATCH_DELETE_IMPLEMENTED_NON_PROMOTABLE":
        raise VersionMatchDeleteError("M5-D Provider projection schema/status differs")
    if value.get("implementationAncestor") != IMPLEMENTATION_ANCESTOR:
        raise VersionMatchDeleteError("M5-D Provider projection ancestor differs")
    if value.get("providerBoundary") != {
        "mechanism": "VERSION_MATCH_DELETE_V1",
        "product": "MinIO RELEASE.2025-09-07T16-13-09Z",
        "imageReference": IMAGE_REFERENCE,
        "imageId": IMAGE_ID,
        "platform": "linux/arm64",
        "bucketVersioningRequired": True,
        "maximumVersionTokenBytes": 1024,
        "responseLossTyped": True,
        "completeListAndFullGetReconciliation": True,
    }:
        raise VersionMatchDeleteError("M5-D real Provider boundary differs")
    if value.get("realProviderTest") != {
        "task": ":nereus-storage-object-s3:v2M5VersionMatchDeleteRealProviderTest",
        "class": "com.nereusstream.storage.object.s3.M5MinioVersionMatchDeleteTest",
        "tests": 1,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }:
        raise VersionMatchDeleteError("M5-D real Provider test binding differs")
    for field in ("defaultTransportUnsupported", "sameKeyRecreationProtected"):
        if value.get(field) is not True:
            raise VersionMatchDeleteError(f"M5-D Provider projection lacks {field}")
    for field in (
        "fullM5DGatePresent",
        "sourceBoundReceiptPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    ):
        if value.get(field) is not False:
            raise VersionMatchDeleteError(f"M5-D Provider projection overstates {field}")


def validate_source_locks(value: object) -> None:
    if not isinstance(value, dict):
        raise VersionMatchDeleteError("source locks are not an object")
    bindings = value.get("m3EvidenceBindings", {}).get("bindings", [])
    exact = f"MINIO_S3_COMPATIBLE|artifactReference={IMAGE_REFERENCE}|artifactConfigDigest={IMAGE_ID}"
    if not isinstance(bindings, list) or not any(
        isinstance(row, dict)
        and row.get("backend") == "MINIO_S3_COMPATIBLE"
        and row.get("sourceIdentity") == exact
        and row.get("sourceProvenance") == exact
        for row in bindings
    ):
        raise VersionMatchDeleteError("source locks lack the exact MinIO image/config boundary")


def validate_sources(root: Path) -> None:
    missing = [path for path in REQUIRED_SOURCES if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise VersionMatchDeleteError(f"M5-D Provider sources are missing/empty: {missing}")
    transport = (root / REQUIRED_SOURCES[0]).read_text(encoding="utf-8")
    session = (root / REQUIRED_SOURCES[1]).read_text(encoding="utf-8")
    s3 = (root / REQUIRED_SOURCES[3]).read_text(encoding="utf-8")
    real = (root / REQUIRED_SOURCES[4]).read_text(encoding="utf-8")
    require_literals(
        transport,
        ("VERSION_MATCH_DELETE_V1", "return ConditionalDeleteResult.UNSUPPORTED", "deleteExactVersion"),
        "shared Provider transport",
    )
    require_literals(
        session,
        (
            "readExactForDelete",
            "deleteExactVersion",
            "AUTHORITATIVELY_ABSENT",
            "EXACT_OLD_VERSION_REMAINS",
            "DIFFERENT_VERSION_OR_BODY",
            "completeList",
        ),
        "M5-D delete session",
    )
    require_literals(
        s3,
        (
            "admitVersionMatchDeleteV1",
            "BucketVersioningStatus.ENABLED",
            "DeleteObjectRequest.builder()",
            ".versionId(versionId)",
            "classifyConditionalDeleteFailure",
        ),
        "S3 version-match adapter",
    )
    require_literals(real, (IMAGE_REFERENCE, IMAGE_ID, "recreatedVersion", "LostDeleteResponseTransport"), "real MinIO test")


def validate_tasks(root: Path) -> None:
    module = (root / "nereus-storage-object-s3/build.gradle.kts").read_text(encoding="utf-8")
    build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    require_literals(
        module,
        ('tasks.register<Test>("v2M5VersionMatchDeleteRealProviderTest")', "M5MinioVersionMatchDeleteTest.class"),
        "S3 build",
    )
    for task in (
        "v2M5VersionMatchDeleteContractTest",
        "v2M5VersionMatchDeleteSourceCheck",
        "v2M5VersionMatchDeleteCheck",
    ):
        if re.search(rf'tasks\.register(?:<[^>]+>)?\("{task}"\)', build) is None:
            raise VersionMatchDeleteError(f"root build lacks {task}")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    prior = load_module(root / "scripts/check-v2-m5-retention-retirement.py", "nereus_m5_c_for_delete_provider")
    prior.validate(root)
    if subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", IMPLEMENTATION_ANCESTOR, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode != 0:
        raise VersionMatchDeleteError("required M5-C implementation ancestor is missing")
    validate_projection_value(json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8")))
    validate_source_locks(json.loads((root / "docs/v2/source-locks.json").read_text(encoding="utf-8")))
    validate_sources(root)
    validate_tasks(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-D version-match delete: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
