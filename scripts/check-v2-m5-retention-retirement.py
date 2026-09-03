#!/usr/bin/env python3
"""Validate the complete non-promotable M5-C retirement implementation gate."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


RESULT = "PASS_V2_M5_RETENTION_RETIREMENT_IMPLEMENTATION_NON_PROMOTABLE"
PROJECTION_PATH = "docs/v2/detailed_design/m5/m5-c-retention-retirement-implementation-projection.json"
AMENDMENT_MANIFEST_SHA256 = "1f769b44740d9bfb2fa5d5d29fe2207d1cc72cb9e1ea5a8384223a6da143b452"
IMPLEMENTATION_ANCESTORS = (
    "580257daff9c09f8c5f36976dc342d94d1a79403",
    "027e3bbdb6e60e2b3ec72108ed4b546ba6a15cc8",
    "90e220c46b1303d6f490db9c09be31efb11c6c5d",
    "89bc667bf01692d883a10b1e5078196707e834f2",
    "70e5efa1643d8ff8fa096a8327512f751d421e3e",
)
REAL_OXIA = {
    "operation": "EXACT_SINGLE_KEY_COMPARE_AND_SET",
    "clientArtifact": "io.github.oxia-db:oxia-client:0.9.4",
    "clientSourceCommit": "091a42c2780d92da56e9ec1f02ce1c3d988adc16",
    "clientJarSha256": "0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5",
    "serverSourceCommit": "37a17bef17202d5fd6e23282da5fd26d94865484",
    "serverImageReference": "nereus/oxia-m3-allocator:37a17bef1720",
    "serverImageId": "sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da",
    "serverImagePlatform": "linux/arm64",
    "serverImageRecipeSha256": "31388e201ce95fd61c1505a8628a66993ec8c070ba02ad5f71aa647ae066d238",
    "tests": [
        "bindingAuthorityMigratesTicketsFencesRetiresAndSurvivesRestart",
        "pulsarAuthorityMigratesTicketsFencesRetiresAndSurvivesRestart",
    ],
    "clientReconnectRequired": True,
    "stalePredecessorRejected": True,
    "atomicMultiKeyTransactionUsed": False,
}
REQUIRED_SOURCES = (
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/v2/retention/Oxia09ExactMetadataTransactionStoreV1.java",
    "nereus-metadata-oxia/src/oxiaIntegrationTest/java/com/nereusstream/metadata/oxia/v2/retention/M5RetentionOxiaIntegrationTest.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5BindingRetirementCoordinatorV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5PulsarAggregateRetirementCoordinatorV1.java",
    "nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5ReferenceMutationGuardV1.java",
    "nereus-storage-object/src/test/java/com/nereusstream/storage/object/retention/M5RetentionRetirementV1Test.java",
    "scripts/run-v2-m5-retention-retirement-check.sh",
)
DOCUMENT_CONTRACTS = (
    (
        "docs/v2/detailed_design/m5/README.md",
        (
            "m5-c-retention-retirement-implementation-projection.json",
            "v2M5RetentionRetirementCheck",
            "no source-bound child receipt",
        ),
    ),
    (
        "docs/v2/detailed_design/m5/m5-implementation-log.md",
        (
            "PASS_V2_M5_RETENTION_RETIREMENT_IMPLEMENTATION_NON_PROMOTABLE",
            "M5RetentionOxiaIntegrationTest: 2 tests",
            "60 actionable tasks: 60 executed",
        ),
    ),
    (
        "docs/v2/08-implementation-plan-and-gates.md",
        ("M5-C implementation complete, source-bound child not run",),
    ),
)


class RetentionRetirementError(RuntimeError):
    """Stable fail-closed full M5-C implementation rejection."""


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RetentionRetirementError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise RetentionRetirementError(f"{label} lacks full M5-C contract: {missing}")


def validate_required_sources(root: Path, paths: tuple[str, ...] = REQUIRED_SOURCES) -> None:
    missing = [path for path in paths if not (root / path).is_file() or not (root / path).read_bytes()]
    if missing:
        raise RetentionRetirementError(f"full M5-C sources are missing/empty: {missing}")


def validate_ancestors(root: Path, ancestors: tuple[str, ...] = IMPLEMENTATION_ANCESTORS) -> None:
    for commit in ancestors:
        result = subprocess.run(
            ["git", "-C", str(root), "merge-base", "--is-ancestor", commit, "HEAD"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode != 0:
            raise RetentionRetirementError(f"required M5 implementation ancestor is missing: {commit}")


def validate_source_lock_value(value: object) -> None:
    if not isinstance(value, dict):
        raise RetentionRetirementError("source-lock root is not an object")
    client = value.get("dependencyEvidenceBindings", {}).get("oxiaClientArtifacts", {})
    artifacts = client.get("artifacts", {}) if isinstance(client, dict) else {}
    client_jar = artifacts.get("clientJar", {}) if isinstance(artifacts, dict) else {}
    if client.get("forkOutputId") != "oxia-client-notification-continuity" or client_jar != {
        "relativePath": "m2/io/github/oxia-db/oxia-client/0.9.4/oxia-client-0.9.4.jar",
        "bytes": 385309,
        "sha256": REAL_OXIA["clientJarSha256"],
    }:
        raise RetentionRetirementError("source-locked Oxia client artifact differs")
    binding = value.get("m3AllocatorEvidenceBinding", {})
    for field, expected in {
        "oxiaClientSourceCommit": REAL_OXIA["clientSourceCommit"],
        "oxiaClientJarSha256": REAL_OXIA["clientJarSha256"],
        "oxiaServerSourceCommit": REAL_OXIA["serverSourceCommit"],
        "oxiaServerImageReference": REAL_OXIA["serverImageReference"],
        "oxiaServerImageDigest": REAL_OXIA["serverImageId"],
        "oxiaServerImagePlatform": REAL_OXIA["serverImagePlatform"],
        "oxiaImageRecipeSha256": REAL_OXIA["serverImageRecipeSha256"],
    }.items():
        if not isinstance(binding, dict) or binding.get(field) != expected:
            raise RetentionRetirementError(f"source-locked real Oxia {field} differs")


def validate_projection_value(value: object) -> None:
    if not isinstance(value, dict) or set(value) != {
        "schema",
        "status",
        "amendmentManifestSha256",
        "implementationAncestors",
        "authorityFamilies",
        "realOxiaBoundary",
        "fullRetirementGatePresent",
        "sourceBoundReceiptPresent",
        "m5DImplementationPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    }:
        raise RetentionRetirementError("full M5-C projection members differ")
    if value.get("schema") != "NEREUS_V2_M5_C_RETENTION_RETIREMENT_IMPLEMENTATION_PROJECTION_V1" or value.get(
        "status"
    ) != "RETENTION_METADATA_RETIREMENT_IMPLEMENTED_NON_PROMOTABLE":
        raise RetentionRetirementError("full M5-C projection schema/status differs")
    if value.get("amendmentManifestSha256") != AMENDMENT_MANIFEST_SHA256:
        raise RetentionRetirementError("full M5-C amendment binding differs")
    if value.get("implementationAncestors") != list(IMPLEMENTATION_ANCESTORS):
        raise RetentionRetirementError("full M5-C implementation ancestry differs")
    if value.get("authorityFamilies") != ["M5R1_BINDING_BATCH", "M5PA_PULSAR_AGGREGATE"]:
        raise RetentionRetirementError("full M5-C authority families differ")
    if value.get("realOxiaBoundary") != REAL_OXIA:
        raise RetentionRetirementError("full M5-C real Oxia boundary differs")
    if value.get("fullRetirementGatePresent") is not True:
        raise RetentionRetirementError("full M5-C implementation gate is not present")
    for field in (
        "sourceBoundReceiptPresent",
        "m5DImplementationPresent",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    ):
        if value.get(field) is not False:
            raise RetentionRetirementError(f"full M5-C implementation projection overstates {field}")


def validate_runtime_contract(root: Path) -> None:
    integration = (root / REQUIRED_SOURCES[1]).read_text(encoding="utf-8")
    binding = (root / REQUIRED_SOURCES[2]).read_text(encoding="utf-8")
    pulsar = (root / REQUIRED_SOURCES[3]).read_text(encoding="utf-8")
    guard = (root / REQUIRED_SOURCES[4]).read_text(encoding="utf-8")
    runner = (root / REQUIRED_SOURCES[6]).read_text(encoding="utf-8")
    require_literals(integration, tuple(REAL_OXIA["tests"]), "real Oxia integration test")
    require_literals(
        integration,
        (
            "Oxia09ExactMetadataTransactionStoreV1",
            "M5BindingRetirementCoordinatorV1",
            "M5PulsarAggregateRetirementCoordinatorV1",
            "MUTATION_APPLIED_AND_TICKET_CLEARED",
            "DEFINITIVE_CONFLICT",
            "supportsAtomicMultiKeyTransactions()).isFalse()",
        ),
        "real Oxia integration test",
    )
    require_literals(binding, ("metadata.compareAndSet", "REFERENCE_SCAN_FENCED_V1"), "Binding coordinator")
    require_literals(pulsar, ("metadata.compareAndSet", "REFERENCE_SCAN_FENCED_V1"), "Pulsar coordinator")
    require_literals(
        guard,
        ("authority.acquire", "authority.containsTicket", "clearVisibleTicket"),
        "reference mutation guard",
    )
    if "conditionalTransaction" in binding or "conditionalTransaction" in pulsar or "conditionalTransaction" in guard:
        raise RetentionRetirementError("accepted M5-C authority path references the rejected multi-key transaction")
    require_literals(
        runner,
        (
            str(REAL_OXIA["serverImageReference"]),
            str(REAL_OXIA["serverImageId"]),
            str(REAL_OXIA["clientJarSha256"]),
            "v2M5RetentionRetirementCheck",
            "v2M5RetentionOxiaServiceAddress",
        ),
        "real Oxia managed runner",
    )


def validate_document_contracts(root: Path) -> None:
    for path, literals in DOCUMENT_CONTRACTS:
        require_literals((root / path).read_text(encoding="utf-8"), literals, path)


def validate_tasks(root: Path) -> None:
    root_build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    oxia_build = (root / "nereus-metadata-oxia/build.gradle.kts").read_text(encoding="utf-8")
    require_literals(
        root_build,
        (
            "v2M5ClosedWriterIntegrationCheck",
            "v2M5RetentionRetirementContractTest",
            "v2M5RetentionRetirementSourceCheck",
            "v2M5RetentionRetirementCheck",
            ":nereus-metadata-oxia:v2M5RetentionRealOxiaTest",
        ),
        "root build",
    )
    require_literals(
        oxia_build,
        (
            'tasks.register<Test>("v2M5RetentionRealOxiaTest")',
            "v2M5RetentionOxiaServiceAddress",
            "doFirst",
        ),
        "metadata-oxia build",
    )
    task_slice = oxia_build[
        oxia_build.index("val v2M5RetentionOxiaServiceAddress") : oxia_build.index(
            'tasks.register<Test>("r1OxiaIntegrationTest")'
        )
    ]
    if re.search(r'\.get\(\)|\.getOrElse\(', task_slice) is not None:
        raise RetentionRetirementError("M5 real Oxia task eagerly reads its optional Gradle property")


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    closed = load_module(
        root / "scripts/check-v2-m5-closed-writer-integration.py", "nereus_m5_closed_for_full_retirement"
    )
    closed.validate(root)
    validate_required_sources(root)
    validate_ancestors(root)
    validate_source_lock_value(json.loads((root / "docs/v2/source-locks.json").read_text(encoding="utf-8")))
    validate_projection_value(json.loads((root / PROJECTION_PATH).read_text(encoding="utf-8")))
    validate_runtime_contract(root)
    validate_document_contracts(root)
    validate_tasks(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-C retention retirement: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
