#!/usr/bin/env python3
"""Validate the accepted additive M5-D target-scoped delete-authority amendment."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


SCHEMA = "NEREUS_V2_M5_DESIGN_AMENDMENT_2_V1"
RESULT = "DESIGN_AMENDMENT_2_ACCEPTED_IMPLEMENTATION_NOT_STARTED"
PASS_RESULT = "PASS_V2_M5_DESIGN_AMENDMENT_2_NO_RUNTIME_AUTHORITY"
AMENDMENT_ID = "M5-AMENDMENT-2-TARGET-SCOPED-PHYSICAL-DELETE-AUTHORITY-V1"
AUTHORIZED_DATE = "2026-09-03"
AUTHORIZATION_SOURCE_COMMIT = "05a20bf2c86ebb01c87c54ef959a8f6ca9a89796"
BASE_DESIGN_COMMIT = "c86fde3ed6f4319642987fd599022bd32e2cca5e"
BASE_MANIFEST_PATH = "docs/v2/detailed_design/m5/m5-design-freeze.json"
BASE_MANIFEST_SHA256 = "1b510471a49876ae047420ebd4e4196381ee47e69ef0756ef950d68c6e78c5f3"
PREDECESSOR_MANIFEST_PATH = "docs/v2/detailed_design/m5/m5-design-amendment-1.json"
PREDECESSOR_MANIFEST_SHA256 = "1f769b44740d9bfb2fa5d5d29fe2207d1cc72cb9e1ea5a8384223a6da143b452"
MANIFEST_PATH = "docs/v2/detailed_design/m5/m5-design-amendment-2.json"
ADR_PATH = "docs/decisions/0147-v2-m5-target-scoped-physical-delete-authority-amendment.md"
DESIGN_PATH = "docs/v2/detailed_design/m5/m5-d-target-scoped-physical-delete-authority-amendment.md"
ORIGINAL_M5_D_PATH = "docs/v2/detailed_design/m5/m5-d-physical-delete-orphan-and-gc.md"
ORIGINAL_M5_D_SHA256 = "ce4a53c872cdeb5c9f8c1db90d0ed2f7be746e65914b1c5dfe3e4f7e69ab9ba9"
PREDECESSOR_CHECKER_PATH = "scripts/check-v2-m5-design-amendment.py"
EXPECTED_DOCUMENTS = (ADR_PATH, DESIGN_PATH)


class Amendment2Error(RuntimeError):
    """Fail-closed M5-D design-amendment rejection."""


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise Amendment2Error(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise Amendment2Error(f"{label} lacks amendment contract: {missing}")


def require_false_authority(value: object, field: str) -> None:
    if value is not False:
        raise Amendment2Error(f"amendment manifest {field} must remain false")


def validate_manifest(root: Path, value: object) -> None:
    members = {
        "schema",
        "result",
        "amendmentId",
        "authorizedDate",
        "authorizationSourceCommit",
        "baseDesignCommit",
        "baseFreezeManifest",
        "predecessorAmendment",
        "documents",
        "implementationAuthority",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    }
    if not isinstance(value, dict) or set(value) != members:
        raise Amendment2Error("amendment manifest members differ")
    expected_header = {
        "schema": SCHEMA,
        "result": RESULT,
        "amendmentId": AMENDMENT_ID,
        "authorizedDate": AUTHORIZED_DATE,
        "authorizationSourceCommit": AUTHORIZATION_SOURCE_COMMIT,
        "baseDesignCommit": BASE_DESIGN_COMMIT,
    }
    for field, expected in expected_header.items():
        if value.get(field) != expected:
            raise Amendment2Error(f"amendment manifest {field} differs")
    expected_links = (
        ("baseFreezeManifest", BASE_MANIFEST_PATH, BASE_MANIFEST_SHA256),
        ("predecessorAmendment", PREDECESSOR_MANIFEST_PATH, PREDECESSOR_MANIFEST_SHA256),
    )
    for field, expected_path, expected_sha in expected_links:
        link = value.get(field)
        if not isinstance(link, dict) or set(link) != {"path", "sha256"}:
            raise Amendment2Error(f"amendment manifest {field} differs")
        if link.get("path") != expected_path or link.get("sha256") != expected_sha:
            raise Amendment2Error(f"amendment manifest {field} identity differs")
        if sha256((root / expected_path).read_bytes()) != expected_sha:
            raise Amendment2Error(f"amendment manifest {field} bound bytes differ")
    rows = value.get("documents")
    if not isinstance(rows, list) or len(rows) != len(EXPECTED_DOCUMENTS):
        raise Amendment2Error("amendment manifest document count differs")
    paths: list[str] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or set(row) != {"path", "sha256"}:
            raise Amendment2Error(f"amendment document row {index} differs")
        path = row.get("path")
        digest = row.get("sha256")
        if not isinstance(path, str) or path.startswith("/") or ".." in Path(path).parts:
            raise Amendment2Error(f"amendment document path {index} is unsafe")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise Amendment2Error(f"amendment document SHA {index} is invalid")
        candidate = root / path
        if not candidate.is_file() or sha256(candidate.read_bytes()) != digest:
            raise Amendment2Error(f"amendment document SHA differs: {path}")
        paths.append(path)
    if tuple(paths) != EXPECTED_DOCUMENTS:
        raise Amendment2Error("amendment document path order/set differs")
    for field in (
        "implementationAuthority",
        "physicalDeleteAuthority",
        "scenarioPromotionAuthority",
        "productionAuthority",
    ):
        require_false_authority(value.get(field), field)


def git_show(root: Path, commit: str, path: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(root), "show", f"{commit}:{path}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise Amendment2Error(f"cannot read historical identity: {commit}:{path}")
    return result.stdout


def validate_immutable_original_bytes(current: bytes, frozen: bytes) -> None:
    if sha256(frozen) != ORIGINAL_M5_D_SHA256:
        raise Amendment2Error("historical immutable M5-D identity differs")
    if current != frozen or sha256(current) != ORIGINAL_M5_D_SHA256:
        raise Amendment2Error("immutable original M5-D was changed")


def validate_chain(root: Path) -> None:
    for commit, label in (
        (BASE_DESIGN_COMMIT, "base M5 hard-freeze"),
        (AUTHORIZATION_SOURCE_COMMIT, "authorized predecessor implementation"),
    ):
        ancestor = subprocess.run(
            ["git", "-C", str(root), "merge-base", "--is-ancestor", commit, "HEAD"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if ancestor.returncode != 0:
            raise Amendment2Error(f"{label} commit is not an ancestor of HEAD")
    frozen_base = git_show(root, BASE_DESIGN_COMMIT, BASE_MANIFEST_PATH)
    if sha256(frozen_base) != BASE_MANIFEST_SHA256:
        raise Amendment2Error("historical base M5 freeze manifest identity differs")
    current_base = (root / BASE_MANIFEST_PATH).read_bytes()
    if current_base != frozen_base:
        raise Amendment2Error("current base M5 freeze manifest differs from historical bytes")
    validate_immutable_original_bytes(
        (root / ORIGINAL_M5_D_PATH).read_bytes(),
        git_show(root, BASE_DESIGN_COMMIT, ORIGINAL_M5_D_PATH),
    )
    predecessor = load_module(
        root / PREDECESSOR_CHECKER_PATH,
        "nereus_m5_predecessor_amendment_for_amendment_2",
    )
    predecessor.validate(root)


def validate_contract(root: Path) -> None:
    adr = (root / ADR_PATH).read_text(encoding="utf-8")
    design = (root / DESIGN_PATH).read_text(encoding="utf-8")
    require_literals(
        adr,
        (
            "Accepted on 2026-09-03",
            AMENDMENT_ID,
            "TargetDeleteAuthorityV1",
            "sole deletion linearization point",
            "OPEN_V1",
            "READ_FENCED_V1",
            "DELETE_INTENT_V1",
            "DELETE_DONE_V1",
            "authorityRevision",
            "No-op CAS is forbidden",
            "Time alone never clears a ticket",
            "Only successful exact application or exact same-key reconciliation of this second CAS grants",
            "There is no unconditional or best-effort downgrade.",
            "Any invariant requiring\nall-targets-or-none deletion remains unsupported.",
            "The immutable original M5-D document remains unchanged.",
        ),
        "ADR 0147",
    )
    require_literals(
        design,
        (
            AMENDMENT_ID,
            BASE_DESIGN_COMMIT,
            "m5-design-amendment-1.json",
            "exact same-key CAS",
            "The key is created once, never deleted, never moved, never reused, and never recreated",
            "authorityRevision",
            "A -> B -> A",
            "Closed writer inventory and admission",
            "CAS-1: close writers",
            "CAS-2: bind identity",
            "Only exact application or exact same-key reconciliation of this CAS authorizes external deletion.",
            "exact Object version",
            "exact sealed ledger fingerprint",
            "exact object-name/upload-ID abort",
            "No cross-key success matrix exists.",
            "cannot call them one atomic transaction",
            "real source-locked Oxia exact same-key CAS",
            "The immutable\noriginal M5-D file is not edited.",
            "No blocking design question remains for this amendment.",
            "no physical-delete or production authority exists at this boundary.",
        ),
        "M5-D design amendment",
    )


def validate_current_governance(root: Path) -> None:
    for path, literals, label in (
        (
            "docs/v2/detailed_design/m5/README.md",
            (DESIGN_PATH.split("/")[-1], MANIFEST_PATH.split("/")[-1], "ADR 0147"),
            "M5 index",
        ),
        (
            "docs/v2/detailed_design/m5/m5-implementation-log.md",
            ("ADR 0147", AMENDMENT_ID, "implementation remains NotStarted"),
            "M5 implementation log",
        ),
        (
            "docs/v2/08-implementation-plan-and-gates.md",
            ("target-scoped physical-delete authority", "m5-design-amendment-2.json"),
            "M5 implementation plan",
        ),
        (
            "docs/v2/05-manifest-read-retention-gc.md",
            ("target-scoped physical-delete authority", "ADR 0147"),
            "M5 architecture",
        ),
        (
            "docs/v2/README.md",
            ("ADR 0147", "target-scoped physical-delete authority", MANIFEST_PATH.split("/")[-1]),
            "V2 index",
        ),
    ):
        require_literals((root / path).read_text(encoding="utf-8"), literals, label)
    build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    require_literals(
        build,
        (
            "v2M5DesignAmendment2ContractTest",
            "v2M5DesignAmendment2SourceCheck",
            "v2M5DesignAmendment2Check",
        ),
        "root build",
    )


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    validate_chain(root)
    try:
        value = json.loads((root / MANIFEST_PATH).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise Amendment2Error(f"cannot read amendment manifest: {error}") from error
    validate_manifest(root, value)
    validate_contract(root)
    validate_current_governance(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-D design amendment: {error}", file=sys.stderr)
        return 1
    print(PASS_RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
