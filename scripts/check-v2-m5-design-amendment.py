#!/usr/bin/env python3
"""Validate the accepted additive M5-C single-Binding authority amendment."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys


SCHEMA = "NEREUS_V2_M5_DESIGN_AMENDMENT_V1"
RESULT = "DESIGN_AMENDMENT_ACCEPTED_IMPLEMENTATION_IN_PROGRESS"
AMENDMENT_ID = "M5-C-SINGLE_BINDING_RETIREMENT_AUTHORITY"
AUTHORIZED_DATE = "2026-09-03"
BASE_DESIGN_COMMIT = "c86fde3ed6f4319642987fd599022bd32e2cca5e"
BASE_MANIFEST_PATH = "docs/v2/detailed_design/m5/m5-design-freeze.json"
BASE_MANIFEST_SHA256 = "1b510471a49876ae047420ebd4e4196381ee47e69ef0756ef950d68c6e78c5f3"
MANIFEST_PATH = "docs/v2/detailed_design/m5/m5-design-amendment-1.json"
ADR_PATH = "docs/decisions/0146-v2-m5-single-binding-retirement-authority-amendment.md"
DESIGN_PATH = "docs/v2/detailed_design/m5/m5-c-single-binding-retirement-authority-amendment.md"
EXPECTED_DOCUMENTS = (ADR_PATH, DESIGN_PATH)


class AmendmentError(RuntimeError):
    """Fail-closed M5-C design-amendment rejection."""


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise AmendmentError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def require_literals(text: str, literals: tuple[str, ...], label: str) -> None:
    missing = [literal for literal in literals if literal not in text]
    if missing:
        raise AmendmentError(f"{label} lacks amendment contract: {missing}")


def validate_manifest(root: Path, value: object) -> None:
    members = {
        "schema",
        "result",
        "amendmentId",
        "authorizedDate",
        "baseDesignCommit",
        "baseFreezeManifestSha256",
        "documents",
    }
    if not isinstance(value, dict) or set(value) != members:
        raise AmendmentError("amendment manifest members differ")
    expected_header = {
        "schema": SCHEMA,
        "result": RESULT,
        "amendmentId": AMENDMENT_ID,
        "authorizedDate": AUTHORIZED_DATE,
        "baseDesignCommit": BASE_DESIGN_COMMIT,
        "baseFreezeManifestSha256": BASE_MANIFEST_SHA256,
    }
    for field, expected in expected_header.items():
        if value.get(field) != expected:
            raise AmendmentError(f"amendment manifest {field} differs")
    rows = value.get("documents")
    if not isinstance(rows, list) or len(rows) != len(EXPECTED_DOCUMENTS):
        raise AmendmentError("amendment manifest document count differs")
    paths: list[str] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or set(row) != {"path", "sha256"}:
            raise AmendmentError(f"amendment document row {index} differs")
        path = row.get("path")
        digest = row.get("sha256")
        if not isinstance(path, str) or path.startswith("/") or ".." in Path(path).parts:
            raise AmendmentError(f"amendment document path {index} is unsafe")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise AmendmentError(f"amendment document SHA {index} is invalid")
        candidate = root / path
        if not candidate.is_file() or sha256(candidate.read_bytes()) != digest:
            raise AmendmentError(f"amendment document SHA differs: {path}")
        paths.append(path)
    if tuple(paths) != EXPECTED_DOCUMENTS:
        raise AmendmentError("amendment document path order/set differs")


def validate_base(root: Path) -> None:
    ancestor = subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", BASE_DESIGN_COMMIT, "HEAD"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if ancestor.returncode != 0:
        raise AmendmentError("base M5 hard-freeze commit is not an ancestor of HEAD")
    frozen = subprocess.run(
        ["git", "-C", str(root), "show", f"{BASE_DESIGN_COMMIT}:{BASE_MANIFEST_PATH}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if frozen.returncode != 0 or sha256(frozen.stdout) != BASE_MANIFEST_SHA256:
        raise AmendmentError("base M5 hard-freeze manifest identity differs")


def validate_contract(root: Path) -> None:
    adr = (root / ADR_PATH).read_text(encoding="utf-8")
    design = (root / DESIGN_PATH).read_text(encoding="utf-8")
    require_literals(
        adr,
        (
            "Accepted on 2026-09-03",
            "BindingRetirementAuthorityV1",
            "PENDING_REFERENCE_MUTATION_V1",
            "REFERENCE_SCAN_FENCED_V1",
            "sole retirement linearization point",
            "no cross-key success matrix and no sequential-CAS fallback.",
            "M4 release eligibility",
            "M5-D physical-delete",
        ),
        "ADR 0146",
    )
    require_literals(
        design,
        (
            BASE_DESIGN_COMMIT,
            "single-Binding retirement authority",
            "One physical authority cell",
            "Ticketed mutation rule",
            "REFERENCE_SCAN_FENCED_V1 / FULL_V1",
            "single exact CAS at the existing selector key is the only linearization point.",
            "No selector/batch-key split-state matrix remains",
            "Pulsar aggregate retirement",
            "real Oxia 0.9.4 exact single-key CAS",
            "No blocking design question remains for this amendment.",
        ),
        "M5-C design amendment",
    )


def validate_current_governance(root: Path) -> None:
    design = load_module(root / "scripts/check-v2-m5-design.py", "nereus_m5_base_for_amendment")
    scenarios = json.loads((root / design.SCENARIO_PATH).read_text(encoding="utf-8"))
    design.validate_scenarios_value(scenarios)
    design.validate_m4_final(root)
    for path, literals, label in (
        (
            "docs/v2/detailed_design/m5/README.md",
            ("m5-c-single-binding-retirement-authority-amendment.md", MANIFEST_PATH.split("/")[-1]),
            "M5 index",
        ),
        (
            "docs/v2/detailed_design/m5/m5-implementation-log.md",
            ("ADR 0146", "source-bound child receipt", "has not run"),
            "M5 implementation log",
        ),
        (
            "docs/v2/08-implementation-plan-and-gates.md",
            ("single-Binding retirement authority", "exact single-key CAS"),
            "M5 implementation plan",
        ),
        (
            "docs/v2/README.md",
            ("ADR 0146", "single-Binding retirement authority"),
            "V2 index",
        ),
    ):
        require_literals((root / path).read_text(encoding="utf-8"), literals, label)
    build = (root / "build.gradle.kts").read_text(encoding="utf-8")
    require_literals(
        build,
        (
            "v2M5DesignAmendmentContractTest",
            "v2M5DesignAmendmentSourceCheck",
            "v2M5DesignAmendmentCheck",
        ),
        "root build",
    )


def validate(root: Path) -> None:
    root = root.resolve(strict=True)
    validate_base(root)
    try:
        value = json.loads((root / MANIFEST_PATH).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise AmendmentError(f"cannot read amendment manifest: {error}") from error
    validate_manifest(root, value)
    validate_contract(root)
    validate_current_governance(root)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    try:
        validate(root)
    except Exception as error:
        print(f"M5-C design amendment: {error}", file=sys.stderr)
        return 1
    print(RESULT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
