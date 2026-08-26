#!/usr/bin/env python3
"""Publish one exact-source, non-promotable V2 M3 child receipt without overwrite."""

from __future__ import annotations

import argparse
import importlib.util
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load receipt contract: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


CONTRACT = _load("check_v2_m3_child_for_publisher", SCRIPT_DIR / "check-v2-m3-child.py")
M2_PUBLISHER = _load(
    "publish_v2_m3_m2_for_child", SCRIPT_DIR / "publish-v2-m3-m2-regression.py"
)


def _read_external(path: Path) -> bytes:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise CONTRACT.ChildError(f"explicit evidence input is missing: {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise CONTRACT.ChildError(f"explicit evidence input is not a regular non-symlink file: {path}")
    size = path.stat().st_size
    if size <= 0 or size > CONTRACT.MAX_SINGLE_ATTACHMENT_BYTES:
        raise CONTRACT.ChildError(f"explicit evidence input bytes outside cap: {path}")
    return path.read_bytes()


def _clean_exact_head(root: Path, tested_commit: str) -> None:
    head = CONTRACT.current_head(root)
    if tested_commit != head:
        raise CONTRACT.ChildError(
            f"publisher requires tested commit to equal exact HEAD: {tested_commit} != {head}"
        )
    status = str(
        CONTRACT.git(root, "status", "--porcelain=v1", "--untracked-files=all", text=True)
    )
    if status:
        raise CONTRACT.ChildError("publisher requires a clean exact HEAD before writing evidence")


def _parse_attachment(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("attachment must use KIND=PATH")
    kind, raw_path = value.split("=", 1)
    if kind not in CONTRACT.ATTACHMENT_KINDS:
        raise argparse.ArgumentTypeError(f"attachment kind is outside the closed inventory: {kind}")
    if not raw_path:
        raise argparse.ArgumentTypeError("attachment path is empty")
    return kind, Path(raw_path)


def _parse_native_junit(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("native JUnit input must use RECEIPT_RELATIVE_PATH=PATH")
    relative, raw_path = value.split("=", 1)
    try:
        CONTRACT._native_relative(relative, "native JUnit receipt path")
    except CONTRACT.ChildError as error:
        raise argparse.ArgumentTypeError(str(error)) from error
    if not raw_path:
        raise argparse.ArgumentTypeError("native JUnit filesystem path is empty")
    return relative, Path(raw_path)


def _parse_allocator_external(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("allocator external input must use LOGICAL_NAME=PATH")
    name, raw_path = value.split("=", 1)
    if name not in CONTRACT.ALLOCATOR_EXTERNAL_NAMES:
        raise argparse.ArgumentTypeError(f"allocator external logical name is unknown: {name}")
    if not raw_path:
        raise argparse.ArgumentTypeError("allocator external filesystem path is empty")
    return name, Path(raw_path)


def _governed_allocator_path(root: Path, name: str, path: Path) -> str:
    root_absolute = Path(os.path.realpath(root))
    candidate = path if path.is_absolute() else root_absolute / path
    candidate = Path(os.path.realpath(candidate))
    try:
        relative = candidate.relative_to(root_absolute)
    except ValueError as error:
        if name in CONTRACT.ALLOCATOR_EXTERNAL_NAMES[6:] and name != "sourceLocks":
            return candidate.as_posix()
        raise CONTRACT.ChildError(
            f"allocator external {name} is outside the repository root: {path}"
        ) from error
    return str(PurePosixPath(*relative.parts))


def _governed_allocator_v2_path(root: Path, path: Path) -> str:
    root_absolute = Path(os.path.realpath(root))
    candidate = path if path.is_absolute() else root_absolute / path
    candidate = Path(os.path.realpath(candidate))
    try:
        relative = candidate.relative_to(root_absolute)
    except ValueError:
        return candidate.as_posix()
    return str(PurePosixPath(*relative.parts))


def _safe_suffix(path: Path, normalized: bool) -> str:
    if normalized:
        return ".json"
    suffix = path.suffix.lower()
    if re.fullmatch(r"\.[a-z0-9]{1,10}", suffix):
        return suffix
    return ".bin"


def _create_parents(path: Path) -> list[Path]:
    missing: list[Path] = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    for directory in reversed(missing):
        directory.mkdir()
    return missing


def _exclusive_write(path: Path, raw: bytes) -> None:
    with path.open("xb") as output:
        output.write(raw)


def _write_external_once(path: Path, raw: bytes, label: str) -> None:
    if path.exists() or path.is_symlink():
        raise CONTRACT.ChildError(f"{label} refuses to overwrite output: {path}")
    created = _create_parents(path.absolute().parent)
    try:
        _exclusive_write(path, raw)
    except OSError as error:
        path.unlink(missing_ok=True)
        for directory in created:
            try:
                directory.rmdir()
            except OSError:
                pass
        raise CONTRACT.ChildError(f"cannot publish {label}: {error}") from error


def _derive_summary(
    root: Path, kind: str, tested_commit: str, sources: list[tuple[str, Path, bytes]]
) -> dict[str, int]:
    summaries: list[dict[str, int]] = []
    _, bindings = CONTRACT.source_bindings(root, tested_commit)
    allocator_authority: dict[str, Any] | None = None
    allocator_v2_authority: dict[str, Any] | None = None
    allocator_profile = (
        CONTRACT.allocator_authority_profile({item[0] for item in sources})
        if kind == "ALLOCATOR_SELECTION"
        else None
    )
    if kind == "ALLOCATOR_SELECTION" and allocator_profile == "V1":
        raw = next(
            (
                value
                for attachment_kind, _, value in sources
                if attachment_kind == "ALLOCATOR_RAW_VERIFICATION"
            ),
            None,
        )
        if raw is None:
            raise CONTRACT.ChildError("allocator publisher lacks governed raw verification")
        allocator_authority = CONTRACT.validate_allocator_verification(
            CONTRACT.load_canonical_json(
                raw,
                "allocator governed raw verification",
                CONTRACT.ALLOCATOR_VERIFICATION_MAX_BYTES,
            ),
            root,
            tested_commit,
        )
    elif kind == "ALLOCATOR_SELECTION":
        raw = next(
            value
            for attachment_kind, _, value in sources
            if attachment_kind == "ALLOCATOR_V2_CAMPAIGN_VERIFICATION"
        )
        allocator_v2_authority = CONTRACT.validate_allocator_v2_campaign_verification(
            CONTRACT.load_canonical_json(
                raw,
                "allocator governed V2 campaign verification",
                CONTRACT.ALLOCATOR_V2_VERIFICATION_MAX_BYTES,
            ),
            root,
            tested_commit,
        )
    for attachment_kind, source_path, raw in sources:
        if attachment_kind == "JUNIT_SUMMARY":
            value = CONTRACT.load_canonical_json(
                raw, os.fspath(source_path), CONTRACT.SEALED_JUNIT_MAX_BYTES
            )
            summaries.append(CONTRACT.validate_junit(value, root, tested_commit, kind))
        elif attachment_kind == "LOCAL_CAP_RESULT":
            summaries.append(
                CONTRACT.validate_local_cap_result(
                    CONTRACT.load_canonical_json(raw, os.fspath(source_path)),
                    root,
                    tested_commit,
                )
            )
        elif attachment_kind == "MUTATION_MANIFEST":
            value = CONTRACT.load_canonical_json(
                raw, os.fspath(source_path), CONTRACT.MUTATION_MANIFEST_MAX_BYTES
            )
            CONTRACT.validate_mutation_manifest(value)
        elif attachment_kind == "RECOVERY_MANIFEST":
            CONTRACT.validate_recovery_manifest(raw)
        elif attachment_kind == "PROTOCOL_FIXTURE":
            CONTRACT.validate_protocol_fixture(raw)
        elif attachment_kind == "NATIVE_RESULT":
            value = CONTRACT.load_canonical_json(
                raw, os.fspath(source_path), CONTRACT.SEALED_NATIVE_MAX_BYTES
            )
            summaries.append(
                CONTRACT.validate_native_result(
                    value,
                    root,
                    kind,
                    tested_commit,
                    bindings[(kind, attachment_kind)],
                )
            )
        elif attachment_kind == "ALLOCATOR_RAW_VERIFICATION":
            assert allocator_authority is not None
            summaries.extend(
                [
                    allocator_authority["rawSummary"],
                    allocator_authority["verifierSummary"],
                ]
            )
        elif attachment_kind == "ALLOCATOR_V2_CAMPAIGN_VERIFICATION":
            assert allocator_v2_authority is not None
            summaries.append(allocator_v2_authority["summary"])
        elif attachment_kind in CONTRACT.ALLOCATOR_DERIVED_KINDS:
            assert allocator_authority is not None
            CONTRACT.validate_allocator_derived_evidence(
                CONTRACT.load_canonical_json(raw, os.fspath(source_path)),
                attachment_kind,
                tested_commit,
                bindings[(kind, attachment_kind)],
                allocator_authority,
            )
        elif attachment_kind in CONTRACT.NORMALIZED_TYPED_KINDS:
            value = CONTRACT.load_canonical_json(raw, os.fspath(source_path))
            summaries.append(
                CONTRACT.validate_typed_evidence(
                    value,
                    attachment_kind,
                    kind,
                    tested_commit,
                    bindings[(kind, attachment_kind)],
                )
            )
    return CONTRACT._sum_summaries(summaries)


def build_generic_receipt(
    root: Path,
    kind: str,
    tested_commit: str,
    output_path: PurePosixPath,
    attachment_inputs: list[tuple[str, Path]],
) -> tuple[dict[str, Any], dict[PurePosixPath, bytes]]:
    if kind not in CONTRACT.GENERIC_CHILD_KINDS:
        raise CONTRACT.ChildError(f"generic publisher cannot publish child kind: {kind}")
    if not CONTRACT.FINAL.is_under(output_path, CONTRACT.CHILD_PREFIX):
        raise CONTRACT.ChildError(f"generic child output is outside {CONTRACT.CHILD_PREFIX}: {output_path}")
    if not attachment_inputs or len(attachment_inputs) > CONTRACT.FINAL.MAX_ATTACHMENTS_PER_CHILD:
        raise CONTRACT.ChildError("explicit attachment count outside the closed cap")
    kinds = [item[0] for item in attachment_inputs]
    if kinds != sorted(set(kinds)):
        raise CONTRACT.ChildError("explicit attachment kinds must be sorted and unique")
    required_inputs = set(CONTRACT.REQUIRED_ATTACHMENTS[kind])
    if kind != "D_LOCAL_CAP" and "LOCAL_CAP_RESULT" in kinds:
        raise CONTRACT.ChildError("D1 local-cap result cannot be attached outside D_LOCAL_CAP")
    allocator_profile: str | None = None
    if kind == "ALLOCATOR_SELECTION":
        input_kinds = set(kinds)
        if "ALLOCATOR_V2_CAMPAIGN_VERIFICATION" in input_kinds:
            allocator_profile = CONTRACT.allocator_authority_profile(input_kinds)
        elif "ALLOCATOR_RAW_VERIFICATION" in input_kinds:
            allocator_profile = "V1"
        else:
            CONTRACT.allocator_authority_profile(input_kinds)
            raise AssertionError("allocator authority profile did not fail closed")
        if allocator_profile == "V1" and input_kinds.intersection(CONTRACT.ALLOCATOR_DERIVED_KINDS):
            raise CONTRACT.ChildError(
                "allocator derived summaries are publisher-owned and cannot be caller supplied"
            )
        if allocator_profile == "V1":
            required_inputs -= CONTRACT.ALLOCATOR_DERIVED_KINDS
    missing = required_inputs - set(kinds)
    if missing:
        raise CONTRACT.ChildError(
            f"explicit mandatory typed attachments are absent: {kind} {sorted(missing)}"
        )

    sources: list[tuple[str, Path, bytes]] = []
    for attachment_kind, path in attachment_inputs:
        raw = _read_external(path)
        if (
            attachment_kind == "JUNIT_SUMMARY"
            or
            attachment_kind in CONTRACT.REAL_RECEIPT_KINDS
            or attachment_kind == "NATIVE_RESULT"
            or attachment_kind == "ALLOCATOR_RAW_VERIFICATION"
            or attachment_kind == "ALLOCATOR_V2_CAMPAIGN_VERIFICATION"
        ):
            maximum = (
                CONTRACT.SEALED_JUNIT_MAX_BYTES
                if attachment_kind == "JUNIT_SUMMARY"
                else (
                    CONTRACT.SEALED_NATIVE_MAX_BYTES
                    if attachment_kind == "NATIVE_RESULT"
                    else (
                        CONTRACT.ALLOCATOR_VERIFICATION_MAX_BYTES
                        if attachment_kind == "ALLOCATOR_RAW_VERIFICATION"
                        else (
                            CONTRACT.ALLOCATOR_V2_VERIFICATION_MAX_BYTES
                            if attachment_kind == "ALLOCATOR_V2_CAMPAIGN_VERIFICATION"
                            else CONTRACT.MAX_CANONICAL_BYTES
                        )
                    )
                )
            )
            value = CONTRACT.load_external_json(raw, os.fspath(path), maximum)
            raw = CONTRACT.canonical_bytes(value)
        sources.append((attachment_kind, path, raw))
    if kind == "ALLOCATOR_SELECTION" and allocator_profile == "V1":
        authority_raw = next(
            raw
            for attachment_kind, _, raw in sources
            if attachment_kind == "ALLOCATOR_RAW_VERIFICATION"
        )
        authority = CONTRACT.validate_allocator_verification(
            CONTRACT.load_canonical_json(
                authority_raw,
                "allocator governed raw verification",
                CONTRACT.ALLOCATOR_VERIFICATION_MAX_BYTES,
            ),
            root,
            tested_commit,
        )
        _, bindings = CONTRACT.source_bindings(root, tested_commit)
        for attachment_kind in sorted(CONTRACT.ALLOCATOR_DERIVED_KINDS):
            binding = bindings[(kind, attachment_kind)]
            _, _, subject_count = CONTRACT._expected_typed_profile(attachment_kind, kind)
            summary = authority["rawSummary"]
            value = {
                "backend": binding["backend"],
                "errors": summary["errors"],
                "evidenceKind": attachment_kind,
                "executionClass": binding["executionClass"],
                "failures": summary["failures"],
                "nereusCommit": tested_commit,
                "result": "PASS_EVIDENCE_ONLY",
                "schema": CONTRACT.TYPED_SCHEMA,
                "skipped": summary["skipped"],
                "sourceIdentity": binding["sourceIdentity"],
                "subjectCount": subject_count,
                "tests": summary["tests"],
            }
            sources.append(
                (
                    attachment_kind,
                    Path(f"derived-{attachment_kind}.json"),
                    CONTRACT.canonical_bytes(value),
                )
            )
        sources.sort(key=lambda item: item[0])
    total_bytes = sum(len(raw) for _, _, raw in sources)
    if total_bytes > CONTRACT.MAX_TOTAL_ATTACHMENT_BYTES:
        raise CONTRACT.ChildError("explicit attachment total bytes exceed cap")
    summary = _derive_summary(root, kind, tested_commit, sources)
    CONTRACT._validate_summary(summary, "derived child testSummary")

    copied: dict[PurePosixPath, bytes] = {}
    rows: list[dict[str, Any]] = []
    for index, (attachment_kind, source_path, raw) in enumerate(sources):
        normalized = (
            attachment_kind == "JUNIT_SUMMARY"
            or attachment_kind in CONTRACT.NORMALIZED_TYPED_KINDS
            or attachment_kind == "ALLOCATOR_RAW_VERIFICATION"
            or attachment_kind == "ALLOCATOR_V2_CAMPAIGN_VERIFICATION"
            or attachment_kind == "LOCAL_CAP_RESULT"
        )
        destination = (
            output_path.parent
            / "attachments"
            / f"{index:02d}-{attachment_kind}{_safe_suffix(source_path, normalized)}"
        )
        copied[destination] = raw
        rows.append(
            {
                "bytes": len(raw),
                "kind": attachment_kind,
                "path": str(destination),
                "sha256": CONTRACT.sha256(raw),
            }
        )
    if [row["path"] for row in rows] != sorted(row["path"] for row in rows):
        raise CONTRACT.ChildError("derived attachment paths are not canonical sorted order")
    receipt = {
        "attachments": rows,
        "exclusions": CONTRACT.expected_exclusions(kind),
        "kind": kind,
        "promotionEligible": False,
        "result": CONTRACT.CHILD_RESULTS[kind],
        "schema": CONTRACT.SCHEMA,
        "sourceTuple": CONTRACT.expected_source_tuple(root, tested_commit),
        "testSummary": summary,
    }
    return receipt, copied


def publish_generic(
    root: Path,
    kind: str,
    tested_commit: str,
    output_path: PurePosixPath,
    attachment_inputs: list[tuple[str, Path]],
) -> bytes:
    root = CONTRACT.ensure_root(root)
    _clean_exact_head(root, tested_commit)
    receipt, copied = build_generic_receipt(
        root, kind, tested_commit, output_path, attachment_inputs
    )
    raw = CONTRACT.canonical_bytes(receipt)
    destinations = [root.joinpath(*path.parts) for path in (*copied, output_path)]
    existing = [path for path in destinations if path.exists() or path.is_symlink()]
    if existing:
        raise CONTRACT.ChildError(
            "publisher refuses to overwrite evidence: " + ", ".join(os.fspath(path) for path in existing)
        )

    created_directories: list[Path] = []
    created_files: list[Path] = []
    try:
        for relative, attachment_raw in copied.items():
            destination = root.joinpath(*relative.parts)
            created_directories.extend(_create_parents(destination.parent))
            _exclusive_write(destination, attachment_raw)
            created_files.append(destination)
        destination = root.joinpath(*output_path.parts)
        created_directories.extend(_create_parents(destination.parent))
        _exclusive_write(destination, raw)
        created_files.append(destination)
    except OSError as error:
        for path in reversed(created_files):
            path.unlink(missing_ok=True)
        for directory in created_directories:
            try:
                directory.rmdir()
            except OSError:
                pass
        raise CONTRACT.ChildError(f"cannot publish child evidence atomically: {error}") from error

    CONTRACT.build_final_child_identity(root, output_path, kind, tested_commit)
    return raw


def publish_w1(
    root: Path,
    tested_commit: str,
    output_path: PurePosixPath,
    child_results: list[Path],
) -> bytes:
    root = CONTRACT.ensure_root(root)
    _clean_exact_head(root, tested_commit)
    try:
        raw = M2_PUBLISHER.publish(root, tested_commit, output_path, child_results)
    except M2_PUBLISHER.CONTRACT.RegressionError as error:
        raise CONTRACT.ChildError(str(error)) from error
    CONTRACT.build_final_child_identity(
        root, output_path, "W1_CURRENT_SOURCE_M2_REGRESSION", tested_commit
    )
    return raw


def seal_real_execution(
    attachment_kind: str,
    tested_commit: str,
    raw_evidence_path: Path,
    junit_xml_path: Path,
    output_path: Path,
) -> bytes:
    raw_evidence = _read_external(raw_evidence_path)
    junit_xml = _read_external(junit_xml_path)
    receipt = CONTRACT.seal_real_execution_receipt(
        raw_evidence, junit_xml, attachment_kind, tested_commit
    )
    raw = CONTRACT.canonical_bytes(receipt)
    _write_external_once(output_path, raw, "sealed real execution receipt")
    return raw


def seal_native_execution(
    root: Path,
    child_kind: str,
    tested_commit: str,
    raw_evidence_path: Path,
    junit_xml_paths: list[tuple[str, Path]],
    output_path: Path,
) -> bytes:
    root = CONTRACT.ensure_root(root)
    _clean_exact_head(root, tested_commit)
    raw_evidence = _read_external(raw_evidence_path)
    junit_inputs = [(relative, _read_external(path)) for relative, path in junit_xml_paths]
    receipt = CONTRACT.seal_native_execution_receipt(
        root, raw_evidence, junit_inputs, child_kind, tested_commit
    )
    raw = CONTRACT.canonical_bytes(receipt)
    _write_external_once(output_path, raw, "sealed native execution receipt")
    return raw


def seal_allocator_verification(
    root: Path,
    tested_commit: str,
    java_verification_path: Path,
    junit_xml_path: Path,
    external_paths: list[tuple[str, Path]],
    output_path: Path,
) -> bytes:
    root = CONTRACT.ensure_root(root)
    _clean_exact_head(root, tested_commit)
    java_verification = _read_external(java_verification_path)
    junit_xml = _read_external(junit_xml_path)
    governed_java_path = _governed_allocator_path(
        root, "raw-verification.json", java_verification_path
    )
    governed_junit_path = _governed_allocator_path(
        root, "verifier-junit.xml", junit_xml_path
    )
    governed_paths = [
        (name, _governed_allocator_path(root, name, path))
        for name, path in external_paths
    ]
    receipt = CONTRACT.seal_allocator_verification_receipt(
        root,
        java_verification,
        governed_java_path,
        junit_xml,
        governed_junit_path,
        tested_commit,
        governed_paths,
    )
    raw = CONTRACT.canonical_bytes(receipt)
    _write_external_once(output_path, raw, "governed allocator verification receipt")
    return raw


def seal_allocator_v2_verification(
    root: Path,
    tested_commit: str,
    checkpoint_path: Path,
    evaluation_path: Path,
    diagnostic_path: Path,
    diagnostic_junit_path: Path,
    formal_junit_path: Path,
    promotion_decision_path: Path,
    executor_artifact_path: Path,
    workload_plan_path: Path,
    execution_attachment_paths: list[Path],
    output_path: Path,
) -> bytes:
    root = CONTRACT.ensure_root(root)
    _clean_exact_head(root, tested_commit)
    inputs = (
        checkpoint_path,
        evaluation_path,
        diagnostic_path,
        diagnostic_junit_path,
        formal_junit_path,
        promotion_decision_path,
        executor_artifact_path,
        workload_plan_path,
        *execution_attachment_paths,
    )
    raw_inputs = {path: _read_external(path) for path in inputs}
    receipt = CONTRACT.seal_allocator_v2_campaign_verification_receipt(
        root,
        tested_commit,
        raw_inputs[checkpoint_path],
        raw_inputs[evaluation_path],
        raw_inputs[diagnostic_path],
        raw_inputs[diagnostic_junit_path],
        raw_inputs[formal_junit_path],
        raw_inputs[promotion_decision_path],
        [
            ("executorArtifact", _governed_allocator_v2_path(root, executor_artifact_path)),
            ("workloadPlan", _governed_allocator_v2_path(root, workload_plan_path)),
        ],
        [
            _governed_allocator_v2_path(root, path)
            for path in execution_attachment_paths
        ],
    )
    raw = CONTRACT.canonical_bytes(receipt)
    _write_external_once(output_path, raw, "governed allocator V2 campaign verification receipt")
    return raw


def seal_junit_execution(
    root: Path,
    child_kind: str,
    tested_commit: str,
    junit_xml_paths: list[tuple[str, Path]],
    output_path: Path,
) -> bytes:
    root = CONTRACT.ensure_root(root)
    _clean_exact_head(root, tested_commit)
    junit_inputs = [(relative, _read_external(path)) for relative, path in junit_xml_paths]
    receipt = CONTRACT.seal_junit_execution_receipt(
        root, junit_inputs, child_kind, tested_commit
    )
    raw = CONTRACT.canonical_bytes(receipt)
    _write_external_once(output_path, raw, "governed JUnit execution receipt")
    return raw


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=SCRIPT_DIR.parent)
    parser.add_argument("--kind", choices=CONTRACT.CHILD_KINDS)
    parser.add_argument("--tested-commit", required=True)
    parser.add_argument("--output")
    parser.add_argument("--attachment", action="append", type=_parse_attachment, default=[])
    parser.add_argument("--m2-child-result", action="append", type=Path, default=[])
    parser.add_argument("--seal-real-kind", choices=sorted(CONTRACT.REAL_EXECUTION_PROFILES))
    parser.add_argument("--seal-native-kind", choices=sorted(CONTRACT.NATIVE_PROFILES))
    parser.add_argument("--seal-junit-kind", choices=sorted(CONTRACT.GENERIC_CHILD_KINDS))
    parser.add_argument("--seal-allocator-verification", action="store_true")
    parser.add_argument("--seal-allocator-v2-verification", action="store_true")
    parser.add_argument("--raw-evidence", type=Path)
    parser.add_argument("--junit-xml", type=Path)
    parser.add_argument(
        "--native-junit-xml", action="append", type=_parse_native_junit, default=[]
    )
    parser.add_argument(
        "--child-junit-xml", action="append", type=_parse_native_junit, default=[]
    )
    parser.add_argument(
        "--allocator-external-file",
        action="append",
        type=_parse_allocator_external,
        default=[],
    )
    parser.add_argument("--sealed-output", type=Path)
    parser.add_argument("--allocator-v2-checkpoint", type=Path)
    parser.add_argument("--allocator-v2-evaluation", type=Path)
    parser.add_argument("--allocator-v2-diagnostic", type=Path)
    parser.add_argument("--allocator-v2-diagnostic-junit", type=Path)
    parser.add_argument("--allocator-v2-formal-junit", type=Path)
    parser.add_argument("--allocator-v2-promotion-decision", type=Path)
    parser.add_argument("--allocator-v2-executor-artifact", type=Path)
    parser.add_argument("--allocator-v2-workload-plan", type=Path)
    parser.add_argument(
        "--allocator-v2-execution-attachment", action="append", type=Path, default=[]
    )
    return parser.parse_args(argv[1:])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        allocator_v2_values = (
            args.allocator_v2_checkpoint,
            args.allocator_v2_evaluation,
            args.allocator_v2_diagnostic,
            args.allocator_v2_diagnostic_junit,
            args.allocator_v2_formal_junit,
            args.allocator_v2_promotion_decision,
            args.allocator_v2_executor_artifact,
            args.allocator_v2_workload_plan,
        )
        if args.seal_allocator_v2_verification:
            if (
                args.kind is not None
                or args.output is not None
                or args.attachment
                or args.m2_child_result
                or args.seal_real_kind is not None
                or args.seal_native_kind is not None
                or args.seal_junit_kind is not None
                or args.seal_allocator_verification
                or args.raw_evidence is not None
                or args.junit_xml is not None
                or args.native_junit_xml
                or args.child_junit_xml
                or args.allocator_external_file
                or any(value is None for value in allocator_v2_values)
                or not args.allocator_v2_execution_attachment
                or args.sealed_output is None
            ):
                raise CONTRACT.ChildError(
                    "governed allocator V2 verification mode has incomplete or mixed arguments"
                )
            raw = seal_allocator_v2_verification(
                args.repo_root,
                args.tested_commit,
                args.allocator_v2_checkpoint,
                args.allocator_v2_evaluation,
                args.allocator_v2_diagnostic,
                args.allocator_v2_diagnostic_junit,
                args.allocator_v2_formal_junit,
                args.allocator_v2_promotion_decision,
                args.allocator_v2_executor_artifact,
                args.allocator_v2_workload_plan,
                args.allocator_v2_execution_attachment,
                args.sealed_output,
            )
            print(
                "V2 M3 allocator V2 campaign verification sealed: "
                f"tested={args.tested_commit} output={args.sealed_output} "
                f"bytes={len(raw)} sha256={CONTRACT.sha256(raw)}"
            )
            return 0
        if any(value is not None for value in allocator_v2_values) or args.allocator_v2_execution_attachment:
            raise CONTRACT.ChildError(
                "allocator V2 verification arguments require --seal-allocator-v2-verification"
            )
        if args.seal_real_kind is not None:
            if (
                args.kind is not None
                or args.output is not None
                or args.attachment
                or args.m2_child_result
                or args.seal_native_kind is not None
                or args.seal_junit_kind is not None
                or args.seal_allocator_verification
                or args.native_junit_xml
                or args.child_junit_xml
                or args.allocator_external_file
                or args.raw_evidence is None
                or args.junit_xml is None
                or args.sealed_output is None
            ):
                raise CONTRACT.ChildError("sealed real execution mode has incomplete or mixed arguments")
            raw = seal_real_execution(
                args.seal_real_kind,
                args.tested_commit,
                args.raw_evidence,
                args.junit_xml,
                args.sealed_output,
            )
            print(
                "V2 M3 real execution sealed: "
                f"kind={args.seal_real_kind} tested={args.tested_commit} "
                f"output={args.sealed_output} bytes={len(raw)} sha256={CONTRACT.sha256(raw)}"
            )
            return 0
        if args.seal_native_kind is not None:
            if (
                args.kind is not None
                or args.output is not None
                or args.attachment
                or args.m2_child_result
                or args.seal_real_kind is not None
                or args.seal_junit_kind is not None
                or args.seal_allocator_verification
                or args.raw_evidence is None
                or args.junit_xml is not None
                or not args.native_junit_xml
                or args.child_junit_xml
                or args.allocator_external_file
                or args.sealed_output is None
            ):
                raise CONTRACT.ChildError("sealed native execution mode has incomplete or mixed arguments")
            raw = seal_native_execution(
                args.repo_root,
                args.seal_native_kind,
                args.tested_commit,
                args.raw_evidence,
                args.native_junit_xml,
                args.sealed_output,
            )
            print(
                "V2 M3 native execution sealed: "
                f"kind={args.seal_native_kind} tested={args.tested_commit} "
                f"output={args.sealed_output} bytes={len(raw)} sha256={CONTRACT.sha256(raw)}"
            )
            return 0
        if args.seal_allocator_verification:
            if (
                args.kind is not None
                or args.output is not None
                or args.attachment
                or args.m2_child_result
                or args.seal_real_kind is not None
                or args.seal_native_kind is not None
                or args.seal_junit_kind is not None
                or args.raw_evidence is None
                or args.junit_xml is None
                or args.native_junit_xml
                or args.child_junit_xml
                or not args.allocator_external_file
                or args.sealed_output is None
            ):
                raise CONTRACT.ChildError(
                    "governed allocator verification mode has incomplete or mixed arguments"
                )
            raw = seal_allocator_verification(
                args.repo_root,
                args.tested_commit,
                args.raw_evidence,
                args.junit_xml,
                args.allocator_external_file,
                args.sealed_output,
            )
            print(
                "V2 M3 allocator verification sealed: "
                f"tested={args.tested_commit} output={args.sealed_output} "
                f"bytes={len(raw)} sha256={CONTRACT.sha256(raw)}"
            )
            return 0
        if args.seal_junit_kind is not None:
            if (
                args.kind is not None
                or args.output is not None
                or args.attachment
                or args.m2_child_result
                or args.seal_real_kind is not None
                or args.seal_native_kind is not None
                or args.seal_allocator_verification
                or args.raw_evidence is not None
                or args.junit_xml is not None
                or args.native_junit_xml
                or args.allocator_external_file
                or not args.child_junit_xml
                or args.sealed_output is None
            ):
                raise CONTRACT.ChildError(
                    "governed JUnit sealing mode has incomplete or mixed arguments"
                )
            raw = seal_junit_execution(
                args.repo_root,
                args.seal_junit_kind,
                args.tested_commit,
                args.child_junit_xml,
                args.sealed_output,
            )
            print(
                "V2 M3 JUnit execution sealed: "
                f"kind={args.seal_junit_kind} tested={args.tested_commit} "
                f"output={args.sealed_output} bytes={len(raw)} sha256={CONTRACT.sha256(raw)}"
            )
            return 0
        if args.kind is None or args.output is None:
            raise CONTRACT.ChildError("child publication mode requires --kind and --output")
        if (
            args.raw_evidence is not None
            or args.junit_xml is not None
            or args.native_junit_xml
            or args.child_junit_xml
            or args.allocator_external_file
            or args.sealed_output is not None
        ):
            raise CONTRACT.ChildError("child publication mode rejects mixed sealing arguments")
        output = CONTRACT.safe_relative(args.output, "--output")
        if args.kind == "W1_CURRENT_SOURCE_M2_REGRESSION":
            if args.attachment:
                raise CONTRACT.ChildError("W1 accepts only explicit --m2-child-result inputs")
            raw = publish_w1(
                args.repo_root, args.tested_commit, output, args.m2_child_result
            )
        else:
            if args.m2_child_result:
                raise CONTRACT.ChildError("generic M3 children do not accept M2 child results")
            raw = publish_generic(
                args.repo_root, args.kind, args.tested_commit, output, args.attachment
            )
    except (CONTRACT.ChildError, OSError) as error:
        print(f"V2 M3 child publisher: {error}", file=sys.stderr)
        return 1
    print(
        "V2 M3 child published: "
        f"kind={args.kind} tested={args.tested_commit} output={output} "
        f"bytes={len(raw)} sha256={CONTRACT.sha256(raw)} "
        "promotionEligible=false scenarioPromotion=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
