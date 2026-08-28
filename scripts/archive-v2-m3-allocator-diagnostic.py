#!/usr/bin/env python3
"""Create one immutable, byte-verified archive of an allocator diagnostic attempt."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import xml.etree.ElementTree as ET


SCHEMA = "NEREUS_V2_M3_ALLOCATOR_DIAGNOSTIC_ARCHIVE_IDENTITY_V1"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def require_hex(value: str, label: str, length: int = 64) -> str:
    if len(value) != length or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} is not lowercase hexadecimal with length {length}")
    return value


def regular_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if path.is_symlink():
            raise ValueError(f"diagnostic archive input contains a symbolic link: {relative}")
        mode = path.stat(follow_symlinks=False).st_mode
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode):
            raise ValueError(f"diagnostic archive input contains a non-regular file: {relative}")
        files.append(path)
    if not files:
        raise ValueError("diagnostic archive input contains no regular files")
    return files


def write_new(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    with os.fdopen(descriptor, "wb") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--diagnostic-output", required=True, type=Path)
    parser.add_argument("--junit-directory", required=True, type=Path)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--archived-on", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--executor-sha256", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-tests", required=True, type=int)
    parser.add_argument("--expected-failures", required=True, type=int)
    parser.add_argument("--expected-errors", required=True, type=int)
    parser.add_argument("--expected-skipped", required=True, type=int)
    parser.add_argument("--expected-suites", required=True, type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    diagnostic = args.diagnostic_output.expanduser().resolve(strict=True)
    junit = args.junit_directory.expanduser().resolve(strict=True)
    archive = args.archive.expanduser().absolute()
    for source, label in ((diagnostic, "diagnostic output"), (junit, "JUnit directory")):
        if source.is_symlink() or not source.is_dir():
            raise ValueError(f"{label} is absent, non-directory, or a symbolic link")
    if archive.exists() or archive.is_symlink():
        raise FileExistsError(f"diagnostic archive target already exists: {archive}")
    require_hex(args.source_commit, "source commit", 40)
    require_hex(args.plan_sha256, "plan digest")
    require_hex(args.executor_sha256, "executor digest")
    if not args.run_id or any(character in args.run_id for character in "\r\n"):
        raise ValueError("diagnostic run ID is absent or noncanonical")

    diagnostic_files = regular_files(diagnostic)
    junit_files = regular_files(junit)
    junit_xml = sorted(junit.glob("TEST-*.xml"))
    if len(junit_xml) != args.expected_suites:
        raise ValueError("diagnostic JUnit suite inventory differs")
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    junit_manifest_rows: list[str] = []
    for path in junit_xml:
        root = ET.parse(path).getroot()
        for field in totals:
            totals[field] += int(root.attrib.get(field, "0"))
        junit_manifest_rows.append(f"{sha256(path)}  {path.name}\n")
    expected = {
        "tests": args.expected_tests,
        "failures": args.expected_failures,
        "errors": args.expected_errors,
        "skipped": args.expected_skipped,
    }
    if totals != expected:
        raise ValueError(f"diagnostic JUnit summary differs: expected={expected} actual={totals}")

    archive.parent.mkdir(parents=True, exist_ok=True)
    archive.mkdir(mode=0o755)
    payload = archive / "payload"
    payload.mkdir(mode=0o755)
    manifest_rows: list[str] = []
    total_bytes = 0
    for label, source, source_files in (
            ("diagnostic-output", diagnostic, diagnostic_files),
            ("junit", junit, junit_files)):
        destination_root = payload / label
        destination_root.mkdir(mode=0o755)
        for source_file in source_files:
            relative = source_file.relative_to(source)
            destination = destination_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            with source_file.open("rb") as source_stream, destination.open("xb") as destination_stream:
                shutil.copyfileobj(source_stream, destination_stream, 1024 * 1024)
                destination_stream.flush()
                os.fsync(destination_stream.fileno())
            digest = sha256(source_file)
            if destination.stat().st_size != source_file.stat().st_size or sha256(destination) != digest:
                raise ValueError(f"diagnostic archive payload copy differs: {label}/{relative.as_posix()}")
            total_bytes += destination.stat().st_size
            manifest_rows.append(f"{digest}  {label}/{relative.as_posix()}\n")
            destination.chmod(0o444)

    copied_files = regular_files(payload)
    expected_file_count = len(diagnostic_files) + len(junit_files)
    expected_total_bytes = sum(path.stat().st_size for path in diagnostic_files + junit_files)
    if len(copied_files) != expected_file_count or total_bytes != expected_total_bytes:
        raise ValueError("diagnostic archive payload inventory differs after copy")
    for directory in sorted((path for path in payload.rglob("*") if path.is_dir()), reverse=True):
        directory.chmod(0o555)
    payload.chmod(0o555)

    manifest = archive / "SHA256SUMS"
    write_new(manifest, "".join(sorted(manifest_rows)).encode("utf-8"))
    range_receipt = diagnostic / "v4-range1024-10ms-formal-sequence.json"
    identity = {
        "schema": SCHEMA,
        "archivedOn": args.archived_on,
        "archivePath": str(archive),
        "payloadPath": str(payload),
        "diagnosticOutputPath": str(diagnostic),
        "junitDirectoryPath": str(junit),
        "sourceCommit": args.source_commit,
        "planSha256": args.plan_sha256,
        "executorSha256": args.executor_sha256,
        "runId": args.run_id,
        "diagnosticOnly": True,
        "authority": False,
        "selectionEligible": False,
        "diagnosticStatus": "FAILED",
        "nadvPresent": any(path.suffix == ".nadv4" for path in diagnostic_files),
        "junit": {**totals, "suites": len(junit_xml)},
        "junitXmlManifestSha256": hashlib.sha256("".join(junit_manifest_rows).encode()).hexdigest(),
        "rangeAttributionSha256": sha256(range_receipt) if range_receipt.is_file() else None,
        "payloadFileCount": len(copied_files),
        "payloadTotalBytes": total_bytes,
        "manifestSha256": sha256(manifest),
        "sourceAndPayloadByteIdentical": True,
        "promotableInput": False,
        "futureCampaignInput": False,
    }
    identity_path = archive / "archive-identity.json"
    write_new(identity_path, (json.dumps(identity, indent=2, ensure_ascii=True) + "\n").encode())
    archive.chmod(0o555)
    print(json.dumps({
        "archivePath": str(archive),
        "identitySha256": sha256(identity_path),
        "manifestSha256": sha256(manifest),
        "payloadFileCount": len(copied_files),
        "payloadTotalBytes": total_bytes,
        "junit": identity["junit"],
        "sourceAndPayloadByteIdentical": True,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
