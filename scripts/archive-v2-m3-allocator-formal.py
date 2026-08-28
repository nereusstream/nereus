#!/usr/bin/env python3
"""Create one immutable, byte-verified external archive of allocator formal evidence."""

from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat


SCHEMA_BY_PROTOCOL = {
    3: "NEREUS_V2_M3_ALLOCATOR_FORMAL_ARCHIVE_IDENTITY_V2",
    4: "NEREUS_V2_M3_ALLOCATOR_FORMAL_ARCHIVE_IDENTITY_V3",
    5: "NEREUS_V2_M3_ALLOCATOR_FORMAL_ARCHIVE_IDENTITY_V4",
}
NON_PROMOTABLE_EVALUATION_STATUSES = {
    "NATIVE_BASELINE_UNAVAILABLE",
    "NONE_QUALIFIED",
    "BOTH_QUALIFIED",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def regular_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if path.is_symlink():
            raise ValueError(f"archive input contains a symbolic link: {relative}")
        mode = path.stat(follow_symlinks=False).st_mode
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode):
            raise ValueError(f"archive input contains a non-regular file: {relative}")
        files.append(path)
    if not files:
        raise ValueError("archive input contains no regular files")
    return files


def require_hex(value: str, label: str, length: int = 64) -> str:
    if len(value) != length or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} is not lowercase hexadecimal with length {length}")
    return value


def write_new(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
    except BaseException:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--protocol-version", type=int, choices=sorted(SCHEMA_BY_PROTOCOL), default=3)
    parser.add_argument("--archived-on", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--campaign-result-sha256", required=True)
    parser.add_argument("--final-checkpoint-relative-path", required=True)
    parser.add_argument("--final-checkpoint-sha256", required=True)
    parser.add_argument("--evaluation-sha256", required=True)
    parser.add_argument("--attachment-root-sha256", required=True)
    parser.add_argument("--formal-junit-sha256", required=True)
    parser.add_argument("--expected-file-count", required=True, type=int)
    parser.add_argument("--expected-total-bytes", required=True, type=int)
    parser.add_argument("--evaluation-status", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source = args.source.expanduser().resolve(strict=True)
    archive = args.archive.expanduser().absolute()
    if source.is_symlink() or not source.is_dir():
        raise ValueError("archive source is absent, non-directory, or a symbolic link")
    if archive.exists() or archive.is_symlink():
        raise FileExistsError(f"archive target already exists: {archive}")
    try:
        archived_on = date.fromisoformat(args.archived_on)
    except ValueError as error:
        raise ValueError("archive date is not canonical ISO-8601") from error
    if archived_on.isoformat() != args.archived_on:
        raise ValueError("archive date is not canonical ISO-8601")
    require_hex(args.source_commit, "source commit", 40)
    for label, value in (
        ("plan digest", args.plan_sha256),
        ("campaign-result digest", args.campaign_result_sha256),
        ("final checkpoint digest", args.final_checkpoint_sha256),
        ("evaluation digest", args.evaluation_sha256),
        ("attachment root digest", args.attachment_root_sha256),
        ("formal JUnit digest", args.formal_junit_sha256),
    ):
        require_hex(value, label)
    if args.evaluation_status not in NON_PROMOTABLE_EVALUATION_STATUSES:
        raise ValueError("archive evaluation status is not a legal non-promotable allocator terminal")

    source_files = regular_files(source)
    source_total_bytes = sum(path.stat().st_size for path in source_files)
    if len(source_files) != args.expected_file_count or source_total_bytes != args.expected_total_bytes:
        raise ValueError("archive source file count or byte count differs")

    campaign_result = source / "campaign-result.json"
    evaluation_relative_path = (
        "evaluation.naev" if args.protocol_version == 3 else f"evaluation.naev{args.protocol_version}"
    )
    evaluation = source / evaluation_relative_path
    final_checkpoint = source / args.final_checkpoint_relative_path
    if sha256(campaign_result) != args.campaign_result_sha256:
        raise ValueError("archive campaign-result digest differs")
    if sha256(evaluation) != args.evaluation_sha256:
        raise ValueError("archive evaluation digest differs")
    if sha256(final_checkpoint) != args.final_checkpoint_sha256:
        raise ValueError("archive final checkpoint digest differs")
    result_value = json.loads(campaign_result.read_text(encoding="utf-8"))
    if result_value.get("status") != "COMPLETED" or result_value.get("terminalReason") != "COMPLETED":
        raise ValueError("archive campaign-result is not terminal COMPLETED")

    archive.parent.mkdir(parents=True, exist_ok=True)
    archive.mkdir(mode=0o755)
    payload = archive / "payload"
    payload.mkdir(mode=0o755)

    manifest_rows: list[str] = []
    copied_total_bytes = 0
    for source_file in source_files:
        relative = source_file.relative_to(source)
        destination = payload / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        with source_file.open("rb") as source_stream, destination.open("xb") as destination_stream:
            shutil.copyfileobj(source_stream, destination_stream, 1024 * 1024)
            destination_stream.flush()
            os.fsync(destination_stream.fileno())
        source_digest = sha256(source_file)
        destination_digest = sha256(destination)
        if source_file.stat().st_size != destination.stat().st_size or source_digest != destination_digest:
            raise ValueError(f"archive payload copy differs: {relative.as_posix()}")
        copied_total_bytes += destination.stat().st_size
        manifest_rows.append(f"{destination_digest}  {relative.as_posix()}\n")
        destination.chmod(0o444)

    copied_files = regular_files(payload)
    if len(copied_files) != len(source_files) or copied_total_bytes != source_total_bytes:
        raise ValueError("archive payload inventory differs after copy")
    for directory in sorted((path for path in payload.rglob("*") if path.is_dir()), reverse=True):
        directory.chmod(0o555)
    payload.chmod(0o555)

    manifest = archive / "SHA256SUMS"
    write_new(manifest, "".join(manifest_rows).encode("utf-8"))
    manifest_digest = sha256(manifest)
    identity = {
        "schema": SCHEMA_BY_PROTOCOL[args.protocol_version],
        "protocolVersion": args.protocol_version,
        "archivedOn": args.archived_on,
        "archivePath": str(archive),
        "payloadPath": str(payload),
        "sourcePath": str(source),
        "sourceCommit": args.source_commit,
        "planSha256": args.plan_sha256,
        "campaignStatus": "COMPLETED",
        "evaluationStatus": args.evaluation_status,
        "selectionEligible": False,
        "allocatorMode": "UNSELECTED",
        f"nars{args.protocol_version}Present": False,
        "payloadFileCount": len(copied_files),
        "payloadTotalBytes": copied_total_bytes,
        "manifestRelativePath": "SHA256SUMS",
        "manifestSha256": manifest_digest,
        "campaignResultRelativePath": "campaign-result.json",
        "campaignResultSha256": args.campaign_result_sha256,
        "evaluationRelativePath": evaluation_relative_path,
        "evaluationSha256": args.evaluation_sha256,
        "finalCheckpointRelativePath": args.final_checkpoint_relative_path,
        "finalCheckpointSha256": args.final_checkpoint_sha256,
        "attachmentRootSha256": args.attachment_root_sha256,
        "formalJUnitSha256": args.formal_junit_sha256,
        "sourceAndPayloadByteIdentical": True,
        "formalEvidenceImmutable": True,
        "promotableInput": False,
        "futureCampaignInput": False,
    }
    identity_path = archive / "archive-identity.json"
    write_new(identity_path, (json.dumps(identity, indent=2, ensure_ascii=True) + "\n").encode("utf-8"))
    print(
        json.dumps(
            {
                "archivePath": str(archive),
                "identitySha256": sha256(identity_path),
                "manifestSha256": manifest_digest,
                "payloadFileCount": len(copied_files),
                "payloadTotalBytes": copied_total_bytes,
                "sourceAndPayloadByteIdentical": True,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
