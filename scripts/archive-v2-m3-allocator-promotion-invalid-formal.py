#!/usr/bin/env python3
"""Archive a completed allocator formal attempt rejected by its promotion integrity gate."""

from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat


SCHEMA = "NEREUS_V2_M3_ALLOCATOR_PROMOTION_INVALID_FORMAL_ARCHIVE_IDENTITY_V1"
PROMOTION_FAILURE_CODE = "PHYSICAL_ATTACHMENT_SIZE_CAP"
LEGACY_ATTACHMENT_CAP_BYTES = 16 * 1024 * 1024
V5_ATTACHMENT_CAP_BYTES = 32 * 1024 * 1024
SELECTED_CANDIDATES = {
    "STRICT_SELECTED": {"STRICT"},
    "RANGE_SELECTED": {"RANGE_16", "RANGE_64", "RANGE_256", "RANGE_1024"},
}


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
    parser.add_argument("--archived-on", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--campaign-result-sha256", required=True)
    parser.add_argument("--final-checkpoint-relative-path", required=True)
    parser.add_argument("--final-checkpoint-sha256", required=True)
    parser.add_argument("--evaluation-sha256", required=True)
    parser.add_argument("--attachment-root-sha256", required=True)
    parser.add_argument("--formal-junit", required=True, type=Path)
    parser.add_argument("--formal-junit-sha256", required=True)
    parser.add_argument("--expected-file-count", required=True, type=int)
    parser.add_argument("--expected-total-bytes", required=True, type=int)
    parser.add_argument("--evaluation-status", choices=sorted(SELECTED_CANDIDATES), required=True)
    parser.add_argument("--selected-candidate", required=True)
    parser.add_argument("--failed-attachment-relative-path", required=True)
    parser.add_argument("--failed-attachment-sha256", required=True)
    parser.add_argument("--failed-attachment-bytes", required=True, type=int)
    parser.add_argument("--promotion-failure-code", required=True)
    parser.add_argument("--promotion-failure-detail", required=True)
    return parser.parse_args()


def checked_relative_path(value: str, label: str) -> Path:
    pure = PurePosixPath(value)
    if pure.is_absolute() or not pure.parts or any(part in {"", ".", ".."} for part in pure.parts):
        raise ValueError(f"{label} is not a canonical relative path")
    return Path(*pure.parts)


def main() -> int:
    args = parse_args()
    source = args.source.expanduser().resolve(strict=True)
    archive = args.archive.expanduser().absolute()
    junit = args.formal_junit.expanduser().resolve(strict=True)
    if source.is_symlink() or not source.is_dir():
        raise ValueError("archive source is absent, non-directory, or a symbolic link")
    if junit.is_symlink() or not junit.is_file():
        raise ValueError("formal JUnit is absent, non-regular, or a symbolic link")
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
        ("failed attachment digest", args.failed_attachment_sha256),
    ):
        require_hex(value, label)
    if args.selected_candidate not in SELECTED_CANDIDATES[args.evaluation_status]:
        raise ValueError("selected candidate does not belong to the evaluation status")
    if args.promotion_failure_code != PROMOTION_FAILURE_CODE:
        raise ValueError("promotion failure code is not the closed attachment-cap failure")
    if not args.promotion_failure_detail.strip():
        raise ValueError("promotion failure detail is empty")

    source_files = regular_files(source)
    source_total_bytes = sum(path.stat().st_size for path in source_files)
    if len(source_files) != args.expected_file_count or source_total_bytes != args.expected_total_bytes:
        raise ValueError("archive source file count or byte count differs")

    checkpoint_relative = checked_relative_path(
        args.final_checkpoint_relative_path, "final checkpoint relative path"
    )
    failed_relative = checked_relative_path(
        args.failed_attachment_relative_path, "failed attachment relative path"
    )
    campaign_result = source / "campaign-result.json"
    evaluation = source / "evaluation.naev5"
    final_checkpoint = source / checkpoint_relative
    failed_attachment = source / failed_relative
    if failed_relative.parts[0] != "actions" or failed_attachment.suffix != ".nare1":
        raise ValueError("failed attachment is not a V5 physical action attachment")
    if sha256(campaign_result) != args.campaign_result_sha256:
        raise ValueError("archive campaign-result digest differs")
    if sha256(evaluation) != args.evaluation_sha256:
        raise ValueError("archive evaluation digest differs")
    if sha256(final_checkpoint) != args.final_checkpoint_sha256:
        raise ValueError("archive final checkpoint digest differs")
    if sha256(junit) != args.formal_junit_sha256:
        raise ValueError("archive formal JUnit digest differs")
    if sha256(failed_attachment) != args.failed_attachment_sha256:
        raise ValueError("archive failed attachment digest differs")
    if failed_attachment.stat().st_size != args.failed_attachment_bytes:
        raise ValueError("archive failed attachment byte count differs")
    if not LEGACY_ATTACHMENT_CAP_BYTES < args.failed_attachment_bytes <= V5_ATTACHMENT_CAP_BYTES:
        raise ValueError("archive failed attachment is outside the V5 bounded-cap correction interval")
    if args.failed_attachment_sha256 not in failed_attachment.name:
        raise ValueError("archive failed attachment filename does not bind its digest")

    result_value = json.loads(campaign_result.read_text(encoding="utf-8"))
    if result_value.get("status") != "COMPLETED" or result_value.get("terminalReason") != "COMPLETED":
        raise ValueError("archive campaign-result is not terminal COMPLETED")
    if result_value.get("selectionCreated") is not False:
        raise ValueError("promotion-invalid campaign-result unexpectedly reports a selection")
    for forbidden in (source / "promotion-decision.json", source / "selection.nars5"):
        if forbidden.exists() or forbidden.is_symlink():
            raise ValueError("promotion-invalid archive contains promotion authority or selection")

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
        digest = sha256(source_file)
        if destination.stat().st_size != source_file.stat().st_size or sha256(destination) != digest:
            raise ValueError(f"archive payload copy differs: {relative.as_posix()}")
        copied_total_bytes += destination.stat().st_size
        manifest_rows.append(f"{digest}  {relative.as_posix()}\n")
        destination.chmod(0o444)

    junit_copy = archive / "formal-junit.xml"
    with junit.open("rb") as source_stream, junit_copy.open("xb") as destination_stream:
        shutil.copyfileobj(source_stream, destination_stream, 1024 * 1024)
        destination_stream.flush()
        os.fsync(destination_stream.fileno())
    if sha256(junit_copy) != args.formal_junit_sha256:
        raise ValueError("archive formal JUnit copy differs")
    junit_copy.chmod(0o444)

    copied_files = regular_files(payload)
    if len(copied_files) != len(source_files) or copied_total_bytes != source_total_bytes:
        raise ValueError("archive payload inventory differs after copy")
    for directory in sorted((path for path in payload.rglob("*") if path.is_dir()), reverse=True):
        directory.chmod(0o555)
    payload.chmod(0o555)

    manifest = archive / "SHA256SUMS"
    write_new(manifest, "".join(manifest_rows).encode("utf-8"))
    identity = {
        "schema": SCHEMA,
        "protocolVersion": 5,
        "archivedOn": args.archived_on,
        "archivePath": str(archive),
        "payloadPath": str(payload),
        "sourcePath": str(source),
        "sourceCommit": args.source_commit,
        "planSha256": args.plan_sha256,
        "campaignStatus": "COMPLETED",
        "evaluationStatus": args.evaluation_status,
        "selectedCandidate": args.selected_candidate,
        "selectionEligible": True,
        "allocatorMode": "UNSELECTED",
        "nars5Present": False,
        "payloadFileCount": len(copied_files),
        "payloadTotalBytes": copied_total_bytes,
        "manifestSha256": sha256(manifest),
        "campaignResultSha256": args.campaign_result_sha256,
        "finalCheckpointRelativePath": checkpoint_relative.as_posix(),
        "finalCheckpointSha256": args.final_checkpoint_sha256,
        "evaluationSha256": args.evaluation_sha256,
        "attachmentRootSha256": args.attachment_root_sha256,
        "formalJUnitSha256": args.formal_junit_sha256,
        "promotionFailureCode": args.promotion_failure_code,
        "promotionFailureDetail": args.promotion_failure_detail,
        "promotionFailureDetailSha256": hashlib.sha256(
            args.promotion_failure_detail.encode("utf-8")
        ).hexdigest(),
        "failedAttachmentRelativePath": failed_relative.as_posix(),
        "failedAttachmentSha256": args.failed_attachment_sha256,
        "failedAttachmentBytes": args.failed_attachment_bytes,
        "legacyAttachmentCapBytes": LEGACY_ATTACHMENT_CAP_BYTES,
        "correctedV5AttachmentCapBytes": V5_ATTACHMENT_CAP_BYTES,
        "sourceAndPayloadByteIdentical": True,
        "formalEvidenceImmutable": True,
        "promotionIntegrityValidated": False,
        "promotableInput": False,
        "futureCampaignInput": False,
    }
    identity_path = archive / "archive-identity.json"
    write_new(identity_path, (json.dumps(identity, indent=2, ensure_ascii=True) + "\n").encode("utf-8"))
    archive.chmod(0o555)
    print(json.dumps({
        "archivePath": str(archive),
        "identitySha256": sha256(identity_path),
        "manifestSha256": sha256(manifest),
        "payloadFileCount": len(copied_files),
        "payloadTotalBytes": copied_total_bytes,
        "sourceAndPayloadByteIdentical": True,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
