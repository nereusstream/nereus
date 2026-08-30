#!/usr/bin/env python3
"""Archive one completed, promotion-validated allocator V5 selection campaign."""

from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat


SCHEMA = "NEREUS_V2_M3_ALLOCATOR_SELECTED_FORMAL_ARCHIVE_IDENTITY_V1"
SELECTED_CANDIDATES = {
    "STRICT_SELECTED": {"STRICT": "STRICT"},
    "RANGE_SELECTED": {
        "RANGE_16": "RANGE",
        "RANGE_64": "RANGE",
        "RANGE_256": "RANGE",
        "RANGE_1024": "RANGE",
    },
}
SELECTED_ORDINALS = {
    "STRICT": 1,
    "RANGE_16": 2,
    "RANGE_64": 3,
    "RANGE_256": 4,
    "RANGE_1024": 5,
}
PROMOTION_MEMBERS = {
    "schema",
    "status",
    "selectedCandidate",
    "checkpointSha256",
    "evaluationSha256",
    "diagnosticSha256",
    "diagnosticJUnitSha256",
    "diagnosticRawManifestSha256",
    "formalJUnitSha256",
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


def checked_relative_path(value: str, label: str) -> Path:
    pure = PurePosixPath(value)
    if pure.is_absolute() or not pure.parts or any(part in {"", ".", ".."} for part in pure.parts):
        raise ValueError(f"{label} is not a canonical relative path")
    return Path(*pure.parts)


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
    parser.add_argument("--promotion-decision-sha256", required=True)
    parser.add_argument("--selection-sha256", required=True)
    parser.add_argument("--formal-junit", required=True, type=Path)
    parser.add_argument("--formal-junit-sha256", required=True)
    parser.add_argument("--expected-file-count", required=True, type=int)
    parser.add_argument("--expected-total-bytes", required=True, type=int)
    parser.add_argument("--evaluation-status", choices=sorted(SELECTED_CANDIDATES), required=True)
    parser.add_argument("--selected-candidate", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_argument = args.source.expanduser()
    junit_argument = args.formal_junit.expanduser()
    if source_argument.is_symlink() or junit_argument.is_symlink():
        raise ValueError("archive source or formal JUnit is a symbolic link")
    source = source_argument.resolve(strict=True)
    archive = args.archive.expanduser().absolute()
    junit = junit_argument.resolve(strict=True)
    if not source.is_dir():
        raise ValueError("archive source is absent or not a directory")
    if not junit.is_file():
        raise ValueError("formal JUnit is absent or not a regular file")
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
        ("promotion-decision digest", args.promotion_decision_sha256),
        ("selection digest", args.selection_sha256),
        ("formal JUnit digest", args.formal_junit_sha256),
    ):
        require_hex(value, label)
    candidates = SELECTED_CANDIDATES[args.evaluation_status]
    if args.selected_candidate not in candidates:
        raise ValueError("selected candidate does not belong to the evaluation status")

    source_files = regular_files(source)
    source_total_bytes = sum(path.stat().st_size for path in source_files)
    if len(source_files) != args.expected_file_count or source_total_bytes != args.expected_total_bytes:
        raise ValueError("archive source file count or byte count differs")

    checkpoint_relative = checked_relative_path(
        args.final_checkpoint_relative_path, "final checkpoint relative path"
    )
    campaign_result = source / "campaign-result.json"
    evaluation = source / "evaluation.naev5"
    promotion = source / "promotion-decision.json"
    selection = source / "selection.nars5"
    final_checkpoint = source / checkpoint_relative
    for label, path, expected in (
        ("campaign-result", campaign_result, args.campaign_result_sha256),
        ("evaluation", evaluation, args.evaluation_sha256),
        ("final checkpoint", final_checkpoint, args.final_checkpoint_sha256),
        ("promotion-decision", promotion, args.promotion_decision_sha256),
        ("selection", selection, args.selection_sha256),
        ("formal JUnit", junit, args.formal_junit_sha256),
    ):
        if path.is_symlink() or not path.is_file() or sha256(path) != expected:
            raise ValueError(f"archive {label} bytes or digest differ")

    result_value = json.loads(campaign_result.read_text(encoding="utf-8"))
    if result_value.get("status") != "COMPLETED" or result_value.get("terminalReason") != "COMPLETED":
        raise ValueError("archive campaign-result is not terminal COMPLETED")
    promotion_value = json.loads(promotion.read_text(encoding="utf-8"))
    if not isinstance(promotion_value, dict) or set(promotion_value) != PROMOTION_MEMBERS:
        raise ValueError("archive promotion decision member inventory differs")
    expected_promotion = {
        "schema": "NEREUS_V2_M3_ALLOCATOR_PROMOTION_DECISION_V5",
        "status": "PROMOTABLE",
        "selectedCandidate": args.selected_candidate,
        "checkpointSha256": args.final_checkpoint_sha256,
        "evaluationSha256": args.evaluation_sha256,
        "formalJUnitSha256": args.formal_junit_sha256,
    }
    if any(promotion_value.get(key) != value for key, value in expected_promotion.items()):
        raise ValueError("archive promotion decision does not bind the selected formal inputs")
    selection_bytes = selection.read_bytes()
    if len(selection_bytes) != 500 or selection_bytes[:10] != b"NARS5\0\0\0\0\x05":
        raise ValueError("archive selection is not fixed-length NARS5")
    if selection_bytes[10] != SELECTED_ORDINALS[args.selected_candidate] or selection_bytes[11] != 0:
        raise ValueError("archive NARS5 selected candidate or reserved byte differs")
    if selection_bytes[12:52] != args.source_commit.encode("ascii"):
        raise ValueError("archive NARS5 source commit differs")
    selection_links = {
        "workload": selection_bytes[148:180].hex(),
        "executionProfile": selection_bytes[180:212].hex(),
        "plan": selection_bytes[212:244].hex(),
        "campaign": selection_bytes[244:276].hex(),
        "checkpointSha256": selection_bytes[276:308].hex(),
        "evaluationSha256": selection_bytes[308:340].hex(),
        "attachmentRootSha256": selection_bytes[340:372].hex(),
        "diagnosticSha256": selection_bytes[372:404].hex(),
        "diagnosticJUnitSha256": selection_bytes[404:436].hex(),
        "diagnosticRawManifestSha256": selection_bytes[436:468].hex(),
        "formalJUnitSummarySha256": selection_bytes[468:500].hex(),
    }
    if (
        selection_links["workload"] != args.plan_sha256
        or selection_links["plan"] != args.plan_sha256
        or selection_links["checkpointSha256"] != args.final_checkpoint_sha256
        or selection_links["evaluationSha256"] != args.evaluation_sha256
        or selection_links["attachmentRootSha256"] != args.attachment_root_sha256
        or selection_links["diagnosticSha256"] != promotion_value["diagnosticSha256"]
        or selection_links["diagnosticJUnitSha256"] != promotion_value["diagnosticJUnitSha256"]
        or selection_links["diagnosticRawManifestSha256"]
        != promotion_value["diagnosticRawManifestSha256"]
        or any(
            selection_links[name] == "0" * 64
            for name in ("executionProfile", "campaign", "formalJUnitSummarySha256")
        )
    ):
        raise ValueError("archive NARS5 fixed source/digest wire differs")

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
        "allocatorMode": candidates[args.selected_candidate],
        "nars5Present": True,
        "payloadFileCount": len(copied_files),
        "payloadTotalBytes": copied_total_bytes,
        "manifestSha256": sha256(manifest),
        "campaignResultSha256": args.campaign_result_sha256,
        "finalCheckpointRelativePath": checkpoint_relative.as_posix(),
        "finalCheckpointSha256": args.final_checkpoint_sha256,
        "evaluationSha256": args.evaluation_sha256,
        "attachmentRootSha256": args.attachment_root_sha256,
        "promotionDecisionSha256": args.promotion_decision_sha256,
        "selectionSha256": args.selection_sha256,
        "formalJUnitSha256": args.formal_junit_sha256,
        "sourceAndPayloadByteIdentical": True,
        "formalEvidenceImmutable": True,
        "promotionIntegrityValidated": True,
        "promotableInput": True,
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
