#!/usr/bin/env python3
"""Freeze the M6 source tuple and separate byte inventories; no execution or PASS authority."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
from pathlib import Path
import stat
import subprocess
import sys


spec = importlib.util.spec_from_file_location(
    "m6_input_primitives", Path(__file__).with_name("m5-candidate-inputs.py")
)
INPUTS = importlib.util.module_from_spec(spec)
spec.loader.exec_module(INPUTS)

REPOSITORIES = INPUTS.REPOSITORIES
SCHEMA = "NEREUS_V2_M6_EXECUTION_INPUTS_V1"
HISTORICAL = "docs/v2/evidence/"
# The Batch 0 tree preserves the published M0–M5 archive bytes, including their original
# receipts and provenance. This archive identity is separate from the tested commits inside it.
ARCHIVE_COMMIT = "704dc5a7134425d15bb8a7fdeb9cda757a01711b"
ARCHIVE_BUDGET = 5 * 1024**3
SOURCE_BUDGET = 1024**3


class InputError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise InputError(message)


def git(root, *args):
    # Do not let caller-supplied repository overrides, fsmonitor or replacement objects
    # select a different source than the complete Git worktree being attested.
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env["GIT_NO_REPLACE_OBJECTS"] = "1"
    return subprocess.check_output(
        ["git", "-C", str(root), "-c", "core.fsmonitor=false", *args], env=env
    )


def revision(root, rev):
    value = git(root, "rev-parse", "--verify", rev).decode().strip()
    require(INPUTS.COMMIT.fullmatch(value), f"not a complete Git object ID: {rev}")
    return value


def tree(root, commit, prefix=None):
    args = ["ls-tree", "-rlz", "--full-tree", commit]
    if prefix is not None:
        args.extend(("--", prefix))
    rows = {}
    for entry in git(root, *args).split(b"\0"):
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        mode, kind, blob, size = metadata.decode().split()
        path = name.decode()
        INPUTS.relative(path)
        require(
            kind == "blob" and mode in {"100644", "100755", "120000"} and INPUTS.COMMIT.fullmatch(blob),
            f"unsupported tracked node at {root}/{path}: {mode} {kind}",
        )
        rows[path] = {"mode": mode, "blob": blob, "bytes": int(size)}
    return rows


def outside(path, roots):
    return all(not path.is_relative_to(root) and not root.is_relative_to(path) for root in roots)


def file_identity(path, git_node=None):
    require(path == path.resolve(strict=True), f"input path contains a symlink: {path}")
    require(stat.S_ISREG(path.lstat().st_mode), f"input is not a regular file: {path}")
    with path.open("rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode), f"input is not a regular file: {path}")
        mode = "100755" if info.st_mode & 0o111 else "100644"
        sha = hashlib.sha256()
        if git_node is not None:
            require(git_node["mode"] == mode and git_node["bytes"] == info.st_size,
                    f"checkout mode or size differs from Git: {path}")
            blob = hashlib.sha1(f"blob {info.st_size}\0".encode())
        size = 0
        while block := stream.read(1024 * 1024):
            size += len(block)
            require(size <= info.st_size, f"input grew during capture: {path}")
            sha.update(block)
            if git_node is not None:
                blob.update(block)
        after = os.fstat(stream.fileno())
        require(size == info.st_size and (info.st_mtime_ns, info.st_ctime_ns)
                == (after.st_mtime_ns, after.st_ctime_ns), f"input changed during capture: {path}")
        if git_node is not None:
            require(git_node["blob"] == blob.hexdigest(), f"checkout bytes differ from Git: {path}")
    return {"mode": mode, "bytes": size, "sha256": sha.hexdigest()}


def source_identity(root, path, node, eligible):
    if node["mode"] != "120000":
        return file_identity(root / path, node)
    link = root / path
    require(link.parent == link.parent.resolve(strict=True), f"source link has a symlink parent: {link}")
    raw = os.fsencode(link.readlink())
    blob = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
    require(not Path(os.fsdecode(raw)).is_absolute() and len(raw) == node["bytes"] and blob == node["blob"],
            f"source link differs from Git or is absolute: {link}")
    target = link.resolve(strict=True)
    require(target.is_relative_to(root) and target.relative_to(root).as_posix() in eligible,
            f"source link points outside the declared inputs: {link}")
    return {"mode": node["mode"], "bytes": len(raw), "sha256": INPUTS.digest(raw)}


def clean_commit(root):
    require(Path(git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve() == root,
            f"not a complete repository root: {root}")
    require(not git(root, "status", "--porcelain=v1", "-z", "--untracked-files=all"),
            f"source checkout is dirty: {root}")
    return revision(root, "HEAD^{commit}")


def capture(roots, output, historical_paths, runtime, runtime_budget):
    """Select historical paths by name; all other archived paths retain version identity only.

    A caller must declare the files its gate reads. An executor must restrict its inputs to
    this closure or independently verify the actual build/test file graph against it.
    """
    require(set(roots) == set(REPOSITORIES), "expected Nereus, Kafka and Pulsar source roots")
    roots = {key: root.resolve(strict=True) for key, root in roots.items()}
    for key, root in roots.items():
        require(outside(root, [p for name, p in roots.items() if name != key]),
                "repository roots overlap")
    output = output.absolute()
    require(output == output.resolve() and outside(output, roots.values()),
            "evidence output must be outside every source root and contain no symlink")
    require(type(runtime_budget) is int and runtime_budget > 0, "invalid runtime byte budget")
    require(historical_paths is None or len(set(historical_paths)) == len(historical_paths),
            "duplicate historical input path")
    versions = {}
    source = {}
    historical = {}
    source_bytes = archive_bytes = 0
    for key, root in roots.items():
        commit = clean_commit(root)
        files = tree(root, commit)
        versions[key] = {"commit": commit, "tree": revision(root, f"{commit}^{{tree}}"), "files": files}
        archive = {}
        if key == "nereus":
            try:
                git(root, "merge-base", "--is-ancestor", ARCHIVE_COMMIT, commit)
            except subprocess.CalledProcessError as error:
                raise InputError("source is not descended from the published archive") from error
            archive = tree(root, ARCHIVE_COMMIT, HISTORICAL)
            require({path: node for path, node in files.items() if path.startswith(HISTORICAL)} == archive,
                    "tracked evidence differs from the published historical archive")
        # A gate such as the existing documentation aggregate can read the full, explicitly
        # inventoried historical archive. A runtime-only stage uses a narrower exact selection.
        selected = set(archive if historical_paths is None else historical_paths) if key == "nereus" else set()
        require(selected <= archive.keys(), "historical input is absent from its immutable archive origin")
        eligible = (files.keys() - archive.keys()) | selected
        current = source[key] = {}
        for path, node in files.items():
            if path in archive:
                if path in selected:
                    archive_bytes += node["bytes"]
                    require(archive_bytes <= ARCHIVE_BUDGET, "historical execution inputs exceed their byte budget")
                    historical[path] = source_identity(root, path, node, eligible)
                    node["classification"] = "HISTORICAL_INPUT"
                else:
                    node["classification"] = "RETAINED_ARCHIVE"
            else:
                source_bytes += node["bytes"]
                require(source_bytes <= SOURCE_BUDGET, "source execution inputs exceed their byte budget")
                current[path] = source_identity(root, path, node, eligible)
                node["classification"] = "SOURCE_INPUT"
        require(clean_commit(root) == commit, f"source commit changed during capture: {key}")
    externals = {}
    runtime_bytes = 0
    for label, path in sorted(runtime.items()):
        require(bool(label) and path.is_absolute() and outside(path, [output, *roots.values()]),
                f"runtime dependency overlaps source or evidence output: {label}")
        runtime_bytes += path.lstat().st_size
        require(runtime_bytes <= runtime_budget, "runtime inputs exceed their byte budget")
        externals[label] = {"path": str(path), **file_identity(path)}
    content = {
        "schema": SCHEMA,
        "sourceIdentity": versions,
        "sourceInputs": source,
        "historicalInputs": {"archiveCommit": ARCHIVE_COMMIT, "files": historical},
        "runtimeInputs": externals,
        "budgets": {"sourceBytes": SOURCE_BUDGET, "historicalBytes": ARCHIVE_BUDGET,
                    "runtimeBytes": runtime_budget},
    }
    return {**content, "inputRootSha256": INPUTS.digest(INPUTS.canonical(content))}


def runtime_manifest(manifest):
    raw = INPUTS.load(INPUTS.regular(manifest, INPUTS.MAX_MANIFEST_BYTES))
    require(raw and all(isinstance(node, dict) and set(node) == {"path", "sha256"}
                        and isinstance(node["path"], str) and isinstance(node["sha256"], str)
                        and INPUTS.SHA.fullmatch(node["sha256"]) for node in raw.values()),
            "runtime declaration must bind dependency files by path and SHA-256")
    require("runtime-declaration" not in raw, "reserved runtime dependency label")
    runtime = {**{label: Path(node["path"]) for label, node in raw.items()}, "runtime-declaration": manifest}
    return runtime, {label: node["sha256"] for label, node in raw.items()}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("capture", "verify"))
    for key in REPOSITORIES:
        parser.add_argument(f"--{key}", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runtime-manifest", type=Path, required=True)
    parser.add_argument("--runtime-byte-budget", type=int, required=True)
    parser.add_argument("--historical-path", action="append", default=[])
    parser.add_argument("--full-historical-archive", action="store_true")
    args = parser.parse_args(argv)
    try:
        output = args.output.absolute()
        roots = {key: getattr(args, key) for key in REPOSITORIES}
        manifest = args.runtime_manifest.absolute()
        require(outside(manifest.resolve(strict=True), [output.resolve()]),
                "the runtime declaration cannot be an output of its own stage")
        runtime, hashes = runtime_manifest(manifest)
        require(not args.full_historical_archive or not args.historical_path,
                "choose exact historical paths or the full archive")
        captured = capture(roots, output, None if args.full_historical_archive else args.historical_path,
                           runtime, args.runtime_byte_budget)
        require(all(captured["runtimeInputs"][label]["sha256"] == expected for label, expected in hashes.items()),
                "runtime dependency differs from its declared SHA-256")
        data = INPUTS.canonical(captured)
        record = output / "execution-inputs.json"
        if args.operation == "capture":
            output.mkdir(parents=True, exist_ok=False)
            with record.open("xb") as stream:
                stream.write(data)
        else:
            require(record.is_file() and not record.is_symlink() and record.read_bytes() == data,
                    "source, historical or runtime inputs changed after capture")
        print(captured["inputRootSha256"])
    except (InputError, INPUTS.ClosureError, ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"M6 execution inputs: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
