#!/usr/bin/env python3
"""Prepare and independently verify M5 candidate inputs; never produce execution or Final authority."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys


SCHEMA = "NEREUS_V2_M5_CANDIDATE_INPUT_CLOSURE_V1"
REPOSITORIES = ("nereus", "kafka", "pulsar")
LAYOUT = {"nereus": "GITHUB/nereus", "kafka": "nereusstream/kafka", "pulsar": "nereusstream/pulsar"}
MAX_FILE_BYTES = 134_217_728
MAX_TOTAL_BYTES = 1_073_741_824
MAX_ENTRIES = 100_000
MAX_MANIFEST_BYTES = 16_777_216
SHA = re.compile(r"[0-9a-f]{64}")
COMMIT = re.compile(r"[0-9a-f]{40}")


class ClosureError(RuntimeError):
    pass


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def pairs(rows):
    value = {}
    for key, item in rows:
        if key in value:
            raise ClosureError(f"duplicate JSON member: {key}")
        value[key] = item
    return value


def load(raw: bytes) -> dict:
    if len(raw) > MAX_MANIFEST_BYTES:
        raise ClosureError("input manifest exceeds finite bound")
    value = json.loads(raw, object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise ClosureError("input manifest must be an object")
    return value


def git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(root), *args], stderr=subprocess.PIPE)


def relative(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if not value or str(path) != value or path.is_absolute() or any(part in {"..", ".git"} for part in path.parts):
        raise ClosureError(f"unsafe source path: {value}")
    return path


def regular(path: Path, maximum: int = MAX_FILE_BYTES) -> bytes:
    if not stat.S_ISREG(path.lstat().st_mode) or path.stat().st_size > maximum:
        raise ClosureError(f"input is not a bounded regular file: {path}")
    raw = path.read_bytes()
    if len(raw) > maximum:
        raise ClosureError(f"input grew beyond its bound: {path}")
    return raw


def node(root: Path, name: str) -> tuple[dict, bytes | None]:
    path = root / relative(name)
    if not path.exists() and not path.is_symlink():
        return {"deleted": True}, None
    info = path.lstat()
    if not path.resolve(strict=True).is_relative_to(root):
        raise ClosureError(f"source path escapes its repository: {name}")
    if stat.S_ISLNK(info.st_mode):
        target = os.readlink(path)
        if os.path.isabs(target) or not path.resolve(strict=True).is_relative_to(root):
            raise ClosureError(f"source symlink escapes its repository: {name}")
        raw = os.fsencode(target)
        mode = "120000"
    else:
        raw = regular(path)
        mode = "100755" if info.st_mode & 0o111 else "100644"
    return {"mode": mode, "bytes": len(raw), "sha256": digest(raw)}, raw


def source_index(root: Path) -> dict:
    root = root.resolve(strict=True)
    if git(root, "ls-files", "--unmerged", "-z"):
        raise ClosureError("candidate input repository has unresolved merge entries")
    tracked = set(filter(None, git(root, "ls-files", "--cached", "-z").decode().split("\0")))
    names = tracked | set(filter(None, git(root, "ls-files", "--others", "--exclude-standard", "-z").decode().split("\0")))
    result = {}
    for name in sorted(names):
        record, _ = node(root, name)
        if record.get("deleted") and name not in tracked:
            raise ClosureError(f"untracked input disappeared: {name}")
        result[name] = record
    if len(result) > MAX_ENTRIES:
        raise ClosureError("source entry inventory exceeds finite bound")
    # Every relative source link must resolve inside the captured file set, including ignored link targets.
    for name in list(result):
        if result[name].get("mode") == "120000":
            target = (root / name).resolve(strict=True).relative_to(root).as_posix()
            result.setdefault(target, node(root, target)[0])
    return dict(sorted(result.items()))


def repository(root: Path) -> dict:
    root = root.resolve(strict=True)
    if Path(git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve() != root:
        raise ClosureError("candidate source must be the complete Git worktree root")
    return {"baseCommit": git(root, "rev-parse", "HEAD").decode().strip(),
            "sourceRoot": str(root.resolve(strict=True)), "files": source_index(root)}


def extra_inputs(specs: list[tuple[str, Path]], roots: dict[str, Path]) -> tuple[dict, list]:
    result = {}
    manifests = []
    for owner, manifest in specs:
        manifest = manifest.resolve(strict=True)
        raw = regular(manifest, MAX_MANIFEST_BYTES)
        manifests.append({"repository": owner, "path": str(manifest)})
        rows = load(raw)
        declared = {str(manifest): digest(raw)}
        for name, expected in rows.items():
            if not isinstance(name, str) or not isinstance(expected, str) or not SHA.fullmatch(expected):
                raise ClosureError("dependency inventory must contain only path/SHA-256 pairs")
            candidate = Path(name) if Path(name).is_absolute() else roots[owner] / relative(name)
            declared[str(candidate.absolute())] = expected
        for name, expected in declared.items():
            candidate = Path(name)
            raw = regular(candidate)
            if digest(raw) != expected or name in result and result[name]["sha256"] != expected:
                raise ClosureError(f"declared input changed or conflicts: {name}")
            result[name] = {"bytes": len(raw), "sha256": expected}
    return dict(sorted(result.items())), sorted(manifests, key=lambda row: (row["repository"], row["path"]))


def validate_value(value: dict) -> None:
    if set(value) != {"schema", "kind", "promotionEligible", "executionEvidence", "repositories", "extraInputs", "inputManifests", "inputRootSha256"}:
        raise ClosureError("candidate closure members differ")
    if value["schema"] != SCHEMA or value["kind"] != "PREPARED_INPUTS_ONLY" or value["promotionEligible"] is not False or value["executionEvidence"] is not False:
        raise ClosureError("source preparation cannot claim execution, promotion or Final authority")
    if set(value["repositories"]) != set(REPOSITORIES):
        raise ClosureError("candidate must include all three source repositories")
    roots = [Path(repo["sourceRoot"]) for repo in value["repositories"].values()]
    if any(left == right or left.is_relative_to(right) or right.is_relative_to(left)
           for index, left in enumerate(roots) for right in roots[index + 1:]):
        raise ClosureError("source repositories must be distinct, nonoverlapping worktrees")
    total = 0
    entries = 0
    for name, repo in value["repositories"].items():
        if set(repo) != {"baseCommit", "sourceRoot", "files"} or not COMMIT.fullmatch(repo["baseCommit"]) or not Path(repo["sourceRoot"]).is_absolute():
            raise ClosureError(f"repository identity differs: {name}")
        for path, record in repo["files"].items():
            relative(path)
            entries += 1
            if record == {"deleted": True}:
                continue
            if set(record) != {"mode", "bytes", "sha256"} or record["mode"] not in {"100644", "100755", "120000"}:
                raise ClosureError("source node mode or members differ")
            total += validate_record(record)
    for path, record in value["extraInputs"].items():
        if not Path(path).is_absolute() or set(record) != {"bytes", "sha256"}:
            raise ClosureError("external input identity differs")
        total += validate_record(record)
        entries += 1
    if total > MAX_TOTAL_BYTES or entries > MAX_ENTRIES:
        raise ClosureError("complete input closure exceeds finite bounds")
    manifests = value["inputManifests"]
    if not isinstance(manifests, list) or not manifests:
        raise ClosureError("candidate requires explicit dependency input inventories")
    for row in manifests:
        if set(row) != {"repository", "path"} or row["repository"] not in REPOSITORIES or not Path(row["path"]).is_absolute():
            raise ClosureError("dependency manifest identity differs")
    if manifests != sorted(manifests, key=lambda row: (row["repository"], row["path"])) or len({row["path"] for row in manifests}) != len(manifests):
        raise ClosureError("dependency manifests repeat or have noncanonical order")
    content = {key: value[key] for key in ("repositories", "extraInputs", "inputManifests")}
    if value["inputRootSha256"] != digest(canonical(content)):
        raise ClosureError("candidate input root differs")


def validate_record(record: dict) -> int:
    if type(record["bytes"]) is not int or not 0 <= record["bytes"] <= MAX_FILE_BYTES or not isinstance(record["sha256"], str) or not SHA.fullmatch(record["sha256"]):
        raise ClosureError("input length or digest is invalid")
    return record["bytes"]


def write_new(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as output:
        output.write(raw)


def store(output: Path, raw: bytes) -> None:
    path = output / "objects" / digest(raw)
    if path.exists():
        if regular(path) != raw:
            raise ClosureError("captured object conflicts")
    else:
        write_new(path, raw)


def prepare(roots: dict[str, Path], specs: list[tuple[str, Path]], output: Path) -> dict:
    roots = {name: path.resolve(strict=True) for name, path in roots.items()}
    if set(roots) != set(REPOSITORIES):
        raise ClosureError("preparation requires all three source repositories")
    initial = {name: repository(roots[name]) for name in REPOSITORIES}
    extras, manifests = extra_inputs(specs, roots)
    value = {"schema": SCHEMA, "kind": "PREPARED_INPUTS_ONLY", "promotionEligible": False,
             "executionEvidence": False, "repositories": initial, "extraInputs": extras, "inputManifests": manifests,
             "inputRootSha256": digest(canonical({"repositories": initial, "extraInputs": extras, "inputManifests": manifests}))}
    validate_value(value)
    if output.exists() or output.is_symlink():
        raise ClosureError("candidate destination already exists; preparation never overwrites")
    for source in roots.values():
        if output.absolute().is_relative_to(source):
            ignored = subprocess.run(["git", "-C", str(source), "check-ignore", "-q", "--",
                                      str(output.absolute().relative_to(source))], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if ignored.returncode != 0:
                raise ClosureError("candidate output must be outside source inputs or in an ignored build directory")
    for parent in (output.parent, *output.parents):
        if parent.is_symlink():
            raise ClosureError("candidate parent is a symlink")
    output.mkdir(parents=True)
    for name in REPOSITORIES:
        source = roots[name]
        workspace = output / "workspace" / LAYOUT[name]
        workspace.parent.mkdir(parents=True, exist_ok=True)
        # Clone metadata/objects only; never checkout, reset, stage or modify the original repositories.
        subprocess.run(["git", "clone", "--local", "--no-checkout", str(source), str(workspace)], check=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        git(workspace, "read-tree", initial[name]["baseCommit"])
        if (workspace / ".git/objects/info/alternates").exists():
            raise ClosureError("candidate Git objects depend on an external alternate")
        for path, expected in initial[name]["files"].items():
            actual, raw = node(source, path)
            if actual != expected:
                raise ClosureError(f"source changed during preparation: {name}/{path}")
            if raw is None:
                continue
            store(output, raw)
            destination = workspace / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            if expected["mode"] == "120000":
                destination.symlink_to(os.fsdecode(raw))
            else:
                write_new(destination, raw)
                destination.chmod(0o755 if expected["mode"] == "100755" else 0o644)
    for path, record in extras.items():
        raw = regular(Path(path))
        if digest(raw) != record["sha256"]:
            raise ClosureError(f"dependency changed during preparation: {path}")
        store(output, raw)
        for name, source in roots.items():
            if Path(path).is_relative_to(source):
                relative_path = Path(path).relative_to(source).as_posix()
                if relative_path not in initial[name]["files"]:
                    write_new(output / "workspace" / LAYOUT[name] / relative_path, raw)
    if initial != {name: repository(roots[name]) for name in REPOSITORIES} or (extras, manifests) != extra_inputs(specs, roots):
        raise ClosureError("source cohort changed during preparation; incomplete candidate is retained")
    write_new(output / "input-closure.json", canonical(value))
    verify(output)
    return value


def verify(output: Path) -> dict:
    if output.is_symlink() or (output / "objects").is_symlink():
        raise ClosureError("candidate or object directory is a symlink")
    raw = regular(output / "input-closure.json", MAX_MANIFEST_BYTES)
    value = load(raw)
    validate_value(value)
    if canonical(value) != raw:
        raise ClosureError("closure manifest is not canonical JSON")
    records = list(value["extraInputs"].values())
    for repo in value["repositories"].values():
        records.extend(record for record in repo["files"].values() if "sha256" in record)
    expected_objects = {record["sha256"] for record in records}
    actual_objects = {path.name for path in (output / "objects").iterdir()}
    if actual_objects != expected_objects:
        raise ClosureError("source object inventory has missing or unregistered files")
    for record in records:
        data = regular(output / "objects" / record["sha256"])
        if len(data) != record["bytes"] or digest(data) != record["sha256"]:
            raise ClosureError("captured input bytes differ")
    referenced = set()
    for manifest in value["inputManifests"]:
        path = manifest["path"]
        if path not in value["extraInputs"]:
            raise ClosureError("dependency manifest object is missing")
        referenced.add(path)
        data = regular(output / "objects" / value["extraInputs"][path]["sha256"], MAX_MANIFEST_BYTES)
        for declared, expected in load(data).items():
            if not isinstance(expected, str) or not SHA.fullmatch(expected):
                raise ClosureError("captured dependency manifest contains a non-digest")
            candidate = Path(declared)
            if not candidate.is_absolute():
                candidate = Path(value["repositories"][manifest["repository"]]["sourceRoot"]) / relative(declared)
            name = str(candidate)
            if name not in value["extraInputs"] or value["extraInputs"][name]["sha256"] != expected:
                raise ClosureError("dependency manifest edge is missing or differs")
            referenced.add(name)
    if referenced != set(value["extraInputs"]):
        raise ClosureError("unreferenced dependency inputs exist")
    for name, expected in value["repositories"].items():
        workspace = output / "workspace" / LAYOUT[name]
        observed = repository(workspace)
        observed["sourceRoot"] = expected["sourceRoot"]
        if observed != expected:
            raise ClosureError(f"independent workspace input inventory differs: {name}")
        if (workspace / ".git/objects/info/alternates").exists():
            raise ClosureError("candidate has external Git object dependencies")
        for source, record in value["extraInputs"].items():
            original = Path(source)
            source_root = Path(expected["sourceRoot"])
            if original.is_relative_to(source_root):
                path = original.relative_to(source_root).as_posix()
                if path in expected["files"] and expected["files"][path].get("sha256") != record["sha256"]:
                    raise ClosureError("dependency inventory conflicts with source archive")
                data = regular(workspace / path)
                observed = {"bytes": len(data), "sha256": digest(data)}
                if observed != record:
                    raise ClosureError(f"materialized dependency input differs: logical={source}; "
                                       f"materialized={workspace / path}; expected={record}; observed={observed}")
    return value


def committed_tree(root: Path, tested: str) -> dict:
    tree = {}
    with subprocess.Popen(["git", "-C", str(root), "cat-file", "--batch"],
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as reader:
        for entry in filter(None, git(root, "ls-tree", "-rz", "--full-tree", tested).split(b"\0")):
            metadata, path = entry.split(b"\t", 1)
            mode, kind, oid = metadata.decode().split()
            if kind != "blob":
                raise ClosureError("candidate source cannot bind a submodule or non-blob node")
            reader.stdin.write((oid + "\n").encode())
            reader.stdin.flush()
            header = reader.stdout.readline().split()
            if len(header) != 3 or header[1] != b"blob" or int(header[2]) > MAX_FILE_BYTES:
                raise ClosureError("committed blob header or length differs")
            raw = reader.stdout.read(int(header[2]))
            if len(raw) != int(header[2]) or reader.stdout.read(1) != b"\n":
                raise ClosureError("committed blob is truncated")
            tree[path.decode()] = {"mode": mode, "bytes": len(raw), "sha256": digest(raw)}
        reader.stdin.close()
        if reader.wait() != 0:
            raise ClosureError("committed blob inventory failed")
    return tree


def bind(output: Path, roots: dict[str, Path], commits: dict[str, str]) -> dict:
    value = verify(output)
    if set(roots) != set(REPOSITORIES) or set(commits) != set(REPOSITORIES):
        raise ClosureError("commit equivalence requires all three exact source identities")
    for name in REPOSITORIES:
        source = roots[name]
        tested = commits[name]
        if not COMMIT.fullmatch(tested):
            raise ClosureError("commit binding requires full commit IDs")
        subprocess.run(["git", "-C", str(source), "merge-base", "--is-ancestor",
                        value["repositories"][name]["baseCommit"], tested], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        tree = committed_tree(source, tested)
        expected = {path: record for path, record in value["repositories"][name]["files"].items() if not record.get("deleted")}
        if tree != expected:
            raise ClosureError(f"committed source differs from executed input closure: {name}")
    return {"schema": "NEREUS_V2_M5_CANDIDATE_COMMIT_EQUIVALENCE_V1", "kind": "INPUT_EQUIVALENCE_ONLY",
            "promotionEligible": False, "executionEvidence": False, "inputRootSha256": value["inputRootSha256"],
            "commits": commits}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("prepare", "verify", "bind"))
    parser.add_argument("--candidate", type=Path, required=True)
    for name in REPOSITORIES:
        parser.add_argument(f"--{name}-root", type=Path)
        parser.add_argument(f"--{name}-commit")
    parser.add_argument("--input-manifest", action="append", default=[], metavar="REPOSITORY=PATH")
    parser.add_argument("--binding-output", type=Path)
    args = parser.parse_args(argv)
    try:
        roots = {name: getattr(args, name + "_root") for name in REPOSITORIES}
        if args.operation == "prepare":
            if any(path is None for path in roots.values()):
                raise ClosureError("preparation requires explicit source roots")
            specs = []
            for item in args.input_manifest:
                owner, path = item.split("=", 1)
                if owner not in REPOSITORIES:
                    raise ClosureError("input manifest repository is unknown")
                specs.append((owner, Path(path)))
            value = prepare(roots, specs, args.candidate)
        elif args.operation == "verify":
            value = verify(args.candidate)
        else:
            if any(path is None for path in roots.values()) or args.binding_output is None:
                raise ClosureError("binding requires explicit roots and a new output path")
            value = bind(args.candidate, roots, {name: getattr(args, name + "_commit") or "" for name in REPOSITORIES})
            write_new(args.binding_output, canonical(value))
    except (ClosureError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f"M5 candidate input closure: {error}", file=sys.stderr)
        return 1
    print(f"M5_INPUT_CLOSURE_ONLY root={value['inputRootSha256']} executionEvidence=false promotionEligible=false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
