"""Check the file graph of the observed Gradle invocations against a frozen source tuple."""

from collections import defaultdict
import hashlib
import json
from pathlib import Path
import zipfile


def identity(row):
    return row["bytes"], row["sha256"]


def known_files(frozen, roots):
    source = {
        str(roots[repo] / path): identity(row)
        for repo, inventory in frozen["sourceInputs"].items()
        for path, row in inventory.items()
    }
    source.update({str(roots["nereus"] / path): identity(row)
                   for path, row in frozen["historicalInputs"]["files"].items()})
    source.update({row["path"]: identity(row) for row in frozen["runtimeInputs"].values()})
    return source


def archive_entry(archive, path, wanted, matches):
    """A transform may depend on the precise bytes of one entry in its captured archive."""
    if not archive.is_file() or not matches(archive):
        return False
    try:
        with zipfile.ZipFile(archive) as zip_file:
            items = [item for item in zip_file.infolist() if item.filename == path]
            if len(items) != 1 or items[0].file_size != wanted[0]:
                return False
            with zip_file.open(items[0]) as content:
                return hashlib.file_digest(content, "sha256").hexdigest() == wanted[1]
    except zipfile.BadZipFile:
        return False


def generated_input(name, wanted, row, matches):
    # Gradle's Jar task includes its generated manifest in the input collection.
    # The task must run and place the same manifest into a produced archive.
    if row.get("jarManifest") == name and row["produced"]:
        return any(archive_entry(Path(archive), "META-INF/MANIFEST.MF", wanted, matches)
                   for archive in row["outputs"] if archive.endswith(".jar"))
    # ProtobufExtract exposes both its source archives and the expanded proto entries.
    prefix = "/build/tmp/.cache/expanded/zip_"
    if prefix not in name or not name.endswith(".proto"):
        return False
    _, separator, member = name.split(prefix, 1)[1].partition("/")
    return bool(separator) and any(archive_entry(Path(archive), member, wanted, matches)
                                   for archive in row.get("protoSources", ()) if archive.endswith((".jar", ".zip")))


def cached_publication(path, wanted, repository, published):
    if repository is None or "files-2.1" not in path.parts:
        return False
    coordinates = path.parts[path.parts.index("files-2.1") + 1:]
    if len(coordinates) != 5:
        return False
    group, module, version, _, filename = coordinates
    if group != "com.nereusstream" or version != "0.2.0-SNAPSHOT":
        return False
    directory = repository / "com/nereusstream" / module / version
    suffix = Path(filename).suffix
    return wanted in published.get((directory, suffix), ())


def verify(traces, frozen, roots, output, file_identity, require, repositories=None):
    immutable = known_files(frozen, roots)
    generated = {}
    published = defaultdict(set)
    results = {}
    current_archive = {}

    def matches(archive):
        if archive not in current_archive:
            current_archive[archive] = identity(file_identity(archive))
        return current_archive[archive] == (archive_outputs.get(str(archive)) or immutable.get(str(archive))
                                           or generated.get(str(archive)))

    def produced_path(path):
        return path.is_relative_to(output) or any(path.is_relative_to(root)
                and "build" in path.relative_to(root).parts[:-1] for root in roots.values())

    for trace in traces:
        captured = file_identity(trace)
        repository = None if repositories is None else repositories.get(trace)
        tasks = 0
        builds = set()
        with trace.open(encoding="utf-8") as stream:
            for line in stream:
                row = json.loads(line)
                require(row["event"] in {"gradle", "task"}, f"unexpected Gradle capture event: {trace}")
                build = Path(row["build"])
                require(any(build.is_relative_to(root) for root in roots.values()), f"unknown Gradle build: {build}")
                inputs = row["files"] if row["event"] == "gradle" else row["inputs"]
                archive_outputs = {name: identity(value) for name, value in row.get("outputs", {}).items()
                                   if value != {"absent": True}}
                for name, value in inputs.items():
                    path = Path(name)
                    require(path.is_absolute() and path == path.resolve(), f"redirected Gradle input: {name}")
                    if value == {"absent": True}:
                        require(name not in immutable, f"missing frozen Gradle input: {name}")
                        continue
                    actual = identity(value)
                    declared = immutable.get(name)
                    require(declared == actual if declared is not None else generated.get(name) == actual
                            or cached_publication(path, actual, repository, published)
                            or generated_input(name, actual, row, matches),
                            f"Gradle consumed an unbound file in {row.get('task', build)}: {name}")
                if row["event"] == "gradle":
                    require(build not in builds, f"duplicate Gradle build capture: {build}")
                    builds.add(build)
                else:
                    tasks += 1
                    if row["produced"]:
                        for name, value in archive_outputs.items():
                            path = Path(name)
                            require(produced_path(path) and name not in immutable,
                                    f"unexpected Gradle output path: {name}")
                            generated[name] = value
                            if path.is_relative_to(output) and path.suffix in (".jar", ".pom", ".module"):
                                published[path.parent, path.suffix].add(value)
                        # Paths can be reused by a later Gradle invocation and now contain new bytes.
                        current_archive.clear()
        require(tasks and builds, f"incomplete Gradle capture: {trace}")
        require(captured == file_identity(trace), f"Gradle capture changed during verification: {trace}")
        results[trace.relative_to(output).as_posix()] = {
            **captured, "tasks": tasks, "builds": sorted(str(build) for build in builds),
        }
    return results
