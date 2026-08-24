#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C
export LANG=C

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tested_commit="${1:-}"
pulsar_worktree="${2:-}"
output_directory="${3:-}"

fail() {
  echo "V2 M3 D1 local-cap evidence: $*" >&2
  exit 1
}

[[ "$tested_commit" =~ ^[0-9a-f]{40}$ ]] || fail "tested commit is not canonical"
[[ -n "$pulsar_worktree" && -f "$pulsar_worktree/settings.gradle.kts" && -f "$pulsar_worktree/.git" ]] ||
  fail "explicit dedicated Pulsar worktree is absent"
[[ -n "$output_directory" && "$output_directory" = /* ]] || fail "output directory must be absolute"
[[ "$output_directory" != "$repo_root"/* ]] || fail "output directory must remain outside the repository"
[[ ! -e "$output_directory" && ! -L "$output_directory" ]] || fail "output directory already exists"
[[ -d "$(dirname "$output_directory")" && ! -L "$(dirname "$output_directory")" ]] ||
  fail "output parent must be an existing non-symlink directory"

cd "$repo_root"
[[ "$(git rev-parse HEAD)" == "$tested_commit" ]] || fail "HEAD differs from tested commit"
[[ "$(git branch --show-current)" == "main" ]] || fail "formal D1 evidence requires main"
[[ "$(git rev-parse origin/main)" == "$tested_commit" ]] || fail "origin/main differs from tested commit"
[[ -z "$(git status --porcelain=v1 --untracked-files=all)" ]] || fail "Nereus worktree is not clean"
[[ "$(git -C "$pulsar_worktree" branch --show-current)" == "nereus/v2-m3-object-wal-evidence" ]] ||
  fail "Pulsar worktree is not the dedicated M3 branch"
[[ -z "$(git -C "$pulsar_worktree" status --porcelain=v1 --untracked-files=all)" ]] ||
  fail "Pulsar worktree is not clean"

harness_source="nereus-storage-object/src/main/java/com/nereusstream/storage/object/extent/ObjectWalLocalCapacityHarnessV1.java"
harness_test="nereus-storage-object/src/test/java/com/nereusstream/storage/object/extent/CheckedExtentAccountingTest.java"
component_sources=(
  "nereus-storage-object/src/main/java/com/nereusstream/storage/object/extent/ObjectWalFormatCaps.java"
  "nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ObjectReaderV1.java"
  "nereus-storage-object/src/main/java/com/nereusstream/storage/object/extent/CheckedExtentAccounting.java"
  "nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1EnvelopeV1.java"
  "nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ZstdV1.java"
  "nereus-storage-object/src/main/java/com/nereusstream/storage/object/extent/CheckedStreamingCounter.java"
)

git_source_sha() {
  local source_path="$1"
  git cat-file -e "$tested_commit:$source_path" || fail "exact source is absent: $source_path"
  git show "$tested_commit:$source_path" | shasum -a 256 | awk '{print $1}'
}

harness_sha="$(git_source_sha "$harness_source")"
harness_test_sha="$(git_source_sha "$harness_test")"
component_shas=()
for source_path in "${component_sources[@]}"; do
  component_shas+=("$(git_source_sha "$source_path")")
done

mkdir "$output_directory"
raw_result="$output_directory/local-cap-result.json"
sealed_junit="$output_directory/local-cap-junit-sealed.json"

gradle_properties=(
  "-PpulsarCheckout=$pulsar_worktree"
  "-Pv2M3LocalCapEvidenceOutput=$raw_result"
  "-Pv2M3LocalCapTestedCommit=$tested_commit"
  "-Pv2M3LocalCapHarnessSourceSha256=$harness_sha"
  "-Pv2M3LocalCapHarnessTestSourceSha256=$harness_test_sha"
)
for ordinal in 0 1 2 3 4 5; do
  gradle_properties+=("-Pv2M3LocalCapComponentSourceSha256$ordinal=${component_shas[$ordinal]}")
done

./gradlew --no-daemon --no-configuration-cache --rerun-tasks --console=plain \
  "${gradle_properties[@]}" \
  :nereus-storage-object:v2M3LocalCapEvidenceEmit

junit_relative="nereus-storage-object/build/test-results/v2M3LocalCapEvidenceTest/TEST-com.nereusstream.storage.object.extent.CheckedExtentAccountingTest.xml"
junit_xml="$repo_root/$junit_relative"
[[ -s "$raw_result" && -s "$junit_xml" ]] || fail "raw result or governed JUnit XML is absent"

python3 - "$junit_xml" <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
actual = {key: int(root.attrib[key]) for key in ("tests", "failures", "errors", "skipped")}
expected = {"tests": 6, "failures": 0, "errors": 0, "skipped": 0}
if actual != expected or len(root.findall("testcase")) != 6:
    raise SystemExit(f"V2 M3 D1 local-cap evidence: governed JUnit differs: {actual}")
PY

python3 scripts/publish-v2-m3-child.py \
  --repo-root "$repo_root" \
  --tested-commit "$tested_commit" \
  --seal-junit-kind D_LOCAL_CAP \
  --child-junit-xml "$junit_relative=$junit_xml" \
  --sealed-output "$sealed_junit"

python3 - "$repo_root" "$tested_commit" "$raw_result" <<'PY'
import importlib.util
from pathlib import Path
import sys

root = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("m3_child_d1_gate", root / "scripts/check-v2-m3-child.py")
if spec is None or spec.loader is None:
    raise SystemExit("V2 M3 D1 local-cap evidence: cannot load governed validator")
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
raw = Path(sys.argv[3]).read_bytes()
value = module.load_canonical_json(raw, "D1 local-cap result")
module.validate_local_cap_result(value, root, sys.argv[2])
PY

[[ "$(git rev-parse HEAD)" == "$tested_commit" && "$(git rev-parse origin/main)" == "$tested_commit" ]] ||
  fail "Nereus source changed during D1 evidence"
[[ -z "$(git status --porcelain=v1 --untracked-files=all)" ]] || fail "D1 evidence changed the worktree"
shasum -a 256 "$raw_result" "$sealed_junit"
