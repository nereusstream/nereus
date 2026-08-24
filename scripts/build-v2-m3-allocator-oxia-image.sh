#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C
export LANG=C

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
oxia_checkout="${1:-/Users/liusinan/apps/ideaproject/nereusstream/oxia-worktrees/nereus-v2-m3}"
image_reference="nereus/oxia-m3-allocator:37a17bef1720"
expected_source="37a17bef17202d5fd6e23282da5fd26d94865484"
expected_branch="nereus/v2-m3-object-wal-evidence"
expected_published_ref="refs/heads/main"
expected_version="0.16.3-167-g37a17bef"
expected_source_epoch="1786412361"
expected_image_id="sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da"

test "$(git -C "$oxia_checkout" rev-parse HEAD)" = "$expected_source"
test "$(git -C "$oxia_checkout" branch --show-current)" = "$expected_branch"
test "$(git -C "$oxia_checkout" rev-parse refs/remotes/origin/main)" = "$expected_source"
test "$(git -C "$oxia_checkout" ls-remote --heads origin "$expected_published_ref" | awk 'NR == 1 {print $1}')" = \
  "$expected_source"
test -z "$(git -C "$oxia_checkout" status --porcelain --untracked-files=all)"
test "$(git -C "$oxia_checkout" show -s --format=%ct "$expected_source")" = "$expected_source_epoch"

docker build \
  --file "$repo_root/scripts/containers/oxia-v2-m3-allocator.Dockerfile" \
  --build-arg SOURCE_DATE_EPOCH="$expected_source_epoch" \
  --build-arg OXIA_VERSION="$expected_version" \
  --build-arg OXIA_SOURCE_COMMIT="$expected_source" \
  --tag "$image_reference" \
  "$oxia_checkout"

observed_image_id="$(docker image inspect "$image_reference" --format '{{.Id}}')"
test "$observed_image_id" = "$expected_image_id"
test "$(docker image inspect "$image_reference" --format '{{.Os}}/{{.Architecture}}')" = "linux/arm64"
test "$(docker image inspect "$image_reference" \
  --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = "$expected_source"
test "$(docker image inspect "$image_reference" \
  --format '{{index .Config.Labels "com.nereusstream.evidence.recipe"}}')" = \
  "NEREUS_V2_M3_ALLOCATOR_OXIA_IMAGE_V1"
test "$(docker run --rm "$image_reference" /oxia/bin/oxia --version)" = "oxia version $expected_version"
test -z "$(git -C "$oxia_checkout" status --porcelain --untracked-files=all)"

printf '%s %s\n' "$image_reference" "$observed_image_id"
