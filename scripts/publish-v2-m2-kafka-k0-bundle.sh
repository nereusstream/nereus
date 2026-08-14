#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M2 Kafka K0 bundle publication: $*" >&2
    exit 1
}

[[ "$(git branch --show-current)" == "main" ]] || fail "publication must run from main"
[[ -z "$(git status --porcelain)" ]] || fail "source worktree must be clean"

source_commit="$(git rev-parse HEAD)"
[[ "$source_commit" =~ ^[0-9a-f]{40}$ ]] || fail "invalid source commit $source_commit"
[[ "$(git rev-parse origin/main)" == "$source_commit" ]] || fail "source commit must already be pushed"

development_version="$(sed -n 's/^nereusVersion=//p' gradle.properties)"
[[ "$development_version" =~ ^([0-9]+\.[0-9]+\.[0-9]+)-SNAPSHOT$ ]] ||
    fail "expected X.Y.Z-SNAPSHOT, found $development_version"
coordinate_version="${BASH_REMATCH[1]}-m2.${source_commit}"
n1_commit="$(jq -er '.n1ArtifactBinding.sourceCommit' docs/v2/source-locks.json)"
n1_version="$(jq -er '.n1ArtifactBinding.coordinateVersion' docs/v2/source-locks.json)"
[[ "$n1_version" == "${BASH_REMATCH[1]}-n1.${n1_commit}" ]] || fail "N1 source/coordinate mismatch"

bundle_parent="$repo_root/gradle/locked-artifacts/nereus-m2"
bundle_root="$bundle_parent/$source_commit"
[[ ! -e "$bundle_root" ]] || fail "refusing to overwrite immutable bundle $bundle_root"
mkdir -p "$bundle_parent"

stage_one="$(mktemp -d "$bundle_parent/.${source_commit}.first.XXXXXX")"
stage_two="$(mktemp -d "$bundle_parent/.${source_commit}.second.XXXXXX")"
cleanup() {
    [[ -z "${stage_one:-}" ]] || rm -rf "$stage_one"
    [[ -z "${stage_two:-}" ]] || rm -rf "$stage_two"
}
trap cleanup EXIT

modules=(nereus-storage-api nereus-storage-bookkeeper nereus-kafka-bookkeeper)
build_once() {
    local destination="$1"
    local tasks=()
    local module
    for module in "${modules[@]}"; do
        tasks+=(
            ":$module:clean"
            ":$module:jar"
            ":$module:sourcesJar"
            ":$module:generatePomFileForMavenJavaPublication"
            ":$module:generateMetadataFileForMavenJavaPublication"
        )
    done
    ./gradlew -PnereusVersion="$coordinate_version" "${tasks[@]}" \
        --rerun-tasks --no-daemon --console=plain

    for module in "${modules[@]}"; do
        local artifact_dir="$destination/m2/com/nereusstream/$module/$coordinate_version"
        mkdir -p "$artifact_dir"
        install -m 0644 "$module/build/libs/$module-$coordinate_version.jar" \
            "$artifact_dir/$module-$coordinate_version.jar"
        install -m 0644 "$module/build/libs/$module-$coordinate_version-sources.jar" \
            "$artifact_dir/$module-$coordinate_version-sources.jar"
        install -m 0644 "$module/build/publications/mavenJava/pom-default.xml" \
            "$artifact_dir/$module-$coordinate_version.pom"
        install -m 0644 "$module/build/publications/mavenJava/module.json" \
            "$artifact_dir/$module-$coordinate_version.module"
    done

    printf '%s\n' "$source_commit" >"$destination/source-commit.txt"
    printf '%s\n' "$coordinate_version" >"$destination/coordinate-version.txt"
    printf '%s\n' "$n1_commit" >"$destination/n1-source-commit.txt"
    printf '%s\n' "$n1_version" >"$destination/n1-coordinate-version.txt"
    (
        cd "$destination"
        find m2 -type f -print | LC_ALL=C sort | while read -r relative_path; do
            printf '%s  %s\n' "$(shasum -a 256 "$relative_path" | awk '{print $1}')" "$relative_path"
        done
        for descriptor in source-commit.txt coordinate-version.txt n1-source-commit.txt n1-coordinate-version.txt; do
            printf '%s  %s\n' "$(shasum -a 256 "$descriptor" | awk '{print $1}')" "$descriptor"
        done
    ) >"$destination/manifest.sha256"
}

build_once "$stage_one"
build_once "$stage_two"
diff -qr "$stage_one" "$stage_two" >/dev/null || fail "two clean builds differ"
rg -F -- '-SNAPSHOT' "$stage_two/m2" && fail "bundle contains a SNAPSHOT coordinate"
grep -Fq "<version>$n1_version</version>" \
    "$stage_two/m2/com/nereusstream/nereus-storage-api/$coordinate_version/"\
"nereus-storage-api-$coordinate_version.pom" || fail "storage API POM does not pin N1"

rm -rf "$stage_one"
stage_one=""
mv "$stage_two" "$bundle_root"
stage_two=""
echo "Published immutable M2 Kafka K0 bundle: $bundle_root"
echo "Coordinate version: $coordinate_version"
echo "Manifest SHA-256: $(shasum -a 256 "$bundle_root/manifest.sha256" | awk '{print $1}')"
