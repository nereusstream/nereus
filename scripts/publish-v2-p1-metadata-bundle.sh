#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 P1 artifact publication: $*" >&2
    exit 1
}

[[ "$(git branch --show-current)" == "main" ]] || fail "publication must run from main"
[[ -z "$(git status --porcelain)" ]] || fail "source worktree must be clean"

source_commit="$(git rev-parse HEAD)"
[[ "$source_commit" =~ ^[0-9a-f]{40}$ ]] || fail "invalid source commit $source_commit"
[[ "$(git rev-parse origin/main)" == "$source_commit" ]] ||
    fail "source commit must already be pushed to origin/main"

development_version="$(sed -n 's/^nereusVersion=//p' gradle.properties)"
[[ "$development_version" =~ ^([0-9]+\.[0-9]+\.[0-9]+)-SNAPSHOT$ ]] ||
    fail "expected X.Y.Z-SNAPSHOT in gradle.properties, found $development_version"
coordinate_version="${BASH_REMATCH[1]}-p1.${source_commit}"
n1_source_commit="$(jq -er '.n1ArtifactBinding.sourceCommit' docs/v2/source-locks.json)"
n1_coordinate_version="$(jq -er '.n1ArtifactBinding.coordinateVersion' docs/v2/source-locks.json)"
n1_bundle_root="$(jq -er '.n1ArtifactBinding.bundleRoot' docs/v2/source-locks.json)"
oxia_client_commit="$(jq -er '
    .dependencyForkOutputs[] | select(.id == "oxia-client-notification-continuity") | .finalForkCommit
' docs/v2/source-locks.json)"
oxia_client_version="$(sed -n 's/^oxia = "\([^"]*\)"/\1/p' gradle/libs.versions.toml)"

[[ "$n1_coordinate_version" == "${BASH_REMATCH[1]}-n1.${n1_source_commit}" ]] ||
    fail "N1 source/coordinate mismatch"
[[ -d "$n1_bundle_root/m2" ]] || fail "missing immutable N1 bundle $n1_bundle_root"
[[ "$oxia_client_version" == "0.9.4" ]] || fail "unexpected Oxia client version $oxia_client_version"

coordinate_root="com/nereusstream"
bundle_parent="$repo_root/gradle/locked-artifacts/nereus-p1"
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

sha() {
    shasum -a "$1" "$2" | awk '{print $1}'
}

md5_digest() {
    openssl dgst -md5 "$1" | awk '{print $NF}'
}

build_once() {
    local destination="$1"
    local artifact_dir="$destination/m2/$coordinate_root/nereus-metadata-oxia-p1/$coordinate_version"
    local jar_name="nereus-metadata-oxia-p1-$coordinate_version.jar"
    local sources_name="nereus-metadata-oxia-p1-$coordinate_version-sources.jar"
    local pom_name="nereus-metadata-oxia-p1-$coordinate_version.pom"
    local module_name="nereus-metadata-oxia-p1-$coordinate_version.module"

    ./gradlew \
        -PnereusVersion="$coordinate_version" \
        :nereus-metadata-oxia:clean \
        :nereus-metadata-oxia:p1ArtifactJar \
        :nereus-metadata-oxia:p1ArtifactSourcesJar \
        --rerun-tasks --no-daemon --console=plain

    mkdir -p "$artifact_dir"
    install -m 0644 "nereus-metadata-oxia/build/libs/$jar_name" "$artifact_dir/$jar_name"
    install -m 0644 "nereus-metadata-oxia/build/libs/$sources_name" "$artifact_dir/$sources_name"

    local unexpected_binary unexpected_sources
    unexpected_binary="$(jar tf "$artifact_dir/$jar_name" |
        grep -Ev '^(META-INF/$|META-INF/MANIFEST.MF$|com/$|com/nereusstream/$|com/nereusstream/metadata/$|com/nereusstream/metadata/oxia/$|com/nereusstream/metadata/oxia/v2/)' || true)"
    if [[ -n "$unexpected_binary" ]]; then
        fail "P1 binary artifact contains a class outside com/nereusstream/metadata/oxia/v2"
    fi
    unexpected_sources="$(jar tf "$artifact_dir/$sources_name" |
        grep -Ev '^(META-INF/$|META-INF/MANIFEST.MF$|com/$|com/nereusstream/$|com/nereusstream/metadata/$|com/nereusstream/metadata/oxia/$|com/nereusstream/metadata/oxia/v2/)' || true)"
    if [[ -n "$unexpected_sources" ]]; then
        fail "P1 source artifact contains a file outside com/nereusstream/metadata/oxia/v2"
    fi

    local jar_size jar_sha512 jar_sha256 jar_sha1 jar_md5
    local sources_size sources_sha512 sources_sha256 sources_sha1 sources_md5
    jar_size="$(wc -c <"$artifact_dir/$jar_name" | tr -d ' ')"
    jar_sha512="$(sha 512 "$artifact_dir/$jar_name")"
    jar_sha256="$(sha 256 "$artifact_dir/$jar_name")"
    jar_sha1="$(sha 1 "$artifact_dir/$jar_name")"
    jar_md5="$(md5_digest "$artifact_dir/$jar_name")"
    sources_size="$(wc -c <"$artifact_dir/$sources_name" | tr -d ' ')"
    sources_sha512="$(sha 512 "$artifact_dir/$sources_name")"
    sources_sha256="$(sha 256 "$artifact_dir/$sources_name")"
    sources_sha1="$(sha 1 "$artifact_dir/$sources_name")"
    sources_md5="$(md5_digest "$artifact_dir/$sources_name")"

    sed \
        -e "s/@P1_VERSION@/$coordinate_version/g" \
        -e "s/@N1_VERSION@/$n1_coordinate_version/g" \
        -e "s/@OXIA_VERSION@/$oxia_client_version/g" \
        scripts/templates/nereus-metadata-oxia-p1.pom.xml \
        >"$artifact_dir/$pom_name"
    sed \
        -e "s/@P1_VERSION@/$coordinate_version/g" \
        -e "s/@N1_VERSION@/$n1_coordinate_version/g" \
        -e "s/@OXIA_VERSION@/$oxia_client_version/g" \
        -e "s/@JAR_NAME@/$jar_name/g" \
        -e "s/@JAR_SIZE@/$jar_size/g" \
        -e "s/@JAR_SHA512@/$jar_sha512/g" \
        -e "s/@JAR_SHA256@/$jar_sha256/g" \
        -e "s/@JAR_SHA1@/$jar_sha1/g" \
        -e "s/@JAR_MD5@/$jar_md5/g" \
        -e "s/@SOURCES_NAME@/$sources_name/g" \
        -e "s/@SOURCES_SIZE@/$sources_size/g" \
        -e "s/@SOURCES_SHA512@/$sources_sha512/g" \
        -e "s/@SOURCES_SHA256@/$sources_sha256/g" \
        -e "s/@SOURCES_SHA1@/$sources_sha1/g" \
        -e "s/@SOURCES_MD5@/$sources_md5/g" \
        scripts/templates/nereus-metadata-oxia-p1.module.json \
        >"$artifact_dir/$module_name"

    printf '%s\n' "$source_commit" >"$destination/source-commit.txt"
    printf '%s\n' "$coordinate_version" >"$destination/coordinate-version.txt"
    printf '%s\n' "$n1_source_commit" >"$destination/n1-source-commit.txt"
    printf '%s\n' "$n1_coordinate_version" >"$destination/n1-coordinate-version.txt"
    printf '%s\n' "$oxia_client_commit" >"$destination/oxia-client-source-commit.txt"

    (
        cd "$destination"
        find m2 -type f -print | LC_ALL=C sort | while read -r relative_path; do
            printf '%s  %s\n' "$(shasum -a 256 "$relative_path" | awk '{print $1}')" "$relative_path"
        done
        for descriptor in source-commit.txt coordinate-version.txt n1-source-commit.txt \
            n1-coordinate-version.txt oxia-client-source-commit.txt; do
            printf '%s  %s\n' "$(shasum -a 256 "$descriptor" | awk '{print $1}')" "$descriptor"
        done
    ) >"$destination/manifest.sha256"
}

build_once "$stage_one"
build_once "$stage_two"

diff -qr "$stage_one" "$stage_two" >/dev/null ||
    fail "two clean P1 builds are not byte-for-byte reproducible"
if rg -Fq -- '-SNAPSHOT' "$stage_two/m2"; then
    fail "P1 bundle contains a dynamic SNAPSHOT dependency or coordinate"
fi
if rg -Fq 'changing=true' "$stage_two/m2"; then
    fail "P1 bundle contains a changing dependency"
fi

rm -rf "$stage_one"
stage_one=""
mv "$stage_two" "$bundle_root"
stage_two=""

echo "Published immutable, reproducible P1 bundle: $bundle_root"
echo "Coordinate version: $coordinate_version"
echo "Manifest SHA-256: $(shasum -a 256 "$bundle_root/manifest.sha256" | awk '{print $1}')"
