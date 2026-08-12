#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 N1 artifact publication: $*" >&2
    exit 1
}

[[ "$(git branch --show-current)" == "main" ]] || fail "bootstrap publication must run from main"
[[ -z "$(git status --porcelain)" ]] || fail "source worktree must be clean"

source_commit="$(git rev-parse HEAD)"
[[ "$source_commit" =~ ^[0-9a-f]{40}$ ]] || fail "invalid source commit $source_commit"
[[ "$(git rev-parse origin/main)" == "$source_commit" ]] ||
    fail "source commit must already be pushed to origin/main"

development_version="$(sed -n 's/^nereusVersion=//p' gradle.properties)"
[[ "$development_version" =~ ^([0-9]+\.[0-9]+\.[0-9]+)-SNAPSHOT$ ]] ||
    fail "expected X.Y.Z-SNAPSHOT in gradle.properties, found $development_version"
coordinate_version="${BASH_REMATCH[1]}-n1.${source_commit}"
coordinate_root="com/nereusstream"
bundle_parent="$repo_root/gradle/locked-artifacts/nereus-n1"
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

build_once() {
    local destination="$1"
    local domain_dir="$destination/m2/$coordinate_root/nereus-domain/$coordinate_version"
    local spi_dir="$destination/m2/$coordinate_root/nereus-metadata-spi/$coordinate_version"

    ./gradlew \
        -PnereusVersion="$coordinate_version" \
        :nereus-domain:clean \
        :nereus-metadata-spi:clean \
        :nereus-domain:jar \
        :nereus-domain:sourcesJar \
        :nereus-domain:generatePomFileForMavenJavaPublication \
        :nereus-domain:generateMetadataFileForMavenJavaPublication \
        :nereus-metadata-spi:jar \
        :nereus-metadata-spi:sourcesJar \
        :nereus-metadata-spi:generatePomFileForMavenJavaPublication \
        :nereus-metadata-spi:generateMetadataFileForMavenJavaPublication \
        --rerun-tasks --no-daemon --console=plain

    mkdir -p "$domain_dir" "$spi_dir"
    install -m 0644 \
        "nereus-domain/build/libs/nereus-domain-$coordinate_version.jar" \
        "$domain_dir/nereus-domain-$coordinate_version.jar"
    install -m 0644 \
        "nereus-domain/build/libs/nereus-domain-$coordinate_version-sources.jar" \
        "$domain_dir/nereus-domain-$coordinate_version-sources.jar"
    install -m 0644 nereus-domain/build/publications/mavenJava/pom-default.xml \
        "$domain_dir/nereus-domain-$coordinate_version.pom"
    install -m 0644 nereus-domain/build/publications/mavenJava/module.json \
        "$domain_dir/nereus-domain-$coordinate_version.module"

    install -m 0644 \
        "nereus-metadata-spi/build/libs/nereus-metadata-spi-$coordinate_version.jar" \
        "$spi_dir/nereus-metadata-spi-$coordinate_version.jar"
    install -m 0644 \
        "nereus-metadata-spi/build/libs/nereus-metadata-spi-$coordinate_version-sources.jar" \
        "$spi_dir/nereus-metadata-spi-$coordinate_version-sources.jar"
    install -m 0644 nereus-metadata-spi/build/publications/mavenJava/pom-default.xml \
        "$spi_dir/nereus-metadata-spi-$coordinate_version.pom"
    install -m 0644 nereus-metadata-spi/build/publications/mavenJava/module.json \
        "$spi_dir/nereus-metadata-spi-$coordinate_version.module"

    printf '%s\n' "$source_commit" >"$destination/source-commit.txt"
    printf '%s\n' "$coordinate_version" >"$destination/coordinate-version.txt"

    (
        cd "$destination"
        find m2 -type f -print | LC_ALL=C sort | while read -r relative_path; do
            printf '%s  %s\n' "$(shasum -a 256 "$relative_path" | awk '{print $1}')" "$relative_path"
        done
        printf '%s  %s\n' "$(shasum -a 256 source-commit.txt | awk '{print $1}')" source-commit.txt
        printf '%s  %s\n' "$(shasum -a 256 coordinate-version.txt | awk '{print $1}')" coordinate-version.txt
    ) >"$destination/manifest.sha256"
}

build_once "$stage_one"
build_once "$stage_two"

diff -qr "$stage_one" "$stage_two" >/dev/null ||
    fail "two clean N1 builds are not byte-for-byte reproducible"

grep -Fq "<version>$coordinate_version</version>" \
    "$stage_two/m2/$coordinate_root/nereus-metadata-spi/$coordinate_version/nereus-metadata-spi-$coordinate_version.pom" ||
    fail "metadata SPI POM does not pin the exact N1 domain version"
if rg -Fq -- '-SNAPSHOT' "$stage_two/m2"; then
    fail "N1 bundle contains a dynamic SNAPSHOT dependency or coordinate"
fi

rm -rf "$stage_one"
stage_one=""
mv "$stage_two" "$bundle_root"
stage_two=""

echo "Published immutable, reproducible N1 bundle: $bundle_root"
echo "Coordinate version: $coordinate_version"
echo "Manifest SHA-256: $(shasum -a 256 "$bundle_root/manifest.sha256" | awk '{print $1}')"
