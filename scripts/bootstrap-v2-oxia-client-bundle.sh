#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 /path/to/exact/o1-client-worktree" >&2
    exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
client_checkout="$(cd "$1" && pwd)"
expected_commit="091a42c2780d92da56e9ec1f02ce1c3d988adc16"
bundle_root="$repo_root/gradle/locked-artifacts/oxia-client-java/$expected_commit"
maven_root="$bundle_root/m2/io/github/oxia-db"

actual_commit="$(git -C "$client_checkout" rev-parse HEAD)"
[[ "$actual_commit" == "$expected_commit" ]] || {
    echo "O1 checkout must be at $expected_commit, found $actual_commit" >&2
    exit 1
}
[[ -z "$(git -C "$client_checkout" status --porcelain)" ]] || {
    echo "O1 checkout must be clean before artifact qualification" >&2
    exit 1
}

"$client_checkout/gradlew" -p "$client_checkout" \
    :client-api:jar \
    :client-api:sourcesJar \
    :client-api:generatePomFileForMavenPublication \
    :client-api:generateMetadataFileForMavenPublication \
    :client:jar \
    :client:sourcesJar \
    :client:generatePomFileForMavenPublication \
    :client:generateMetadataFileForMavenPublication \
    --rerun-tasks --no-daemon --console=plain

client_api_dir="$maven_root/oxia-client-api/0.9.4"
client_dir="$maven_root/oxia-client/0.9.4"
mkdir -p "$client_api_dir" "$client_dir"

install -m 0644 "$client_checkout/client-api/build/libs/client-api-0.9.4.jar" \
    "$client_api_dir/oxia-client-api-0.9.4.jar"
install -m 0644 "$client_checkout/client-api/build/libs/client-api-0.9.4-sources.jar" \
    "$client_api_dir/oxia-client-api-0.9.4-sources.jar"
install -m 0644 "$client_checkout/client-api/build/publications/maven/pom-default.xml" \
    "$client_api_dir/oxia-client-api-0.9.4.pom"
install -m 0644 "$client_checkout/client-api/build/publications/maven/module.json" \
    "$client_api_dir/oxia-client-api-0.9.4.module"

install -m 0644 "$client_checkout/client/build/libs/client-0.9.4.jar" \
    "$client_dir/oxia-client-0.9.4.jar"
install -m 0644 "$client_checkout/client/build/libs/client-0.9.4-sources.jar" \
    "$client_dir/oxia-client-0.9.4-sources.jar"
install -m 0644 "$client_checkout/client/build/publications/maven/pom-default.xml" \
    "$client_dir/oxia-client-0.9.4.pom"
install -m 0644 "$client_checkout/client/build/publications/maven/module.json" \
    "$client_dir/oxia-client-0.9.4.module"

while read -r expected relative_path; do
    actual="$(shasum -a 256 "$bundle_root/$relative_path" | awk '{print $1}')"
    [[ "$actual" == "$expected" ]] || {
        echo "qualified artifact digest mismatch for $relative_path: $actual" >&2
        exit 1
    }
done <"$bundle_root/manifest.sha256"

echo "Qualified immutable O1 Maven bundle at $bundle_root"
