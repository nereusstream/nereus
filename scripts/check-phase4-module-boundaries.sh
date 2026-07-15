#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
materialization_build="$repo_root/nereus-materialization/build.gradle.kts"

for dependency in nereus-api nereus-core nereus-metadata-oxia nereus-object-store; do
    if ! rg -Fq -- "project(\":$dependency\")" "$materialization_build"; then
        echo "nereus-materialization is missing its Phase 4 dependency on $dependency" >&2
        exit 1
    fi
done

if rg -n 'project\(":nereus-(managed-ledger|pulsar-adapter|kop-adapter|materialization)"\)' \
        "$repo_root/nereus-core/build.gradle.kts" \
        "$repo_root/nereus-metadata-oxia/build.gradle.kts"; then
    echo "Phase 4 dependency direction would create a core/metadata cycle" >&2
    exit 1
fi

if rg -n 'project\(":nereus-(managed-ledger|pulsar-adapter|kop-adapter)"\)' "$materialization_build"; then
    echo "nereus-materialization must remain protocol-neutral" >&2
    exit 1
fi

neutral_sources=(
    "$repo_root/nereus-api/src"
    "$repo_root/nereus-core/src"
    "$repo_root/nereus-metadata-oxia/src/main"
    "$repo_root/nereus-object-store/src/main"
    "$repo_root/nereus-materialization/src"
)
if rg -n '^import (org\.apache\.pulsar|org\.apache\.bookkeeper|org\.apache\.kafka|io\.confluent)' \
        "${neutral_sources[@]}"; then
    echo "Phase 4 protocol-neutral modules import broker/protocol implementation classes" >&2
    exit 1
fi

echo "Phase 4 materialization/core/metadata/object-store dependency direction verified."
