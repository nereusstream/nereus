#!/usr/bin/env bash
set -euo pipefail

required=(
  settings.gradle.kts
  build.gradle.kts
  gradle.properties
  gradle/libs.versions.toml
  gradlew
  README.md
  LICENSE
  NOTICE
)

for path in "${required[@]}"; do
  if [[ ! -e "$path" ]]; then
    echo "missing required file: $path" >&2
    exit 1
  fi
done

if [[ -d integrations ]]; then
  echo "integrations/ should not exist in the main repo; use org forks instead" >&2
  exit 1
fi

if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if [[ -n "$(git ls-files docs)" ]]; then
    echo "docs/ should stay out of the pushed code repository for now" >&2
    exit 1
  fi
fi

for module in nereus-api nereus-core nereus-metadata-oxia nereus-object-store \
              nereus-managed-ledger nereus-pulsar-adapter nereus-kop-adapter; do
  if [[ ! -f "$module/build.gradle.kts" ]]; then
    echo "missing module build file: $module/build.gradle.kts" >&2
    exit 1
  fi
done

echo "Phase 0 Gradle scaffold looks complete."
