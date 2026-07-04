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
  docs/design/nereus-design-index.md
  docs/phase0/repository-plan.md
  docs/phase0/upstream-forks.md
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

for module in nereus-api nereus-core nereus-metadata-oxia nereus-object-store \
              nereus-managed-ledger nereus-pulsar-adapter nereus-kop-adapter; do
  if [[ ! -f "$module/build.gradle.kts" ]]; then
    echo "missing module build file: $module/build.gradle.kts" >&2
    exit 1
  fi
done

echo "Phase 0 Gradle scaffold looks complete."
