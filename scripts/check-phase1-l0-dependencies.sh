#!/usr/bin/env bash
set -euo pipefail

modules=(
  nereus-api
  nereus-core
  nereus-metadata-oxia
  nereus-object-store
)

forbidden=(
  org.apache.pulsar
  org.apache.bookkeeper
  org.apache.kafka
  io.confluent
)

for module in "${modules[@]}"; do
  build_file="$module/build.gradle.kts"
  if [[ ! -f "$build_file" ]]; then
    echo "missing Phase 1 L0 build file: $build_file" >&2
    exit 1
  fi
  for needle in "${forbidden[@]}"; do
    if rg -q "$needle" "$build_file"; then
      echo "$build_file must not declare dependency on $needle" >&2
      exit 1
    fi
  done
  if [[ -d "$module/src" ]] && rg -n \
      '^import (org\.apache\.pulsar|org\.apache\.bookkeeper|org\.apache\.kafka|io\.confluent)' \
      "$module/src"; then
    echo "$module source sets must remain free of Pulsar, BookKeeper, Kafka, and Confluent imports" >&2
    exit 1
  fi
done

echo "Phase 1 L0 dependency guard passed."
