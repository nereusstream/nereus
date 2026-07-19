#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

fail() {
  echo "BookKeeper module boundary check failed: $*" >&2
  exit 1
}

[[ -f nereus-bookkeeper/build.gradle.kts ]] || fail "nereus-bookkeeper module is missing"

if rg -n 'org\.apache\.bookkeeper\.' nereus-api/src nereus-core/src --glob '*.java'; then
  fail "L0 API/core must not import BookKeeper provider classes"
fi
if rg -n 'org\.apache\.bookkeeper\.mledger\.' nereus-bookkeeper/src --glob '*.java'; then
  fail "nereus-bookkeeper must not depend on ManagedLedger implementation types"
fi
if rg -n 'project\(\":nereus-bookkeeper"\)' nereus-api/build.gradle.kts nereus-core/build.gradle.kts; then
  fail "L0 modules must not depend on nereus-bookkeeper"
fi
if ! rg -q 'project\(\":nereus-api"\)' nereus-bookkeeper/build.gradle.kts \
    || ! rg -q 'project\(\":nereus-core"\)' nereus-bookkeeper/build.gradle.kts \
    || ! rg -q 'project\(\":nereus-metadata-oxia"\)' nereus-bookkeeper/build.gradle.kts; then
  fail "nereus-bookkeeper is missing its provider-neutral dependencies"
fi
if ! rg -q 'bookkeeper[.-]server' nereus-bookkeeper/build.gradle.kts; then
  fail "nereus-bookkeeper must compile against the pinned BookKeeper client artifact"
fi

echo "BookKeeper module boundaries passed."
