#!/usr/bin/env bash
set -euo pipefail

expected_group="com.nereusstream"
actual_group="$(sed -n 's/^nereusGroup=//p' gradle.properties)"

if [[ "$actual_group" != "$expected_group" ]]; then
  echo "Expected nereusGroup=$expected_group, found '${actual_group:-<missing>}'" >&2
  exit 1
fi

if find nereus-* -type f \( \
    -path '*/src/*/java/io/nereus/*' -o \
    -path '*/src/*/resources/io/nereus/*' \
  \) -print -quit | grep -q .; then
  echo "Legacy io/nereus source or resource path found" >&2
  exit 1
fi

package_lines="$(rg -n '^package ' nereus-* --glob '*.java' || true)"
if [[ -n "$package_lines" ]] && printf '%s\n' "$package_lines" | rg -v ':[0-9]+:package com\.nereusstream([.;])' >/dev/null; then
  echo "Java package outside com.nereusstream:" >&2
  printf '%s\n' "$package_lines" | rg -v ':[0-9]+:package com\.nereusstream([.;])' >&2
  exit 1
fi

echo "Phase 1 Java and Maven namespace check passed."
