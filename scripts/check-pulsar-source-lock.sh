#!/usr/bin/env bash

set -euo pipefail

EXPECTED_HEAD="100d3ef0ff7c7da36d497453b141ddff6f34a9d3"
PULSAR_CHECKOUT="${1:?usage: check-pulsar-source-lock.sh PULSAR_CHECKOUT}"

if [[ ! -f "${PULSAR_CHECKOUT}/settings.gradle.kts" ]]; then
  echo "Pulsar checkout is missing or not the locked Gradle source tree: ${PULSAR_CHECKOUT}" >&2
  exit 1
fi

actual_head="$(git -C "${PULSAR_CHECKOUT}" rev-parse HEAD)"
if [[ "${actual_head}" != "${EXPECTED_HEAD}" ]]; then
  echo "Pulsar checkout HEAD mismatch: expected ${EXPECTED_HEAD}, got ${actual_head}" >&2
  exit 1
fi

if [[ -n "$(git -C "${PULSAR_CHECKOUT}" status --porcelain --untracked-files=no)" ]]; then
  echo "Pulsar checkout has tracked modifications: ${PULSAR_CHECKOUT}" >&2
  exit 1
fi

echo "Locked Pulsar checkout verified: ${actual_head}"
