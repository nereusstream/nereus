#!/usr/bin/env bash

set -euo pipefail

PULSAR_CHECKOUT="${1:?usage: check-pulsar-source-lock.sh PULSAR_CHECKOUT EXPECTED_HEAD}"
EXPECTED_HEAD="${2:?usage: check-pulsar-source-lock.sh PULSAR_CHECKOUT EXPECTED_HEAD}"

if [[ ! "${EXPECTED_HEAD}" =~ ^[0-9a-f]{40}$ ]]; then
  echo "Expected Pulsar HEAD must be one full lowercase commit hash: ${EXPECTED_HEAD}" >&2
  exit 1
fi

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
