#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

fail() {
    echo "V2 M1 active-graph check: $*" >&2
    exit 1
}

python3 - "$repo_root/settings.gradle.kts" <<'PY'
import pathlib
import re
import sys

settings = pathlib.Path(sys.argv[1]).read_text()
actual = set(re.findall(r'^include\("([^\"]+)"\)', settings, re.MULTILINE))
expected = {"nereus-bom", "nereus-domain", "nereus-metadata-spi", "nereus-metadata-oxia"}
if actual != expected:
    raise SystemExit(f"V2 M1 active-graph check: active settings graph differs: {sorted(actual)}")
PY

for removed in \
    nereus-api nereus-core nereus-object-store nereus-materialization nereus-bookkeeper \
    nereus-managed-ledger nereus-pulsar-adapter nereus-kop-adapter nereus-kafka-adapter nereus-admin; do
    [[ ! -e "$repo_root/$removed" ]] || fail "removed V1 module remains: $removed"
done

for required in \
    "$repo_root/nereus-domain" \
    "$repo_root/nereus-metadata-spi" \
    "$repo_root/nereus-metadata-oxia" \
    "$repo_root/docs/v1" \
    "$repo_root/docs/v2/07-protocol-integrations.md"; do
    [[ -e "$required" ]] || fail "required V2/archive boundary is missing: ${required#"$repo_root/"}"
done

if find "$repo_root/nereus-metadata-oxia/src" -type f ! -path '*/v2/*' -print -quit | grep -q .; then
    fail "a non-V2 metadata-oxia source or test remains after the mechanical prune"
fi

while IFS= read -r script; do
    case "${script#"$repo_root/"}" in
        scripts/check-v2-*|scripts/bootstrap-v2-*|scripts/publish-v2-*|scripts/templates/*) ;;
        *) fail "a V1/Phase/F9 executable script remains: ${script#"$repo_root/"}" ;;
    esac
done < <(find "$repo_root/scripts" -type f -print | sort)

[[ ! -e "$repo_root/docker/nereus-admin" ]] || fail "the V1 Admin Docker runtime remains"

if rg -n \
    'StreamStorage|ManagedLedgerProjectionMetadataStore|KafkaPartitionMetadataStore|interface OxiaMetadataStore|ProjectionRef|ProjectionType' \
    "$repo_root/nereus-domain/src/main" "$repo_root/nereus-metadata-spi/src/main" \
    "$repo_root/nereus-metadata-oxia/src/main" --glob '*.java'; then
    fail "V1 API or umbrella metadata authority remains in the active production graph"
fi

if rg -n 'project\(\":nereus-(api|core|object-store|materialization|bookkeeper|managed-ledger|pulsar-adapter|kop-adapter|kafka-adapter|admin)\"\)' \
    "$repo_root" --glob 'build.gradle.kts' --glob '!docs/**'; then
    fail "an active build edge still points at a removed V1 module"
fi

if rg -n 'tasks\.register[^\n]*\("(?:phase[0-9]|f9|bookKeeperPrimaryWal)' \
    "$repo_root/build.gradle.kts" --pcre2; then
    fail "a Phase/F9/V1 executable task remains in the root build"
fi

echo "V2 M1 active graph contains only BOM/domain/SPI/Oxia; V1 runtime, Phase/F9 scripts, and KoP runtime are absent; archives and KoP design remain."
