#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
domain_src="$repo_root/nereus-domain/src/main/java"
spi_src="$repo_root/nereus-metadata-spi/src/main/java"
capability_dir="$spi_src/com/nereusstream/metadata/spi/capability"

fail() {
    echo "V2 M1.1a-A foundation check: $*" >&2
    exit 1
}

[[ -d "$domain_src" ]] || fail "missing nereus-domain production sources"
[[ -d "$spi_src" ]] || fail "missing nereus-metadata-spi production sources"
[[ -d "$capability_dir" ]] || fail "missing metadata SPI capability package"

actual_capabilities="$(
    find "$capability_dir" -maxdepth 1 -type f -name '*.java' -exec basename {} .java \; | sort | paste -sd ' ' -
)"
expected_capabilities="PulsarTopicGenerationSelectorStore PulsarVirtualLedgerNamespaceRegistryStore TopicBindingAggregatePublisher TopicBindingAggregateReader"
[[ "$actual_capabilities" == "$expected_capabilities" ]] ||
    fail "capability inventory differs: $actual_capabilities"

if rg -n '^import ' "$domain_src" |
    rg -v '^.*:import (java\.|javax\.|org\.w3c\.dom\.|com\.nereusstream\.domain\.)' >/dev/null; then
    fail "nereus-domain imports a non-JDK or non-domain production package"
fi

if rg -n '^import ' "$spi_src" |
    rg -v '^.*:import (java\.|com\.nereusstream\.domain\.|com\.nereusstream\.metadata\.spi\.)' >/dev/null; then
    fail "nereus-metadata-spi imports a forbidden production package"
fi

for forbidden in \
    'org.apache.kafka' \
    'org.apache.pulsar' \
    'io.streamnative.oxia' \
    'io.netty' \
    'io.grpc' \
    'reactor.' \
    'io.vertx' \
    'com.google.common'; do
    if rg -Fq -- "$forbidden" "$domain_src" "$spi_src"; then
        fail "forbidden production dependency symbol: $forbidden"
    fi
done

if rg -n '\b(get|put|delete|list|watch)\s*\(' "$capability_dir" >/dev/null; then
    fail "generic CRUD/list/watch method leaked into the capability package"
fi

for forbidden in \
    'interface MetadataStore' \
    'interface TopicBindingStore' \
    'interface StorageEpochStore'; do
    if rg -Fq -- "$forbidden" "$spi_src" "$domain_src"; then
        fail "forbidden foundation API: $forbidden"
    fi
done

for forbidden in \
    'STRICT_SERIALIZED' \
    'RANGE_LEASED' \
    'maxWriterCount'; do
    if rg -Fq -- "$forbidden" "$capability_dir"; then
        fail "later allocator authority leaked into the M1 foundation capabilities: $forbidden"
    fi
done

if find "$spi_src" -type f \( -name '*Nta1*' -o -name '*NTA1*' \) | rg -q .; then
    fail "NTA1 codec ownership leaked from the domain into metadata SPI"
fi

for outcome in CREATED EXISTING_EXACT DEFINITIVE_CONFLICT INDETERMINATE; do
    rg -Fq -- "$outcome" "$spi_src/com/nereusstream/metadata/spi/model/CreateMutationOutcome.java" ||
        fail "missing create outcome $outcome"
done
for outcome in APPLIED_EXACT PREDECESSOR_UNCHANGED DEFINITIVE_CONFLICT INDETERMINATE; do
    rg -Fq -- "$outcome" "$spi_src/com/nereusstream/metadata/spi/model/ConditionalCasOutcome.java" ||
        fail "missing CAS outcome $outcome"
done

echo "V2 M1.1a-A foundation API check passed; later domain codecs do not change its partial boundary; no M1 PASS."
