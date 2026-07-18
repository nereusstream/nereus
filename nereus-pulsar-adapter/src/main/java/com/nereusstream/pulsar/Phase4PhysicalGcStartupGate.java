/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.pulsar;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.metadata.oxia.GenerationProtocolActivationStore;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Prevents mutating physical-GC startup and DELETING recovery unless durable authority names the exact local scope.
 */
final class Phase4PhysicalGcStartupGate {
    private final String cluster;
    private final GenerationProtocolActivationStore activations;
    private final List<ReferenceDomainVersionRecord> requiredDomains;
    private final String expectedCapabilitySha256;

    Phase4PhysicalGcStartupGate(
            String cluster,
            GenerationProtocolActivationStore activations,
            List<ReferenceDomainVersionRecord> requiredDomains,
            String expectedCapabilitySha256) {
        this.cluster = new OxiaKeyspace(cluster).cluster();
        this.activations = Objects.requireNonNull(activations, "activations");
        this.requiredDomains = canonicalDomains(requiredDomains);
        this.expectedCapabilitySha256 = requireSha256(
                expectedCapabilitySha256, "expectedCapabilitySha256");
    }

    String expectedCapabilitySha256() {
        return expectedCapabilitySha256;
    }

    /**
     * Returns false while deletion has not been activated. Once either V1 delete bit is durable, every authority fact
     * must be exact or startup fails non-retryably.
     */
    CompletableFuture<Boolean> destructiveLifecycleAuthorized() {
        return activations.get(cluster).thenApply(optional -> {
            if (optional.isEmpty()) {
                return false;
            }
            GenerationProtocolActivationRecord activation = optional.orElseThrow().value();
            if (!activation.physicalDeleteEnabled()
                    && !activation.cursorSnapshotDeleteEnabled()) {
                return false;
            }
            if (activation.lifecycle()
                            != GenerationProtocolActivationLifecycle.ACTIVE
                    || !activation.publicationEnabled()
                    || !activation.physicalDeleteEnabled()
                    || !activation.cursorSnapshotDeleteEnabled()
                    || !activation.streamRegistrationBackfill().complete()
                    || !activation.physicalRootBackfill().complete()
                    || !activation.cursorSnapshotBackfill().complete()) {
                throw invariant(
                        "durable physical-deletion authority is internally incomplete");
            }
            if (!activation.requiredReferenceDomains().equals(requiredDomains)) {
                throw invariant(
                        "durable physical-deletion domain set differs from the local runtime");
            }
            if (!activation.objectStoreCapabilitySha256()
                    .equals(expectedCapabilitySha256)) {
                throw invariant(
                        "durable physical-deletion authority belongs to another configured object-store scope");
            }
            return true;
        });
    }

    private static List<ReferenceDomainVersionRecord> canonicalDomains(
            List<ReferenceDomainVersionRecord> supplied) {
        List<ReferenceDomainVersionRecord> domains = Objects.requireNonNull(
                        supplied, "requiredDomains")
                .stream()
                .sorted(Comparator.naturalOrder())
                .toList();
        if (domains.isEmpty()
                || domains.size()
                        > GenerationProtocolActivationRecord.MAX_REFERENCE_DOMAINS) {
            throw new IllegalArgumentException(
                    "requiredDomains must be non-empty and bounded");
        }
        HashSet<String> ids = new HashSet<>();
        for (ReferenceDomainVersionRecord domain : domains) {
            if (!ids.add(domain.domainId())) {
                throw new IllegalArgumentException(
                        "requiredDomains must use unique domain ids");
            }
        }
        return domains;
    }

    private static String requireSha256(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.length() != 64) {
            throw new IllegalArgumentException(
                    field + " must be lowercase SHA-256");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (!((character >= '0' && character <= '9')
                    || (character >= 'a' && character <= 'f'))) {
                throw new IllegalArgumentException(
                        field + " must be lowercase SHA-256");
            }
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }
}
