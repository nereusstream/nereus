/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationLifecycle;
import com.nereusstream.metadata.oxia.records.GenerationProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.ReferenceDomainVersionRecord;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Canonical local/durable reference-domain set comparison for generation protocol V1. */
final class GenerationProtocolDomainSets {
    private GenerationProtocolDomainSets() {
    }

    static List<ReferenceDomainVersionRecord> canonicalInstalled(
            List<GcReferenceDomainVersion> supplied) {
        List<ReferenceDomainVersionRecord> installed = Objects.requireNonNull(
                        supplied, "installedDomains")
                .stream()
                .map(version -> new ReferenceDomainVersionRecord(
                        version.domainId(), version.protocolVersion()))
                .sorted(Comparator.naturalOrder())
                .toList();
        if (installed.isEmpty()
                || installed.size()
                        > GenerationProtocolActivationRecord.MAX_REFERENCE_DOMAINS) {
            throw new IllegalArgumentException(
                    "installed reference-domain set must be non-empty and bounded");
        }
        HashSet<String> ids = new HashSet<>();
        for (ReferenceDomainVersionRecord version : installed) {
            if (!ids.add(version.domainId())) {
                throw new IllegalArgumentException(
                        "installed reference-domain ids must be unique");
            }
        }
        return installed;
    }

    static boolean exactMatch(
            GenerationProtocolActivationRecord activation,
            List<ReferenceDomainVersionRecord> installed) {
        return activation.requiredReferenceDomains().equals(installed);
    }

    static boolean deletionReady(
            GenerationProtocolActivationRecord activation,
            List<ReferenceDomainVersionRecord> installed) {
        Objects.requireNonNull(activation, "activation");
        Objects.requireNonNull(installed, "installed");
        return activation.lifecycle() == GenerationProtocolActivationLifecycle.ACTIVE
                && activation.publicationEnabled()
                && activation.physicalDeleteEnabled()
                && activation.cursorSnapshotDeleteEnabled()
                && activation.streamRegistrationBackfill().complete()
                && activation.physicalRootBackfill().complete()
                && activation.cursorSnapshotBackfill().complete()
                && !activation.objectStoreCapabilitySha256().isEmpty()
                && exactMatch(activation, installed);
    }
}
