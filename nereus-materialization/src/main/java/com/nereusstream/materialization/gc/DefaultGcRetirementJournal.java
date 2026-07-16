/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GcRetirementProtectionScanPage;
import com.nereusstream.metadata.oxia.GcRetirementRemovalScanPage;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedGcRetirementProtection;
import com.nereusstream.metadata.oxia.VersionedGcRetirementRemoval;
import com.nereusstream.metadata.oxia.records.GcDomainSnapshotProofRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementProtectionRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/** Oxia-backed, manifest-last GC retirement journal. */
public final class DefaultGcRetirementJournal implements GcRetirementJournal {
    private final String cluster;
    private final PhysicalObjectMetadataStore metadataStore;
    private final PhysicalGcConfig config;

    public DefaultGcRetirementJournal(
            String cluster,
            PhysicalObjectMetadataStore metadataStore,
            PhysicalGcConfig config) {
        this.cluster = requireText(cluster, "cluster");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public CompletableFuture<GcRetirementJournalSnapshot> prepare(
            String gcAttemptId,
            GcCandidate candidate,
            List<GcReferenceSnapshot> domainSnapshots,
            List<GcPlannedProtectionRemoval> plannedProtectionRemovals,
            List<GcPlannedMetadataRemoval> plannedMetadataRemovals,
            Checksum referenceSetSha256,
            long createdAtMillis,
            MaterializationDeadline deadline) {
        final Preparation preparation;
        try {
            preparation = preparation(
                    gcAttemptId,
                    candidate,
                    domainSnapshots,
                    plannedProtectionRemovals,
                    plannedMetadataRemovals,
                    referenceSetSha256,
                    createdAtMillis);
            Objects.requireNonNull(deadline, "deadline");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return load(preparation.object(), preparation.gcAttemptId(), deadline)
                .thenCompose(existing -> existing
                        .<CompletableFuture<GcRetirementJournalSnapshot>>map(snapshot ->
                                requireExpected(snapshot, preparation))
                        .orElseGet(() -> writeAndSeal(preparation, deadline)));
    }

    @Override
    public CompletableFuture<Optional<GcRetirementJournalSnapshot>> load(
            ObjectKeyHash object,
            String gcAttemptId,
            MaterializationDeadline deadline) {
        ObjectKeyHash exactObject = Objects.requireNonNull(object, "object");
        String exactAttempt = GcPlanValidation.requireBase32Id(gcAttemptId, "gcAttemptId");
        Objects.requireNonNull(deadline, "deadline");
        return deadline.bound(
                        () -> metadataStore.getRetirementManifest(
                                cluster, exactObject, exactAttempt),
                        "load GC retirement-journal manifest")
                .thenCompose(optional -> optional
                        .<CompletableFuture<Optional<GcRetirementJournalSnapshot>>>map(manifest -> {
                            if (!manifest.value().objectKeyHash().equals(exactObject.value())
                                    || !manifest.value().gcAttemptId().equals(exactAttempt)) {
                                return CompletableFuture.failedFuture(invariant(
                                        "GC retirement manifest escaped its object/attempt key"));
                            }
                            CompletableFuture<List<VersionedGcRetirementProtection>> protections =
                                    scanProtectionEntries(
                                            exactObject,
                                            exactAttempt,
                                            manifest.value().protectionCount(),
                                            deadline);
                            CompletableFuture<List<VersionedGcRetirementRemoval>> removals =
                                    scanRemovalEntries(
                                            exactObject,
                                            exactAttempt,
                                            manifest.value().metadataRemovalCount(),
                                            deadline);
                            return protections.thenCombine(removals, (protectionValues, removalValues) ->
                                            new GcRetirementJournalSnapshot(
                                                    manifest, protectionValues, removalValues))
                                    .thenApply(Optional::of);
                        })
                        .orElseGet(() -> CompletableFuture.completedFuture(Optional.empty())));
    }

    private CompletableFuture<GcRetirementJournalSnapshot> writeAndSeal(
            Preparation preparation,
            MaterializationDeadline deadline) {
        List<Throwable> writeFailures = Collections.synchronizedList(new ArrayList<>());
        CompletableFuture<Void> writes = writeBatches(
                        preparation.protectionRecords(),
                        value -> deadline.bound(
                                () -> metadataStore.createRetirementProtection(cluster, value),
                                "create GC retirement-journal protection"),
                        writeFailures)
                .thenCompose(ignored -> writeBatches(
                        preparation.removalRecords(),
                        value -> deadline.bound(
                                () -> metadataStore.createRetirementRemoval(cluster, value),
                                "create GC retirement-journal removal"),
                        writeFailures));
        return writes.thenCompose(ignored -> scanExactEntries(preparation, deadline))
                .thenCompose(entries -> {
                    Throwable mismatch = entryMismatch(preparation, entries);
                    if (mismatch != null) {
                        if (!writeFailures.isEmpty()) {
                            Throwable first = unwrap(writeFailures.get(0));
                            first.addSuppressed(mismatch);
                            return CompletableFuture.failedFuture(first);
                        }
                        return CompletableFuture.failedFuture(mismatch);
                    }
                    return sealManifest(preparation, deadline);
                });
    }

    private CompletableFuture<GcRetirementJournalSnapshot> sealManifest(
            Preparation preparation,
            MaterializationDeadline deadline) {
        GcRetirementManifestRecord manifest = new GcRetirementManifestRecord(
                1,
                preparation.object().value(),
                preparation.gcAttemptId(),
                GcRetirementManifestRecord.REFERENCE_SET_PROTOCOL_VERSION,
                preparation.queryIdentitySha256().value(),
                preparation.domainProofs().stream()
                        .map(DefaultGcRetirementJournal::proofRecord)
                        .toList(),
                preparation.protections().size(),
                preparation.removals().size(),
                preparation.referenceSetSha256().value(),
                preparation.createdAtMillis(),
                0);
        return deadline.bound(
                        () -> metadataStore.createRetirementManifest(cluster, manifest),
                        "seal GC retirement-journal manifest")
                .handle((ignored, failure) -> failure)
                .thenCompose(createFailure -> load(
                                preparation.object(), preparation.gcAttemptId(), deadline)
                        .thenCompose(optional -> {
                            if (optional.isPresent()) {
                                return requireExpected(optional.orElseThrow(), preparation);
                            }
                            if (createFailure != null) {
                                return CompletableFuture.failedFuture(unwrap(createFailure));
                            }
                            return CompletableFuture.failedFuture(invariant(
                                    "sealed GC retirement manifest disappeared on reload"));
                        }));
    }

    private CompletableFuture<EntrySet> scanExactEntries(
            Preparation preparation,
            MaterializationDeadline deadline) {
        CompletableFuture<List<VersionedGcRetirementProtection>> protections =
                scanProtectionEntries(
                        preparation.object(),
                        preparation.gcAttemptId(),
                        preparation.protections().size(),
                        deadline);
        CompletableFuture<List<VersionedGcRetirementRemoval>> removals = scanRemovalEntries(
                preparation.object(),
                preparation.gcAttemptId(),
                preparation.removals().size(),
                deadline);
        return protections.thenCombine(removals, EntrySet::new);
    }

    private Throwable entryMismatch(Preparation preparation, EntrySet entries) {
        try {
            List<GcPlannedProtectionRemoval> protections = GcPlanValidation.canonicalAllowEmpty(
                    entries.protections().stream()
                            .map(GcRetirementJournalSnapshot::plannedProtection)
                            .sorted(GcPlanValidation.PROTECTION_ORDER)
                            .toList(),
                    GcPlanValidation.PROTECTION_ORDER,
                    GcRetirementManifestRecord.MAX_PLAN_ENTRIES,
                    "journalProtections");
            List<GcPlannedMetadataRemoval> removals = GcPlanValidation.canonicalAllowEmpty(
                    entries.removals().stream()
                            .map(GcRetirementJournalSnapshot::plannedRemoval)
                            .sorted(GcPlanValidation.METADATA_ORDER)
                            .toList(),
                    GcPlanValidation.METADATA_ORDER,
                    GcRetirementManifestRecord.MAX_PLAN_ENTRIES,
                    "journalRemovals");
            if (!protections.equals(preparation.protections())
                    || !removals.equals(preparation.removals())) {
                return invariant(
                        "GC retirement journal entries differ from the exact source plan");
            }
            Checksum digest = GcPlanValidation.referenceSetSha256(
                    preparation.queryIdentitySha256(),
                    preparation.domainProofs(),
                    protections,
                    removals);
            if (!digest.equals(preparation.referenceSetSha256())) {
                return invariant(
                        "GC retirement journal entries do not reproduce the reference-set digest");
            }
            return null;
        } catch (Throwable failure) {
            return failure;
        }
    }

    private CompletableFuture<List<VersionedGcRetirementProtection>> scanProtectionEntries(
            ObjectKeyHash object,
            String gcAttemptId,
            int expectedCount,
            MaterializationDeadline deadline) {
        return scanProtectionEntries(
                object,
                gcAttemptId,
                expectedCount,
                Optional.empty(),
                new ArrayList<>(),
                null,
                deadline);
    }

    private CompletableFuture<List<VersionedGcRetirementProtection>> scanProtectionEntries(
            ObjectKeyHash object,
            String gcAttemptId,
            int expectedCount,
            Optional<F4ScanToken> continuation,
            ArrayList<VersionedGcRetirementProtection> values,
            String lastKey,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> metadataStore.scanRetirementProtections(
                                cluster,
                                object,
                                gcAttemptId,
                                continuation,
                                config.metadataScanPageSize()),
                        "scan GC retirement-journal protections")
                .thenCompose(page -> {
                    requireIncreasing(page, lastKey);
                    for (VersionedGcRetirementProtection value : page.values()) {
                        requireEntryIdentity(
                                object,
                                gcAttemptId,
                                value.value().objectKeyHash(),
                                value.value().gcAttemptId());
                        values.add(value);
                        if (values.size() > expectedCount) {
                            return CompletableFuture.failedFuture(invariant(
                                    "GC retirement journal has extra protection entries"));
                        }
                    }
                    if (page.continuation().isEmpty()) {
                        if (values.size() != expectedCount) {
                            return CompletableFuture.failedFuture(invariant(
                                    "GC retirement journal is missing protection entries"));
                        }
                        return CompletableFuture.completedFuture(List.copyOf(values));
                    }
                    String nextLast = page.values().get(page.values().size() - 1).key();
                    return scanProtectionEntries(
                            object,
                            gcAttemptId,
                            expectedCount,
                            page.continuation(),
                            values,
                            nextLast,
                            deadline);
                });
    }

    private CompletableFuture<List<VersionedGcRetirementRemoval>> scanRemovalEntries(
            ObjectKeyHash object,
            String gcAttemptId,
            int expectedCount,
            MaterializationDeadline deadline) {
        return scanRemovalEntries(
                object,
                gcAttemptId,
                expectedCount,
                Optional.empty(),
                new ArrayList<>(),
                null,
                deadline);
    }

    private CompletableFuture<List<VersionedGcRetirementRemoval>> scanRemovalEntries(
            ObjectKeyHash object,
            String gcAttemptId,
            int expectedCount,
            Optional<F4ScanToken> continuation,
            ArrayList<VersionedGcRetirementRemoval> values,
            String lastKey,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> metadataStore.scanRetirementRemovals(
                                cluster,
                                object,
                                gcAttemptId,
                                continuation,
                                config.metadataScanPageSize()),
                        "scan GC retirement-journal removals")
                .thenCompose(page -> {
                    requireIncreasing(page, lastKey);
                    for (VersionedGcRetirementRemoval value : page.values()) {
                        requireEntryIdentity(
                                object,
                                gcAttemptId,
                                value.value().objectKeyHash(),
                                value.value().gcAttemptId());
                        values.add(value);
                        if (values.size() > expectedCount) {
                            return CompletableFuture.failedFuture(invariant(
                                    "GC retirement journal has extra removal entries"));
                        }
                    }
                    if (page.continuation().isEmpty()) {
                        if (values.size() != expectedCount) {
                            return CompletableFuture.failedFuture(invariant(
                                    "GC retirement journal is missing removal entries"));
                        }
                        return CompletableFuture.completedFuture(List.copyOf(values));
                    }
                    String nextLast = page.values().get(page.values().size() - 1).key();
                    return scanRemovalEntries(
                            object,
                            gcAttemptId,
                            expectedCount,
                            page.continuation(),
                            values,
                            nextLast,
                            deadline);
                });
    }

    private <T> CompletableFuture<Void> writeBatches(
            List<T> values,
            Function<T, CompletableFuture<?>> operation,
            List<Throwable> failures) {
        int batchSize = Math.min(config.maxConcurrentDeletes(), 1_000);
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int start = 0; start < values.size(); start += batchSize) {
            int from = start;
            int to = Math.min(values.size(), start + batchSize);
            chain = chain.thenCompose(ignored -> {
                List<CompletableFuture<?>> batch = new ArrayList<>(to - from);
                for (T value : values.subList(from, to)) {
                    CompletableFuture<?> future;
                    try {
                        future = Objects.requireNonNull(
                                operation.apply(value), "journal write future");
                    } catch (Throwable failure) {
                        failures.add(failure);
                        continue;
                    }
                    batch.add(future.handle((result, failure) -> {
                        if (failure != null) {
                            failures.add(failure);
                        }
                        return null;
                    }));
                }
                return CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new));
            });
        }
        return chain;
    }

    private Preparation preparation(
            String gcAttemptId,
            GcCandidate candidate,
            List<GcReferenceSnapshot> domainSnapshots,
            List<GcPlannedProtectionRemoval> plannedProtectionRemovals,
            List<GcPlannedMetadataRemoval> plannedMetadataRemovals,
            Checksum referenceSetSha256,
            long createdAtMillis) {
        String attempt = GcPlanValidation.requireBase32Id(gcAttemptId, "gcAttemptId");
        GcCandidate exactCandidate = Objects.requireNonNull(candidate, "candidate");
        if (createdAtMillis < 0) {
            throw new IllegalArgumentException("createdAtMillis must be non-negative");
        }
        List<GcReferenceSnapshot> snapshots = GcPlanValidation.canonical(
                domainSnapshots,
                GcPlanValidation.DOMAIN_ORDER,
                GcPlanValidation.MAX_REFERENCE_DOMAINS,
                "domainSnapshots");
        List<GcPlannedProtectionRemoval> protections = GcPlanValidation.canonicalAllowEmpty(
                plannedProtectionRemovals,
                GcPlanValidation.PROTECTION_ORDER,
                config.maxReferencesPerDomainSnapshot(),
                "plannedProtectionRemovals");
        List<GcPlannedMetadataRemoval> removals = GcPlanValidation.canonicalAllowEmpty(
                plannedMetadataRemovals,
                GcPlanValidation.METADATA_ORDER,
                config.maxReferencesPerDomainSnapshot(),
                "plannedMetadataRemovals");
        Checksum computed = GcPlan.computeReferenceSetSha256(
                config, exactCandidate, snapshots, protections, removals);
        if (!computed.equals(referenceSetSha256)) {
            throw new IllegalArgumentException(
                    "referenceSetSha256 does not match the journal source plan");
        }
        List<GcDomainSnapshotProof> proofs = snapshots.stream()
                .map(GcDomainSnapshotProof::from)
                .toList();
        List<GcRetirementProtectionRecord> protectionRecords = protections.stream()
                .map(value -> protectionRecord(attempt, exactCandidate.object().objectKeyHash(), value))
                .toList();
        List<GcRetirementRemovalRecord> removalRecords = removals.stream()
                .map(value -> removalRecord(attempt, exactCandidate.object().objectKeyHash(), value))
                .toList();
        return new Preparation(
                attempt,
                exactCandidate.object().objectKeyHash(),
                exactCandidate.referenceQuery().queryIdentitySha256(),
                proofs,
                protections,
                removals,
                protectionRecords,
                removalRecords,
                computed,
                createdAtMillis);
    }

    private static CompletableFuture<GcRetirementJournalSnapshot> requireExpected(
            GcRetirementJournalSnapshot snapshot,
            Preparation expected) {
        if (!snapshot.object().equals(expected.object())
                || !snapshot.gcAttemptId().equals(expected.gcAttemptId())
                || !snapshot.queryIdentitySha256().equals(expected.queryIdentitySha256())
                || !snapshot.domainProofs().equals(expected.domainProofs())
                || !snapshot.plannedProtectionRemovals().equals(expected.protections())
                || !snapshot.plannedMetadataRemovals().equals(expected.removals())
                || !snapshot.referenceSetSha256().equals(expected.referenceSetSha256())) {
            return CompletableFuture.failedFuture(invariant(
                    "existing GC retirement journal conflicts with the exact source plan"));
        }
        return CompletableFuture.completedFuture(snapshot);
    }

    private static GcRetirementProtectionRecord protectionRecord(
            String attempt,
            ObjectKeyHash object,
            GcPlannedProtectionRemoval removal) {
        return new GcRetirementProtectionRecord(
                1,
                object.value(),
                attempt,
                removal.protection().key(),
                removal.protection().metadataVersion(),
                removal.protection().durableValueSha256().value(),
                removal.protection().value(),
                0);
    }

    private static GcRetirementRemovalRecord removalRecord(
            String attempt,
            ObjectKeyHash object,
            GcPlannedMetadataRemoval removal) {
        return new GcRetirementRemovalRecord(
                1,
                object.value(),
                attempt,
                removal.removalType(),
                removal.key(),
                removal.metadataVersion(),
                removal.durableValueSha256().value(),
                0);
    }

    private static GcDomainSnapshotProofRecord proofRecord(GcDomainSnapshotProof proof) {
        return new GcDomainSnapshotProofRecord(
                proof.domainId(),
                proof.protocolVersion(),
                proof.queryIdentitySha256().value(),
                proof.snapshotSha256().value());
    }

    private static void requireIncreasing(
            GcRetirementProtectionScanPage page, String lastKey) {
        if (lastKey != null && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(lastKey) <= 0) {
            throw invariant("GC retirement-protection scan did not advance monotonically");
        }
    }

    private static void requireIncreasing(GcRetirementRemovalScanPage page, String lastKey) {
        if (lastKey != null && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(lastKey) <= 0) {
            throw invariant("GC retirement-removal scan did not advance monotonically");
        }
    }

    private static void requireEntryIdentity(
            ObjectKeyHash object,
            String attempt,
            String entryObject,
            String entryAttempt) {
        if (!object.value().equals(entryObject) || !attempt.equals(entryAttempt)) {
            throw invariant("GC retirement scan escaped its object/attempt identity");
        }
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private record Preparation(
            String gcAttemptId,
            ObjectKeyHash object,
            Checksum queryIdentitySha256,
            List<GcDomainSnapshotProof> domainProofs,
            List<GcPlannedProtectionRemoval> protections,
            List<GcPlannedMetadataRemoval> removals,
            List<GcRetirementProtectionRecord> protectionRecords,
            List<GcRetirementRemovalRecord> removalRecords,
            Checksum referenceSetSha256,
            long createdAtMillis) {
        private Preparation {
            domainProofs = List.copyOf(domainProofs);
            protections = List.copyOf(protections);
            removals = List.copyOf(removals);
            protectionRecords = List.copyOf(protectionRecords);
            removalRecords = List.copyOf(removalRecords);
        }
    }

    private record EntrySet(
            List<VersionedGcRetirementProtection> protections,
            List<VersionedGcRetirementRemoval> removals) {
        private EntrySet {
            protections = List.copyOf(protections);
            removals = List.copyOf(removals);
        }
    }
}
