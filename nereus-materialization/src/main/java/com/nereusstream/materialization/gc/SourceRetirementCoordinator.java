/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.ObjectProtectionIdentity;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.DeleteObjectResult;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Restart-safe destructive recovery for one root that already carries durable DELETING intent. */
public final class SourceRetirementCoordinator {
    private final String cluster;
    private final PhysicalGcConfig config;
    private final PhysicalObjectMetadataStore metadataStore;
    private final GcRetirementJournal retirementJournal;
    private final GcMetadataRetirementRegistry metadataRetirements;
    private final ObjectStore objectStore;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;

    public SourceRetirementCoordinator(
            String cluster,
            PhysicalGcConfig config,
            PhysicalObjectMetadataStore metadataStore,
            GcRetirementJournal retirementJournal,
            GcMetadataRetirementRegistry metadataRetirements,
            ObjectStore objectStore,
            Clock clock,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.retirementJournal = Objects.requireNonNull(
                retirementJournal, "retirementJournal");
        this.metadataRetirements = Objects.requireNonNull(
                metadataRetirements, "metadataRetirements");
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    public CompletableFuture<PhysicalGcDeletionResult> resume(
            VersionedPhysicalObjectRoot discoveredRoot) {
        VersionedPhysicalObjectRoot discovered = Objects.requireNonNull(
                discoveredRoot, "discoveredRoot");
        if (discovered.value().lifecycle() != PhysicalObjectLifecycle.DELETING
                && discovered.value().lifecycle() != PhysicalObjectLifecycle.DELETED) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "destructive GC recovery requires a DELETING or DELETED root"));
        }
        if (!config.enabled()) {
            return CompletableFuture.completedFuture(
                    PhysicalGcDeletionResult.simple(PhysicalGcDeletionStatus.DISABLED));
        }
        if (config.dryRun()) {
            return CompletableFuture.completedFuture(
                    PhysicalGcDeletionResult.simple(PhysicalGcDeletionStatus.DRY_RUN));
        }
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        CompletableFuture<PhysicalGcDeletionResult> result = resume(
                discovered, nonNegativeNow(), deadline);
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    private CompletableFuture<PhysicalGcDeletionResult> resume(
            VersionedPhysicalObjectRoot discovered,
            long now,
            MaterializationDeadline deadline) {
        ObjectKeyHash object = new ObjectKeyHash(discovered.value().objectKeyHash());
        return deadline.bound(
                        () -> metadataStore.getRoot(cluster, object),
                        "reload destructive GC root")
                .thenCompose(optional -> {
                    if (optional.isEmpty()) {
                        return CompletableFuture.completedFuture(
                                PhysicalGcDeletionResult.simple(
                                        PhysicalGcDeletionStatus.ROOT_CHANGED));
                    }
                    VersionedPhysicalObjectRoot current = optional.orElseThrow();
                    if (discovered.value().lifecycle() == PhysicalObjectLifecycle.DELETED) {
                        return CompletableFuture.completedFuture(
                                exactObservedDeleted(discovered, current)
                                        ? PhysicalGcDeletionResult.alreadyDeleted(current)
                                        : PhysicalGcDeletionResult.simple(
                                                PhysicalGcDeletionStatus.ROOT_CHANGED));
                    }
                    if (exactDeletedAttempt(discovered, current)) {
                        return CompletableFuture.completedFuture(
                                PhysicalGcDeletionResult.alreadyDeleted(current));
                    }
                    if (!exactDeletingAttempt(discovered, current)) {
                        return CompletableFuture.completedFuture(
                                PhysicalGcDeletionResult.simple(
                                        PhysicalGcDeletionStatus.ROOT_CHANGED));
                    }
                    return loadExactJournal(current, deadline)
                            .thenCompose(journal -> retireAndDelete(
                                    current, journal, now, deadline));
                });
    }

    private CompletableFuture<GcRetirementJournalSnapshot> loadExactJournal(
            VersionedPhysicalObjectRoot root,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> retirementJournal.load(
                                new ObjectKeyHash(root.value().objectKeyHash()),
                                root.value().gcAttemptId(),
                                deadline),
                        "load root-authenticated destructive GC journal")
                .thenApply(optional -> {
                    GcRetirementJournalSnapshot journal = optional.orElseThrow(() -> invariant(
                            "DELETING root is missing its sealed retirement journal"));
                    requireJournalMatchesRoot(root, journal);
                    metadataRetirements.requireSupports(
                            journal.plannedMetadataRemovals());
                    return journal;
                });
    }

    private CompletableFuture<PhysicalGcDeletionResult> retireAndDelete(
            VersionedPhysicalObjectRoot root,
            GcRetirementJournalSnapshot journal,
            long now,
            MaterializationDeadline deadline) {
        GcMetadataRetirementContext context = new GcMetadataRetirementContext(root, journal);
        RetirementCounts counts = new RetirementCounts();
        return retireMetadata(context, journal.plannedMetadataRemovals(), counts, deadline)
                .thenCompose(ignored -> reauthenticateContext(context, deadline))
                .thenCompose(authenticated -> retireProtections(
                        authenticated,
                        journal.plannedProtectionRemovals(),
                        counts,
                        deadline))
                .thenCompose(ignored -> reauthenticateContext(context, deadline))
                .thenCompose(authenticated -> deletePhysicalObject(authenticated, deadline)
                        .thenCompose(objectStatus -> completeDeletedRoot(
                                authenticated,
                                objectStatus,
                                counts,
                                now,
                                deadline)));
    }

    private CompletableFuture<Void> retireMetadata(
            GcMetadataRetirementContext context,
            List<GcPlannedMetadataRemoval> removals,
            RetirementCounts counts,
            MaterializationDeadline deadline) {
        return runAuthenticatedBatches(
                context,
                removals,
                deadline,
                (authenticated, removal) -> metadataRetirements.retire(
                                authenticated, removal, deadline)
                        .thenAccept(outcome -> {
                            if (outcome == GcMetadataRetirementOutcome.RETIRED) {
                                counts.metadataRetired.incrementAndGet();
                            } else {
                                counts.metadataAbsent.incrementAndGet();
                            }
                        }));
    }

    private CompletableFuture<Void> retireProtections(
            GcMetadataRetirementContext context,
            List<GcPlannedProtectionRemoval> planned,
            RetirementCounts counts,
            MaterializationDeadline deadline) {
        VersionedPhysicalObjectRoot root = context.deletingRoot();
        return scanProtections(new ObjectKeyHash(root.value().objectKeyHash()), deadline)
                .thenCompose(current -> {
                    Map<String, GcPlannedProtectionRemoval> expected = new HashMap<>();
                    for (GcPlannedProtectionRemoval removal : planned) {
                        if (expected.put(removal.protection().key(), removal) != null) {
                            throw invariant("journal contains duplicate protection keys");
                        }
                    }
                    for (VersionedObjectProtection protection : current) {
                        GcPlannedProtectionRemoval removal = expected.get(protection.key());
                        if (removal == null) {
                            throw invariant(
                                    "DELETING root has an unjournaled object protection");
                        }
                        if (!removal.protection().equals(protection)) {
                            throw invariant(
                                    "journaled object protection changed before retirement");
                        }
                    }
                    Map<String, VersionedObjectProtection> currentByKey = new HashMap<>();
                    current.forEach(value -> currentByKey.put(value.key(), value));
                    return runAuthenticatedBatches(
                            context, planned, deadline, (authenticated, removal) -> {
                                VersionedObjectProtection value = currentByKey.get(
                                        removal.protection().key());
                                if (value == null) {
                                    counts.protectionsAbsent.incrementAndGet();
                                    return CompletableFuture.completedFuture(null);
                                }
                                return deleteProtection(authenticated, removal, deadline)
                                        .thenAccept(outcome -> {
                                            if (outcome
                                                    == GcMetadataRetirementOutcome.RETIRED) {
                                                counts.protectionsRetired.incrementAndGet();
                                            } else {
                                                counts.protectionsAbsent.incrementAndGet();
                                            }
                                        });
                            });
                })
                .thenCompose(ignored -> reauthenticateContext(context, deadline))
                .thenCompose(authenticated -> scanProtections(
                        new ObjectKeyHash(
                                authenticated.deletingRoot().value().objectKeyHash()),
                        deadline))
                .thenAccept(remaining -> {
                    if (!remaining.isEmpty()) {
                        throw invariant(
                                "object protections remain after journaled retirement");
                    }
                });
    }

    private CompletableFuture<GcMetadataRetirementOutcome> deleteProtection(
            GcMetadataRetirementContext context,
            GcPlannedProtectionRemoval removal,
            MaterializationDeadline deadline) {
        VersionedObjectProtection planned = removal.protection();
        ObjectProtectionIdentity identity = removal.identity();
        CompletableFuture<Void> delete = deadline.bound(
                () -> metadataStore.deleteProtection(
                        cluster, identity, planned.metadataVersion()),
                "conditionally delete journaled object protection");
        return delete.handle((ignored, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(
                        GcMetadataRetirementOutcome.RETIRED);
            }
            Throwable original = unwrap(failure);
            return reauthenticateContext(context, deadline)
                    .thenCompose(authenticated -> findProtection(
                            identity.object(), planned.key(), deadline))
                    .thenCompose(current -> {
                        if (current.isEmpty()) {
                            return CompletableFuture.completedFuture(
                                    GcMetadataRetirementOutcome.ALREADY_ABSENT);
                        }
                        if (current.orElseThrow().equals(planned)) {
                            return CompletableFuture.failedFuture(original);
                        }
                        return CompletableFuture.failedFuture(invariant(
                                "object protection changed after uncertain delete"));
                    });
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<DeleteObjectResult.Status> deletePhysicalObject(
            GcMetadataRetirementContext context,
            MaterializationDeadline deadline) {
        VersionedPhysicalObjectRoot root = context.deletingRoot();
        PhysicalObjectIdentity object = PhysicalObjectIdentity.from(root.value());
        return headExact(object, deadline).thenCompose(head -> {
            if (head.isEmpty()) {
                return CompletableFuture.completedFuture(
                        DeleteObjectResult.Status.ALREADY_ABSENT);
            }
            CompletableFuture<DeleteObjectResult> delete = deadline.bound(
                    () -> objectStore.deleteObject(
                            object.objectKey(),
                            new DeleteObjectOptions(
                                    object.objectLength(),
                                    object.storageChecksum(),
                                    object.etag(),
                                    deadline.remaining())),
                    "delete exact immutable physical object");
            return delete.handle((result, failure) -> {
                if (failure == null) {
                    if (!result.key().equals(object.objectKey())) {
                        return CompletableFuture.<DeleteObjectResult.Status>failedFuture(
                                invariant("object delete returned a different key"));
                    }
                    return CompletableFuture.completedFuture(result.status());
                }
                Throwable original = unwrap(failure);
                return reauthenticateContext(context, deadline)
                        .thenCompose(authenticated -> headExact(object, deadline))
                        .thenCompose(reloaded -> reloaded.isEmpty()
                                ? CompletableFuture.completedFuture(
                                        DeleteObjectResult.Status.ALREADY_ABSENT)
                                : CompletableFuture.failedFuture(original));
            }).thenCompose(Function.identity());
        });
    }

    private CompletableFuture<PhysicalGcDeletionResult> completeDeletedRoot(
            GcMetadataRetirementContext context,
            DeleteObjectResult.Status objectStatus,
            RetirementCounts counts,
            long observedNow,
            MaterializationDeadline deadline) {
        VersionedPhysicalObjectRoot deletingRoot = context.deletingRoot();
        return reloadExactDeleting(deletingRoot, deadline).thenCompose(optional -> {
            if (optional.isEmpty()) {
                return deadline.bound(
                                () -> metadataStore.getRoot(
                                        cluster,
                                        new ObjectKeyHash(
                                                deletingRoot.value().objectKeyHash())),
                                "reload root after physical object deletion")
                        .thenApply(current -> current
                                .filter(value -> exactDeletedAttempt(
                                        deletingRoot, value))
                                .map(PhysicalGcDeletionResult::alreadyDeleted)
                                .orElseGet(() -> PhysicalGcDeletionResult.simple(
                                        PhysicalGcDeletionStatus.ROOT_CHANGED)));
            }
            VersionedPhysicalObjectRoot exact = optional.orElseThrow();
            return loadExactJournal(exact, deadline).thenCompose(reloadedJournal -> {
                requireSameJournal(context.journal(), reloadedJournal);
                long deletedAt = Math.max(observedNow, exact.value().deleteStartedAtMillis());
                PhysicalObjectRootRecord replacement = deleted(exact.value(), deletedAt);
                CompletableFuture<VersionedPhysicalObjectRoot> cas = deadline.bound(
                        () -> metadataStore.compareAndSetRoot(
                                cluster, replacement, exact.metadataVersion()),
                        "CAS DELETING physical root to DELETED");
                return cas.handle((deleted, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(result(
                                deleted, objectStatus, counts));
                    }
                    Throwable original = unwrap(failure);
                    return deadline.bound(
                                    () -> metadataStore.getRoot(
                                            cluster,
                                            new ObjectKeyHash(
                                                    exact.value().objectKeyHash())),
                                    "reload physical root after uncertain DELETED CAS")
                            .thenCompose(reloaded -> {
                                if (reloaded.isPresent()
                                        && exactReplacement(
                                                reloaded.orElseThrow(), replacement)) {
                                    return CompletableFuture.completedFuture(result(
                                            reloaded.orElseThrow(), objectStatus, counts));
                                }
                                if (reloaded.isPresent()
                                        && exactDeletingAttempt(
                                                exact, reloaded.orElseThrow())) {
                                    return CompletableFuture.failedFuture(original);
                                }
                                return CompletableFuture.completedFuture(
                                        PhysicalGcDeletionResult.simple(
                                                PhysicalGcDeletionStatus.ROOT_CHANGED));
                            });
                }).thenCompose(Function.identity());
            });
        });
    }

    private PhysicalGcDeletionResult result(
            VersionedPhysicalObjectRoot root,
            DeleteObjectResult.Status objectStatus,
            RetirementCounts counts) {
        return new PhysicalGcDeletionResult(
                PhysicalGcDeletionStatus.DELETED,
                Optional.of(root),
                counts.metadataRetired.get(),
                counts.metadataAbsent.get(),
                counts.protectionsRetired.get(),
                counts.protectionsAbsent.get(),
                Optional.of(objectStatus));
    }

    private CompletableFuture<Optional<HeadObjectResult>> headExact(
            PhysicalObjectIdentity object,
            MaterializationDeadline deadline) {
        CompletableFuture<HeadObjectResult> head = deadline.bound(
                () -> objectStore.headObject(
                        object.objectKey(),
                        new HeadObjectOptions(deadline.remaining())),
                "HEAD exact immutable physical object");
        return head.handle((value, failure) -> {
            if (failure == null) {
                requireExactHead(object, value);
                return Optional.of(value);
            }
            Throwable exact = unwrap(failure);
            if (exact instanceof NereusException nereus
                    && nereus.code() == ErrorCode.OBJECT_NOT_FOUND) {
                return Optional.empty();
            }
            throw propagate(exact);
        });
    }

    private static void requireExactHead(
            PhysicalObjectIdentity expected,
            HeadObjectResult actual) {
        boolean etagMatches = expected.etag().isEmpty()
                || expected.etag().equals(actual.etag());
        if (!actual.key().equals(expected.objectKey())
                || actual.objectLength() != expected.objectLength()
                || !actual.checksum().equals(expected.storageChecksum())
                || !etagMatches) {
            throw invariant("physical object HEAD does not match the immutable root");
        }
    }

    private CompletableFuture<List<VersionedObjectProtection>> scanProtections(
            ObjectKeyHash object,
            MaterializationDeadline deadline) {
        return scanProtections(
                object, Optional.empty(), new ArrayList<>(), null, deadline);
    }

    private CompletableFuture<List<VersionedObjectProtection>> scanProtections(
            ObjectKeyHash object,
            Optional<F4ScanToken> continuation,
            ArrayList<VersionedObjectProtection> values,
            String previousKey,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> metadataStore.scanProtections(
                                cluster,
                                object,
                                continuation,
                                config.metadataScanPageSize()),
                        "scan protections during destructive GC recovery")
                .thenCompose(page -> {
                    requireIncreasing(page, previousKey);
                    for (VersionedObjectProtection value : page.values()) {
                        if (!value.value().objectKeyHash().equals(object.value())) {
                            return CompletableFuture.failedFuture(invariant(
                                    "protection scan escaped the DELETING object"));
                        }
                        values.add(value);
                        if (values.size() > config.maxReferencesPerDomainSnapshot()) {
                            return CompletableFuture.failedFuture(invariant(
                                    "protection scan exceeded destructive recovery bounds"));
                        }
                    }
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(List.copyOf(values));
                    }
                    String next = page.values().get(page.values().size() - 1).key();
                    return scanProtections(
                            object, page.continuation(), values, next, deadline);
                });
    }

    private CompletableFuture<Optional<VersionedObjectProtection>> findProtection(
            ObjectKeyHash object,
            String key,
            MaterializationDeadline deadline) {
        return scanProtections(object, deadline).thenApply(values -> values.stream()
                .filter(value -> value.key().equals(key))
                .findFirst());
    }

    private CompletableFuture<Optional<VersionedPhysicalObjectRoot>> reloadExactDeleting(
            VersionedPhysicalObjectRoot expected,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> metadataStore.getRoot(
                                cluster,
                                new ObjectKeyHash(expected.value().objectKeyHash())),
                        "reload exact DELETING physical root")
                .thenApply(optional -> optional.filter(root ->
                        exactDeletingAttempt(expected, root)));
    }

    private CompletableFuture<GcMetadataRetirementContext> reauthenticateContext(
            GcMetadataRetirementContext expected,
            MaterializationDeadline deadline) {
        return reloadExactDeleting(expected.deletingRoot(), deadline)
                .thenCompose(reloaded -> reloaded
                        .<CompletableFuture<GcMetadataRetirementContext>>map(root ->
                                loadExactJournal(root, deadline).thenApply(journal -> {
                                    requireSameJournal(expected.journal(), journal);
                                    return new GcMetadataRetirementContext(root, journal);
                                }))
                        .orElseGet(() -> CompletableFuture.failedFuture(invariant(
                                "DELETING root changed during destructive recovery"))));
    }

    private <T> CompletableFuture<Void> runAuthenticatedBatches(
            GcMetadataRetirementContext context,
            List<T> values,
            MaterializationDeadline deadline,
            BiFunction<GcMetadataRetirementContext, T, CompletableFuture<?>> operation) {
        int batchSize = Math.min(config.maxConcurrentDeletes(), 1_000);
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int start = 0; start < values.size(); start += batchSize) {
            int from = start;
            int to = Math.min(values.size(), start + batchSize);
            chain = chain.thenCompose(ignored -> reauthenticateContext(context, deadline))
                    .thenCompose(authenticated -> {
                        List<CompletableFuture<?>> batch = new ArrayList<>(to - from);
                        for (T value : values.subList(from, to)) {
                            batch.add(Objects.requireNonNull(
                                    operation.apply(authenticated, value),
                                    "retirement operation future"));
                        }
                        return CompletableFuture.allOf(
                                batch.toArray(CompletableFuture[]::new));
                    });
        }
        return chain;
    }

    private static void requireJournalMatchesRoot(
            VersionedPhysicalObjectRoot root,
            GcRetirementJournalSnapshot journal) {
        if (!root.value().objectKeyHash().equals(journal.object().value())
                || !root.value().gcAttemptId().equals(journal.gcAttemptId())
                || !root.value().referenceSetSha256().equals(
                        journal.referenceSetSha256().value())) {
            throw invariant(
                    "retirement journal does not match the DELETING root authority");
        }
    }

    private static void requireSameJournal(
            GcRetirementJournalSnapshot expected,
            GcRetirementJournalSnapshot actual) {
        if (!expected.manifest().equals(actual.manifest())
                || !expected.protectionEntries().equals(actual.protectionEntries())
                || !expected.removalEntries().equals(actual.removalEntries())) {
            throw invariant("sealed retirement journal changed during destructive recovery");
        }
    }

    private static boolean exactDeletingAttempt(
            VersionedPhysicalObjectRoot expected,
            VersionedPhysicalObjectRoot actual) {
        return actual.value().lifecycle() == PhysicalObjectLifecycle.DELETING
                && exactAttemptIdentity(expected, actual)
                && actual.metadataVersion() == expected.metadataVersion()
                && actual.durableValueSha256().equals(expected.durableValueSha256())
                && actual.value().lifecycleEpoch() == expected.value().lifecycleEpoch()
                && actual.value().deleteStartedAtMillis()
                        == expected.value().deleteStartedAtMillis();
    }

    private static boolean exactDeletedAttempt(
            VersionedPhysicalObjectRoot expected,
            VersionedPhysicalObjectRoot actual) {
        return actual.value().lifecycle() == PhysicalObjectLifecycle.DELETED
                && exactAttemptIdentity(expected, actual)
                && actual.value().lifecycleEpoch()
                        == Math.addExact(expected.value().lifecycleEpoch(), 1)
                && actual.value().deleteStartedAtMillis()
                        == expected.value().deleteStartedAtMillis();
    }

    private static boolean exactObservedDeleted(
            VersionedPhysicalObjectRoot expected,
            VersionedPhysicalObjectRoot actual) {
        return expected.value().lifecycle() == PhysicalObjectLifecycle.DELETED
                && actual.value().lifecycle() == PhysicalObjectLifecycle.DELETED
                && exactAttemptIdentity(expected, actual)
                && actual.value().lifecycleEpoch() == expected.value().lifecycleEpoch()
                && actual.value().deleteStartedAtMillis()
                        == expected.value().deleteStartedAtMillis()
                && actual.value().deletedAtMillis() == expected.value().deletedAtMillis();
    }

    private static boolean exactAttemptIdentity(
            VersionedPhysicalObjectRoot expected,
            VersionedPhysicalObjectRoot actual) {
        return PhysicalObjectIdentity.from(expected.value()).equals(
                        PhysicalObjectIdentity.from(actual.value()))
                && expected.value().gcAttemptId().equals(actual.value().gcAttemptId())
                && expected.value().referenceSetSha256().equals(
                        actual.value().referenceSetSha256())
                && expected.value().markedAtMillis() == actual.value().markedAtMillis()
                && expected.value().deleteNotBeforeMillis()
                        == actual.value().deleteNotBeforeMillis();
    }

    private static boolean exactReplacement(
            VersionedPhysicalObjectRoot actual,
            PhysicalObjectRootRecord replacement) {
        return actual.value().withMetadataVersion(0).equals(replacement);
    }

    private static PhysicalObjectRootRecord deleted(
            PhysicalObjectRootRecord current,
            long deletedAtMillis) {
        return new PhysicalObjectRootRecord(
                current.schemaVersion(),
                current.objectKeyHash(),
                current.objectKey(),
                current.objectId(),
                current.objectKindId(),
                current.objectLength(),
                current.storageChecksumType(),
                current.storageChecksumValue(),
                current.contentSha256(),
                current.etag(),
                PhysicalObjectLifecycle.DELETED,
                Math.addExact(current.lifecycleEpoch(), 1),
                current.createdAtMillis(),
                current.orphanNotBeforeMillis(),
                current.gcAttemptId(),
                current.referenceSetSha256(),
                current.markedAtMillis(),
                current.deleteNotBeforeMillis(),
                current.deleteStartedAtMillis(),
                deletedAtMillis,
                0,
                "",
                "",
                0);
    }

    private static void requireIncreasing(
            ObjectProtectionScanPage page,
            String previousKey) {
        if (previousKey != null
                && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(previousKey) <= 0) {
            throw invariant("protection scan did not advance during destructive recovery");
        }
    }

    private long nonNegativeNow() {
        long value = clock.millis();
        if (value < 0) {
            throw new IllegalStateException("clock returned a negative epoch millisecond");
        }
        return value;
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

    private static CompletionException propagate(Throwable failure) {
        return failure instanceof CompletionException completion
                ? completion
                : new CompletionException(failure);
    }

    private static final class RetirementCounts {
        private final AtomicInteger metadataRetired = new AtomicInteger();
        private final AtomicInteger metadataAbsent = new AtomicInteger();
        private final AtomicInteger protectionsRetired = new AtomicInteger();
        private final AtomicInteger protectionsAbsent = new AtomicInteger();
    }
}
