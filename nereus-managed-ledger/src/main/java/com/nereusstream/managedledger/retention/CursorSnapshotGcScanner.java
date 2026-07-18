/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.managedledger.cursor.CursorLedgerIdentity;
import com.nereusstream.managedledger.cursor.CursorSnapshotInventory;
import com.nereusstream.managedledger.cursor.CursorSnapshotKeys;
import com.nereusstream.metadata.oxia.CursorKeyspace;
import com.nereusstream.metadata.oxia.CursorMetadataDigests;
import com.nereusstream.metadata.oxia.CursorMetadataStore;
import com.nereusstream.metadata.oxia.CursorScanPage;
import com.nereusstream.metadata.oxia.CursorScanToken;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedCursorRetention;
import com.nereusstream.metadata.oxia.VersionedCursorState;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.CursorSnapshotReferenceRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.objectstore.ListObjectsOptions;
import com.nereusstream.objectstore.ListObjectsResult;
import com.nereusstream.objectstore.ListedObject;
import com.nereusstream.objectstore.ObjectKeyPrefix;
import com.nereusstream.objectstore.ObjectStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Complete, bounded F3 cursor-root/object inventory used to discover F4 snapshot GC candidates.
 *
 * <p>This scanner is deliberately read-only. It emits exact ACTIVE-root candidates and removable protection
 * versions, then repeats the complete inventory through {@link #revalidate(Candidate)} after the central GC owner has
 * drained readers. It never marks roots, removes protections, or deletes object bytes.
 */
public final class CursorSnapshotGcScanner implements AutoCloseable {
    public static final int MAX_INVENTORY_VALUES = 10_000;

    private final String cluster;
    private final CursorMetadataStore cursorMetadataStore;
    private final PhysicalObjectMetadataStore physicalMetadataStore;
    private final ObjectStore objectStore;
    private final Configuration config;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final CursorKeyspace cursorKeys;
    private final AtomicBoolean scanning = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public CursorSnapshotGcScanner(
            String cluster,
            CursorMetadataStore cursorMetadataStore,
            PhysicalObjectMetadataStore physicalMetadataStore,
            ObjectStore objectStore,
            Configuration config,
            Clock clock,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.cursorMetadataStore = Objects.requireNonNull(cursorMetadataStore, "cursorMetadataStore");
        this.physicalMetadataStore = Objects.requireNonNull(physicalMetadataStore, "physicalMetadataStore");
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.cursorKeys = new CursorKeyspace(cluster);
    }

    /** Performs one complete stream-prefix pass and invokes the visitor strictly one candidate at a time. */
    public CompletableFuture<ScanResult> scan(
            CursorLedgerIdentity ledger,
            CandidateVisitor visitor) {
        Objects.requireNonNull(ledger, "ledger");
        Objects.requireNonNull(visitor, "visitor");
        if (closed.get()) {
            return CompletableFuture.failedFuture(closed("cursor snapshot GC scan rejected after close"));
        }
        if (!scanning.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("a cursor snapshot GC scan is already running"));
        }
        if (closed.get()) {
            scanning.set(false);
            return CompletableFuture.failedFuture(closed("cursor snapshot GC scan raced close"));
        }
        final long now;
        try {
            now = nonNegativeNow();
        } catch (Throwable failure) {
            scanning.set(false);
            return CompletableFuture.failedFuture(failure);
        }
        OperationDeadline deadline = new OperationDeadline(config.operationTimeout(), scheduler);
        CompletableFuture<ScanResult> result = loadInventory(ledger, deadline)
                .thenCompose(optional -> {
                    if (optional.isEmpty()) {
                        return CompletableFuture.completedFuture(ScanResult.missingRetentionResult());
                    }
                    InventoryCut cut = optional.orElseThrow();
                    Counts counts = new Counts(cut);
                    if (cut.inventory().deletionVetoed()) {
                        return CompletableFuture.completedFuture(counts.result(true, false));
                    }
                    List<ObjectKey> candidates = cut.inventory().unreferencedCandidates().stream()
                            .sorted(Comparator.comparing(ObjectKey::value))
                            .toList();
                    return visitCandidates(cut, candidates, 0, now, visitor, counts, deadline)
                            .thenApply(ignored -> counts.result(false, false));
                });
        result.whenComplete((ignored, failure) -> {
            deadline.close();
            scanning.set(false);
        });
        return result;
    }

    /**
     * Repeats the full list/retention/cursor/protection cut for one marked candidate.
     *
     * <p>Ordinary authority or inventory drift returns {@code false}. Backend/decode/limit failures propagate so the
     * central collector retains MARKED and retries rather than treating an incomplete pass as deletion evidence.
     */
    public CompletableFuture<Boolean> revalidate(Candidate candidate) {
        Objects.requireNonNull(candidate, "candidate");
        if (closed.get()) {
            return CompletableFuture.failedFuture(
                    closed("cursor snapshot GC revalidation rejected after close"));
        }
        final long now;
        try {
            now = nonNegativeNow();
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        OperationDeadline deadline = new OperationDeadline(config.operationTimeout(), scheduler);
        CompletableFuture<Boolean> result = loadInventory(candidate.ledger(), deadline)
                .thenCompose(optional -> {
                    if (optional.isEmpty()) {
                        return CompletableFuture.completedFuture(false);
                    }
                    InventoryCut latest = optional.orElseThrow();
                    if (latest.inventory().deletionVetoed()
                            || !candidate.inventory().stillMatches(
                                    latest.retention(), latest.cursorRoots())
                            || !latest.inventory().unreferencedCandidates().contains(
                                    candidate.listedObject().key())
                            || !candidate.listedObject().equals(
                                    latest.listedObjects().get(candidate.listedObject().key()))) {
                        return CompletableFuture.completedFuture(false);
                    }
                    return bound(
                                    deadline,
                                    () -> physicalMetadataStore.getRoot(
                                            cluster,
                                            candidate.object().objectKeyHash()),
                                    "reload cursor snapshot physical root")
                            .thenCompose(root -> {
                                if (root.isEmpty()
                                        || !sameActiveOrMarkedSuccessor(
                                                candidate.activeRoot(), root.orElseThrow())) {
                                    return CompletableFuture.completedFuture(false);
                                }
                                return scanProtections(
                                                candidate.object().objectKeyHash(),
                                                Optional.empty(),
                                                new ArrayList<>(),
                                                null,
                                                deadline)
                                        .thenApply(protections -> {
                                            Optional<List<VersionedObjectProtection>> safe = safeProtections(
                                                    candidate.parsedKey(),
                                                    candidate.activeRoot(),
                                                    streamId(candidate.ledger()),
                                                    latest.cursorRoots(),
                                                    protections,
                                                    now);
                                            if (safe.isEmpty()
                                                    || !safe.orElseThrow().equals(
                                                            candidate.plannedProtectionRemovals())) {
                                                return false;
                                            }
                                            Checksum evidence = evidence(
                                                    candidate.ledger(),
                                                    latest.inventory(),
                                                    candidate.listedObject(),
                                                    candidate.activeRoot(),
                                                    protections,
                                                    candidate.discoveredAtMillis(),
                                                    candidate.notBeforeMillis());
                                            GcReferenceQuery query = GcReferenceQuery.create(
                                                    GcReferenceQueryKind.CURSOR_SNAPSHOT_CANDIDATE,
                                                    candidate.object(),
                                                    List.of(streamId(candidate.ledger())),
                                                    evidence);
                                            return evidence.equals(candidate.discoveryEvidenceSha256())
                                                    && query.equals(candidate.referenceQuery());
                                        });
                            });
                });
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    private CompletableFuture<Void> visitCandidates(
            InventoryCut cut,
            List<ObjectKey> candidates,
            int index,
            long now,
            CandidateVisitor visitor,
            Counts counts,
            OperationDeadline deadline) {
        if (index == candidates.size()) {
            return CompletableFuture.completedFuture(null);
        }
        ListedObject listed = cut.listedObjects().get(candidates.get(index));
        return evaluate(cut, listed, now, deadline).thenCompose(decision -> {
            counts.add(decision.block());
            if (decision.candidate().isEmpty()) {
                return visitCandidates(
                        cut, candidates, index + 1, now, visitor, counts, deadline);
            }
            Candidate candidate = decision.candidate().orElseThrow();
            counts.eligibleCandidates = Math.addExact(counts.eligibleCandidates, 1);
            return bound(
                            deadline,
                            () -> visitor.visit(candidate),
                            "visit cursor snapshot GC candidate " + candidate.listedObject().key().value())
                    .thenCompose(ignored -> {
                        counts.visitedCandidates = Math.addExact(counts.visitedCandidates, 1);
                        return visitCandidates(
                                cut, candidates, index + 1, now, visitor, counts, deadline);
                    });
        });
    }

    private CompletableFuture<CandidateDecision> evaluate(
            InventoryCut cut,
            ListedObject listed,
            long now,
            OperationDeadline deadline) {
        final CursorSnapshotKeys.ParsedSnapshotKey parsed;
        try {
            parsed = CursorSnapshotKeys.parse(cluster, cut.ledger(), listed.key());
        } catch (RuntimeException malformed) {
            return CompletableFuture.completedFuture(CandidateDecision.blocked(Block.MALFORMED_KEY));
        }
        if (listed.lastModified().isEmpty()) {
            return CompletableFuture.completedFuture(CandidateDecision.blocked(Block.UNKNOWN_AGE));
        }
        Optional<Long> listingNotBefore = listingNotBefore(listed.lastModified().orElseThrow());
        if (listingNotBefore.isEmpty() || now < listingNotBefore.orElseThrow()) {
            return CompletableFuture.completedFuture(CandidateDecision.blocked(Block.TOO_YOUNG));
        }
        return bound(
                        deadline,
                        () -> physicalMetadataStore.getRoot(
                                cluster, com.nereusstream.api.ObjectKeyHash.from(listed.key())),
                        "load cursor snapshot physical root")
                .thenCompose(optionalRoot -> {
                    if (optionalRoot.isEmpty()) {
                        return CompletableFuture.completedFuture(
                                CandidateDecision.blocked(Block.MISSING_ROOT));
                    }
                    VersionedPhysicalObjectRoot root = optionalRoot.orElseThrow();
                    if (root.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE
                            || PhysicalObjectIdentity.from(root.value()).kind()
                                    != PhysicalObjectKind.CURSOR_SNAPSHOT) {
                        return CompletableFuture.completedFuture(
                                CandidateDecision.blocked(Block.ROOT_STATE));
                    }
                    PhysicalObjectIdentity object = PhysicalObjectIdentity.from(root.value());
                    if (!listingMatches(object, listed)) {
                        return CompletableFuture.completedFuture(
                                CandidateDecision.blocked(Block.IDENTITY_MISMATCH));
                    }
                    long notBefore = Math.max(
                            root.value().orphanNotBeforeMillis(), listingNotBefore.orElseThrow());
                    if (now < root.value().createdAtMillis() || now < notBefore) {
                        return CompletableFuture.completedFuture(
                                CandidateDecision.blocked(Block.TOO_YOUNG));
                    }
                    return scanProtections(
                                    object.objectKeyHash(),
                                    Optional.empty(),
                                    new ArrayList<>(),
                                    null,
                                    deadline)
                            .thenApply(protections -> {
                                Optional<List<VersionedObjectProtection>> safe = safeProtections(
                                        parsed,
                                        root,
                                        streamId(cut.ledger()),
                                        cut.cursorRoots(),
                                        protections,
                                        now);
                                if (safe.isEmpty()) {
                                    return CandidateDecision.blocked(Block.PROTECTION);
                                }
                                Checksum evidence = evidence(
                                        cut.ledger(),
                                        cut.inventory(),
                                        listed,
                                        root,
                                        protections,
                                        now,
                                        notBefore);
                                GcReferenceQuery query = GcReferenceQuery.create(
                                        GcReferenceQueryKind.CURSOR_SNAPSHOT_CANDIDATE,
                                        object,
                                        List.of(streamId(cut.ledger())),
                                        evidence);
                                return CandidateDecision.eligible(new Candidate(
                                        cut.ledger(),
                                        cut.inventory(),
                                        parsed,
                                        listed,
                                        root,
                                        safe.orElseThrow(),
                                        object,
                                        query,
                                        evidence,
                                        now,
                                        notBefore));
                            });
                });
    }

    private CompletableFuture<Optional<InventoryCut>> loadInventory(
            CursorLedgerIdentity ledger,
            OperationDeadline deadline) {
        StreamId streamId = streamId(ledger);
        return bound(
                        deadline,
                        () -> cursorMetadataStore.getRetention(cluster, streamId),
                        "load cursor snapshot retention authority")
                .thenCompose(optionalRetention -> {
                    if (optionalRetention.isEmpty()) {
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                    VersionedCursorRetention retention = optionalRetention.orElseThrow();
                    return scanCursorRoots(
                                    streamId,
                                    Optional.empty(),
                                    new ArrayList<>(),
                                    null,
                                    deadline)
                            .thenCompose(roots -> listObjects(
                                            ledger,
                                            Optional.empty(),
                                            new LinkedHashMap<>(),
                                            new HashSet<>(),
                                            null,
                                            deadline)
                                    .thenApply(objects -> {
                                        CursorSnapshotInventory inventory;
                                        try {
                                            inventory = CursorSnapshotInventory.classify(
                                                    cluster,
                                                    ledger,
                                                    retention,
                                                    roots,
                                                    objects.keySet());
                                        } catch (IllegalArgumentException failure) {
                                            throw invariant(
                                                    "cursor snapshot inventory authority/listing is inconsistent",
                                                    failure);
                                        }
                                        return Optional.of(new InventoryCut(
                                                ledger,
                                                retention,
                                                roots,
                                                objects,
                                                inventory));
                                    }));
                });
    }

    private CompletableFuture<List<VersionedCursorState>> scanCursorRoots(
            StreamId streamId,
            Optional<CursorScanToken> continuation,
            ArrayList<VersionedCursorState> values,
            String lastKey,
            OperationDeadline deadline) {
        return bound(
                        deadline,
                        () -> cursorMetadataStore.scanCursors(
                                cluster,
                                streamId,
                                continuation,
                                config.cursorScanPageSize()),
                        "scan cursor snapshot roots")
                .thenCompose(page -> {
                    requireIncreasingCursorPage(streamId, page, lastKey);
                    if (page.records().size() > config.cursorScanPageSize()) {
                        return CompletableFuture.failedFuture(invariant(
                                "cursor metadata scan exceeded its requested page bound"));
                    }
                    for (VersionedCursorState value : page.records()) {
                        values.add(value);
                        if (values.size() > config.maxCursorRoots()) {
                            return CompletableFuture.failedFuture(limit(
                                    "cursor snapshot root inventory exceeds the configured complete-scan bound"));
                        }
                    }
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(List.copyOf(values));
                    }
                    String nextLast = cursorKey(
                            streamId,
                            page.records().get(page.records().size() - 1));
                    return scanCursorRoots(
                            streamId,
                            page.continuation(),
                            values,
                            nextLast,
                            deadline);
                });
    }

    private CompletableFuture<Map<ObjectKey, ListedObject>> listObjects(
            CursorLedgerIdentity ledger,
            Optional<String> continuation,
            LinkedHashMap<ObjectKey, ListedObject> values,
            Set<String> seenContinuations,
            String lastKey,
            OperationDeadline deadline) {
        ObjectKeyPrefix prefix = new ObjectKeyPrefix(
                CursorSnapshotKeys.streamPrefix(cluster, ledger));
        return bound(
                        deadline,
                        () -> objectStore.listObjects(
                                prefix,
                                continuation,
                                new ListObjectsOptions(
                                        config.objectListPageSize(), deadline.remaining())),
                        "list cursor snapshot objects")
                .thenCompose(page -> {
                    requireIncreasingObjectPage(prefix, page, lastKey);
                    if (page.objects().size() > config.objectListPageSize()) {
                        return CompletableFuture.failedFuture(invariant(
                                "object-store list exceeded its requested page bound"));
                    }
                    for (ListedObject value : page.objects()) {
                        ListedObject prior = values.putIfAbsent(value.key(), value);
                        if (prior != null) {
                            return CompletableFuture.failedFuture(invariant(
                                    "cursor snapshot object listing repeated a key"));
                        }
                        if (values.size() > config.maxSnapshotObjects()) {
                            return CompletableFuture.failedFuture(limit(
                                    "cursor snapshot object inventory exceeds the configured complete-scan bound"));
                        }
                    }
                    if (page.continuationToken().isEmpty()) {
                        return CompletableFuture.completedFuture(immutableMap(values));
                    }
                    String token = page.continuationToken().orElseThrow();
                    if (!seenContinuations.add(token)) {
                        return CompletableFuture.failedFuture(invariant(
                                "cursor snapshot object listing repeated a continuation token"));
                    }
                    String nextLast = page.objects().get(page.objects().size() - 1).key().value();
                    return listObjects(
                            ledger,
                            page.continuationToken(),
                            values,
                            seenContinuations,
                            nextLast,
                            deadline);
                });
    }

    private CompletableFuture<List<VersionedObjectProtection>> scanProtections(
            com.nereusstream.api.ObjectKeyHash object,
            Optional<F4ScanToken> continuation,
            ArrayList<VersionedObjectProtection> values,
            String lastKey,
            OperationDeadline deadline) {
        return bound(
                        deadline,
                        () -> physicalMetadataStore.scanProtections(
                                cluster,
                                object,
                                continuation,
                                config.metadataScanPageSize()),
                        "scan cursor snapshot protections")
                .thenCompose(page -> {
                    requireIncreasingProtectionPage(page, lastKey);
                    if (page.values().size() > config.metadataScanPageSize()) {
                        return CompletableFuture.failedFuture(invariant(
                                "protection scan exceeded its requested page bound"));
                    }
                    for (VersionedObjectProtection value : page.values()) {
                        values.add(value);
                        if (values.size() > config.maxProtectionsPerObject()) {
                            return CompletableFuture.failedFuture(limit(
                                    "cursor snapshot protection inventory exceeds the complete-scan bound"));
                        }
                    }
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(List.copyOf(values));
                    }
                    String nextLast = page.values().get(page.values().size() - 1).key();
                    return scanProtections(
                            object,
                            page.continuation(),
                            values,
                            nextLast,
                            deadline);
                });
    }

    private Optional<List<VersionedObjectProtection>> safeProtections(
            CursorSnapshotKeys.ParsedSnapshotKey parsed,
            VersionedPhysicalObjectRoot activeRoot,
            StreamId streamId,
            List<VersionedCursorState> cursorRoots,
            List<VersionedObjectProtection> protections,
            long now) {
        Map<String, VersionedCursorState> owners = new HashMap<>();
        for (VersionedCursorState root : cursorRoots) {
            if (owners.put(
                            cursorKeys.cursorStateKey(streamId, root.value().cursorName()),
                            root)
                    != null) {
                return Optional.empty();
            }
        }
        String previous = null;
        for (VersionedObjectProtection protection : protections) {
            if (previous != null && previous.compareTo(protection.key()) >= 0) {
                return Optional.empty();
            }
            previous = protection.key();
            var value = protection.value();
            if (!value.objectKeyHash().equals(activeRoot.value().objectKeyHash())
                    || value.rootLifecycleEpoch() != activeRoot.value().lifecycleEpoch()
                    || !value.referenceId().equals(parsed.snapshotId())) {
                return Optional.empty();
            }
            ObjectProtectionType type = ObjectProtectionType.fromWireId(value.protectionTypeId());
            if (type != ObjectProtectionType.CURSOR_SNAPSHOT_ROOT
                    && type != ObjectProtectionType.CURSOR_SNAPSHOT_PENDING) {
                return Optional.empty();
            }
            VersionedCursorState owner = owners.get(value.ownerKey());
            if (owner == null
                    || !owner.value().cursorNameHash().equals(parsed.cursorNameHash())
                    || value.ownerMetadataVersion() > owner.metadataVersion()) {
                return Optional.empty();
            }
            if (value.ownerMetadataVersion() == owner.metadataVersion()
                    && !value.ownerIdentitySha256().equals(
                            CursorMetadataDigests.durableValueSha256(owner.value()).value())) {
                return Optional.empty();
            }
            if (type == ObjectProtectionType.CURSOR_SNAPSHOT_PENDING) {
                Optional<Long> safeExpiry = checkedAdd(
                        value.expiresAtMillis(), config.maximumClockSkew().toMillis());
                if (safeExpiry.isEmpty() || now <= safeExpiry.orElseThrow()) {
                    return Optional.empty();
                }
            }
        }
        return Optional.of(List.copyOf(protections));
    }

    private Optional<Long> listingNotBefore(Instant lastModified) {
        Optional<Long> afterGrace = checkedAdd(
                lastModified.toEpochMilli(), config.orphanGrace().toMillis());
        return afterGrace.flatMap(value -> checkedAdd(
                value, config.maximumClockSkew().toMillis()));
    }

    private static boolean listingMatches(
            PhysicalObjectIdentity object, ListedObject listed) {
        return object.objectKey().equals(listed.key())
                && object.objectLength() == listed.objectLength()
                && object.etag().equals(listed.etag());
    }

    private static boolean sameActiveOrMarkedSuccessor(
            VersionedPhysicalObjectRoot active,
            VersionedPhysicalObjectRoot current) {
        if (!PhysicalObjectIdentity.from(active.value()).equals(
                PhysicalObjectIdentity.from(current.value()))) {
            return false;
        }
        if (current.equals(active)) {
            return true;
        }
        return active.value().lifecycle() == PhysicalObjectLifecycle.ACTIVE
                && current.value().lifecycle() == PhysicalObjectLifecycle.MARKED
                && current.value().lifecycleEpoch()
                        == Math.addExact(active.value().lifecycleEpoch(), 1)
                && current.value().createdAtMillis() == active.value().createdAtMillis()
                && current.value().orphanNotBeforeMillis()
                        == active.value().orphanNotBeforeMillis();
    }

    private Checksum evidence(
            CursorLedgerIdentity ledger,
            CursorSnapshotInventory inventory,
            ListedObject listed,
            VersionedPhysicalObjectRoot root,
            List<VersionedObjectProtection> protections,
            long discoveredAtMillis,
            long notBeforeMillis) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            text(digest, "nereus-cursor-snapshot-gc-candidate-v1");
            text(digest, cluster);
            text(digest, ledger.managedLedgerName());
            text(digest, ledger.managedLedgerNameHash());
            number(digest, ledger.projection().storageClassBindingGeneration());
            number(digest, ledger.projection().incarnation());
            text(digest, ledger.projection().streamId());
            number(digest, ledger.projection().virtualLedgerId());
            var authority = inventory.authority();
            number(digest, authority.retentionMetadataVersion());
            text(digest, authority.ownerSessionId());
            text(digest, authority.retentionLifecycle().name());
            number(digest, authority.retentionMutationSequence());
            number(digest, inventory.roots().size());
            inventory.roots().forEach((name, version) -> {
                text(digest, name);
                number(digest, version.cursorGeneration());
                text(digest, version.lifecycle().name());
                number(digest, version.mutationSequence());
                number(digest, version.metadataVersion());
                optionalText(digest, version.snapshotKey().map(ObjectKey::value));
            });
            number(digest, inventory.liveReferences().size());
            inventory.liveReferences().forEach((key, live) -> {
                text(digest, key.value());
                text(digest, live.cursorName());
                number(digest, live.cursorGeneration());
                number(digest, live.rootMutationSequence());
                number(digest, live.rootMetadataVersion());
                reference(digest, live.reference());
            });
            text(digest, listed.key().value());
            number(digest, listed.objectLength());
            optionalText(digest, listed.etag());
            Instant modified = listed.lastModified().orElseThrow();
            number(digest, modified.getEpochSecond());
            number(digest, modified.getNano());
            number(digest, root.metadataVersion());
            text(digest, root.durableValueSha256().value());
            number(digest, root.value().lifecycleEpoch());
            number(digest, root.value().createdAtMillis());
            number(digest, root.value().orphanNotBeforeMillis());
            text(digest, PhysicalObjectIdentity.from(root.value()).identitySha256().value());
            number(digest, protections.size());
            for (VersionedObjectProtection protection : protections) {
                var value = protection.value();
                text(digest, protection.key());
                number(digest, protection.metadataVersion());
                text(digest, protection.durableValueSha256().value());
                number(digest, value.protectionTypeId());
                text(digest, value.referenceId());
                text(digest, value.ownerKey());
                number(digest, value.ownerMetadataVersion());
                text(digest, value.ownerIdentitySha256());
                number(digest, value.rootLifecycleEpoch());
                number(digest, value.createdAtMillis());
                number(digest, value.expiresAtMillis());
            }
            number(digest, discoveredAtMillis);
            number(digest, notBeforeMillis);
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(digest.digest()));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static void reference(
            MessageDigest digest, CursorSnapshotReferenceRecord reference) {
        text(digest, reference.objectKey());
        text(digest, reference.snapshotId());
        number(digest, reference.cursorGeneration());
        number(digest, reference.sourceMutationSequence());
        number(digest, reference.baseMarkDeleteOffset());
        number(digest, reference.objectLength());
        text(digest, reference.storageChecksumType());
        text(digest, reference.storageChecksumValue());
        number(digest, reference.formatCrc32c());
        number(digest, reference.formatVersion());
        number(digest, reference.createdAtMillis());
    }

    private static void optionalText(
            MessageDigest digest, Optional<String> value) {
        number(digest, value.isPresent() ? 1 : 0);
        value.ifPresent(text -> text(digest, text));
    }

    private static void text(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(bytes.length)
                .array());
        digest.update(bytes);
    }

    private static void number(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(value)
                .array());
    }

    private void requireIncreasingCursorPage(
            StreamId streamId, CursorScanPage page, String lastKey) {
        String previous = lastKey;
        for (VersionedCursorState value : page.records()) {
            String key = cursorKey(streamId, value);
            if (previous != null && previous.compareTo(key) >= 0) {
                throw invariant("cursor snapshot root scan did not advance monotonically");
            }
            previous = key;
        }
    }

    private static void requireIncreasingObjectPage(
            ObjectKeyPrefix prefix, ListObjectsResult page, String lastKey) {
        if (!page.prefix().equals(prefix)) {
            throw invariant("cursor snapshot object listing returned a different prefix");
        }
        if (lastKey != null
                && !page.objects().isEmpty()
                && page.objects().get(0).key().value().compareTo(lastKey) <= 0) {
            throw invariant("cursor snapshot object listing did not advance monotonically");
        }
    }

    private static void requireIncreasingProtectionPage(
            ObjectProtectionScanPage page, String lastKey) {
        if (lastKey != null
                && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(lastKey) <= 0) {
            throw invariant("cursor snapshot protection scan did not advance monotonically");
        }
    }

    private String cursorKey(StreamId streamId, VersionedCursorState root) {
        return cursorKeys.cursorStateKey(streamId, root.value().cursorName());
    }

    private static StreamId streamId(CursorLedgerIdentity ledger) {
        return new StreamId(ledger.projection().streamId());
    }

    private static <T> CompletableFuture<T> bound(
            OperationDeadline deadline,
            Supplier<CompletableFuture<T>> operation,
            String stage) {
        return deadline.bound(operation, stage);
    }

    private long nonNegativeNow() {
        long now = clock.millis();
        if (now < 0) {
            throw new IllegalStateException("cursor snapshot GC clock returned a negative epoch millisecond");
        }
        return now;
    }

    private static Optional<Long> checkedAdd(long left, long right) {
        try {
            return Optional.of(Math.addExact(left, right));
        } catch (ArithmeticException overflow) {
            return Optional.empty();
        }
    }

    private static <K, V> Map<K, V> immutableMap(LinkedHashMap<K, V> source) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    public record Configuration(
            int metadataScanPageSize,
            int objectListPageSize,
            int cursorScanPageSize,
            int maxCursorRoots,
            int maxSnapshotObjects,
            int maxProtectionsPerObject,
            Duration orphanGrace,
            Duration maximumClockSkew,
            Duration operationTimeout) {
        public Configuration {
            requireInRange(metadataScanPageSize, 1, 1_000, "metadataScanPageSize");
            requireInRange(objectListPageSize, 1, 1_000, "objectListPageSize");
            requireInRange(cursorScanPageSize, 1, MAX_INVENTORY_VALUES, "cursorScanPageSize");
            requireInRange(maxCursorRoots, 1, MAX_INVENTORY_VALUES, "maxCursorRoots");
            requireInRange(maxSnapshotObjects, 1, MAX_INVENTORY_VALUES, "maxSnapshotObjects");
            requireInRange(maxProtectionsPerObject, 1, MAX_INVENTORY_VALUES, "maxProtectionsPerObject");
            if (cursorScanPageSize > maxCursorRoots) {
                throw new IllegalArgumentException("cursorScanPageSize exceeds maxCursorRoots");
            }
            orphanGrace = requireDuration(orphanGrace, true, "orphanGrace");
            maximumClockSkew = requireDuration(maximumClockSkew, false, "maximumClockSkew");
            operationTimeout = requireDuration(operationTimeout, true, "operationTimeout");
        }
    }

    public record Candidate(
            CursorLedgerIdentity ledger,
            CursorSnapshotInventory inventory,
            CursorSnapshotKeys.ParsedSnapshotKey parsedKey,
            ListedObject listedObject,
            VersionedPhysicalObjectRoot activeRoot,
            List<VersionedObjectProtection> plannedProtectionRemovals,
            PhysicalObjectIdentity object,
            GcReferenceQuery referenceQuery,
            Checksum discoveryEvidenceSha256,
            long discoveredAtMillis,
            long notBeforeMillis) {
        public Candidate {
            Objects.requireNonNull(ledger, "ledger");
            Objects.requireNonNull(inventory, "inventory");
            Objects.requireNonNull(parsedKey, "parsedKey");
            Objects.requireNonNull(listedObject, "listedObject");
            Objects.requireNonNull(activeRoot, "activeRoot");
            plannedProtectionRemovals = List.copyOf(Objects.requireNonNull(
                    plannedProtectionRemovals, "plannedProtectionRemovals"));
            Objects.requireNonNull(object, "object");
            Objects.requireNonNull(referenceQuery, "referenceQuery");
            discoveryEvidenceSha256 = GcReferenceQuery.requireSha256(
                    discoveryEvidenceSha256, "discoveryEvidenceSha256");
            if (!inventory.ledger().equals(ledger)
                    || inventory.deletionVetoed()
                    || !inventory.unreferencedCandidates().contains(listedObject.key())
                    || activeRoot.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE
                    || !PhysicalObjectIdentity.from(activeRoot.value()).equals(object)
                    || object.kind() != PhysicalObjectKind.CURSOR_SNAPSHOT
                    || !object.objectKey().equals(listedObject.key())
                    || referenceQuery.kind() != GcReferenceQueryKind.CURSOR_SNAPSHOT_CANDIDATE
                    || !referenceQuery.object().equals(object)
                    || !referenceQuery.candidateEvidenceSha256().equals(discoveryEvidenceSha256)
                    || !referenceQuery.affectedStreams().equals(List.of(streamId(ledger)))) {
                throw new IllegalArgumentException("cursor snapshot GC candidate facts are inconsistent");
            }
            if (discoveredAtMillis < activeRoot.value().createdAtMillis()
                    || notBeforeMillis < activeRoot.value().orphanNotBeforeMillis()) {
                throw new IllegalArgumentException("cursor snapshot GC candidate timestamps are invalid");
            }
            String previous = null;
            for (VersionedObjectProtection protection : plannedProtectionRemovals) {
                if (previous != null && previous.compareTo(protection.key()) >= 0) {
                    throw new IllegalArgumentException("planned cursor protections are not strictly ordered");
                }
                previous = protection.key();
            }
        }
    }

    public record ScanResult(
            long listedObjects,
            long liveReferences,
            long unreferencedObjects,
            long eligibleCandidates,
            long visitedCandidates,
            long malformedKeys,
            long unknownAgeObjects,
            long tooYoungObjects,
            long missingRoots,
            long rootStateBlocked,
            long identityMismatches,
            long protectionBlocked,
            boolean deletionVetoed,
            boolean retentionMissing) {
        public ScanResult {
            if (listedObjects < 0
                    || liveReferences < 0
                    || unreferencedObjects < 0
                    || eligibleCandidates < 0
                    || visitedCandidates < 0
                    || malformedKeys < 0
                    || unknownAgeObjects < 0
                    || tooYoungObjects < 0
                    || missingRoots < 0
                    || rootStateBlocked < 0
                    || identityMismatches < 0
                    || protectionBlocked < 0
                    || visitedCandidates > eligibleCandidates
                    || (retentionMissing && deletionVetoed)) {
                throw new IllegalArgumentException("cursor snapshot GC scan counters are invalid");
            }
        }

        private static ScanResult missingRetentionResult() {
            return new ScanResult(
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, true);
        }
    }

    @FunctionalInterface
    public interface CandidateVisitor {
        CompletableFuture<Void> visit(Candidate candidate);
    }

    private record InventoryCut(
            CursorLedgerIdentity ledger,
            VersionedCursorRetention retention,
            List<VersionedCursorState> cursorRoots,
            Map<ObjectKey, ListedObject> listedObjects,
            CursorSnapshotInventory inventory) {
        private InventoryCut {
            Objects.requireNonNull(ledger, "ledger");
            Objects.requireNonNull(retention, "retention");
            cursorRoots = List.copyOf(cursorRoots);
            listedObjects = Map.copyOf(listedObjects);
            Objects.requireNonNull(inventory, "inventory");
        }
    }

    private record CandidateDecision(Optional<Candidate> candidate, Block block) {
        private CandidateDecision {
            candidate = Objects.requireNonNull(candidate, "candidate");
            Objects.requireNonNull(block, "block");
            if (candidate.isPresent() != (block == Block.NONE)) {
                throw new IllegalArgumentException("candidate decision is inconsistent");
            }
        }

        private static CandidateDecision eligible(Candidate candidate) {
            return new CandidateDecision(Optional.of(candidate), Block.NONE);
        }

        private static CandidateDecision blocked(Block block) {
            return new CandidateDecision(Optional.empty(), block);
        }
    }

    private enum Block {
        NONE,
        MALFORMED_KEY,
        UNKNOWN_AGE,
        TOO_YOUNG,
        MISSING_ROOT,
        ROOT_STATE,
        IDENTITY_MISMATCH,
        PROTECTION
    }

    private static final class Counts {
        private final long listedObjects;
        private final long liveReferences;
        private final long unreferencedObjects;
        private long eligibleCandidates;
        private long visitedCandidates;
        private long malformedKeys;
        private long unknownAgeObjects;
        private long tooYoungObjects;
        private long missingRoots;
        private long rootStateBlocked;
        private long identityMismatches;
        private long protectionBlocked;

        private Counts(InventoryCut cut) {
            this.listedObjects = cut.listedObjects().size();
            this.liveReferences = cut.inventory().liveReferences().size();
            this.unreferencedObjects = cut.inventory().unreferencedCandidates().size();
        }

        private void add(Block block) {
            switch (block) {
                case NONE -> { }
                case MALFORMED_KEY -> malformedKeys = Math.addExact(malformedKeys, 1);
                case UNKNOWN_AGE -> unknownAgeObjects = Math.addExact(unknownAgeObjects, 1);
                case TOO_YOUNG -> tooYoungObjects = Math.addExact(tooYoungObjects, 1);
                case MISSING_ROOT -> missingRoots = Math.addExact(missingRoots, 1);
                case ROOT_STATE -> rootStateBlocked = Math.addExact(rootStateBlocked, 1);
                case IDENTITY_MISMATCH -> identityMismatches = Math.addExact(identityMismatches, 1);
                case PROTECTION -> protectionBlocked = Math.addExact(protectionBlocked, 1);
            }
        }

        private ScanResult result(boolean deletionVetoed, boolean retentionMissing) {
            return new ScanResult(
                    listedObjects,
                    liveReferences,
                    unreferencedObjects,
                    eligibleCandidates,
                    visitedCandidates,
                    malformedKeys,
                    unknownAgeObjects,
                    tooYoungObjects,
                    missingRoots,
                    rootStateBlocked,
                    identityMismatches,
                    protectionBlocked,
                    deletionVetoed,
                    retentionMissing);
        }
    }

    private static final class OperationDeadline implements AutoCloseable {
        private final long deadlineNanos;
        private final ScheduledExecutorService scheduler;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final Set<BoundState<?>> active = ConcurrentHashMap.newKeySet();

        private OperationDeadline(
                Duration timeout, ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
            long now = System.nanoTime();
            long timeoutNanos;
            try {
                timeoutNanos = timeout.toNanos();
            } catch (ArithmeticException overflow) {
                timeoutNanos = Long.MAX_VALUE;
            }
            this.deadlineNanos = timeoutNanos >= Long.MAX_VALUE - now
                    ? Long.MAX_VALUE
                    : now + timeoutNanos;
        }

        private Duration remaining() {
            long nanos = deadlineNanos == Long.MAX_VALUE
                    ? Long.MAX_VALUE
                    : deadlineNanos - System.nanoTime();
            if (nanos <= 0) {
                throw timeout("cursor snapshot GC operation deadline expired");
            }
            return Duration.ofNanos(nanos);
        }

        private <T> CompletableFuture<T> bound(
                Supplier<CompletableFuture<T>> operation, String stage) {
            if (closed.get()) {
                return CompletableFuture.failedFuture(closed(stage + " rejected after deadline close"));
            }
            final long nanos;
            try {
                nanos = remaining().toNanos();
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
            final CompletableFuture<T> source;
            try {
                source = Objects.requireNonNull(operation.get(), stage + " future");
            } catch (Throwable failure) {
                return CompletableFuture.failedFuture(failure);
            }
            CompletableFuture<T> result = new CompletableFuture<>();
            BoundState<T> state = new BoundState<>(source, result, stage);
            active.add(state);
            if (closed.get()) {
                state.fail(closed(stage + " cancelled during deadline close"));
                return result;
            }
            try {
                state.timeout = scheduler.schedule(
                        () -> state.fail(timeout(stage + " timed out")),
                        nanos,
                        TimeUnit.NANOSECONDS);
            } catch (RejectedExecutionException failure) {
                state.fail(new NereusException(
                        ErrorCode.STORAGE_CLOSED,
                        false,
                        stage + " timeout scheduler rejected admitted work",
                        failure));
                return result;
            }
            source.whenComplete(state::completeFromSource);
            result.whenComplete((ignored, failure) -> {
                if (result.isCancelled()) {
                    state.cancelFromCaller();
                }
            });
            return result;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            for (BoundState<?> state : Set.copyOf(active)) {
                state.fail(closed(state.stage + " cancelled during deadline close"));
            }
        }

        private final class BoundState<T> {
            private final CompletableFuture<T> source;
            private final CompletableFuture<T> result;
            private final String stage;
            private final AtomicBoolean terminal = new AtomicBoolean();
            private volatile ScheduledFuture<?> timeout;

            private BoundState(
                    CompletableFuture<T> source,
                    CompletableFuture<T> result,
                    String stage) {
                this.source = source;
                this.result = result;
                this.stage = stage;
            }

            private void completeFromSource(T value, Throwable failure) {
                if (!terminal.compareAndSet(false, true)) {
                    return;
                }
                cleanup();
                active.remove(this);
                if (failure == null) {
                    result.complete(value);
                } else {
                    result.completeExceptionally(failure);
                }
            }

            private void fail(Throwable failure) {
                if (!terminal.compareAndSet(false, true)) {
                    return;
                }
                cleanup();
                active.remove(this);
                source.cancel(true);
                result.completeExceptionally(failure);
            }

            private void cancelFromCaller() {
                if (!terminal.compareAndSet(false, true)) {
                    return;
                }
                cleanup();
                active.remove(this);
                source.cancel(true);
            }

            private void cleanup() {
                ScheduledFuture<?> scheduled = timeout;
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
            }
        }
    }

    private static Duration requireDuration(
            Duration value, boolean positive, String name) {
        Objects.requireNonNull(value, name);
        long millis;
        try {
            millis = value.toMillis();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(name + " must fit milliseconds", overflow);
        }
        if (value.isNegative()
                || (positive && (value.isZero() || millis <= 0))
                || !value.equals(Duration.ofMillis(millis))) {
            throw new IllegalArgumentException(
                    name + " must be " + (positive ? "positive" : "non-negative")
                            + " and exactly millisecond-representable");
        }
        return value;
    }

    private static void requireInRange(
            int value, int minimum, int maximum, String name) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(
                    name + " must be in [" + minimum + ", " + maximum + "]");
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

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static NereusException limit(String message) {
        return new NereusException(
                ErrorCode.METADATA_LIMIT_EXCEEDED, false, message);
    }

    private static NereusException timeout(String message) {
        return new NereusException(ErrorCode.TIMEOUT, true, message);
    }

    private static NereusException closed(String message) {
        return new NereusException(ErrorCode.STORAGE_CLOSED, false, message);
    }
}
