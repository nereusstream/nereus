/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ListObjectsOptions;
import com.nereusstream.objectstore.ListObjectsResult;
import com.nereusstream.objectstore.ListedObject;
import com.nereusstream.objectstore.ObjectStore;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Complete known-prefix audit that repairs only old, exact-HEAD objects whose physical root is absent.
 *
 * <p>Listing is discovery input, never absence or deletion authority. Newly registered roots receive another full
 * orphan grace before the metadata-root scanner may evaluate them. Disabled/dry-run configurations perform every
 * read and report {@code wouldRegister}, but never create metadata.
 */
public final class ObjectInventoryScanner implements AutoCloseable {
    private final String cluster;
    private final PhysicalGcConfig config;
    private final PhysicalObjectMetadataStore metadataStore;
    private final ObjectStore objectStore;
    private final List<ObjectInventoryFamily> families;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean scanning = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public ObjectInventoryScanner(
            String cluster,
            PhysicalGcConfig config,
            PhysicalObjectMetadataStore metadataStore,
            ObjectStore objectStore,
            List<ObjectInventoryFamily> families,
            Clock clock,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.families = canonicalFamilies(families);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    public CompletableFuture<ObjectInventoryScanResult> scan() {
        if (closed.get()) {
            return CompletableFuture.failedFuture(closed("object inventory rejected after close"));
        }
        if (!scanning.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("an object inventory scan is already running"));
        }
        if (closed.get()) {
            scanning.set(false);
            return CompletableFuture.failedFuture(closed("object inventory raced close"));
        }
        final long now;
        try {
            now = nonNegativeNow();
        } catch (Throwable failure) {
            scanning.set(false);
            return CompletableFuture.failedFuture(failure);
        }
        Counts counts = new Counts();
        CompletableFuture<ObjectInventoryScanResult> result = scanFamily(
                0, Optional.empty(), now, counts);
        result.whenComplete((ignored, failure) -> scanning.set(false));
        return result;
    }

    private CompletableFuture<ObjectInventoryScanResult> scanFamily(
            int familyIndex,
            Optional<String> continuation,
            long now,
            Counts counts) {
        if (familyIndex == families.size()) {
            return CompletableFuture.completedFuture(counts.result());
        }
        ObjectInventoryFamily family = families.get(familyIndex);
        return bound(
                        deadline -> objectStore.listObjects(
                                family.prefix(),
                                continuation,
                                new ListObjectsOptions(
                                        config.objectListPageSize(), deadline.remaining())),
                        "list object inventory family " + family.familyId())
                .thenCompose(page -> {
                    requirePage(family, page, continuation);
                    counts.page();
                    return inspectPage(family, page.objects(), 0, now, counts)
                            .thenCompose(ignored -> {
                                if (page.continuationToken().isPresent()) {
                                    return scanFamily(
                                            familyIndex,
                                            page.continuationToken(),
                                            now,
                                            counts);
                                }
                                counts.family();
                                return scanFamily(
                                        familyIndex + 1,
                                        Optional.empty(),
                                        now,
                                        counts);
                            });
                });
    }

    private CompletableFuture<Void> inspectPage(
            ObjectInventoryFamily family,
            List<ListedObject> values,
            int index,
            long now,
            Counts counts) {
        if (index == values.size()) {
            return CompletableFuture.completedFuture(null);
        }
        ListedObject listed = values.get(index);
        return inspect(family, listed, now)
                .thenAccept(counts::add)
                .thenCompose(ignored -> inspectPage(family, values, index + 1, now, counts));
    }

    private CompletableFuture<Outcome> inspect(
            ObjectInventoryFamily family,
            ListedObject listed,
            long now) {
        final ObjectInventoryKey parsed;
        try {
            parsed = Objects.requireNonNull(family.parse(listed.key()), "parsed inventory key");
            if (!parsed.objectKey().equals(listed.key())) {
                return CompletableFuture.completedFuture(Outcome.MALFORMED_KEY);
            }
        } catch (RuntimeException malformed) {
            return CompletableFuture.completedFuture(Outcome.MALFORMED_KEY);
        }
        ObjectKeyHash object = ObjectKeyHash.from(listed.key());
        return bound(
                        ignored -> metadataStore.getRoot(cluster, object),
                        "load physical root for listed object")
                .thenCompose(existing -> {
                    if (existing.isPresent()) {
                        return CompletableFuture.completedFuture(
                                parsed.matchesRootKeyFacts(existing.orElseThrow())
                                        ? Outcome.ALREADY_ROOTED
                                        : Outcome.ROOT_CONFLICT);
                    }
                    Optional<Long> registrationAt = registrationNotBefore(listed, now);
                    if (registrationAt.isEmpty() || now < registrationAt.orElseThrow()) {
                        return CompletableFuture.completedFuture(Outcome.AGE_BLOCKED);
                    }
                    return head(listed.key()).thenCompose(head -> {
                        if (head.isEmpty()) {
                            return CompletableFuture.completedFuture(Outcome.STALE_LISTING);
                        }
                        if (!listingMatchesHead(listed, head.orElseThrow())) {
                            return CompletableFuture.completedFuture(Outcome.HEAD_MISMATCH);
                        }
                        final PhysicalObjectIdentity identity;
                        try {
                            identity = parsed.exactHeadIdentity(head.orElseThrow());
                        } catch (RuntimeException mismatch) {
                            return CompletableFuture.completedFuture(Outcome.HEAD_MISMATCH);
                        }
                        return registerIfStillMissing(parsed, identity, listed, now);
                    });
                });
    }

    private CompletableFuture<Outcome> registerIfStillMissing(
            ObjectInventoryKey parsed,
            PhysicalObjectIdentity identity,
            ListedObject listed,
            long now) {
        return bound(
                        ignored -> metadataStore.getRoot(cluster, identity.objectKeyHash()),
                        "recheck physical root before inventory registration")
                .thenCompose(existing -> {
                    if (existing.isPresent()) {
                        return CompletableFuture.completedFuture(
                                exactIdentity(existing.orElseThrow(), identity)
                                        ? Outcome.ROOT_CONVERGED
                                        : Outcome.ROOT_CONFLICT);
                    }
                    if (!config.enabled() || config.dryRun()) {
                        return CompletableFuture.completedFuture(Outcome.WOULD_REGISTER);
                    }
                    long createdAt = listed.lastModified().orElseThrow().toEpochMilli();
                    long orphanNotBefore = Math.addExact(
                            now,
                            Math.addExact(
                                    config.orphanGrace().toMillis(),
                                    config.maximumClockSkew().toMillis()));
                    PhysicalObjectRootRecord root = active(identity, createdAt, orphanNotBefore);
                    return createOrRecover(identity, root);
                });
    }

    private CompletableFuture<Outcome> createOrRecover(
            PhysicalObjectIdentity identity,
            PhysicalObjectRootRecord root) {
        return bound(
                        ignored -> metadataStore.createRoot(cluster, root),
                        "register inventory-discovered physical root")
                .handle((created, failure) -> {
                    if (failure == null) {
                        return CompletableFuture.completedFuture(
                                exactIdentity(created, identity)
                                                && exactDesiredRoot(created, root)
                                        ? Outcome.ROOT_REGISTERED
                                        : Outcome.ROOT_CONFLICT);
                    }
                    Throwable original = unwrap(failure);
                    return bound(
                                    ignored -> metadataStore.getRoot(
                                            cluster, identity.objectKeyHash()),
                                    "reload physical root after uncertain inventory registration")
                            .thenCompose(reloaded -> {
                                if (reloaded.isEmpty()) {
                                    return CompletableFuture.failedFuture(original);
                                }
                                return CompletableFuture.completedFuture(
                                        exactIdentity(reloaded.orElseThrow(), identity)
                                                && exactDesiredRoot(reloaded.orElseThrow(), root)
                                                ? Outcome.ROOT_CONVERGED
                                                : Outcome.ROOT_CONFLICT);
                            });
                })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Optional<HeadObjectResult>> head(
            com.nereusstream.api.ObjectKey key) {
        return bound(
                        deadline -> objectStore.headObject(
                                key, new HeadObjectOptions(deadline.remaining())),
                        "HEAD inventory-discovered object")
                .handle((value, failure) -> {
                    if (failure == null) {
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

    private Optional<Long> registrationNotBefore(ListedObject listed, long now) {
        if (listed.lastModified().isEmpty()) {
            return Optional.empty();
        }
        Instant modified = listed.lastModified().orElseThrow();
        final long modifiedAt;
        try {
            modifiedAt = modified.toEpochMilli();
        } catch (ArithmeticException failure) {
            return Optional.empty();
        }
        if (modifiedAt < 0 || modifiedAt > now) {
            return Optional.empty();
        }
        try {
            return Optional.of(Math.addExact(
                    modifiedAt,
                    Math.addExact(
                            config.orphanGrace().toMillis(),
                            config.maximumClockSkew().toMillis())));
        } catch (ArithmeticException failure) {
            return Optional.empty();
        }
    }

    private <T> CompletableFuture<T> bound(
            Function<MaterializationDeadline, CompletableFuture<T>> operation,
            String stage) {
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        CompletableFuture<T> result = deadline.bound(
                () -> operation.apply(deadline), stage);
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private long nonNegativeNow() {
        long now = clock.millis();
        if (now < 0) {
            throw new IllegalStateException("object inventory clock cannot be negative");
        }
        return now;
    }

    private static boolean listingMatchesHead(ListedObject listed, HeadObjectResult head) {
        return head.key().equals(listed.key())
                && head.objectLength() == listed.objectLength()
                && (listed.etag().isEmpty() || listed.etag().equals(head.etag()));
    }

    private static boolean exactIdentity(
            VersionedPhysicalObjectRoot root, PhysicalObjectIdentity identity) {
        return root.value().lifecycle() == PhysicalObjectLifecycle.ACTIVE
                && com.nereusstream.core.physical.PhysicalObjectIdentity.from(root.value())
                        .equals(identity);
    }

    private static boolean exactDesiredRoot(
            VersionedPhysicalObjectRoot actual,
            PhysicalObjectRootRecord desired) {
        return actual.metadataVersion() > 0
                && actual.value().metadataVersion() == actual.metadataVersion()
                && actual.value().equals(desired.withMetadataVersion(actual.metadataVersion()));
    }

    private static PhysicalObjectRootRecord active(
            PhysicalObjectIdentity identity,
            long createdAtMillis,
            long orphanNotBeforeMillis) {
        return new PhysicalObjectRootRecord(
                1,
                identity.objectKeyHash().value(),
                identity.objectKey().value(),
                identity.objectId().map(value -> value.value()).orElse(""),
                identity.kind().wireId(),
                identity.objectLength(),
                identity.storageChecksum().type().name(),
                identity.storageChecksum().value(),
                identity.contentSha256().map(value -> value.value()).orElse(""),
                identity.etag().orElse(""),
                PhysicalObjectLifecycle.ACTIVE,
                1,
                createdAtMillis,
                orphanNotBeforeMillis,
                "",
                "",
                0,
                0,
                0,
                0,
                0,
                "",
                "",
                0);
    }

    private static void requirePage(
            ObjectInventoryFamily family,
            ListObjectsResult page,
            Optional<String> suppliedContinuation) {
        if (!page.prefix().equals(family.prefix())
                || (page.continuationToken().isPresent()
                        && page.continuationToken().equals(suppliedContinuation))) {
            throw invariant(
                    "object inventory listing escaped its prefix or repeated the supplied opaque token");
        }
    }

    private static List<ObjectInventoryFamily> canonicalFamilies(
            List<ObjectInventoryFamily> values) {
        List<ObjectInventoryFamily> exact = new ArrayList<>(
                List.copyOf(Objects.requireNonNull(values, "families")));
        if (exact.isEmpty() || exact.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("object inventory requires at least one non-null family");
        }
        exact.sort(Comparator.comparing(value -> value.prefix().value()));
        Set<String> ids = new HashSet<>();
        Set<String> prefixes = new HashSet<>();
        String previousPrefix = null;
        for (ObjectInventoryFamily family : exact) {
            String id = requireText(family.familyId(), "familyId");
            String prefix = Objects.requireNonNull(family.prefix(), "family prefix").value();
            if (!prefix.endsWith("/")
                    || !ids.add(id)
                    || !prefixes.add(prefix)
                    || (previousPrefix != null && prefix.startsWith(previousPrefix))) {
                throw new IllegalArgumentException(
                        "object inventory family ids/prefixes must be unique disjoint canonical directories");
            }
            previousPrefix = prefix;
        }
        return List.copyOf(exact);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException closed(String message) {
        return new NereusException(ErrorCode.STORAGE_CLOSED, false, message);
    }

    private static RuntimeException propagate(Throwable failure) {
        return failure instanceof RuntimeException runtime
                ? runtime
                : new CompletionException(failure);
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private enum Outcome {
        ALREADY_ROOTED,
        WOULD_REGISTER,
        ROOT_REGISTERED,
        ROOT_CONVERGED,
        AGE_BLOCKED,
        MALFORMED_KEY,
        STALE_LISTING,
        HEAD_MISMATCH,
        ROOT_CONFLICT
    }

    private static final class Counts {
        private long families;
        private long pages;
        private long listed;
        private long already;
        private long wouldRegister;
        private long registered;
        private long converged;
        private long ageBlocked;
        private long malformed;
        private long stale;
        private long headMismatch;
        private long conflict;

        private void family() {
            families = Math.addExact(families, 1);
        }

        private void page() {
            pages = Math.addExact(pages, 1);
        }

        private void add(Outcome outcome) {
            listed = Math.addExact(listed, 1);
            switch (outcome) {
                case ALREADY_ROOTED -> already = Math.addExact(already, 1);
                case WOULD_REGISTER -> wouldRegister = Math.addExact(wouldRegister, 1);
                case ROOT_REGISTERED -> registered = Math.addExact(registered, 1);
                case ROOT_CONVERGED -> converged = Math.addExact(converged, 1);
                case AGE_BLOCKED -> ageBlocked = Math.addExact(ageBlocked, 1);
                case MALFORMED_KEY -> malformed = Math.addExact(malformed, 1);
                case STALE_LISTING -> stale = Math.addExact(stale, 1);
                case HEAD_MISMATCH -> headMismatch = Math.addExact(headMismatch, 1);
                case ROOT_CONFLICT -> conflict = Math.addExact(conflict, 1);
            }
        }

        private ObjectInventoryScanResult result() {
            return new ObjectInventoryScanResult(
                    families,
                    pages,
                    listed,
                    already,
                    wouldRegister,
                    registered,
                    converged,
                    ageBlocked,
                    malformed,
                    stale,
                    headMismatch,
                    conflict);
        }
    }
}
