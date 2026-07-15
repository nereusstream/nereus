/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.core.physical.GcReferenceDomain;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceSnapshot;
import com.nereusstream.materialization.MaterializationDeadline;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Exact bounded local reference-domain registry used by GC discovery and drain revalidation. */
public final class GcReferenceDomainRegistry {
    private final PhysicalGcConfig config;
    private final ScheduledExecutorService scheduler;
    private final List<RegisteredDomain> domains;
    private final Map<String, RegisteredDomain> byId;
    private final List<GcReferenceDomainVersion> requiredDomains;

    public GcReferenceDomainRegistry(
            PhysicalGcConfig config,
            ScheduledExecutorService scheduler,
            List<GcReferenceDomain> domains) {
        this.config = Objects.requireNonNull(config, "config");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        List<RegisteredDomain> registered = Objects.requireNonNull(domains, "domains").stream()
                .map(RegisteredDomain::new)
                .sorted(Comparator.comparing(RegisteredDomain::version))
                .toList();
        if (registered.isEmpty() || registered.size() > GcPlanValidation.MAX_REFERENCE_DOMAINS) {
            throw new IllegalArgumentException("reference-domain registry must be non-empty and bounded");
        }
        HashSet<String> ids = new HashSet<>();
        for (RegisteredDomain domain : registered) {
            if (!ids.add(domain.version().domainId())) {
                throw new IllegalArgumentException("reference-domain ids must be unique");
            }
        }
        this.domains = registered;
        this.byId = registered.stream().collect(Collectors.toUnmodifiableMap(
                domain -> domain.version().domainId(), Function.identity()));
        this.requiredDomains = registered.stream().map(RegisteredDomain::version).toList();
    }

    public List<GcReferenceDomainVersion> requiredDomains() {
        return requiredDomains;
    }

    public boolean contains(String domainId, int protocolVersion) {
        RegisteredDomain domain = byId.get(Objects.requireNonNull(domainId, "domainId"));
        return domain != null && domain.version().protocolVersion() == protocolVersion;
    }

    public CompletableFuture<GcReferenceCollection> snapshotForDeletion(GcReferenceQuery query) {
        Objects.requireNonNull(query, "query");
        MaterializationDeadline deadline = new MaterializationDeadline(config.operationTimeout(), scheduler);
        CompletableFuture<GcReferenceCollection> result = snapshotForDeletion(query, deadline);
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    CompletableFuture<GcReferenceCollection> snapshotForDeletion(
            GcReferenceQuery query, MaterializationDeadline deadline) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(deadline, "deadline");
        ArrayList<GcReferenceSnapshot> snapshots = new ArrayList<>(domains.size());
        return collect(query, snapshots, 0, deadline);
    }

    public CompletableFuture<Boolean> stillMatches(GcReferenceCollection collection) {
        Objects.requireNonNull(collection, "collection");
        if (!collection.clear()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "only a CLEAR reference collection can be revalidated"));
        }
        requireExactRegisteredSet(collection.snapshots());
        MaterializationDeadline deadline = new MaterializationDeadline(config.operationTimeout(), scheduler);
        CompletableFuture<Boolean> result = stillMatches(collection, deadline);
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    CompletableFuture<Boolean> stillMatches(
            GcReferenceCollection collection, MaterializationDeadline deadline) {
        Objects.requireNonNull(collection, "collection");
        Objects.requireNonNull(deadline, "deadline");
        if (!collection.clear()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "only a CLEAR reference collection can be revalidated"));
        }
        requireExactRegisteredSet(collection.snapshots());
        return revalidate(collection.snapshots(), 0, deadline);
    }

    private CompletableFuture<GcReferenceCollection> collect(
            GcReferenceQuery query,
            ArrayList<GcReferenceSnapshot> snapshots,
            int index,
            MaterializationDeadline deadline) {
        if (index == domains.size()) {
            return CompletableFuture.completedFuture(new GcReferenceCollection(
                    query,
                    snapshots,
                    GcReferenceCollectionStatus.CLEAR,
                    Optional.empty()));
        }
        RegisteredDomain registered = domains.get(index);
        return deadline.bound(
                        () -> registered.domain().snapshot(query),
                        "snapshot GC reference domain " + registered.version().domainId())
                .thenCompose(snapshot -> {
                    requireExactSnapshot(registered, query, snapshot);
                    snapshots.add(snapshot);
                    GcReferenceCollectionStatus blocker = blocker(snapshot);
                    if (blocker != null) {
                        return CompletableFuture.completedFuture(new GcReferenceCollection(
                                query,
                                snapshots,
                                blocker,
                                Optional.of(snapshot.domainId())));
                    }
                    return collect(query, snapshots, index + 1, deadline);
                });
    }

    private CompletableFuture<Boolean> revalidate(
            List<GcReferenceSnapshot> snapshots,
            int index,
            MaterializationDeadline deadline) {
        if (index == snapshots.size()) {
            return CompletableFuture.completedFuture(true);
        }
        GcReferenceSnapshot snapshot = snapshots.get(index);
        RegisteredDomain registered = byId.get(snapshot.domainId());
        return deadline.bound(
                        () -> registered.domain().stillMatches(snapshot),
                        "revalidate GC reference domain " + snapshot.domainId())
                .thenCompose(matches -> {
                    if (matches == null) {
                        return CompletableFuture.failedFuture(invariant(
                                "reference domain returned a null stillMatches result"));
                    }
                    return matches
                            ? revalidate(snapshots, index + 1, deadline)
                            : CompletableFuture.completedFuture(false);
                });
    }

    private GcReferenceCollectionStatus blocker(GcReferenceSnapshot snapshot) {
        if (snapshot.authorityCount() > config.maxAuthoritiesPerDomainSnapshot()
                || snapshot.referenceCount() > config.maxReferencesPerDomainSnapshot()) {
            return GcReferenceCollectionStatus.LIMIT_EXCEEDED;
        }
        if (!snapshot.complete()) {
            return GcReferenceCollectionStatus.INCOMPLETE;
        }
        return snapshot.veto() ? GcReferenceCollectionStatus.VETOED : null;
    }

    private void requireExactRegisteredSet(List<GcReferenceSnapshot> snapshots) {
        if (snapshots.size() != domains.size()) {
            throw invariant("CLEAR reference collection does not cover the registered domain set");
        }
        for (int index = 0; index < domains.size(); index++) {
            RegisteredDomain expected = domains.get(index);
            GcReferenceSnapshot actual = snapshots.get(index);
            if (!expected.matches(actual)) {
                throw invariant("reference collection differs from the registered domain set");
            }
        }
    }

    private static void requireExactSnapshot(
            RegisteredDomain registered,
            GcReferenceQuery query,
            GcReferenceSnapshot snapshot) {
        if (snapshot == null
                || !registered.matches(snapshot)
                || !snapshot.queryIdentitySha256().equals(query.queryIdentitySha256())) {
            throw invariant("reference domain returned a mismatched snapshot identity");
        }
    }

    private static NereusException invariant(String message) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private record RegisteredDomain(
            GcReferenceDomain domain,
            GcReferenceDomainVersion version) {
        private RegisteredDomain(GcReferenceDomain domain) {
            this(
                    Objects.requireNonNull(domain, "domain"),
                    new GcReferenceDomainVersion(domain.domainId(), domain.protocolVersion()));
        }

        private boolean matches(GcReferenceSnapshot snapshot) {
            return snapshot.domainId().equals(version.domainId())
                    && snapshot.protocolVersion() == version.protocolVersion();
        }
    }
}
