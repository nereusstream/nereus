/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.append.GenerationZeroProtectionIdentities;
import com.nereusstream.core.physical.ObjectProtection;
import com.nereusstream.core.physical.ObjectProtectionManager;
import com.nereusstream.core.physical.ObjectProtectionOwner;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.AppendRecoveryCommitEncoding;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.StreamCommitKeyIdentity;
import com.nereusstream.metadata.oxia.VersionedObjectProtection;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.retirement.SourceRetirementMetadataStore;
import com.nereusstream.metadata.oxia.retirement.VersionedGenerationZeroCommit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Freezes abandoned {@code REACHABLE_APPEND} owners into the ordinary sealed physical-GC plan.
 *
 * <p>A protection is never interpreted as abandoned by age alone. The exact canonical commit key is decoded first,
 * its present value (if any) must still identify the same Object-WAL target and owner version/SHA, and the resulting
 * commit/protection removals are reloaded at every collector fence. Head, recovery, generation and projection
 * reachability remain owned by the registered global reference domains.
 */
public final class AbandonedAppendIntentPlanBuilder implements GcPlanMetadataRevalidator {
    private static final ReadTargetCodecRegistry TARGET_CODECS =
            ReadTargetCodecRegistry.phase15();

    private final String cluster;
    private final PhysicalObjectMetadataStore physicalMetadata;
    private final SourceRetirementMetadataStore sourceMetadata;
    private final ObjectProtectionManager protectionManager;
    private final PhysicalGcConfig config;
    private final ScheduledExecutorService scheduler;
    private final OxiaKeyspace l0Keys;
    private final long abandonmentGraceMillis;

    public AbandonedAppendIntentPlanBuilder(
            String cluster,
            PhysicalObjectMetadataStore physicalMetadata,
            SourceRetirementMetadataStore sourceMetadata,
            ObjectProtectionManager protectionManager,
            PhysicalGcConfig config,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.physicalMetadata = Objects.requireNonNull(
                physicalMetadata, "physicalMetadata");
        this.sourceMetadata = Objects.requireNonNull(sourceMetadata, "sourceMetadata");
        this.protectionManager = Objects.requireNonNull(
                protectionManager, "protectionManager");
        this.config = Objects.requireNonNull(config, "config");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.l0Keys = new OxiaKeyspace(this.cluster);
        this.abandonmentGraceMillis = checkedAdd(
                config.orphanGrace().toMillis(),
                config.maximumClockSkew().toMillis());
    }

    /** Inspects one ACTIVE root and epoch-rebinds only stale, exactly revalidated append protections. */
    public CompletableFuture<Inspection> inspectActive(
            VersionedPhysicalObjectRoot activeRoot) {
        VersionedPhysicalObjectRoot active = Objects.requireNonNull(
                activeRoot, "activeRoot");
        if (active.value().lifecycle() != PhysicalObjectLifecycle.ACTIVE) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "abandoned append inspection requires an exact ACTIVE root"));
        }
        return inspect(active, true);
    }

    /** Reconstructs the same exact append-intent plan for a MARKED root without performing mutation. */
    public CompletableFuture<Inspection> inspectMarked(
            VersionedPhysicalObjectRoot markedRoot) {
        VersionedPhysicalObjectRoot marked = Objects.requireNonNull(
                markedRoot, "markedRoot");
        if (marked.value().lifecycle() != PhysicalObjectLifecycle.MARKED) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "abandoned append recovery requires an exact MARKED root"));
        }
        return inspect(marked, false);
    }

    /** Reloads all current append-protection owners so owner appearance/absence is plan drift. */
    @Override
    public CompletableFuture<List<GcPlannedMetadataRemoval>> reload(
            GcCandidate candidate,
            List<GcPlannedMetadataRemoval> expectedRemovals) {
        GcCandidate exact = Objects.requireNonNull(candidate, "candidate");
        GcPlanValidation.canonicalAllowEmpty(
                expectedRemovals,
                GcPlanValidation.METADATA_ORDER,
                config.maxReferencesPerDomainSnapshot(),
                "expectedRemovals");
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        CompletableFuture<List<GcPlannedMetadataRemoval>> result = scanProtections(
                        exact.object(), deadline)
                .thenCompose(protections -> resolve(
                        exact.object(),
                        exact.activeRootLifecycleEpoch(),
                        protections,
                        deadline))
                .thenCompose(resolved -> resolved.blocked()
                        ? CompletableFuture.failedFuture(condition(
                                "ownerless metadata revalidation found a non-removable protection"))
                        : CompletableFuture.completedFuture(resolved.metadataRemovals()));
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    private CompletableFuture<Inspection> inspect(
            VersionedPhysicalObjectRoot root,
            boolean allowEpochRebind) {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.from(root.value());
        long activeEpoch = root.value().lifecycle() == PhysicalObjectLifecycle.ACTIVE
                ? root.value().lifecycleEpoch()
                : Math.subtractExact(root.value().lifecycleEpoch(), 1);
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        CompletableFuture<Inspection> result = scanProtections(object, deadline)
                .thenCompose(protections -> resolve(
                        object, activeEpoch, protections, deadline))
                .thenCompose(resolved -> {
                    if (resolved.blocked()) {
                        return CompletableFuture.completedFuture(
                                Inspection.blocked(root.value().orphanNotBeforeMillis()));
                    }
                    List<ResolvedProtection> stale = resolved.protections().stream()
                            .filter(value -> value.protection()
                                            .protection()
                                            .value()
                                            .rootLifecycleEpoch()
                                    != activeEpoch)
                            .toList();
                    if (stale.isEmpty()) {
                        return CompletableFuture.completedFuture(Inspection.eligible(
                                Math.max(
                                        root.value().orphanNotBeforeMillis(),
                                        resolved.notBeforeMillis()),
                                resolved.protectionRemovals(),
                                resolved.metadataRemovals()));
                    }
                    if (!allowEpochRebind) {
                        return CompletableFuture.completedFuture(
                                Inspection.blocked(root.value().orphanNotBeforeMillis()));
                    }
                    return rebindStale(
                                    object, stale, 0, deadline)
                            .thenApply(ignored -> Inspection.rebound(
                                    root.value().orphanNotBeforeMillis()));
                });
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    private CompletableFuture<ResolvedInspection> resolve(
            PhysicalObjectIdentity object,
            long activeEpoch,
            List<GcPlannedProtectionRemoval> protections,
            MaterializationDeadline deadline) {
        if (protections.isEmpty()) {
            return CompletableFuture.completedFuture(ResolvedInspection.clear(
                    List.of(), List.of(), List.of(), 0));
        }
        if (object.kind() != PhysicalObjectKind.OBJECT_WAL) {
            boolean hasReachableAppend = protections.stream().anyMatch(value ->
                    value.identity().type() == ObjectProtectionType.REACHABLE_APPEND);
            if (hasReachableAppend) {
                return CompletableFuture.failedFuture(invariant(
                        "REACHABLE_APPEND protects a non-Object-WAL physical root"));
            }
            return CompletableFuture.completedFuture(
                    ResolvedInspection.blockedInspection());
        }
        return resolveOne(
                object,
                activeEpoch,
                protections,
                0,
                new ArrayList<>(),
                new ArrayList<>(),
                0,
                deadline);
    }

    private CompletableFuture<ResolvedInspection> resolveOne(
            PhysicalObjectIdentity object,
            long activeEpoch,
            List<GcPlannedProtectionRemoval> protections,
            int index,
            ArrayList<ResolvedProtection> resolved,
            ArrayList<GcPlannedMetadataRemoval> metadataRemovals,
            long notBeforeMillis,
            MaterializationDeadline deadline) {
        if (index == protections.size()) {
            List<GcPlannedMetadataRemoval> canonicalMetadata = metadataRemovals.stream()
                    .sorted(Comparator.comparing(GcPlannedMetadataRemoval::key))
                    .toList();
            return CompletableFuture.completedFuture(ResolvedInspection.clear(
                    protections,
                    resolved,
                    canonicalMetadata,
                    notBeforeMillis));
        }
        GcPlannedProtectionRemoval planned = protections.get(index);
        VersionedObjectProtection protection = planned.protection();
        ObjectProtectionRecord value = protection.value();
        if (planned.identity().type() != ObjectProtectionType.REACHABLE_APPEND) {
            return CompletableFuture.completedFuture(
                    ResolvedInspection.blockedInspection());
        }
        if (value.rootLifecycleEpoch() > activeEpoch) {
            return CompletableFuture.failedFuture(invariant(
                    "REACHABLE_APPEND lifecycle epoch is ahead of its root"));
        }
        StreamCommitKeyIdentity ownerIdentity;
        try {
            ownerIdentity = l0Keys.parseStreamCommitKey(value.ownerKey());
        } catch (IllegalArgumentException malformed) {
            return CompletableFuture.failedFuture(invariant(
                    "REACHABLE_APPEND owner key is not canonical", malformed));
        }
        String expectedReferenceId =
                GenerationZeroProtectionIdentities.reachableAppendReferenceId(
                        ownerIdentity.streamId(),
                        ownerIdentity.commitId(),
                        object.objectKeyHash());
        if (!value.referenceId().equals(expectedReferenceId)) {
            return CompletableFuture.failedFuture(invariant(
                    "REACHABLE_APPEND reference id does not match its exact owner/object"));
        }
        long protectionBoundary = abandonmentBoundary(value.createdAtMillis());
        return deadline.bound(
                        () -> sourceMetadata.getCommitNodeByKey(
                                cluster, value.ownerKey()),
                        "load exact REACHABLE_APPEND owner")
                .thenCompose(optionalOwner -> {
                    Optional<VersionedGenerationZeroCommit> owner = optionalOwner.map(commit -> {
                        requireCommitMatches(
                                object, ownerIdentity, protection, commit);
                        metadataRemovals.add(removal(commit));
                        return commit;
                    });
                    long ownerBoundary = owner
                            .map(commit -> abandonmentBoundary(
                                    commit.canonicalCommit().preparedAtMillis()))
                            .orElse(0L);
                    resolved.add(new ResolvedProtection(planned, owner));
                    return resolveOne(
                            object,
                            activeEpoch,
                            protections,
                            index + 1,
                            resolved,
                            metadataRemovals,
                            Math.max(
                                    notBeforeMillis,
                                    Math.max(protectionBoundary, ownerBoundary)),
                            deadline);
                });
    }

    private CompletableFuture<Void> rebindStale(
            PhysicalObjectIdentity object,
            List<ResolvedProtection> stale,
            int index,
            MaterializationDeadline deadline) {
        if (index == stale.size()) {
            return CompletableFuture.completedFuture(null);
        }
        ResolvedProtection resolved = stale.get(index);
        VersionedObjectProtection versioned = resolved.protection().protection();
        ObjectProtectionRecord value = versioned.value();
        ObjectProtectionOwner owner = owner(value);
        ObjectProtection handle = new ObjectProtection(
                object,
                resolved.protection().identity(),
                owner,
                value.rootLifecycleEpoch(),
                value.createdAtMillis(),
                value.expiresAtMillis(),
                versioned.metadataVersion(),
                versioned.durableValueSha256());
        return deadline.bound(
                        () -> protectionManager.transfer(
                                handle,
                                owner,
                                expected -> revalidateOwnerState(
                                        object,
                                        resolved,
                                        expected,
                                        deadline)),
                        "rebind abandoned REACHABLE_APPEND to the current ACTIVE root epoch")
                .thenCompose(ignored -> rebindStale(
                        object, stale, index + 1, deadline));
    }

    private CompletableFuture<Void> revalidateOwnerState(
            PhysicalObjectIdentity object,
            ResolvedProtection resolved,
            ObjectProtectionOwner expectedOwner,
            MaterializationDeadline deadline) {
        VersionedObjectProtection protection = resolved.protection().protection();
        ObjectProtectionRecord value = protection.value();
        ObjectProtectionOwner plannedOwner = owner(value);
        if (!plannedOwner.equals(expectedOwner)) {
            return CompletableFuture.failedFuture(condition(
                    "REACHABLE_APPEND epoch rebind received another owner"));
        }
        StreamCommitKeyIdentity ownerIdentity;
        try {
            ownerIdentity = l0Keys.parseStreamCommitKey(value.ownerKey());
        } catch (IllegalArgumentException malformed) {
            return CompletableFuture.failedFuture(invariant(
                    "REACHABLE_APPEND owner key changed during epoch rebind", malformed));
        }
        return deadline.bound(
                        () -> sourceMetadata.getCommitNodeByKey(
                                cluster, value.ownerKey()),
                        "revalidate REACHABLE_APPEND owner during epoch rebind")
                .thenCompose(current -> {
                    if (resolved.owner().isEmpty()) {
                        return current.isEmpty()
                                ? CompletableFuture.completedFuture(null)
                                : CompletableFuture.failedFuture(condition(
                                        "previously absent append owner appeared during epoch rebind"));
                    }
                    if (current.isEmpty()
                            || !current.orElseThrow().equals(
                                    resolved.owner().orElseThrow())) {
                        return CompletableFuture.failedFuture(condition(
                                "append owner changed during protection epoch rebind"));
                    }
                    requireCommitMatches(
                            object,
                            ownerIdentity,
                            protection,
                            current.orElseThrow());
                    return CompletableFuture.completedFuture(null);
                });
    }

    private CompletableFuture<List<GcPlannedProtectionRemoval>> scanProtections(
            PhysicalObjectIdentity object,
            MaterializationDeadline deadline) {
        return scanProtections(
                object,
                Optional.empty(),
                new ArrayList<>(),
                null,
                deadline);
    }

    private CompletableFuture<List<GcPlannedProtectionRemoval>> scanProtections(
            PhysicalObjectIdentity object,
            Optional<F4ScanToken> continuation,
            ArrayList<GcPlannedProtectionRemoval> values,
            String previousKey,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> physicalMetadata.scanProtections(
                                cluster,
                                object.objectKeyHash(),
                                continuation,
                                config.metadataScanPageSize()),
                        "scan append-intent physical protections")
                .thenCompose(page -> {
                    requireProgress(page, previousKey);
                    for (VersionedObjectProtection protection : page.values()) {
                        if (!protection.value().objectKeyHash().equals(
                                object.objectKeyHash().value())) {
                            return CompletableFuture.failedFuture(invariant(
                                    "append-intent protection scan escaped its object"));
                        }
                        values.add(new GcPlannedProtectionRemoval(protection));
                        if (values.size() > config.maxReferencesPerDomainSnapshot()) {
                            return CompletableFuture.failedFuture(invariant(
                                    "append-intent protection scan exceeded its configured bound"));
                        }
                    }
                    if (page.continuation().isEmpty()) {
                        return CompletableFuture.completedFuture(List.copyOf(values));
                    }
                    String nextPrevious = page.values()
                            .get(page.values().size() - 1)
                            .key();
                    return scanProtections(
                            object,
                            page.continuation(),
                            values,
                            nextPrevious,
                            deadline);
                });
    }

    private static void requireCommitMatches(
            PhysicalObjectIdentity object,
            StreamCommitKeyIdentity ownerIdentity,
            VersionedObjectProtection protection,
            VersionedGenerationZeroCommit commit) {
        ObjectProtectionRecord value = protection.value();
        if (commit.sourceEncoding()
                        != AppendRecoveryCommitEncoding.GENERIC_STREAM_COMMIT_TARGET_V1
                || !commit.key().equals(value.ownerKey())
                || !commit.streamId().equals(ownerIdentity.streamId())
                || !commit.commitId().equals(ownerIdentity.commitId())
                || commit.metadataVersion() != value.ownerMetadataVersion()
                || !commit.durableValueSha256().value().equals(
                        value.ownerIdentitySha256())) {
            throw invariant("REACHABLE_APPEND owner version/SHA/identity changed");
        }
        ReadTarget target = TARGET_CODECS.decode(
                commit.canonicalCommit().readTarget());
        if (!(target instanceof ObjectSliceReadTarget objectTarget)
                || objectTarget.objectType()
                        != ObjectType.MULTI_STREAM_WAL_OBJECT
                || !objectTarget.objectKey().equals(object.objectKey())
                || (object.objectId().isPresent()
                        && !object.objectId().orElseThrow().equals(
                                objectTarget.objectId()))) {
            throw invariant("REACHABLE_APPEND owner does not identify the protected Object WAL");
        }
    }

    private static GcPlannedMetadataRemoval removal(
            VersionedGenerationZeroCommit commit) {
        return new GcPlannedMetadataRemoval(
                GenerationZeroCommitRetirementHandler.REMOVAL_TYPE,
                commit.key(),
                commit.metadataVersion(),
                commit.durableValueSha256());
    }

    private static ObjectProtectionOwner owner(ObjectProtectionRecord value) {
        return new ObjectProtectionOwner(
                value.ownerKey(),
                value.ownerMetadataVersion(),
                new Checksum(
                        ChecksumType.SHA256,
                        value.ownerIdentitySha256()));
    }

    private long abandonmentBoundary(long timestamp) {
        return checkedAdd(timestamp, abandonmentGraceMillis);
    }

    private static long checkedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static void requireProgress(
            ObjectProtectionScanPage page, String previousKey) {
        if (previousKey != null
                && !page.values().isEmpty()
                && page.values().get(0).key().compareTo(previousKey) <= 0) {
            throw invariant("append-intent protection scan did not advance");
        }
        if (page.continuation().isPresent() && page.values().isEmpty()) {
            throw invariant("append-intent protection scan returned an empty continuation page");
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static NereusException invariant(String message) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
    }

    private static NereusException invariant(
            String message, Throwable cause) {
        return new NereusException(
                ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static NereusException condition(String message) {
        return new NereusException(
                ErrorCode.METADATA_CONDITION_FAILED, true, message);
    }

    private record ResolvedProtection(
            GcPlannedProtectionRemoval protection,
            Optional<VersionedGenerationZeroCommit> owner) {
        private ResolvedProtection {
            Objects.requireNonNull(protection, "protection");
            owner = Objects.requireNonNull(owner, "owner");
        }
    }

    private record ResolvedInspection(
            boolean blocked,
            List<GcPlannedProtectionRemoval> protectionRemovals,
            List<ResolvedProtection> protections,
            List<GcPlannedMetadataRemoval> metadataRemovals,
            long notBeforeMillis) {
        private ResolvedInspection {
            protectionRemovals = List.copyOf(Objects.requireNonNull(
                    protectionRemovals, "protectionRemovals"));
            protections = List.copyOf(Objects.requireNonNull(
                    protections, "protections"));
            metadataRemovals = List.copyOf(Objects.requireNonNull(
                    metadataRemovals, "metadataRemovals"));
            if (notBeforeMillis < 0) {
                throw new IllegalArgumentException("notBeforeMillis must be non-negative");
            }
        }

        private static ResolvedInspection clear(
                List<GcPlannedProtectionRemoval> protectionRemovals,
                List<ResolvedProtection> protections,
                List<GcPlannedMetadataRemoval> metadataRemovals,
                long notBeforeMillis) {
            return new ResolvedInspection(
                    false,
                    protectionRemovals,
                    protections,
                    metadataRemovals,
                    notBeforeMillis);
        }

        private static ResolvedInspection blockedInspection() {
            return new ResolvedInspection(
                    true, List.of(), List.of(), List.of(), 0);
        }
    }

    public enum InspectionStatus {
        ELIGIBLE,
        BLOCKED,
        REBOUND
    }

    /** Exact local append-intent facts admitted to, or conservatively excluded from, ownerless GC. */
    public record Inspection(
            InspectionStatus status,
            long notBeforeMillis,
            List<GcPlannedProtectionRemoval> protectionRemovals,
            List<GcPlannedMetadataRemoval> metadataRemovals) {
        public Inspection {
            Objects.requireNonNull(status, "status");
            if (notBeforeMillis < 0) {
                throw new IllegalArgumentException("notBeforeMillis must be non-negative");
            }
            protectionRemovals = List.copyOf(Objects.requireNonNull(
                    protectionRemovals, "protectionRemovals"));
            metadataRemovals = List.copyOf(Objects.requireNonNull(
                    metadataRemovals, "metadataRemovals"));
            if (status != InspectionStatus.ELIGIBLE
                    && (!protectionRemovals.isEmpty()
                            || !metadataRemovals.isEmpty())) {
                throw new IllegalArgumentException(
                        "only an eligible inspection carries a removal plan");
            }
        }

        public boolean eligible() {
            return status == InspectionStatus.ELIGIBLE;
        }

        private static Inspection eligible(
                long notBeforeMillis,
                List<GcPlannedProtectionRemoval> protectionRemovals,
                List<GcPlannedMetadataRemoval> metadataRemovals) {
            return new Inspection(
                    InspectionStatus.ELIGIBLE,
                    notBeforeMillis,
                    protectionRemovals,
                    metadataRemovals);
        }

        private static Inspection blocked(long notBeforeMillis) {
            return new Inspection(
                    InspectionStatus.BLOCKED,
                    notBeforeMillis,
                    List.of(),
                    List.of());
        }

        private static Inspection rebound(long notBeforeMillis) {
            return new Inspection(
                    InspectionStatus.REBOUND,
                    notBeforeMillis,
                    List.of(),
                    List.of());
        }
    }
}
