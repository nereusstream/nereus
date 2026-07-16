/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization.gc;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectId;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.core.physical.GcReferenceQuery;
import com.nereusstream.core.physical.GcReferenceQueryKind;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.materialization.MaterializationDeadline;
import com.nereusstream.metadata.oxia.ObjectProtectionScanPage;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.ReaderLeaseScanPage;
import com.nereusstream.metadata.oxia.VersionedPhysicalObjectRoot;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import com.nereusstream.metadata.oxia.retirement.ObjectAuditRetirementStore;
import com.nereusstream.metadata.oxia.retirement.VersionedObjectManifestAudit;
import com.nereusstream.metadata.oxia.retirement.VersionedObjectReferencesAudit;
import com.nereusstream.objectstore.DeleteObjectOptions;
import com.nereusstream.objectstore.DeleteObjectResult;
import com.nereusstream.objectstore.HeadObjectOptions;
import com.nereusstream.objectstore.HeadObjectResult;
import com.nereusstream.objectstore.ObjectStore;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * Implements the separated exact-HEAD and authority windows required before retiring a DELETED root.
 *
 * <p>The root is always the last metadata key removed. Phase 1 references are deleted before their manifest, and
 * every response-loss path reauthenticates the exact unchanged DELETED root before accepting absence.
 */
public final class DefaultPhysicalRootTombstoneRetirementCoordinator
        implements PhysicalRootTombstoneRetirementCoordinator {
    private final String cluster;
    private final PhysicalGcConfig config;
    private final PhysicalObjectMetadataStore metadataStore;
    private final ObjectAuditRetirementStore auditStore;
    private final GcReferenceDomainRegistry referenceDomains;
    private final ObjectStore objectStore;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;

    public DefaultPhysicalRootTombstoneRetirementCoordinator(
            String cluster,
            PhysicalGcConfig config,
            PhysicalObjectMetadataStore metadataStore,
            ObjectAuditRetirementStore auditStore,
            GcReferenceDomainRegistry referenceDomains,
            ObjectStore objectStore,
            Clock clock,
            ScheduledExecutorService scheduler) {
        this.cluster = requireText(cluster, "cluster");
        this.config = Objects.requireNonNull(config, "config");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.auditStore = Objects.requireNonNull(auditStore, "auditStore");
        this.referenceDomains = Objects.requireNonNull(referenceDomains, "referenceDomains");
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    @Override
    public CompletableFuture<TombstoneRetirementResult> retire(
            VersionedPhysicalObjectRoot deletedRoot) {
        VersionedPhysicalObjectRoot discovered = Objects.requireNonNull(
                deletedRoot, "deletedRoot");
        if (discovered.value().lifecycle() != PhysicalObjectLifecycle.DELETED) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "tombstone retirement requires an exact DELETED root"));
        }
        ObjectKeyHash object = object(discovered);
        if (!config.enabled()) {
            return CompletableFuture.completedFuture(TombstoneRetirementResult.simple(
                    object, discovered.metadataVersion(), TombstoneRetirementStatus.DISABLED));
        }
        if (config.dryRun()) {
            return CompletableFuture.completedFuture(TombstoneRetirementResult.simple(
                    object, discovered.metadataVersion(), TombstoneRetirementStatus.DRY_RUN));
        }
        long now = nonNegativeNow();
        MaterializationDeadline deadline = new MaterializationDeadline(
                config.operationTimeout(), scheduler);
        CompletableFuture<TombstoneRetirementResult> result = reloadExact(discovered, deadline)
                .thenCompose(current -> current
                        .<CompletableFuture<TombstoneRetirementResult>>map(root -> {
                            if (!elapsed(
                                    root.value().deletedAtMillis(),
                                    config.tombstoneAuditGrace(),
                                    now)) {
                                return CompletableFuture.completedFuture(simple(
                                        root, TombstoneRetirementStatus.NOT_OLD_ENOUGH));
                            }
                            return firstAuthorityPass(root, now, deadline);
                        })
                        .orElseGet(() -> CompletableFuture.completedFuture(
                                simple(discovered, TombstoneRetirementStatus.VERSION_CHANGED))));
        result.whenComplete((ignored, failure) -> deadline.close());
        return result;
    }

    private CompletableFuture<TombstoneRetirementResult> firstAuthorityPass(
            VersionedPhysicalObjectRoot root,
            long now,
            MaterializationDeadline deadline) {
        return scanAuthority(root, deadline).thenCompose(scan -> {
            if (!scan.clearAbsent()) {
                return resolveBlockedScan(root, scan, false, false, deadline);
            }
            Checksum proof = proof(root, scan.collection().orElseThrow());
            if (root.value().tombstoneFirstAbsentAtMillis() == 0
                    || !root.value().tombstoneProofSha256().equals(proof.value())) {
                return replaceObservation(root, now, proof, deadline)
                        .thenApply(updated -> updated
                                .map(value -> simple(
                                        value,
                                        TombstoneRetirementStatus.NOT_OLD_ENOUGH))
                                .orElseGet(() -> simple(
                                        root,
                                        TombstoneRetirementStatus.VERSION_CHANGED)));
            }
            if (!elapsed(
                    root.value().tombstoneFirstAbsentAtMillis(),
                    config.orphanGrace(),
                    now)) {
                return CompletableFuture.completedFuture(
                        simple(root, TombstoneRetirementStatus.NOT_OLD_ENOUGH));
            }
            return secondAuthorityPass(root, proof, now, deadline);
        });
    }

    private CompletableFuture<TombstoneRetirementResult> secondAuthorityPass(
            VersionedPhysicalObjectRoot expected,
            Checksum firstProof,
            long now,
            MaterializationDeadline deadline) {
        return reloadExact(expected, deadline).thenCompose(reloaded -> {
            if (reloaded.isEmpty()) {
                return CompletableFuture.completedFuture(
                        simple(expected, TombstoneRetirementStatus.VERSION_CHANGED));
            }
            VersionedPhysicalObjectRoot d2 = reloaded.orElseThrow();
            return scanAuthority(d2, deadline).thenCompose(scan -> {
                if (!scan.clearAbsent()) {
                    return resolveBlockedScan(d2, scan, false, false, deadline);
                }
                Checksum secondProof = proof(d2, scan.collection().orElseThrow());
                if (!firstProof.equals(secondProof)
                        || !d2.value().tombstoneProofSha256().equals(secondProof.value())) {
                    return replaceObservation(d2, now, secondProof, deadline)
                            .thenApply(updated -> updated
                                    .map(value -> simple(
                                            value,
                                            TombstoneRetirementStatus.NOT_OLD_ENOUGH))
                                    .orElseGet(() -> simple(
                                            d2,
                                            TombstoneRetirementStatus.VERSION_CHANGED)));
                }
                return captureAudits(d2, deadline)
                        .thenCompose(audits -> head(d2, deadline)
                                .thenCompose(head -> {
                                    if (head.state() != HeadState.ABSENT) {
                                        return resolveHeadAfterClearScan(
                                                d2,
                                                scan.collection().orElseThrow(),
                                                head,
                                                false,
                                                false,
                                                deadline);
                                    }
                                    return retireAuditsAndRoot(
                                            d2,
                                            secondProof,
                                            audits,
                                            now,
                                            deadline);
                                }));
            });
        });
    }

    private CompletableFuture<TombstoneRetirementResult> retireAuditsAndRoot(
            VersionedPhysicalObjectRoot root,
            Checksum expectedProof,
            AuditSnapshot audits,
            long now,
            MaterializationDeadline deadline) {
        return reloadExact(root, deadline).thenCompose(beforeReferences -> {
            if (beforeReferences.isEmpty()) {
                return CompletableFuture.completedFuture(result(
                        root,
                        TombstoneRetirementStatus.VERSION_CHANGED,
                        false,
                        false,
                        false));
            }
            return deleteReferences(root, audits.references(), deadline)
                    .thenCompose(referencesRetired -> reloadExact(root, deadline)
                            .thenCompose(beforeManifest -> {
                                if (beforeManifest.isEmpty()) {
                                    return CompletableFuture.completedFuture(result(
                                            root,
                                            TombstoneRetirementStatus.VERSION_CHANGED,
                                            referencesRetired,
                                            false,
                                            false));
                                }
                                return deleteManifest(root, audits.manifest(), deadline)
                                        .thenCompose(manifestRetired -> requireAuditsAbsent(
                                                        root, deadline)
                                                .thenCompose(ignored ->
                                                        scanAuthority(root, deadline))
                                                .thenCompose(scan -> {
                                                    if (!scan.clearAbsent()) {
                                                        return resolveBlockedScan(
                                                                root,
                                                                scan,
                                                                referencesRetired,
                                                                manifestRetired,
                                                                deadline);
                                                    }
                                                    Checksum finalProof = proof(
                                                            root,
                                                            scan.collection()
                                                                    .orElseThrow());
                                                    if (!expectedProof.equals(finalProof)) {
                                                        return replaceObservation(
                                                                        root,
                                                                        now,
                                                                        finalProof,
                                                                        deadline)
                                                                .thenApply(updated -> result(
                                                                        updated.orElse(root),
                                                                        TombstoneRetirementStatus
                                                                                .NOT_OLD_ENOUGH,
                                                                        referencesRetired,
                                                                        manifestRetired,
                                                                        false));
                                                    }
                                                    return reloadExact(root, deadline)
                                                            .thenCompose(exact -> {
                                                                if (exact.isEmpty()) {
                                                                    return CompletableFuture
                                                                            .completedFuture(
                                                                                    result(
                                                                                            root,
                                                                                            TombstoneRetirementStatus
                                                                                                    .VERSION_CHANGED,
                                                                                            referencesRetired,
                                                                                            manifestRetired,
                                                                                            false));
                                                                }
                                                                return deleteRoot(
                                                                        exact.orElseThrow(),
                                                                        referencesRetired,
                                                                        manifestRetired,
                                                                        deadline);
                                                            });
                                                }));
                            }));
        });
    }

    private CompletableFuture<TombstoneRetirementResult> resolveBlockedScan(
            VersionedPhysicalObjectRoot root,
            AuthorityScan scan,
            boolean referencesRetired,
            boolean manifestRetired,
            MaterializationDeadline deadline) {
        if (scan.head().state() == HeadState.EXACT_PRESENT
                && scan.blockingStatus().orElseThrow()
                        == TombstoneRetirementStatus.OBJECT_PRESENT) {
            return deleteLateObject(
                    root,
                    scan.collection().orElseThrow(),
                    referencesRetired,
                    manifestRetired,
                    deadline);
        }
        TombstoneRetirementStatus status = switch (scan.head().state()) {
            case QUARANTINED -> TombstoneRetirementStatus.QUARANTINED;
            case ABSENT, EXACT_PRESENT -> scan.blockingStatus().orElseThrow();
        };
        return clearObservation(root, deadline).thenApply(updated -> result(
                updated.orElse(root),
                updated.isPresent() ? status : TombstoneRetirementStatus.VERSION_CHANGED,
                referencesRetired,
                manifestRetired,
                false));
    }

    private CompletableFuture<TombstoneRetirementResult> resolveHeadAfterClearScan(
            VersionedPhysicalObjectRoot root,
            GcReferenceCollection collection,
            HeadResult head,
            boolean referencesRetired,
            boolean manifestRetired,
            MaterializationDeadline deadline) {
        if (head.state() == HeadState.EXACT_PRESENT) {
            return deleteLateObject(
                    root,
                    collection,
                    referencesRetired,
                    manifestRetired,
                    deadline);
        }
        return clearObservation(root, deadline).thenApply(updated -> result(
                updated.orElse(root),
                updated.isPresent()
                        ? TombstoneRetirementStatus.QUARANTINED
                        : TombstoneRetirementStatus.VERSION_CHANGED,
                referencesRetired,
                manifestRetired,
                false));
    }

    private CompletableFuture<TombstoneRetirementResult> deleteLateObject(
            VersionedPhysicalObjectRoot root,
            GcReferenceCollection collection,
            boolean referencesRetired,
            boolean manifestRetired,
            MaterializationDeadline deadline) {
        return reloadExact(root, deadline).thenCompose(exact -> {
            if (exact.isEmpty()) {
                return CompletableFuture.completedFuture(result(
                        root,
                        TombstoneRetirementStatus.VERSION_CHANGED,
                        referencesRetired,
                        manifestRetired,
                        false));
            }
            return handlesPresent(root, deadline).thenCompose(present -> {
                if (present) {
                    return clearObservation(root, deadline).thenApply(updated -> result(
                            updated.orElse(root),
                            updated.isPresent()
                                    ? TombstoneRetirementStatus.HANDLE_PRESENT
                                    : TombstoneRetirementStatus.VERSION_CHANGED,
                            referencesRetired,
                            manifestRetired,
                            false));
                }
                return referenceDomains.stillMatches(collection, deadline)
                        .thenCompose(matches -> {
                            if (!matches) {
                                return clearObservation(root, deadline)
                                        .thenApply(updated -> result(
                                                updated.orElse(root),
                                                updated.isPresent()
                                                        ? TombstoneRetirementStatus
                                                                .DOMAIN_VETO
                                                        : TombstoneRetirementStatus
                                                                .VERSION_CHANGED,
                                                referencesRetired,
                                                manifestRetired,
                                                false));
                            }
                            return head(root, deadline).thenCompose(head -> {
                                if (head.state() == HeadState.ABSENT) {
                                    return clearObservation(root, deadline)
                                            .thenApply(updated -> result(
                                                    updated.orElse(root),
                                                    updated.isPresent()
                                                            ? TombstoneRetirementStatus
                                                                    .OBJECT_PRESENT
                                                            : TombstoneRetirementStatus
                                                                    .VERSION_CHANGED,
                                                    referencesRetired,
                                                    manifestRetired,
                                                    false));
                                }
                                if (head.state() == HeadState.QUARANTINED) {
                                    return clearObservation(root, deadline)
                                            .thenApply(updated -> result(
                                                    updated.orElse(root),
                                                    updated.isPresent()
                                                            ? TombstoneRetirementStatus
                                                                    .QUARANTINED
                                                            : TombstoneRetirementStatus
                                                                    .VERSION_CHANGED,
                                                    referencesRetired,
                                                    manifestRetired,
                                                    false));
                                }
                                return deleteExactLateObject(root, deadline)
                                        .thenCompose(outcome -> {
                                            if (outcome
                                                    == LateObjectDeleteOutcome
                                                            .VERSION_CHANGED) {
                                                return CompletableFuture.completedFuture(
                                                        result(
                                                                root,
                                                                TombstoneRetirementStatus
                                                                        .VERSION_CHANGED,
                                                                referencesRetired,
                                                                manifestRetired,
                                                                false));
                                            }
                                            if (outcome
                                                    == LateObjectDeleteOutcome
                                                            .QUARANTINED) {
                                                return clearObservation(root, deadline)
                                                        .thenApply(updated -> result(
                                                                updated.orElse(root),
                                                                updated.isPresent()
                                                                        ? TombstoneRetirementStatus
                                                                                .QUARANTINED
                                                                        : TombstoneRetirementStatus
                                                                                .VERSION_CHANGED,
                                                                referencesRetired,
                                                                manifestRetired,
                                                                false));
                                            }
                                            return clearObservation(root, deadline)
                                                    .thenApply(updated -> result(
                                                            updated.orElse(root),
                                                            updated.isPresent()
                                                                    ? TombstoneRetirementStatus
                                                                            .OBJECT_PRESENT
                                                                    : TombstoneRetirementStatus
                                                                            .VERSION_CHANGED,
                                                            referencesRetired,
                                                            manifestRetired,
                                                            false));
                                        });
                            });
                        });
            });
        });
    }

    private CompletableFuture<LateObjectDeleteOutcome> deleteExactLateObject(
            VersionedPhysicalObjectRoot root,
            MaterializationDeadline deadline) {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.from(root.value());
        CompletableFuture<DeleteObjectResult> deletion = deadline.bound(
                () -> objectStore.deleteObject(
                        object.objectKey(),
                        new DeleteObjectOptions(
                                object.objectLength(),
                                object.storageChecksum(),
                                object.etag(),
                                deadline.remaining())),
                "delete exact reappearing object under DELETED root");
        return deletion.handle((deleted, failure) -> {
            if (failure == null) {
                if (!deleted.key().equals(object.objectKey())) {
                    return CompletableFuture.completedFuture(
                            LateObjectDeleteOutcome.QUARANTINED);
                }
                return CompletableFuture.completedFuture(
                        LateObjectDeleteOutcome.DELETED_OR_ABSENT);
            }
            Throwable original = unwrap(failure);
            return reloadExact(root, deadline).thenCompose(exact -> {
                if (exact.isEmpty()) {
                    return CompletableFuture.completedFuture(
                            LateObjectDeleteOutcome.VERSION_CHANGED);
                }
                return head(root, deadline).thenCompose(reloaded -> {
                    if (reloaded.state() == HeadState.ABSENT) {
                        return CompletableFuture.completedFuture(
                                LateObjectDeleteOutcome.DELETED_OR_ABSENT);
                    }
                    if (reloaded.state() == HeadState.QUARANTINED) {
                        return CompletableFuture.completedFuture(
                                LateObjectDeleteOutcome.QUARANTINED);
                    }
                    return CompletableFuture.failedFuture(original);
                });
            });
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<AuthorityScan> scanAuthority(
            VersionedPhysicalObjectRoot root,
            MaterializationDeadline deadline) {
        return head(root, deadline).thenCompose(head -> {
            if (head.state() == HeadState.QUARANTINED) {
                return CompletableFuture.completedFuture(
                        AuthorityScan.blocked(head, TombstoneRetirementStatus.QUARANTINED));
            }
            return handlesPresent(root, deadline).thenCompose(handles -> {
                if (handles) {
                    return CompletableFuture.completedFuture(
                            AuthorityScan.blocked(
                                    head, TombstoneRetirementStatus.HANDLE_PRESENT));
                }
                GcReferenceQuery query = ownerlessQuery(root);
                return referenceDomains.snapshotForDeletion(query, deadline)
                        .thenApply(collection -> {
                            boolean owner = collection.snapshots().stream()
                                    .anyMatch(snapshot -> snapshot.referenceCount() > 0);
                            if (owner) {
                                return AuthorityScan.blocked(
                                        head,
                                        TombstoneRetirementStatus.OWNER_PRESENT,
                                        collection);
                            }
                            if (!collection.clear()) {
                                return AuthorityScan.blocked(
                                        head,
                                        TombstoneRetirementStatus.DOMAIN_VETO,
                                        collection);
                            }
                            if (head.state() == HeadState.EXACT_PRESENT) {
                                return AuthorityScan.blocked(
                                        head,
                                        TombstoneRetirementStatus.OBJECT_PRESENT,
                                        collection);
                            }
                            return AuthorityScan.clear(head, collection);
                        });
            });
        });
    }

    private CompletableFuture<Boolean> handlesPresent(
            VersionedPhysicalObjectRoot root,
            MaterializationDeadline deadline) {
        ObjectKeyHash object = object(root);
        return deadline.bound(
                        () -> metadataStore.scanReaderLeases(
                                cluster,
                                object,
                                Optional.empty(),
                                config.metadataScanPageSize()),
                        "scan reader handles before tombstone retirement")
                .thenCompose(readers -> {
                    requireReaderPageObject(readers, object);
                    if (!readers.values().isEmpty()) {
                        return CompletableFuture.completedFuture(true);
                    }
                    return deadline.bound(
                                    () -> metadataStore.scanProtections(
                                            cluster,
                                            object,
                                            Optional.empty(),
                                            config.metadataScanPageSize()),
                                    "scan protection handles before tombstone retirement")
                            .thenApply(protections -> {
                                requireProtectionPageObject(protections, object);
                                return !protections.values().isEmpty();
                            });
                });
    }

    private CompletableFuture<HeadResult> head(
            VersionedPhysicalObjectRoot root,
            MaterializationDeadline deadline) {
        PhysicalObjectIdentity expected = PhysicalObjectIdentity.from(root.value());
        CompletableFuture<HeadObjectResult> head;
        try {
            head = deadline.bound(
                    () -> objectStore.headObject(
                            expected.objectKey(),
                            new HeadObjectOptions(deadline.remaining())),
                    "HEAD exact DELETED physical object key");
        } catch (UnsupportedOperationException unsupported) {
            return CompletableFuture.completedFuture(HeadResult.quarantined());
        }
        return head.handle((actual, failure) -> {
            if (failure == null) {
                return exactHead(expected, actual)
                        ? HeadResult.exact(actual)
                        : HeadResult.quarantined();
            }
            Throwable exact = unwrap(failure);
            if (exact instanceof NereusException nereus
                    && nereus.code() == ErrorCode.OBJECT_NOT_FOUND) {
                return HeadResult.absent();
            }
            if (exact instanceof UnsupportedOperationException) {
                return HeadResult.quarantined();
            }
            throw propagate(exact);
        });
    }

    private CompletableFuture<AuditSnapshot> captureAudits(
            VersionedPhysicalObjectRoot root,
            MaterializationDeadline deadline) {
        Optional<ObjectId> objectId = PhysicalObjectIdentity.from(root.value()).objectId();
        if (objectId.isEmpty()) {
            return CompletableFuture.completedFuture(AuditSnapshot.empty());
        }
        ObjectId id = objectId.orElseThrow();
        return deadline.bound(
                        () -> auditStore.getReferences(cluster, id),
                        "load exact object-reference audit before retirement")
                .thenCompose(references -> deadline.bound(
                                () -> auditStore.getManifest(cluster, id),
                                "load exact object-manifest audit before retirement")
                        .thenApply(manifest -> {
                            validateAudits(root, references, manifest);
                            return new AuditSnapshot(id, references, manifest);
                        }));
    }

    private CompletableFuture<Boolean> deleteReferences(
            VersionedPhysicalObjectRoot root,
            Optional<VersionedObjectReferencesAudit> references,
            MaterializationDeadline deadline) {
        if (references.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }
        VersionedObjectReferencesAudit expected = references.orElseThrow();
        CompletableFuture<Void> deletion = deadline.bound(
                () -> auditStore.deleteReferences(
                        cluster,
                        new ObjectId(expected.value().objectId()),
                        expected.metadataVersion(),
                        expected.durableValueSha256()),
                "conditionally delete object-reference audit");
        return acceptAuditDeleteResponseLoss(
                root,
                expected,
                deletion,
                () -> auditStore.getReferences(
                        cluster, new ObjectId(expected.value().objectId())),
                deadline);
    }

    private CompletableFuture<Boolean> deleteManifest(
            VersionedPhysicalObjectRoot root,
            Optional<VersionedObjectManifestAudit> manifest,
            MaterializationDeadline deadline) {
        if (manifest.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }
        VersionedObjectManifestAudit expected = manifest.orElseThrow();
        CompletableFuture<Void> deletion = deadline.bound(
                () -> auditStore.deleteManifest(
                        cluster,
                        new ObjectId(expected.value().objectId()),
                        expected.metadataVersion(),
                        expected.durableValueSha256()),
                "conditionally delete object-manifest audit");
        return acceptAuditDeleteResponseLoss(
                root,
                expected,
                deletion,
                () -> auditStore.getManifest(
                        cluster, new ObjectId(expected.value().objectId())),
                deadline);
    }

    private <T> CompletableFuture<Boolean> acceptAuditDeleteResponseLoss(
            VersionedPhysicalObjectRoot root,
            T expected,
            CompletableFuture<Void> deletion,
            java.util.function.Supplier<CompletableFuture<Optional<T>>> reload,
            MaterializationDeadline deadline) {
        return deletion.handle((ignored, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(true);
            }
            Throwable original = unwrap(failure);
            return reloadExact(root, deadline).thenCompose(exact -> {
                if (exact.isEmpty()) {
                    return CompletableFuture.failedFuture(invariant(
                            "DELETED root changed after uncertain audit deletion"));
                }
                return deadline.bound(reload, "reload audit after uncertain conditional delete")
                        .thenCompose(current -> {
                            if (current.isEmpty()) {
                                return CompletableFuture.completedFuture(true);
                            }
                            if (current.orElseThrow().equals(expected)) {
                                return CompletableFuture.failedFuture(original);
                            }
                            return CompletableFuture.failedFuture(invariant(
                                    "object audit changed after uncertain conditional delete"));
                        });
            });
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<Void> requireAuditsAbsent(
            VersionedPhysicalObjectRoot root,
            MaterializationDeadline deadline) {
        Optional<ObjectId> objectId = PhysicalObjectIdentity.from(root.value()).objectId();
        if (objectId.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        ObjectId id = objectId.orElseThrow();
        return deadline.bound(
                        () -> auditStore.getReferences(cluster, id),
                        "verify object-reference audit absence")
                .thenCompose(references -> {
                    if (references.isPresent()) {
                        return CompletableFuture.failedFuture(invariant(
                                "object-reference audit remains after retirement"));
                    }
                    return deadline.bound(
                                    () -> auditStore.getManifest(cluster, id),
                                    "verify object-manifest audit absence")
                            .thenAccept(manifest -> {
                                if (manifest.isPresent()) {
                                    throw invariant(
                                            "object-manifest audit remains after retirement");
                                }
                            });
                });
    }

    private CompletableFuture<TombstoneRetirementResult> deleteRoot(
            VersionedPhysicalObjectRoot root,
            boolean referencesRetired,
            boolean manifestRetired,
            MaterializationDeadline deadline) {
        ObjectKeyHash object = object(root);
        CompletableFuture<Void> deletion = deadline.bound(
                () -> metadataStore.deleteRoot(
                        cluster,
                        object,
                        root.metadataVersion(),
                        root.durableValueSha256()),
                "conditionally retire DELETED physical root as final metadata action");
        return deletion.handle((ignored, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(result(
                        root,
                        TombstoneRetirementStatus.RETIRED,
                        referencesRetired,
                        manifestRetired,
                        true));
            }
            Throwable original = unwrap(failure);
            return deadline.bound(
                            () -> metadataStore.getRoot(cluster, object),
                            "reload physical root after uncertain final retirement")
                    .thenCompose(current -> {
                        if (current.isEmpty()) {
                            return CompletableFuture.completedFuture(result(
                                    root,
                                    TombstoneRetirementStatus.RETIRED,
                                    referencesRetired,
                                    manifestRetired,
                                    true));
                        }
                        if (current.orElseThrow().equals(root)) {
                            return CompletableFuture.failedFuture(original);
                        }
                        return CompletableFuture.completedFuture(result(
                                current.orElseThrow(),
                                TombstoneRetirementStatus.VERSION_CHANGED,
                                referencesRetired,
                                manifestRetired,
                                false));
                    });
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<Optional<VersionedPhysicalObjectRoot>> replaceObservation(
            VersionedPhysicalObjectRoot root,
            long firstAbsentAtMillis,
            Checksum proof,
            MaterializationDeadline deadline) {
        PhysicalObjectRootRecord replacement = observation(
                root.value(), firstAbsentAtMillis, proof.value());
        return tombstoneAuditCas(root, replacement, deadline);
    }

    private CompletableFuture<Optional<VersionedPhysicalObjectRoot>> clearObservation(
            VersionedPhysicalObjectRoot root,
            MaterializationDeadline deadline) {
        if (root.value().tombstoneFirstAbsentAtMillis() == 0) {
            return reloadExact(root, deadline);
        }
        return tombstoneAuditCas(root, observation(root.value(), 0, ""), deadline);
    }

    private CompletableFuture<Optional<VersionedPhysicalObjectRoot>> tombstoneAuditCas(
            VersionedPhysicalObjectRoot root,
            PhysicalObjectRootRecord replacement,
            MaterializationDeadline deadline) {
        CompletableFuture<VersionedPhysicalObjectRoot> cas = deadline.bound(
                () -> metadataStore.compareAndSetRoot(
                        cluster, replacement, root.metadataVersion()),
                "CAS DELETED tombstone audit checkpoint");
        return cas.handle((updated, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(
                        Optional.<VersionedPhysicalObjectRoot>of(updated));
            }
            Throwable original = unwrap(failure);
            return deadline.bound(
                            () -> metadataStore.getRoot(cluster, object(root)),
                            "reload DELETED root after uncertain tombstone audit CAS")
                    .thenCompose(current -> {
                        if (current.isPresent()
                                && exactReplacement(current.orElseThrow(), replacement)) {
                            return CompletableFuture.completedFuture(current);
                        }
                        if (current.isPresent() && current.orElseThrow().equals(root)) {
                            return CompletableFuture
                                    .<Optional<VersionedPhysicalObjectRoot>>failedFuture(
                                            original);
                        }
                        return CompletableFuture.completedFuture(
                                Optional.<VersionedPhysicalObjectRoot>empty());
                    });
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<Optional<VersionedPhysicalObjectRoot>> reloadExact(
            VersionedPhysicalObjectRoot expected,
            MaterializationDeadline deadline) {
        return deadline.bound(
                        () -> metadataStore.getRoot(cluster, object(expected)),
                        "reload exact DELETED physical root")
                .thenApply(current -> current.filter(expected::equals));
    }

    private GcReferenceQuery ownerlessQuery(VersionedPhysicalObjectRoot root) {
        Checksum evidence = TombstoneRetirementDigests.candidateEvidence(root);
        return GcReferenceQuery.create(
                GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE,
                PhysicalObjectIdentity.from(root.value()),
                List.of(),
                evidence);
    }

    private static Checksum proof(
            VersionedPhysicalObjectRoot root,
            GcReferenceCollection collection) {
        return TombstoneRetirementDigests.proof(
                root,
                collection.query().queryIdentitySha256(),
                collection.snapshots());
    }

    private boolean elapsed(
            long baseMillis,
            java.time.Duration grace,
            long now) {
        OptionalLong withGrace = config.deadline(baseMillis, grace);
        if (withGrace.isEmpty()) {
            return false;
        }
        OptionalLong withSkew = config.deadline(
                withGrace.orElseThrow(), config.maximumClockSkew());
        return withSkew.isPresent() && withSkew.orElseThrow() < now;
    }

    private static void validateAudits(
            VersionedPhysicalObjectRoot root,
            Optional<VersionedObjectReferencesAudit> references,
            Optional<VersionedObjectManifestAudit> manifest) {
        PhysicalObjectIdentity object = PhysicalObjectIdentity.from(root.value());
        ObjectId objectId = object.objectId().orElseThrow();
        references.ifPresent(value -> {
            if (!value.value().objectId().equals(objectId.value())) {
                throw invariant("object-reference audit does not match the DELETED root");
            }
        });
        manifest.ifPresent(value -> {
            if (!value.value().objectId().equals(objectId.value())
                    || !value.value().objectKey().equals(object.objectKey().value())
                    || value.value().objectLength() != object.objectLength()
                    || !value.value().storageChecksumType().equals(
                            object.storageChecksum().type().name())
                    || !value.value().storageChecksumValue().equals(
                            object.storageChecksum().value())) {
                throw invariant("object-manifest audit does not match the DELETED root");
            }
        });
    }

    private static boolean exactHead(
            PhysicalObjectIdentity expected,
            HeadObjectResult actual) {
        boolean etagMatches = expected.etag().isEmpty()
                || expected.etag().equals(actual.etag());
        return actual.key().equals(expected.objectKey())
                && actual.objectLength() == expected.objectLength()
                && actual.checksum().equals(expected.storageChecksum())
                && etagMatches;
    }

    private static void requireReaderPageObject(
            ReaderLeaseScanPage page,
            ObjectKeyHash object) {
        if (page.values().stream()
                .anyMatch(value -> !value.value().objectKeyHash().equals(object.value()))) {
            throw invariant("reader-handle scan escaped the DELETED object");
        }
    }

    private static void requireProtectionPageObject(
            ObjectProtectionScanPage page,
            ObjectKeyHash object) {
        if (page.values().stream()
                .anyMatch(value -> !value.value().objectKeyHash().equals(object.value()))) {
            throw invariant("protection-handle scan escaped the DELETED object");
        }
    }

    private static PhysicalObjectRootRecord observation(
            PhysicalObjectRootRecord root,
            long firstAbsentAtMillis,
            String proofSha256) {
        return new PhysicalObjectRootRecord(
                root.schemaVersion(),
                root.objectKeyHash(),
                root.objectKey(),
                root.objectId(),
                root.objectKindId(),
                root.objectLength(),
                root.storageChecksumType(),
                root.storageChecksumValue(),
                root.contentSha256(),
                root.etag(),
                root.lifecycle(),
                root.lifecycleEpoch(),
                root.createdAtMillis(),
                root.orphanNotBeforeMillis(),
                root.gcAttemptId(),
                root.referenceSetSha256(),
                root.markedAtMillis(),
                root.deleteNotBeforeMillis(),
                root.deleteStartedAtMillis(),
                root.deletedAtMillis(),
                firstAbsentAtMillis,
                proofSha256,
                root.stateReason(),
                0);
    }

    private static boolean exactReplacement(
            VersionedPhysicalObjectRoot actual,
            PhysicalObjectRootRecord replacement) {
        return actual.value().withMetadataVersion(0).equals(replacement);
    }

    private static ObjectKeyHash object(VersionedPhysicalObjectRoot root) {
        return new ObjectKeyHash(root.value().objectKeyHash());
    }

    private static TombstoneRetirementResult simple(
            VersionedPhysicalObjectRoot root,
            TombstoneRetirementStatus status) {
        return TombstoneRetirementResult.simple(
                object(root), root.metadataVersion(), status);
    }

    private static TombstoneRetirementResult result(
            VersionedPhysicalObjectRoot root,
            TombstoneRetirementStatus status,
            boolean referencesRetired,
            boolean manifestRetired,
            boolean rootRetired) {
        return new TombstoneRetirementResult(
                object(root),
                root.metadataVersion(),
                status,
                referencesRetired,
                manifestRetired,
                rootRetired);
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
        while ((current instanceof CompletionException
                        || current instanceof ExecutionException)
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

    private enum HeadState {
        ABSENT,
        EXACT_PRESENT,
        QUARANTINED
    }

    private enum LateObjectDeleteOutcome {
        DELETED_OR_ABSENT,
        VERSION_CHANGED,
        QUARANTINED
    }

    private record HeadResult(
            HeadState state,
            Optional<HeadObjectResult> value) {
        private HeadResult {
            Objects.requireNonNull(state, "state");
            value = Objects.requireNonNull(value, "value");
            if ((state == HeadState.EXACT_PRESENT) != value.isPresent()) {
                throw new IllegalArgumentException(
                        "only an exact-present HEAD result carries a value");
            }
        }

        private static HeadResult absent() {
            return new HeadResult(HeadState.ABSENT, Optional.empty());
        }

        private static HeadResult exact(HeadObjectResult value) {
            return new HeadResult(HeadState.EXACT_PRESENT, Optional.of(value));
        }

        private static HeadResult quarantined() {
            return new HeadResult(HeadState.QUARANTINED, Optional.empty());
        }
    }

    private record AuthorityScan(
            HeadResult head,
            Optional<TombstoneRetirementStatus> blockingStatus,
            Optional<GcReferenceCollection> collection) {
        private AuthorityScan {
            Objects.requireNonNull(head, "head");
            blockingStatus = Objects.requireNonNull(blockingStatus, "blockingStatus");
            collection = Objects.requireNonNull(collection, "collection");
            if (blockingStatus.isEmpty() != (head.state() == HeadState.ABSENT
                    && collection.isPresent()
                    && collection.orElseThrow().clear())) {
                throw new IllegalArgumentException("authority scan state is inconsistent");
            }
        }

        private static AuthorityScan clear(
                HeadResult head,
                GcReferenceCollection collection) {
            return new AuthorityScan(
                    head, Optional.empty(), Optional.of(collection));
        }

        private static AuthorityScan blocked(
                HeadResult head,
                TombstoneRetirementStatus status) {
            return new AuthorityScan(
                    head, Optional.of(status), Optional.empty());
        }

        private static AuthorityScan blocked(
                HeadResult head,
                TombstoneRetirementStatus status,
                GcReferenceCollection collection) {
            return new AuthorityScan(
                    head, Optional.of(status), Optional.of(collection));
        }

        private boolean clearAbsent() {
            return blockingStatus.isEmpty();
        }
    }

    private record AuditSnapshot(
            Optional<ObjectId> objectId,
            Optional<VersionedObjectReferencesAudit> references,
            Optional<VersionedObjectManifestAudit> manifest) {
        private AuditSnapshot(
                ObjectId objectId,
                Optional<VersionedObjectReferencesAudit> references,
                Optional<VersionedObjectManifestAudit> manifest) {
            this(Optional.of(objectId), references, manifest);
        }

        private AuditSnapshot {
            objectId = Objects.requireNonNull(objectId, "objectId");
            references = Objects.requireNonNull(references, "references");
            manifest = Objects.requireNonNull(manifest, "manifest");
            if (objectId.isEmpty() && (references.isPresent() || manifest.isPresent())) {
                throw new IllegalArgumentException(
                        "audit records require a physical object id");
            }
        }

        private static AuditSnapshot empty() {
            return new AuditSnapshot(Optional.empty(), Optional.empty(), Optional.empty());
        }
    }
}
