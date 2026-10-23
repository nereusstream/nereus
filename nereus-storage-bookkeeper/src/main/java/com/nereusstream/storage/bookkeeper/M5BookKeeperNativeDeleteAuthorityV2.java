/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.bookkeeper;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdCodecV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.BookKeeperDeleteTargetV1;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.DeleteResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * Permanent native GC epoch for one guarded BK ledger. Every delete atomically checks the epoch and ledger version.
 *
 * <p>This low-level native adapter does not collect eligibility, prove grace or create an M5 intent. Its composition
 * must hold exact current M5 authority and admission before calling delete. Epoch nodes are permanent and are never
 * deleted, including after the ledger disappears. Namespace/task/reservation checks exclude unguarded legacy ledgers.
 */
public final class M5BookKeeperNativeDeleteAuthorityV2 {
    private static final int MAGIC = 0x4d354445; // M5DE
    private static final int MAX_BYTES = 65536 + 128;

    public record Snapshot(
            PhysicalResourceIdV2.BookKeeperLedger resource,
            UUID owner,
            long epoch,
            Sha256Digest capabilitySha256,
            int nativeVersion) {
        public Snapshot {
            Objects.requireNonNull(resource, "resource");
            Objects.requireNonNull(owner, "owner");
            Objects.requireNonNull(capabilitySha256, "capabilitySha256");
            if (capabilitySha256.isZero()
                    || owner.equals(new UUID(0, 0))
                    || nativeVersion < 0
                    || epoch != (long) nativeVersion + 1) {
                throw new IllegalArgumentException("native delete epoch/version/owner/capability differs");
            }
        }

        public CanonicalBytes encode() {
            return bytes(out -> {
                out.writeInt(MAGIC);
                out.writeInt(1);
                byte[] physical = resource.canonicalBytes().toByteArray();
                out.writeInt(physical.length);
                out.write(physical);
                out.writeLong(owner.getMostSignificantBits());
                out.writeLong(owner.getLeastSignificantBits());
                out.writeLong(epoch);
                out.write(capabilitySha256.bytes().toByteArray());
            });
        }
    }

    private final ZooKeeper zk;
    private final M5BookKeeperNativeLedgerManagerV2 manager;
    private final M5BookKeeperNativeCreateGuardV2 guard;
    private final List<ACL> acls;
    private final RunLedgerHandleV1 handle;
    private final Optional<M5PulsarNativeCreateSpecV1> pulsarSpec;
    private final PhysicalResourceIdV2.BookKeeperLedger resource;
    private final BookKeeperCapabilitySnapshotV1 capability;
    private final Sha256Digest capabilitySha;
    private final String path;
    private final Supplier<CompletionStage<Void>> beforeDelete;
    private final M5BookKeeperNativeDeleteQuotaV2 quota;

    M5BookKeeperNativeDeleteAuthorityV2(
            ZooKeeper zk,
            M5BookKeeperNativeLedgerManagerV2 manager,
            M5BookKeeperNativeCreateGuardV2 guard,
            List<ACL> acls,
            RunLedgerHandleV1 handle,
            PhysicalResourceIdV2.Namespace namespace,
            BookKeeperCapabilitySnapshotV1 capability,
            Supplier<CompletionStage<Void>> beforeDelete) {
        this(
                zk,
                manager,
                guard,
                acls,
                handle,
                new PhysicalResourceIdV2.BookKeeperLedger(
                        namespace, handle.ledgerIdentity().ledgerId()),
                Optional.empty(),
                capability,
                beforeDelete);
    }

    M5BookKeeperNativeDeleteAuthorityV2(
            ZooKeeper zk,
            M5BookKeeperNativeLedgerManagerV2 manager,
            M5BookKeeperNativeCreateGuardV2 guard,
            List<ACL> acls,
            M5PulsarNativeCreateSpecV1 spec,
            PhysicalResourceIdV2.BookKeeperLedger resource,
            BookKeeperCapabilitySnapshotV1 capability) {
        this(
                zk,
                manager,
                guard,
                acls,
                null,
                resource,
                Optional.of(spec),
                capability,
                () -> CompletableFuture.completedFuture(null));
        if (spec.bookKeeperGcGrace().isEmpty()
                || !spec.namespaceBinding().physicalNamespace().equals(resource.namespace())
                || !spec.providerScope().equals(capability.providerScopeId().digest())
                || !spec.configuration().equals(capability.configurationDigest())) {
            throw new IllegalArgumentException("Native Pulsar delete scope/capability differs");
        }
    }

    private M5BookKeeperNativeDeleteAuthorityV2(
            ZooKeeper zk,
            M5BookKeeperNativeLedgerManagerV2 manager,
            M5BookKeeperNativeCreateGuardV2 guard,
            List<ACL> acls,
            RunLedgerHandleV1 handle,
            PhysicalResourceIdV2.BookKeeperLedger resource,
            Optional<M5PulsarNativeCreateSpecV1> pulsarSpec,
            BookKeeperCapabilitySnapshotV1 capability,
            Supplier<CompletionStage<Void>> beforeDelete) {
        this.zk = Objects.requireNonNull(zk, "zk");
        this.manager = Objects.requireNonNull(manager, "manager");
        this.guard = Objects.requireNonNull(guard, "guard");
        this.acls = List.copyOf(acls);
        this.handle = handle;
        this.pulsarSpec = pulsarSpec;
        this.resource = Objects.requireNonNull(resource, "resource");
        this.capability = Objects.requireNonNull(capability, "capability");
        this.capabilitySha = capabilitySha256(capability);
        this.beforeDelete = Objects.requireNonNull(beforeDelete, "beforeDelete");
        this.quota = guard.deleteQuota();
        this.path = quota.epochPath(resource.ledgerId());
    }

    /** Transient SDK observation, never a replacement for the native epoch or the durable M5 intent. */
    public record PulsarTarget(
            PhysicalResourceIdV2.BookKeeperLedger resource,
            Sha256Digest createScope,
            long metadataVersion,
            long lastEntryId,
            long length,
            Sha256Digest metadataSha256) {
        public PulsarTarget {
            Objects.requireNonNull(resource, "resource");
            Objects.requireNonNull(createScope, "createScope");
            Objects.requireNonNull(metadataSha256, "metadataSha256");
            if (createScope.isZero()
                    || metadataSha256.isZero()
                    || metadataVersion < 0
                    || metadataVersion > Integer.MAX_VALUE
                    || lastEntryId < 0
                    || length <= 0) {
                throw new IllegalArgumentException("Native Pulsar sealed delete target is invalid");
            }
        }
    }

    static PulsarTarget pulsarTarget(
            PhysicalResourceIdV2.BookKeeperLedger resource,
            M5PulsarNativeCreateSpecV1 spec,
            org.apache.bookkeeper.versioning.Versioned<org.apache.bookkeeper.client.api.LedgerMetadata> observed) {
        var metadata = observed.getValue();
        M5PulsarNativeCreateSpecV1.requireBoundedMetadata(metadata);
        if (spec.bookKeeperGcGrace().isEmpty()
                || !resource.namespace().equals(spec.namespaceBinding().physicalNamespace())
                || !(observed.getVersion() instanceof LongVersion version)
                || !metadata.isClosed()
                || metadata.getCustomMetadata().containsKey("pulsar/cursor")
                || spec.bindingFor(metadata, true).isEmpty()) {
            throw new IllegalArgumentException("Native Pulsar target is not its exact tagged CLOSED data ledger");
        }
        try {
            // The SDK serializer preserves custom-row iteration order. Sort it so a reconnect's map order
            // cannot change the fingerprint of the same sealed native metadata.
            var canonical = org.apache.bookkeeper.client.LedgerMetadataBuilder.from(metadata)
                    .withCToken(metadata.getCToken())
                    .withCustomMetadata(new java.util.TreeMap<>(metadata.getCustomMetadata()))
                    .build();
            var bytes = new org.apache.bookkeeper.meta.LedgerMetadataSerDe().serialize(canonical);
            // BK's serializer can omit ctoken and ctime. They still distinguish the original physical ledger.
            var identity = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(identity)) {
                out.writeInt(0x4d355054); // M5PT
                out.write(spec.sha256().bytes().toByteArray());
                out.writeLong(resource.ledgerId());
                out.writeLong(metadata.getCToken());
                out.writeLong(metadata.getCtime());
                out.writeBoolean(org.apache.bookkeeper.client.LedgerMetadataUtils.shouldStoreCtime(metadata));
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            return new PulsarTarget(
                    resource,
                    spec.sha256(),
                    version.getLongVersion(),
                    metadata.getLastEntryId(),
                    metadata.getLength(),
                    Sha256Digest.hash(CanonicalBytes.copyOf(identity.toByteArray())));
        } catch (java.io.IOException failure) {
            throw new java.util.concurrent.CompletionException(failure);
        }
    }

    /** Actual reads through the original tagged scope. Only native NoSuchLedger establishes absence. */
    public CompletionStage<Optional<PulsarTarget>> capturePulsarTarget() {
        if (pulsarSpec.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalStateException("not a Native Pulsar delete scope"));
        }
        return guard.requireOwned(resource.ledgerId())
                .thenCompose(ignored -> manager.readLedgerMetadata(resource.ledgerId())
                        .handle((observed, failure) -> {
                            if (failure == null) {
                                return Optional.of(pulsarTarget(resource, pulsarSpec.orElseThrow(), observed));
                            }
                            if (absent(failure)) {
                                return Optional.<PulsarTarget>empty();
                            }
                            throw new java.util.concurrent.CompletionException(failure);
                        }));
    }

    public PhysicalResourceIdV2.BookKeeperLedger resource() {
        return resource;
    }

    /** Identity of the original Native create scope; this observation grants no deletion eligibility. */
    public Optional<Sha256Digest> pulsarCreateScopeSha256() {
        return pulsarSpec.map(M5PulsarNativeCreateSpecV1::sha256);
    }

    public Sha256Digest admittedCapabilitySha256() {
        return capabilitySha;
    }

    /** Reads actual durable native data; absence is never interpreted as ownership or recreated implicitly. */
    public CompletionStage<Optional<Snapshot>> read() {
        var result = new CompletableFuture<Optional<Snapshot>>();
        zk.getData(
                path,
                false,
                (rc, actualPath, ignored, data, stat) -> {
                    try {
                        if (rc == KeeperException.Code.NONODE.intValue()) {
                            result.complete(Optional.empty());
                        } else if (rc != KeeperException.Code.OK.intValue()) {
                            result.completeExceptionally(M5BookKeeperNativeCreateGuardV2.failure(rc, path));
                        } else {
                            if (!path.equals(actualPath) || stat.getEphemeralOwner() != 0) {
                                throw new IllegalStateException(
                                        "native delete epoch is not the exact permanent record");
                            }
                            Snapshot value = decode(CanonicalBytes.copyOf(data), stat.getVersion());
                            if (!value.resource().equals(resource)) {
                                throw new IllegalStateException("native delete epoch belongs to another resource");
                            }
                            result.complete(Optional.of(value));
                        }
                    } catch (Throwable failure) {
                        result.completeExceptionally(failure);
                    }
                },
                null);
        return result;
    }

    /** Exact next native epoch revokes all prior native dispatches, including a paused pre-dispatch callback. */
    public CompletionStage<Snapshot> claim(Optional<Snapshot> previous, UUID owner) {
        Objects.requireNonNull(previous, "previous");
        previous.ifPresent(value -> {
            if (!resource.equals(value.resource())) {
                throw new IllegalArgumentException("native predecessor belongs to another resource");
            }
        });
        int version =
                previous.map(value -> Math.addExact(value.nativeVersion(), 1)).orElse(0);
        Snapshot candidate = new Snapshot(resource, owner, (long) version + 1, capabilitySha, version);
        CompletionStage<Snapshot> operation = guard.requireOwned(resource.ledgerId())
                .thenCompose(ignored -> read())
                .thenCompose(observed -> {
                    if (observed.equals(Optional.of(candidate))) {
                        return requireCurrent(candidate).thenApply(ignored -> candidate);
                    }
                    if (!observed.equals(previous)) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("native delete owner CAS conflict"));
                    }
                    CompletionStage<List<Op>> capacity = previous.isEmpty()
                            ? quota.firstClaimChecks(guard.requiresDeleteQuota(), candidate)
                            : quota.requireReserved(guard.requiresDeleteQuota()).thenApply(ignored -> List.of());
                    return capacity.thenCompose(reservation -> {
                        var ops = new ArrayList<>(guard.deleteChecks(resource.ledgerId()));
                        ops.addAll(reservation);
                        byte[] encoded = candidate.encode().toByteArray();
                        ops.add(
                                previous.isPresent()
                                        ? Op.setData(
                                                path,
                                                encoded,
                                                previous.orElseThrow().nativeVersion())
                                        : Op.create(path, encoded, acls, CreateMode.PERSISTENT));
                        return multi(ops)
                                .handle((rc, failure) -> null)
                                .thenCompose(ignored -> read())
                                .thenApply(stored -> {
                                    if (!stored.equals(Optional.of(candidate))) {
                                        throw new IllegalStateException(
                                                "native delete owner mutation is not exactly reconciled");
                                    }
                                    return candidate;
                                });
                    });
                });
        return operation
                .thenCompose(value -> requireCurrent(value).thenApply(ignored -> value))
                .thenApply(value -> value);
    }

    /** Native checks occur in ZooKeeper, not just in the local callback that precedes dispatch. */
    public CompletionStage<Void> requireCurrent(Snapshot expected) {
        if (!resource.equals(expected.resource()) || !capabilitySha.equals(expected.capabilitySha256())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("native delete capability/resource differs"));
        }
        return quota.requireReserved(guard.requiresDeleteQuota())
                .thenCompose(ignored -> guard.requireOwned(resource.ledgerId()))
                .thenCompose(ignored -> read())
                .thenCompose(observed -> {
                    if (!observed.equals(Optional.of(expected))) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("native delete owner is fenced"));
                    }
                    return multi(checks(expected)).thenApply(rc -> {
                        requireOk(rc);
                        return null;
                    });
                });
    }

    /** Actual permanent native intent bytes; no caller-supplied checkpoint can reconstruct missing authority. */
    public CompletionStage<Optional<M5BookKeeperNativeDeleteIntentV2>> readIntent() {
        var result = new CompletableFuture<Optional<M5BookKeeperNativeDeleteIntentV2>>();
        zk.getData(
                intentPath(),
                false,
                (rc, actualPath, ignored, data, stat) -> {
                    try {
                        if (rc == KeeperException.Code.NONODE.intValue()) {
                            result.complete(Optional.empty());
                        } else if (rc != KeeperException.Code.OK.intValue()) {
                            result.completeExceptionally(M5BookKeeperNativeCreateGuardV2.failure(rc, intentPath()));
                        } else {
                            if (!intentPath().equals(actualPath) || stat.getEphemeralOwner() != 0) {
                                throw new IllegalStateException(
                                        "native delete intent is not the exact permanent record");
                            }
                            var value = M5BookKeeperNativeDeleteIntentV2.decode(
                                    CanonicalBytes.copyOf(data), stat.getVersion());
                            if (!resource.equals(value.epoch().resource())) {
                                throw new IllegalStateException("native delete intent belongs to another resource");
                            }
                            result.complete(Optional.of(value));
                        }
                    } catch (Throwable failure) {
                        result.completeExceptionally(failure);
                    }
                },
                null);
        return result;
    }

    /** The caller must reread and verify the full actual M5 intent before and after this low-level binding. */
    public CompletionStage<M5BookKeeperNativeDeleteIntentV2> bindIntent(
            Snapshot expected, Sha256Digest dispatchToken, Sha256Digest intentAuthority, Sha256Digest ledgerMetadata) {
        CompletionStage<M5BookKeeperNativeDeleteIntentV2> operation = requireCurrent(expected)
                .thenCompose(ignored -> readIntent())
                .thenCompose(previous -> {
                    int version = previous.map(value -> value.epoch().equals(expected)
                                    ? value.nativeVersion()
                                    : Math.addExact(value.nativeVersion(), 1))
                            .orElse(0);
                    var candidate = new M5BookKeeperNativeDeleteIntentV2(
                            expected, dispatchToken, intentAuthority, ledgerMetadata, version);
                    if (previous.equals(Optional.of(candidate))) {
                        return requireIntent(candidate).thenApply(ignored -> candidate);
                    }
                    if (previous.isPresent() && previous.orElseThrow().epoch().epoch() >= expected.epoch()) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("changed native intent requires a newer GC epoch"));
                    }
                    var ops = new ArrayList<>(checks(expected));
                    ops.add(
                            previous.isPresent()
                                    ? Op.setData(
                                            intentPath(),
                                            candidate.encode().toByteArray(),
                                            previous.orElseThrow().nativeVersion())
                                    : Op.create(
                                            intentPath(),
                                            candidate.encode().toByteArray(),
                                            acls,
                                            CreateMode.PERSISTENT));
                    return multi(ops)
                            .handle((rc, failure) -> null)
                            .thenCompose(ignored -> readIntent())
                            .thenApply(stored -> {
                                if (!stored.equals(Optional.of(candidate))) {
                                    throw new IllegalStateException(
                                            "native delete intent mutation is not exactly reconciled");
                                }
                                return candidate;
                            });
                });
        return operation
                .thenCompose(value -> requireIntent(value).thenApply(ignored -> value))
                .thenApply(value -> value);
    }

    public CompletionStage<Void> requireIntent(M5BookKeeperNativeDeleteIntentV2 expected) {
        Objects.requireNonNull(expected, "expected");
        return requireCurrent(expected.epoch())
                .thenCompose(ignored -> readIntent())
                .thenCompose(stored -> {
                    if (!stored.equals(Optional.of(expected))) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("native delete intent is fenced"));
                    }
                    var ops = new ArrayList<>(checks(expected.epoch()));
                    ops.add(Op.check(intentPath(), expected.nativeVersion()));
                    return multi(ops).thenApply(rc -> {
                        requireOk(rc);
                        return null;
                    });
                });
    }

    private String intentPath() {
        return path + "-intent";
    }

    /**
     * Caller must first prove the exact M5 intent/eligibility/admission. Identity is re-read and its native version
     * participates in the same server transaction as owner/capability, namespace and closed-create fences.
     */
    CompletionStage<DeleteResult> deleteExact(Snapshot expected, BookKeeperDeleteTargetV1 target) {
        return deleteExact(expected, Optional.empty(), target);
    }

    /** Dispatch using an exact durable native intent binding; admission must still be supplied by the M5 owner. */
    public CompletionStage<DeleteResult> deleteExact(
            M5BookKeeperNativeDeleteIntentV2 intent, BookKeeperDeleteTargetV1 target) {
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(target, "target");
        if (!intent.ledgerMetadataSha256().equals(target.metadataSha256())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("native intent ledger identity differs"));
        }
        if (guard.requiresDeleteQuota()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("bound native delete requires Cell budget"));
        }
        return requireIntent(intent)
                .thenCompose(ignored -> deleteExact(intent.epoch(), Optional.of(intent), target))
                .thenCompose(result -> requireIntent(intent).thenApply(ignored -> result))
                .thenApply(result -> result);
    }

    /** Bound native operation: reserves Cell dispatch and potential-unknown capacity before any delete. */
    public CompletionStage<M5BookKeeperDeleteCellBudgetV2.Result> deleteExact(
            M5BookKeeperDeleteCellBudgetV2 budget,
            M5BookKeeperNativeDeleteIntentV2 intent,
            BookKeeperDeleteTargetV1 target) {
        if (!intent.ledgerMetadataSha256().equals(target.metadataSha256())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("native intent ledger identity differs"));
        }
        var issued = new java.util.concurrent.atomic.AtomicBoolean();
        return guard.requireNamespaceBinding()
                .thenCompose(binding -> requireIntent(intent).thenCompose(ignored -> {
                    var checks = new ArrayList<>(checks(intent.epoch()));
                    checks.add(Op.check(intentPath(), intent.nativeVersion()));
                    return budget.execute(
                            binding, capability.providerScopeId(), intent, checks, issued, () -> deleteExact(
                                            intent.epoch(), Optional.of(intent), target, issued)
                                    .thenCompose(result -> requireIntent(intent).thenApply(unused -> result)));
                }))
                .thenApply(value -> value);
    }

    /** Same native epoch, intent and Cell dispatch transaction for an actual Pulsar tagged data ledger. */
    public CompletionStage<M5BookKeeperDeleteCellBudgetV2.Result> deletePulsarExact(
            M5BookKeeperDeleteCellBudgetV2 budget, M5BookKeeperNativeDeleteIntentV2 intent, PulsarTarget target) {
        if (pulsarSpec.isEmpty()
                || target == null
                || intent == null
                || !resource.equals(target.resource())
                || !pulsarSpec.orElseThrow().sha256().equals(target.createScope())
                || !intent.ledgerMetadataSha256().equals(target.metadataSha256())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Native Pulsar intent/target differs"));
        }
        var issued = new java.util.concurrent.atomic.AtomicBoolean();
        return guard.requireNamespaceBinding()
                .thenCompose(binding -> requireIntent(intent).thenCompose(ignored -> {
                    var admissionChecks = new ArrayList<>(checks(intent.epoch()));
                    admissionChecks.add(Op.check(intentPath(), intent.nativeVersion()));
                    return budget.execute(
                            binding,
                            capability.providerScopeId(),
                            intent,
                            admissionChecks,
                            issued,
                            () -> capturePulsarTarget()
                                    .thenCompose(observed -> {
                                        if (observed.isEmpty()) {
                                            return CompletableFuture.completedFuture(
                                                    DeleteResult.authoritativelyAbsent());
                                        }
                                        if (!observed.equals(Optional.of(target))) {
                                            return CompletableFuture.completedFuture(
                                                    DeleteResult.differentLedgerOrMetadata());
                                        }
                                        var deletionChecks = new ArrayList<>(admissionChecks);
                                        deletionChecks.add(Op.delete(
                                                manager.nativeLedgerPath(resource.ledgerId()),
                                                Math.toIntExact(target.metadataVersion())));
                                        return beforeDelete
                                                .get()
                                                .thenCompose(unused -> {
                                                    issued.set(true);
                                                    return multi(deletionChecks);
                                                })
                                                .thenCompose(rc -> {
                                                    if (rc == KeeperException.Code.BADVERSION.intValue()
                                                            || rc == KeeperException.Code.NOAUTH.intValue()) {
                                                        return CompletableFuture.failedFuture(
                                                                M5BookKeeperNativeCreateGuardV2.failure(rc, path));
                                                    }
                                                    return capturePulsarTarget().handle((remaining, failure) -> {
                                                        if (failure != null) {
                                                            return DeleteResult.outcomeUnknown();
                                                        }
                                                        if (remaining.isEmpty()) {
                                                            return DeleteResult.authoritativelyAbsent();
                                                        }
                                                        if (!remaining.equals(Optional.of(target))) {
                                                            return DeleteResult.differentLedgerOrMetadata();
                                                        }
                                                        return rc == KeeperException.Code.OK.intValue()
                                                                ? DeleteResult.exactLedgerRemains()
                                                                : DeleteResult.outcomeUnknown();
                                                    });
                                                });
                                    })
                                    .thenCompose(result -> requireIntent(intent).thenApply(unused -> result)));
                }))
                .thenApply(value -> value);
    }

    /** Read-only recovery of callback-terminal UNKNOWN; in-flight reservations are never cleared by timeout/restart. */
    public CompletionStage<M5BookKeeperDeleteCellBudgetV2.Result> reconcileCellDelete(
            M5BookKeeperDeleteCellBudgetV2 budget,
            M5BookKeeperNativeDeleteIntentV2 intent,
            BookKeeperDeleteTargetV1 target) {
        if (!intent.ledgerMetadataSha256().equals(target.metadataSha256())
                || !Objects.equals(handle, target.handle())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("native intent ledger identity differs"));
        }
        return guard.requireNamespaceBinding()
                .thenCompose(binding -> requireIntent(intent)
                        .thenCompose(ignored -> budget.reconcileUnknown(
                                binding, capability.providerScopeId(), intent, () -> reconcile(target, false)
                                        .thenCompose(
                                                result -> requireIntent(intent).thenApply(unused -> result)))))
                .thenApply(value -> value);
    }

    /**
     * Read-only reconciliation of callback-terminal UNKNOWN from permanent native intent and actual ledger metadata.
     * A fresh process need not reconstruct the vanished full target from a local checkpoint. Presence or an
     * uncertain read retains the Cell reservation; absence releases only Cell capacity, not M5 DONE authority.
     */
    public CompletionStage<M5BookKeeperDeleteCellBudgetV2.Result> reconcileCellDeleteAbsence(
            M5BookKeeperDeleteCellBudgetV2 budget, M5BookKeeperNativeDeleteIntentV2 intent) {
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(intent, "intent");
        if (!resource.equals(intent.epoch().resource())
                || !capabilitySha.equals(intent.epoch().capabilitySha256())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "native Cell recovery intent belongs to another resource or capability"));
        }
        return guard.requireNamespaceBinding()
                .thenCompose(binding -> requireIntent(intent)
                        .thenCompose(ignored -> budget.reconcileUnknown(
                                binding, capability.providerScopeId(), intent, () -> manager.readLedgerMetadata(
                                                resource.ledgerId())
                                        .handle((observed, failure) -> {
                                            if (failure != null) {
                                                return absent(failure)
                                                        ? DeleteResult.authoritativelyAbsent()
                                                        : DeleteResult.outcomeUnknown();
                                            }
                                            if (pulsarSpec.isPresent()) {
                                                var actual = pulsarTarget(resource, pulsarSpec.orElseThrow(), observed);
                                                return actual.metadataSha256().equals(intent.ledgerMetadataSha256())
                                                        ? DeleteResult.exactLedgerRemains()
                                                        : DeleteResult.differentLedgerOrMetadata();
                                            }
                                            var actual = M5BookKeeperDeleteAdapterV1.exactTarget(
                                                    observed.getValue(), handle, capability, new byte[0]);
                                            return actual.map(target -> target.metadataSha256()
                                                                    .equals(intent.ledgerMetadataSha256())
                                                            ? DeleteResult.exactLedgerRemains()
                                                            : DeleteResult.differentLedgerOrMetadata())
                                                    .orElseGet(DeleteResult::differentLedgerOrMetadata);
                                        })
                                        .thenCompose(
                                                result -> requireIntent(intent).thenApply(unused -> result)))))
                .thenApply(value -> value);
    }

    /**
     * Recovers only native Cell capacity after a newer permanent epoch has fenced the old delete transaction.
     * The caller must separately requalify M5 eligibility and reconcile actual ledger presence/absence; this
     * method neither dispatches deletion nor records a physical terminal.
     */
    public CompletionStage<Boolean> reconcileFencedCellDelete(
            M5BookKeeperDeleteCellBudgetV2 budget, M5BookKeeperNativeDeleteIntentV2 oldIntent) {
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(oldIntent, "oldIntent");
        if (!resource.equals(oldIntent.epoch().resource())
                || !capabilitySha.equals(oldIntent.epoch().capabilitySha256())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "native Cell recovery intent belongs to another resource or capability"));
        }
        return guard.requireNamespaceBinding()
                .thenCompose(binding -> budget.reconcileFenced(
                        binding, capability.providerScopeId(), oldIntent, () -> read().thenCompose(observed -> {
                            var successor = observed.orElseThrow(
                                    () -> new IllegalStateException("native delete epoch is absent"));
                            if (successor.nativeVersion() <= oldIntent.epoch().nativeVersion()) {
                                return CompletableFuture.failedFuture(
                                        new IllegalStateException("native delete predecessor epoch is not fenced"));
                            }
                            return requireCurrent(successor);
                        })))
                .thenApply(value -> value);
    }

    private CompletionStage<DeleteResult> deleteExact(
            Snapshot expected, Optional<M5BookKeeperNativeDeleteIntentV2> intent, BookKeeperDeleteTargetV1 target) {
        return deleteExact(expected, intent, target, new java.util.concurrent.atomic.AtomicBoolean());
    }

    private CompletionStage<DeleteResult> deleteExact(
            Snapshot expected,
            Optional<M5BookKeeperNativeDeleteIntentV2> intent,
            BookKeeperDeleteTargetV1 target,
            java.util.concurrent.atomic.AtomicBoolean issued) {
        Objects.requireNonNull(target, "target");
        if (!Objects.equals(handle, target.handle())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("native delete target handle differs"));
        }
        CompletionStage<DeleteResult> operation = requireCurrent(expected)
                .thenCompose(ignored -> manager.readLedgerMetadata(resource.ledgerId())
                        .handle((value, failure) -> {
                            if (failure != null) {
                                if (absent(failure)) {
                                    return Optional
                                            .<org.apache.bookkeeper.versioning.Versioned<
                                                            org.apache.bookkeeper.client.api.LedgerMetadata>>
                                                    empty();
                                }
                                throw new java.util.concurrent.CompletionException(failure);
                            }
                            return Optional.of(Objects.requireNonNull(value, "native ledger metadata"));
                        }))
                .thenCompose(observed -> {
                    if (observed.isEmpty()) {
                        return CompletableFuture.completedFuture(DeleteResult.authoritativelyAbsent());
                    }
                    var nativeValue = observed.orElseThrow();
                    if (!M5BookKeeperDeleteAdapterV1.exactTarget(
                                    nativeValue.getValue(), handle, capability, new byte[0])
                            .equals(Optional.of(target))) {
                        return CompletableFuture.completedFuture(DeleteResult.differentLedgerOrMetadata());
                    }
                    if (!(nativeValue.getVersion() instanceof LongVersion nativeVersion)) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("unknown native ledger version"));
                    }
                    long version = nativeVersion.getLongVersion();
                    if (version < 0 || version > Integer.MAX_VALUE) {
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("native ledger version exceeds bound"));
                    }
                    var ops = new ArrayList<>(checks(expected));
                    intent.ifPresent(value -> ops.add(Op.check(intentPath(), value.nativeVersion())));
                    ops.add(Op.delete(manager.nativeLedgerPath(resource.ledgerId()), (int) version));
                    return Objects.requireNonNull(beforeDelete.get(), "beforeDelete stage")
                            .thenCompose(ignored -> {
                                issued.set(true);
                                return multi(ops);
                            })
                            .thenCompose(rc -> {
                                if (rc == KeeperException.Code.BADVERSION.intValue()
                                        || rc == KeeperException.Code.NOAUTH.intValue()) {
                                    return CompletableFuture.failedFuture(
                                            M5BookKeeperNativeCreateGuardV2.failure(rc, path));
                                }
                                return reconcile(target, rc == KeeperException.Code.OK.intValue());
                            });
                })
                .thenCompose(result -> requireCurrent(expected).thenApply(ignored -> result));
        return operation.thenApply(result -> result);
    }

    private CompletionStage<DeleteResult> reconcile(BookKeeperDeleteTargetV1 expected, boolean confirmedMutation) {
        return manager.readLedgerMetadata(resource.ledgerId()).handle((nativeValue, failure) -> {
            if (failure != null) {
                return absent(failure) ? DeleteResult.authoritativelyAbsent() : DeleteResult.outcomeUnknown();
            }
            var actual =
                    M5BookKeeperDeleteAdapterV1.exactTarget(nativeValue.getValue(), handle, capability, new byte[0]);
            if (!actual.equals(Optional.of(expected))) {
                return DeleteResult.differentLedgerOrMetadata();
            }
            // A lost transaction response plus presence cannot establish that no delayed mutation remains.
            return confirmedMutation ? DeleteResult.exactLedgerRemains() : DeleteResult.outcomeUnknown();
        });
    }

    private List<Op> checks(Snapshot expected) {
        var ops = new ArrayList<>(guard.deleteChecks(resource.ledgerId()));
        ops.add(Op.check(path, expected.nativeVersion()));
        if (guard.requiresDeleteQuota()) {
            ops.add(Op.check(quota.path(), -1));
        }
        return List.copyOf(ops);
    }

    private CompletionStage<Integer> multi(List<Op> ops) {
        var result = new CompletableFuture<Integer>();
        zk.multi(ops, (rc, ignoredPath, ignored, replies) -> result.complete(rc), null);
        return result;
    }

    private void requireOk(int rc) {
        if (rc != KeeperException.Code.OK.intValue()) {
            throw new java.util.concurrent.CompletionException(M5BookKeeperNativeCreateGuardV2.failure(rc, path));
        }
    }

    private static boolean absent(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause instanceof BKException bk
                && (bk.getCode() == BKException.Code.NoSuchLedgerExistsException
                        || bk.getCode() == BKException.Code.NoSuchLedgerExistsOnMetadataServerException);
    }

    static Snapshot decode(CanonicalBytes bytes, int nativeVersion) {
        if (bytes.length() > MAX_BYTES) {
            throw new IllegalArgumentException("native delete epoch exceeds bound");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readInt() != 1) {
                throw new IllegalArgumentException("native delete epoch preamble differs");
            }
            int length = in.readInt();
            if (length <= 0 || length > 65536) {
                throw new IllegalArgumentException("native delete resource exceeds bound");
            }
            var physical = PhysicalResourceIdCodecV2.decode(CanonicalBytes.copyOf(in.readNBytes(length)));
            if (!(physical instanceof PhysicalResourceIdV2.BookKeeperLedger ledger)) {
                throw new IllegalArgumentException("native delete resource is not BK");
            }
            var snapshot = new Snapshot(
                    ledger,
                    new UUID(in.readLong(), in.readLong()),
                    in.readLong(),
                    Sha256Digest.copyOf(in.readNBytes(Sha256Digest.LENGTH)),
                    nativeVersion);
            if (in.available() != 0 || !snapshot.encode().equals(bytes)) {
                throw new IllegalArgumentException("native delete epoch is noncanonical");
            }
            return snapshot;
        } catch (IOException failure) {
            throw new IllegalArgumentException("native delete epoch is truncated", failure);
        }
    }

    private static Sha256Digest capabilitySha256(BookKeeperCapabilitySnapshotV1 cap) {
        return Sha256Digest.hash(bytes(out -> {
            out.writeUTF("NEREUS_M5_BK_NATIVE_DELETE_CAPABILITY_V2");
            out.write(cap.providerScopeId().digest().bytes().toByteArray());
            out.writeUTF(cap.clientSourceCommit());
            out.write(cap.clientArtifactSha256().bytes().toByteArray());
            out.writeUTF(cap.serverSourceCommit());
            out.write(cap.serverImageManifestSha256().bytes().toByteArray());
            out.writeUTF(cap.protocolMode().name());
            out.writeInt(cap.clientFrameLimitBytes());
            out.writeInt(cap.serverFrameLimitBytes());
            out.writeInt(cap.maximumAddPayloadBytes());
            out.writeBoolean(cap.explicitEntryIdsSupported());
            out.writeInt(cap.ensembleSize());
            out.writeInt(cap.writeQuorumSize());
            out.writeInt(cap.ackQuorumSize());
            out.writeUTF(cap.digestType().name());
            out.writeBoolean(cap.fencingSupported());
            out.writeBoolean(cap.recoverySupported());
            out.writeLong(cap.timeoutClass().connectMillis());
            out.writeLong(cap.timeoutClass().addMillis());
            out.writeLong(cap.timeoutClass().readMillis());
            out.writeLong(cap.timeoutClass().recoveryMillis());
            out.writeUTF(cap.credentialIdentityVersion());
            out.write(cap.configurationDigest().bytes().toByteArray());
        }));
    }

    private static CanonicalBytes bytes(Writer writer) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                writer.write(out);
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IllegalArgumentException("native delete epoch exceeds bound");
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("native delete encoding failed", failure);
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream out) throws IOException;
    }
}
