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
    private final PhysicalResourceIdV2.BookKeeperLedger resource;
    private final BookKeeperCapabilitySnapshotV1 capability;
    private final Sha256Digest capabilitySha;
    private final String path;
    private final Supplier<CompletionStage<Void>> beforeDelete;

    M5BookKeeperNativeDeleteAuthorityV2(
            ZooKeeper zk,
            M5BookKeeperNativeLedgerManagerV2 manager,
            M5BookKeeperNativeCreateGuardV2 guard,
            List<ACL> acls,
            RunLedgerHandleV1 handle,
            PhysicalResourceIdV2.Namespace namespace,
            BookKeeperCapabilitySnapshotV1 capability,
            Supplier<CompletionStage<Void>> beforeDelete) {
        this.zk = Objects.requireNonNull(zk, "zk");
        this.manager = Objects.requireNonNull(manager, "manager");
        this.guard = Objects.requireNonNull(guard, "guard");
        this.acls = List.copyOf(acls);
        this.handle = Objects.requireNonNull(handle, "handle");
        this.resource = new PhysicalResourceIdV2.BookKeeperLedger(
                namespace, handle.ledgerIdentity().ledgerId());
        this.capability = Objects.requireNonNull(capability, "capability");
        this.capabilitySha = capabilitySha256(capability);
        this.beforeDelete = Objects.requireNonNull(beforeDelete, "beforeDelete");
        this.path = guard.reservationPath(handle.ledgerIdentity().ledgerId()) + "-delete-epoch";
    }

    public PhysicalResourceIdV2.BookKeeperLedger resource() {
        return resource;
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
                    var ops = new ArrayList<>(guard.deleteChecks(resource.ledgerId()));
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
        return guard.requireOwned(resource.ledgerId())
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

    /**
     * Caller must first prove the exact M5 intent/eligibility/admission. Identity is re-read and its native version
     * participates in the same server transaction as owner/capability, namespace and closed-create fences.
     */
    public CompletionStage<DeleteResult> deleteExact(Snapshot expected, BookKeeperDeleteTargetV1 target) {
        Objects.requireNonNull(target, "target");
        if (!handle.equals(target.handle())) {
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
                    ops.add(Op.delete(manager.nativeLedgerPath(resource.ledgerId()), (int) version));
                    return Objects.requireNonNull(beforeDelete.get(), "beforeDelete stage")
                            .thenCompose(ignored -> multi(ops))
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

    private static Snapshot decode(CanonicalBytes bytes, int nativeVersion) {
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
