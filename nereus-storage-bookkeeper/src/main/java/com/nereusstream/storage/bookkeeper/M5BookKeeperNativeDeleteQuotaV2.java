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
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * Permanent native epoch/intent capacity, independent of Oxia authority quota and Cell I/O budgets. The first epoch
 * and its complete lifetime reservation commit in one native transaction. There is no refund or implicit expansion.
 * This accounts canonical paths/values, not ZooKeeper replicas, ACL/stat overhead, transaction logs or disk capacity.
 */
public final class M5BookKeeperNativeDeleteQuotaV2 {
    private static final int MAGIC = 0x4d354e51; // M5NQ
    static final int HEAD_BYTES = 104;
    private final ZooKeeper zk;
    private final M5BookKeeperNamespaceGateV2 gate;
    private final List<ACL> acls;
    private final String ledgerReservations;
    private final String path;
    private final Sha256Digest scope;
    private final long resourceCharge;

    M5BookKeeperNativeDeleteQuotaV2(ZooKeeper zk, M5BookKeeperNamespaceGateV2 gate, List<ACL> acls) {
        this.zk = zk;
        this.gate = gate;
        this.acls = List.copyOf(acls);
        String base = gate.path().substring(0, gate.path().lastIndexOf('/'));
        ledgerReservations = base + "/ledgers";
        path = base + "/delete-capacity";
        var resource = new PhysicalResourceIdV2.BookKeeperLedger(gate.namespace(), 0);
        scope = Sha256Digest.hash(resource.canonicalBytes());
        var epoch = new M5BookKeeperNativeDeleteAuthorityV2.Snapshot(resource, new UUID(0, 1), 1, scope, 0);
        var intent = new M5BookKeeperNativeDeleteIntentV2(epoch, scope, scope, scope, 0);
        resourceCharge = (long) utf8(epochPath(0))
                + epoch.encode().length()
                + utf8(epochPath(0) + "-intent")
                + intent.encode().length();
    }

    /** Public counters are loaded from the actual native record; nativeVersion is its exact ZooKeeper stat version. */
    public record Snapshot(
            Sha256Digest scope, long capacityBytes, long reservedResources, long resourceCharge, int nativeVersion) {
        public Snapshot {
            if (scope == null
                    || scope.isZero()
                    || capacityBytes < 1
                    || reservedResources < 0
                    || resourceCharge < 1
                    || nativeVersion < 0
                    || reservedResources > nativeVersion) {
                throw new IllegalArgumentException("invalid native GC quota counters/version");
            }
        }

        public CanonicalBytes encode() {
            var out = ByteBuffer.allocate(HEAD_BYTES)
                    .putInt(MAGIC)
                    .putInt(1)
                    .put(scope.bytes().toByteArray())
                    .putLong(capacityBytes)
                    .putLong(reservedResources)
                    .putLong(resourceCharge)
                    .putInt(nativeVersion)
                    .putInt(0);
            out.put(Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position())))
                    .bytes()
                    .toByteArray());
            return CanonicalBytes.copyOf(out.array());
        }
    }

    public long capacityForResources(long count) {
        if (count < 0) {
            throw new IllegalArgumentException("negative native GC resource capacity");
        }
        return Math.addExact(headCharge(), Math.multiplyExact(count, resourceCharge));
    }

    public long chargedBytes(Snapshot snapshot) {
        verify(snapshot);
        return capacityForResources(snapshot.reservedResources());
    }

    /**
     * Explicit bootstrap before ANY guarded ledger reservation. Creating the reservation parent in the same
     * transaction excludes races and importing previously unaccounted resources. A missing head cannot be repaired
     * after that permanent parent exists. An existing head is read without expansion or resetting its counters.
     */
    public CompletionStage<Snapshot> initialize(long capacityBytes) {
        var initial = new Snapshot(scope, capacityBytes, 0, resourceCharge, 0);
        verify(initial);
        return gate.read().thenCompose(namespace -> readOptional().thenCompose(existing -> {
            if (existing.isPresent()) {
                return requireCapacity(existing.orElseThrow(), capacityBytes);
            }
            return multi(List.of(
                            Op.check(gate.path(), namespace.nativeVersion()),
                            Op.create(ledgerReservations, new byte[0], acls, CreateMode.PERSISTENT),
                            Op.create(path, initial.encode().toByteArray(), acls, CreateMode.PERSISTENT)))
                    .thenCompose(ignored -> snapshot())
                    .thenCompose(actual -> requireCapacity(actual, capacityBytes));
        }));
    }

    public CompletionStage<Snapshot> snapshot() {
        return readOptional()
                .thenApply(value -> value.orElseThrow(() -> new IllegalStateException(
                        "native GC quota is not initialized; existing resources cannot be imported")));
    }

    /** Explicit expansion only. Contention is bounded: a conflicting smaller result fails and can be retried. */
    public CompletionStage<Snapshot> expand(long capacityBytes) {
        return snapshot().thenCompose(previous -> {
            if (capacityBytes < previous.capacityBytes()) {
                throw new IllegalArgumentException("native GC capacity cannot shrink");
            }
            if (capacityBytes == previous.capacityBytes()) {
                return CompletableFuture.completedFuture(previous);
            }
            var next = new Snapshot(
                    scope,
                    capacityBytes,
                    previous.reservedResources(),
                    resourceCharge,
                    Math.addExact(previous.nativeVersion(), 1));
            return gate.read()
                    .thenCompose(namespace -> multi(List.of(
                            Op.check(gate.path(), namespace.nativeVersion()),
                            Op.setData(path, next.encode().toByteArray(), previous.nativeVersion()))))
                    .thenCompose(ignored -> snapshot())
                    .thenCompose(actual -> requireCapacity(actual, capacityBytes));
        });
    }

    /** Internal operations must be committed WITH creation of this resource's first permanent epoch. */
    CompletionStage<List<Op>> firstClaimChecks(boolean required, M5BookKeeperNativeDeleteAuthorityV2.Snapshot epoch) {
        if (!epoch.resource().namespace().equals(gate.namespace()) || epoch.nativeVersion() != 0) {
            throw new IllegalArgumentException("native capacity requires the first exact resource epoch");
        }
        return admitted(required).thenApply(previous -> {
            if (previous.isEmpty()) {
                return List.of(); // Historical unbound fixture profile only; public bound composition requires quota.
            }
            var current = previous.orElseThrow();
            long count = Math.addExact(current.reservedResources(), 1);
            if (capacityForResources(count) > current.capacityBytes()) {
                throw new IllegalStateException("native GC quota exhausted");
            }
            var next = new Snapshot(
                    scope, current.capacityBytes(), count, resourceCharge, Math.addExact(current.nativeVersion(), 1));
            return List.of(Op.setData(path, next.encode().toByteArray(), current.nativeVersion()));
        });
    }

    CompletionStage<Void> requireReserved(boolean required) {
        return admitted(required)
                .thenAccept(value -> value.ifPresent(snapshot -> {
                    if (snapshot.reservedResources() == 0) {
                        throw new IllegalStateException("native epoch lacks a permanent capacity reservation");
                    }
                }));
    }

    String path() {
        return path;
    }

    String epochPath(long ledgerId) {
        if (ledgerId < 0) {
            throw new IllegalArgumentException("negative native ledger ID");
        }
        String shard = Sha256Digest.hash(CanonicalBytes.copyOf(
                        ByteBuffer.allocate(8).putLong(ledgerId).array()))
                .toHex()
                .substring(0, 2);
        return ledgerReservations + "/" + shard + "/" + String.format(java.util.Locale.ROOT, "%020d", ledgerId)
                + "-delete-epoch";
    }

    private CompletionStage<Optional<Snapshot>> admitted(boolean required) {
        return required ? snapshot().thenApply(Optional::of) : readOptional();
    }

    private CompletionStage<Optional<Snapshot>> readOptional() {
        return gate.read().thenCompose(ignored -> {
            var result = new CompletableFuture<Optional<Snapshot>>();
            zk.getData(
                    path,
                    false,
                    (rc, actualPath, context, bytes, stat) -> {
                        try {
                            if (rc == KeeperException.Code.NONODE.intValue()) {
                                result.complete(Optional.empty());
                            } else if (rc != KeeperException.Code.OK.intValue()) {
                                result.completeExceptionally(M5BookKeeperNativeCreateGuardV2.failure(rc, path));
                            } else {
                                if (!path.equals(actualPath) || stat.getEphemeralOwner() != 0) {
                                    throw new IllegalStateException("native GC quota is not the exact permanent head");
                                }
                                var snapshot = decode(CanonicalBytes.copyOf(bytes), stat.getVersion());
                                verify(snapshot);
                                result.complete(Optional.of(snapshot));
                            }
                        } catch (Throwable failure) {
                            result.completeExceptionally(failure);
                        }
                    },
                    null);
            return result;
        });
    }

    private void verify(Snapshot value) {
        if (!value.scope().equals(scope)
                || value.resourceCharge() != resourceCharge
                || capacityForResources(value.reservedResources()) > value.capacityBytes()) {
            throw new IllegalArgumentException("native GC quota scope/charge/capacity differs");
        }
    }

    private CompletionStage<Snapshot> requireCapacity(Snapshot actual, long requested) {
        if (actual.capacityBytes() < requested) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("explicit native GC quota expansion required"));
        }
        return CompletableFuture.completedFuture(actual);
    }

    private CompletionStage<Integer> multi(List<Op> ops) {
        var result = new CompletableFuture<Integer>();
        zk.multi(ops, (rc, path, context, replies) -> result.complete(rc), null);
        return result;
    }

    private long headCharge() {
        return utf8(path) + HEAD_BYTES;
    }

    private static int utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    static Snapshot decode(CanonicalBytes bytes, int nativeVersion) {
        if (bytes.length() != HEAD_BYTES) {
            throw new IllegalArgumentException("native GC quota byte length differs");
        }
        var in = ByteBuffer.wrap(bytes.toByteArray());
        if (in.getInt() != MAGIC || in.getInt() != 1) {
            throw new IllegalArgumentException("native GC quota wire identity differs");
        }
        byte[] scope = new byte[32];
        in.get(scope);
        var result = new Snapshot(Sha256Digest.copyOf(scope), in.getLong(), in.getLong(), in.getLong(), in.getInt());
        if (in.getInt() != 0
                || result.nativeVersion() != nativeVersion
                || !result.encode().equals(bytes)) {
            throw new IllegalArgumentException("native GC quota checksum/version differs");
        }
        return result;
    }
}
