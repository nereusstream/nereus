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
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.DeleteOutcome;
import com.nereusstream.storage.bookkeeper.M5BookKeeperDeleteAdapterV1.DeleteResult;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * One bounded permanent native Cell head. Every operation reserves both dispatch and possible-unknown capacity.
 * Observer cancellation cannot release it. In-flight records survive restart; only terminal unknowns admit read-only
 * absence reconciliation. This does not account transport buffers, per-Binding shares, rates or protocol eligibility.
 */
public final class M5BookKeeperDeleteCellBudgetV2 {
    public static final int MAX_RESERVATIONS = 64;
    private static final int MAGIC = 0x4d354342; // M5CB
    private static final int HEADER_BYTES = 120;
    private static final int HOLD_BYTES = 84;
    private static final int MAX_RELEASE_ATTEMPTS = 4;
    private final ZooKeeper zk;
    private final M5BookKeeperNamespaceGateV2 gate;
    private final CellProviderScopeId cell;
    private final Sha256Digest namespace;
    private final List<ACL> acls;
    private final String parent;
    private final String path;

    M5BookKeeperDeleteCellBudgetV2(
            ZooKeeper zk, M5BookKeeperNamespaceGateV2 gate, List<ACL> acls, CellProviderScopeId cell) {
        this.zk = zk;
        this.gate = gate;
        this.acls = List.copyOf(acls);
        this.cell = cell;
        namespace = Sha256Digest.hash(new PhysicalResourceIdV2.BookKeeperLedger(gate.namespace(), 0).canonicalBytes());
        parent = gate.path().substring(0, gate.path().lastIndexOf('/')) + "/delete-cells/"
                + cell.digest().toHex();
        path = parent + "/head";
    }

    public record Limits(int dispatchSlots, int unknownSlots) {
        public Limits {
            if (dispatchSlots < 1
                    || unknownSlots < 1
                    || dispatchSlots > MAX_RESERVATIONS
                    || unknownSlots > MAX_RESERVATIONS) {
                throw new IllegalArgumentException("native Cell delete slot limits are outside bounds");
            }
        }

        int capacity() {
            return Math.min(dispatchSlots, unknownSlots);
        }
    }

    public record Hold(UUID operation, Sha256Digest resource, Sha256Digest nativeIntent, boolean terminalUnknown) {
        public Hold {
            if (operation == null
                    || operation.equals(new UUID(0, 0))
                    || resource == null
                    || resource.isZero()
                    || nativeIntent == null
                    || nativeIntent.isZero()) {
                throw new IllegalArgumentException("invalid native Cell reservation identity");
            }
        }

        Hold unknown() {
            return new Hold(operation, resource, nativeIntent, true);
        }
    }

    public record Snapshot(
            Sha256Digest namespace,
            CellProviderScopeId cell,
            int nativeVersion,
            Limits limits,
            List<Hold> reservations) {
        public Snapshot {
            reservations = List.copyOf(reservations);
            if (namespace == null
                    || namespace.isZero()
                    || cell == null
                    || limits == null
                    || nativeVersion < 0
                    || reservations.size() > limits.capacity()
                    || !reservations.equals(reservations.stream()
                            .sorted(Comparator.comparing(
                                    value -> value.resource().toHex()))
                            .toList())
                    || reservations.stream().map(Hold::resource).distinct().count() != reservations.size()
                    || reservations.stream().map(Hold::operation).distinct().count() != reservations.size()) {
                throw new IllegalArgumentException("native Cell head is invalid or exceeds reserved capacity");
            }
        }

        public CanonicalBytes encode() {
            var out = ByteBuffer.allocate(HEADER_BYTES + HOLD_BYTES * reservations.size());
            out.putInt(MAGIC)
                    .putInt(1)
                    .put(namespace.bytes().toByteArray())
                    .put(cell.digest().bytes().toByteArray())
                    .putInt(nativeVersion)
                    .putInt(limits.dispatchSlots())
                    .putInt(limits.unknownSlots())
                    .putInt(reservations.size());
            for (var hold : reservations) {
                out.putLong(hold.operation().getMostSignificantBits())
                        .putLong(hold.operation().getLeastSignificantBits())
                        .put(hold.resource().bytes().toByteArray())
                        .put(hold.nativeIntent().bytes().toByteArray())
                        .putInt(hold.terminalUnknown() ? 1 : 0);
            }
            out.put(Sha256Digest.hash(CanonicalBytes.copyOf(java.util.Arrays.copyOf(out.array(), out.position())))
                    .bytes()
                    .toByteArray());
            return CanonicalBytes.copyOf(out.array());
        }

        Snapshot replace(List<Hold> holds) {
            return new Snapshot(
                    namespace,
                    cell,
                    Math.addExact(nativeVersion, 1),
                    limits,
                    holds.stream()
                            .sorted(Comparator.comparing(
                                    value -> value.resource().toHex()))
                            .toList());
        }
    }

    public record Result(DeleteResult deleteResult, boolean reservationRetained) {}

    /** Explicit provisioning only. A permanent Cell parent prevents recreating a missing head. Limits are immutable. */
    public CompletionStage<Snapshot> initialize(Limits limits) {
        var initial = new Snapshot(namespace, cell, 0, limits, List.of());
        return binding().thenCompose(ignored -> readOptional().thenCompose(existing -> {
            if (existing.isPresent()) {
                return exactLimits(existing.orElseThrow(), limits);
            }
            var parents = new CompletableFuture<Void>();
            String cells = parent.substring(0, parent.lastIndexOf('/'));
            ZkUtils.asyncCreateFullPathOptimistic(
                    zk,
                    cells,
                    new byte[0],
                    acls,
                    CreateMode.PERSISTENT,
                    (rc, path, context, name) -> {
                        if (rc == 0 || rc == KeeperException.Code.NODEEXISTS.intValue()) {
                            parents.complete(null);
                        } else {
                            parents.completeExceptionally(M5BookKeeperNativeCreateGuardV2.failure(rc, cells));
                        }
                    },
                    null);
            return parents.thenCompose(unused -> multi(List.of(
                            Op.check(gate.path(), 1),
                            Op.create(parent, new byte[0], acls, CreateMode.PERSISTENT),
                            Op.create(path, initial.encode().toByteArray(), acls, CreateMode.PERSISTENT))))
                    .thenCompose(unused -> snapshot())
                    .thenCompose(actual -> exactLimits(actual, limits));
        }));
    }

    public CompletionStage<Snapshot> snapshot() {
        return readOptional()
                .thenApply(value -> value.orElseThrow(() -> new IllegalStateException(
                        "native Cell delete budget head is absent; no automatic recreation")));
    }

    public long reservedCanonicalHeadBytes(Limits limits) {
        return path.getBytes(StandardCharsets.UTF_8).length + HEADER_BYTES + (long) HOLD_BYTES * limits.capacity();
    }

    CompletionStage<Result> execute(
            PhysicalNamespaceAuthorityBindingV2 binding,
            CellProviderScopeId expectedCell,
            M5BookKeeperNativeDeleteIntentV2 intent,
            List<Op> nativeChecks,
            AtomicBoolean deleteIssued,
            Supplier<CompletionStage<DeleteResult>> work) {
        var hold = new Hold(
                UUID.randomUUID(), intent.epoch().resource().sha256(), Sha256Digest.hash(intent.encode()), false);
        CompletionStage<Result> operation = requireScope(binding, expectedCell)
                .thenCompose(ignored -> snapshot())
                .thenCompose(current -> {
                    if (current.reservations().stream()
                            .anyMatch(row -> row.resource().equals(hold.resource()))) {
                        throw new IllegalStateException("native delete already has a Cell reservation");
                    }
                    if (current.reservations().size() == current.limits().capacity()) {
                        throw new IllegalStateException("native Cell dispatch or unknown capacity exhausted");
                    }
                    var rows = new ArrayList<>(current.reservations());
                    rows.add(hold);
                    return mutate(current, current.replace(rows), nativeChecks)
                            .thenCompose(unused -> snapshot())
                            .thenApply(stored -> {
                                if (!stored.reservations().contains(hold)) {
                                    throw new IllegalStateException(
                                            "native Cell reservation was not exactly reconciled");
                                }
                                return hold;
                            });
                })
                .thenCompose(acquired -> {
                    CompletionStage<DeleteResult> dispatched;
                    try {
                        dispatched = java.util.Objects.requireNonNull(work.get(), "native delete stage");
                    } catch (Throwable failure) {
                        dispatched = CompletableFuture.failedFuture(failure);
                    }
                    return dispatched
                            .handle((result, failure) -> new Completion(result, failure))
                            .thenCompose(done -> {
                                if (done.failure() != null) {
                                    return (deleteIssued.get()
                                                    ? update(hold, false, MAX_RELEASE_ATTEMPTS)
                                                    : update(hold, true, MAX_RELEASE_ATTEMPTS))
                                            .thenCompose(
                                                    ignored -> CompletableFuture.<Result>failedFuture(done.failure()));
                                }
                                boolean release = done.result().outcome() != DeleteOutcome.OUTCOME_UNKNOWN;
                                return update(hold, release, MAX_RELEASE_ATTEMPTS)
                                        .thenApply(released -> new Result(done.result(), !release || !released));
                            });
                });
        return operation.thenApply(value -> value);
    }

    /** Only a callback-terminal UNKNOWN can reconcile; an old in-flight reservation never becomes terminal by age. */
    CompletionStage<Result> reconcileUnknown(
            PhysicalNamespaceAuthorityBindingV2 binding,
            CellProviderScopeId expectedCell,
            M5BookKeeperNativeDeleteIntentV2 intent,
            Supplier<CompletionStage<DeleteResult>> observe) {
        return requireScope(binding, expectedCell)
                .thenCompose(ignored -> snapshot())
                .thenCompose(current -> {
                    var hold = current.reservations().stream()
                            .filter(row -> row.resource()
                                    .equals(intent.epoch().resource().sha256()))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException("native Cell reservation is absent"));
                    if (!hold.terminalUnknown() || !hold.nativeIntent().equals(Sha256Digest.hash(intent.encode()))) {
                        throw new IllegalStateException(
                                "native Cell reservation is in flight or belongs to another intent");
                    }
                    return observe.get()
                            .thenCompose(result -> result.outcome() == DeleteOutcome.AUTHORITATIVELY_ABSENT
                                    ? update(hold, true, MAX_RELEASE_ATTEMPTS)
                                            .thenApply(released -> new Result(result, !released))
                                    : CompletableFuture.completedFuture(new Result(result, true)));
                })
                .thenApply(value -> value);
    }

    private CompletionStage<Boolean> update(Hold original, boolean release, int attempts) {
        return snapshot().thenCompose(current -> {
            var observed = current.reservations().stream()
                    .filter(row -> row.operation().equals(original.operation()))
                    .findFirst();
            if (observed.isEmpty()) {
                return CompletableFuture.completedFuture(release);
            }
            var actual = observed.orElseThrow();
            if (!actual.equals(original) && !actual.equals(original.unknown())) {
                throw new IllegalStateException("native Cell operation identity changed");
            }
            if (!release && actual.terminalUnknown()) {
                return CompletableFuture.completedFuture(false);
            }
            var rows = new ArrayList<>(current.reservations());
            rows.remove(actual);
            if (!release) {
                rows.add(actual.unknown());
            }
            return mutate(current, current.replace(rows), List.of())
                    .thenCompose(ignored -> snapshot())
                    .thenCompose(after -> {
                        boolean remains = after.reservations().stream()
                                .anyMatch(row -> row.operation().equals(original.operation()));
                        if (!remains || (!release && after.reservations().contains(original.unknown()))) {
                            return CompletableFuture.completedFuture(!remains);
                        }
                        return attempts > 1
                                ? update(original, release, attempts - 1)
                                : CompletableFuture.completedFuture(false);
                    });
        });
    }

    private CompletionStage<Void> requireScope(
            PhysicalNamespaceAuthorityBindingV2 expected, CellProviderScopeId expectedCell) {
        if (!cell.equals(expectedCell)) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("native delete budget belongs to another Cell"));
        }
        return binding().thenAccept(actual -> {
            if (!actual.equals(expected)) {
                throw new IllegalArgumentException("native delete budget belongs to another namespace binding");
            }
        });
    }

    private CompletionStage<PhysicalNamespaceAuthorityBindingV2> binding() {
        return gate.read().thenApply(value -> value.binding()
                .orElseThrow(() -> new IllegalStateException("native Cell budget requires a bound namespace")));
    }

    private CompletionStage<Optional<Snapshot>> readOptional() {
        return binding().thenCompose(ignored -> {
            var result = new CompletableFuture<Optional<Snapshot>>();
            zk.getData(
                    path,
                    false,
                    (rc, actualPath, context, bytes, stat) -> {
                        try {
                            if (rc == KeeperException.Code.NONODE.intValue()) {
                                result.complete(Optional.empty());
                            } else if (rc != 0) {
                                result.completeExceptionally(M5BookKeeperNativeCreateGuardV2.failure(rc, path));
                            } else {
                                if (!path.equals(actualPath) || stat.getEphemeralOwner() != 0) {
                                    throw new IllegalStateException(
                                            "native Cell head is not the exact permanent record");
                                }
                                var value = decode(CanonicalBytes.copyOf(bytes), stat.getVersion());
                                if (!value.cell().equals(cell)
                                        || !value.namespace().equals(namespace)) {
                                    throw new IllegalStateException("native Cell head scope differs");
                                }
                                result.complete(Optional.of(value));
                            }
                        } catch (Throwable failure) {
                            result.completeExceptionally(failure);
                        }
                    },
                    null);
            return result;
        });
    }

    private CompletionStage<Snapshot> exactLimits(Snapshot actual, Limits expected) {
        return actual.limits().equals(expected)
                ? CompletableFuture.completedFuture(actual)
                : CompletableFuture.failedFuture(new IllegalStateException("native Cell delete limits differ"));
    }

    private CompletionStage<Integer> mutate(Snapshot previous, Snapshot next, List<Op> nativeChecks) {
        var ops = new ArrayList<>(nativeChecks);
        ops.add(Op.check(gate.path(), 1));
        ops.add(Op.setData(path, next.encode().toByteArray(), previous.nativeVersion()));
        return multi(ops);
    }

    private CompletionStage<Integer> multi(List<Op> ops) {
        var result = new CompletableFuture<Integer>();
        zk.multi(ops, (rc, path, context, replies) -> result.complete(rc), null);
        return result;
    }

    static Snapshot decode(CanonicalBytes bytes, int nativeVersion) {
        if (bytes.length() < HEADER_BYTES || bytes.length() > HEADER_BYTES + HOLD_BYTES * MAX_RESERVATIONS) {
            throw new IllegalArgumentException("native Cell head byte length differs");
        }
        var in = ByteBuffer.wrap(bytes.toByteArray());
        if (in.getInt() != MAGIC || in.getInt() != 1) {
            throw new IllegalArgumentException("native Cell head wire identity differs");
        }
        var namespace = digest(in);
        var cell = new CellProviderScopeId(digest(in));
        int version = in.getInt();
        var limits = new Limits(in.getInt(), in.getInt());
        int count = in.getInt();
        if (count < 0 || count > limits.capacity() || bytes.length() != HEADER_BYTES + count * HOLD_BYTES) {
            throw new IllegalArgumentException("native Cell head reservation count differs");
        }
        var rows = new ArrayList<Hold>();
        for (int index = 0; index < count; index++) {
            var operation = new UUID(in.getLong(), in.getLong());
            var resource = digest(in);
            var intent = digest(in);
            int state = in.getInt();
            if (state != 0 && state != 1) {
                throw new IllegalArgumentException("native Cell reservation state differs");
            }
            rows.add(new Hold(operation, resource, intent, state == 1));
        }
        var value = new Snapshot(namespace, cell, version, limits, rows);
        if (nativeVersion != version || !value.encode().equals(bytes)) {
            throw new IllegalArgumentException("native Cell head checksum/version differs");
        }
        return value;
    }

    private static Sha256Digest digest(ByteBuffer in) {
        var bytes = new byte[32];
        in.get(bytes);
        return Sha256Digest.copyOf(bytes);
    }

    private record Completion(DeleteResult result, Throwable failure) {}
}
