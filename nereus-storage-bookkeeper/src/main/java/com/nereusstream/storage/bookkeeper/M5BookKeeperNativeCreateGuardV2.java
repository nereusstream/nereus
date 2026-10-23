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
import com.nereusstream.storage.api.bookkeeper.RunLedgerConfigurationV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerHandleV1;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/** Permanent native task-create fence and per-ledger ownership; neither record is a deletion authorization. */
final class M5BookKeeperNativeCreateGuardV2 {
    private final ZooKeeper zk;
    private final String base;
    private final String instancePath;
    private final String nativeInstanceId;
    private final Sha256Digest scopeSha;
    private final List<ACL> acls;
    private final String taskPath;
    private final byte[] open;
    private final byte[] fenced;
    private final String birthRoot;
    private final String namespaceGatePath;
    private final int namespaceGateVersion;
    private final M5BookKeeperNamespaceGateV2 namespaceGate;
    private final Optional<PhysicalNamespaceAuthorityBindingV2> admittedBinding;

    M5BookKeeperNativeCreateGuardV2(
            ZooKeeper zk,
            String ledgerRoot,
            M5BookKeeperNativeCreateSpecV2 spec,
            List<ACL> acls,
            Optional<PhysicalNamespaceAuthorityBindingV2> binding)
            throws Exception {
        this(zk, ledgerRoot, spec.nativeInstanceId(), spec.taskId(), spec.encode(), acls, binding, false, false);
    }

    M5BookKeeperNativeCreateGuardV2(
            ZooKeeper zk,
            String ledgerRoot,
            String nativeInstanceId,
            Sha256Digest scopeId,
            CanonicalBytes scope,
            List<ACL> acls,
            Optional<PhysicalNamespaceAuthorityBindingV2> binding)
            throws Exception {
        this(zk, ledgerRoot, nativeInstanceId, scopeId, scope, acls, binding, true, false);
    }

    M5BookKeeperNativeCreateGuardV2(
            ZooKeeper zk,
            String ledgerRoot,
            String nativeInstanceId,
            Sha256Digest scopeId,
            CanonicalBytes scope,
            List<ACL> acls,
            Optional<PhysicalNamespaceAuthorityBindingV2> binding,
            boolean existingOnly)
            throws Exception {
        this(zk, ledgerRoot, nativeInstanceId, scopeId, scope, acls, binding, true, existingOnly);
    }

    private M5BookKeeperNativeCreateGuardV2(
            ZooKeeper zk,
            String ledgerRoot,
            String nativeInstanceId,
            Sha256Digest scopeId,
            CanonicalBytes scope,
            List<ACL> acls,
            Optional<PhysicalNamespaceAuthorityBindingV2> binding,
            boolean indexed,
            boolean existingOnly)
            throws Exception {
        this.zk = zk;
        this.nativeInstanceId = nativeInstanceId;
        this.scopeSha = Sha256Digest.hash(scope);
        this.acls = List.copyOf(acls);
        instancePath = ledgerRoot + "/INSTANCEID";
        String actualInstance = new String(zk.getData(instancePath, false, null), StandardCharsets.US_ASCII);
        if (!actualInstance.equals(nativeInstanceId)) {
            throw new IllegalStateException("native BookKeeper INSTANCEID differs from the task create scope");
        }
        namespaceGate = new M5BookKeeperNamespaceGateV2(zk, ledgerRoot, actualInstance, acls);
        admittedBinding = binding;
        var namespaceSnapshot = binding.isPresent()
                ? namespaceGate.read().get()
                : namespaceGate.initializeUnbound().get();
        if (!namespaceSnapshot.binding().equals(binding)) {
            throw new IllegalStateException("native M5 create client lacks the exact bound metadata namespace");
        }
        namespaceGatePath = namespaceGate.path();
        namespaceGateVersion = namespaceSnapshot.nativeVersion();
        base = ledgerRoot + "/nereus-m5-native-v2";
        String hash = scopeId.toHex();
        taskPath = base + "/tasks/" + hash.substring(0, 2) + "/" + hash;
        byte[] encoded = scope.toByteArray();
        open = ByteBuffer.allocate(encoded.length + 1)
                .put((byte) 0)
                .put(encoded)
                .array();
        fenced = open.clone();
        fenced[0] = 1;
        birthRoot = indexed ? taskPath + "/pulsar-births-v1" : null;
        boolean created = false;
        if (!existingOnly) {
            createParents(taskPath.substring(0, taskPath.lastIndexOf('/'))).get();
            try {
                zk.create(taskPath, open, acls, CreateMode.PERSISTENT);
                created = true;
            } catch (KeeperException.NodeExistsException exists) {
                // A fresh client may reopen the scope for read/reconciliation; its native creates still check version
                // zero.
            }
        }
        Stat stat = new Stat();
        byte[] value = zk.getData(taskPath, false, stat);
        if (stat.getEphemeralOwner() != 0
                || !((stat.getVersion() == 0 && Arrays.equals(value, open))
                        || (stat.getVersion() == 1 && Arrays.equals(value, fenced)))) {
            throw new IllegalStateException("native BK task create fence differs or has an invalid revision");
        }
        if (existingOnly && stat.getVersion() != 1) {
            throw new IllegalStateException("Native scope observation requires its exact existing fence");
        }
        if (created && birthRoot != null) {
            zk.create(birthRoot, birthHead(0), acls, CreateMode.PERSISTENT);
        }
    }

    String taskPath() {
        return taskPath;
    }

    CompletableFuture<PhysicalNamespaceAuthorityBindingV2> requireNamespaceBinding() {
        if (admittedBinding.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalStateException("native client was not opened bound"));
        }
        return namespaceGate
                .read()
                .thenApply(actual -> {
                    if (actual.nativeVersion() != namespaceGateVersion
                            || !actual.binding().equals(admittedBinding)) {
                        throw new IllegalStateException("native client namespace binding changed");
                    }
                    return actual.binding().orElseThrow();
                })
                .toCompletableFuture();
    }

    boolean requiresDeleteQuota() {
        return admittedBinding.isPresent();
    }

    M5BookKeeperNativeDeleteQuotaV2 deleteQuota() {
        return new M5BookKeeperNativeDeleteQuotaV2(zk, namespaceGate, acls);
    }

    String reservationPath(long ledgerId) {
        if (ledgerId < 0) {
            throw new IllegalArgumentException("native ledger ID is negative");
        }
        String id = String.format(java.util.Locale.ROOT, "%020d", ledgerId);
        String shard = com.nereusstream.domain.bytes.Sha256Digest.hash(CanonicalBytes.copyOf(
                        ByteBuffer.allocate(8).putLong(ledgerId).array()))
                .toHex()
                .substring(0, 2);
        return base + "/ledgers/" + shard + "/" + id;
    }

    CompletableFuture<Void> reserve(long ledgerId) {
        String path = reservationPath(ledgerId);
        UUID nonce = UUID.randomUUID();
        byte[] candidate = ByteBuffer.allocate(56)
                .put(scopeSha.bytes().toByteArray())
                .putLong(ledgerId)
                .putLong(nonce.getMostSignificantBits())
                .putLong(nonce.getLeastSignificantBits())
                .array();
        if (birthRoot != null) {
            return createParents(path.substring(0, path.lastIndexOf('/')))
                    .thenCompose(ignored -> reserveIndexed(ledgerId, candidate, 8));
        }
        return createParents(path.substring(0, path.lastIndexOf('/'))).thenCompose(ignored -> {
            var result = new CompletableFuture<Void>();
            zk.multi(
                    List.of(
                            Op.check(namespaceGatePath, namespaceGateVersion),
                            Op.check(taskPath, 0),
                            Op.create(path, candidate, acls, CreateMode.PERSISTENT)),
                    (rc, actualPath, context, operations) -> {
                        if (rc == KeeperException.Code.OK.intValue()) {
                            result.complete(null);
                        } else if (rc == KeeperException.Code.NODEEXISTS.intValue()
                                || rc == KeeperException.Code.CONNECTIONLOSS.intValue()
                                || rc == KeeperException.Code.OPERATIONTIMEOUT.intValue()) {
                            zk.getData(
                                    path,
                                    false,
                                    (readRc, readPath, ignoredContext, data, stat) -> {
                                        if (readRc != KeeperException.Code.OK.intValue()) {
                                            result.completeExceptionally(failure(readRc, path));
                                        } else if (stat.getVersion() == 0
                                                && stat.getEphemeralOwner() == 0
                                                && Arrays.equals(data, candidate)) {
                                            result.complete(null);
                                        } else {
                                            result.completeExceptionally(new BKException.BKLedgerExistException());
                                        }
                                    },
                                    null);
                        } else {
                            result.completeExceptionally(failure(rc, taskPath));
                        }
                    },
                    null);
            return result;
        });
    }

    private byte[] birthHead(long count) {
        var out = ByteBuffer.allocate(80)
                .putInt(0x4d355042)
                .putInt(1)
                .put(scopeSha.bytes().toByteArray())
                .putLong(count);
        out.put(Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(out.array(), 48)))
                .bytes()
                .toByteArray());
        return out.array();
    }

    private long requireBirthHead(ExistingNode head) {
        return requireBirthHead(head, scopeSha);
    }

    private long requireBirthHead(ExistingNode head, Sha256Digest expectedScope) {
        var in = ByteBuffer.wrap(head.bytes());
        if (head.bytes().length != 80 || in.getInt() != 0x4d355042 || in.getInt() != 1) {
            throw new IllegalStateException("Native Pulsar birth head format differs");
        }
        byte[] scope = new byte[32];
        in.get(scope);
        long count = in.getLong();
        byte[] checksum = new byte[32];
        in.get(checksum);
        if (count < 0
                || count > 1024
                || head.stat().getEphemeralOwner() != 0
                || head.stat().getVersion() != count
                || !Arrays.equals(scope, expectedScope.bytes().toByteArray())
                || !Arrays.equals(
                        checksum,
                        Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(head.bytes(), 48)))
                                .bytes()
                                .toByteArray())) {
            throw new IllegalStateException("Native Pulsar birth head identity/count/version differs");
        }
        return count;
    }

    private CompletableFuture<Void> reserveIndexed(long ledgerId, byte[] candidate, int attempts) {
        return readExisting(birthRoot).thenCompose(head -> {
            long count = requireBirthHead(head);
            if (count == 1024) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Native Pulsar birth inventory is full"));
            }
            String index = birthRoot + "/" + String.format(java.util.Locale.ROOT, "%020d", ledgerId);
            var result = new CompletableFuture<Void>();
            zk.multi(
                    List.of(
                            Op.check(namespaceGatePath, namespaceGateVersion),
                            Op.check(taskPath, 0),
                            Op.setData(
                                    birthRoot, birthHead(count + 1), head.stat().getVersion()),
                            Op.create(reservationPath(ledgerId), candidate, acls, CreateMode.PERSISTENT),
                            Op.create(index, candidate, acls, CreateMode.PERSISTENT)),
                    (rc, path, context, operations) -> {
                        if (rc == KeeperException.Code.OK.intValue()) {
                            result.complete(null);
                        } else if (rc == KeeperException.Code.BADVERSION.intValue() && attempts > 1) {
                            // Only the original ID/nonce may retry a concurrent index-head CAS. The task check remains
                            // atomic.
                            readExisting(taskPath)
                                    .thenCompose(task -> {
                                        if (task.stat().getVersion() != 0 || !Arrays.equals(task.bytes(), open)) {
                                            return CompletableFuture.failedFuture(
                                                    new IllegalStateException("Native create scope is fenced"));
                                        }
                                        return reserveIndexed(ledgerId, candidate, attempts - 1);
                                    })
                                    .whenComplete((ignored, failure) -> {
                                        if (failure == null) {
                                            result.complete(null);
                                        } else {
                                            result.completeExceptionally(failure);
                                        }
                                    });
                        } else if (rc == KeeperException.Code.NODEEXISTS.intValue()
                                || rc == KeeperException.Code.CONNECTIONLOSS.intValue()
                                || rc == KeeperException.Code.OPERATIONTIMEOUT.intValue()) {
                            readExisting(reservationPath(ledgerId))
                                    .thenCompose(
                                            reservation -> readExisting(index).thenApply(entry -> {
                                                requireReservation(scopeSha, ledgerId, reservation);
                                                requireReservation(scopeSha, ledgerId, entry);
                                                if (!Arrays.equals(reservation.bytes(), candidate)
                                                        || !Arrays.equals(entry.bytes(), candidate)) {
                                                    throw new IllegalStateException(
                                                            "Native birth reservation belongs to another invocation");
                                                }
                                                return (Void) null;
                                            }))
                                    .whenComplete((ignored, failure) -> {
                                        if (failure == null) {
                                            result.complete(null);
                                        } else {
                                            result.completeExceptionally(failure);
                                        }
                                    });
                        } else {
                            result.completeExceptionally(failure(rc, taskPath));
                        }
                    },
                    null);
            return result;
        });
    }

    record IndexedBirth(
            long ledgerId,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact indexEntry,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact reservation) {}

    record FencedBirthIndex(
            M5BookKeeperNativeCreateClientV2.NativeRecordFact instance,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact task,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact head,
            List<IndexedBirth> births) {}

    record PriorPulsarBirth(
            M5PulsarNativeCreateSpecV1 scope, List<M5BookKeeperNativeCreateClientV2.NativeRecordFact> facts) {}

    /** Exact existing fenced scope and one indexed birth; this path never creates or fences a scope. */
    CompletableFuture<PriorPulsarBirth> readPriorPulsarBirth(Sha256Digest priorScope, long ledgerId) {
        String hash = priorScope.toHex();
        String priorTask = base + "/tasks/" + hash.substring(0, 2) + "/" + hash;
        String priorHead = priorTask + "/pulsar-births-v1";
        String priorIndex = priorHead + "/" + String.format(java.util.Locale.ROOT, "%020d", ledgerId);
        String priorReservation = reservationPath(ledgerId);
        return requireNamespaceBinding()
                .thenCompose(unused -> readInstanceFact())
                .thenCompose(instance -> readExisting(priorTask).thenCompose(task -> {
                    if (task.stat().getEphemeralOwner() != 0
                            || task.stat().getVersion() != 1
                            || task.bytes().length < 2
                            || task.bytes().length > M5PulsarNativeCreateSpecV1.MAX_BYTES + 1
                            || task.bytes()[0] != 1) {
                        throw new IllegalStateException("prior Native Pulsar scope lacks its exact create fence");
                    }
                    var original = M5PulsarNativeCreateSpecV1.decode(
                            CanonicalBytes.copyOf(Arrays.copyOfRange(task.bytes(), 1, task.bytes().length)));
                    if (!original.sha256().equals(priorScope)
                            || !original.nativeInstanceId().equals(nativeInstanceId)
                            || !Optional.of(original.namespaceBinding()).equals(admittedBinding)) {
                        throw new IllegalStateException("prior Native Pulsar scope belongs to another namespace");
                    }
                    return readExisting(priorHead).thenCompose(head -> {
                        if (requireBirthHead(head, priorScope) == 0) {
                            throw new IllegalStateException("prior Native Pulsar birth inventory is empty");
                        }
                        return readExisting(priorIndex).thenCompose(index -> readExisting(priorReservation)
                                .thenCompose(reservation -> {
                                    requireReservation(priorScope, ledgerId, index);
                                    requireReservation(priorScope, ledgerId, reservation);
                                    if (!Arrays.equals(index.bytes(), reservation.bytes())) {
                                        throw new IllegalStateException(
                                                "prior Native Pulsar birth reservation differs");
                                    }
                                    var facts = List.of(
                                            instance,
                                            nativeFact(priorTask, task),
                                            nativeFact(priorHead, head),
                                            nativeFact(priorIndex, index),
                                            nativeFact(priorReservation, reservation));
                                    CompletableFuture<Void> reread = CompletableFuture.completedFuture(null);
                                    for (var fact : facts) {
                                        reread = reread.thenCompose(unused -> readNativeRecord(fact.nativePath())
                                                .thenAccept(current -> {
                                                    if (!current.equals(fact)) {
                                                        throw new IllegalStateException(
                                                                "prior Native Pulsar birth facts changed"
                                                                        + " during capture");
                                                    }
                                                }));
                                    }
                                    return reread.thenCompose(unused -> requireNamespaceBinding())
                                            .thenApply(unused -> new PriorPulsarBirth(original, facts));
                                }));
                    });
                }));
    }

    CompletableFuture<FencedBirthIndex> readFencedBirthIndex() {
        if (birthRoot == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Native scope has no Pulsar birth index"));
        }
        return requireNamespaceBinding()
                .thenCompose(ignored -> readInstanceFact())
                .thenCompose(instance -> readExisting(taskPath).thenCompose(task -> {
                    if (task.stat().getVersion() != 1 || !Arrays.equals(task.bytes(), fenced)) {
                        throw new IllegalStateException("Native birth enumeration requires its exact create fence");
                    }
                    return readExisting(birthRoot).thenCompose(head -> {
                        long count = requireBirthHead(head);
                        return readChildren(birthRoot).thenCompose(names -> {
                            if (names.size() != count) {
                                throw new IllegalStateException("Native birth index count/children differ");
                            }
                            CompletableFuture<List<IndexedBirth>> reading =
                                    CompletableFuture.completedFuture(new java.util.ArrayList<>());
                            for (String name : names.stream().sorted().toList()) {
                                long id;
                                try {
                                    id = Long.parseLong(name);
                                } catch (NumberFormatException invalid) {
                                    throw new IllegalStateException("Native birth child name is invalid", invalid);
                                }
                                if (id < 0 || !name.equals(String.format(java.util.Locale.ROOT, "%020d", id))) {
                                    throw new IllegalStateException("Native birth child identity is not canonical");
                                }
                                reading = reading.thenCompose(births -> readExisting(birthRoot + "/" + name)
                                        .thenCompose(entry -> readExisting(reservationPath(id))
                                                .thenApply(reservation -> {
                                                    requireReservation(scopeSha, id, entry);
                                                    requireReservation(scopeSha, id, reservation);
                                                    if (!Arrays.equals(entry.bytes(), reservation.bytes())) {
                                                        throw new IllegalStateException(
                                                                "Native birth index and physical reservation differ");
                                                    }
                                                    births.add(new IndexedBirth(
                                                            id,
                                                            nativeFact(birthRoot + "/" + name, entry),
                                                            nativeFact(reservationPath(id), reservation)));
                                                    return births;
                                                })));
                            }
                            return reading.thenCompose(
                                    births -> readExisting(taskPath).thenCompose(afterTask -> readExisting(birthRoot)
                                            .thenApply(afterHead -> {
                                                if (!sameNativeNode(task, afterTask)
                                                        || !sameNativeNode(head, afterHead)) {
                                                    throw new IllegalStateException(
                                                            "Native birth index changed during enumeration");
                                                }
                                                return new FencedBirthIndex(
                                                        instance,
                                                        nativeFact(taskPath, task),
                                                        nativeFact(birthRoot, head),
                                                        List.copyOf(births));
                                            })));
                        });
                    });
                }));
    }

    private static boolean sameNativeNode(ExistingNode left, ExistingNode right) {
        return left.stat().getVersion() == right.stat().getVersion()
                && left.stat().getCzxid() == right.stat().getCzxid()
                && left.stat().getMzxid() == right.stat().getMzxid()
                && Arrays.equals(left.bytes(), right.bytes());
    }

    private CompletableFuture<List<String>> readChildren(String path) {
        var result = new CompletableFuture<List<String>>();
        zk.getChildren(
                path,
                false,
                (rc, returnedPath, context, children, stat) -> {
                    if (rc == KeeperException.Code.OK.intValue() && path.equals(returnedPath)) {
                        result.complete(List.copyOf(children));
                    } else {
                        result.completeExceptionally(failure(rc, path));
                    }
                },
                null);
        return result;
    }

    CompletableFuture<Optional<M5BookKeeperNativeCreateClientV2.NativeRecordFact>> readOptionalNativeRecord(
            String path) {
        var result = new CompletableFuture<Optional<M5BookKeeperNativeCreateClientV2.NativeRecordFact>>();
        zk.getData(
                path,
                false,
                (rc, returnedPath, context, bytes, stat) -> {
                    if (rc == KeeperException.Code.NONODE.intValue()) {
                        result.complete(Optional.empty());
                    } else if (rc == KeeperException.Code.OK.intValue() && path.equals(returnedPath)) {
                        try {
                            result.complete(Optional.of(nativeFact(path, new ExistingNode(bytes, stat))));
                        } catch (RuntimeException invalid) {
                            result.completeExceptionally(invalid);
                        }
                    } else {
                        result.completeExceptionally(failure(rc, path));
                    }
                },
                null);
        return result;
    }

    CompletableFuture<Void> requireOwned(long ledgerId) {
        String path = reservationPath(ledgerId);
        var result = new CompletableFuture<Void>();
        zk.getData(
                path,
                false,
                (rc, actualPath, context, data, stat) -> {
                    if (rc == KeeperException.Code.OK.intValue()
                            && data.length == 56
                            && stat.getEphemeralOwner() == 0
                            && stat.getVersion() == 0
                            && Arrays.equals(
                                    Arrays.copyOf(data, 32), scopeSha.bytes().toByteArray())
                            && ByteBuffer.wrap(data, 32, 8).getLong() == ledgerId) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(new BKException.BKUnauthorizedAccessException());
                    }
                },
                null);
        return result;
    }

    List<Op> createChecks(long ledgerId) {
        return List.of(
                Op.check(namespaceGatePath, namespaceGateVersion),
                Op.check(taskPath, 0),
                Op.check(reservationPath(ledgerId), 0));
    }

    List<Op> deleteChecks(long ledgerId) {
        return List.of(
                Op.check(namespaceGatePath, namespaceGateVersion),
                Op.check(taskPath, 1),
                Op.check(reservationPath(ledgerId), 0));
    }

    CompletableFuture<Void> fenceCreates() {
        var result = new CompletableFuture<Void>();
        zk.setData(
                taskPath,
                fenced,
                0,
                (rc, path, context, stat) -> {
                    // Always reread exact durable state, including a response lost after native CAS.
                    zk.getData(
                            taskPath,
                            false,
                            (readRc, readPath, ignored, bytes, observed) -> {
                                if (readRc == KeeperException.Code.OK.intValue()
                                        && observed.getVersion() == 1
                                        && observed.getEphemeralOwner() == 0
                                        && Arrays.equals(bytes, fenced)) {
                                    result.complete(null);
                                } else {
                                    result.completeExceptionally(failure(readRc == 0 ? rc : readRc, taskPath));
                                }
                            },
                            null);
                },
                null);
        return result;
    }

    /** Exact permanent scope fence; a missing or rewritten fence never proves late creates are excluded. */
    CompletableFuture<M5BookKeeperNativeCreateClientV2.NativeRecordFact> readClosedTaskFact() {
        return readNativeRecord(taskPath).thenApply(value -> {
            if (value.nativeVersion() != 1
                    || !Arrays.equals(value.rawStoredBytes().toByteArray(), fenced)) {
                throw new IllegalStateException("Native create scope is not exactly fenced");
            }
            return value;
        });
    }

    /** Fences an already recorded exact scope without creating a missing native task or reservation. */
    CompletableFuture<Void> fenceExistingScope(
            M5BookKeeperNativeCreateSpecV2 expected,
            RunLedgerHandleV1 handle,
            BookKeeperCapabilitySnapshotV1 capability) {
        final ExistingScope scope;
        try {
            scope = existingScope(expected, handle, capability);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return readExisting(reservationPath(scope.ledgerId())).thenCompose(reservation -> {
            requireReservation(scope.specSha256(), scope.ledgerId(), reservation);
            return readExisting(scope.taskPath()).thenCompose(task -> {
                if (task.stat().getEphemeralOwner() != 0) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("existing native task fence is ephemeral"));
                }
                if (task.stat().getVersion() == 1 && Arrays.equals(task.bytes(), scope.fenced())) {
                    return CompletableFuture.completedFuture(null);
                }
                if (task.stat().getVersion() != 0 || !Arrays.equals(task.bytes(), scope.open())) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("existing native task differs from exact open scope"));
                }
                var result = new CompletableFuture<Void>();
                zk.setData(
                        scope.taskPath(),
                        scope.fenced(),
                        0,
                        (rc, path, context, stat) -> readExisting(scope.taskPath())
                                .whenComplete((observed, failure) -> {
                                    if (failure == null
                                            && observed.stat().getVersion() == 1
                                            && observed.stat().getEphemeralOwner() == 0
                                            && Arrays.equals(observed.bytes(), scope.fenced())) {
                                        result.complete(null);
                                    } else {
                                        result.completeExceptionally(
                                                failure == null
                                                        ? new IllegalStateException(
                                                                "existing native task fence is unresolved")
                                                        : failure);
                                    }
                                }),
                        null);
                return result;
            });
        });
    }

    record ScopeFacts(
            M5BookKeeperNativeCreateClientV2.NativeRecordFact taskFence,
            M5BookKeeperNativeCreateClientV2.NativeRecordFact reservation) {}

    /** Reads an existing full Task scope; never creates or advances its native admission. */
    CompletableFuture<M5BookKeeperNativeCreateClientV2.NativeRecordFact> readTaskCreateScopeFact(
            M5BookKeeperNativeCreateSpecV2 expected, BookKeeperCapabilitySnapshotV1 capability) {
        if (!expected.nativeInstanceId().equals(nativeInstanceId)
                || expected.configurations().stream()
                        .anyMatch(configuration -> !configuration.equals(
                                RunLedgerConfigurationV1.from(capability, configuration.runId())))) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("existing native Task scope changes namespace or capability"));
        }
        String hash = expected.taskId().toHex();
        String path = base + "/tasks/" + hash.substring(0, 2) + "/" + hash;
        var encoded = expected.encode().toByteArray();
        return readNativeRecord(path).thenApply(fact -> {
            byte[] bytes = fact.rawStoredBytes().toByteArray();
            if (fact.nativeVersion() > 1
                    || bytes.length != encoded.length + 1
                    || bytes[0] != fact.nativeVersion()
                    || !Arrays.equals(Arrays.copyOfRange(bytes, 1, bytes.length), encoded)) {
                throw new IllegalStateException("existing native Task create scope differs from its exact revision");
            }
            return fact;
        });
    }

    /** Read-only post-recovery observation; a missing task or reservation is never recreated. */
    CompletableFuture<ScopeFacts> readFencedExistingScope(
            M5BookKeeperNativeCreateSpecV2 expected,
            RunLedgerHandleV1 handle,
            BookKeeperCapabilitySnapshotV1 capability) {
        final ExistingScope scope;
        try {
            scope = existingScope(expected, handle, capability);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
        String reservationPath = reservationPath(scope.ledgerId());
        return readExisting(reservationPath).thenCompose(reservation -> {
            requireReservation(scope.specSha256(), scope.ledgerId(), reservation);
            return readExisting(scope.taskPath()).thenApply(task -> {
                if (task.stat().getEphemeralOwner() != 0
                        || task.stat().getVersion() != 1
                        || !Arrays.equals(task.bytes(), scope.fenced())) {
                    throw new IllegalStateException("existing native task lacks its exact permanent create fence");
                }
                return new ScopeFacts(nativeFact(scope.taskPath(), task), nativeFact(reservationPath, reservation));
            });
        });
    }

    private record ExistingScope(String taskPath, byte[] open, byte[] fenced, long ledgerId, Sha256Digest specSha256) {}

    /** A sealed ledger can belong to a still-open exact Task; this read never fences or recreates that Task. */
    CompletableFuture<ScopeFacts> readSealedExistingScope(
            M5BookKeeperNativeCreateSpecV2 expected,
            RunLedgerHandleV1 handle,
            BookKeeperCapabilitySnapshotV1 capability) {
        if (!handle.providerScopeId().equals(capability.providerScopeId())
                || !handle.configurationDigest().equals(capability.configurationDigest())
                || !expected.configurations().contains(RunLedgerConfigurationV1.from(capability, handle.runId()))) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("sealed native ledger is outside its exact Task configuration"));
        }
        String path = reservationPath(handle.ledgerIdentity().ledgerId());
        return readTaskCreateScopeFact(expected, capability)
                .thenCompose(task -> readExisting(path).thenApply(reservation -> {
                    requireReservation(
                            expected.sha256(), handle.ledgerIdentity().ledgerId(), reservation);
                    return new ScopeFacts(task, nativeFact(path, reservation));
                }));
    }

    private ExistingScope existingScope(
            M5BookKeeperNativeCreateSpecV2 expected,
            RunLedgerHandleV1 handle,
            BookKeeperCapabilitySnapshotV1 capability) {
        if (expected == null
                || handle == null
                || capability == null
                || !expected.nativeInstanceId().equals(nativeInstanceId)
                || !expected.configurations().equals(List.of(RunLedgerConfigurationV1.from(capability, handle.runId())))
                || !handle.providerScopeId().equals(capability.providerScopeId())
                || !handle.configurationDigest().equals(capability.configurationDigest())) {
            throw new IllegalArgumentException("existing native scope differs from exact ledger and capability");
        }
        long ledgerId = handle.ledgerIdentity().ledgerId();
        String hash = expected.taskId().toHex();
        String existingTaskPath = base + "/tasks/" + hash.substring(0, 2) + "/" + hash;
        byte[] encoded = expected.encode().toByteArray();
        byte[] expectedOpen = ByteBuffer.allocate(encoded.length + 1)
                .put((byte) 0)
                .put(encoded)
                .array();
        byte[] expectedFenced = expectedOpen.clone();
        expectedFenced[0] = 1;
        return new ExistingScope(existingTaskPath, expectedOpen, expectedFenced, ledgerId, expected.sha256());
    }

    private static void requireReservation(Sha256Digest specSha256, long ledgerId, ExistingNode reservation) {
        byte[] bytes = reservation.bytes();
        if (reservation.stat().getVersion() != 0
                || reservation.stat().getEphemeralOwner() != 0
                || bytes.length != 56
                || !Arrays.equals(Arrays.copyOf(bytes, 32), specSha256.bytes().toByteArray())
                || ByteBuffer.wrap(bytes, 32, 8).getLong() != ledgerId) {
            throw new IllegalStateException("native ledger reservation differs from exact existing scope");
        }
    }

    private record ExistingNode(byte[] bytes, Stat stat) {}

    CompletableFuture<M5BookKeeperNativeCreateClientV2.NativeRecordFact> readNativeRecord(String path) {
        return readExisting(path).thenApply(node -> nativeFact(path, node));
    }

    CompletableFuture<M5BookKeeperNativeCreateClientV2.NativeRecordFact> readInstanceFact() {
        return readNativeRecord(instancePath).thenApply(value -> {
            if (!Arrays.equals(
                    value.rawStoredBytes().toByteArray(), nativeInstanceId.getBytes(StandardCharsets.US_ASCII))) {
                throw new IllegalStateException("native BookKeeper INSTANCEID changed after admission");
            }
            return value;
        });
    }

    private static M5BookKeeperNativeCreateClientV2.NativeRecordFact nativeFact(String path, ExistingNode node) {
        if (node.stat().getEphemeralOwner() != 0) {
            throw new IllegalStateException("native authority fact is ephemeral");
        }
        return new M5BookKeeperNativeCreateClientV2.NativeRecordFact(
                path,
                CanonicalBytes.copyOf(node.bytes()),
                node.stat().getCzxid(),
                node.stat().getMzxid(),
                node.stat().getVersion(),
                node.stat().getCtime());
    }

    private CompletableFuture<ExistingNode> readExisting(String path) {
        var result = new CompletableFuture<ExistingNode>();
        zk.getData(
                path,
                false,
                (rc, actualPath, context, bytes, stat) -> {
                    if (rc == KeeperException.Code.OK.intValue() && path.equals(actualPath)) {
                        result.complete(new ExistingNode(bytes, stat));
                    } else {
                        result.completeExceptionally(failure(rc, path));
                    }
                },
                null);
        return result;
    }

    CompletableFuture<Void> createParents(String path) {
        var result = new CompletableFuture<Void>();
        ZkUtils.asyncCreateFullPathOptimistic(
                zk,
                path,
                new byte[0],
                acls,
                CreateMode.PERSISTENT,
                (rc, actualPath, context, created) -> {
                    if (rc == KeeperException.Code.OK.intValue() || rc == KeeperException.Code.NODEEXISTS.intValue()) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(failure(rc, path));
                    }
                },
                null);
        return result;
    }

    static BKException failure(int rc, String path) {
        KeeperException.Code code;
        try {
            code = KeeperException.Code.get(rc);
        } catch (IllegalArgumentException unknownCode) {
            code = KeeperException.Code.SYSTEMERROR;
        }
        if (code == null || code == KeeperException.Code.OK) {
            code = KeeperException.Code.DATAINCONSISTENCY;
        }
        return new BKException.ZKException(KeeperException.create(code, path));
    }
}
