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
    private final M5BookKeeperNativeCreateSpecV2 spec;
    private final List<ACL> acls;
    private final String taskPath;
    private final byte[] open;
    private final byte[] fenced;
    private final String namespaceGatePath;
    private final int namespaceGateVersion;

    M5BookKeeperNativeCreateGuardV2(
            ZooKeeper zk,
            String ledgerRoot,
            M5BookKeeperNativeCreateSpecV2 spec,
            List<ACL> acls,
            Optional<PhysicalNamespaceAuthorityBindingV2> binding)
            throws Exception {
        this.zk = zk;
        this.spec = spec;
        this.acls = List.copyOf(acls);
        String actualInstance =
                new String(zk.getData(ledgerRoot + "/INSTANCEID", false, null), StandardCharsets.US_ASCII);
        if (!actualInstance.equals(spec.nativeInstanceId())) {
            throw new IllegalStateException("native BookKeeper INSTANCEID differs from the task create scope");
        }
        var namespaceGate = new M5BookKeeperNamespaceGateV2(zk, ledgerRoot, actualInstance, acls);
        var namespaceSnapshot = binding.isPresent()
                ? namespaceGate.read().get()
                : namespaceGate.initializeUnbound().get();
        if (!namespaceSnapshot.binding().equals(binding)) {
            throw new IllegalStateException("native M5 create client lacks the exact bound metadata namespace");
        }
        namespaceGatePath = namespaceGate.path();
        namespaceGateVersion = namespaceSnapshot.nativeVersion();
        base = ledgerRoot + "/nereus-m5-native-v2";
        String hash = spec.taskId().toHex();
        taskPath = base + "/tasks/" + hash.substring(0, 2) + "/" + hash;
        byte[] encoded = spec.encode().toByteArray();
        open = ByteBuffer.allocate(encoded.length + 1)
                .put((byte) 0)
                .put(encoded)
                .array();
        fenced = open.clone();
        fenced[0] = 1;
        createParents(taskPath.substring(0, taskPath.lastIndexOf('/'))).get();
        try {
            zk.create(taskPath, open, acls, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException exists) {
            // A fresh client may reopen the scope for read/reconciliation; its native creates still check version zero.
        }
        Stat stat = new Stat();
        byte[] value = zk.getData(taskPath, false, stat);
        if (stat.getEphemeralOwner() != 0
                || !((stat.getVersion() == 0 && Arrays.equals(value, open))
                        || (stat.getVersion() == 1 && Arrays.equals(value, fenced)))) {
            throw new IllegalStateException("native BK task create fence differs or has an invalid revision");
        }
    }

    String taskPath() {
        return taskPath;
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
                .put(spec.sha256().bytes().toByteArray())
                .putLong(ledgerId)
                .putLong(nonce.getMostSignificantBits())
                .putLong(nonce.getLeastSignificantBits())
                .array();
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
                                        if (readRc == KeeperException.Code.OK.intValue()
                                                && stat.getVersion() == 0
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
                                    Arrays.copyOf(data, 32),
                                    spec.sha256().bytes().toByteArray())
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
