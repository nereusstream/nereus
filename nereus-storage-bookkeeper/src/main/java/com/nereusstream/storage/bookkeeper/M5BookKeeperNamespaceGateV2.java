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
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/** Permanent native creation gate: UNBOUND/version 0 becomes BOUND/version 1 exactly once, closing old creates. */
final class M5BookKeeperNamespaceGateV2 {
    private static final int MAGIC = 0x4d354247; // M5BG
    private final ZooKeeper zk;
    private final String ledgerRoot;
    private final String nativeInstance;
    private final PhysicalResourceIdV2.Namespace namespace;
    private final String path;
    private final List<ACL> acls;

    M5BookKeeperNamespaceGateV2(ZooKeeper zk, String ledgerRoot, String nativeInstance, List<ACL> acls) {
        this.zk = zk;
        this.ledgerRoot = ledgerRoot;
        this.nativeInstance = nativeInstance;
        this.namespace = M5BookKeeperNativeCreateSpecV2.namespace(nativeInstance);
        this.path = ledgerRoot + "/nereus-m5-native-v2/authority-namespace";
        this.acls = List.copyOf(acls);
    }

    String path() {
        return path;
    }

    PhysicalResourceIdV2.Namespace namespace() {
        return namespace;
    }

    CompletableFuture<Snapshot> initializeUnbound() {
        return requireInstance().thenCompose(ignored -> {
            var parent = new CompletableFuture<Void>();
            ZkUtils.asyncCreateFullPathOptimistic(
                    zk,
                    path.substring(0, path.lastIndexOf('/')),
                    new byte[0],
                    acls,
                    CreateMode.PERSISTENT,
                    (rc, actualPath, context, created) -> {
                        if (rc == KeeperException.Code.OK.intValue()
                                || rc == KeeperException.Code.NODEEXISTS.intValue()) {
                            parent.complete(null);
                        } else {
                            parent.completeExceptionally(M5BookKeeperNativeCreateGuardV2.failure(rc, path));
                        }
                    },
                    null);
            return parent.thenCompose(created -> {
                var attempted = new CompletableFuture<Void>();
                zk.create(
                        path,
                        encode(Optional.empty()).toByteArray(),
                        acls,
                        CreateMode.PERSISTENT,
                        (rc, actualPath, context, name) -> attempted.complete(null),
                        null);
                return attempted.thenCompose(done -> read());
            });
        });
    }

    CompletableFuture<Snapshot> read() {
        return requireInstance().thenCompose(ignored -> {
            var result = new CompletableFuture<Snapshot>();
            zk.getData(
                    path,
                    false,
                    (rc, actualPath, context, bytes, stat) -> {
                        if (rc != KeeperException.Code.OK.intValue()) {
                            result.completeExceptionally(M5BookKeeperNativeCreateGuardV2.failure(rc, path));
                            return;
                        }
                        try {
                            var binding = decode(CanonicalBytes.copyOf(bytes));
                            int version = binding.isPresent() ? 1 : 0;
                            if (!path.equals(actualPath)
                                    || stat.getEphemeralOwner() != 0
                                    || stat.getVersion() != version) {
                                throw new IllegalStateException(
                                        "native physical namespace gate is not permanent at its exact version");
                            }
                            result.complete(new Snapshot(binding, version));
                        } catch (RuntimeException invalid) {
                            result.completeExceptionally(invalid);
                        }
                    },
                    null);
            return result;
        });
    }

    CompletableFuture<PhysicalNamespaceAuthorityBindingV2> bind(PhysicalNamespaceAuthorityBindingV2 candidate) {
        if (!candidate.physicalNamespace().equals(namespace)) {
            throw new IllegalArgumentException("native BK namespace differs from authority assignment");
        }
        return read().thenCompose(previous -> {
            if (previous.binding().isPresent()) {
                return requireExact(previous, candidate);
            }
            var attempted = new CompletableFuture<Void>();
            zk.setData(
                    path,
                    encode(Optional.of(candidate)).toByteArray(),
                    0,
                    (rc, actualPath, context, stat) -> attempted.complete(null),
                    null);
            return attempted.thenCompose(ignored -> read()).thenCompose(observed -> requireExact(observed, candidate));
        });
    }

    private CompletableFuture<Void> requireInstance() {
        var result = new CompletableFuture<Void>();
        zk.getData(
                ledgerRoot + "/INSTANCEID",
                false,
                (rc, actualPath, context, bytes, stat) -> {
                    if (rc == KeeperException.Code.OK.intValue()
                            && stat.getEphemeralOwner() == 0
                            && actualPath.equals(ledgerRoot + "/INSTANCEID")
                            && new String(bytes, StandardCharsets.US_ASCII).equals(nativeInstance)) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(
                                new IllegalStateException("native BookKeeper INSTANCEID changed or is unavailable"));
                    }
                },
                null);
        return result;
    }

    private CompletableFuture<PhysicalNamespaceAuthorityBindingV2> requireExact(
            Snapshot observed, PhysicalNamespaceAuthorityBindingV2 candidate) {
        if (observed.binding().filter(candidate::equals).isEmpty()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("physical namespace is bound to another metadata namespace"));
        }
        return CompletableFuture.completedFuture(candidate);
    }

    private CanonicalBytes encode(Optional<PhysicalNamespaceAuthorityBindingV2> binding) {
        var instance = nativeInstance.getBytes(StandardCharsets.US_ASCII);
        var assigned = binding.map(value -> value.encode().toByteArray()).orElseGet(() -> new byte[0]);
        var out = ByteBuffer.allocate(12 + instance.length + 4 + assigned.length + 32)
                .putInt(MAGIC)
                .putInt(2)
                .putInt(instance.length)
                .put(instance)
                .putInt(assigned.length)
                .put(assigned);
        out.put(Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position())))
                .bytes()
                .toByteArray());
        return CanonicalBytes.copyOf(out.array());
    }

    private Optional<PhysicalNamespaceAuthorityBindingV2> decode(CanonicalBytes bytes) {
        if (bytes.length() < 84 || bytes.length() > PhysicalNamespaceAuthorityBindingV2.MAX_BYTES + 84) {
            throw new IllegalArgumentException("native namespace gate byte length differs");
        }
        try {
            var in = ByteBuffer.wrap(bytes.toByteArray());
            if (in.getInt() != MAGIC || in.getInt() != 2 || in.getInt() != 36) {
                throw new IllegalArgumentException("native namespace gate wire identity differs");
            }
            var instance = new byte[36];
            in.get(instance);
            if (!Arrays.equals(instance, nativeInstance.getBytes(StandardCharsets.US_ASCII))) {
                throw new IllegalArgumentException("native namespace gate has another INSTANCEID");
            }
            int length = in.getInt();
            if (length < 0 || length > PhysicalNamespaceAuthorityBindingV2.MAX_BYTES || length + 32 != in.remaining()) {
                throw new IllegalArgumentException("native namespace gate binding size differs");
            }
            var assigned = new byte[length];
            in.get(assigned);
            var binding = length == 0
                    ? Optional.<PhysicalNamespaceAuthorityBindingV2>empty()
                    : Optional.of(PhysicalNamespaceAuthorityBindingV2.decode(CanonicalBytes.copyOf(assigned)));
            if (binding.filter(value -> !value.physicalNamespace().equals(namespace))
                            .isPresent()
                    || !encode(binding).equals(bytes)) {
                throw new IllegalArgumentException("native namespace gate binding/checksum differs");
            }
            return binding;
        } catch (java.nio.BufferUnderflowException malformed) {
            throw new IllegalArgumentException("native namespace gate is truncated", malformed);
        }
    }

    record Snapshot(Optional<PhysicalNamespaceAuthorityBindingV2> binding, int nativeVersion) {}
}
