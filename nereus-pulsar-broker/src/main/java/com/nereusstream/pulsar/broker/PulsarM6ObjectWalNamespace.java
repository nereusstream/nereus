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

package com.nereusstream.pulsar.broker;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.BookKeeperInstanceIdV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/** Verifies the dedicated BookKeeper compatibility namespace with all native writers excluded by its ACLs. */
final class PulsarM6ObjectWalNamespace {
    static final String NATIVE_ID_NODE = "qualified-bk-idgen";
    static final String NATIVE_ID_PRINCIPAL = "nereus-qualified-bk-id";
    private static final Set<String> ROOT_NODES =
            Set.of("INSTANCEID", "LAYOUT", "available", "cookies", NATIVE_ID_NODE);
    private static final String AVAILABLE = "/available";
    private static final String READONLY = "/readonly";

    private PulsarM6ObjectWalNamespace() {}

    record Locked(BookKeeperInstanceIdV1 instance, Sha256Digest proof, long generation) {
        Sha256Digest nativePrincipal() {
            return Sha256Digest.hash(
                    com.nereusstream.domain.bytes.CanonicalUtf8.fromString(NATIVE_ID_PRINCIPAL + "/" + instance.value())
                            .bytes());
        }
    }

    static Locked read(String servers, String path, Duration timeout) throws Exception {
        var connected = new CompletableFuture<Void>();
        try (var zk = new ZooKeeper(servers, Math.toIntExact(timeout.toMillis()), event -> {
            if (event.getState() == KeeperState.SyncConnected) {
                connected.complete(null);
            }
        })) {
            connected.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            return read(zk, path);
        }
    }

    static Locked read(ZooKeeper zk, String path) throws Exception {
        try (var bytes = new ByteArrayOutputStream();
                var output = new DataOutputStream(bytes)) {
            output.writeUTF("NEREUS_PULSAR_BOOKKEEPER_COMPATIBILITY_LOCK_V1");
            output.writeUTF(path);
            Stat root = new Stat();
            var names = new TreeSet<>(zk.getChildren(path, false, root));
            if (root.getEphemeralOwner() != 0
                    || !names.containsAll(Set.of("INSTANCEID", "LAYOUT", NATIVE_ID_NODE))
                    || !ROOT_NODES.containsAll(names)) {
                throw new IllegalStateException("Object WAL requires a fresh isolated BookKeeper namespace");
            }
            requireFrozen(zk, path, root, output);
            byte[] instance = null;
            for (String name : names) {
                String child = path + "/" + name;
                if (NATIVE_ID_NODE.equals(name)) {
                    requireNativeIdGenerator(zk, child, output);
                    continue;
                }
                Stat value = new Stat();
                byte[] data = zk.getData(child, false, value);
                requireFrozen(zk, child, value, output);
                output.writeInt(data.length);
                output.write(data);
                var children = zk.getChildren(child, false);
                if (child.endsWith(AVAILABLE) && children.equals(List.of("readonly"))) {
                    var leaf = child + READONLY;
                    Stat stat = new Stat();
                    if (!zk.getChildren(leaf, false, stat).isEmpty()) {
                        throw new IllegalStateException("Object WAL compatibility namespace has active bookies");
                    }
                    requireFrozen(zk, leaf, stat, output);
                } else if (!children.isEmpty()) {
                    throw new IllegalStateException("Object WAL compatibility namespace has native writers or ledgers");
                }
                if ("INSTANCEID".equals(name)) {
                    instance = data;
                }
            }
            if (root.getCzxid() <= 0 || zk.exists(path, false).getCzxid() != root.getCzxid()) {
                throw new IllegalStateException("Object WAL compatibility namespace was replaced");
            }
            output.flush();
            return new Locked(
                    BookKeeperInstanceIdV1.fromBytes(instance),
                    Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray())),
                    root.getCzxid());
        }
    }

    private static void requireNativeIdGenerator(ZooKeeper zk, String path, DataOutputStream proof) throws Exception {
        var stat = new Stat();
        var acls = zk.getACL(path, stat);
        int mutable = ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE;
        if (acls.size() != 2
                || !acls.contains(ZooDefs.Ids.READ_ACL_UNSAFE.get(0))
                || stat.getEphemeralOwner() != 0
                || acls.stream()
                                .filter(acl -> acl.getPerms() == mutable
                                        && acl.getId().getScheme().equals("digest")
                                        && acl.getId().getId().startsWith(NATIVE_ID_PRINCIPAL + ":"))
                                .count()
                        != 1) {
            throw new IllegalStateException("BookKeeper ID generator does not have its exact bounded writer ACL");
        }
        // The ID generator may mutate its child sequence. Data, ACL and the parent structure remain immutable.
        proof.writeUTF(path);
        proof.writeUTF(NATIVE_ID_PRINCIPAL);
        proof.writeLong(stat.getCzxid());
        proof.writeInt(stat.getVersion());
        proof.writeInt(stat.getAversion());
        proof.writeInt(mutable);
    }

    private static void requireFrozen(ZooKeeper zk, String path, Stat original, DataOutputStream proof)
            throws Exception {
        Stat now = new Stat();
        var acls = zk.getACL(path, now);
        if (!acls.equals(ZooDefs.Ids.READ_ACL_UNSAFE)
                || now.getEphemeralOwner() != 0
                || original.getCzxid() != now.getCzxid()
                || original.getVersion() != now.getVersion()
                || original.getCversion() != now.getCversion()
                || original.getAversion() != now.getAversion()) {
            throw new IllegalStateException("BookKeeper compatibility namespace is writable or changed");
        }
        proof.writeUTF(path);
        proof.writeLong(now.getCzxid());
        proof.writeInt(now.getVersion());
        proof.writeInt(now.getCversion());
        proof.writeInt(now.getAversion());
        proof.writeInt(ZooDefs.Perms.READ);
    }
}
