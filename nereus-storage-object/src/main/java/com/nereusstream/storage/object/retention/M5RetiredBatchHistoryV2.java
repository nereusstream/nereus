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

package com.nereusstream.storage.object.retention;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredSourceRetirementBatchTombstoneV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/** Immutable sparse Merkle history. A root acquires authority only through the existing Binding selector CAS. */
public final class M5RetiredBatchHistoryV2 {
    public static final int DEPTH = 256;
    public static final int MAX_NODE_BYTES = 4_096;
    public static final int MAX_INSERT_NODES = DEPTH + 1;
    private static final int MAGIC = 0x4d354832; // M5H2

    public record Root(Sha256Digest sha256, long count) {
        public Root {
            requireDigest(sha256);
            if (count < 0) {
                throw new IllegalArgumentException("negative retired history count");
            }
        }
    }

    /** Siblings are in root-to-leaf order and have exactly 256 entries, even for an empty subtree. */
    public record Proof(
            BindingIdentity binding,
            Sha256Digest batchId,
            Optional<RetiredSourceRetirementBatchTombstoneV1> tombstone,
            List<Root> siblings) {
        public Proof {
            Objects.requireNonNull(binding, "binding");
            requireDigest(batchId);
            tombstone = Objects.requireNonNull(tombstone, "tombstone");
            siblings = List.copyOf(siblings);
            if (siblings.size() != DEPTH
                    || tombstone
                            .filter(value -> !value.binding().equals(binding)
                                    || !value.batchIdSha256().equals(batchId))
                            .isPresent()) {
                throw new IllegalArgumentException("retired history proof Binding, key or depth differs");
            }
            tombstone.ifPresent(M5RetentionCodecV1::encodeRetiredBatch);
        }
    }

    public record NodeWrite(Root root, CanonicalBytes bytes) {
        public NodeWrite {
            Objects.requireNonNull(root, "root");
            Objects.requireNonNull(bytes, "bytes");
            if (root.count() <= 0
                    || bytes.isEmpty()
                    || bytes.length() > MAX_NODE_BYTES
                    || !Sha256Digest.hash(bytes).equals(root.sha256())) {
                throw new IllegalArgumentException("retired history node differs from its content address");
            }
        }
    }

    public record Insertion(Root predecessor, Root successor, Proof absenceProof, List<NodeWrite> nodes) {
        public Insertion {
            Objects.requireNonNull(predecessor, "predecessor");
            Objects.requireNonNull(successor, "successor");
            Objects.requireNonNull(absenceProof, "absenceProof");
            nodes = List.copyOf(nodes);
            if (nodes.size() != MAX_INSERT_NODES
                    || successor.count() != Math.addExact(predecessor.count(), 1)
                    || !nodes.get(nodes.size() - 1).root().equals(successor)) {
                throw new IllegalArgumentException("retired history insertion is not one complete bounded path");
            }
        }

        public long encodedBytes() {
            return nodes.stream().mapToLong(node -> node.bytes().length()).sum();
        }
    }

    private record Node(
            int depth, Root root, Root left, Root right, Optional<RetiredSourceRetirementBatchTombstoneV1> tombstone) {}

    private final BindingIdentity binding;
    private final Root[] empty = new Root[DEPTH + 1];

    public M5RetiredBatchHistoryV2(BindingIdentity binding) {
        this.binding = Objects.requireNonNull(binding, "binding");
        for (int depth = 0; depth <= DEPTH; depth++) {
            int level = depth;
            empty[depth] = new Root(Sha256Digest.hash(encode(out -> preamble(out, 0, level))), 0);
        }
    }

    public Root emptyRoot() {
        return empty[0];
    }

    public static Root emptyRoot(BindingIdentity binding) {
        return new M5RetiredBatchHistoryV2(binding).emptyRoot();
    }

    public String key(Sha256Digest digest) {
        requireDigest(digest);
        return "v2/m5-retired-history/" + binding.bindingId().digest().toHex() + "/"
                + binding.incarnationSha256().toHex() + "/"
                + binding.storageEpochSha256().toHex()
                + "/nodes/" + digest.toHex();
    }

    public void requireHead(Root root, Optional<CanonicalBytes> stored) {
        requireRoot(root, 0);
        if (root.count() != 0) {
            readNode(root, 0, stored);
        }
    }

    /** Only the root supplied by the exact selector predecessor may be used for admission. */
    public void verify(Root expected, Sha256Digest expectedBatchId, Proof proof) {
        requireRoot(expected, 0);
        if (!proof.binding().equals(binding) || !proof.batchId().equals(expectedBatchId)) {
            throw new IllegalArgumentException("retired history proof belongs to another Binding or requested key");
        }
        Root current = proof.tombstone().map(this::leaf).map(NodeWrite::root).orElse(empty[DEPTH]);
        byte[] path = proof.batchId().bytes().toByteArray();
        for (int depth = DEPTH - 1; depth >= 0; depth--) {
            Root sibling = proof.siblings().get(depth);
            requireRoot(sibling, depth + 1);
            current = bit(path, depth) ? combine(depth, sibling, current) : combine(depth, current, sibling);
        }
        if (!current.equals(expected)) {
            throw new IllegalArgumentException("retired history proof differs from the selected root or count");
        }
    }

    public Proof readProof(Root root, Sha256Digest batchId, Function<String, Optional<CanonicalBytes>> reader) {
        requireDigest(batchId);
        requireRoot(root, 0);
        List<Root> siblings = new ArrayList<>(DEPTH);
        Root current = root;
        byte[] path = batchId.bytes().toByteArray();
        for (int depth = 0; depth < DEPTH; depth++) {
            requireRoot(current, depth);
            if (current.count() == 0) {
                siblings.add(empty[depth + 1]);
                current = empty[depth + 1];
            } else {
                Node node = readNode(current, depth, reader.apply(key(current.sha256())));
                boolean right = bit(path, depth);
                siblings.add(right ? node.left() : node.right());
                current = right ? node.right() : node.left();
            }
        }
        Optional<RetiredSourceRetirementBatchTombstoneV1> tombstone = current.count() == 0
                ? Optional.empty()
                : readNode(current, DEPTH, reader.apply(key(current.sha256()))).tombstone();
        Proof proof = new Proof(binding, batchId, tombstone, siblings);
        verify(root, batchId, proof);
        return proof;
    }

    public CompletionStage<Proof> readProofAsync(
            Root root, Sha256Digest batchId, Function<String, CompletionStage<Optional<CanonicalBytes>>> reader) {
        requireDigest(batchId);
        requireRoot(root, 0);
        List<Root> siblings = new ArrayList<>(DEPTH);
        byte[] path = batchId.bytes().toByteArray();
        CompletionStage<Root> next = CompletableFuture.completedFuture(root);
        for (int depth = 0; depth < DEPTH; depth++) {
            int level = depth;
            next = next.thenCompose(current -> {
                requireRoot(current, level);
                if (current.count() == 0) {
                    siblings.add(empty[level + 1]);
                    return CompletableFuture.completedFuture(empty[level + 1]);
                }
                return reader.apply(key(current.sha256())).thenApply(bytes -> {
                    Node node = readNode(current, level, bytes);
                    boolean right = bit(path, level);
                    siblings.add(right ? node.left() : node.right());
                    return right ? node.right() : node.left();
                });
            });
        }
        return next.thenCompose(last -> last.count() == 0
                        ? CompletableFuture.completedFuture(Optional.<RetiredSourceRetirementBatchTombstoneV1>empty())
                        : reader.apply(key(last.sha256()))
                                .thenApply(bytes -> readNode(last, DEPTH, bytes).tombstone()))
                .thenApply(tombstone -> {
                    Proof proof = new Proof(binding, batchId, tombstone, siblings);
                    verify(root, batchId, proof);
                    return proof;
                });
    }

    public Insertion insert(Root root, RetiredSourceRetirementBatchTombstoneV1 tombstone, Proof proof) {
        verify(root, tombstone.batchIdSha256(), proof);
        if (proof.tombstone().isPresent()
                || !proof.batchId().equals(tombstone.batchIdSha256())
                || !tombstone.binding().equals(binding)) {
            throw new IllegalArgumentException("retired history insertion reuses a BatchId or differs from its proof");
        }
        List<NodeWrite> nodes = new ArrayList<>(MAX_INSERT_NODES);
        NodeWrite current = leaf(tombstone);
        nodes.add(current);
        byte[] path = proof.batchId().bytes().toByteArray();
        for (int depth = DEPTH - 1; depth >= 0; depth--) {
            Root sibling = proof.siblings().get(depth);
            current =
                    bit(path, depth) ? branch(depth, sibling, current.root()) : branch(depth, current.root(), sibling);
            nodes.add(current);
        }
        return new Insertion(root, current.root(), proof, nodes);
    }

    private Root combine(int depth, Root left, Root right) {
        return left.count() == 0 && right.count() == 0
                ? empty[depth]
                : branch(depth, left, right).root();
    }

    private NodeWrite branch(int depth, Root left, Root right) {
        requireRoot(left, depth + 1);
        requireRoot(right, depth + 1);
        long count = Math.addExact(left.count(), right.count());
        if (count == 0) {
            throw new IllegalArgumentException("empty history branch is not canonical");
        }
        CanonicalBytes bytes = encode(out -> {
            preamble(out, 1, depth);
            writeRoot(out, left);
            writeRoot(out, right);
        });
        return new NodeWrite(new Root(Sha256Digest.hash(bytes), count), bytes);
    }

    private NodeWrite leaf(RetiredSourceRetirementBatchTombstoneV1 tombstone) {
        if (!tombstone.binding().equals(binding)) {
            throw new IllegalArgumentException("history tombstone belongs to another Binding");
        }
        CanonicalBytes tombstoneBytes = M5RetentionCodecV1.encodeRetiredBatch(tombstone);
        CanonicalBytes bytes = encode(out -> {
            preamble(out, 2, DEPTH);
            out.writeInt(tombstoneBytes.length());
            out.write(tombstoneBytes.toByteArray());
        });
        return new NodeWrite(new Root(Sha256Digest.hash(bytes), 1), bytes);
    }

    private Node readNode(Root expected, int depth, Optional<CanonicalBytes> stored) {
        CanonicalBytes bytes = stored.orElseThrow(() -> new IllegalStateException("retired history node is missing"));
        if (bytes.length() > MAX_NODE_BYTES || !Sha256Digest.hash(bytes).equals(expected.sha256())) {
            throw new IllegalStateException("retired history node content address differs");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (input.readInt() != MAGIC || input.readUnsignedByte() != 2) {
                throw new IllegalArgumentException("retired history node wire differs");
            }
            int type = input.readUnsignedByte();
            int level = input.readUnsignedShort();
            if (level != depth
                    || !readDigest(input).equals(binding.bindingId().digest())
                    || !readDigest(input).equals(binding.incarnationSha256())
                    || !readDigest(input).equals(binding.storageEpochSha256())) {
                throw new IllegalArgumentException("retired history node Binding or depth differs");
            }
            Node node;
            NodeWrite canonical;
            if (type == 1 && depth < DEPTH) {
                Root left = readRoot(input);
                Root right = readRoot(input);
                canonical = branch(depth, left, right);
                node = new Node(depth, canonical.root(), left, right, Optional.empty());
            } else if (type == 2 && depth == DEPTH) {
                int size = input.readInt();
                if (size <= 0 || size > MAX_NODE_BYTES) {
                    throw new IllegalArgumentException("retired history leaf byte length exceeds bound");
                }
                var tombstone = M5RetentionCodecV1.decodeRetiredBatch(CanonicalBytes.copyOf(input.readNBytes(size)));
                canonical = leaf(tombstone);
                node = new Node(depth, canonical.root(), null, null, Optional.of(tombstone));
            } else {
                throw new IllegalArgumentException("retired history node type or depth differs");
            }
            if (input.read() != -1
                    || !canonical.bytes().equals(bytes)
                    || !node.root().equals(expected)) {
                throw new IllegalArgumentException("retired history node count or canonical bytes differ");
            }
            return node;
        } catch (IOException failure) {
            throw new IllegalArgumentException("retired history node is truncated", failure);
        }
    }

    private void requireRoot(Root root, int depth) {
        Objects.requireNonNull(root, "root");
        if (depth < 0
                || depth > DEPTH
                || root.count() == 0 && !root.equals(empty[depth])
                || depth == DEPTH && root.count() > 1) {
            throw new IllegalArgumentException("retired history empty subtree, count or depth differs");
        }
    }

    private void preamble(DataOutputStream out, int type, int depth) throws IOException {
        out.writeInt(MAGIC);
        out.writeByte(2);
        out.writeByte(type);
        out.writeShort(depth);
        out.write(binding.bindingId().digest().bytes().toByteArray());
        out.write(binding.incarnationSha256().bytes().toByteArray());
        out.write(binding.storageEpochSha256().bytes().toByteArray());
    }

    private static boolean bit(byte[] key, int depth) {
        return (key[depth >>> 3] & (1 << (7 - (depth & 7)))) != 0;
    }

    private static void writeRoot(DataOutputStream out, Root root) throws IOException {
        out.write(root.sha256().bytes().toByteArray());
        out.writeLong(root.count());
    }

    private static Root readRoot(DataInputStream input) throws IOException {
        return new Root(readDigest(input), input.readLong());
    }

    private static Sha256Digest readDigest(DataInputStream input) throws IOException {
        return Sha256Digest.copyOf(input.readNBytes(Sha256Digest.LENGTH));
    }

    private static void requireDigest(Sha256Digest digest) {
        if (Objects.requireNonNull(digest, "digest").isZero()) {
            throw new IllegalArgumentException("zero retired history identity");
        }
    }

    private static CanonicalBytes encode(Writer writer) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(buffer)) {
                writer.write(out);
            }
            return CanonicalBytes.copyOf(buffer.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("in-memory retired history encoding failed", failure);
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream out) throws IOException;
    }
}
