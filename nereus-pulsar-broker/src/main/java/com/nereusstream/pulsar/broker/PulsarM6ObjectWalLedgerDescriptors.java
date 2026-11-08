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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.ChainPointerV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.metadata.spi.allocator.VersionedManagedLedgerAllocatorHeadV1;
import com.nereusstream.metadata.spi.allocator.VersionedVirtualLedgerCandidateNodeV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.LedgerNode;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.NvLedgerDescriptorAuthority;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.ResolvedNvHead;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Immutable descriptor lookup. Visibility and ordering still come from the exact NVAH1/NVAN1 chain. */
final class PulsarM6ObjectWalLedgerDescriptors implements NvLedgerDescriptorAuthority {
    private static final int MAGIC = 0x4e504c31; // NPL1
    private static final int BYTES = Integer.BYTES + Sha256Digest.LENGTH + 4 * Long.BYTES;
    private final ExactMetadataTransactionStoreV1 exact;
    private final String prefix;
    private final PulsarM6ObjectWalAllocator.Scope scope;
    private final ManagedLedgerIncarnationIdV1 incarnation;
    private final PulsarBindingKey binding;
    private final Sha256Digest bindingDigest;
    private final Supplier<CompletionStage<Void>> requireOwner;

    PulsarM6ObjectWalLedgerDescriptors(
            ExactMetadataTransactionStoreV1 exact,
            String root,
            PulsarM6ObjectWalAllocator.Scope scope,
            ManagedLedgerIncarnationIdV1 incarnation,
            PulsarBindingKey binding,
            Supplier<CompletionStage<Void>> requireOwner) {
        this.exact = exact;
        prefix = root + "/native-object-ledger-descriptors/";
        this.scope = scope;
        this.incarnation = incarnation;
        this.binding = binding;
        this.requireOwner = requireOwner;
        bindingDigest = Sha256Digest.hash(CanonicalUtf8.fromString(String.join(
                        "\0",
                        "NEREUS_OBJECT_MANAGED_LEDGER_BINDING_V1",
                        binding.protocolCellId(),
                        binding.topicBindingId(),
                        binding.topicIncarnation(),
                        binding.storageEpochId(),
                        binding.positionDomainId()))
                .bytes());
    }

    @Override
    public Sha256Digest requireDescriptorDigest(LedgerNode candidate) {
        return Sha256Digest.hash(encode(candidate));
    }

    @Override
    public CompletionStage<Void> persistCandidate(
            VersionedVirtualLedgerCandidateNodeV1 exactNode, LedgerNode candidate) {
        var node = exactNode.value();
        var bytes = encode(candidate);
        requireNode(exactNode, node.pointer(), candidate, bytes);
        String key = key(node.pointer());
        return requireOwner.get().thenCompose(unused -> exact.read(key)).thenCompose(stored -> {
            if (stored.isPresent()) {
                return requireExact(stored, bytes);
            }
            return exact.compareAndSet(Optional.empty(), key, bytes)
                    .thenCompose(unused -> exact.read(key))
                    .thenCompose(found -> requireExact(found, bytes));
        });
    }

    @Override
    public CompletionStage<ResolvedNvHead> resolveVisibleHead(
            PulsarBindingKey requested, VersionedManagedLedgerAllocatorHeadV1 head) {
        if (!binding.equals(requested) || !incarnation.equals(head.value().managedLedgerIncarnation())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Object WAL descriptor binding differs"));
        }
        return readPointer(head.value().visibleChainHead());
    }

    @Override
    public CompletionStage<ResolvedNvHead> resolvePredecessor(ResolvedNvHead child) {
        return readPointer(child.exactNode().value().expectedPredecessor());
    }

    private CompletionStage<ResolvedNvHead> readPointer(ChainPointerV1 pointer) {
        return exact.read(key(pointer)).thenCompose(found -> {
            var stored = found.orElseThrow(() -> new IllegalStateException("Object WAL node descriptor is absent"));
            var bytes = stored.canonicalStoredBytes();
            var logical = decode(bytes);
            var assignment = scope.slice().value().assignment();
            return scope.store()
                    .readNode(
                            assignment.ledgerIdCompatibilityNamespaceId(),
                            assignment.sliceAssignmentId(),
                            incarnation,
                            logical.virtualLedgerId())
                    .thenApply(actual -> {
                        var node =
                                actual.orElseThrow(() -> new IllegalStateException("Exact Object WAL NVAN1 is absent"));
                        requireNode(node, pointer, logical, bytes);
                        return new ResolvedNvHead(node, logical);
                    });
        });
    }

    private void requireNode(
            VersionedVirtualLedgerCandidateNodeV1 exactNode,
            ChainPointerV1 pointer,
            LedgerNode logical,
            CanonicalBytes bytes) {
        var node = exactNode.value();
        if (!node.pointer().equals(pointer)
                || !node.managedLedgerIncarnation().equals(incarnation)
                || node.ledgerId() != logical.virtualLedgerId()
                || node.creatorOwnerEpoch() != logical.ownerEpoch()
                || node.expectedPredecessor().isAbsent()
                        == logical.predecessorLedgerId().isPresent()
                || !node.ledgerDescriptorDigest().equals(Sha256Digest.hash(bytes))) {
            throw new IllegalStateException("Object WAL descriptor differs from the exact typed chain node");
        }
    }

    private static CompletionStage<Void> requireExact(
            Optional<ExactMetadataTransactionStoreV1.VersionedValue> stored, CanonicalBytes expected) {
        if (stored.isEmpty() || !stored.orElseThrow().canonicalStoredBytes().equals(expected)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Original Object WAL descriptor is unresolved"));
        }
        return CompletableFuture.completedFuture(null);
    }

    private String key(ChainPointerV1 pointer) {
        if (pointer.isAbsent()) {
            throw new IllegalArgumentException("Object WAL descriptor cannot open an absent chain pointer");
        }
        return prefix + pointer.nodeId().toHex();
    }

    private CanonicalBytes encode(LedgerNode node) {
        if (!node.binding().equals(binding)) {
            throw new IllegalArgumentException("Object WAL ledger descriptor names another Binding");
        }
        scope.geometry().requireContains(node.virtualLedgerId());
        return CanonicalBytes.copyOf(ByteBuffer.allocate(BYTES)
                .putInt(MAGIC)
                .put(bindingDigest.bytes().toByteArray())
                .putLong(node.virtualLedgerId())
                .putLong(node.predecessorLedgerId().orElse(-1))
                .putLong(node.predecessorTerminalEntryId())
                .putLong(node.ownerEpoch())
                .array());
    }

    private LedgerNode decode(CanonicalBytes bytes) {
        if (bytes.length() != BYTES) {
            throw new IllegalArgumentException("Object WAL ledger descriptor length differs");
        }
        var input = ByteBuffer.wrap(bytes.toByteArray());
        if (input.getInt() != MAGIC) {
            throw new IllegalArgumentException("Object WAL ledger descriptor version differs");
        }
        var identity = new byte[Sha256Digest.LENGTH];
        input.get(identity);
        if (!bindingDigest.equals(Sha256Digest.copyOf(identity))) {
            throw new IllegalArgumentException("Object WAL ledger descriptor binding differs");
        }
        long ledger = input.getLong();
        long previous = input.getLong();
        var node = new LedgerNode(
                binding,
                ledger,
                previous == -1 ? OptionalLong.empty() : OptionalLong.of(previous),
                input.getLong(),
                input.getLong());
        if (!encode(node).equals(bytes)) {
            throw new IllegalArgumentException("Object WAL ledger descriptor is not canonical");
        }
        return node;
    }
}
