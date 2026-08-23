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

package com.nereusstream.metadata.oxia.v2.allocator;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.registry.allocator.AllocatorWireV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCandidateNodeV1;
import com.nereusstream.domain.registry.allocator.VirtualLedgerCellAllocatorStateV1;
import com.nereusstream.metadata.oxia.v2.OperationAdmission;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.ExactRecordResolver;
import com.nereusstream.metadata.oxia.v2.mutation.MetadataVersionMapper;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.spi.allocator.PulsarVirtualLedgerAllocatorStore;
import com.nereusstream.metadata.spi.allocator.VersionedAllocatorCellStateV1;
import com.nereusstream.metadata.spi.allocator.VersionedManagedLedgerAllocatorHeadV1;
import com.nereusstream.metadata.spi.allocator.VersionedVirtualLedgerCandidateNodeV1;
import com.nereusstream.metadata.spi.model.ConditionalCasResult;
import com.nereusstream.metadata.spi.model.CreateMutationResult;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Oxia production adapter with same-key exact reread reconciliation after every dispatched conditional mutation. */
public final class OxiaVirtualLedgerAllocatorStore implements PulsarVirtualLedgerAllocatorStore {
    private final OperationAdmission admission;
    private final OxiaVirtualLedgerAllocatorKeys keys;
    private final OxiaConditionalClient client;
    private final ConditionalMutationEngine mutationEngine;

    public OxiaVirtualLedgerAllocatorStore(
            OperationAdmission admission,
            OxiaVirtualLedgerAllocatorKeys keys,
            OxiaConditionalClient client,
            ConditionalMutationEngine mutationEngine) {
        this.admission = Objects.requireNonNull(admission, "admission");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.client = Objects.requireNonNull(client, "client");
        this.mutationEngine = Objects.requireNonNull(mutationEngine, "mutationEngine");
    }

    @Override
    public CompletionStage<Optional<VersionedAllocatorCellStateV1>> readCell(
            Sha256Digest namespaceId, Sha256Digest sliceAssignmentId) {
        return AdapterFutures.localValidation(() -> {
            admission.requireOpen();
            String key = keys.cellKey(namespaceId, sliceAssignmentId);
            return client.read(key)
                    .thenApply(record -> record.map(value -> decodeCell(key, namespaceId, sliceAssignmentId, value)));
        });
    }

    @Override
    public CompletionStage<CreateMutationResult<VersionedAllocatorCellStateV1>> createCell(
            VirtualLedgerCellAllocatorStateV1 candidate) {
        return AdapterFutures.localValidation(() -> {
            admission.requireOpen();
            Objects.requireNonNull(candidate, "candidate");
            String key = keys.cellKey(candidate.ledgerIdCompatibilityNamespaceId(), candidate.sliceAssignmentId());
            CanonicalBytes bytes = AllocatorWireV1.encodeCell(candidate);
            return mutationEngine.create(key, bytes, cellResolver(key, candidate, null));
        });
    }

    @Override
    public CompletionStage<ConditionalCasResult<VersionedAllocatorCellStateV1>> compareAndSetCell(
            VersionedAllocatorCellStateV1 exactPredecessor, VirtualLedgerCellAllocatorStateV1 candidate) {
        return AdapterFutures.localValidation(() -> {
            admission.requireOpen();
            Objects.requireNonNull(exactPredecessor, "exactPredecessor");
            Objects.requireNonNull(candidate, "candidate");
            requireSameCell(exactPredecessor.value(), candidate);
            String key = keys.cellKey(candidate.ledgerIdCompatibilityNamespaceId(), candidate.sliceAssignmentId());
            return mutationEngine.compareAndSet(
                    key,
                    AllocatorWireV1.encodeCell(candidate),
                    MetadataVersionMapper.toOxia(exactPredecessor.metadataVersion()),
                    cellResolver(key, candidate, exactPredecessor));
        });
    }

    @Override
    public CompletionStage<Optional<VersionedManagedLedgerAllocatorHeadV1>> readHead(
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            ManagedLedgerIncarnationIdV1 managedLedgerIncarnation) {
        return AdapterFutures.localValidation(() -> {
            admission.requireOpen();
            String key = keys.headKey(namespaceId, sliceAssignmentId, managedLedgerIncarnation);
            return client.read(key)
                    .thenApply(record -> record.map(
                            value -> decodeHead(key, namespaceId, sliceAssignmentId, managedLedgerIncarnation, value)));
        });
    }

    @Override
    public CompletionStage<CreateMutationResult<VersionedManagedLedgerAllocatorHeadV1>> createHead(
            Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, ManagedLedgerAllocatorHeadV1 candidate) {
        return AdapterFutures.localValidation(() -> {
            admission.requireOpen();
            Objects.requireNonNull(candidate, "candidate");
            String key = keys.headKey(namespaceId, sliceAssignmentId, candidate.managedLedgerIncarnation());
            CanonicalBytes bytes = AllocatorWireV1.encodeHead(candidate);
            return mutationEngine.create(
                    key, bytes, headResolver(key, namespaceId, sliceAssignmentId, candidate, null));
        });
    }

    @Override
    public CompletionStage<ConditionalCasResult<VersionedManagedLedgerAllocatorHeadV1>> compareAndSetHead(
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            VersionedManagedLedgerAllocatorHeadV1 exactPredecessor,
            ManagedLedgerAllocatorHeadV1 candidate) {
        return AdapterFutures.localValidation(() -> {
            admission.requireOpen();
            Objects.requireNonNull(exactPredecessor, "exactPredecessor");
            Objects.requireNonNull(candidate, "candidate");
            if (!exactPredecessor.value().managedLedgerIncarnation().equals(candidate.managedLedgerIncarnation())) {
                throw new IllegalArgumentException("allocator Head predecessor and candidate incarnations differ");
            }
            String key = keys.headKey(namespaceId, sliceAssignmentId, candidate.managedLedgerIncarnation());
            return mutationEngine.compareAndSet(
                    key,
                    AllocatorWireV1.encodeHead(candidate),
                    MetadataVersionMapper.toOxia(exactPredecessor.metadataVersion()),
                    headResolver(key, namespaceId, sliceAssignmentId, candidate, exactPredecessor));
        });
    }

    @Override
    public CompletionStage<Optional<VersionedVirtualLedgerCandidateNodeV1>> readNode(
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            ManagedLedgerIncarnationIdV1 managedLedgerIncarnation,
            long ledgerId) {
        return AdapterFutures.localValidation(() -> {
            admission.requireOpen();
            String key = keys.nodeKey(namespaceId, sliceAssignmentId, managedLedgerIncarnation, ledgerId);
            return client.read(key)
                    .thenApply(record -> record.map(value -> decodeNode(
                            key, namespaceId, sliceAssignmentId, managedLedgerIncarnation, ledgerId, value)));
        });
    }

    @Override
    public CompletionStage<CreateMutationResult<VersionedVirtualLedgerCandidateNodeV1>> createNode(
            Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, VirtualLedgerCandidateNodeV1 candidate) {
        return AdapterFutures.localValidation(() -> {
            admission.requireOpen();
            Objects.requireNonNull(candidate, "candidate");
            String key = keys.nodeKey(
                    namespaceId, sliceAssignmentId, candidate.managedLedgerIncarnation(), candidate.ledgerId());
            CanonicalBytes bytes = AllocatorWireV1.encodeNode(candidate);
            return mutationEngine.create(key, bytes, nodeResolver(key, namespaceId, sliceAssignmentId, candidate));
        });
    }

    private ExactRecordResolver<VersionedAllocatorCellStateV1> cellResolver(
            String key, VirtualLedgerCellAllocatorStateV1 candidate, VersionedAllocatorCellStateV1 predecessor) {
        return new ExactRecordResolver<>() {
            @Override
            public VersionedAllocatorCellStateV1 decode(AuthorityRecord record) {
                return decodeCell(
                        key, candidate.ledgerIdCompatibilityNamespaceId(), candidate.sliceAssignmentId(), record);
            }

            @Override
            public boolean isCandidateExact(VersionedAllocatorCellStateV1 snapshot) {
                return snapshot.value().equals(candidate);
            }

            @Override
            public boolean isPredecessorExact(VersionedAllocatorCellStateV1 snapshot) {
                return predecessor != null && snapshot.equals(predecessor);
            }
        };
    }

    private ExactRecordResolver<VersionedManagedLedgerAllocatorHeadV1> headResolver(
            String key,
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            ManagedLedgerAllocatorHeadV1 candidate,
            VersionedManagedLedgerAllocatorHeadV1 predecessor) {
        return new ExactRecordResolver<>() {
            @Override
            public VersionedManagedLedgerAllocatorHeadV1 decode(AuthorityRecord record) {
                return decodeHead(key, namespaceId, sliceAssignmentId, candidate.managedLedgerIncarnation(), record);
            }

            @Override
            public boolean isCandidateExact(VersionedManagedLedgerAllocatorHeadV1 snapshot) {
                return snapshot.value().equals(candidate);
            }

            @Override
            public boolean isPredecessorExact(VersionedManagedLedgerAllocatorHeadV1 snapshot) {
                return predecessor != null && snapshot.equals(predecessor);
            }
        };
    }

    private ExactRecordResolver<VersionedVirtualLedgerCandidateNodeV1> nodeResolver(
            String key,
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            VirtualLedgerCandidateNodeV1 candidate) {
        return new ExactRecordResolver<>() {
            @Override
            public VersionedVirtualLedgerCandidateNodeV1 decode(AuthorityRecord record) {
                return decodeNode(
                        key,
                        namespaceId,
                        sliceAssignmentId,
                        candidate.managedLedgerIncarnation(),
                        candidate.ledgerId(),
                        record);
            }

            @Override
            public boolean isCandidateExact(VersionedVirtualLedgerCandidateNodeV1 snapshot) {
                return snapshot.value().equals(candidate);
            }

            @Override
            public boolean isPredecessorExact(VersionedVirtualLedgerCandidateNodeV1 snapshot) {
                return false;
            }
        };
    }

    private VersionedAllocatorCellStateV1 decodeCell(
            String key, Sha256Digest namespaceId, Sha256Digest sliceAssignmentId, AuthorityRecord record) {
        requireRecordKey(key, record);
        VirtualLedgerCellAllocatorStateV1 decoded = AllocatorWireV1.decodeCell(record.storedBytes());
        if (!decoded.ledgerIdCompatibilityNamespaceId().equals(namespaceId)
                || !decoded.sliceAssignmentId().equals(sliceAssignmentId)
                || !key.equals(keys.cellKey(namespaceId, sliceAssignmentId))) {
            throw new IllegalArgumentException("allocator Cell key and value identities differ");
        }
        return new VersionedAllocatorCellStateV1(decoded, MetadataVersionMapper.fromOxia(record.versionId()));
    }

    private VersionedManagedLedgerAllocatorHeadV1 decodeHead(
            String key,
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            ManagedLedgerIncarnationIdV1 incarnation,
            AuthorityRecord record) {
        requireRecordKey(key, record);
        ManagedLedgerAllocatorHeadV1 decoded = AllocatorWireV1.decodeHead(record.storedBytes());
        if (!decoded.managedLedgerIncarnation().equals(incarnation)
                || !key.equals(keys.headKey(namespaceId, sliceAssignmentId, incarnation))) {
            throw new IllegalArgumentException("allocator Head key and value identities differ");
        }
        return new VersionedManagedLedgerAllocatorHeadV1(decoded, MetadataVersionMapper.fromOxia(record.versionId()));
    }

    private VersionedVirtualLedgerCandidateNodeV1 decodeNode(
            String key,
            Sha256Digest namespaceId,
            Sha256Digest sliceAssignmentId,
            ManagedLedgerIncarnationIdV1 incarnation,
            long ledgerId,
            AuthorityRecord record) {
        requireRecordKey(key, record);
        VirtualLedgerCandidateNodeV1 decoded = AllocatorWireV1.decodeNode(record.storedBytes());
        if (!decoded.managedLedgerIncarnation().equals(incarnation)
                || decoded.ledgerId() != ledgerId
                || !key.equals(keys.nodeKey(namespaceId, sliceAssignmentId, incarnation, ledgerId))) {
            throw new IllegalArgumentException("allocator node key and value identities differ");
        }
        return new VersionedVirtualLedgerCandidateNodeV1(decoded, MetadataVersionMapper.fromOxia(record.versionId()));
    }

    private static void requireSameCell(
            VirtualLedgerCellAllocatorStateV1 predecessor, VirtualLedgerCellAllocatorStateV1 candidate) {
        if (!predecessor.ledgerIdCompatibilityNamespaceId().equals(candidate.ledgerIdCompatibilityNamespaceId())
                || !predecessor.sliceAssignmentId().equals(candidate.sliceAssignmentId())
                || predecessor.mode() != candidate.mode()
                || predecessor.allocatorProtocolVersion() != candidate.allocatorProtocolVersion()
                || predecessor.sliceStartInclusive() != candidate.sliceStartInclusive()
                || predecessor.sliceEndInclusive() != candidate.sliceEndInclusive()) {
            throw new IllegalArgumentException("allocator Cell immutable identity/geometry changed");
        }
    }

    private static void requireRecordKey(String expectedKey, AuthorityRecord record) {
        if (!expectedKey.equals(record.key())) {
            throw new IllegalArgumentException("allocator exact read returned a different authority key");
        }
    }
}
