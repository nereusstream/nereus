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

import com.nereusstream.domain.aggregate.StorageProfileV1;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.PulsarProtocolCellIdentity;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.registry.BookKeeperInstanceIdV1;
import com.nereusstream.domain.registry.LedgerIdCompatibilityNamespaceV1;
import com.nereusstream.domain.registry.Rae1RegistryAdmissionEvidenceCodecV1;
import com.nereusstream.domain.registry.RegistryWriterKindV1;
import com.nereusstream.domain.registry.allocator.AllocatorModeV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerAllocatorHeadV1;
import com.nereusstream.domain.registry.allocator.ManagedLedgerIncarnationIdV1;
import com.nereusstream.metadata.oxia.v2.OperationAdmission;
import com.nereusstream.metadata.oxia.v2.allocator.OxiaVirtualLedgerAllocatorKeys;
import com.nereusstream.metadata.oxia.v2.allocator.OxiaVirtualLedgerAllocatorStore;
import com.nereusstream.metadata.oxia.v2.codec.OxiaV2CodecSet;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.ConditionalMutationEngine;
import com.nereusstream.metadata.oxia.v2.mutation.MutationFailureClassifier;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.allocator.ProductionVirtualLedgerAllocator;
import com.nereusstream.metadata.spi.allocator.ProductionVirtualLedgerAllocator.Range64RuntimeArtifacts;
import com.nereusstream.metadata.spi.allocator.VersionedAllocatorCellStateV1;
import com.nereusstream.metadata.spi.allocator.VersionedManagedLedgerAllocatorHeadV1;
import com.nereusstream.metadata.spi.model.ConditionalCasOutcome;
import com.nereusstream.metadata.spi.model.VersionedVirtualLedgerSliceViewV1;
import com.nereusstream.pulsar.offload.PulsarNativeBindingV1;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.FixedSlice;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.NvBindingContext;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.ProductionNvAllocatorHeadAdapter;
import com.nereusstream.pulsar.offload.objectwal.PulsarVirtualLedgerChainControllerV1.PulsarBindingKey;
import io.oxia.client.api.AsyncOxiaClient;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import org.apache.pulsar.broker.storage.nereus.v2.NereusPulsarTopicAuthority;

/** Resolves an existing exact Registry and allocator Cell; a Broker does not provision either one. */
final class PulsarM6ObjectWalAllocator {
    private final String root;
    private final PulsarProtocolCellIdentity cell;
    private final BookKeeperInstanceIdV1 bookKeeper;
    private final Sha256Digest namespace;
    private final PulsarM6ObjectWalNamespace.Locked locked;
    private final Sha256Digest admittedRegistry;
    private final Sha256Digest nativeBookKeeperJar;
    private final Sha256Digest virtualWriterQualification;
    private final OxiaV2AuthorityKeys keys;
    private final Oxia09ExactMetadataTransactionStoreV1 exact;
    private final OperationAdmission continuity;
    private final OxiaVirtualLedgerAllocatorStore store;
    private final ProductionVirtualLedgerAllocator allocator;

    PulsarM6ObjectWalAllocator(
            AsyncOxiaClient oxia,
            OperationAdmission continuity,
            String root,
            PulsarProtocolCellIdentity cell,
            PulsarM6ObjectWalNamespace.Locked actualBookKeeper,
            Sha256Digest admittedRegistry,
            Sha256Digest nativeBookKeeperJar,
            CanonicalBytes selection,
            Range64RuntimeArtifacts artifacts) {
        this.root = root;
        this.continuity = continuity;
        this.cell = cell;
        this.locked = actualBookKeeper;
        this.bookKeeper = actualBookKeeper.instance();
        this.admittedRegistry = admittedRegistry;
        this.nativeBookKeeperJar = nativeBookKeeperJar;
        virtualWriterQualification = virtualWriterQualification(artifacts);
        namespace = LedgerIdCompatibilityNamespaceV1.derive(bookKeeper);
        keys = new OxiaV2AuthorityKeys(root);
        exact = new Oxia09ExactMetadataTransactionStoreV1(oxia);
        var conditional = new AsyncOxiaConditionalClient(oxia);
        var mutation = new ConditionalMutationEngine(conditional, new MutationFailureClassifier());
        store = new OxiaVirtualLedgerAllocatorStore(
                continuity, new OxiaVirtualLedgerAllocatorKeys(root), conditional, mutation);
        allocator = ProductionVirtualLedgerAllocator.activateFrozenRange64(selection, artifacts, store);
    }

    static Sha256Digest virtualWriterPrincipal(PulsarProtocolCellIdentity cell) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(
                        "native-object-writer/" + cell.cellId().value().toHex())
                .bytes());
    }

    static Sha256Digest virtualWriterQualification(Range64RuntimeArtifacts artifacts) {
        return Sha256Digest.hash(CanonicalUtf8.fromString(String.join(
                        "/",
                        artifacts.domain().toHex(),
                        artifacts.metadataSpi().toHex(),
                        artifacts.metadataOxia().toHex()))
                .bytes());
    }

    record Scope(
            VersionedVirtualLedgerSliceViewV1 slice,
            VersionedAllocatorCellStateV1 allocatorCell,
            FixedSlice geometry,
            ProductionVirtualLedgerAllocator allocator,
            OxiaVirtualLedgerAllocatorStore store) {}

    record BoundChain(
            PulsarBindingKey binding,
            ManagedLedgerIncarnationIdV1 incarnation,
            PulsarM6ObjectWalOwner owner,
            PulsarVirtualLedgerChainControllerV1 controller) {}

    CompletionStage<BoundChain> openBinding(
            PulsarNativeBindingV1 nativeBinding,
            NereusPulsarTopicAuthority authority,
            BiFunction<PulsarM6ObjectWalOwner, VersionedManagedLedgerAllocatorHeadV1, CompletionStage<Void>>
                    recoverPredecessor) {
        var aggregate = nativeBinding.authority().aggregate();
        var topic = aggregate.binding();
        var epoch = aggregate.initialEpoch();
        if (!topic.cellIdentity().equals(cell)
                || epoch.storageProfile() != StorageProfileV1.OBJECT_WAL
                || !(topic.incarnationIdentity() instanceof PulsarTopicIncarnationIdentity)) {
            throw new IllegalArgumentException("Object WAL binding differs from the admitted Cell");
        }
        var incarnation = incarnation(nativeBinding);
        var binding = bindingKey(nativeBinding);
        var ownerKey = root + "/native-object-owner/" + incarnation.value().toHex();
        return authority.requestRefresh().thenCompose(unused -> open()).thenCompose(scope -> readHead(
                        scope, incarnation)
                .thenCompose(head -> {
                    var initial = ManagedLedgerAllocatorHeadV1.initial(
                            incarnation, 1, head.value().nextLedgerId());
                    return PulsarM6ObjectWalOwner.acquire(
                                    exact, ownerKey, authority, head.value().equals(initial))
                            .thenCompose(owner -> recoverPredecessor
                                    .apply(owner, head)
                                    .thenCompose(unused -> selectHeadOwner(owner, scope, head)
                                            .thenApply(ignored -> {
                                                var descriptors = new PulsarM6ObjectWalLedgerDescriptors(
                                                        exact,
                                                        root,
                                                        scope,
                                                        incarnation,
                                                        binding,
                                                        owner::requireCurrent);
                                                var adapter = new ProductionNvAllocatorHeadAdapter(
                                                        scope.geometry(),
                                                        allocator,
                                                        store,
                                                        requested -> {
                                                            if (!requested.equals(binding)) {
                                                                throw new IllegalArgumentException(
                                                                        "Foreign Object WAL binding");
                                                            }
                                                            return owner.requireCurrent()
                                                                    .thenCompose(v -> readSlice())
                                                                    .thenApply(slice -> new NvBindingContext(
                                                                            binding, slice, incarnation));
                                                        },
                                                        descriptors,
                                                        (requested, ownerEpoch, original, slice) ->
                                                                requestId(incarnation, original));
                                                return new BoundChain(
                                                        binding,
                                                        incarnation,
                                                        owner,
                                                        new PulsarVirtualLedgerChainControllerV1(
                                                                scope.geometry(), adapter, adapter));
                                            })));
                }));
    }

    static PulsarBindingKey bindingKey(PulsarNativeBindingV1 nativeBinding) {
        var aggregate = nativeBinding.authority().aggregate();
        var topic = aggregate.binding();
        var cell = (PulsarProtocolCellIdentity) topic.cellIdentity();
        return new PulsarBindingKey(
                cell.cellId().value().toHex(),
                topic.bindingId().digest().toHex(),
                incarnation(nativeBinding).value().toHex(),
                aggregate.initialEpoch().storageEpochId().digest().toHex(),
                "PULSAR_LEDGER_ENTRY");
    }

    static ManagedLedgerIncarnationIdV1 incarnation(PulsarNativeBindingV1 nativeBinding) {
        var binding = nativeBinding.authority().aggregate().binding().bindingId();
        return new ManagedLedgerIncarnationIdV1(
                Sha256Digest.hash(CanonicalUtf8.fromString("NEREUS_NATIVE_OBJECT_LEDGER_INCARNATION_V1/"
                                + binding.digest().toHex())
                        .bytes()));
    }

    private CompletionStage<VersionedManagedLedgerAllocatorHeadV1> readHead(
            Scope scope, ManagedLedgerIncarnationIdV1 incarnation) {
        var slice = scope.slice().value().assignment();
        return store.readHead(namespace, slice.sliceAssignmentId(), incarnation)
                .thenApply(found -> found.orElseThrow(
                        () -> new IllegalStateException("Object WAL binding lacks its provisioned Head")));
    }

    private CompletionStage<Void> selectHeadOwner(
            PulsarM6ObjectWalOwner owner, Scope scope, VersionedManagedLedgerAllocatorHeadV1 predecessor) {
        if (owner.epoch() < predecessor.value().ownerEpoch()) {
            throw new IllegalStateException("Object WAL owner predates the durable Head");
        }
        return owner.requireCurrent()
                .thenCompose(unused -> {
                    if (owner.epoch() == predecessor.value().ownerEpoch()) {
                        return readHead(scope, predecessor.value().managedLedgerIncarnation())
                                .thenAccept(current -> {
                                    if (!current.equals(predecessor)) {
                                        throw new IllegalStateException(
                                                "Object WAL Head changed during owner recovery");
                                    }
                                });
                    }
                    return open().thenCompose(
                                    actual -> allocator.takeover(actual.allocatorCell(), predecessor, owner.epoch()))
                            .thenAccept(outcome -> {
                                if (outcome.outcome() != ConditionalCasOutcome.APPLIED_EXACT
                                        && outcome.outcome() != ConditionalCasOutcome.PREDECESSOR_UNCHANGED) {
                                    throw new IllegalStateException("Object WAL Head takeover remains unresolved");
                                }
                                var selected =
                                        outcome.exactSnapshot().orElseThrow().value();
                                if (selected.ownerEpoch() != owner.epoch()
                                        || !selected.allocationState()
                                                .equals(predecessor.value().allocationState())) {
                                    throw new IllegalStateException(
                                            "Object WAL takeover changed its predecessor chain");
                                }
                            });
                })
                .thenCompose(unused -> owner.requireCurrent());
    }

    private static Sha256Digest requestId(
            ManagedLedgerIncarnationIdV1 incarnation, VersionedManagedLedgerAllocatorHeadV1 original) {
        var head = original.value();
        return Sha256Digest.hash(CanonicalUtf8.fromString(String.join(
                        "/",
                        "NEREUS_NATIVE_OBJECT_RANGE_V1",
                        incarnation.value().toHex(),
                        Long.toString(head.grantId()),
                        Long.toString(head.nextLedgerId()),
                        head.visibleChainHead().nodeId().toHex()))
                .bytes());
    }

    CompletionStage<VersionedAllocatorCellStateV1> createInitialCell(VersionedVirtualLedgerSliceViewV1 slice) {
        return allocator.createCell(slice).thenApply(result -> result.exactSnapshot()
                .orElseThrow());
    }

    CompletionStage<VersionedManagedLedgerAllocatorHeadV1> createInitialHead(
            VersionedAllocatorCellStateV1 initial,
            VersionedVirtualLedgerSliceViewV1 slice,
            ManagedLedgerIncarnationIdV1 incarnation) {
        return allocator.createHead(initial, slice, incarnation, 1).thenApply(result -> result.exactSnapshot()
                .orElseThrow());
    }

    CompletionStage<Scope> open() {
        return readSlice().thenCompose(slice -> {
            var assignment = slice.value().assignment();
            return store.readCell(namespace, assignment.sliceAssignmentId()).thenApply(found -> {
                var actual = found.orElseThrow(() -> new IllegalStateException("Object WAL allocator Cell is absent"));
                if (actual.value().mode() != AllocatorModeV1.RANGE_LEASED
                        || !actual.value().ledgerIdCompatibilityNamespaceId().equals(namespace)
                        || !actual.value().sliceAssignmentId().equals(assignment.sliceAssignmentId())
                        || actual.value().sliceStartInclusive() != assignment.startInclusive()
                        || actual.value().sliceEndInclusive() != assignment.endInclusive()) {
                    throw new IllegalStateException("Object WAL allocator differs from the admitted Registry slice");
                }
                return new Scope(
                        slice,
                        actual,
                        new FixedSlice(assignment.startInclusive(), assignment.endInclusive()),
                        allocator,
                        store);
            });
        });
    }

    CompletionStage<VersionedVirtualLedgerSliceViewV1> readSlice() {
        continuity.requireOpen();
        String key = keys.registryKey(cell.deploymentId(), cell.reservationDomainId(), namespace);
        return exact.read(key).thenCompose(found -> {
            continuity.requireOpen();
            var stored = found.orElseThrow(() -> new IllegalStateException("Object WAL Registry is absent"));
            var installed = OxiaV2CodecSet.productionR1()
                    .registry()
                    .decode(
                            key,
                            cell.deploymentId(),
                            cell.reservationDomainId(),
                            namespace,
                            stored.canonicalStoredBytes(),
                            stored.metadataVersion());
            var value = installed.value().domainValue();
            if (!installed.value().canonicalStoredDigest().equals(admittedRegistry)
                    || !value.instanceId().equals(bookKeeper)) {
                throw new IllegalStateException("Object WAL Registry differs from its admitted BookKeeper namespace");
            }
            var reference = value.admissionEvidence();
            return exact.read(keys.registryAdmissionEvidenceKey(reference.digest()))
                    .thenApply(evidence -> {
                        continuity.requireOpen();
                        var bytes = evidence.orElseThrow(() ->
                                        new IllegalStateException("Object WAL Registry admission evidence is absent"))
                                .canonicalStoredBytes();
                        var receipt = new Rae1RegistryAdmissionEvidenceCodecV1().decode(bytes);
                        if (!receipt.reference().equals(reference)
                                || !receipt.deploymentId().equals(cell.deploymentId())
                                || !receipt.reservationDomainId().equals(cell.reservationDomainId())
                                || !receipt.instanceId().equals(bookKeeper)
                                || receipt.candidateRegistryEpoch() != value.registryEpoch()
                                || !receipt.candidateWriterRows().equals(value.writers())
                                || !receipt.adminInterlockDigest().equals(locked.proof())
                                || receipt.admittedWriters().size() != 2
                                || receipt.admittedWriters().stream()
                                        .anyMatch(writer -> writer.interlockGeneration() != locked.generation()
                                                || !writer.interlockDigest().equals(locked.proof()))
                                || receipt.admittedWriters().stream()
                                        .noneMatch(writer -> writer.writerKind()
                                                        == RegistryWriterKindV1.NATIVE_BOOKKEEPER_LEDGER_ID
                                                && writer.principalDigest().equals(locked.nativePrincipal())
                                                && writer.sourceQualificationDigest()
                                                        .equals(nativeBookKeeperJar))
                                || receipt.admittedWriters().stream()
                                        .noneMatch(writer -> writer.writerKind()
                                                        == RegistryWriterKindV1.NEREUS_VIRTUAL_LEDGER_ID
                                                && writer.principalDigest().equals(virtualWriterPrincipal(cell))
                                                && writer.sourceQualificationDigest()
                                                        .equals(virtualWriterQualification))) {
                            throw new IllegalStateException("Object WAL Registry admission evidence differs");
                        }
                        var slice = installed.sliceView(cell.cellId());
                        if (!slice.value().allocationAllowed()) {
                            throw new IllegalStateException("Object WAL Registry has no active slice for this Cell");
                        }
                        return slice;
                    });
        });
    }
}
