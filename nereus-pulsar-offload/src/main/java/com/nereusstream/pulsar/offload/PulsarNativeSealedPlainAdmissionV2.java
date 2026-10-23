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
package com.nereusstream.pulsar.offload;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.bookkeeper.M5PulsarNativeCreateSpecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterEnrollmentV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1.RegisteredReferenceWriterV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.ExternalMutationOutcomeV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.ExternalMutationResultV1;
import com.nereusstream.storage.object.retention.M5ReferenceMutationGuardV1.GuardedMutationRequestV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceTargetKindV1;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.mledger.impl.MetaStoreImpl;

/** Owns one actual plain/offload admission; recovery never creates or retags a Native source. */
final class PulsarNativeSealedPlainAdmissionV2 {
    private static final int MAX_BYTES = 8_388_608;
    private final PulsarNativeBookKeeperCreateV1 creation;
    private final PulsarNativeBindingV1 binding;
    private final PulsarNativeReadCellBudgetV1 budget;
    private final MetaStoreImpl store;
    private final PulsarNativeReadCellBudgetV1.Reservation workspace;
    private final ProofBoundWriterEnrollmentV1 enrollment;
    private final List<TargetDeleteAuthorityV1> candidates = new ArrayList<>();

    private record SourceContext(
            OxiaQuotaTargetDeleteStoreV2 route,
            ProofBoundWriterEnrollmentV1 enrollment,
            Sha256Digest providerScope,
            Sha256Digest capability,
            java.util.Optional<Sha256Digest> priorScopeFacts) {}

    private final Map<PhysicalResourceIdV2, SourceContext> contexts = new java.util.HashMap<>();
    private final List<PulsarNativeOffloadedReferenceV2> sdkCaptures = new ArrayList<>();
    private final NereusPulsarLedgerOffloaderV1 offloader;
    private final S3PulsarOffloadObjectStoreV1.MaterializationConnection objectConnection;
    private final OxiaQuotaTargetDeleteStoreV2 objectRoute;
    private final PulsarNativeM4ControlV1 sourceControl;
    private PulsarNativeM4ControlV1.SelectedSourceSnapshot selection;
    private PulsarNativeM4SourceMapV1.FloorDirectory selectedDirectory;
    private PulsarNativeM4ControlV1.SelectedMaterializationSnapshot selectedMaterialization;
    private MetaStoreImpl.StoredMutationValue snapshot;
    private boolean missingFirstParent;
    private final Map<String, MetaStoreImpl.StoredMutationValue> cursorSnapshots = new java.util.TreeMap<>();
    private List<String> cursorNames = List.of();
    private int snapshotBytes;
    private boolean physicalStarted;
    private final M5ReferenceMutationGuardV1 aggregateGuard;
    private final RegisteredReferenceWriterV1 writer;
    private GuardedMutationRequestV1 request;
    private ExternalMutationResultV1 terminal;
    private final CompletableFuture<Void> original;
    private CompletableFuture<Void> cleanup;

    PulsarNativeSealedPlainAdmissionV2(
            PulsarNativeBookKeeperCreateV1 creation,
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            MetaStoreImpl store,
            Sha256Digest implementation,
            M5ReferenceMutationGuardV1 aggregateGuard,
            RegisteredReferenceWriterV1 writer,
            NereusPulsarLedgerOffloaderV1 offloader,
            S3PulsarOffloadObjectStoreV1.MaterializationConnection objectConnection,
            OxiaQuotaTargetDeleteStoreV2 objectRoute) {
        this.creation = creation;
        this.binding = binding;
        this.budget = budget;
        this.store = store;
        this.aggregateGuard = aggregateGuard;
        this.writer = writer;
        this.offloader = offloader;
        this.objectConnection = objectConnection;
        this.objectRoute = objectRoute;
        sourceControl = binding.m4Control();
        workspace = budget.reserveCursorObservation(binding, MAX_BYTES);
        enrollment = new ProofBoundWriterEnrollmentV1(
                List.of(ProofBoundWriterClassV1.values()),
                creation.spec().configuration(),
                implementation,
                creation.spec().sha256());
        original = workspace
                .ready()
                .thenCompose(unused -> {
                    workspace.actorStarting();
                    return requireOwner();
                })
                .thenCompose(unused -> sourceControl.captureSelectedSourceMap())
                .thenAccept(observed -> {
                    selection = observed;
                    var direct = observed.map()
                            .filter(map -> !PulsarNativeMaterializedReadV2.isRecipe(map.actualViewBytes()));
                    if (direct.isPresent()) {
                        selectedDirectory = direct.orElseThrow().floorDirectory(binding);
                        if (!selectedDirectory
                                        .bookKeeper()
                                        .equals(creation.spec().namespaceBinding())
                                || objectConnection == null
                                || !selectedDirectory.objects().equals(objectConnection.binding())) {
                            throw new IllegalArgumentException("Native selected source admission namespace differs");
                        }
                    }
                })
                .thenCompose(unused -> {
                    boolean mapped = selection.manifest()
                            || selection
                                    .map()
                                    .filter(map -> PulsarNativeMaterializedReadV2.isRecipe(map.actualViewBytes()))
                                    .isPresent();
                    return !mapped
                            ? CompletableFuture.completedFuture(null)
                            : sourceControl
                                    .readSelectedMaterializationSnapshot(creation.controlExecutor())
                                    .thenAccept(observed -> {
                                        selectedMaterialization = observed;
                                        if (objectConnection == null
                                                || !observed.selected()
                                                        .providerProfile()
                                                        .physicalBinding()
                                                        .equals(objectConnection.binding())) {
                                            throw new IllegalArgumentException(
                                                    "Native mapped source admission namespace differs");
                                        }
                                        selectedDirectory =
                                                observed.selected().readMap().originalNativeDirectory(binding);
                                        if (!selectedDirectory
                                                        .bookKeeper()
                                                        .equals(creation.spec().namespaceBinding())
                                                || !selectedDirectory.objects().equals(objectConnection.binding())) {
                                            throw new IllegalArgumentException(
                                                    "Native original mapped directory namespace differs");
                                        }
                                    });
                })
                .thenCompose(unused -> store.readStoredMutationValue(binding.managedLedgerName(), path(), MAX_BYTES))
                .thenCompose(value -> {
                    if (value.isEmpty()) {
                        if (selection.manifest() || selection.map().isPresent()) {
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("selected Native source has no parent metadata"));
                        }
                        missingFirstParent = true;
                        return binding.requireFirstNativeFactory(creation)
                                .thenCompose(ignored -> readCursorNames())
                                .thenAccept(names -> {
                                    if (!names.isEmpty()) {
                                        throw new IllegalStateException(
                                                "first Native parent has existing cursor metadata");
                                    }
                                });
                    }
                    snapshot = value.orElseThrow();
                    final org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo info;
                    try {
                        info = store.parseManagedLedgerInfo(snapshot.storedBytes(), MAX_BYTES);
                        binding.requireStoredMetadataBinding(info);
                        if (info.getLedgerInfosList().size() > 512) {
                            throw new IllegalArgumentException("Native source admission exceeds the chain bound");
                        }
                    } catch (Exception invalid) {
                        return CompletableFuture.failedFuture(invalid);
                    }
                    CompletableFuture<Void> capture = captureMappedOutputs();
                    for (var source : info.getLedgerInfosList()) {
                        if (selectedMaterialization != null
                                && selectedMaterialization
                                        .selected()
                                        .readMap()
                                        .fullyReplaces(source.getLedgerId(), source.getEntries() - 1)) {
                            continue;
                        }
                        var pinned = selectedDirectory == null
                                ? null
                                : selectedDirectory.objectOnly().get(source.getLedgerId());
                        if (pinned != null
                                && (!source.hasOffloadContext()
                                        || !source.getOffloadContext().isComplete()
                                        || source.getEntries() != Math.addExact(pinned.lastEntry(), 1)
                                        || source.getSize() != pinned.logicalLength()
                                        || source.getOffloadContext().getUidMsb()
                                                != pinned.identity()
                                                        .attemptUuid()
                                                        .getMostSignificantBits()
                                        || source.getOffloadContext().getUidLsb()
                                                != pinned.identity()
                                                        .attemptUuid()
                                                        .getLeastSignificantBits())) {
                            throw new IllegalStateException("Native selected Object source differs from actual parent");
                        }
                        if (pinned == null
                                && (!source.hasOffloadContext()
                                        || !source.getOffloadContext().isComplete()
                                        || !source.getOffloadContext().isBookkeeperDeleted())) {
                            long id = source.getLedgerId();
                            capture = capture.thenCompose(unused -> captureClosedHeader(id));
                        }
                        if (source.hasOffloadContext()
                                && source.getOffloadContext().isComplete()) {
                            capture = capture.thenCompose(unused -> captureOffloaded(source, pinned));
                        }
                    }
                    snapshotBytes = snapshot.storedBytes().length;
                    return capture.thenCompose(unused -> captureCursorSources());
                })
                .thenCompose(unused -> requireSnapshot())
                .thenCompose(unused -> {
                    var root = sourceAdmissionRoot();
                    request = new GuardedMutationRequestV1(
                            writer,
                            ReferenceTargetKindV1.PULSAR_AGGREGATE,
                            binding.authority().aggregate().canonicalStoredDigest(),
                            Sha256Digest.hash(CanonicalBytes.copyOf(
                                    UUID.randomUUID().toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII))),
                            root);
                    return aggregateGuard.execute(request, () -> {
                        return admitCaptured().thenApply(done -> {
                            terminal = new ExternalMutationResultV1(ExternalMutationOutcomeV1.APPLIED_EXACT, root);
                            return terminal;
                        });
                    });
                })
                .thenCompose(result -> {
                    if (result.outcome()
                            != M5ReferenceMutationGuardV1.GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED) {
                        throw new IllegalStateException("original Native sealed-source C ticket remains unresolved");
                    }
                    return requireSnapshot().thenCompose(unused -> release());
                });
        original.whenComplete((unused, failure) -> {
            if (failure != null) {
                workspace.retainUndeliveredSdk(this::reconcile, false);
            }
        });
    }

    CompletableFuture<Void> observe(MetaStoreImpl originalStore) {
        if (store != originalStore) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Native source backend was replaced"));
        }
        return original.copy();
    }

    synchronized CompletableFuture<Void> reconcile() {
        if (cleanup == null || cleanup.isCompletedExceptionally()) {
            cleanup = original.handle((unused, failure) -> failure).thenCompose(failure -> {
                if (failure == null) {
                    return CompletableFuture.completedFuture(null);
                }
                if (request == null) {
                    return release();
                }
                if (!physicalStarted) {
                    return aggregateGuard.reconcileUndispatched(request).thenCompose(result -> {
                        if (result.outcome()
                                != M5ReferenceMutationGuardV1.GuardOutcomeV1.UNDISPATCHED_TICKET_FENCED_AND_CLEARED) {
                            throw new IllegalStateException("original Native source C admission remains unresolved");
                        }
                        return release();
                    });
                }
                return (terminal == null ? admitCaptured() : CompletableFuture.<Void>completedFuture(null))
                        .thenCompose(unused -> {
                            terminal = new ExternalMutationResultV1(
                                    ExternalMutationOutcomeV1.APPLIED_EXACT, request.externalAuthorityRootSha256());
                            return aggregateGuard.reconcileCompleted(request, terminal);
                        })
                        .thenCompose(result -> {
                            if (result.outcome()
                                    != M5ReferenceMutationGuardV1.GuardOutcomeV1.MUTATION_APPLIED_AND_TICKET_CLEARED) {
                                throw new IllegalStateException("original Native source C cleanup remains unresolved");
                            }
                            return release();
                        });
            });
        }
        return cleanup.copy();
    }

    private CompletableFuture<Void> release() {
        return CompletableFuture.allOf(List.copyOf(sdkCaptures).stream()
                        .map(PulsarNativeOffloadedReferenceV2::close)
                        .toArray(CompletableFuture[]::new))
                .thenCompose(unused -> workspace.releaseAfterDrain())
                .thenRun(() -> {
                    snapshot = null;
                    candidates.clear();
                    contexts.clear();
                    sdkCaptures.clear();
                    cursorSnapshots.clear();
                    cursorNames = List.of();
                    selection = null;
                    selectedDirectory = null;
                    selectedMaterialization = null;
                });
    }

    private CompletableFuture<Void> captureClosedHeader(long id) {
        return captureClosedHeader(id, null);
    }

    private CompletableFuture<Void> captureClosedHeader(long id, String cursorName) {
        var manager = creation.namespace().borrowClient().getLedgerManager();
        var resource =
                new PhysicalResourceIdV2.BookKeeperLedger(creation.namespace().namespace(), id);
        return manager.readLedgerMetadata(id).thenCompose(observed -> {
            var metadata = observed.getValue();
            if (cursorName != null
                    && !java.util.Arrays.equals(
                            metadata.getCustomMetadata().get("pulsar/cursor"),
                            cursorName.getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
                throw new IllegalArgumentException("Native cursor source SDK header names another cursor");
            }
            final CanonicalBytes header;
            try {
                M5PulsarNativeCreateSpecV1.requireBoundedMetadata(metadata);
                header = CanonicalBytes.copyOf(new LedgerMetadataSerDe().serialize(metadata));
            } catch (Exception invalid) {
                return CompletableFuture.failedFuture(invalid);
            }
            CompletableFuture<com.nereusstream.storage.bookkeeper.M5PulsarNativeMetadataDriverV1.PriorSealedBirth>
                    prior;
            if (creation.spec().bindingFor(metadata, true).isPresent()) {
                prior = creation.namespace().requirePulsarReservation(id).thenApply(unused -> null);
            } else if (creation.spec().sealedSourceBindingFor(metadata).isPresent()) {
                prior = CompletableFuture.completedFuture(null);
            } else {
                prior = creation.namespace().readPriorPulsarSealedBirth(id, metadata);
            }
            return prior.thenCompose(original -> manager.readLedgerMetadata(id).thenCompose(current -> {
                try {
                    if (!current.getVersion().equals(observed.getVersion())
                            || !CanonicalBytes.copyOf(new LedgerMetadataSerDe().serialize(current.getValue()))
                                    .equals(header)) {
                        throw new IllegalStateException("Native sealed source SDK header changed during capture");
                    }
                    if (original != null) {
                        return capturePriorPhysicalSource(resource, original);
                    }
                    if (creation.spec().bindingFor(metadata, true).isEmpty()) {
                        return captureLegacyPhysicalSource(resource, header);
                    }
                    var root = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(132)
                            .putInt(0x4e505342) // NPSB: actual immutable Native plain source.
                            .put(resource.sha256().bytes().toByteArray())
                            .put(creation.spec().sha256().bytes().toByteArray())
                            .put(Sha256Digest.hash(header).bytes().toByteArray())
                            .put(binding.authority()
                                    .aggregate()
                                    .canonicalStoredDigest()
                                    .bytes()
                                    .toByteArray())
                            .array()));
                    candidates.add(M5TargetDeleteAuthorityStateMachineV1.open(
                            PhysicalDeleteTargetV1.create(resource), enrollment, root));
                    contexts.put(
                            resource,
                            new SourceContext(
                                    creation.route(),
                                    enrollment,
                                    creation.spec().providerScope(),
                                    creation.spec().configuration(),
                                    java.util.Optional.empty()));
                    return CompletableFuture.<Void>completedFuture(null);
                } catch (java.io.IOException invalid) {
                    throw new CompletionException(invalid);
                }
            }));
        });
    }

    private Sha256Digest legacySourceRoot(PhysicalResourceIdV2 resource, Sha256Digest scope, CanonicalBytes header) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(132)
                .putInt(0x4e505342)
                .put(resource.sha256().bytes().toByteArray())
                .put(scope.bytes().toByteArray())
                .put(Sha256Digest.hash(header).bytes().toByteArray())
                .put(binding.authority()
                        .aggregate()
                        .canonicalStoredDigest()
                        .bytes()
                        .toByteArray())
                .array()));
    }

    private CompletableFuture<Void> captureLegacyPhysicalSource(
            PhysicalResourceIdV2.BookKeeperLedger resource, CanonicalBytes header) {
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(creation.route());
        var sourceWitness = legacySourceRoot(
                resource, Sha256Digest.hash(creation.spec().namespaceBinding().encode()), header);
        var sourceEnrollment = new ProofBoundWriterEnrollmentV1(
                enrollment.writerClasses(),
                enrollment.capabilitySetRootSha256(),
                enrollment.implementationRootSha256(),
                sourceWitness);
        return coordinator
                .inspect(resource.authorityKey())
                .thenCompose(found -> {
                    if (found.isEmpty()) {
                        candidates.add(M5TargetDeleteAuthorityStateMachineV1.open(
                                PhysicalDeleteTargetV1.create(resource), sourceEnrollment, sourceWitness));
                        contexts.put(
                                resource,
                                new SourceContext(
                                        creation.route(),
                                        sourceEnrollment,
                                        creation.spec().providerScope(),
                                        creation.spec().configuration(),
                                        java.util.Optional.empty()));
                        return CompletableFuture.completedFuture(null);
                    }
                    var actual = found.orElseThrow();
                    var authority = actual.fullAuthority().orElseThrow();
                    var expectedRoot = sourceWitness;
                    if (actual.state() != TargetDeleteAuthorityStateV1.OPEN_V1
                            || !authority
                                    .writerEnrollment()
                                    .capabilitySetRootSha256()
                                    .equals(creation.spec().configuration())
                            || !authority.writerEnrollment().policyRootSha256().equals(expectedRoot)) {
                        throw new IllegalStateException(
                                "original legacy sealed source header/P1 authority differs or is fenced: resource="
                                        + resource.ledgerId() + " state=" + actual.state()
                                        + " capabilityMatches="
                                        + authority
                                                .writerEnrollment()
                                                .capabilitySetRootSha256()
                                                .equals(creation.spec().configuration())
                                        + " originalProof="
                                        + authority
                                                .writerEnrollment()
                                                .policyRootSha256()
                                                .toHex()
                                        + " observedProof=" + expectedRoot.toHex());
                    }
                    var capture = creation.route()
                            .members()
                            .readFact(resource)
                            .thenAccept(member -> {
                                if (!member.inventory()
                                                .protocolCell()
                                                .equals(Sha256Digest.hash(
                                                        creation.spec().protocolCell()))
                                        || !member.inventory()
                                                .members()
                                                .contains(new Member(
                                                        PositionDomain.PULSAR_ENTRY,
                                                        creation.spec().providerScope(),
                                                        binding.bindingIdentity()))) {
                                    throw new IllegalStateException(
                                            "original legacy source member differs from its Cell/Binding");
                                }
                            })
                            .toCompletableFuture();
                    return capture.thenRun(() -> {
                        candidates.add(authority);
                        contexts.put(
                                resource,
                                new SourceContext(
                                        creation.route(),
                                        authority.writerEnrollment(),
                                        creation.spec().providerScope(),
                                        creation.spec().configuration(),
                                        java.util.Optional.of(expectedRoot)));
                    });
                })
                .toCompletableFuture();
    }

    private CompletableFuture<Void> capturePriorPhysicalSource(
            PhysicalResourceIdV2.BookKeeperLedger resource,
            com.nereusstream.storage.bookkeeper.M5PulsarNativeMetadataDriverV1.PriorSealedBirth original) {
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(creation.route());
        return coordinator
                .inspect(resource.authorityKey())
                .thenCompose(found -> {
                    var actual = found.orElseThrow(() ->
                            new IllegalStateException("prior Native sealed source has no original physical authority"));
                    var authority = actual.fullAuthority().orElseThrow();
                    if (actual.state() != TargetDeleteAuthorityStateV1.OPEN_V1
                            || !authority
                                    .writerEnrollment()
                                    .capabilitySetRootSha256()
                                    .equals(original.scope().configuration())
                            || !authority
                                    .writerEnrollment()
                                    .policyRootSha256()
                                    .equals(original.scope().sha256())
                            || !authority
                                    .writerEnrollment()
                                    .writerClasses()
                                    .equals(List.of(ProofBoundWriterClassV1.values()))) {
                        throw new IllegalStateException(
                                "prior Native sealed source physical authority differs or is fenced");
                    }
                    return creation.route().members().readFact(resource).thenAccept(member -> {
                        if (!member.inventory()
                                        .protocolCell()
                                        .equals(Sha256Digest.hash(
                                                creation.spec().protocolCell()))
                                || !member.inventory()
                                        .members()
                                        .contains(new Member(
                                                PositionDomain.PULSAR_ENTRY,
                                                original.scope().providerScope(),
                                                binding.bindingIdentity()))) {
                            throw new IllegalStateException(
                                    "prior Native sealed source member differs from its actual Cell/Binding");
                        }
                        var encoded = new java.io.ByteArrayOutputStream();
                        try (var out = new java.io.DataOutputStream(encoded)) {
                            out.writeInt(0x4e505346); // NPSF: exact prior fenced SDK birth facts.
                            out.writeInt(1);
                            out.write(resource.sha256().bytes().toByteArray());
                            out.write(original.scope().sha256().bytes().toByteArray());
                            var facts = new ArrayList<>(original.facts());
                            facts.add(original.ledgerMetadata());
                            for (var fact : facts) {
                                out.writeUTF(fact.nativePath());
                                out.write(fact.rawStoredSha256().bytes().toByteArray());
                                out.writeLong(fact.nativeCreateZxid());
                                out.writeLong(fact.nativeModifyZxid());
                                out.writeInt(fact.nativeVersion());
                                out.writeLong(fact.nativeCreatedAtMillis());
                            }
                        } catch (java.io.IOException invalid) {
                            throw new CompletionException(invalid);
                        }
                        candidates.add(authority);
                        contexts.put(
                                resource,
                                new SourceContext(
                                        creation.route(),
                                        authority.writerEnrollment(),
                                        original.scope().providerScope(),
                                        original.scope().configuration(),
                                        java.util.Optional.of(
                                                Sha256Digest.hash(CanonicalBytes.copyOf(encoded.toByteArray())))));
                    });
                })
                .toCompletableFuture();
    }

    private CompletableFuture<List<String>> readCursorNames() {
        var result = new CompletableFuture<List<String>>();
        store.readStoredCursorNames(
                binding.managedLedgerName(), new org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback<>() {
                    public void operationComplete(List<String> names, org.apache.pulsar.metadata.api.Stat stat) {
                        if (names.size() > PulsarNativeCursorMetadataV2.MAX_CURSORS
                                || names.stream().distinct().count() != names.size()) {
                            result.completeExceptionally(
                                    new IllegalArgumentException("Native source cursor directory exceeds its bound"));
                        } else {
                            result.complete(names.stream().sorted().toList());
                        }
                    }

                    public void operationFailed(
                            org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException failure) {
                        result.completeExceptionally(failure);
                    }
                });
        return result;
    }

    private CompletableFuture<Void> captureCursorSources() {
        return readCursorNames().thenCompose(names -> {
            cursorNames = names;
            CompletableFuture<Void> captured = CompletableFuture.completedFuture(null);
            for (var name : names) {
                captured = captured.thenCompose(unused -> store.readStoredMutationValue(
                                binding.managedLedgerName(), path() + "/" + name, MAX_BYTES)
                        .thenCompose(value -> {
                            var actual = value.orElseThrow(
                                    () -> new IllegalStateException("Native source cursor disappeared"));
                            snapshotBytes = Math.addExact(
                                    snapshotBytes,
                                    actual.storedBytes().length
                                            + name.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
                            if (snapshotBytes > MAX_BYTES) {
                                throw new IllegalArgumentException(
                                        "Native source metadata snapshots exceed the admitted bound");
                            }
                            final org.apache.bookkeeper.mledger.proto.ManagedCursorInfo info;
                            try {
                                info = store.parseManagedCursorInfo(actual.storedBytes(), MAX_BYTES);
                            } catch (Exception invalid) {
                                return CompletableFuture.failedFuture(invalid);
                            }
                            cursorSnapshots.put(name, actual);
                            return !info.hasCursorsLedgerId() || info.getCursorsLedgerId() < 0
                                    ? CompletableFuture.completedFuture(null)
                                    : captureClosedHeader(info.getCursorsLedgerId(), name);
                        }));
            }
            return captured;
        });
    }

    private CompletableFuture<Void> captureMappedOutputs() {
        if (selectedMaterialization == null) {
            return CompletableFuture.completedFuture(null);
        }
        var selected = selectedMaterialization.selected();
        var profile = selected.providerProfile();
        CompletableFuture<Void> work = CompletableFuture.completedFuture(null);
        for (var credit : selected.readMap().readableOutputs()) {
            var resource = credit.resource().orElseThrow();
            if (contexts.containsKey(resource)) {
                continue;
            }
            work = work.thenCompose(unused -> objectRoute.readResourceFact(resource))
                    .thenCompose(fact -> {
                        var authority = com.nereusstream.storage.object.gc.M5TargetDeleteStoredValueV2.decode(
                                        fact.authority())
                                .fullAuthority()
                                .orElseThrow();
                        if (!fact.binding().equals(profile.physicalBinding())
                                || authority.state() != TargetDeleteAuthorityStateV1.OPEN_V1
                                || !authority
                                        .writerEnrollment()
                                        .capabilitySetRootSha256()
                                        .equals(profile.providerConfigurationSha256())
                                || !authority
                                        .writerEnrollment()
                                        .writerClasses()
                                        .equals(List.of(ProofBoundWriterClassV1.values()))) {
                            throw new IllegalStateException("Native mapped output authority differs or is fenced");
                        }
                        return objectRoute.members().readFact(resource).thenAccept(member -> {
                            if (!member.inventory()
                                            .protocolCell()
                                            .equals(Sha256Digest.hash(
                                                    creation.spec().protocolCell()))
                                    || !member.inventory()
                                            .members()
                                            .contains(new Member(
                                                    PositionDomain.PULSAR_ENTRY,
                                                    profile.scopeId().digest(),
                                                    binding.bindingIdentity()))) {
                                throw new IllegalStateException(
                                        "Native mapped output member differs from its Cell/Binding");
                            }
                            var prior = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(64)
                                    .put(fact.reservation()
                                            .canonicalStoredSha256()
                                            .bytes()
                                            .toByteArray())
                                    .put(fact.authority()
                                            .canonicalStoredSha256()
                                            .bytes()
                                            .toByteArray())
                                    .array()));
                            candidates.add(authority);
                            contexts.put(
                                    resource,
                                    new SourceContext(
                                            objectRoute,
                                            authority.writerEnrollment(),
                                            profile.scopeId().digest(),
                                            profile.providerConfigurationSha256(),
                                            java.util.Optional.of(prior)));
                        });
                    })
                    .toCompletableFuture();
        }
        return work;
    }

    private CompletableFuture<Void> captureOffloaded(
            org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo source,
            PulsarNativeM4SourceMapV1.PublishedObject pinned) {
        if (offloader == null || objectConnection == null || objectRoute == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Native completed offload admission requires its original Object scope"));
        }
        return objectConnection.requireProfileCurrent().thenCompose(unused -> {
            var profile = objectConnection.profile();
            var original = new PulsarNativeOffloadedReferenceV2(
                    offloader, profile, binding.managedLedgerName(), source, pinned == null ? null : pinned.identity());
            sdkCaptures.add(original);
            return original.observe().thenAccept(actual -> {
                final Sha256Digest implementation;
                try (var bytes = PulsarNativeOffloadedReferenceV2.class.getResourceAsStream(
                        "PulsarNativeOffloadedReferenceV2.class")) {
                    implementation = Sha256Digest.hash(CanonicalBytes.copyOf(
                            java.util.Objects.requireNonNull(bytes).readAllBytes()));
                } catch (java.io.IOException failure) {
                    throw new CompletionException(failure);
                }
                var enrollment = new ProofBoundWriterEnrollmentV1(
                        List.of(ProofBoundWriterClassV1.values()),
                        profile.providerConfigurationSha256(),
                        implementation,
                        Sha256Digest.hash(profile.encode()));
                var root = Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(100)
                        .putInt(0x4e50534f) // NPSO: original readonly SDK Object source.
                        .put(actual.sdkFactsSha256().bytes().toByteArray())
                        .put(Sha256Digest.hash(profile.encode()).bytes().toByteArray())
                        .put(binding.authority()
                                .aggregate()
                                .canonicalStoredDigest()
                                .bytes()
                                .toByteArray())
                        .array()));
                for (var resource : actual.resources()) {
                    candidates.add(M5TargetDeleteAuthorityStateMachineV1.open(
                            PhysicalDeleteTargetV1.create(resource), enrollment, root));
                    contexts.put(
                            resource,
                            new SourceContext(
                                    objectRoute,
                                    enrollment,
                                    profile.scopeId().digest(),
                                    profile.providerConfigurationSha256(),
                                    java.util.Optional.empty()));
                }
            });
        });
    }

    private CompletableFuture<Void> admitCaptured() {
        CompletableFuture<Void> work = requireOwner();
        for (var candidate : candidates) {
            var resource = candidate.target().resourceId();
            var context = contexts.get(resource);
            var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(context.route());
            work = work.thenCompose(unused -> coordinator
                            .inspect(resource.authorityKey())
                            .thenCompose(found -> {
                                if (found.isPresent()) {
                                    var actual = found.orElseThrow();
                                    if (actual.state() != TargetDeleteAuthorityStateV1.OPEN_V1
                                            || actual.fullAuthority()
                                                    .filter(value -> value.writerEnrollment()
                                                            .equals(context.enrollment()))
                                                    .isEmpty()) {
                                        throw new IllegalStateException(
                                                "Native sealed source authority differs or is fenced");
                                    }
                                    return CompletableFuture.completedFuture(null);
                                }
                                physicalStarted = true;
                                return coordinator.create(candidate).thenAccept(value -> {
                                    if (!value.exactCandidateIsAuthoritative()) {
                                        throw new IllegalStateException(
                                                "Native sealed source admission remains unresolved");
                                    }
                                });
                            })
                            .thenCompose(done -> {
                                physicalStarted = true;
                                return context.route()
                                        .members()
                                        .register(
                                                resource,
                                                Sha256Digest.hash(
                                                        creation.spec().protocolCell()),
                                                new Member(
                                                        PositionDomain.PULSAR_ENTRY,
                                                        context.providerScope(),
                                                        binding.bindingIdentity()),
                                                context.capability(),
                                                Sha256Digest.hash(
                                                        creation.spec().allocatorOwner()));
                            })
                            .thenCompose(done -> context.route().readResourceFact(resource)))
                    .thenApply(unused -> null);
        }
        return work.thenCompose(unused -> requireOwner());
    }

    private CompletableFuture<Void> requireOwner() {
        return budget.requireNativeOwner(binding, creation.spec().allocatorOwner())
                .thenCompose(unused -> binding.revalidate())
                .thenCompose(unused -> objectConnection == null
                        ? CompletableFuture.completedFuture(null)
                        : objectConnection.requireProfileCurrent());
    }

    private Sha256Digest sourceAdmissionRoot() {
        if (missingFirstParent) {
            // Explicit absence under the one-time actual factory claim; no synthetic Native bytes or version.
            return Sha256Digest.hash(CanonicalBytes.copyOf(ByteBuffer.allocate(104)
                    .putInt(0x4e505341)
                    .putInt(6)
                    .put(selectionRoot().bytes().toByteArray())
                    .put(binding.authority()
                            .aggregate()
                            .canonicalStoredDigest()
                            .bytes()
                            .toByteArray())
                    .put(creation.spec().sha256().bytes().toByteArray())
                    .array()));
        }
        var roots = ByteBuffer.allocate(92 + 64 * candidates.size() + 72 * cursorSnapshots.size())
                .putInt(0x4e505341) // NPSA
                .putInt(5)
                .put(selectionRoot().bytes().toByteArray())
                .put(Sha256Digest.hash(CanonicalBytes.copyOf(snapshot.storedBytes()))
                        .bytes()
                        .toByteArray())
                .putLong(snapshot.stat().getVersion())
                .putLong(candidates.size());
        candidates.forEach(value -> {
            roots.put(value.authorityCanonicalSha256().bytes().toByteArray());
            roots.put(contexts.get(value.target().resourceId())
                    .priorScopeFacts()
                    .orElse(value.authorityCanonicalSha256())
                    .bytes()
                    .toByteArray());
        });
        roots.putInt(cursorSnapshots.size());
        cursorSnapshots.forEach((name, value) -> roots.put(
                        Sha256Digest.hash(CanonicalBytes.copyOf(name.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                                .bytes()
                                .toByteArray())
                .put(Sha256Digest.hash(CanonicalBytes.copyOf(value.storedBytes()))
                        .bytes()
                        .toByteArray())
                .putLong(value.stat().getVersion()));
        return Sha256Digest.hash(CanonicalBytes.copyOf(roots.array()));
    }

    private Sha256Digest selectionRoot() {
        try {
            var bytes = new java.io.ByteArrayOutputStream();
            try (var out = new java.io.DataOutputStream(bytes)) {
                var facts = new ArrayList<
                        com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue>(
                        java.util.stream.Stream.concat(selection.selector().stream(), selection.source().stream())
                                .toList());
                if (selectedMaterialization != null) {
                    facts.addAll(selectedMaterialization.immutableFacts().values().stream()
                            .sorted(java.util.Comparator.comparing(value -> value.key()))
                            .toList());
                    facts.addAll(selectedMaterialization.nativeFacts().values().stream()
                            .sorted(java.util.Comparator.comparing(value -> value.key()))
                            .toList());
                }
                out.writeInt(facts.size());
                for (var fact : facts) {
                    out.writeUTF(fact.key());
                    var version = fact.metadataVersion().value().toByteArray();
                    out.writeInt(version.length);
                    out.write(version);
                    out.write(fact.canonicalStoredSha256().bytes().toByteArray());
                }
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (java.io.IOException failure) {
            throw new CompletionException(failure);
        }
    }

    private CompletableFuture<Void> requireSnapshot() {
        return requireOwner()
                .thenCompose(unused -> store.readStoredMutationValue(binding.managedLedgerName(), path(), MAX_BYTES))
                .thenAccept(current -> {
                    if (missingFirstParent) {
                        if (current.isPresent()) {
                            throw new IllegalStateException("first Native parent appeared during source admission");
                        }
                        return;
                    }
                    if (current.isEmpty()
                            || !current.orElseThrow().stat().equals(snapshot.stat())
                            || !java.util.Arrays.equals(current.orElseThrow().storedBytes(), snapshot.storedBytes())) {
                        throw new IllegalStateException("Native source metadata changed during admission");
                    }
                })
                .thenCompose(unused -> missingFirstParent
                        ? binding.requireFirstNativeFactory(creation).toCompletableFuture()
                        : CompletableFuture.completedFuture(null))
                .thenCompose(unused -> readCursorNames())
                .thenCompose(currentNames -> {
                    if (!currentNames.equals(cursorNames)) {
                        throw new IllegalStateException("Native source cursor directory changed during admission");
                    }
                    CompletableFuture<Void> verified = CompletableFuture.completedFuture(null);
                    for (var entry : cursorSnapshots.entrySet()) {
                        verified = verified.thenCompose(unused -> store.readStoredMutationValue(
                                        binding.managedLedgerName(), path() + "/" + entry.getKey(), MAX_BYTES)
                                .thenAccept(value -> {
                                    var original = entry.getValue();
                                    if (value.isEmpty()
                                            || !value.orElseThrow().stat().equals(original.stat())
                                            || !java.util.Arrays.equals(
                                                    value.orElseThrow().storedBytes(), original.storedBytes())) {
                                        throw new IllegalStateException(
                                                "Native source cursor metadata changed during admission");
                                    }
                                }));
                    }
                    return verified;
                })
                .thenCompose(unused -> sourceControl.requireSelectedSourceSnapshot(selection))
                .thenCompose(unused -> selectedMaterialization == null
                        ? CompletableFuture.completedFuture(null)
                        : sourceControl.requireSelectedMaterializationSnapshot(
                                selectedMaterialization, creation.controlExecutor()));
    }

    private String path() {
        return "/managed-ledgers/" + binding.managedLedgerName();
    }
}
