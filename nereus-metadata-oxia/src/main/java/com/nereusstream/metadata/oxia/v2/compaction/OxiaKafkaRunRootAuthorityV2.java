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

package com.nereusstream.metadata.oxia.v2.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.storage.api.bookkeeper.ProviderMutationResultV1;
import com.nereusstream.storage.api.bookkeeper.RunLedgerRecoveryProofV1;
import com.nereusstream.storage.api.bookkeeper.StorageRunId;
import com.nereusstream.storage.api.kafka.KafkaBookKeeperOwnerAuthorityV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerAdmissionV1;
import com.nereusstream.storage.api.kafka.KafkaOwnerIdentityV1;
import com.nereusstream.storage.api.kafka.KafkaRunRecoveryCutV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootAuthority;
import com.nereusstream.storage.api.kafka.KafkaRunRootCatalogV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2.Link;
import com.nereusstream.storage.api.kafka.KafkaRunRootRecordV2.Scope;
import com.nereusstream.storage.api.kafka.KafkaRunRootSnapshotV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootStateV1;
import com.nereusstream.storage.api.kafka.KafkaRunRootVerifierV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ProofBoundWriterClassV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Completion;
import com.nereusstream.storage.object.gc.M5TargetDeleteMultiWriterGuardV2.Context;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Native one-key run choices and sealed roots. Prewrites are not readable until genesis/parent selection is durable.
 * Every resource must already have admitted permanent GC authority; this adapter never initializes absent authority.
 * Protocol-owner admission is ordered with run attachment on one native head; physical verification is separate.
 */
public final class OxiaKafkaRunRootAuthorityV2
        implements KafkaRunRootAuthority, KafkaRunRootCatalogV2, KafkaBookKeeperOwnerAuthorityV1 {
    private record Stored(AuthorityRecord nativeValue, KafkaRunRootRecordV2 value) {}

    private record Terminal(ProviderMutationResultV1<KafkaRunRootSnapshotV1> result, Sha256Digest proof) {}

    private final OxiaConditionalClient client;
    private final PhysicalNamespaceAuthorityBindingV2 binding;
    private final Scope scope;
    private final KafkaRunRootVerifierV2 verifier;
    private final M5TargetDeleteMultiWriterGuardV2 guard;
    private final String prefix;

    /** Native namespace factory supplies its guarded client; the low-level constructor is not admission proof. */
    public OxiaKafkaRunRootAuthorityV2(
            OxiaConditionalClient client,
            PhysicalNamespaceAuthorityBindingV2 binding,
            Scope scope,
            KafkaRunRootVerifierV2 verifier,
            M5TargetDeleteMultiWriterGuardV2 guard) {
        this.client = Objects.requireNonNull(client, "client");
        this.binding = Objects.requireNonNull(binding, "binding");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.verifier = Objects.requireNonNull(verifier, "verifier");
        this.guard = Objects.requireNonNull(guard, "guard");
        if (binding.physicalNamespace().providerKind() != PhysicalResourceIdV2.ProviderKind.BOOKKEEPER
                || verifier.capabilitySha256().isZero()) {
            throw new IllegalArgumentException("run authority requires an admitted BK namespace and capability");
        }
        prefix = binding.authorityRoot() + "/kafka-runs-v2/"
                + Sha256Digest.hash(scope.encode()).toHex();
    }

    public String nativeRootKey(StorageRunId runId) {
        return prefix + "/" + runId.value().toHex() + "/root-v2";
    }

    public String nativeGenesisKey() {
        return prefix + "/genesis-v2";
    }

    public String nativeOwnerKey() {
        return prefix + "/owner-admission-v1";
    }

    public String nativeClosedOwnerKey(long ownerEpoch) {
        if (ownerEpoch <= 0) {
            throw new IllegalArgumentException("closed owner epoch must be positive");
        }
        return prefix + "/closed-owners-v1/" + ownerEpoch;
    }

    @Override
    public CompletionStage<Optional<KafkaOwnerAdmissionV1>> readOwnerAdmission() {
        return readOwnerHead().thenApply(value -> value.map(this::decodeAdmission));
    }

    @Override
    public CompletionStage<Optional<KafkaOwnerAdmissionV1>> readClosedOwner(long ownerEpoch) {
        String key = nativeClosedOwnerKey(ownerEpoch);
        return client.read(key)
                .thenApply(observed -> observed.map(value -> {
                    var admission = decodeAdmission(value);
                    if (!value.key().equals(key)
                            || !admission.closed()
                            || admission.owner().ownerEpoch() != ownerEpoch) {
                        throw new IllegalStateException("closed owner archive differs from its native identity");
                    }
                    return admission;
                }));
    }

    @Override
    public CompletionStage<ProviderMutationResultV1<KafkaOwnerAdmissionV1>> openOwner(
            KafkaOwnerIdentityV1 owner, Optional<KafkaOwnerAdmissionV1> expectedClosed) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(expectedClosed, "expectedClosed");
        expectedClosed.ifPresent(value -> {
            if (!value.closed() || !value.scopeSha256().equals(scopeDigest()) || !owner.succeeds(value.owner())) {
                throw new IllegalArgumentException("new owner does not succeed its exact closed native history");
            }
        });
        var candidate = new KafkaOwnerAdmissionV1(
                scopeDigest(),
                owner,
                false,
                expectedClosed.map(value -> new KafkaOwnerAdmissionV1.Previous(
                        value.owner().ownerEpoch(), Sha256Digest.hash(value.encode()), value.tail())),
                List.of());
        var work = readOwnerHead().thenCompose(before -> {
            if (before.isPresent()) {
                var current = decodeAdmission(before.orElseThrow());
                if (current.owner().equals(owner)) {
                    return CompletableFuture.completedFuture(
                            current.closed() || !current.previous().equals(candidate.previous())
                                    ? ProviderMutationResultV1.<KafkaOwnerAdmissionV1>fencedOrConflict()
                                    : ProviderMutationResultV1.appliedExact(current));
                }
                if (!expectedClosed.equals(Optional.of(current))) {
                    return CompletableFuture.completedFuture(
                            ProviderMutationResultV1.<KafkaOwnerAdmissionV1>fencedOrConflict());
                }
            } else if (expectedClosed.isPresent()) {
                return CompletableFuture.completedFuture(
                        ProviderMutationResultV1.<KafkaOwnerAdmissionV1>fencedOrConflict());
            }
            CompletionStage<Void> archive =
                    expectedClosed.map(this::archiveClosed).orElseGet(() -> CompletableFuture.completedFuture(null));
            return archive.thenCompose(ignored -> before.isPresent()
                            ? client.compareAndSet(
                                    nativeOwnerKey(),
                                    candidate.encode(),
                                    before.orElseThrow().versionId())
                            : client.createIfAbsent(nativeOwnerKey(), candidate.encode()))
                    .handle((ignored, failure) -> null)
                    .thenCompose(ignored -> readOwnerAdmission())
                    .thenApply(after -> after.filter(value -> value.owner().equals(owner)
                                    && !value.closed()
                                    && value.previous().equals(candidate.previous()))
                            .map(ProviderMutationResultV1::appliedExact)
                            .orElseGet(ProviderMutationResultV1::fencedOrConflict));
        });
        return work.exceptionally(failure -> ProviderMutationResultV1.outcomeUnknown())
                .thenApply(value -> value);
    }

    @Override
    public CompletionStage<ProviderMutationResultV1<KafkaOwnerAdmissionV1>> closeOwner(
            KafkaOwnerIdentityV1 expectedOwner) {
        Objects.requireNonNull(expectedOwner, "expectedOwner");
        return closeOwner(expectedOwner, KafkaOwnerAdmissionV1.MAX_RUNS + 1)
                .exceptionally(failure -> ProviderMutationResultV1.outcomeUnknown())
                .thenApply(value -> value);
    }

    private CompletionStage<ProviderMutationResultV1<KafkaOwnerAdmissionV1>> closeOwner(
            KafkaOwnerIdentityV1 expectedOwner, int attemptsLeft) {
        return readOwnerHead().thenCompose(before -> {
            if (before.isEmpty()
                    || !decodeAdmission(before.orElseThrow()).owner().equals(expectedOwner)) {
                return readClosedOwner(expectedOwner.ownerEpoch()).thenApply(archived -> archived.filter(
                                value -> value.owner().equals(expectedOwner))
                        .map(ProviderMutationResultV1::appliedExact)
                        .orElseGet(ProviderMutationResultV1::fencedOrConflict));
            }
            var current = decodeAdmission(before.orElseThrow());
            if (current.closed()) {
                return archiveClosed(current).thenApply(ignored -> ProviderMutationResultV1.appliedExact(current));
            }
            var closed = current.close();
            return client.compareAndSet(
                            nativeOwnerKey(),
                            closed.encode(),
                            before.orElseThrow().versionId())
                    .handle((ignored, failure) -> null)
                    .thenCompose(ignored -> readOwnerAdmission())
                    .thenCompose(after -> {
                        if (after.equals(Optional.of(closed))) {
                            return archiveClosed(closed)
                                    .thenApply(ignored -> ProviderMutationResultV1.appliedExact(closed));
                        }
                        // Only admitted roots can contend with closure, and their set has a fixed capacity.
                        return attemptsLeft == 0
                                ? CompletableFuture.completedFuture(ProviderMutationResultV1.outcomeUnknown())
                                : closeOwner(expectedOwner, attemptsLeft - 1);
                    });
        });
    }

    private CompletionStage<Optional<AuthorityRecord>> readOwnerHead() {
        return client.read(nativeOwnerKey()).thenApply(value -> {
            if (value.isPresent() && !value.orElseThrow().key().equals(nativeOwnerKey())) {
                throw new IllegalStateException("owner admission read returned another key");
            }
            return value;
        });
    }

    private KafkaOwnerAdmissionV1 decodeAdmission(AuthorityRecord record) {
        var value = KafkaOwnerAdmissionV1.decode(record.storedBytes());
        if (!value.scopeSha256().equals(scopeDigest())) {
            throw new IllegalStateException("owner admission belongs to another protocol scope");
        }
        return value;
    }

    private Sha256Digest scopeDigest() {
        return Sha256Digest.hash(scope.encode());
    }

    private CompletionStage<Void> archiveClosed(KafkaOwnerAdmissionV1 closed) {
        String key = nativeClosedOwnerKey(closed.owner().ownerEpoch());
        return create(key, closed.encode())
                .thenCompose(ignored -> readClosedOwner(closed.owner().ownerEpoch()))
                .thenAccept(after -> {
                    if (!after.equals(Optional.of(closed))) {
                        throw new IllegalStateException("closed owner archive was not established exactly");
                    }
                });
    }

    private CompletionStage<Optional<KafkaOwnerAdmissionV1>> admissionFor(long epoch) {
        return readOwnerAdmission()
                .thenCompose(current -> current.filter(value -> value.owner().ownerEpoch() == epoch)
                                .isPresent()
                        ? CompletableFuture.completedFuture(current)
                        : readClosedOwner(epoch));
    }

    private CompletionStage<Void> admitCandidate(KafkaRunRootRecordV2 candidate) {
        return readOwnerHead().thenCompose(before -> {
            var exact = before.orElseThrow(() -> new IllegalStateException("native owner admission is absent"));
            var current = decodeAdmission(exact);
            if (current.runs().contains(candidate.initialLink())) {
                return CompletableFuture.completedFuture(null);
            }
            var replacement = current.append(candidate);
            return client.compareAndSet(nativeOwnerKey(), replacement.encode(), exact.versionId())
                    .handle((ignored, failure) -> null)
                    .thenCompose(ignored -> admissionFor(candidate.root().creatorOwnerEpoch()))
                    .thenAccept(after -> {
                        if (after.isEmpty() || !after.orElseThrow().runs().contains(candidate.initialLink())) {
                            throw new IllegalStateException("run attachment lost the owner admission CAS");
                        }
                    });
        });
    }

    @Override
    public CompletionStage<Optional<KafkaRunRootRecordV2>> readSelectedRoot(String nativeKey) {
        Objects.requireNonNull(nativeKey, "nativeKey");
        String start = prefix + "/";
        if (nativeKey.length() != start.length() + 32 + "/root-v2".length()
                || !nativeKey.startsWith(start)
                || !nativeKey.endsWith("/root-v2")) {
            throw new IllegalArgumentException("run catalog key is outside its native route");
        }
        String id = nativeKey.substring(start.length(), nativeKey.length() - "/root-v2".length());
        if (!id.matches("[0-9a-f]{32}")) {
            throw new IllegalArgumentException("run catalog key has a noncanonical run ID");
        }
        var run = new StorageRunId(com.nereusstream.domain.identity.Id128.fromBytes(
                java.util.HexFormat.of().parseHex(id)));
        return read(run).thenCompose(current -> {
            if (current.isEmpty()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            var value = current.orElseThrow().value();
            return selected(value).thenApply(chosen -> chosen ? Optional.of(value) : Optional.empty());
        });
    }

    @Override
    public CompletionStage<Optional<KafkaRunRootSnapshotV1>> openRoot(StorageRunId runId) {
        return read(runId).thenCompose(current -> {
            if (current.isEmpty()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            var record = current.orElseThrow().value();
            return selected(record).thenApply(admitted -> admitted ? Optional.of(record.root()) : Optional.empty());
        });
    }

    @Override
    public CompletionStage<Optional<KafkaRunRootRecordV2>> readAdmittedRun(KafkaRunRootRecordV2.Link link) {
        Objects.requireNonNull(link, "link");
        return read(link.runId()).thenCompose(current -> {
            if (current.isEmpty()
                    || !current.orElseThrow().value().initialLink().equals(link)) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            var record = current.orElseThrow().value();
            return selected(record).thenApply(chosen -> chosen ? Optional.of(record) : Optional.empty());
        });
    }

    @Override
    public CompletionStage<Boolean> isDurablyRetired(KafkaRunRootSnapshotV1 exactSealed) {
        Objects.requireNonNull(exactSealed, "exactSealed");
        var expectedResource = resource(exactSealed);
        return read(exactSealed.runId())
                .thenApply(observed -> observed.map(value -> value.value().retired()
                                && value.value().root().equals(exactSealed)
                                && value.value().resource().equals(expectedResource))
                        .orElse(false));
    }

    /** Preserve the selected lineage after an exact permanent physical DELETE_DONE, including compact done. */
    @Override
    public CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> retireDeletedRoot(
            KafkaRunRootSnapshotV1 exactSealed) {
        Objects.requireNonNull(exactSealed, "exactSealed");
        var expectedResource = resource(exactSealed);
        if (exactSealed.state() != KafkaRunRootStateV1.SEALED) {
            throw new IllegalArgumentException("only an exact sealed run root can be retired");
        }
        var work = guard.isPermanentlyDeleted(expectedResource).thenCompose(deleted -> {
            if (!deleted) {
                return CompletableFuture.completedFuture(
                        ProviderMutationResultV1.<KafkaRunRootSnapshotV1>fencedOrConflict());
            }
            return read(exactSealed.runId()).thenCompose(observed -> {
                if (observed.isEmpty()
                        || !observed.orElseThrow().value().root().equals(exactSealed)
                        || !observed.orElseThrow().value().resource().equals(expectedResource)) {
                    return CompletableFuture.completedFuture(
                            ProviderMutationResultV1.<KafkaRunRootSnapshotV1>fencedOrConflict());
                }
                var exact = observed.orElseThrow();
                if (exact.value().retired()) {
                    return CompletableFuture.completedFuture(ProviderMutationResultV1.appliedExact(exactSealed));
                }
                var retired = exact.value().retire();
                return client.compareAndSet(
                                exact.nativeValue().key(),
                                retired.encode(),
                                exact.nativeValue().versionId())
                        .handle((ignored, failure) -> null)
                        .thenCompose(ignored -> read(exactSealed.runId()))
                        .thenApply(after -> after.map(value -> value.value().equals(retired)
                                        ? ProviderMutationResultV1.appliedExact(exactSealed)
                                        : ProviderMutationResultV1.<KafkaRunRootSnapshotV1>fencedOrConflict())
                                .orElseGet(ProviderMutationResultV1::fencedOrConflict));
            });
        });
        return work.exceptionally(failure -> ProviderMutationResultV1.outcomeUnknown())
                .thenApply(value -> value);
    }

    @Override
    public CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> createRoot(KafkaRunRootSnapshotV1 active) {
        var candidate = candidate(active);
        if (active.predecessorRunId().isPresent()) {
            throw new IllegalArgumentException("genesis cannot have a predecessor");
        }
        return guarded(candidate, Optional.empty(), () -> verifier.requireNative(candidate)
                .thenCompose(ignored -> prewrite(candidate))
                .thenCompose(ignored -> admitCandidate(candidate))
                .thenCompose(ignored -> promote(candidate)));
    }

    @Override
    public CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> createSuccessor(
            KafkaRunRootSnapshotV1 sealed, KafkaRunRootSnapshotV1 active) {
        var parent = new KafkaRunRootRecordV2(resource(sealed), sealed, true, Optional.empty());
        var child = candidate(active);
        parent.select(child);
        return guarded(child, Optional.of(parent), () -> verifier.requireNative(child)
                .thenCompose(ignored -> read(sealed.runId()))
                .thenCompose(observed -> {
                    var exact = observed.orElseThrow(() -> new IllegalStateException("successor parent is absent"));
                    if (!exact.value().root().equals(sealed)) {
                        throw new IllegalStateException("successor parent differs from the exact sealed root");
                    }
                    var selected = exact.value().select(child);
                    return prewrite(child)
                            .thenCompose(ignored -> admitCandidate(child))
                            .thenCompose(ignored -> selected.equals(exact.value())
                                    ? CompletableFuture.completedFuture(null)
                                    : cas(exact, selected));
                })
                .thenCompose(ignored -> promote(child)));
    }

    @Override
    public CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> sealRoot(
            KafkaRunRootSnapshotV1 active, KafkaRunRootSnapshotV1 sealed) {
        var before = candidate(active).admit();
        var candidate = before.seal(sealed);
        return guarded(candidate, Optional.empty(), () -> verifier.requireNative(candidate)
                .thenCompose(ignored -> promote(before))
                .thenCompose(ignored -> read(active.runId()))
                .thenCompose(observed -> {
                    var exact = observed.orElseThrow(() -> new IllegalStateException("sealed root is absent"));
                    if (!exact.value().root().equals(active)) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return cas(exact, exact.value().seal(sealed));
                }));
    }

    @Override
    public CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> sealRecoveredRun(
            KafkaRunRootSnapshotV1 active,
            KafkaOwnerAdmissionV1 closed,
            RunLedgerRecoveryProofV1 proof,
            long endOffset,
            OptionalLong inertFromEntryId) {
        Objects.requireNonNull(closed, "closed");
        Objects.requireNonNull(proof, "proof");
        var before = candidate(active).admit();
        if (!closed.closed()
                || !closed.scopeSha256().equals(Sha256Digest.hash(scope.encode()))
                || closed.owner().ownerEpoch() != active.creatorOwnerEpoch()
                || closed.owner().kafkaLeaderEpoch() != active.kafkaLeaderEpoch()
                || !closed.runs().contains(before.initialLink())
                || !proof.handle().runId().equals(active.runId())
                || !proof.handle().ledgerIdentity().equals(active.ledgerIdentity())
                || !proof.handle().providerScopeId().equals(active.providerScopeId())
                || !proof.handle().configurationDigest().equals(verifier.capabilitySha256())) {
            throw new IllegalArgumentException("crash run differs from closed membership or exact native proof");
        }
        var sealed = new KafkaRunRootSnapshotV1(
                active.bindingId(),
                active.topicIncarnation(),
                active.partitionId(),
                active.storageEpochId(),
                active.creatorOwnerEpoch(),
                active.kafkaLeaderEpoch(),
                active.providerScopeId(),
                active.runId(),
                active.ledgerIdentity(),
                active.kafkaStartOffset(),
                OptionalLong.of(endOffset),
                KafkaRunRootStateV1.SEALED,
                active.predecessorRunId());
        var candidate = before.recover(
                sealed,
                new KafkaRunRecoveryCutV1(
                        Sha256Digest.hash(closed.encode()), proof.lastAddConfirmed(), inertFromEntryId));
        return readClosedOwner(closed.owner().ownerEpoch())
                .thenCompose(archive -> {
                    if (!archive.equals(Optional.of(closed))) {
                        return CompletableFuture.completedFuture(
                                ProviderMutationResultV1.<KafkaRunRootSnapshotV1>fencedOrConflict());
                    }
                    return guarded(candidate, Optional.empty(), () -> verifier.requireRecovered(candidate, closed)
                            .thenCompose(ignored -> promote(before))
                            .thenCompose(ignored -> read(active.runId()))
                            .thenCompose(observed -> {
                                var exact = observed.orElseThrow(
                                        () -> new IllegalStateException("recovered root is absent"));
                                if (!exact.value().root().equals(active)) {
                                    return CompletableFuture.completedFuture(null);
                                }
                                return cas(
                                        exact,
                                        exact.value()
                                                .recover(
                                                        sealed,
                                                        candidate.recoveryCut().orElseThrow()));
                            }));
                })
                .exceptionally(failure -> ProviderMutationResultV1.outcomeUnknown());
    }

    private CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> guarded(
            KafkaRunRootRecordV2 candidate,
            Optional<KafkaRunRootRecordV2> parent,
            Supplier<CompletionStage<Void>> mutation) {
        var resources = parent.<List<PhysicalResourceIdV2>>map(value -> List.of(value.resource(), candidate.resource()))
                .orElseGet(() -> List.of(candidate.resource()));
        var candidateBytes = candidate.encode();
        var parentBytes = parent.map(KafkaRunRootRecordV2::encode).orElse(CanonicalBytes.empty());
        var facts = ByteBuffer.allocate(12 + candidateBytes.length() + parentBytes.length())
                .putInt(0x4d354b57)
                .putInt(candidateBytes.length())
                .put(candidateBytes.toByteArray())
                .putInt(parentBytes.length())
                .put(parentBytes.toByteArray())
                .array();
        var context = new Context(
                ProofBoundWriterClassV1.MANIFEST_SELECTOR_GENERATION_REPRESENTATION_V1,
                verifier.capabilitySha256(),
                candidate.initialLink().initialRootSha256(),
                Sha256Digest.hash(CanonicalBytes.copyOf(facts)));
        var work = terminal(candidate).thenCompose(known -> {
            if (known.isPresent()) {
                return reconcile(resources, context, known.orElseThrow());
            }
            return guard.execute(resources, context, () -> mutation.get()
                            .handle((ignored, failure) -> null)
                            .thenCompose(ignored -> terminal(candidate))
                            .thenApply(decision -> new Completion<>(decision, decision.map(Terminal::proof))))
                    .thenCompose(result -> {
                        if (!result.mutationInvoked()) {
                            return CompletableFuture.completedFuture(
                                    ProviderMutationResultV1.<KafkaRunRootSnapshotV1>fencedOrConflict());
                        }
                        var decision = result.value().flatMap(value -> value);
                        return decision.isEmpty()
                                ? CompletableFuture.completedFuture(
                                        ProviderMutationResultV1.<KafkaRunRootSnapshotV1>outcomeUnknown())
                                : reconcile(resources, context, decision.orElseThrow());
                    });
        });
        // Caller cancellation is only observer cancellation; the admitted mutation and its cleanup continue.
        return work.exceptionally(failure -> ProviderMutationResultV1.outcomeUnknown())
                .thenApply(value -> value);
    }

    private CompletionStage<ProviderMutationResultV1<KafkaRunRootSnapshotV1>> reconcile(
            List<PhysicalResourceIdV2> resources, Context context, Terminal terminal) {
        return guard.reconcileTerminal(resources, context, terminal.proof())
                .thenApply(left -> left.isEmpty() ? terminal.result() : ProviderMutationResultV1.outcomeUnknown());
    }

    private CompletionStage<Optional<Terminal>> terminal(KafkaRunRootRecordV2 candidate) {
        return read(candidate.root().runId()).thenCompose(observed -> {
            if (observed.isPresent()) {
                var current = observed.orElseThrow().value();
                if (!current.initialLink().equals(candidate.initialLink())) {
                    return CompletableFuture.completedFuture(Optional.of(conflict(current.encode())));
                }
                if (current.retired()) {
                    return CompletableFuture.completedFuture(Optional.of(conflict(current.encode())));
                }
                if (candidate.root().state() == KafkaRunRootStateV1.SEALED) {
                    return CompletableFuture.completedFuture(
                            current.root().state() == KafkaRunRootStateV1.SEALED
                                    ? Optional.of(
                                            current.root().equals(candidate.root())
                                                            && current.recoveryCut()
                                                                    .equals(candidate.recoveryCut())
                                                    ? applied(current.root(), current.encode())
                                                    : conflict(current.encode()))
                                    : Optional.empty());
                }
                return selected(current).thenCompose(admitted -> {
                    if (admitted) {
                        return CompletableFuture.completedFuture(Optional.of(
                                current.root().equals(candidate.root())
                                        ? applied(current.root(), current.encode())
                                        : conflict(current.encode())));
                    }
                    return rejectedChoice(candidate);
                });
            }
            return rejectedChoice(candidate);
        });
    }

    private CompletionStage<Optional<Terminal>> rejectedChoice(KafkaRunRootRecordV2 candidate) {
        return readOwnerAdmission().thenCompose(current -> {
            if (current.isEmpty()) {
                return CompletableFuture.completedFuture(Optional.of(conflict(candidate.encode())));
            }
            var owner = current.orElseThrow();
            boolean canAttach = !owner.closed()
                    && owner.owner().ownerEpoch() == candidate.root().creatorOwnerEpoch()
                    && owner.owner().kafkaLeaderEpoch() == candidate.root().kafkaLeaderEpoch()
                    && candidate.root().predecessorRunId().equals(owner.tail().map(Link::runId))
                    && owner.runs().size() < KafkaOwnerAdmissionV1.MAX_RUNS;
            if (!canAttach) {
                return CompletableFuture.completedFuture(Optional.of(conflict(owner.encode())));
            }
            return candidate.root().predecessorRunId().isEmpty()
                    ? CompletableFuture.completedFuture(Optional.empty())
                    : read(candidate.root().predecessorRunId().orElseThrow()).thenApply(parent -> parent.filter(
                                    value -> value.value().retired())
                            .map(value -> conflict(value.value().encode())));
        });
    }

    private CompletionStage<Boolean> selected(KafkaRunRootRecordV2 candidate) {
        if (candidate.retired()) {
            return CompletableFuture.completedFuture(false);
        }
        return guard.isRootMetadataReadable(candidate.resource())
                .thenCompose(readable -> readable
                        ? admissionFor(candidate.root().creatorOwnerEpoch()).thenApply(owner -> owner.filter(
                                        value -> value.runs().contains(candidate.initialLink()))
                                .isPresent())
                        : CompletableFuture.completedFuture(false));
    }

    private CompletionStage<Void> prewrite(KafkaRunRootRecordV2 candidate) {
        return create(nativeRootKey(candidate.root().runId()), candidate.encode())
                .thenCompose(ignored -> read(candidate.root().runId()))
                .thenAccept(observed -> {
                    if (observed.isEmpty()
                            || !observed.orElseThrow().value().initialLink().equals(candidate.initialLink())) {
                        throw new IllegalStateException(
                                "native root prewrite is absent or has another initial identity");
                    }
                });
    }

    private CompletionStage<Void> promote(KafkaRunRootRecordV2 candidate) {
        return read(candidate.root().runId()).thenCompose(observed -> {
            var current = observed.orElseThrow(() -> new IllegalStateException("selected run prewrite is absent"));
            if (!current.value().initialLink().equals(candidate.initialLink())) {
                throw new IllegalStateException("selected run initial identity changed");
            }
            if (current.value().admitted()) {
                return CompletableFuture.completedFuture(null);
            }
            return selected(current.value()).thenCompose(chosen -> {
                if (!chosen) {
                    throw new IllegalStateException("run prewrite was not selected by its parent or genesis");
                }
                return cas(current, current.value().admit());
            });
        });
    }

    private CompletionStage<Optional<Stored>> read(StorageRunId run) {
        String key = nativeRootKey(run);
        return client.read(key)
                .thenApply(observed -> observed.map(nativeValue -> {
                    var value = KafkaRunRootRecordV2.decode(nativeValue.storedBytes());
                    if (!nativeValue.key().equals(key)
                            || !value.root().runId().equals(run)
                            || !value.resource().namespace().equals(binding.physicalNamespace())
                            || !Scope.of(value.root()).equals(scope)) {
                        throw new IllegalStateException("native run root differs from its exact configured route");
                    }
                    return new Stored(nativeValue, value);
                }));
    }

    private CompletionStage<Void> create(String key, CanonicalBytes bytes) {
        return client.createIfAbsent(key, bytes).handle((ignored, failure) -> null);
    }

    private CompletionStage<Void> cas(Stored before, KafkaRunRootRecordV2 after) {
        return client.compareAndSet(
                        before.nativeValue().key(),
                        after.encode(),
                        before.nativeValue().versionId())
                .handle((ignored, failure) -> null);
    }

    private KafkaRunRootRecordV2 candidate(KafkaRunRootSnapshotV1 root) {
        resource(root);
        return KafkaRunRootRecordV2.pending(binding.physicalNamespace(), root);
    }

    private PhysicalResourceIdV2.BookKeeperLedger resource(KafkaRunRootSnapshotV1 root) {
        if (!Scope.of(root).equals(scope)) {
            throw new IllegalArgumentException("run root belongs to another protocol scope");
        }
        return new PhysicalResourceIdV2.BookKeeperLedger(
                binding.physicalNamespace(), root.ledgerIdentity().ledgerId());
    }

    private static Terminal conflict(CanonicalBytes proof) {
        return new Terminal(ProviderMutationResultV1.fencedOrConflict(), Sha256Digest.hash(proof));
    }

    private static Terminal applied(KafkaRunRootSnapshotV1 root, CanonicalBytes proof) {
        return new Terminal(ProviderMutationResultV1.appliedExact(root), Sha256Digest.hash(proof));
    }
}
