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
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.metadata.oxia.v2.retention.OxiaPhysicalMemberInventoryV2.Member;
import com.nereusstream.metadata.oxia.v2.retention.OxiaQuotaTargetDeleteStoreV2;
import com.nereusstream.pulsar.offload.PulsarNativeOffloadAttemptStoreV2.Original;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.Body;
import com.nereusstream.pulsar.offload.PulsarOffloadObjectStoreV1.ImmutableObject;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectIdentityKind;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.ObjectVersion;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.BirthResult;
import com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCoordinatorV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.PhysicalDeleteTargetV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.TargetDeleteAuthorityStateV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityStateMachineV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.PositionDomain;
import com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.Role;
import com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2.Slot;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import com.nereusstream.storage.object.retention.M5ClosedWriterRegistryV1;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerV1;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedObjectReadIdentity;
import org.apache.bookkeeper.mledger.SourceSafeLedgerOffloader.OffloadedReadSourceIdentity;

/** Birth, exact version proof and reference admission for the original native NPD1/NPO1 pair. */
final class PulsarNativeOffloadBirthAdmissionV2 {
    private static final int PROOF_RANGE = 1024 * 1024;
    private final PulsarNativeBindingV1 binding;
    private final PulsarNativeReadCellBudgetV1 budget;
    private final S3PulsarOffloadObjectStoreV1.MaterializationConnection connection;
    private final OxiaQuotaTargetDeleteStoreV2 route;
    private final PulsarOffloadObjectStoreV1 objectStore;
    private final M5ClosedWriterRegistryV1 writers;
    private final PulsarNativeOffloadAttemptStoreV2 attempts;
    private final Supplier<CompletionStage<Void>> requireNativeOwnership;
    private final CanonicalBytes originalCellOwner;
    private final Map<String, PendingOutput> ownedOutputs = new LinkedHashMap<>();
    private CompletableFuture<Void> closeAttempt;
    private UUID originalAttempt;
    private boolean running;
    private boolean draining;

    PulsarNativeOffloadBirthAdmissionV2(
            PulsarNativeBindingV1 binding,
            PulsarNativeReadCellBudgetV1 budget,
            S3PulsarOffloadObjectStoreV1.MaterializationConnection connection,
            OxiaQuotaTargetDeleteStoreV2 route,
            PulsarOffloadObjectStoreV1 objectStore,
            M5ClosedWriterRegistryV1 writers,
            PulsarNativeOffloadAttemptStoreV2 attempts,
            Supplier<CompletionStage<Void>> requireNativeOwnership) {
        this.binding = binding;
        this.budget = budget;
        this.connection = connection;
        this.route = route;
        this.objectStore = objectStore;
        this.writers = writers;
        this.attempts = attempts;
        this.requireNativeOwnership = requireNativeOwnership;
        originalCellOwner = budget.nativeOwnerIdentity(binding);
    }

    PulsarNativeOffloadAttemptStoreV2 attempts() {
        return attempts;
    }

    synchronized void begin(UUID attempt) {
        if (draining || running || originalAttempt != null && !originalAttempt.equals(attempt)) {
            throw new IllegalStateException("A different or unsettled Native offload still owns the original scope");
        }
        originalAttempt = attempt;
        running = true;
    }

    synchronized void finish(UUID attempt, Throwable failure) {
        if (!running || !attempt.equals(originalAttempt)) {
            throw new IllegalStateException("Native offload completion differs from the original attempt");
        }
        running = false;
        if (failure == null || ownedOutputs.isEmpty()) {
            originalAttempt = null;
            ownedOutputs.clear();
        }
    }

    /** Native ledger close has drained the original callbacks. Reconcile accepted outputs without dispatching. */
    synchronized CompletableFuture<Void> drain() {
        draining = true;
        if (running) {
            return CompletableFuture.failedFuture(new IllegalStateException("Original Native offload has not drained"));
        }
        if (closeAttempt == null || closeAttempt.isCompletedExceptionally()) {
            CompletionStage<Void> checks = CompletableFuture.completedFuture(null);
            for (var output : List.copyOf(ownedOutputs.values())) {
                checks = checks.thenCompose(unused -> reconcile(output)).thenRun(() -> release(output));
            }
            closeAttempt = checks.thenRun(() -> {
                        synchronized (this) {
                            originalAttempt = null;
                        }
                    })
                    .toCompletableFuture();
        }
        return closeAttempt.copy();
    }

    private record PendingOutput(String claim, M5MaterializationWorkerV1 worker, Slot slot, ObjectIdentity body) {}

    private synchronized void own(PendingOutput output) {
        var previous = ownedOutputs.putIfAbsent(output.claim(), output);
        if (previous != null && !previous.equals(output)) {
            throw new IllegalStateException("Original native Object body changed");
        }
    }

    private synchronized void release(PendingOutput output) {
        ownedOutputs.remove(output.claim(), output);
    }

    private CompletionStage<Void> reconcile(PendingOutput output) {
        var quota = route.quota();
        return requireRetainedCell()
                .thenCompose(unused -> quota.recover())
                .thenCompose(recovered -> {
                    if (!recovered) {
                        throw new IllegalStateException("Original native offload quota remains unresolved");
                    }
                    return quota.readBirth(output.claim());
                })
                .thenCompose(found -> {
                    if (found.isEmpty()) {
                        // No durable reservation or arm survived the original call; no PUT was admitted.
                        return CompletableFuture.completedFuture(null);
                    }
                    var birth = found.orElseThrow();
                    requireBirth(output, birth);
                    return switch (birth.state()) {
                        // Keep the bounded reservation for the same original attempt; a successor may arm it.
                        case RESERVED, CANCELLED_NO_DISPATCH -> CompletableFuture.completedFuture(null);
                        case WRITE_ARMED, ASSIGNED ->
                            resolve(output.body(), birth).thenCompose(proof -> verifyFullBody(output.body(), proof)
                                    .thenCompose(
                                            unused -> assignAndRegister(output, proof, this::requireRetainedCell)));
                    };
                });
    }

    private static void requireBirth(PendingOutput output, M5ObjectBirthCreditV3 birth) {
        if (!birth.worker().equals(output.worker())
                || !birth.slot().equals(output.slot())
                || birth.output().filter(value -> !value.equals(output.body())).isPresent()) {
            throw new IllegalStateException("Original native offload quota facts changed");
        }
    }

    private CompletionStage<ImmutableObject> resolve(ObjectIdentity expected, M5ObjectBirthCreditV3 birth) {
        if (birth.state() == M5ObjectBirthCreditV3.State.ASSIGNED) {
            var resource = birth.resource().orElseThrow();
            if (!resource.namespace().equals(connection.binding().physicalNamespace())
                    || !resource.objectKey().value().equals(expected.key())
                    || resource.identityKind() != ObjectIdentityKind.IMMUTABLE_VERSION) {
                throw new IllegalStateException("Original native offload assigned resource differs");
            }
            return CompletableFuture.completedFuture(new ImmutableObject(
                    "version:" + resource.immutableIdentity().value(),
                    expected.bodyLength(),
                    expected.bodySha256().toHex()));
        }
        return objectStore.head(expected.key());
    }

    /** The native complete marker refers to these already assigned exact versions. No Provider HEAD is needed. */
    CompletionStage<OffloadedReadSourceIdentity> capture(long ledgerId, UUID attempt, PulsarOffloadKeysV1 keys) {
        var attemptKey = attempts.attemptKey(ledgerId, attempt);
        var attemptHash = Sha256Digest.hash(CanonicalBytes.copyOf(attemptKey.getBytes(StandardCharsets.UTF_8)));
        var profile = connection.profile();
        return attempts.readBirthOwner(attemptKey).thenCompose(worker -> {
            if (!worker.identity().protocolCellSha256().equals(profile.protocolCellSha256())
                    || !worker.identity()
                            .providerScopeSha256()
                            .equals(profile.scopeId().digest())
                    || !worker.identity().binding().equals(binding.bindingIdentity())
                    || !worker.nativeNamespaceBindingSha256()
                            .equals(Sha256Digest.hash(connection.binding().encode()))
                    || !worker.planRootSha256().equals(attemptHash)
                    || !worker.taskIdSha256().equals(attemptHash)) {
                throw new IllegalStateException("Published native offload attempt belongs to another scope");
            }
            return route.quota().snapshot().thenCompose(snapshot -> {
                var scope = snapshot.head().scope();
                return captureOutput(scope, worker, new Slot(Role.INDEX, 0, keys.rootKey()))
                        .thenCombine(
                                captureOutput(scope, worker, new Slot(Role.PAYLOAD, 0, keys.dataKey())),
                                (root, data) -> new OffloadedReadSourceIdentity(ledgerId, attempt, root, data));
            });
        });
    }

    private CompletionStage<OffloadedObjectReadIdentity> captureOutput(
            Sha256Digest scope, M5MaterializationWorkerV1 worker, Slot slot) {
        var claim = M5ObjectBirthCreditV3.key(scope, worker, slot);
        return route.quota().readBirth(claim).thenApply(found -> {
            var birth = found.orElseThrow(() -> new IllegalStateException("Published native offload birth is missing"));
            var body = birth.output().orElseThrow();
            var resource = birth.resource().orElseThrow();
            if (!birth.worker().equals(worker)
                    || !birth.slot().equals(slot)
                    || !resource.namespace().equals(connection.binding().physicalNamespace())
                    || body.bodyLength() > connection.profile().maximumObjectBytes()
                    || !body.key().startsWith(connection.profile().exclusiveNamespacePrefix() + "/")) {
                throw new IllegalStateException("Published native offload has no exact assigned Object version");
            }
            return new OffloadedObjectReadIdentity(
                    body.key(),
                    "version:" + resource.immutableIdentity().value(),
                    body.bodyLength(),
                    body.bodySha256().toHex());
        });
    }

    CompletionStage<PulsarSealedLedgerPublisherV1.ImmutableOutputPublisher> open(Original original) {
        var profile = connection.profile();
        var attemptHash = Sha256Digest.hash(CanonicalBytes.copyOf(original.key().getBytes(StandardCharsets.UTF_8)));
        return requireCurrent()
                .thenCompose(unused -> budget.captureOwnerEpoch(binding))
                .thenCompose(owner -> {
                    // The common quota wire token names this durable native attempt, not an NMS1 plan.
                    var candidate = new M5MaterializationWorkerV1(
                            new IdentityEnvelope(
                                    profile.protocolCellSha256(),
                                    profile.scopeId().digest(),
                                    binding.bindingIdentity(),
                                    owner.generation(),
                                    owner.generation(),
                                    owner.generation(),
                                    writers.capability()),
                            Sha256Digest.hash(owner.nativeOwner()),
                            Sha256Digest.hash(connection.binding().encode()),
                            binding.authority().selector().value().canonicalStoredDigest(),
                            original.fingerprint(),
                            attemptHash,
                            attemptHash);
                    return attempts.bindBirthOwner(original, candidate);
                })
                .thenApply(worker -> (key, body, dispatch) -> publish(original, worker, key, body, dispatch));
    }

    CompletionStage<Void> requireCurrent() {
        return requireNativeOwnership.get().thenCompose(unused -> requireRetainedCell());
    }

    private CompletionStage<Void> requireRetainedCell() {
        return budget.requireNativeOwner(binding, originalCellOwner)
                .thenCompose(unused -> binding.revalidate())
                .thenCompose(unused -> connection.requireProfileCurrent());
    }

    private CompletionStage<ImmutableObject> publish(
            Original original,
            M5MaterializationWorkerV1 worker,
            String key,
            Body body,
            Supplier<CompletionStage<ImmutableObject>> dispatch) {
        if (!key.startsWith(connection.profile().exclusiveNamespacePrefix() + "/")
                || body.bytes() > connection.profile().maximumObjectBytes()) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Native offload Object is outside its admitted provider scope"));
        }
        var role = original.keys().dataKey().equals(key) ? Role.PAYLOAD : Role.INDEX;
        var slot = new Slot(role, 0, key);
        var expected = new ObjectIdentity(
                key, body.bytes(), Sha256Digest.copyOf(HexFormat.of().parseHex(body.sha256())));
        var quota = route.quota();
        return attempts.bindBody(original, key, body)
                .thenCompose(unused -> requireCurrent())
                .thenCompose(unused -> quota.snapshot())
                .thenCompose(snapshot -> {
                    String claim = M5ObjectBirthCreditV3.key(snapshot.head().scope(), worker, slot);
                    var output = new PendingOutput(claim, worker, slot, expected);
                    return quota.readBirth(claim)
                            .thenCompose(existing -> {
                                own(output);
                                if (existing.isPresent()) {
                                    var found = existing.orElseThrow();
                                    requireBirth(output, found);
                                    return switch (found.state()) {
                                        case CANCELLED_NO_DISPATCH ->
                                            CompletableFuture.failedFuture(
                                                    new IllegalStateException("Original native offload was cancelled"));
                                        case WRITE_ARMED, ASSIGNED -> resolve(expected, found);
                                        case RESERVED -> armAndDispatch(output, dispatch);
                                    };
                                }
                                return retryQuota(() -> quota.reserveBirth(worker, slot), 8)
                                        .thenCompose(reserved -> {
                                            if (reserved == BirthResult.EXHAUSTED) {
                                                release(output);
                                            }
                                            if (reserved != BirthResult.RESERVED) {
                                                throw new IllegalStateException(
                                                        "Native offload birth was not reserved: " + reserved);
                                            }
                                            return armAndDispatch(output, dispatch);
                                        });
                            })
                            .thenCompose(proof -> verifyFullBody(expected, proof)
                                    .thenCompose(unused -> assignAndRegister(output, proof, this::requireCurrent))
                                    .thenApply(unused -> {
                                        release(output);
                                        return proof;
                                    }));
                });
    }

    private CompletionStage<ImmutableObject> armAndDispatch(
            PendingOutput output, Supplier<CompletionStage<ImmutableObject>> dispatch) {
        return requireCurrent()
                .thenCompose(unused -> retryQuota(() -> route.quota().armBirth(output.claim(), output.body()), 8))
                .thenCompose(armed -> {
                    if (armed != BirthResult.WRITE_ARMED) {
                        throw new IllegalStateException("Native offload birth was not armed: " + armed);
                    }
                    return requireCurrent().thenCompose(unused -> dispatch.get());
                });
    }

    private CompletionStage<Void> assignAndRegister(
            PendingOutput output, ImmutableObject proof, Supplier<CompletionStage<Void>> requireAuthority) {
        var resource = new ObjectVersion(
                connection.binding().physicalNamespace(),
                CanonicalUtf8.fromString(output.body().key()),
                ObjectIdentityKind.IMMUTABLE_VERSION,
                CanonicalUtf8.fromString(proof.immutableVersion().substring("version:".length())));
        var target = PhysicalDeleteTargetV1.create(resource);
        var profile = connection.profile();
        var enrollment = PulsarNativeSealedPlainAdmissionV2.offloadedEnrollment(profile);
        var candidate = M5TargetDeleteAuthorityStateMachineV1.open(
                target, enrollment, output.worker().planRootSha256());
        var coordinator = new M5TargetDeleteAuthorityCoordinatorV1(route);
        return requireAuthority
                .get()
                .thenCompose(unused -> retryQuota(() -> route.quota().assignBirth(output.claim(), resource), 8))
                .thenCompose(assigned -> {
                    if (assigned != BirthResult.ASSIGNED) {
                        throw new IllegalStateException("Original Object version was not assigned: " + assigned);
                    }
                    return coordinator
                            .create(candidate)
                            .thenCompose(created -> coordinator.inspect(resource.authorityKey()));
                })
                .thenCompose(observed -> {
                    var actual = observed.flatMap(value -> value.fullAuthority())
                            .orElseThrow(
                                    () -> new IllegalStateException("Original native offload target was not enrolled"));
                    // Registering a member and taking reference tickets legitimately advance OPEN revisions.
                    // The original ASSIGNED birth above still binds the exact quota credit and immutable version.
                    if (actual.state() != TargetDeleteAuthorityStateV1.OPEN_V1
                            || !actual.target().equals(candidate.target())
                            || !actual.writerEnrollment().equals(candidate.writerEnrollment())
                            || actual.authorityRevision() == 1 && !actual.equals(candidate)) {
                        throw new IllegalStateException(
                                "Original native offload target authority differs or is fenced");
                    }
                    return route.members()
                            .register(
                                    resource,
                                    profile.protocolCellSha256(),
                                    new Member(
                                            PositionDomain.PULSAR_ENTRY,
                                            profile.scopeId().digest(),
                                            binding.bindingIdentity()),
                                    profile.providerConfigurationSha256(),
                                    output.worker().nativeWorkerIdentitySha256());
                })
                .thenCompose(unused -> route.readResourceFact(resource))
                .thenCompose(unused -> requireAuthority.get());
    }

    private CompletionStage<Void> verifyFullBody(ObjectIdentity expected, ImmutableObject actual) {
        if (!actual.immutableVersion().startsWith("version:")
                || actual.bytes() != expected.bodyLength()
                || !actual.sha256().equals(expected.bodySha256().toHex())) {
            return CompletableFuture.failedFuture(new IllegalStateException("Original immutable Object body changed"));
        }
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
        CompletionStage<Void> checks = CompletableFuture.completedFuture(null);
        for (long from = 0; from < expected.bodyLength(); from += PROOF_RANGE) {
            long offset = from;
            int length = (int) Math.min(PROOF_RANGE, expected.bodyLength() - offset);
            checks = checks.thenCompose(unused -> objectStore.readRange(expected.key(), actual, offset, length))
                    .thenAccept(bytes -> {
                        if (bytes.length != length) {
                            throw new IllegalStateException("Original Object returned a short proof range");
                        }
                        digest.update(bytes);
                    });
        }
        return checks.thenRun(() -> {
            if (!Sha256Digest.copyOf(digest.digest()).equals(expected.bodySha256())) {
                throw new IllegalStateException("Original immutable Object full proof differs");
            }
        });
    }

    private static CompletionStage<BirthResult> retryQuota(
            Supplier<CompletionStage<BirthResult>> mutation, int remaining) {
        return mutation.get()
                .thenCompose(result -> result == BirthResult.RETRY && remaining > 1
                        ? retryQuota(mutation, remaining - 1)
                        : CompletableFuture.completedFuture(result));
    }
}
