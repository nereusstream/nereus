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

package com.nereusstream.metadata.oxia.v2.retention;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.objectwal.OxiaCanonicalControlMetadataStore;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.testing.DeterministicOxiaConditionalClient.MutationMode;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.MutationOutcome;
import com.nereusstream.storage.object.control.ControlMutationOutcome;
import com.nereusstream.storage.object.materialization.M5MaterializationAdmissionV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import com.nereusstream.storage.object.materialization.M5MaterializationCoordinatorV1;
import com.nereusstream.storage.object.materialization.M5MaterializationKeysV1;
import com.nereusstream.storage.object.materialization.M5MaterializationPlannerV1;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1;
import com.nereusstream.storage.object.materialization.M5PulsarCoverageV2;
import com.nereusstream.storage.object.read.control.M4ReadControlCodecV1;
import com.nereusstream.storage.object.read.control.M4ReadControlKeysV1;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.QuiescenceProofHead;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import com.nereusstream.storage.object.retention.M5BindingAuthorityCodecV1;
import com.nereusstream.storage.object.retention.M5BindingBootstrapV1;
import com.nereusstream.storage.object.retention.M5LogicalTrimCoordinatorV1;
import com.nereusstream.storage.object.retention.M5RetentionCodecV1;
import com.nereusstream.storage.object.retention.M5RetentionKeysV1;
import com.nereusstream.storage.object.retention.M5RetentionPlannerV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.BatchMetadataStateV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.ReferenceKindV1;
import com.nereusstream.storage.object.retention.M5RetentionRecordsV1.RetiredSourceRetirementBatchTombstoneV1;
import com.nereusstream.storage.object.retention.M5RetiredBatchHistoryV2;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class OxiaBindingLifecycleMetadataStoreV2Test {
    private static final String ROOT = "/nereus/cells/a";
    private static final BindingIdentity BINDING =
            new BindingIdentity(new TopicBindingId(digest("binding")), digest("incarnation"), digest("storage"));
    private static final CapabilityBinding CAPABILITY = new CapabilityBinding(1, digest("capability"));
    private static final M4ReadControlKeysV1 KEYS = new M4ReadControlKeysV1(7, BINDING);

    @Test
    void nativeFactRetainsPhysicalKeyAndRejectsSameValueVersionChanges() {
        var backend = new DeterministicOxiaConditionalClient();
        var route = new OxiaBindingLifecycleMetadataStoreV2(backend, ROOT, 7, BINDING);
        var head = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 1, List.of(), List.of()));
        backend.seed(route.nativeKey(KEYS.proofHead()), head, 9);
        var captured = route.read(KEYS.proofHead()).toCompletableFuture().join().orElseThrow();
        var nativeFact = route.nativeFact(captured).toCompletableFuture().join();
        assertThat(new Oxia09ExactMetadataTransactionStoreV1(backend)
                        .read(nativeFact.key())
                        .toCompletableFuture()
                        .join())
                .contains(nativeFact);
        assertThat(nativeFact.key()).isEqualTo(route.nativeKey(KEYS.proofHead()));
        assertThat(nativeFact.metadataVersion()).isNotEqualTo(captured.metadataVersion());
        backend.seed(route.nativeKey(KEYS.proofHead()), head, 10);
        assertThatThrownBy(
                        () -> route.nativeFact(captured).toCompletableFuture().join())
                .hasRootCauseMessage("native lifecycle fact differs from its exact routed observation");
    }

    @Test
    void typedRetentionRecordsSurviveReconnectAndLostTrimResponse() {
        var client = new DeterministicOxiaConditionalClient();
        var cell = digest("retention-cell");
        var route = new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, 7, BINDING, cell);
        enrollTrimRoute(route);
        var head = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 1, List.of(), List.of()));
        assertThat(route.rawControlMetadata().putIfAbsent(KEYS.proofHead(), head))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        var stored = route.read(KEYS.proofHead()).toCompletableFuture().join().orElseThrow();
        var fact = new M5RetentionRecordsV1.AuthorityFactV1(
                stored.key(), stored.metadataVersion(), stored.canonicalStoredSha256());
        var snapshot = typedFloor(BINDING, cell, fact);
        var keys = new M5RetentionKeysV1(7, BINDING);
        String snapshotKey = keys.floorSnapshot(snapshot.snapshotRootSha256());
        assertThat(route.compareAndSet(Optional.empty(), snapshotKey, M5RetentionCodecV1.encodeSnapshot(snapshot))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);
        var applied = new M5LogicalTrimCoordinatorV1(route)
                .advance(keys.trimFrontier(), snapshot)
                .toCompletableFuture()
                .join();
        assertThat(applied.outcome()).isEqualTo(M5LogicalTrimCoordinatorV1.Outcome.APPLIED_EXACT);
        assertThat(new M5LogicalTrimCoordinatorV1(route)
                        .advance(keys.trimFrontier(), snapshot)
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(M5LogicalTrimCoordinatorV1.Outcome.EXISTING_EXACT);
        assertThat(applied.exactFrontier()
                        .orElseThrow()
                        .pulsarContext()
                        .orElseThrow()
                        .newFrontier())
                .isEqualTo(new M5PulsarCoverageV2.Position(91, 3));
        var reopened = new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, 7, BINDING, cell);
        assertThat(M5RetentionCodecV1.decodeSnapshot(reopened.read(snapshotKey)
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .canonicalStoredBytes()))
                .isEqualTo(snapshot);
        assertThat(reopened.readLogicalTrimFrontier()
                        .toCompletableFuture()
                        .join()
                        .orElseThrow())
                .isEqualTo(applied.exactFrontier().orElseThrow());
        var next =
                typedFloor(BINDING, cell, fact, snapshot.minimumPulsarFloor(), new M5PulsarCoverageV2.Position(7, 2));
        reopened.compareAndSet(
                        Optional.empty(),
                        keys.floorSnapshot(next.snapshotRootSha256()),
                        M5RetentionCodecV1.encodeSnapshot(next))
                .toCompletableFuture()
                .join();
        var advanced = new M5LogicalTrimCoordinatorV1(reopened)
                .advance(keys.trimFrontier(), next)
                .toCompletableFuture()
                .join();
        assertThat(advanced.exactFrontier().orElseThrow().generation()).isEqualTo(2);
        assertThat(advanced.exactFrontier()
                        .orElseThrow()
                        .pulsarContext()
                        .orElseThrow()
                        .newFrontier())
                .isEqualTo(next.minimumPulsarFloor());
        var exact = reopened.read(snapshotKey).toCompletableFuture().join().orElseThrow();
        assertThatThrownBy(() -> reopened.compareAndSet(Optional.of(exact), snapshotKey, exact.canonicalStoredBytes()))
                .hasMessageContaining("create-only");
        var coverage = M5MaterializationRecordsV1.ProtocolCoverage.pulsar(
                snapshot.pulsarContext().orElseThrow().coverage());
        var rows = java.util.Arrays.stream(ReferenceKindV1.values())
                .map(kind -> new M5RetentionRecordsV1.ReferenceObservationV1(
                        kind,
                        fact,
                        digest("synthetic-route-target"),
                        coverage,
                        M5RetentionRecordsV1.ReferenceDispositionV1.ABSENT,
                        true))
                .toList();
        var scans = java.util.Arrays.stream(ReferenceKindV1.values())
                .map(kind -> new M5RetentionRecordsV1.ReferenceScanSummaryV1(kind, 1, 1, 64, true))
                .toList();
        var proof = M5RetentionCodecV1.finalizeProof(new M5RetentionRecordsV1.ReferenceFreeProofV1(
                snapshot.identity(),
                M5RetentionRecordsV1.ReferenceTargetKindV1.UNSELECTED_OUTPUT,
                digest("synthetic-route-target"),
                coverage,
                fact,
                fact,
                fact,
                snapshot.snapshotRootSha256(),
                M5RetentionCodecV1.calculateObservationsRoot(rows),
                List.of(),
                fact,
                fact,
                fact,
                fact,
                1,
                2,
                scans,
                rows,
                digest("placeholder")));
        var proofKey = keys.referenceFreeProof(proof.proofSha256());
        assertThat(reopened.compareAndSet(
                                Optional.empty(), proofKey, M5RetentionCodecV1.encodeReferenceFreeProof(proof))
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        assertThat(M5RetentionCodecV1.decodeReferenceFreeProof(route.read(proofKey)
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .canonicalStoredBytes()))
                .isEqualTo(proof);
        var exactProof = route.read(proofKey).toCompletableFuture().join().orElseThrow();
        assertThatThrownBy(
                        () -> route.compareAndSet(Optional.of(exactProof), proofKey, exactProof.canonicalStoredBytes()))
                .hasMessageContaining("create-only");
    }

    @Test
    void legacySeparateFrontierMigratesIntoTheSameAuthorityWithoutChangingItsBytes() {
        var backend = new DeterministicOxiaConditionalClient();
        var route = new OxiaBindingLifecycleMetadataStoreV2(backend, ROOT, 7, BINDING, digest("retention-cell"));
        enrollTrimRoute(route);
        var legacyAuthority =
                route.read(KEYS.selector()).toCompletableFuture().join().orElseThrow();
        assertThat(M5BindingAuthorityCodecV1.decodeAuthority(legacyAuthority.canonicalStoredBytes())
                        .wireVersion())
                .isLessThan(5);
        var head = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 1, List.of(), List.of()));
        // Pre-upgrade state: the old route stored its frontier separately from the selector.
        backend.createIfAbsent(route.nativeKey(KEYS.proofHead()), head)
                .toCompletableFuture()
                .join();
        var snapshot = storedTypedFloor(
                route, route.read(KEYS.proofHead()).toCompletableFuture().join().orElseThrow());
        var frontier = new M5RetentionPlannerV1().plan(snapshot, Optional.empty());
        var bytes = M5RetentionCodecV1.encodeTrimFrontier(frontier);
        var keys = new M5RetentionKeysV1(7, BINDING);
        backend.createIfAbsent(route.nativeKey(keys.trimFrontier()), bytes)
                .toCompletableFuture()
                .join();
        assertThat(route.readLogicalTrimFrontier().toCompletableFuture().join()).contains(frontier);
        assertThat(new M5LogicalTrimCoordinatorV1(route)
                        .advance(keys.trimFrontier(), snapshot)
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(M5LogicalTrimCoordinatorV1.Outcome.EXISTING_EXACT);
        var migrated = M5BindingAuthorityCodecV1.decodeAuthority(route.read(KEYS.selector())
                .toCompletableFuture()
                .join()
                .orElseThrow()
                .canonicalStoredBytes());
        assertThat(migrated.wireVersion()).isEqualTo(5);
        assertThat(migrated.logicalTrim().orElseThrow().frontier()).contains(frontier);
        var reopened = new OxiaBindingLifecycleMetadataStoreV2(backend, ROOT, 7, BINDING, digest("retention-cell"));
        assertThat(reopened.readLogicalTrimFrontier().toCompletableFuture().join())
                .contains(frontier);
        assertThat(backend.read(route.nativeKey(keys.trimFrontier()))
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .storedBytes())
                .isEqualTo(bytes);
        assertThatThrownBy(() ->
                        reopened.read(keys.trimFrontier()).toCompletableFuture().join())
                .hasRootCauseMessage(
                        "logical trim frontier is stored in selector authority; use readLogicalTrimFrontier");
    }

    @Test
    void floorWriterCannotRacePastFinalTrimScan() {
        var backend = new DeterministicOxiaConditionalClient();
        var reached = new java.util.concurrent.CompletableFuture<Void>();
        var release = new java.util.concurrent.CompletableFuture<Void>();
        var keys = new M5RetentionKeysV1(7, BINDING);
        String nativeTrim = ROOT + "/" + KEYS.selector();
        var held = new OxiaConditionalClient() {
            @Override
            public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                return backend.read(key);
            }

            @Override
            public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes value) {
                return backend.createIfAbsent(key, value);
            }

            @Override
            public CompletionStage<Void> compareAndSet(String key, CanonicalBytes value, long version) {
                if (key.equals(nativeTrim)
                        && M5BindingAuthorityCodecV1.isAuthorityValue(value)
                        && M5BindingAuthorityCodecV1.decodeAuthority(value)
                                .logicalTrim()
                                .flatMap(
                                        com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2
                                                        .LogicalTrimStateV2::frontier)
                                .isPresent()) {
                    reached.complete(null);
                    return release.thenCompose(ignored -> backend.compareAndSet(key, value, version));
                }
                return backend.compareAndSet(key, value, version);
            }
        };
        var cell = digest("retention-cell");
        var route = new OxiaBindingLifecycleMetadataStoreV2(held, ROOT, 7, BINDING, cell);
        enrollTrimRoute(route);
        var head = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 1, List.of(), List.of()));
        route.rawControlMetadata().putIfAbsent(KEYS.proofHead(), head);
        var original = route.read(KEYS.proofHead()).toCompletableFuture().join().orElseThrow();
        var snapshot = typedFloor(
                BINDING,
                cell,
                new M5RetentionRecordsV1.AuthorityFactV1(
                        original.key(), original.metadataVersion(), original.canonicalStoredSha256()));
        route.compareAndSet(
                        Optional.empty(),
                        keys.floorSnapshot(snapshot.snapshotRootSha256()),
                        M5RetentionCodecV1.encodeSnapshot(snapshot))
                .toCompletableFuture()
                .join();
        var advance = new M5LogicalTrimCoordinatorV1(route)
                .advance(keys.trimFrontier(), snapshot)
                .toCompletableFuture();
        assertThat(reached).isCompleted();
        assertThat(advance).isNotDone();
        var changed = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 2, List.of(), List.of()));
        assertThat(route.rawControlMetadata().compareAndSet(KEYS.proofHead(), Optional.of(head), changed))
                .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(route.read(KEYS.proofHead()).toCompletableFuture().join()).contains(original);
        release.complete(null);
        assertThat(advance.join().exactFrontier()).isPresent();
        assertThat(route.readLogicalTrimFrontier()
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .pulsarContext()
                        .orElseThrow()
                        .newFrontier())
                .isEqualTo(snapshot.minimumPulsarFloor());
        assertThat(backend.read(route.nativeKey(keys.trimFrontier()))
                        .toCompletableFuture()
                        .join())
                .isEmpty();
    }

    @Test
    void admittedFloorWriterVetoesTrimAndVersionBarrierExcludesItsLatePayload() {
        var backend = new DeterministicOxiaConditionalClient();
        var entered = new java.util.concurrent.CompletableFuture<Void>();
        var release = new java.util.concurrent.CompletableFuture<Void>();
        var hold = new java.util.concurrent.atomic.AtomicBoolean(true);
        var port = new OxiaConditionalClient() {
            public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                return backend.read(key);
            }

            public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes bytes) {
                return backend.createIfAbsent(key, bytes);
            }

            public CompletionStage<Void> compareAndSet(String key, CanonicalBytes bytes, long version) {
                if (key.equals(ROOT + "/" + KEYS.proofHead()) && hold.compareAndSet(true, false)) {
                    entered.complete(null);
                    return release.thenCompose(ignored -> backend.compareAndSet(key, bytes, version));
                }
                return backend.compareAndSet(key, bytes, version);
            }
        };
        var route = new OxiaBindingLifecycleMetadataStoreV2(port, ROOT, 7, BINDING, digest("retention-cell"));
        enrollTrimRoute(route);
        var head = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 1, List.of(), List.of()));
        route.rawControlMetadata().putIfAbsent(KEYS.proofHead(), head);
        var before = route.read(KEYS.proofHead()).toCompletableFuture().join().orElseThrow();
        var snapshot = storedTypedFloor(route, before);
        var changed = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 2, List.of(), List.of()));
        var writer = route.compareAndSet(Optional.of(before), KEYS.proofHead(), changed)
                .toCompletableFuture();
        assertThat(entered).isCompleted();
        assertThat(writer).isNotDone();
        assertThat(trimTickets(route)).hasSize(1);
        assertThat(new M5LogicalTrimCoordinatorV1(route)
                        .advance(new M5RetentionKeysV1(7, BINDING).trimFrontier(), snapshot)
                        .toCompletableFuture()
                        .join()
                        .outcome())
                .isEqualTo(M5LogicalTrimCoordinatorV1.Outcome.CONFLICT);
        route.reconcileLogicalTrimWrites().toCompletableFuture().join();
        assertThat(trimTickets(route)).isEmpty();
        assertThat(writer).isNotDone();
        release.complete(null);
        assertThat(writer.join()).isEqualTo(MutationOutcome.DEFINITIVE_CONFLICT);
        var fenced = route.read(KEYS.proofHead()).toCompletableFuture().join().orElseThrow();
        assertThat(fenced.canonicalStoredBytes()).isEqualTo(head);
        assertThat(fenced.metadataVersion()).isNotEqualTo(before.metadataVersion());
        assertThat(route.readLogicalTrimFrontier().toCompletableFuture().join()).isEmpty();
    }

    @Test
    void unknownFloorWriteRetainsItsOriginalTicketUntilAnExactReread() {
        var backend = new DeterministicOxiaConditionalClient();
        var readsLost = new java.util.concurrent.atomic.AtomicInteger();
        var writes = new java.util.concurrent.atomic.AtomicInteger();
        var port = new OxiaConditionalClient() {
            public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                if (key.equals(ROOT + "/" + KEYS.proofHead()) && readsLost.getAndUpdate(v -> Math.max(0, v - 1)) > 0) {
                    return java.util.concurrent.CompletableFuture.failedFuture(
                            new java.io.IOException("lost exact read"));
                }
                return backend.read(key);
            }

            public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes bytes) {
                return backend.createIfAbsent(key, bytes);
            }

            public CompletionStage<Void> compareAndSet(String key, CanonicalBytes bytes, long version) {
                var actual = backend.compareAndSet(key, bytes, version);
                if (key.equals(ROOT + "/" + KEYS.proofHead())) {
                    writes.incrementAndGet();
                    return actual.thenCompose(ignored -> {
                        readsLost.set(2);
                        return java.util.concurrent.CompletableFuture.failedFuture(
                                new java.io.IOException("lost write reply"));
                    });
                }
                return actual;
            }
        };
        var route = new OxiaBindingLifecycleMetadataStoreV2(port, ROOT, 7, BINDING, digest("retention-cell"));
        enrollTrimRoute(route);
        var head = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 1, List.of(), List.of()));
        route.rawControlMetadata().putIfAbsent(KEYS.proofHead(), head);
        var before = route.read(KEYS.proofHead()).toCompletableFuture().join().orElseThrow();
        var changed = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 2, List.of(), List.of()));
        assertThat(route.compareAndSet(Optional.of(before), KEYS.proofHead(), changed)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.RESPONSE_UNKNOWN);
        assertThat(trimTickets(route)).hasSize(1);
        route.reconcileLogicalTrimWrites().toCompletableFuture().join();
        assertThat(trimTickets(route)).isEmpty();
        assertThat(writes.get()).isEqualTo(1);
        assertThat(route.read(KEYS.proofHead())
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .canonicalStoredBytes())
                .isEqualTo(changed);
    }

    @Test
    void cancelledFloorWriterObserverCannotReleaseItsOriginalTicket() {
        var backend = new DeterministicOxiaConditionalClient();
        var applied = new java.util.concurrent.CompletableFuture<Void>();
        var reply = new java.util.concurrent.CompletableFuture<Void>();
        var port = new OxiaConditionalClient() {
            public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                return backend.read(key);
            }

            public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes bytes) {
                return backend.createIfAbsent(key, bytes);
            }

            public CompletionStage<Void> compareAndSet(String key, CanonicalBytes bytes, long version) {
                return backend.compareAndSet(key, bytes, version).thenCompose(ignored -> {
                    if (key.equals(ROOT + "/" + KEYS.proofHead())) {
                        applied.complete(null);
                        return reply;
                    }
                    return java.util.concurrent.CompletableFuture.completedFuture(null);
                });
            }
        };
        var route = new OxiaBindingLifecycleMetadataStoreV2(port, ROOT, 7, BINDING, digest("retention-cell"));
        enrollTrimRoute(route);
        var head = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 1, List.of(), List.of()));
        route.rawControlMetadata().putIfAbsent(KEYS.proofHead(), head);
        var before = route.read(KEYS.proofHead()).toCompletableFuture().join().orElseThrow();
        var changed = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 2, List.of(), List.of()));
        var observer = route.compareAndSet(Optional.of(before), KEYS.proofHead(), changed)
                .toCompletableFuture();
        assertThat(applied).isCompleted();
        assertThat(observer.cancel(true)).isTrue();
        assertThat(trimTickets(route)).hasSize(1);
        reply.complete(null);
        assertThat(trimTickets(route)).isEmpty();
        assertThat(observer).isCancelled();
    }

    @Test
    void unchangedSameValueVersionIsNotProofThatTheOriginalWriteApplied() {
        var backend = new DeterministicOxiaConditionalClient();
        var first = new java.util.concurrent.atomic.AtomicBoolean(true);
        var nativeWrites = new java.util.concurrent.atomic.AtomicInteger();
        var port = new OxiaConditionalClient() {
            public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                return backend.read(key);
            }

            public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes bytes) {
                return backend.createIfAbsent(key, bytes);
            }

            public CompletionStage<Void> compareAndSet(String key, CanonicalBytes bytes, long version) {
                if (key.equals(ROOT + "/" + KEYS.proofHead())) {
                    if (first.compareAndSet(true, false)) {
                        return java.util.concurrent.CompletableFuture.failedFuture(
                                new java.io.IOException("uncertain no-op"));
                    }
                    nativeWrites.incrementAndGet();
                }
                return backend.compareAndSet(key, bytes, version);
            }
        };
        var route = new OxiaBindingLifecycleMetadataStoreV2(port, ROOT, 7, BINDING, digest("retention-cell"));
        enrollTrimRoute(route);
        var head = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 1, List.of(), List.of()));
        route.rawControlMetadata().putIfAbsent(KEYS.proofHead(), head);
        var before = route.read(KEYS.proofHead()).toCompletableFuture().join().orElseThrow();
        assertThat(route.compareAndSet(Optional.of(before), KEYS.proofHead(), head)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.RESPONSE_UNKNOWN);
        assertThat(trimTickets(route)).hasSize(1);
        assertThat(route.read(KEYS.proofHead()).toCompletableFuture().join()).contains(before);
        route.reconcileLogicalTrimWrites().toCompletableFuture().join();
        assertThat(trimTickets(route)).isEmpty();
        assertThat(nativeWrites.get()).isEqualTo(1);
        assertThat(route.read(KEYS.proofHead())
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .metadataVersion())
                .isNotEqualTo(before.metadataVersion());
    }

    private static List<com.nereusstream.storage.object.retention.M5LogicalTrimRecordsV2.LogicalTrimMutationV2>
            trimTickets(OxiaBindingLifecycleMetadataStoreV2 route) {
        return M5BindingAuthorityCodecV1.decodeAuthority(route.read(KEYS.selector())
                        .toCompletableFuture()
                        .join()
                        .orElseThrow()
                        .canonicalStoredBytes())
                .trimMutationTickets();
    }

    private static M5RetentionRecordsV1.RetentionFloorSnapshotV1 storedTypedFloor(
            OxiaBindingLifecycleMetadataStoreV2 route,
            com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue before) {
        var snapshot = typedFloor(
                BINDING,
                digest("retention-cell"),
                new M5RetentionRecordsV1.AuthorityFactV1(
                        before.key(), before.metadataVersion(), before.canonicalStoredSha256()));
        var keys = new M5RetentionKeysV1(7, BINDING);
        route.compareAndSet(
                        Optional.empty(),
                        keys.floorSnapshot(snapshot.snapshotRootSha256()),
                        M5RetentionCodecV1.encodeSnapshot(snapshot))
                .toCompletableFuture()
                .join();
        return snapshot;
    }

    @Test
    void retentionRouteRejectsForeignIdentityWrongKeyAndUnbackedOrForgedTrim() {
        var client = new DeterministicOxiaConditionalClient();
        var cell = digest("retention-cell");
        var route = new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, 7, BINDING, cell);
        var fact = new M5RetentionRecordsV1.AuthorityFactV1(
                KEYS.proofHead(), new MetadataVersion(CanonicalBytes.copyOf(new byte[] {1})), digest("fixture"));
        var snapshot = typedFloor(BINDING, cell, fact);
        var keys = new M5RetentionKeysV1(7, BINDING);
        assertThatThrownBy(() ->
                        new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, 7, BINDING).read(keys.trimFrontier()))
                .hasMessageContaining("exact lifecycle Binding route");
        assertThatThrownBy(() -> route.compareAndSet(
                        Optional.empty(),
                        keys.floorSnapshot(digest("wrong-key")),
                        M5RetentionCodecV1.encodeSnapshot(snapshot)))
                .hasMessageContaining("content-addressed");
        for (var foreign :
                List.of(typedFloor(foreign(), cell, fact), typedFloor(BINDING, digest("foreign-cell"), fact))) {
            assertThatThrownBy(() -> route.compareAndSet(
                            Optional.empty(),
                            keys.floorSnapshot(foreign.snapshotRootSha256()),
                            M5RetentionCodecV1.encodeSnapshot(foreign)))
                    .hasMessageContaining("Binding/Cell");
        }
        var trim = new M5RetentionPlannerV1().plan(snapshot, Optional.empty());
        assertThatThrownBy(() -> route.compareAndSet(
                                Optional.empty(), keys.trimFrontier(), M5RetentionCodecV1.encodeTrimFrontier(trim))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("trim frontier lacks its exact immutable floor snapshot");
        assertThat(client.createCount()).isZero();
        route.compareAndSet(
                        Optional.empty(),
                        keys.floorSnapshot(snapshot.snapshotRootSha256()),
                        M5RetentionCodecV1.encodeSnapshot(snapshot))
                .toCompletableFuture()
                .join();
        var forged = new M5RetentionRecordsV1.BindingTrimFrontierV1(
                trim.identity(),
                trim.domain(),
                -1,
                -1,
                trim.retentionPolicyRootSha256(),
                trim.floorSnapshotRootSha256(),
                trim.ownerFence(),
                trim.storageFence(),
                trim.generation(),
                trim.capability(),
                Optional.of(new M5RetentionRecordsV1.PulsarTrimContextV2(
                        snapshot.pulsarContext().orElseThrow().coverage(),
                        new M5PulsarCoverageV2.Position(91, 0),
                        new M5PulsarCoverageV2.Position(91, 2))));
        assertThatThrownBy(() -> route.compareAndSet(
                                Optional.empty(), keys.trimFrontier(), M5RetentionCodecV1.encodeTrimFrontier(forged))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("trim frontier differs from the exact floor minimum/predecessor");
        assertThat(client.casCount()).isZero();
    }

    private static void enrollTrimRoute(OxiaBindingLifecycleMetadataStoreV2 route) {
        var initial = M5BindingAuthorityCodecV1.initial(selector(BINDING, 1));
        route.compareAndSet(Optional.empty(), KEYS.selector(), M5BindingAuthorityCodecV1.encodeAuthority(initial))
                .toCompletableFuture()
                .join();
        var exact = route.read(KEYS.selector()).toCompletableFuture().join().orElseThrow();
        var enrollment =
                new com.nereusstream.storage.object.retention.M5BindingAuthorityRecordsV1.ReferenceWriterEnrollmentV1(
                        CAPABILITY,
                        List.of(M5RetentionRecordsV1.FloorClassV1.values()),
                        List.of(ReferenceKindV1.values()),
                        digest("route-writer-registry"));
        new com.nereusstream.storage.object.retention.M5BindingRetirementCoordinatorV1(route)
                .enrollWriters(
                        new com.nereusstream.storage.object.retention.M5BindingRetirementCoordinatorV1
                                .EnrollmentRequest(KEYS.selector(), exact, enrollment))
                .toCompletableFuture()
                .join();
    }

    private static M5RetentionRecordsV1.RetentionFloorSnapshotV1 typedFloor(
            BindingIdentity binding, Sha256Digest cell, M5RetentionRecordsV1.AuthorityFactV1 fact) {
        return typedFloor(
                binding, cell, fact, new M5PulsarCoverageV2.Position(91, 0), new M5PulsarCoverageV2.Position(91, 3));
    }

    private static M5RetentionRecordsV1.RetentionFloorSnapshotV1 typedFloor(
            BindingIdentity binding,
            Sha256Digest cell,
            M5RetentionRecordsV1.AuthorityFactV1 fact,
            M5PulsarCoverageV2.Position prior,
            M5PulsarCoverageV2.Position minimum) {
        var coverage = M5PulsarCoverageV2.of(
                binding,
                digest("synthetic-route-chain"),
                List.of(
                        new M5PulsarCoverageV2.ChainLedger(91, false, 3, digest("source91")),
                        new M5PulsarCoverageV2.ChainLedger(7, false, 4, digest("source7"))),
                List.of(new M5PulsarCoverageV2.EntryRange(91, 0, 3), new M5PulsarCoverageV2.EntryRange(7, 0, 4)));
        var identity =
                new M5MaterializationRecordsV1.IdentityEnvelope(cell, digest("provider"), binding, 1, 1, 1, CAPABILITY);
        var rows = java.util.Arrays.stream(M5RetentionRecordsV1.FloorClassV1.values())
                .map(kind -> new M5RetentionRecordsV1.RetentionFloorObservationV1(
                        kind,
                        fact,
                        M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY,
                        -1,
                        true,
                        true,
                        Optional.of(minimum)))
                .toList();
        return M5RetentionCodecV1.finalizeSnapshot(new M5RetentionRecordsV1.RetentionFloorSnapshotV1(
                identity,
                M5MaterializationRecordsV1.PositionDomain.PULSAR_ENTRY,
                1,
                -1,
                digest("policy"),
                fact,
                fact,
                1,
                4096,
                rows,
                digest("placeholder"),
                Optional.of(new M5RetentionRecordsV1.PulsarFloorContextV2(coverage, prior))));
    }

    @Test
    void completeMaterializationPlanIsCanonicalScopedAndAvailableAfterRouteRestart() {
        var client = new DeterministicOxiaConditionalClient();
        var cell = digest("materialization-cell");
        var route = new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, 7, BINDING, cell);
        var plan = plan(BINDING, cell);
        var coordinator = new M5MaterializationCoordinatorV1(route.rawControlMetadata(), 7, BINDING);
        assertThat(coordinator.register(plan)).isEqualTo(M5MaterializationRecordsV1.PublicationOutcome.APPLIED_EXACT);
        var restarted = new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, 7, BINDING, cell);
        assertThat(new M5MaterializationCoordinatorV1(restarted.rawControlMetadata(), 7, BINDING)
                        .readPlan(plan.taskIdSha256()))
                .contains(plan);
        var keys = new M5MaterializationKeysV1(7, BINDING);
        var exact = route.read(keys.plan(plan.taskIdSha256()))
                .toCompletableFuture()
                .join()
                .orElseThrow();
        assertThatThrownBy(() -> route.compareAndSet(Optional.of(exact), exact.key(), exact.canonicalStoredBytes()))
                .hasMessageContaining("create-only");
        var foreign = plan(BINDING, digest("foreign-cell"));
        assertThatThrownBy(() -> route.rawControlMetadata()
                        .putIfAbsent(keys.plan(foreign.taskIdSha256()), M5MaterializationCodecV1.encodePlan(foreign)))
                .hasMessageContaining("Binding/Cell");
        assertThatThrownBy(() -> route.read(keys.plan(digest("another-task")) + "/unlisted"))
                .hasMessageContaining("exact lifecycle Binding route");
        assertThatThrownBy(() -> new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, 7, BINDING)
                        .read(keys.plan(plan.taskIdSha256())))
                .hasMessageContaining("exact lifecycle Binding route");
    }

    @Test
    void opaqueTaskRequiresStoredCompletePlanAndItsExactOutputIdentity() {
        var client = new DeterministicOxiaConditionalClient();
        var cell = digest("materialization-cell");
        var route = new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, 7, BINDING, cell);
        var plan = plan(BINDING, cell);
        var keys = new M5MaterializationKeysV1(7, BINDING);
        var task = new M5MaterializationRecordsV1.MaterializationTask(
                plan.taskIdSha256(),
                M5MaterializationRecordsV1.TaskState.PLANNED,
                M5MaterializationCodecV1.sourceCutSha256(plan.sourceCut()),
                plan.outputIdentitySha256(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        assertThatThrownBy(() -> route.compareAndSet(
                                Optional.empty(),
                                keys.task(task.taskIdSha256()),
                                M5MaterializationCodecV1.encodeTask(task))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("materialization value has no complete stored plan");
        var coordinator = new M5MaterializationCoordinatorV1(route.rawControlMetadata(), 7, BINDING);
        assertThat(coordinator.register(plan)).isEqualTo(M5MaterializationRecordsV1.PublicationOutcome.APPLIED_EXACT);
        var wrong = new M5MaterializationRecordsV1.MaterializationTask(
                task.taskIdSha256(),
                task.state(),
                task.sourceCutSha256(),
                digest("foreign-output"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        var original =
                route.read(keys.task(task.taskIdSha256())).toCompletableFuture().join();
        assertThatThrownBy(() -> route.compareAndSet(
                                original, keys.task(task.taskIdSha256()), M5MaterializationCodecV1.encodeTask(wrong))
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("task differs from its complete stored plan");
    }

    @Test
    void materializationCapacityIsOneCellStateSharedAcrossBindingRoutes() {
        var client = new DeterministicOxiaConditionalClient();
        var cell = digest("materialization-cell");
        var other = new BindingIdentity(
                new TopicBindingId(digest("other-binding")), digest("other-incarnation"), digest("other-storage"));
        var first = new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, 7, BINDING, cell);
        var second = new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, 7, other, cell);
        String state = M5MaterializationAdmissionV1.stateKey(cell);
        assertThat(first.nativeKey(state)).isEqualTo(second.nativeKey(state));
        var a = new M5MaterializationAdmissionV1(first.rawControlMetadata(), cell);
        var b = new M5MaterializationAdmissionV1(second.rawControlMetadata(), cell);
        var caps = new M5MaterializationAdmissionV1.Caps(1, 100, 1000, 2, 2, 2, 2);
        assertThat(a.install(caps)).isEqualTo(M5MaterializationAdmissionV1.Outcome.APPLIED_EXACT);
        assertThat(a.reserve(plan(BINDING, cell), 100, 1))
                .isEqualTo(M5MaterializationAdmissionV1.Outcome.APPLIED_EXACT);
        assertThat(b.reserve(plan(other, cell), 100, 1)).isEqualTo(M5MaterializationAdmissionV1.Outcome.REJECTED_CAP);
        assertThat(b.read()).isEqualTo(a.read());
    }

    private static M5MaterializationRecordsV1.MaterializationPlan plan(BindingIdentity binding, Sha256Digest cell) {
        var identity =
                new M5MaterializationRecordsV1.IdentityEnvelope(cell, digest("provider"), binding, 1, 1, 1, CAPABILITY);
        var coverage = new M5MaterializationRecordsV1.ProtocolCoverage(
                M5MaterializationRecordsV1.PositionDomain.KAFKA_OFFSET, 0, 3);
        var source = new M5MaterializationRecordsV1.SourceExtent(
                M5MaterializationRecordsV1.SourceKind.BOOKKEEPER_LEDGER,
                digest("source"),
                coverage,
                "fixture-ledger",
                3,
                1,
                1,
                1,
                digest("body"),
                Optional.empty(),
                Optional.of(digest("ledger")),
                digest("format"),
                digest("encryption"),
                false,
                false,
                List.of(binding.bindingId().digest()));
        var selected = selector(binding, 1);
        var cut = new M5MaterializationRecordsV1.MaterializationSourceCut(
                identity,
                selected,
                Sha256Digest.hash(M4ReadControlCodecV1.encodeSelector(selected)),
                selected.selectedViewSha256(),
                coverage,
                3,
                3,
                3,
                3,
                0,
                digest("protocol"),
                digest("recovery"),
                digest("policy"),
                digest("output-format"),
                M5MaterializationCodecV1.calculateSourceSetSha256(List.of(source)),
                List.of(source));
        return new M5MaterializationPlannerV1()
                .plan(
                        cut,
                        new M5MaterializationPlannerV1.PlanningPolicy(
                                M5MaterializationRecordsV1.PayloadKind.KAFKA_BATCH_PRESERVING_V1,
                                true,
                                digest("encryption"),
                                digest("compression"),
                                digest("checksum"),
                                List.of(),
                                "fixture-cell"));
    }

    @Test
    void legacyM3AndNewControlViewUseOneNativeSelectorAndPreserveExactVersions() {
        var client = new DeterministicOxiaConditionalClient();
        var legacy = new OxiaCanonicalControlMetadataStore(client, ROOT, 7);
        var route = route(client);
        var before = M4ReadControlCodecV1.encodeSelector(selector(BINDING, 1));
        assertThat(legacy.putIfAbsent(KEYS.selector(), before)).isEqualTo(ControlMutationOutcome.APPLIED);
        var exact = route.read(KEYS.selector()).toCompletableFuture().join().orElseThrow();
        assertThat(exact.key()).isEqualTo(KEYS.selector());
        assertThat(route.controlMetadata().get(KEYS.selector())).contains(before);
        var after = M4ReadControlCodecV1.encodeSelector(selector(BINDING, 2));
        assertThat(route.controlMetadata().compareAndSet(KEYS.selector(), Optional.of(before), after))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(legacy.get(KEYS.selector())).get().satisfies(bytes -> {
            assertThat(M5BindingAuthorityCodecV1.isAuthorityValue(bytes)).isTrue();
            assertThat(M5BindingAuthorityCodecV1.projectSelector(bytes)).isEqualTo(selector(BINDING, 2));
        });
        assertThat(client.stored(route.nativeKey(KEYS.selector())))
                .get()
                .extracting(AuthorityRecord::versionId)
                .isEqualTo(1L);
        assertThat(legacy.compareAndSet(KEYS.selector(), Optional.of(before), before))
                .isEqualTo(ControlMutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(route.rawControlMetadata().get(KEYS.selector())).isEqualTo(legacy.get(KEYS.selector()));
        var coordinatorView = new com.nereusstream.storage.object.retention.M5BindingAuthorityControlMetadataStoreV1(
                route.rawControlMetadata(), KEYS.selector());
        var next = M4ReadControlCodecV1.encodeSelector(selector(BINDING, 3));
        assertThat(coordinatorView.compareAndSet(KEYS.selector(), Optional.of(after), next))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        assertThat(route.controlMetadata().get(KEYS.selector())).contains(next);
    }

    @Test
    void invalidRoutesFailBeforeAnyNativeIoAndExactMaximumFits() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client);
        var history = new M5RetiredBatchHistoryV2(BINDING);
        String node = history.key(digest("node"));
        for (String key : List.of(
                ROOT + "/" + KEYS.selector(),
                "../" + KEYS.selector(),
                KEYS.selector() + "/extra",
                KEYS.selector().replace("0000000007", "0000000008"),
                new M4ReadControlKeysV1(7, foreign()).selector(),
                node.toUpperCase(java.util.Locale.ROOT),
                node.replace(
                        BINDING.storageEpochSha256().toHex(), digest("other").toHex()),
                "v2/object-wal/shards/0000000007/current")) {
            assertThatThrownBy(() -> route.read(key)).isInstanceOf(IllegalArgumentException.class);
        }
        for (String root : List.of("/a//b", "/a/../b", "/a/", "/单元", "a")) {
            assertThatThrownBy(() -> new OxiaBindingLifecycleMetadataStoreV2(client, root, 7, BINDING))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        String maximumRoot = "/" + "a".repeat(512 - 2 - node.length());
        var maximum = new OxiaBindingLifecycleMetadataStoreV2(client, maximumRoot, 7, BINDING);
        assertThat(maximum.nativeKey(node)).hasSize(512);
        assertThatThrownBy(() -> new OxiaBindingLifecycleMetadataStoreV2(client, maximumRoot + "a", 7, BINDING))
                .hasMessageContaining("512-byte");
        assertThat(client.readCount()).isZero();
        assertThat(client.createCount()).isZero();
        assertThat(client.casCount()).isZero();
    }

    @Test
    void recordsMustMatchTheConfiguredIncarnationAndNativeKey() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client);
        var sameIdOtherEpoch = new BindingIdentity(BINDING.bindingId(), BINDING.incarnationSha256(), digest("other"));
        var wrong = M4ReadControlCodecV1.encodeSelector(selector(sameIdOtherEpoch, 1));
        assertThatThrownBy(() -> route.compareAndSet(Optional.empty(), KEYS.selector(), wrong))
                .hasMessageContaining("incarnation");
        var head = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 1, List.of(), List.of()));
        assertThat(route.compareAndSet(Optional.empty(), KEYS.proofHead(), head)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        assertThatThrownBy(() -> route.compareAndSet(Optional.empty(), KEYS.proof(1), head))
                .isInstanceOf(IllegalArgumentException.class);
        client.seed(route.nativeKey(KEYS.selector()), wrong, 4);
        assertThatThrownBy(
                        () -> route.read(KEYS.selector()).toCompletableFuture().join())
                .hasRootCauseMessage("lifecycle record Binding, incarnation, storage epoch or key differs");
    }

    @Test
    void everyHistoryNodeIsCanonicalBindingBoundAndCreateOnlyBeforeSelection() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client);
        var history = new M5RetiredBatchHistoryV2(BINDING);
        var tombstone = tombstone();
        var insertion = history.insert(
                history.emptyRoot(),
                tombstone,
                history.readProof(history.emptyRoot(), tombstone.batchIdSha256(), ignored -> Optional.empty()));
        for (var node : insertion.nodes()) {
            String key = history.key(node.root().sha256());
            assertThat(route.compareAndSet(Optional.empty(), key, node.bytes())
                            .toCompletableFuture()
                            .join())
                    .isEqualTo(MutationOutcome.APPLIED_EXACT);
            assertThat(route.controlMetadata().get(key)).contains(node.bytes());
        }
        assertThat(route.read(KEYS.selector()).toCompletableFuture().join()).isEmpty();
        var node = insertion.nodes().get(0);
        String key = history.key(node.root().sha256());
        var exact = route.read(key).toCompletableFuture().join().orElseThrow();
        int writes = client.createCount() + client.casCount();
        assertThatThrownBy(() -> route.compareAndSet(Optional.of(exact), key, node.bytes()))
                .hasMessageContaining("create-only");
        assertThatThrownBy(() -> route.compareAndSet(Optional.empty(), history.key(digest("wrong")), node.bytes()))
                .hasMessageContaining("content address");
        var foreignHistory = new M5RetiredBatchHistoryV2(foreign());
        assertThatThrownBy(() -> foreignHistory.verifyNode(node.root().sha256(), node.bytes()))
                .hasMessageContaining("Binding");
        assertThat(client.createCount() + client.casCount()).isEqualTo(writes);
    }

    @Test
    void nativeVersionAbaCannotBeHiddenByIdenticalSelectorBytes() {
        var client = new DeterministicOxiaConditionalClient();
        OxiaConditionalClient aba = new OxiaConditionalClient() {
            public CompletionStage<Optional<AuthorityRecord>> read(String key) {
                return client.read(key);
            }

            public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes value) {
                return client.createIfAbsent(key, value);
            }

            public CompletionStage<Void> compareAndSet(String key, CanonicalBytes value, long version) {
                client.seed(key, client.stored(key).orElseThrow().storedBytes(), version + 1);
                return client.compareAndSet(key, value, version);
            }
        };
        var route = route(aba);
        var before = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.initial(selector(BINDING, 1)));
        client.seed(route.nativeKey(KEYS.selector()), before, 4);
        var exact = route.read(KEYS.selector()).toCompletableFuture().join().orElseThrow();
        var candidate = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.selectorSuccessor(
                M5BindingAuthorityCodecV1.decodeAuthority(before), selector(BINDING, 2)));
        assertThat(route.compareAndSet(Optional.of(exact), KEYS.selector(), candidate)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(client.stored(route.nativeKey(KEYS.selector())))
                .get()
                .extracting(AuthorityRecord::storedBytes)
                .isEqualTo(before);
    }

    @Test
    void responseLossReconcilesAndReadFailureNeverMeansAbsence() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client);
        var initial = M4ReadControlCodecV1.encodeSelector(selector(BINDING, 1));
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);
        assertThat(route.controlMetadata().putIfAbsent(KEYS.selector(), initial))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        var candidate = M4ReadControlCodecV1.encodeSelector(selector(BINDING, 2));
        client.nextMutation(MutationMode.APPLY_THEN_RESPONSE_LOSS);
        assertThat(route.controlMetadata().compareAndSet(KEYS.selector(), Optional.of(initial), candidate))
                .isEqualTo(ControlMutationOutcome.APPLIED);
        client.failNextRead();
        assertThatThrownBy(() -> route.controlMetadata().get(KEYS.selector()))
                .hasRootCauseMessage("scripted reread failure");
        assertThat(route.controlMetadata().get(KEYS.selector())).contains(candidate);
    }

    @Test
    void aSecondCellCannotObserveOrConsumeTheFirstCellsVersionedValue() {
        var client = new DeterministicOxiaConditionalClient();
        var first = route(client);
        var second = new OxiaBindingLifecycleMetadataStoreV2(client, "/nereus/cells/b", 7, BINDING);
        var bytes = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.initial(selector(BINDING, 1)));
        assertThat(first.compareAndSet(Optional.empty(), KEYS.selector(), bytes)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        assertThat(second.read(KEYS.selector()).toCompletableFuture().join()).isEmpty();
        var before = first.read(KEYS.selector()).toCompletableFuture().join().orElseThrow();
        var candidate = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.selectorSuccessor(
                M5BindingAuthorityCodecV1.decodeAuthority(bytes), selector(BINDING, 2)));
        client.seed(second.nativeKey(KEYS.selector()), bytes, 0);
        assertThatThrownBy(() -> second.compareAndSet(Optional.of(before), KEYS.selector(), candidate))
                .hasMessageContaining("another route");
        assertThat(client.stored(second.nativeKey(KEYS.selector())))
                .get()
                .extracting(AuthorityRecord::storedBytes)
                .isEqualTo(bytes);
    }

    @Test
    void wrappedSelectorCannotDowngradeReplayOrImportHistory() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client);
        var initial =
                M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.initial(selector(BINDING, 1)));
        route.compareAndSet(Optional.empty(), KEYS.selector(), initial)
                .toCompletableFuture()
                .join();
        var before = route.read(KEYS.selector()).toCompletableFuture().join().orElseThrow();
        assertThatThrownBy(() -> route.compareAndSet(
                        Optional.of(before),
                        KEYS.selector(),
                        M4ReadControlCodecV1.encodeSelector(selector(BINDING, 2))))
                .hasMessageContaining("downgrade");
        assertThatThrownBy(() -> route.compareAndSet(Optional.of(before), KEYS.selector(), initial))
                .hasMessageContaining("next revision");
        var successor = M5BindingAuthorityCodecV1.encodeAuthority(M5BindingAuthorityCodecV1.selectorSuccessor(
                M5BindingAuthorityCodecV1.decodeAuthority(initial), selector(BINDING, 2)));
        assertThatThrownBy(() -> route.compareAndSet(Optional.empty(), KEYS.selector(), successor))
                .hasMessageContaining("import");
        assertThat(client.casCount()).isZero();
    }

    @Test
    void bootstrapRequiresActualCapabilityAndCannotBeReplacedOrImportedIntoLegacyHistory() {
        var client = new DeterministicOxiaConditionalClient();
        var route = route(client);
        var evidence = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityEvidence(
                BINDING,
                1,
                1,
                com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityKind
                        .DURABLE_DRAIN_ONLY_V1,
                com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityState.ADMITTED,
                digest("adapter"),
                digest("backend"),
                digest("read"),
                digest("verifier"),
                digest("input"),
                digest("receipt"),
                digest("no-expiry"),
                1_000,
                0,
                0);
        var capability = new CapabilityBinding(1, M4ReadControlCodecV1.capabilityEvidenceSha256(evidence));
        var bootstrap = new M5BindingBootstrapV1(BINDING, capability, digest("sdk-scope"));
        assertThatThrownBy(() -> route.compareAndSet(Optional.empty(), KEYS.selector(), bootstrap.encode())
                        .toCompletableFuture()
                        .join())
                .hasRootCauseMessage("Binding bootstrap capability is missing");
        route.rawControlMetadata().putIfAbsent(KEYS.capability(1), M4ReadControlCodecV1.encodeCapability(evidence));
        assertThat(route.compareAndSet(Optional.empty(), KEYS.selector(), bootstrap.encode())
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        assertThat(route.controlMetadata().get(KEYS.selector())).isEmpty();
        var before = route.read(KEYS.selector()).toCompletableFuture().join().orElseThrow();
        assertThatThrownBy(() -> route.compareAndSet(Optional.of(before), KEYS.selector(), bootstrap.encode()))
                .hasMessageContaining("create-only");
        var head = M4ReadControlCodecV1.encodeHead(new QuiescenceProofHead(BINDING, 1, List.of(), List.of()));
        assertThat(route.compareAndSet(Optional.empty(), KEYS.proofHead(), head)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.DEFINITIVE_CONFLICT);
        var selected = new BindingReadSelector(
                BINDING,
                digest("real-view"),
                1,
                1,
                1,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                capability,
                List.of(),
                List.of());
        var activated = M5BindingAuthorityCodecV1.encodeAuthority(
                M5BindingAuthorityCodecV1.activateBootstrap(bootstrap, selected));
        assertThat(route.compareAndSet(Optional.of(before), KEYS.selector(), activated)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.APPLIED_EXACT);
        assertThat(route.compareAndSet(Optional.of(before), KEYS.selector(), activated)
                        .toCompletableFuture()
                        .join())
                .isEqualTo(MutationOutcome.DEFINITIVE_CONFLICT);
        assertThat(M5BindingAuthorityCodecV1.decodeAuthority(route.read(KEYS.selector())
                                .toCompletableFuture()
                                .join()
                                .orElseThrow()
                                .canonicalStoredBytes())
                        .bootstrap())
                .contains(bootstrap);
        var old = route(new DeterministicOxiaConditionalClient());
        var legacy = M4ReadControlCodecV1.encodeSelector(selected);
        old.compareAndSet(Optional.empty(), KEYS.selector(), legacy)
                .toCompletableFuture()
                .join();
        var prior = old.read(KEYS.selector()).toCompletableFuture().join().orElseThrow();
        assertThatThrownBy(() -> old.compareAndSet(Optional.of(prior), KEYS.selector(), activated))
                .hasMessageContaining("import first-admission");
    }

    private static OxiaBindingLifecycleMetadataStoreV2 route(OxiaConditionalClient client) {
        return new OxiaBindingLifecycleMetadataStoreV2(client, ROOT, 7, BINDING);
    }

    private static BindingIdentity foreign() {
        return new BindingIdentity(new TopicBindingId(digest("foreign")), digest("incarnation"), digest("storage"));
    }

    private static BindingReadSelector selector(BindingIdentity binding, long generation) {
        return new BindingReadSelector(
                binding,
                digest("view-" + generation),
                1,
                1,
                generation,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                CAPABILITY,
                List.of(),
                List.of());
    }

    private static RetiredSourceRetirementBatchTombstoneV1 tombstone() {
        return M5RetentionCodecV1.finalizeRetiredBatch(new RetiredSourceRetirementBatchTombstoneV1(
                BatchMetadataStateV1.RETIRED_V1,
                BINDING,
                digest("batch"),
                digest("full"),
                digest("proof"),
                new MetadataVersion(CanonicalBytes.copyOf(new byte[] {1})),
                digest("before"),
                CAPABILITY,
                digest("placeholder")));
    }

    private static Sha256Digest digest(String value) {
        return Sha256Digest.hash(CanonicalBytes.copyOf(value.getBytes(StandardCharsets.UTF_8)));
    }
}
