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

package com.nereusstream.kafka.bookkeeper.object.publication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBatchDuplicateIdentityV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaCoherentCommitCoordinatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolAppendPlanV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaProtocolBatchDeltaV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaSpeculativeCommitV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaTransactionBatchKindV1;
import com.nereusstream.kafka.bookkeeper.object.ObjectKafkaTestFixtures;
import com.nereusstream.kafka.bookkeeper.object.control.KafkaObjectWholeSuffixRollbackV1;
import com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectBindingReadAdapterV1;
import com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectWalM4ReaderV1;
import com.nereusstream.kafka.bookkeeper.object.read.KafkaObjectWalM4ReaderV1.ValidatedRange;
import com.nereusstream.kafka.bookkeeper.pipeline.KafkaOffsetAssignedAppendV1;
import com.nereusstream.storage.object.read.BindingReadAsyncExecutorV1;
import com.nereusstream.storage.object.read.BindingReadBatchContextV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1.CaptureOutcome;
import com.nereusstream.storage.object.read.BindingReadHazardPoolV1.ScanOutcome;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.AdmissionState;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingReadSelector;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.SelectorMode;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class KafkaObjectPublicationBridgeV1Test {
    @Test
    void repositoryRootSelectsLocatorNativeStateAndQueueBeforeAckThenRetainsLocatorBudget() {
        Context context = context(ignored -> {});
        KafkaSpeculativeCommitV1 commit = ObjectKafkaTestFixtures.commit(0);
        stage(context.coordinator, commit);
        Ready ready = ready(context, commit);
        AtomicBoolean acked = new AtomicBoolean();

        KafkaObjectCoherentProtocolSnapshotV1 published = context.bridge
                .publishNext(() -> {
                    assertThat(context.tracker.pendingUnits()).isOne();
                    KafkaObjectCoherentProtocolSnapshotV1 selected = context.coordinator.captureObject();
                    assertThat(selected.activeTail().floor(0, 1)).contains(ready.locator);
                    acked.set(true);
                })
                .orElseThrow();

        assertThat(acked).isTrue();
        assertThat(published.root().frontiers().durableEndOffset()).isEqualTo(1);
        assertThat(published.root().frontiers().readableEndOffset()).isEqualTo(1);
        assertThat(published.root().frontiers().highWatermark()).isEqualTo(1);
        assertThat(published.speculativeQueue().commits()).isEmpty();
        assertThat(published.root().references().activeTail().contentDigest())
                .isEqualTo(Sha256Digest.hash(KafkaObjectStateCodecV1.activeTail(published.activeTail())));
        assertThat(context.tracker.pendingUnits()).isZero();
        assertThat(context.tracker.retainedLocatorBytes()).isEqualTo(ready.reservation.locatorBytes());
        assertThat(context.tracker.reservedLocatorBytes()).isEqualTo(ready.reservation.locatorBytes());
    }

    @Test
    void ackFailureRetainsRootPublishedTicketAndRetryDoesNotRepublish() {
        Context context = context(ignored -> {
            throw new IllegalStateException("post-CAS observer fault");
        });
        KafkaSpeculativeCommitV1 commit = ObjectKafkaTestFixtures.commit(0);
        stage(context.coordinator, commit);
        Ready ready = ready(context, commit);

        long publishedVersion;
        assertThatThrownBy(() -> context.bridge.publishNext(() -> {
                    throw new IllegalStateException("ACK transport fault");
                }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("ACK transport fault");

        KafkaObjectCoherentProtocolSnapshotV1 selected = context.coordinator.captureObject();
        publishedVersion = selected.root().stateVersion();
        assertThat(selected.root().frontiers().readableEndOffset()).isEqualTo(1);
        assertThat(selected.activeTail().floor(0, 1)).contains(ready.locator);
        assertThat(context.tracker.pendingUnits()).isOne();
        assertThat(context.tracker.retainedLocatorBytes()).isZero();

        KafkaObjectCoherentProtocolSnapshotV1 retried =
                context.bridge.publishNext(() -> {}).orElseThrow();
        assertThat(retried.root().stateVersion()).isEqualTo(publishedVersion);
        assertThat(context.coordinator.captureObject().root().stateVersion()).isEqualTo(publishedVersion);
        assertThat(context.tracker.pendingUnits()).isZero();
        assertThat(context.tracker.retainedLocatorBytes()).isEqualTo(ready.reservation.locatorBytes());
    }

    @Test
    void invalidNativeCutFailsBeforeRootCasAndLeavesTicketAndLocatorUnpublished() {
        Context context = context(ignored -> {});
        KafkaSpeculativeCommitV1 commit = ObjectKafkaTestFixtures.commit(0);
        stage(context.coordinator, commit);
        Ready ready = ready(
                context,
                commit,
                new KafkaObjectNativeStateV1(
                        context.coordinator.captureObject().committedProducerState(),
                        context.coordinator.captureObject().transactionState(),
                        context.coordinator.captureObject().leaderEpochIndex(),
                        1,
                        1));

        assertThatThrownBy(() -> context.bridge.publishNext(() -> {}))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("native state differs");
        assertThat(context.coordinator.captureObject().activeTail().locators()).isEmpty();
        assertThat(context.coordinator.captureObject().root().frontiers().readableEndOffset())
                .isZero();
        assertThat(context.tracker.pendingUnits()).isOne();
        assertThat(context.tracker.reservedLocatorBytes()).isEqualTo(ready.reservation.locatorBytes());
    }

    @Test
    void forgedLastStableOffsetCannotExposeAnOpenTransaction() {
        Context context = context(ignored -> {});
        var transactionalDelta = new KafkaProtocolBatchDeltaV1(
                1,
                Optional.of(new KafkaBatchDuplicateIdentityV1(22, (short) 0, 0, 0)),
                KafkaTransactionBatchKindV1.TRANSACTIONAL_DATA,
                22,
                -1);
        KafkaSpeculativeCommitV1 commit = KafkaSpeculativeCommitV1.assign(
                new KafkaProtocolAppendPlanV1(ObjectKafkaTestFixtures.fence(), List.of(transactionalDelta)), 0, 1);
        stage(context.coordinator, commit);
        KafkaObjectCoherentProtocolSnapshotV1 before = context.coordinator.captureObject();
        ready(
                context,
                commit,
                new KafkaObjectNativeStateV1(
                        before.committedProducerState().apply(commit),
                        before.transactionState().apply(commit),
                        before.leaderEpochIndex()
                                .observe(before.root().fence().kafkaLeaderEpoch(), commit.startOffset()),
                        1,
                        1));

        assertThatThrownBy(() -> context.bridge.publishNext(() -> {}))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("HW/LSO");
        assertThat(context.coordinator.captureObject().root().frontiers().readableEndOffset())
                .isZero();
        assertThat(context.tracker.pendingUnits()).isOne();
    }

    @Test
    void locatorReservationUsesTheActualCanonicalLocatorWireCharge() {
        int exactBytes = KafkaObjectStateCodecV1.exactLocatorBytes();
        var tracker = new KafkaObjectCompletionTrackerV1(2, exactBytes, 6);
        var reservation = tracker.reserveBeforePosition();
        assertThat(reservation.locatorBytes()).isEqualTo(exactBytes);
        assertThat(tracker.reservedLocatorBytes()).isEqualTo(exactBytes);
        assertThatThrownBy(tracker::reserveBeforePosition).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void retainedLocatorBudgetRequiresManifestBoundRootRetirementAndExactPinDrain() {
        Context context = context(ignored -> {});
        KafkaSpeculativeCommitV1 commit = ObjectKafkaTestFixtures.commit(0);
        stage(context.coordinator, commit);
        Ready ready = ready(context, commit);
        context.bridge.publishNext(() -> {}).orElseThrow();
        var protection = new KafkaObjectSourceProtectionTrackerV1(
                binding(), ready.locator.extent().walRunRootSha());
        var pin = protection.pin(ready.locator);

        assertThatThrownBy(() -> protection.prepareManifestRetirement(
                        context.coordinator.captureObject().activeTail(),
                        1,
                        "manifests/sha256-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.nwm",
                        ObjectKafkaTestFixtures.digest(21)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("read pin");

        pin.close();
        var plan = protection.prepareManifestRetirement(
                context.coordinator.captureObject().activeTail(),
                1,
                "manifests/sha256-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.nwm",
                ObjectKafkaTestFixtures.digest(21));
        assertThatThrownBy(() -> protection.pin(ready.locator))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retiring");
        KafkaObjectCoherentProtocolSnapshotV1 retired =
                context.coordinator.retireObjectTail(protection, plan, context.tracker);

        assertThat(retired.activeTail().startOffset()).isOne();
        assertThat(retired.activeTail().locators()).isEmpty();
        assertThat(context.tracker.retainedLocatorBytes()).isZero();
        assertThat(context.tracker.reservedLocatorBytes()).isZero();
    }

    @Test
    void m4CurrentSourceReadPinsExactM3LocatorUntilProviderAndOuterLeaseDrain() throws Exception {
        Context context = context(ignored -> {});
        KafkaSpeculativeCommitV1 commit = ObjectKafkaTestFixtures.commit(0);
        stage(context.coordinator, commit);
        Ready ready = ready(context, commit);
        KafkaObjectCoherentProtocolSnapshotV1 published =
                context.bridge.publishNext(() -> {}).orElseThrow();
        KafkaObjectSourceProtectionTrackerV1 protection = new KafkaObjectSourceProtectionTrackerV1(
                binding(), ready.locator.extent().walRunRootSha());
        BindingReadHazardPoolV1 pool = new BindingReadHazardPoolV1(8, 4);
        CompletableFuture<CanonicalBytes> provider = new CompletableFuture<>();
        CompletableFuture<Void> providerStarted = new CompletableFuture<>();
        ExecutorService eventLoop = Executors.newSingleThreadExecutor();
        try {
            KafkaObjectWalM4ReaderV1 reader = new KafkaObjectWalM4ReaderV1(
                    published,
                    m4Selector(published),
                    protection,
                    (locator, start, end) -> {
                        providerStarted.complete(null);
                        return provider.thenApply(bytes -> new ValidatedRange(locator, start, end, bytes));
                    },
                    pool,
                    eventLoop);

            BindingReadBatchContextV1 capturedBeforeInnerPin = new BindingReadBatchContextV1();
            assertThat(pool.tryCapture(reader.currentAuthority(), capturedBeforeInnerPin))
                    .isEqualTo(CaptureOutcome.CAPTURED);
            assertThatThrownBy(() -> protection.prepareManifestRetirement(
                            published.activeTail(),
                            1,
                            "manifests/sha256-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.nwm",
                            ObjectKafkaTestFixtures.digest(21)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("M4 read pin");
            capturedBeforeInnerPin.closeNewSourceUse();
            assertThat(capturedBeforeInnerPin.terminalClearExactLease()).isTrue();

            CompletableFuture<KafkaObjectWalM4ReaderV1.ReadResult> read = reader.read(0, 1, 1);
            providerStarted.get(10, TimeUnit.SECONDS);
            assertThat(pool.scan(published.root().fence().bindingId(), 1)).isEqualTo(ScanOutcome.PINNED);
            assertThatThrownBy(() -> protection.prepareManifestRetirement(
                            published.activeTail(),
                            1,
                            "manifests/sha256-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.nwm",
                            ObjectKafkaTestFixtures.digest(21)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("read pin");

            provider.complete(CanonicalBytes.copyOf(new byte[] {1, 2, 3}));
            assertThat(read.get(10, TimeUnit.SECONDS).ranges())
                    .singleElement()
                    .extracting(ValidatedRange::locator)
                    .isEqualTo(ready.locator);
            assertThat(pool.scan(published.root().fence().bindingId(), 1)).isEqualTo(ScanOutcome.CLEAN);

            var plan = protection.prepareManifestRetirement(
                    published.activeTail(),
                    1,
                    "manifests/sha256-v1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.nwm",
                    ObjectKafkaTestFixtures.digest(21));
            KafkaObjectCoherentProtocolSnapshotV1 retired =
                    context.coordinator.retireObjectTail(protection, plan, context.tracker);
            assertThat(retired.activeTail().locators()).isEmpty();
        } finally {
            eventLoop.shutdownNow();
        }
    }

    @Test
    void m4KafkaCurrentSourceLatencyAllocationAndCapacityAreBounded() throws Exception {
        Context context = context(ignored -> {});
        KafkaSpeculativeCommitV1 commit = ObjectKafkaTestFixtures.commit(0);
        stage(context.coordinator, commit);
        Ready ready = ready(context, commit);
        KafkaObjectCoherentProtocolSnapshotV1 published =
                context.bridge.publishNext(() -> {}).orElseThrow();
        KafkaObjectSourceProtectionTrackerV1 protection = new KafkaObjectSourceProtectionTrackerV1(
                binding(), ready.locator.extent().walRunRootSha());
        BindingReadHazardPoolV1 pool = new BindingReadHazardPoolV1(8, 4);
        CanonicalBytes payload = CanonicalBytes.copyOf(new byte[] {1, 2, 3});
        AtomicReference<CompletableFuture<CanonicalBytes>> providerGate = new AtomicReference<>();
        AtomicInteger providerCalls = new AtomicInteger();
        AtomicReference<Thread> ownerThread = new AtomicReference<>();
        ExecutorService eventLoop = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "m4-kafka-current-source-owner");
            ownerThread.set(thread);
            return thread;
        });
        try {
            KafkaObjectWalM4ReaderV1 reader = new KafkaObjectWalM4ReaderV1(
                    published,
                    m4Selector(published),
                    protection,
                    (locator, start, end) -> {
                        providerCalls.incrementAndGet();
                        CompletableFuture<CanonicalBytes> gate = providerGate.get();
                        CompletableFuture<CanonicalBytes> bytes =
                                gate == null ? CompletableFuture.completedFuture(payload) : gate;
                        return bytes.thenApply(value -> new ValidatedRange(locator, start, end, value));
                    },
                    pool,
                    eventLoop);
            for (int index = 0; index < 200; index++) {
                reader.read(0, 1, 1).get(10, TimeUnit.SECONDS);
            }

            com.sun.management.ThreadMXBean bean =
                    (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
            assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
            bean.setThreadAllocatedMemoryEnabled(true);
            int operations = 1_000;
            long[] latencyNanos = new long[operations];
            long callerThreadId = Thread.currentThread().getId();
            long ownerThreadId = ownerThread.get().getId();
            long callerAllocatedBefore = bean.getThreadAllocatedBytes(callerThreadId);
            long ownerAllocatedBefore = bean.getThreadAllocatedBytes(ownerThreadId);
            long elapsedStart = System.nanoTime();
            for (int index = 0; index < operations; index++) {
                long started = System.nanoTime();
                assertThat(reader.read(0, 1, 1).get(10, TimeUnit.SECONDS).ranges())
                        .hasSize(1);
                latencyNanos[index] = System.nanoTime() - started;
            }
            long elapsedNanos = System.nanoTime() - elapsedStart;
            long callerAllocatedBytes = bean.getThreadAllocatedBytes(callerThreadId) - callerAllocatedBefore;
            long ownerAllocatedBytes = bean.getThreadAllocatedBytes(ownerThreadId) - ownerAllocatedBefore;
            long callerAllocatedBytesPerOperation = Math.floorDiv(callerAllocatedBytes, operations);
            long ownerAllocatedBytesPerOperation = Math.floorDiv(ownerAllocatedBytes, operations);
            long measuredAllocatedBytes = Math.addExact(callerAllocatedBytes, ownerAllocatedBytes);
            long measuredAllocatedBytesPerOperation = Math.floorDiv(measuredAllocatedBytes, operations);
            Arrays.sort(latencyNanos);
            long p99Nanos = percentile(latencyNanos, 99);
            long measuredThroughput = throughput(operations, elapsedNanos);
            assertThat(p99Nanos).isLessThan(50_000_000);
            assertThat(measuredThroughput).isGreaterThan(100);
            assertThat(callerAllocatedBytesPerOperation).isLessThan(1_048_576);
            assertThat(ownerAllocatedBytesPerOperation).isLessThan(1_048_576);
            assertThat(measuredAllocatedBytesPerOperation).isLessThan(2_097_152);

            CompletableFuture<CanonicalBytes> blockedProvider = new CompletableFuture<>();
            providerGate.set(blockedProvider);
            int beforeCapacityCalls = providerCalls.get();
            List<CompletableFuture<KafkaObjectWalM4ReaderV1.ReadResult>> pending = new ArrayList<>();
            for (int index = 0; index < pool.capacity(); index++) {
                pending.add(reader.read(0, 1, 1));
            }
            awaitProviderCalls(providerCalls, beforeCapacityCalls + pool.capacity());
            CompletableFuture<KafkaObjectWalM4ReaderV1.ReadResult> rejected = reader.read(0, 1, 1);
            assertThatThrownBy(rejected::join)
                    .hasRootCauseInstanceOf(BindingReadAsyncExecutorV1.AdmissionException.class)
                    .hasRootCauseMessage("M4 read admission failed before source I/O: EXHAUSTED");
            blockedProvider.complete(payload);
            for (CompletableFuture<KafkaObjectWalM4ReaderV1.ReadResult> read : pending) {
                assertThat(read.get(10, TimeUnit.SECONDS).ranges()).hasSize(1);
            }
            assertThat(pool.scan(published.root().fence().bindingId(), 1)).isEqualTo(ScanOutcome.CLEAN);
            System.out.printf(
                    Locale.ROOT,
                    "M4_METRIC KAFKA_CURRENT_SOURCE operations=%d elapsedNanos=%d p50Nanos=%d p99Nanos=%d "
                            + "maxNanos=%d throughputOpsPerSecond=%d callerAllocatedBytes=%d "
                            + "callerAllocatedBytesPerOperation=%d ownerAllocatedBytes=%d "
                            + "ownerAllocatedBytesPerOperation=%d measuredAllocatedBytes=%d "
                            + "measuredAllocatedBytesPerOperation=%d hazardCapacity=%d peakInFlight=%d "
                            + "rejectedAtCapacity=1 outerHazardCasPerRead=2 perCallbackSlotCas=0 "
                            + "reusablePlanCapacity=256%n",
                    operations,
                    elapsedNanos,
                    percentile(latencyNanos, 50),
                    p99Nanos,
                    latencyNanos[latencyNanos.length - 1],
                    measuredThroughput,
                    callerAllocatedBytes,
                    callerAllocatedBytesPerOperation,
                    ownerAllocatedBytes,
                    ownerAllocatedBytesPerOperation,
                    measuredAllocatedBytes,
                    measuredAllocatedBytesPerOperation,
                    pool.capacity(),
                    pool.capacity());
        } finally {
            eventLoop.shutdownNow();
        }
    }

    @Test
    void wholeSuffixRollbackUsesRepositoryRootCasThenReleasesExactSuffixTicket() {
        Context context = context(ignored -> {});
        KafkaSpeculativeCommitV1 first = ObjectKafkaTestFixtures.commit(0);
        KafkaSpeculativeCommitV1 second = ObjectKafkaTestFixtures.commit(1);
        stage(context.coordinator, first);
        stage(context.coordinator, second);
        var firstReservation = context.tracker.reserveBeforePosition();
        var firstTicket = context.tracker.assignPosition(firstReservation, first);
        var secondReservation = context.tracker.reserveBeforePosition();
        var secondTicket = context.tracker.assignPosition(secondReservation, second);
        var rollback = new KafkaObjectWholeSuffixRollbackV1(context.tracker);

        KafkaObjectCoherentProtocolSnapshotV1 published = rollback.rollbackM2Suffix(1, context.coordinator);

        assertThat(published.root().frontiers().allocatedEndOffset()).isEqualTo(1);
        assertThat(published.root().frontiers().durableEndOffset()).isZero();
        assertThat(published.speculativeQueue().commits()).containsExactly(first);
        assertThat(context.tracker.pendingUnits()).isOne();
        assertThat(context.tracker.reservedLocatorBytes()).isEqualTo(firstReservation.locatorBytes());
        assertThatThrownBy(() -> context.tracker.rootPublished(secondTicket, second))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void issuedWholeSuffixRollbackFencesSequenceUntilExactRootCasCompletes() {
        Context context = context(ignored -> {});
        KafkaSpeculativeCommitV1 commit = ObjectKafkaTestFixtures.commit(0);
        stage(context.coordinator, commit);
        var reservation = context.tracker.reserveBeforePosition();
        var ticket = context.tracker.assignPosition(reservation, commit);
        var plan = context.tracker.prepareRollbackSuffix(0);

        assertThatThrownBy(() -> context.tracker.sequenceStarted(ticket))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("out of order");

        KafkaObjectCoherentProtocolSnapshotV1 selected = context.coordinator.rollbackObjectSuffix(0, plan.commits());
        context.tracker.completeRollbackAfterRootCas(plan);
        assertThat(selected.speculativeQueue().commits()).isEmpty();
        assertThat(context.tracker.pendingUnits()).isZero();
    }

    @Test
    void noEffectSequenceClaimFreezesRollbackAndAbortRestoresExactEligibility() {
        Context context = context(ignored -> {});
        KafkaSpeculativeCommitV1 commit = ObjectKafkaTestFixtures.commit(0);
        stage(context.coordinator, commit);
        var reservation = context.tracker.reserveBeforePosition();
        var ticket = context.tracker.assignPosition(reservation, commit);
        var claim = context.tracker.claimSequence(ticket);

        assertThatThrownBy(() -> context.tracker.prepareRollbackSuffix(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot roll back");

        context.tracker.abortSequenceBeforeEffect(claim);
        var plan = context.tracker.prepareRollbackSuffix(0);
        context.coordinator.rollbackObjectSuffix(0, plan.commits());
        context.tracker.completeRollbackAfterRootCas(plan);
        assertThat(context.tracker.pendingUnits()).isZero();
    }

    @Test
    void rollbackAfterSequenceEffectLeavesRootAndExactTicketIntact() {
        Context context = context(ignored -> {});
        KafkaSpeculativeCommitV1 commit = ObjectKafkaTestFixtures.commit(0);
        stage(context.coordinator, commit);
        var reservation = context.tracker.reserveBeforePosition();
        var ticket = context.tracker.assignPosition(reservation, commit);
        var rollback = new KafkaObjectWholeSuffixRollbackV1(context.tracker);
        context.tracker.sequenceStarted(ticket);

        assertThatThrownBy(() -> rollback.rollbackM2Suffix(0, context.coordinator))
                .isInstanceOf(IllegalStateException.class);
        assertThat(context.coordinator.captureObject().root().frontiers().allocatedEndOffset())
                .isEqualTo(1);
        assertThat(context.coordinator.captureObject().speculativeQueue().commits())
                .containsExactly(commit);
        assertThat(context.tracker.pendingUnits()).isOne();
        assertThat(context.tracker.reservedLocatorBytes()).isEqualTo(reservation.locatorBytes());
    }

    @Test
    void rollbackValidatesExactTicketCommitPairBeforeRootCas() {
        Context context = context(ignored -> {});
        KafkaSpeculativeCommitV1 staged = ObjectKafkaTestFixtures.commit(0);
        stage(context.coordinator, staged);
        var reservation = context.tracker.reserveBeforePosition();
        var wrongTicket = context.tracker.assignPosition(reservation, ObjectKafkaTestFixtures.commit(100));
        var rollback = new KafkaObjectWholeSuffixRollbackV1(context.tracker);

        assertThatThrownBy(() -> rollback.rollbackM2Suffix(0, context.coordinator))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("live assigned suffix boundary");
        assertThat(context.coordinator.captureObject().root().frontiers().allocatedEndOffset())
                .isEqualTo(1);
        assertThat(context.coordinator.captureObject().speculativeQueue().commits())
                .containsExactly(staged);
        assertThat(context.tracker.pendingUnits()).isOne();
    }

    @Test
    void sharedPhysicalAndBindingFailureDomainsRemainIndependent() {
        var root = ObjectKafkaTestFixtures.digest(12);
        var physical = new KafkaObjectPhysicalFrontiersV1(root);
        var extent = new KafkaObjectExtentIdentityV1(root, 2, 0, 256, 300, ObjectKafkaTestFixtures.digest(13));
        physical.resolve(extent);
        assertThat(physical.resolvedThrough(2)).isZero();
        assertThat(physical.resolvedThrough(0)).isEqualTo(-1);

        var fence = ObjectKafkaTestFixtures.fence();
        var bindingA = binding();
        var bindingB = new KafkaObjectBindingKeyV1(
                new com.nereusstream.domain.identity.TopicBindingId(ObjectKafkaTestFixtures.digest(30)),
                fence.topicIncarnation().topicId(),
                fence.partitionId() + 1,
                fence.storageEpochId());
        var isolated = new KafkaSharedExtentValidationV1(extent, List.of(bindingA, bindingB));
        isolated.sharedVerified(extent);
        isolated.memberFailed(bindingA);
        isolated.memberVerified(bindingB);
        assertThat(isolated.canPublish(bindingA)).isFalse();
        assertThat(isolated.canPublish(bindingB)).isTrue();

        var commonFailure = new KafkaSharedExtentValidationV1(extent, List.of(bindingA, bindingB));
        commonFailure.sharedFailed();
        assertThat(commonFailure.canPublish(bindingA)).isFalse();
        assertThat(commonFailure.canPublish(bindingB)).isFalse();
    }

    @Test
    void takeoverRejectsAssignedPositionsAndInvalidatesOnlyUnassignedReservationValues() {
        int exactBytes = KafkaObjectStateCodecV1.exactLocatorBytes();
        var tracker = new KafkaObjectCompletionTrackerV1(2, exactBytes * 2L, 6, Long.MAX_VALUE);
        var terminalReservation = tracker.reserveBeforePosition();
        var terminalTicket = tracker.assignPosition(terminalReservation, ObjectKafkaTestFixtures.commit(0));
        assertThat(terminalTicket.ticket()).isEqualTo(Long.MAX_VALUE);
        var blockedReservation = tracker.reserveBeforePosition();
        assertThatThrownBy(() -> tracker.assignPosition(blockedReservation, ObjectKafkaTestFixtures.commit(1)))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> tracker.discardOnTakeover(7)).isInstanceOf(IllegalStateException.class);
        assertThat(tracker.pendingUnits()).isEqualTo(2);

        var unassignedOnly = new KafkaObjectCompletionTrackerV1(1, exactBytes, 6);
        var oldReservation = unassignedOnly.reserveBeforePosition();
        unassignedOnly.discardOnTakeover(7);
        assertThatThrownBy(() -> unassignedOnly.cancelBeforePosition(oldReservation))
                .isInstanceOf(IllegalStateException.class);
        assertThat(unassignedOnly.pendingUnits()).isZero();
        assertThat(terminalTicket.ticket()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void takeoverKeepsRootSelectedLocatorBudgetUntilTypedRetirement() {
        Context context = context(ignored -> {});
        KafkaSpeculativeCommitV1 commit = ObjectKafkaTestFixtures.commit(0);
        stage(context.coordinator, commit);
        Ready ready = ready(context, commit);
        context.bridge.publishNext(() -> {}).orElseThrow();

        context.tracker.discardOnTakeover(7);

        assertThat(context.tracker.pendingUnits()).isZero();
        assertThat(context.tracker.retainedLocatorBytes()).isEqualTo(ready.reservation.locatorBytes());
        assertThat(context.tracker.reservedLocatorBytes()).isEqualTo(ready.reservation.locatorBytes());
    }

    private static Context context(
            com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionPublicationObserver observer) {
        var coordinator =
                KafkaCoherentCommitCoordinatorV1.bootstrapObject(ObjectKafkaTestFixtures.fence(), 0, observer);
        int exactBytes = KafkaObjectStateCodecV1.exactLocatorBytes();
        var tracker = new KafkaObjectCompletionTrackerV1(8, exactBytes * 8L, 6);
        return new Context(
                coordinator, tracker, new KafkaObjectPublicationBridgeV1(binding(), 0, tracker, coordinator));
    }

    private static Ready ready(Context context, KafkaSpeculativeCommitV1 commit) {
        var before = context.coordinator.captureObject();
        return ready(
                context,
                commit,
                new KafkaObjectNativeStateV1(
                        before.committedProducerState().apply(commit),
                        before.transactionState().apply(commit),
                        before.leaderEpochIndex()
                                .observe(before.root().fence().kafkaLeaderEpoch(), commit.startOffset()),
                        commit.endOffsetExclusive(),
                        commit.endOffsetExclusive()));
    }

    private static Ready ready(Context context, KafkaSpeculativeCommitV1 commit, KafkaObjectNativeStateV1 nativeState) {
        var reservation = context.tracker.reserveBeforePosition();
        var ticket = context.tracker.assignPosition(reservation, commit);
        var extent = new KafkaObjectExtentIdentityV1(
                ObjectKafkaTestFixtures.digest(12), 0, 0, 256, 300, ObjectKafkaTestFixtures.digest(13));
        var locator = new KafkaObjectExtentLocatorV1(
                binding(), commit.startOffset(), commit.endOffsetExclusive(), extent, 0, 1);
        var physical = new KafkaObjectPhysicalFrontiersV1(extent.walRunRootSha());
        physical.resolve(extent);
        context.tracker.sequenceStarted(ticket);
        context.tracker.providerDispatched(ticket);
        context.tracker.providerResolved(
                ticket,
                physical,
                new KafkaVerifiedNwg1CommitV1(locator, ObjectKafkaTestFixtures.digest(14), 1),
                nativeState);
        return new Ready(reservation, ticket, locator);
    }

    private static void stage(KafkaCoherentCommitCoordinatorV1 coordinator, KafkaSpeculativeCommitV1 commit) {
        KafkaProtocolAppendPlanV1 plan = new KafkaProtocolAppendPlanV1(
                commit.expectedFence(),
                commit.batches().stream().map(batch -> batch.delta()).toList());
        var hooks = coordinator.protocolHooks(plan);
        hooks.validateBeforeOffsetAssignment();
        hooks.prepareAfterOffsetAssignment(
                new KafkaOffsetAssignedAppendV1(commit.startOffset(), commit.endOffsetExclusive(), ignored -> {
                    throw new AssertionError("Object M3 staging must not activate a native BookKeeper group");
                }));
    }

    private static KafkaObjectBindingKeyV1 binding() {
        var fence = ObjectKafkaTestFixtures.fence();
        return new KafkaObjectBindingKeyV1(
                fence.bindingId(), fence.topicIncarnation().topicId(), fence.partitionId(), fence.storageEpochId());
    }

    private static BindingReadSelector m4Selector(KafkaObjectCoherentProtocolSnapshotV1 snapshot) {
        return new BindingReadSelector(
                KafkaObjectBindingReadAdapterV1.bindingIdentity(snapshot),
                ObjectKafkaTestFixtures.digest(20),
                snapshot.root().fence().ownerEpoch(),
                1,
                1,
                SelectorMode.PREFERRED_ONLY,
                AdmissionState.ADMITTING,
                Optional.empty(),
                new CapabilityBinding(1, ObjectKafkaTestFixtures.digest(22)),
                List.of(),
                List.of());
    }

    private static void awaitProviderCalls(AtomicInteger calls, int expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (calls.get() < expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(calls).hasValue(expected);
    }

    private static long percentile(long[] sortedNanos, int percentile) {
        int index = Math.floorDiv(Math.addExact(Math.multiplyExact(sortedNanos.length, percentile), 99), 100) - 1;
        return sortedNanos[Math.max(0, Math.min(index, sortedNanos.length - 1))];
    }

    private static long throughput(long operations, long elapsedNanos) {
        return Math.max(1, Math.floorDiv(Math.multiplyExact(operations, 1_000_000_000L), elapsedNanos));
    }

    private record Context(
            KafkaCoherentCommitCoordinatorV1 coordinator,
            KafkaObjectCompletionTrackerV1 tracker,
            KafkaObjectPublicationBridgeV1 bridge) {}

    private record Ready(
            KafkaObjectCompletionTrackerV1.Reservation reservation,
            KafkaObjectCompletionTrackerV1.AssignedTicket ticket,
            KafkaObjectExtentLocatorV1 locator) {}
}
