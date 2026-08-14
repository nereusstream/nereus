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

import static org.assertj.core.api.Assertions.assertThat;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.ChildSource;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.FailureKind;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.MetadataSnapshot;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.Preference;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.RangeResult;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.Source;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.SourceReadException;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.DeleteState;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PulsarDualSourceReadHandleV1Test {
    private static final UUID ATTEMPT = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    @Test
    void beforeCompletionReadsBookKeeperOnlyDespiteObjectPreference() {
        Harness harness = harness(preCompletion(), Preference.OBJECT_FIRST);

        RangeResult result = harness.handle.read(0, 2).toCompletableFuture().join();

        assertThat(result.source()).isEqualTo(Source.BOOKKEEPER);
        assertThat(harness.bookKeeper.reads).isEqualTo(1);
        assertThat(harness.object.reads).isZero();
        result.release().toCompletableFuture().join();
    }

    @Test
    void completeLedgerUsesPreferredObjectAndKeepsBookKeeperLazy() {
        Harness harness = harness(completeNone(), Preference.OBJECT_FIRST);

        RangeResult result = harness.handle.read(1, 3).toCompletableFuture().join();

        assertThat(result.source()).isEqualTo(Source.OBJECT);
        assertThat(result.entries()).extracting(EntryPayload::entryId).containsExactly(1L, 2L, 3L);
        assertThat(harness.bookKeeper.reads).isZero();
        result.release().toCompletableFuture().join();
    }

    @Test
    void objectIntegrityFailureReleasesPartialRangeAndRetriesWholeRangeFromBookKeeper() {
        Harness harness = harness(completeNone(), Preference.OBJECT_FIRST);
        harness.object.failureKind = FailureKind.INTEGRITY;
        harness.object.returnPartialOnFailure = true;
        harness.object.failRelease = true;

        RangeResult result = harness.handle.read(0, 3).toCompletableFuture().join();

        assertThat(result.source()).isEqualTo(Source.BOOKKEEPER);
        assertThat(result.entries()).extracting(EntryPayload::entryId).containsExactly(0L, 1L, 2L, 3L);
        assertThat(harness.object.releases).isEqualTo(1);
        assertThat(harness.observedObjectFailures).hasValue(1);
        result.release().toCompletableFuture().join();
        assertThat(harness.handle.close().toCompletableFuture()).isCompleted();
    }

    @Test
    void bookKeeperFirstFallsBackOnlyForNoSuchLedger() {
        Harness missing = harness(completeNone(), Preference.BOOKKEEPER_FIRST);
        missing.bookKeeper.failureKind = FailureKind.NO_SUCH_LEDGER;
        RangeResult fallback = missing.handle.read(0, 1).toCompletableFuture().join();
        assertThat(fallback.source()).isEqualTo(Source.OBJECT);
        fallback.release().toCompletableFuture().join();

        Harness transientFailure = harness(completeNone(), Preference.BOOKKEEPER_FIRST);
        transientFailure.bookKeeper.failureKind = FailureKind.TRANSIENT;
        assertThat(failure(transientFailure.handle.read(0, 1)).kind()).isEqualTo(FailureKind.TRANSIENT);
        assertThat(transientFailure.object.reads).isZero();
    }

    @Test
    void cancellationInvalidRangeAndUnsupportedErrorsNeverFallback() {
        for (FailureKind kind : List.of(FailureKind.CANCELLED, FailureKind.INVALID_RANGE, FailureKind.UNSUPPORTED)) {
            Harness harness = harness(completeNone(), Preference.OBJECT_FIRST);
            harness.object.failureKind = kind;
            assertThat(failure(harness.handle.read(0, 1)).kind()).isEqualTo(kind);
            assertThat(harness.bookKeeper.reads).isZero();
        }
    }

    @Test
    void dualFailureReturnsPrimaryAndSuppressesSecondary() {
        Harness harness = harness(completeNone(), Preference.OBJECT_FIRST);
        harness.object.failureKind = FailureKind.TIMEOUT;
        harness.bookKeeper.failureKind = FailureKind.TRANSIENT;

        SourceReadException failure = failure(harness.handle.read(0, 1));

        assertThat(failure.source()).isEqualTo(Source.OBJECT);
        assertThat(failure.kind()).isEqualTo(FailureKind.TIMEOUT);
        assertThat(failure.getSuppressed()).hasSize(1);
    }

    @Test
    void deleteIntentAuthorizesObjectOnlyEvenWhenPhysicalBookKeeperStillExists() {
        Harness harness = harness(intent(), Preference.BOOKKEEPER_FIRST);

        RangeResult result = harness.handle.read(0, 1).toCompletableFuture().join();

        assertThat(result.source()).isEqualTo(Source.OBJECT);
        assertThat(harness.bookKeeper.reads).isZero();
        result.release().toCompletableFuture().join();
    }

    @Test
    void bookKeeperFenceWaitsForAdmittedPinAndReroutesNewReadsToObject() {
        Harness harness = harness(completeNone(), Preference.BOOKKEEPER_FIRST);
        RangeResult admitted = harness.handle.read(0, 1).toCompletableFuture().join();

        CompletionStage<Void> drain = harness.handle.fenceBookKeeper(harness.metadata.get());
        assertThat(drain.toCompletableFuture()).isNotDone();
        RangeResult rerouted = harness.handle.read(2, 3).toCompletableFuture().join();
        assertThat(rerouted.source()).isEqualTo(Source.OBJECT);

        admitted.release().toCompletableFuture().join();
        assertThat(drain.toCompletableFuture()).isCompleted();
        rerouted.release().toCompletableFuture().join();
    }

    @Test
    void closeDrainsAcceptedRangeAndClosesEachInitializedChildExactlyOnce() {
        Harness harness = harness(completeNone(), Preference.OBJECT_FIRST);
        RangeResult admitted = harness.handle.read(0, 1).toCompletableFuture().join();

        CompletionStage<Void> firstClose = harness.handle.close();
        CompletionStage<Void> secondClose = harness.handle.close();
        assertThat(firstClose).isSameAs(secondClose);
        assertThat(firstClose.toCompletableFuture()).isNotDone();
        assertThat(failure(harness.handle.read(2, 3)).kind()).isEqualTo(FailureKind.CLOSED);

        admitted.release().toCompletableFuture().join();
        firstClose.toCompletableFuture().join();
        assertThat(harness.object.closes).isEqualTo(1);
        assertThat(harness.bookKeeper.closes).isZero();
    }

    @Test
    void wrongChildSourceIsReleasedAndCannotMixWithFallback() {
        Harness harness = harness(completeNone(), Preference.OBJECT_FIRST);
        harness.object.returnedSource = Source.BOOKKEEPER;

        RangeResult result = harness.handle.read(0, 2).toCompletableFuture().join();

        assertThat(result.source()).isEqualTo(Source.BOOKKEEPER);
        assertThat(harness.object.releases).isEqualTo(1);
        assertThat(result.entries()).extracting(EntryPayload::entryId).containsExactly(0L, 1L, 2L);
        result.release().toCompletableFuture().join();
    }

    private static Harness harness(MetadataSnapshot initial, Preference preference) {
        AtomicReference<MetadataSnapshot> metadata = new AtomicReference<>(initial);
        FakeChild object = new FakeChild(Source.OBJECT);
        FakeChild bookKeeper = new FakeChild(Source.BOOKKEEPER);
        AtomicInteger observed = new AtomicInteger();
        PulsarDualSourceReadHandleV1 handle = new PulsarDualSourceReadHandleV1(
                metadata::get,
                preference,
                () -> CompletableFuture.completedFuture(object),
                () -> CompletableFuture.completedFuture(bookKeeper),
                (snapshot, failure) -> observed.incrementAndGet());
        return new Harness(metadata, handle, object, bookKeeper, observed);
    }

    private static MetadataSnapshot preCompletion() {
        return new MetadataSnapshot(
                42, 1, null, false, RetentionClass.DELETE_AFTER_VERIFIED, DeleteState.BK_DELETE_NONE, false);
    }

    private static MetadataSnapshot completeNone() {
        return new MetadataSnapshot(
                42, 2, ATTEMPT, true, RetentionClass.DELETE_AFTER_VERIFIED, DeleteState.BK_DELETE_NONE, false);
    }

    private static MetadataSnapshot intent() {
        return new MetadataSnapshot(
                42, 3, ATTEMPT, true, RetentionClass.DELETE_AFTER_VERIFIED, DeleteState.BK_DELETE_INTENT, true);
    }

    private static SourceReadException failure(CompletionStage<?> stage) {
        try {
            stage.toCompletableFuture().join();
            throw new AssertionError("expected source read failure");
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof SourceReadException sourceFailure) {
                return sourceFailure;
            }
            throw failure;
        }
    }

    private record Harness(
            AtomicReference<MetadataSnapshot> metadata,
            PulsarDualSourceReadHandleV1 handle,
            FakeChild object,
            FakeChild bookKeeper,
            AtomicInteger observedObjectFailures) {}

    private static final class FakeChild implements ChildSource {
        private final Source source;
        private int reads;
        private int releases;
        private int closes;
        private FailureKind failureKind;
        private boolean returnPartialOnFailure;
        private boolean failRelease;
        private Source returnedSource;

        private FakeChild(Source source) {
            this.source = source;
            returnedSource = source;
        }

        @Override
        public CompletionStage<RangeResult> read(long firstEntryId, long lastEntryId) {
            reads++;
            if (failureKind != null) {
                RangeResult partial = returnPartialOnFailure ? range(returnedSource, firstEntryId, firstEntryId) : null;
                return CompletableFuture.failedFuture(
                        new SourceReadException(source, failureKind, "injected " + failureKind, null, partial));
            }
            return CompletableFuture.completedFuture(range(returnedSource, firstEntryId, lastEntryId));
        }

        @Override
        public CompletionStage<Void> close() {
            closes++;
            return CompletableFuture.completedFuture(null);
        }

        private RangeResult range(Source resultSource, long first, long last) {
            List<EntryPayload> entries = new ArrayList<>();
            for (long entryId = first; entryId <= last; entryId++) {
                entries.add(new EntryPayload(entryId, new byte[] {(byte) entryId}));
            }
            return new RangeResult(resultSource, entries, () -> {
                releases++;
                return failRelease
                        ? CompletableFuture.failedFuture(new IllegalStateException("release failed"))
                        : CompletableFuture.completedFuture(null);
            });
        }
    }
}
