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
import com.nereusstream.pulsar.offload.PulsarBookKeeperDeletionCoordinatorV1.BookKeeperDeleter;
import com.nereusstream.pulsar.offload.PulsarBookKeeperDeletionCoordinatorV1.NativeMetadataCas;
import com.nereusstream.pulsar.offload.PulsarBookKeeperDeletionCoordinatorV1.ObjectRevalidator;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.ChildSource;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.MetadataSnapshot;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.Preference;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.RangeResult;
import com.nereusstream.pulsar.offload.PulsarDualSourceReadHandleV1.Source;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.DeleteState;
import com.nereusstream.pulsar.offload.PulsarSealedLedgerAttemptV1.RetentionClass;
import com.nereusstream.pulsar.offload.npd1.Npd1CodecV1.EntryPayload;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PulsarBookKeeperDeletionCoordinatorV1Test {
    private static final UUID ATTEMPT = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    @Test
    void drainsPinRevalidatesCasIntentClosesBkDeletesAndPublishesDoneInOrder() {
        Harness harness = harness(none(RetentionClass.DELETE_AFTER_VERIFIED));
        RangeResult admitted = harness.dual.read(0, 1).toCompletableFuture().join();

        CompletionStage<MetadataSnapshot> deletion = harness.coordinator.deleteOrReconcile();
        assertThat(deletion.toCompletableFuture()).isNotDone();
        assertThat(harness.order).isEmpty();
        admitted.release().toCompletableFuture().join();
        MetadataSnapshot done = deletion.toCompletableFuture().join();

        assertThat(done.deleteState()).isEqualTo(DeleteState.BK_DELETE_DONE);
        assertThat(harness.order).containsExactly("revalidate", "intent", "close-bk", "delete", "done");
    }

    @Test
    void revalidationFailureRetainsNoneAndReopensBookKeeperPinAdmission() {
        Harness harness = harness(none(RetentionClass.DELETE_AFTER_VERIFIED));
        harness.revalidator.fail = true;

        assertThat(failure(harness.coordinator.deleteOrReconcile())).hasMessage("revalidation failed");
        RangeResult read = harness.dual.read(0, 0).toCompletableFuture().join();

        assertThat(harness.metadata.get().deleteState()).isEqualTo(DeleteState.BK_DELETE_NONE);
        assertThat(read.source()).isEqualTo(Source.BOOKKEEPER);
        read.release().toCompletableFuture().join();
    }

    @Test
    void definitiveIntentCasFailureRetainsNoneAndReopensBookKeeperPins() {
        Harness harness = harness(none(RetentionClass.DELETE_AFTER_VERIFIED));
        harness.cas.failIntent = true;

        assertThat(failure(harness.coordinator.deleteOrReconcile())).hasMessage("intent CAS failed");
        RangeResult read = harness.dual.read(0, 0).toCompletableFuture().join();

        assertThat(read.source()).isEqualTo(Source.BOOKKEEPER);
        assertThat(harness.order).containsExactly("revalidate", "intent");
        read.release().toCompletableFuture().join();
    }

    @Test
    void physicalDeleteFailureLeavesIntentFencedAndRetryReconcilesToDone() {
        Harness harness = harness(none(RetentionClass.DELETE_AFTER_VERIFIED));
        harness.deleter.fail = true;

        assertThat(failure(harness.coordinator.deleteOrReconcile())).hasMessage("physical delete failed");
        assertThat(harness.metadata.get().deleteState()).isEqualTo(DeleteState.BK_DELETE_INTENT);
        RangeResult objectRead = harness.dual.read(0, 0).toCompletableFuture().join();
        assertThat(objectRead.source()).isEqualTo(Source.OBJECT);
        objectRead.release().toCompletableFuture().join();

        harness.deleter.fail = false;
        MetadataSnapshot done =
                harness.coordinator.deleteOrReconcile().toCompletableFuture().join();
        assertThat(done.deleteState()).isEqualTo(DeleteState.BK_DELETE_DONE);
        assertThat(harness.order).containsExactly("revalidate", "intent", "delete", "delete", "done");
    }

    @Test
    void restartIntentSkipsRevalidationAndIntentCas() {
        Harness harness = harness(intent());

        MetadataSnapshot done =
                harness.coordinator.deleteOrReconcile().toCompletableFuture().join();

        assertThat(done.deleteState()).isEqualTo(DeleteState.BK_DELETE_DONE);
        assertThat(harness.order).containsExactly("delete", "done");
    }

    @Test
    void retainBkPolicyNeverBeginsDeletion() {
        Harness harness = harness(none(RetentionClass.RETAIN_BK));

        assertThat(failure(harness.coordinator.deleteOrReconcile()))
                .hasMessage("native metadata is not eligible for verified BK deletion");
        assertThat(harness.order).isEmpty();
    }

    @Test
    void malformedIntentCasResultKeepsTheFenceAndNeverDeletes() {
        Harness harness = harness(none(RetentionClass.DELETE_AFTER_VERIFIED));
        harness.cas.invalidIntent = true;

        assertThat(failure(harness.coordinator.deleteOrReconcile()))
                .hasMessage("native delete-state CAS returned a different attempt or transition");
        RangeResult rerouted = harness.dual.read(0, 0).toCompletableFuture().join();

        assertThat(rerouted.source()).isEqualTo(Source.OBJECT);
        assertThat(harness.order).containsExactly("revalidate", "intent");
        rerouted.release().toCompletableFuture().join();
    }

    @Test
    void pinDrainTimeoutRetainsNoneAndReopensBookKeeperAfterTheLatePinDrains() {
        Harness harness = harness(none(RetentionClass.DELETE_AFTER_VERIFIED), 10);
        RangeResult admitted = harness.dual.read(0, 0).toCompletableFuture().join();

        assertThat(failure(harness.coordinator.deleteOrReconcile()))
                .hasMessage("BookKeeper source-pin drain timed out");
        assertThat(harness.metadata.get().deleteState()).isEqualTo(DeleteState.BK_DELETE_NONE);
        admitted.release().toCompletableFuture().join();
        RangeResult next = harness.dual.read(1, 1).toCompletableFuture().join();

        assertThat(next.source()).isEqualTo(Source.BOOKKEEPER);
        assertThat(harness.order).isEmpty();
        next.release().toCompletableFuture().join();
    }

    @Test
    void doneStateIsIdempotentAndDoesNoFurtherIo() {
        Harness harness = harness(done());

        MetadataSnapshot result =
                harness.coordinator.deleteOrReconcile().toCompletableFuture().join();

        assertThat(result.deleteState()).isEqualTo(DeleteState.BK_DELETE_DONE);
        assertThat(harness.order).isEmpty();
    }

    private static Harness harness(MetadataSnapshot initial) {
        return harness(initial, 5_000);
    }

    private static Harness harness(MetadataSnapshot initial, long pinDrainTimeoutMillis) {
        AtomicReference<MetadataSnapshot> metadata = new AtomicReference<>(initial);
        List<String> order = new ArrayList<>();
        FakeChild object = new FakeChild(Source.OBJECT, "close-object", order);
        FakeChild bookKeeper = new FakeChild(Source.BOOKKEEPER, "close-bk", order);
        PulsarDualSourceReadHandleV1 dual = new PulsarDualSourceReadHandleV1(
                metadata::get,
                Preference.BOOKKEEPER_FIRST,
                () -> CompletableFuture.completedFuture(object),
                () -> CompletableFuture.completedFuture(bookKeeper),
                (snapshot, failure) -> {});
        FakeRevalidator revalidator = new FakeRevalidator(order);
        FakeCas cas = new FakeCas(metadata, order);
        FakeDeleter deleter = new FakeDeleter(order);
        PulsarBookKeeperDeletionCoordinatorV1 coordinator = new PulsarBookKeeperDeletionCoordinatorV1(
                metadata::get, dual, revalidator, cas, deleter, pinDrainTimeoutMillis);
        return new Harness(metadata, dual, coordinator, revalidator, cas, deleter, order);
    }

    private static MetadataSnapshot none(RetentionClass retentionClass) {
        return new MetadataSnapshot(42, 7, ATTEMPT, true, retentionClass, DeleteState.BK_DELETE_NONE, false);
    }

    private static MetadataSnapshot intent() {
        return new MetadataSnapshot(
                42, 8, ATTEMPT, true, RetentionClass.DELETE_AFTER_VERIFIED, DeleteState.BK_DELETE_INTENT, true);
    }

    private static MetadataSnapshot done() {
        return new MetadataSnapshot(
                42, 9, ATTEMPT, true, RetentionClass.DELETE_AFTER_VERIFIED, DeleteState.BK_DELETE_DONE, true);
    }

    private static Throwable failure(CompletionStage<?> stage) {
        try {
            stage.toCompletableFuture().join();
            throw new AssertionError("expected deletion failure");
        } catch (CompletionException failure) {
            return failure.getCause();
        }
    }

    private record Harness(
            AtomicReference<MetadataSnapshot> metadata,
            PulsarDualSourceReadHandleV1 dual,
            PulsarBookKeeperDeletionCoordinatorV1 coordinator,
            FakeRevalidator revalidator,
            FakeCas cas,
            FakeDeleter deleter,
            List<String> order) {}

    private static final class FakeChild implements ChildSource {
        private final Source source;
        private final String closeEvent;
        private final List<String> order;

        private FakeChild(Source source, String closeEvent, List<String> order) {
            this.source = source;
            this.closeEvent = closeEvent;
            this.order = order;
        }

        @Override
        public CompletionStage<RangeResult> read(long firstEntryId, long lastEntryId) {
            List<EntryPayload> entries = new ArrayList<>();
            for (long entryId = firstEntryId; entryId <= lastEntryId; entryId++) {
                entries.add(new EntryPayload(entryId, new byte[] {(byte) entryId}));
            }
            return CompletableFuture.completedFuture(
                    new RangeResult(source, entries, () -> CompletableFuture.completedFuture(null)));
        }

        @Override
        public CompletionStage<Void> close() {
            order.add(closeEvent);
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class FakeRevalidator implements ObjectRevalidator {
        private final List<String> order;
        private boolean fail;

        private FakeRevalidator(List<String> order) {
            this.order = order;
        }

        @Override
        public CompletionStage<Void> revalidate(MetadataSnapshot expected) {
            order.add("revalidate");
            return fail
                    ? CompletableFuture.failedFuture(new IOException("revalidation failed"))
                    : CompletableFuture.completedFuture(null);
        }
    }

    private static final class FakeCas implements NativeMetadataCas {
        private final AtomicReference<MetadataSnapshot> metadata;
        private final List<String> order;
        private boolean failIntent;
        private boolean invalidIntent;

        private FakeCas(AtomicReference<MetadataSnapshot> metadata, List<String> order) {
            this.metadata = metadata;
            this.order = order;
        }

        @Override
        public CompletionStage<MetadataSnapshot> publishIntent(MetadataSnapshot expectedNone) {
            order.add("intent");
            if (failIntent) {
                return CompletableFuture.failedFuture(new IOException("intent CAS failed"));
            }
            if (invalidIntent) {
                return CompletableFuture.completedFuture(new MetadataSnapshot(
                        42,
                        expectedNone.version() + 1,
                        UUID.fromString("223e4567-e89b-12d3-a456-426614174000"),
                        true,
                        RetentionClass.DELETE_AFTER_VERIFIED,
                        DeleteState.BK_DELETE_INTENT,
                        true));
            }
            MetadataSnapshot intent = new MetadataSnapshot(
                    expectedNone.ledgerId(),
                    expectedNone.version() + 1,
                    expectedNone.attemptUuid(),
                    true,
                    expectedNone.retentionClass(),
                    DeleteState.BK_DELETE_INTENT,
                    true);
            metadata.set(intent);
            return CompletableFuture.completedFuture(intent);
        }

        @Override
        public CompletionStage<MetadataSnapshot> publishDone(MetadataSnapshot expectedIntent) {
            order.add("done");
            MetadataSnapshot done = new MetadataSnapshot(
                    expectedIntent.ledgerId(),
                    expectedIntent.version() + 1,
                    expectedIntent.attemptUuid(),
                    true,
                    expectedIntent.retentionClass(),
                    DeleteState.BK_DELETE_DONE,
                    true);
            metadata.set(done);
            return CompletableFuture.completedFuture(done);
        }
    }

    private static final class FakeDeleter implements BookKeeperDeleter {
        private final List<String> order;
        private boolean fail;

        private FakeDeleter(List<String> order) {
            this.order = order;
        }

        @Override
        public CompletionStage<Void> deleteAndProveAbsent(long ledgerId) {
            order.add("delete");
            return fail
                    ? CompletableFuture.failedFuture(new IOException("physical delete failed"))
                    : CompletableFuture.completedFuture(null);
        }
    }
}
