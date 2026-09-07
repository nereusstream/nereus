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

package com.nereusstream.storage.object.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.Result;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Entry;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Head;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class M5GcQuotaV2Test {
    private static final Layout LAYOUT = new Layout("/quota-test", resource(0).namespace());

    @Test
    void fixedCanonicalValuesRejectEveryCorruptionAndInvalidAccounting() {
        var head = Head.empty(LAYOUT, LAYOUT.headCharge() + LAYOUT.reservationCharge())
                .reserve(LAYOUT, resource(1).sha256());
        var entry = head.pending().orElseThrow();
        assertThat(head.encode().length()).isEqualTo(272);
        assertThat(entry.encode().length()).isEqualTo(156);
        assertThat(Head.decode(head.encode())).isEqualTo(head);
        assertThat(Entry.decode(entry.encode())).isEqualTo(entry);
        for (var value : new CanonicalBytes[] {head.encode(), entry.encode()}) {
            for (int offset = 0; offset < value.length(); offset++) {
                var bad = value.toByteArray();
                bad[offset] ^= 1;
                assertThatThrownBy(() -> {
                            if (value.length() == 272) {
                                Head.decode(CanonicalBytes.copyOf(bad));
                            } else {
                                Entry.decode(CanonicalBytes.copyOf(bad));
                            }
                        })
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }
        assertThatThrownBy(() -> Head.empty(LAYOUT, LAYOUT.headCharge() - 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Layout("/other", resource(0).namespace()).verify(head))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Head(LAYOUT.scope(), 1, Long.MAX_VALUE, 0, Long.MAX_VALUE, 0, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void exhaustedQuotaRetainsExistingReservationAndRefundsOnlyPermanentCompactDone() {
        var store = new Store(1);
        var quota = store.quota();
        assertThat(await(quota.reserve(resource(1)))).isEqualTo(Result.GRANTED);
        assertThat(await(quota.reserve(resource(2)))).isEqualTo(Result.EXHAUSTED);
        assertThat(await(quota.requireExistingReservation(resource(1))).settled())
                .isFalse();
        var full = SyntheticDeleteAuthorityFixturesV2.phases(resource(1)).get(3);
        store.seed(resource(1).authorityKey(), M5TargetDeleteAuthorityCodecV1.encodeAuthority(full));
        assertThatThrownBy(() -> await(quota.settle(resource(1))))
                .hasRootCauseMessage("active or full done authority still retains its complete reservation");
        store.done(1);
        assertThat(await(quota.settle(resource(1)))).isEqualTo(Result.SETTLED);
        assertThat(await(quota.settle(resource(1)))).isEqualTo(Result.SETTLED);
        var snapshot = await(quota.snapshot());
        assertThat(snapshot.head().reservedResources()).isZero();
        assertThat(snapshot.head().settledResources()).isEqualTo(1);
        assertThat(snapshot.chargedCanonicalBytes()).isEqualTo(store.actualCanonicalBytes());
        assertThat(await(quota.reserve(resource(1)))).isEqualTo(Result.SETTLED);
        assertThat(await(quota.reserve(resource(2)))).isEqualTo(Result.EXHAUSTED);
        assertThat(await(quota.expand(snapshot.head().capacityBytes() + LAYOUT.reservationCharge())))
                .isTrue();
        assertThat(await(quota.reserve(resource(2)))).isEqualTo(Result.GRANTED);
    }

    @Test
    void restartAtEveryGrantWriteRecoversExactReservationWithoutDoubleCharge() {
        for (int write = 1; write <= 3; write++) {
            var store = new Store(2);
            store.crashAfter = write;
            assertThatThrownBy(() -> await(store.quota().reserve(resource(1))))
                    .hasRootCauseMessage("crashed after write");
            store.crashAfter = 0;
            var restarted = store.quota();
            await(restarted.recover());
            assertThat(await(restarted.reserve(resource(1)))).isEqualTo(Result.GRANTED);
            assertThat(await(restarted.snapshot()).head().reservedResources()).isEqualTo(1);
            assertThat(await(restarted.snapshot()).head().pending()).isEmpty();
        }
    }

    @Test
    void restartAtEveryRefundWriteRetainsGrantIdentityAndCannotRefundTwice() {
        for (int write = 1; write <= 3; write++) {
            var store = new Store(2);
            await(store.quota().reserve(resource(1)));
            var original = await(store.quota().requireExistingReservation(resource(1)));
            store.done(1);
            store.crashAfter = write;
            assertThatThrownBy(() -> await(store.quota().settle(resource(1))))
                    .hasRootCauseMessage("crashed after write");
            store.crashAfter = 0;
            var restarted = store.quota();
            await(restarted.recover());
            assertThat(await(restarted.settle(resource(1)))).isEqualTo(Result.SETTLED);
            assertThat(await(restarted.requireExistingReservation(resource(1))).grant())
                    .isEqualTo(original);
            assertThat(await(restarted.snapshot()).head().settledResources()).isEqualTo(1);
            assertThat(await(restarted.snapshot()).head().reservedResources()).isZero();
            assertThat(await(restarted.snapshot()).chargedCanonicalBytes()).isEqualTo(store.actualCanonicalBytes());
        }
    }

    @Test
    void staleAbsentEntryCannotDoubleChargeAfterAnotherWorkerGrantsAndSettles() {
        var store = new Store(2);
        store.beforeHeadCas = () -> {
            assertThat(await(store.quota().reserve(resource(1)))).isEqualTo(Result.GRANTED);
            store.done(1);
            assertThat(await(store.quota().settle(resource(1)))).isEqualTo(Result.SETTLED);
        };
        assertThat(await(store.quota().reserve(resource(1)))).isEqualTo(Result.SETTLED);
        assertThat(await(store.quota().snapshot()).head().settledResources()).isEqualTo(1);
        assertThat(await(store.quota().snapshot()).chargedCanonicalBytes()).isEqualTo(store.actualCanonicalBytes());
    }

    @Test
    void staleGrantCannotRefundTwiceAfterAnotherWorkerSettlesAndClearsTheHead() {
        var store = new Store(2);
        await(store.quota().reserve(resource(1)));
        store.done(1);
        store.beforeHeadCas =
                () -> assertThat(await(store.quota().settle(resource(1)))).isEqualTo(Result.SETTLED);
        assertThat(await(store.quota().settle(resource(1)))).isEqualTo(Result.SETTLED);
        assertThat(await(store.quota().snapshot()).head().settledResources()).isEqualTo(1);
        assertThat(await(store.quota().snapshot()).chargedCanonicalBytes()).isEqualTo(store.actualCanonicalBytes());
    }

    @Test
    void pendingOperationDoesNotBlockExistingReservedIntentAndExpansionPreservesPending() {
        var store = new Store(2);
        await(store.quota().reserve(resource(1)));
        store.crashAfter = 1;
        assertThatThrownBy(() -> await(store.quota().reserve(resource(2)))).hasRootCauseMessage("crashed after write");
        store.crashAfter = 0;
        var quota = store.quota();
        var before = await(quota.snapshot());
        assertThat(await(quota.requireExistingReservation(resource(1))).settled())
                .isFalse();
        assertThat(await(quota.expand(before.head().capacityBytes() + 500))).isTrue();
        assertThat(await(quota.snapshot()).head().pending())
                .isEqualTo(before.head().pending());
        assertThatThrownBy(() -> await(quota.expand(before.head().capacityBytes())))
                .hasRootCauseMessage("quota expansion must be strictly monotonic");
        assertThat(await(quota.recover())).isTrue();
        assertThat(await(quota.reserve(resource(2)))).isEqualTo(Result.GRANTED);
    }

    @Test
    void permanentHistoryOutlivesActiveCapacityAndHeadNeverAccumulatesEntries() {
        var store = new Store(2);
        for (int i = 0; i < 130; i++) {
            assertThat(await(store.quota().reserve(resource(i)))).isEqualTo(Result.GRANTED);
            store.done(i);
            assertThat(await(store.quota().settle(resource(i)))).isEqualTo(Result.SETTLED);
            assertThat(await(store.quota().snapshot()).head().encode().length()).isEqualTo(272);
        }
        assertThat(await(store.quota().snapshot()).head().settledResources()).isEqualTo(130);
        assertThat(await(store.quota().snapshot()).chargedCanonicalBytes()).isEqualTo(store.actualCanonicalBytes());
        assertThat(await(store.quota().reserve(resource(0)))).isEqualTo(Result.SETTLED);
    }

    @Test
    void unknownOrMissingHeadAndUnaccountedAuthorityNeverMeanFreeCapacity() {
        var store = new Store(2);
        store.done(1);
        assertThatThrownBy(() -> await(store.quota().reserve(resource(1))))
                .hasRootCauseMessage("unaccounted authority cannot be imported into a zero quota grant");
        store.failReads = true;
        assertThatThrownBy(() -> await(store.quota().reserve(resource(2)))).hasRootCauseMessage("unavailable metadata");
        store.failReads = false;
        store.values.remove(M5GcQuotaRecordsV2.HEAD_KEY);
        assertThatThrownBy(() -> await(store.quota().reserve(resource(2))))
                .hasRootCauseMessage("durable GC quota is not initialized");
    }

    private static PhysicalResourceIdV2 resource(int id) {
        return SyntheticDeleteAuthorityFixturesV2.resource(id);
    }

    private static <T> T await(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }

    private static final class Store implements ExactMetadataTransactionStoreV1 {
        final Map<String, VersionedValue> values = new HashMap<>();
        long version;
        int crashAfter;
        boolean failReads;
        Runnable beforeHeadCas;

        Store(int slots) {
            seed(
                    M5GcQuotaRecordsV2.HEAD_KEY,
                    Head.empty(LAYOUT, LAYOUT.headCharge() + slots * LAYOUT.reservationCharge())
                            .encode());
        }

        M5GcQuotaCoordinatorV2 quota() {
            return new M5GcQuotaCoordinatorV2(LAYOUT, this, this);
        }

        void done(int id) {
            seed(
                    resource(id).authorityKey(),
                    M5TargetDeleteDoneV2.from(SyntheticDeleteAuthorityFixturesV2.phases(resource(id))
                                    .get(3))
                            .encode());
        }

        void seed(String key, CanonicalBytes bytes) {
            values.put(
                    key,
                    VersionedValue.of(
                            key,
                            bytes,
                            new MetadataVersion(CanonicalBytes.copyOf(
                                    ByteBuffer.allocate(8).putLong(++version).array()))));
        }

        long actualCanonicalBytes() {
            return values.values().stream()
                    .mapToLong(value -> LAYOUT.nativeKey(value.key()).length()
                            + value.canonicalStoredBytes().length())
                    .sum();
        }

        public CompletionStage<Optional<VersionedValue>> read(String key) {
            return failReads
                    ? CompletableFuture.failedFuture(new IllegalStateException("unavailable metadata"))
                    : CompletableFuture.completedFuture(Optional.ofNullable(values.get(key)));
        }

        public CompletionStage<MutationOutcome> compareAndSet(
                Optional<VersionedValue> previous, String key, CanonicalBytes bytes) {
            if (key.equals(M5GcQuotaRecordsV2.HEAD_KEY) && beforeHeadCas != null) {
                var action = beforeHeadCas;
                beforeHeadCas = null;
                action.run();
            }
            if (!Optional.ofNullable(values.get(key)).equals(previous)) {
                return CompletableFuture.completedFuture(MutationOutcome.DEFINITIVE_CONFLICT);
            }
            seed(key, bytes);
            if (crashAfter > 0 && --crashAfter == 0) {
                return CompletableFuture.failedFuture(new IllegalStateException("crashed after write"));
            }
            return CompletableFuture.completedFuture(MutationOutcome.APPLIED_EXACT);
        }

        public CompletionStage<TransactionOutcome> conditionalTransaction(ExactTransaction transaction) {
            return CompletableFuture.completedFuture(TransactionOutcome.UNSUPPORTED);
        }

        public boolean supportsAtomicMultiKeyTransactions() {
            return false;
        }
    }
}
