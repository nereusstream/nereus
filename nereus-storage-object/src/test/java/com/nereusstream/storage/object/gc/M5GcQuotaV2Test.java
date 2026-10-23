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
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.spi.model.MetadataVersion;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.BirthResult;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.Result;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Entry;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Head;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerV1;
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
    void armedOriginalBodySurvivesEveryWriteAndCannotChangeOrBecomeAnInventedVersion() {
        var output = new com.nereusstream.storage.object.provider.ObjectIdentity(
                slot(1).objectKey(), 8, SyntheticDeleteAuthorityFixturesV2.digest("original-body"));
        for (int write = 1; write <= 3; write++) {
            var store = new Store(birthCapacity(LAYOUT, 2));
            await(store.quota().reserveBirth(declaredWorker(), slot(1)));
            long charged = await(store.quota().snapshot()).chargedCanonicalBytes();
            store.crashAfter = write;
            assertThatThrownBy(() -> await(store.quota().armBirth(claimKey(1), output)))
                    .hasRootCauseMessage("crashed after write");
            store.crashAfter = 0;
            await(store.quota().recover());
            assertThat(await(store.quota().armBirth(claimKey(1), output))).isEqualTo(BirthResult.WRITE_ARMED);
            var armed = await(store.quota().readBirth(claimKey(1))).orElseThrow();
            assertThat(M5ObjectBirthCreditV3.decode(armed.encode())).isEqualTo(armed);
            assertThat(armed.output()).contains(output);
            assertThat(armed.resource()).isEmpty();
            assertThat(await(store.quota().snapshot()).chargedCanonicalBytes()).isEqualTo(charged);
            assertThat(await(store.quota().assignBirth(claimKey(1), (PhysicalResourceIdV2.ObjectVersion) resource(1))))
                    .isEqualTo(BirthResult.ASSIGNED);
            var assigned = await(store.quota().readBirth(claimKey(1))).orElseThrow();
            assertThat(assigned.output()).contains(output);
            assertThat(M5ObjectBirthCreditV3.decode(assigned.encode())).isEqualTo(assigned);
            assertThat(await(store.quota().snapshot()).chargedCanonicalBytes()).isEqualTo(charged);
            var changed =
                    new com.nereusstream.storage.object.provider.ObjectIdentity(output.key(), 9, output.bodySha256());
            assertThatThrownBy(() -> await(store.quota().armBirth(claimKey(1), changed)))
                    .hasRootCauseMessage("write intent differs from the original body commitment");
        }
        var store = new Store(birthCapacity(LAYOUT, 1));
        await(store.quota().reserveBirth(declaredWorker(), slot(1)));
        await(store.quota().armBirth(claimKey(1), output));
        assertThat(await(store.quota().cancelNoDispatchBirth(claimKey(1))))
                .isEqualTo(BirthResult.CANCELLED_NO_DISPATCH);
        assertThat(await(store.quota().readBirth(claimKey(1))).orElseThrow().output())
                .contains(output);
        assertThatThrownBy(() ->
                        await(store.quota().assignBirth(claimKey(1), (PhysicalResourceIdV2.ObjectVersion) resource(1))))
                .hasRootCauseMessage("birth terminal differs from the original assignment/no-dispatch");
    }

    @Test
    void birthCapacityIncludesPermanentCreditHistoryAndTransfersOnlyToActualResourceIdentity() {
        var layout = Layout.withPhysicalMembers(LAYOUT.nativeRoot(), LAYOUT.namespace());
        long capacity = birthCapacity(layout, 1);
        var before = Head.empty(layout, capacity);
        assertThat(before.encode().length()).isEqualTo(M5GcQuotaRecordsV2.HEAD_BYTES);
        assertThatThrownBy(() -> before.expand(capacity + 1).reserveBirth(LAYOUT, declaredWorker(), slot(1)))
                .isInstanceOf(IllegalArgumentException.class);
        var pending = before.reserveBirth(layout, declaredWorker(), slot(1));
        assertThat(pending.availableBytes(layout)).isZero();
        assertThat(pending.reservedResources()).isZero(); // A chosen key is not a physical resource/version.
        assertThat(pending.encode().length()).isEqualTo(M5GcQuotaRecordsV2.BIRTH_HEAD_BYTES);
        assertThat(Head.decode(pending.encode())).isEqualTo(pending);
        var credit = pending.births().orElseThrow().pending().orElseThrow();
        assertThat(M5ObjectBirthCreditV3.decode(credit.encode())).isEqualTo(credit);
        assertThat(credit.resource()).isEmpty();
        var assigned = pending.clearPendingBirth()
                .finishBirth(credit.assign((PhysicalResourceIdV2.ObjectVersion) resource(1)), true);
        assertThat(assigned.usedBytes(layout)).isEqualTo(capacity);
        assertThat(assigned.pending().orElseThrow().resource())
                .isEqualTo(resource(1).sha256());
        assertThat(assigned.births().orElseThrow().unassigned()).isZero();
        var cancelled = pending.clearPendingBirth()
                .finishBirth(credit.cancelNoDispatch(), false)
                .clearPendingBirth();
        assertThat(cancelled.availableBytes(layout)).isEqualTo(layout.reservationCharge());
        assertThat(cancelled.usedBytes(layout))
                .isEqualTo(layout.headCharge()
                        + M5GcQuotaRecordsV2.BIRTH_HEAD_BYTES
                        - M5GcQuotaRecordsV2.HEAD_BYTES
                        + layout.birthRecordCharge());
        assertThatThrownBy(() -> cancelled.reserveBirth(layout, declaredWorker(), slot(2)))
                .isInstanceOf(IllegalArgumentException.class); // Terminal history remains charged.
        var wrong = (PhysicalResourceIdV2.ObjectVersion) resource(2);
        assertThatThrownBy(() -> credit.assign(wrong)).isInstanceOf(IllegalArgumentException.class);
        for (int size :
                new int[] {0, credit.encode().length() - 1, credit.encode().length() + 1}) {
            assertThatThrownBy(() -> M5ObjectBirthCreditV3.decode(CanonicalBytes.copyOf(
                            java.util.Arrays.copyOf(credit.encode().toByteArray(), size))))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void everyBirthWriteRecoversSameCreditAndAssignmentWithoutDoubleChargeOrInventedVersion() {
        for (int write = 1; write <= 3; write++) {
            var store = new Store(birthCapacity(LAYOUT, 2));
            store.crashAfter = write;
            assertThatThrownBy(() -> await(store.quota().reserveBirth(declaredWorker(), slot(1))))
                    .hasRootCauseMessage("crashed after write");
            store.crashAfter = 0;
            await(store.quota().recover());
            assertThat(await(store.quota().reserveBirth(declaredWorker(), slot(1))))
                    .isEqualTo(BirthResult.RESERVED);
            assertThat(await(store.quota().snapshot())
                            .head()
                            .births()
                            .orElseThrow()
                            .issued())
                    .isEqualTo(1);
        }
        for (int write = 1; write <= 5; write++) {
            var store = new Store(birthCapacity(LAYOUT, 2));
            await(store.quota().reserveBirth(declaredWorker(), slot(1)));
            var key = claimKey(1);
            var original = await(store.quota().readBirth(key)).orElseThrow();
            long charged = await(store.quota().snapshot()).chargedCanonicalBytes();
            store.crashAfter = write;
            assertThatThrownBy(() ->
                            await(store.quota().assignBirth(key, (PhysicalResourceIdV2.ObjectVersion) resource(1))))
                    .hasRootCauseMessage("crashed after write");
            store.crashAfter = 0;
            await(store.quota().recover());
            assertThat(await(store.quota().assignBirth(key, (PhysicalResourceIdV2.ObjectVersion) resource(1))))
                    .isEqualTo(BirthResult.ASSIGNED);
            var snapshot = await(store.quota().snapshot());
            assertThat(snapshot.chargedCanonicalBytes()).isEqualTo(charged);
            assertThat(snapshot.head().reservedResources()).isEqualTo(1);
            assertThat(snapshot.head().pending()).isEmpty();
            assertThat(snapshot.head().births().orElseThrow().pending()).isEmpty();
            assertThat(await(store.quota().readBirth(key)))
                    .contains(original.assign((PhysicalResourceIdV2.ObjectVersion) resource(1)));
            assertThatThrownBy(() -> await(store.quota().cancelNoDispatchBirth(key)))
                    .hasRootCauseMessage("birth terminal differs from the original assignment/no-dispatch");
        }
    }

    @Test
    void staleBirthAndNoDispatchCallbacksCannotRefundTwiceOrResurrectPermanentTerminal() {
        var store = new Store(birthCapacity(LAYOUT, 2));
        store.beforeHeadCas = () -> assertThat(await(store.quota().reserveBirth(declaredWorker(), slot(1))))
                .isEqualTo(BirthResult.RESERVED);
        assertThat(await(store.quota().reserveBirth(declaredWorker(), slot(1)))).isEqualTo(BirthResult.RESERVED);
        assertThat(await(store.quota().snapshot()).head().births().orElseThrow().issued())
                .isEqualTo(1);
        for (int write = 1; write <= 3; write++) {
            var interrupted = new Store(birthCapacity(LAYOUT, 1));
            await(interrupted.quota().reserveBirth(declaredWorker(), slot(1)));
            interrupted.crashAfter = write;
            assertThatThrownBy(() -> await(interrupted.quota().cancelNoDispatchBirth(claimKey(1))))
                    .hasRootCauseMessage("crashed after write");
            interrupted.crashAfter = 0;
            await(interrupted.quota().recover());
            assertThat(await(interrupted.quota().cancelNoDispatchBirth(claimKey(1))))
                    .isEqualTo(BirthResult.CANCELLED_NO_DISPATCH);
            assertThat(await(interrupted.quota().reserveBirth(declaredWorker(), slot(1))))
                    .isEqualTo(BirthResult.CANCELLED_NO_DISPATCH);
            var snapshot = await(interrupted.quota().snapshot());
            assertThat(snapshot.head().births().orElseThrow().cancelled()).isEqualTo(1);
            assertThat(snapshot.head().reservedResources()).isZero();
            assertThat(await(interrupted.quota().reserveBirth(declaredWorker(), slot(2))))
                    .isEqualTo(BirthResult.EXHAUSTED);
            assertThatThrownBy(() ->
                            await(interrupted.quota().assignBirth(claimKey(1), (PhysicalResourceIdV2.ObjectVersion)
                                    resource(1))))
                    .hasRootCauseMessage("birth terminal differs from the original assignment/no-dispatch");
        }
        store.beforeHeadCas = () -> assertThat(await(store.quota().cancelNoDispatchBirth(claimKey(1))))
                .isEqualTo(BirthResult.CANCELLED_NO_DISPATCH);
        assertThat(await(store.quota().cancelNoDispatchBirth(claimKey(1))))
                .isEqualTo(BirthResult.CANCELLED_NO_DISPATCH);
        assertThat(await(store.quota().snapshot()).head().births().orElseThrow().cancelled())
                .isEqualTo(1);
    }

    private static long birthCapacity(Layout layout, int slots) {
        return layout.headCharge()
                + M5GcQuotaRecordsV2.BIRTH_HEAD_BYTES
                - M5GcQuotaRecordsV2.HEAD_BYTES
                + slots * (layout.birthRecordCharge() + layout.reservationCharge());
    }

    /** Declaration fixture only; this is not a native Worker or Provider version qualification. */
    private static M5MaterializationWorkerV1 declaredWorker() {
        var digest = SyntheticDeleteAuthorityFixturesV2.digest("declared-worker");
        var identity = new IdentityEnvelope(
                digest,
                digest,
                new BindingIdentity(new TopicBindingId(digest), digest, digest),
                1,
                1,
                1,
                new CapabilityBinding(1, digest));
        return new M5MaterializationWorkerV1(identity, digest, digest, digest, digest, digest, digest);
    }

    private static M5PendingOutputInventoryV2.Slot slot(int id) {
        return new M5PendingOutputInventoryV2.Slot(
                M5PendingOutputInventoryV2.Role.PAYLOAD,
                id,
                ((PhysicalResourceIdV2.ObjectVersion) resource(id)).objectKey().value());
    }

    private static String claimKey(int id) {
        return new M5ObjectBirthCreditV3(
                        LAYOUT.scope(),
                        declaredWorker(),
                        slot(id),
                        2,
                        M5ObjectBirthCreditV3.State.RESERVED,
                        Optional.empty())
                .key();
    }

    @Test
    void memberProfileRejectsOldBudgetAndRetainsItsHistoryReservationAfterDone() {
        var members = Layout.withPhysicalMembers(LAYOUT.nativeRoot(), LAYOUT.namespace());
        long oldCapacity = LAYOUT.headCharge() + LAYOUT.reservationCharge();
        var oldHead = Head.empty(LAYOUT, oldCapacity);
        assertThatThrownBy(() -> members.verify(oldHead)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Head.empty(members, oldCapacity)
                        .reserve(members, resource(1).sha256()))
                .hasMessage("durable GC quota exhausted");
        assertThatThrownBy(() -> LAYOUT.nativeKey(
                        M5GcQuotaRecordsV2.physicalMemberKey(resource(1).sha256())))
                .isInstanceOf(IllegalArgumentException.class);
        var reserved = Head.empty(members, members.headCharge() + members.reservationCharge())
                .reserve(members, resource(1).sha256());
        var grant = reserved.pending().orElseThrow();
        var done = M5TargetDeleteDoneV2.from(
                SyntheticDeleteAuthorityFixturesV2.phases(resource(1)).get(3));
        var terminal = reserved.clearPending().settle(grant.settle(done)).clearPending();
        assertThat(terminal.usedBytes(members))
                .isEqualTo(members.headCharge()
                        + members.entryCharge()
                        + members.authorityKeyCharge()
                        + done.encode().length()
                        + members.permanentMemberCharge());
        assertThat(reserved.usedBytes(members) - terminal.usedBytes(members))
                .isEqualTo(ExactMetadataTransactionStoreV1.MAX_VALUE_BYTES
                        - done.encode().length());
        assertThatThrownBy(() -> LAYOUT.verify(terminal)).isInstanceOf(IllegalArgumentException.class);
        assertThat(Head.decode(terminal.encode())).isEqualTo(terminal);
    }

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
            this(LAYOUT.headCharge() + slots * LAYOUT.reservationCharge());
        }

        Store(long capacity) {
            seed(M5GcQuotaRecordsV2.HEAD_KEY, Head.empty(LAYOUT, capacity).encode());
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
