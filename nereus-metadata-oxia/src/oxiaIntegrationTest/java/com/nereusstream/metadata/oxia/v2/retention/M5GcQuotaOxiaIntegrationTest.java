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
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.metadata.oxia.v2.mutation.AsyncOxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.mutation.AuthorityRecord;
import com.nereusstream.metadata.oxia.v2.mutation.OxiaConditionalClient;
import com.nereusstream.metadata.oxia.v2.retention.M5PermanentDoneOxiaIntegrationTest.Fixture;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1;
import com.nereusstream.metadata.spi.retention.ExactMetadataTransactionStoreV1.VersionedValue;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.BirthResult;
import com.nereusstream.storage.object.gc.M5GcQuotaCoordinatorV2.Result;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Head;
import com.nereusstream.storage.object.gc.M5GcQuotaRecordsV2.Layout;
import com.nereusstream.storage.object.gc.M5ObjectBirthCreditV3;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityCodecV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteDoneV2;
import com.nereusstream.storage.object.gc.SyntheticDeleteAuthorityFixturesV2;
import com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1.IdentityEnvelope;
import com.nereusstream.storage.object.materialization.M5PendingOutputInventoryV2;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity;
import com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.CapabilityBinding;
import com.nereusstream.storage.object.retention.M5MaterializationWorkerV1;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.RangeScanConsumer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Real source-locked native metadata and CAS; deletion eligibility, owner and absence remain synthetic fixtures. */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class M5GcQuotaOxiaIntegrationTest {
    @Test
    void nativeOriginalArmedBodyRecoversAtEveryConditionalWriteWithoutCapacityRefund() throws Exception {
        var output = new com.nereusstream.storage.object.provider.ObjectIdentity(
                declaredSlot().objectKey(), 8, SyntheticDeleteAuthorityFixturesV2.digest("declared-original-output"));
        for (int write = 1; write <= 3; write++) {
            String root = Fixture.root();
            try (var original = new QFixture(root);
                    var independent = new QFixture(root)) {
                Fixture.await(original.route.initialize(birthCapacity(original.layout)));
                Fixture.await(original.route.quota().reserveBirth(declaredWorker(), declaredSlot()));
                long charged = Fixture.await(original.route.quota().snapshot()).chargedCanonicalBytes();
                var claim = declaredClaim(original.layout);
                original.faults.stopAfterWrites = write;
                assertThatThrownBy(() -> Fixture.await(original.route.quota().armBirth(claim.key(), output)))
                        .hasRootCauseMessage("injected quota client stopped");
                assertThat(Fixture.await(independent.route.quota().recover())).isTrue();
                assertThat(Fixture.await(independent.route.quota().armBirth(claim.key(), output)))
                        .isEqualTo(BirthResult.WRITE_ARMED);
                var armed = Fixture.await(independent.route.quota().readBirth(claim.key()))
                        .orElseThrow();
                assertThat(armed.output()).contains(output);
                assertThat(armed.resource()).isEmpty();
                assertThat(Fixture.await(independent.route.quota().snapshot()).chargedCanonicalBytes())
                        .isEqualTo(charged);
                assertThat(Fixture.await(
                                independent.route.quota().assignBirth(claim.key(), (PhysicalResourceIdV2.ObjectVersion)
                                        resource(1))))
                        .isEqualTo(BirthResult.ASSIGNED);
                var assigned = Fixture.await(independent.route.quota().readBirth(claim.key()))
                        .orElseThrow();
                assertThat(assigned.output()).contains(output);
                assertThat(M5ObjectBirthCreditV3.decode(assigned.encode())).isEqualTo(assigned);
                assertThat(Fixture.await(independent.route.quota().snapshot()).chargedCanonicalBytes())
                        .isEqualTo(charged);
            }
        }
    }

    @Test
    void nativeBirthQuotaRecoversEveryGrantAndAssignmentWriteWithoutDoubleAccounting() throws Exception {
        for (int write = 1; write <= 3; write++) {
            String root = Fixture.root();
            try (var original = new QFixture(root);
                    var independent = new QFixture(root)) {
                Fixture.await(original.route.initialize(birthCapacity(original.layout)));
                original.faults.stopAfterWrites = write;
                assertThatThrownBy(() ->
                                Fixture.await(original.route.quota().reserveBirth(declaredWorker(), declaredSlot())))
                        .hasRootCauseMessage("injected quota client stopped");
                assertThat(Fixture.await(independent.route.quota().recover())).isTrue();
                assertThat(Fixture.await(independent.route.quota().reserveBirth(declaredWorker(), declaredSlot())))
                        .isEqualTo(BirthResult.RESERVED);
                var snapshot = Fixture.await(independent.route.quota().snapshot());
                assertThat(snapshot.head().births().orElseThrow().issued()).isEqualTo(1);
                assertThat(snapshot.head().reservedResources()).isZero();
                assertThat(snapshot.availableCanonicalBytes()).isZero();
            }
        }
        for (int write = 1; write <= 5; write++) {
            String root = Fixture.root();
            try (var original = new QFixture(root);
                    var independent = new QFixture(root)) {
                Fixture.await(original.route.initialize(birthCapacity(original.layout)));
                Fixture.await(original.route.quota().reserveBirth(declaredWorker(), declaredSlot()));
                var claim = declaredClaim(original.layout);
                long charged = Fixture.await(original.route.quota().snapshot()).chargedCanonicalBytes();
                original.faults.stopAfterWrites = write;
                assertThatThrownBy(() -> Fixture.await(
                                original.route.quota().assignBirth(claim.key(), (PhysicalResourceIdV2.ObjectVersion)
                                        resource(1))))
                        .hasRootCauseMessage("injected quota client stopped");
                assertThat(Fixture.await(independent.route.quota().recover())).isTrue();
                assertThat(Fixture.await(
                                independent.route.quota().assignBirth(claim.key(), (PhysicalResourceIdV2.ObjectVersion)
                                        resource(1))))
                        .isEqualTo(BirthResult.ASSIGNED);
                var snapshot = Fixture.await(independent.route.quota().snapshot());
                assertThat(snapshot.chargedCanonicalBytes()).isEqualTo(charged);
                assertThat(snapshot.head().reservedResources()).isEqualTo(1);
                assertThat(snapshot.head().births().orElseThrow().assigned()).isEqualTo(1);
                assertThat(snapshot.head().pending()).isEmpty();
                assertThat(snapshot.head().births().orElseThrow().pending()).isEmpty();
                assertThat(Fixture.await(independent.route.quota().readBirth(claim.key()))
                                .orElseThrow()
                                .resource())
                        .contains((PhysicalResourceIdV2.ObjectVersion) resource(1));
            }
        }
    }

    @Test
    void nativeBirthHistoryBlocksReinitializationAndStaleNoDispatchCannotRefundTwice() throws Exception {
        try (var orphan = new QFixture(Fixture.root())) {
            var claim = declaredClaim(orphan.layout);
            Fixture.await(orphan.nativeFixture.client.put(
                    orphan.layout.nativeKey(claim.key()), claim.encode().toByteArray()));
            assertThatThrownBy(() -> Fixture.await(orphan.route.initialize(birthCapacity(orphan.layout))))
                    .hasRootCauseMessage("durable GC quota is not initialized");
        }
        String root = Fixture.root();
        try (var original = new QFixture(root);
                var independent = new QFixture(root)) {
            Fixture.await(original.route.initialize(birthCapacity(original.layout)));
            Fixture.await(original.route.quota().reserveBirth(declaredWorker(), declaredSlot()));
            var claim = declaredClaim(original.layout);
            original.faults.hold = (key, bytes) -> key.equals(original.layout.nativeKey(M5GcQuotaRecordsV2.HEAD_KEY));
            var delayed =
                    original.route.quota().cancelNoDispatchBirth(claim.key()).toCompletableFuture();
            Fixture.await(original.faults.held);
            assertThat(Fixture.await(independent.route.quota().cancelNoDispatchBirth(claim.key())))
                    .isEqualTo(BirthResult.CANCELLED_NO_DISPATCH);
            var terminal = Fixture.await(independent.route.quota().snapshot());
            original.faults.release.get().run();
            assertThat(Fixture.await(delayed)).isEqualTo(BirthResult.CANCELLED_NO_DISPATCH);
            assertThat(Fixture.await(original.route.quota().snapshot())).isEqualTo(terminal);
            assertThat(terminal.head().births().orElseThrow().cancelled()).isEqualTo(1);
            assertThat(terminal.availableCanonicalBytes()).isEqualTo(original.layout.reservationCharge());
            assertThat(Fixture.await(original.route.quota().reserveBirth(declaredWorker(), declaredSlot())))
                    .isEqualTo(BirthResult.CANCELLED_NO_DISPATCH);
        }
    }

    /** These record declarations exercise real native CAS only, not SDK/Worker/delete qualification. */
    static M5MaterializationWorkerV1 declaredWorker() {
        var digest = SyntheticDeleteAuthorityFixturesV2.digest("declared-quota-worker");
        return new M5MaterializationWorkerV1(
                new IdentityEnvelope(
                        digest,
                        digest,
                        new BindingIdentity(new TopicBindingId(digest), digest, digest),
                        1,
                        1,
                        1,
                        new CapabilityBinding(1, digest)),
                digest,
                digest,
                digest,
                digest,
                digest,
                digest);
    }

    static M5PendingOutputInventoryV2.Slot declaredSlot() {
        return new M5PendingOutputInventoryV2.Slot(
                M5PendingOutputInventoryV2.Role.PAYLOAD,
                0,
                ((PhysicalResourceIdV2.ObjectVersion) resource(1)).objectKey().value());
    }

    static M5ObjectBirthCreditV3 declaredClaim(Layout layout) {
        return new M5ObjectBirthCreditV3(
                layout.scope(),
                declaredWorker(),
                declaredSlot(),
                2,
                M5ObjectBirthCreditV3.State.RESERVED,
                Optional.empty());
    }

    static long birthCapacity(Layout layout) {
        return layout.headCharge()
                + M5GcQuotaRecordsV2.BIRTH_HEAD_BYTES
                - M5GcQuotaRecordsV2.HEAD_BYTES
                + layout.birthRecordCharge()
                + layout.reservationCharge();
    }

    @Test
    void memberProfileRejectsOldHeadAndOrphanNativeMemberRecords() throws Exception {
        String root = Fixture.root();
        var memberLayout = Layout.withPhysicalMembers(root, resource(0).namespace());
        try (var legacy = new QFixture(root);
                var members = new QFixture(root, memberLayout)) {
            legacy.initialize(1);
            var before = Fixture.await(
                    legacy.nativeFixture.client.get(legacy.layout.nativeKey(M5GcQuotaRecordsV2.HEAD_KEY)));
            assertThatThrownBy(() -> members.initialize(1))
                    .hasRootCauseMessage("quota head scope or accounted capacity differs");
            assertThat(Fixture.await(
                            legacy.nativeFixture.client.get(legacy.layout.nativeKey(M5GcQuotaRecordsV2.HEAD_KEY))))
                    .isEqualTo(before);
            assertThatThrownBy(() -> legacy.route.members())
                    .hasMessage("physical member accounting profile is not installed");
        }
        root = Fixture.root();
        memberLayout = Layout.withPhysicalMembers(root, resource(0).namespace());
        try (var members = new QFixture(root, memberLayout)) {
            Fixture.await(members.nativeFixture.client.put(
                    memberLayout.nativeKey(
                            M5GcQuotaRecordsV2.physicalMemberKey(resource(1).sha256())),
                    memberValue(1).toByteArray()));
            assertThatThrownBy(() -> members.initialize(1)).hasRootCauseMessage("durable GC quota is not initialized");
            assertThat(Fixture.await(
                            members.nativeFixture.client.get(memberLayout.nativeKey(M5GcQuotaRecordsV2.HEAD_KEY))))
                    .isNull();
        }
    }

    @Test
    void memberHistoryStaysChargedAfterPermanentDoneAndNativeReopen() throws Exception {
        String root = Fixture.root();
        var layout = Layout.withPhysicalMembers(root, resource(0).namespace());
        var member = memberValue(1);
        String key = layout.nativeKey(
                M5GcQuotaRecordsV2.physicalMemberKey(resource(1).sha256()));
        long charged;
        try (var q = new QFixture(root, layout)) {
            q.initialize(1);
            assertThat(q.reserve(1)).isEqualTo(Result.GRANTED);
            assertThat(q.reserve(2)).isEqualTo(Result.EXHAUSTED);
            Fixture.await(q.nativeFixture.client.put(key, member.toByteArray()));
            q.intent(1);
            var done = q.complete(1);
            assertThat(q.settle(1)).isEqualTo(Result.SETTLED);
            charged = Fixture.await(q.route.quota().snapshot()).chargedCanonicalBytes();
            assertThat(charged)
                    .isEqualTo(layout.headCharge()
                            + layout.entryCharge()
                            + layout.authorityKeyCharge()
                            + done.encode().length()
                            + layout.permanentMemberCharge())
                    .isGreaterThanOrEqualTo(q.actualCanonicalBytes());
        }
        try (var q = new QFixture(root, layout)) {
            assertThat(Fixture.await(q.route.quota().snapshot()).chargedCanonicalBytes())
                    .isEqualTo(charged);
            assertThat(CanonicalBytes.copyOf(
                            Fixture.await(q.nativeFixture.client.get(key)).value()))
                    .isEqualTo(member);
            assertThat(q.reserve(1)).isEqualTo(Result.SETTLED);
            assertThat(q.reserve(2)).isEqualTo(Result.EXHAUSTED);
        }
    }

    static CanonicalBytes memberValue(int id) {
        var digest = com.nereusstream.domain.bytes.Sha256Digest.hash(CanonicalBytes.copyOf(new byte[] {(byte) id}));
        var binding = new com.nereusstream.storage.object.read.control.M4ReadControlRecordsV1.BindingIdentity(
                new com.nereusstream.domain.identity.TopicBindingId(digest), digest, digest);
        return new OxiaPhysicalMemberInventoryV2.Inventory(
                        resource(id).sha256(),
                        digest,
                        java.util.List.of(new OxiaPhysicalMemberInventoryV2.Member(
                                com.nereusstream.storage.object.materialization.M5MaterializationRecordsV1
                                        .PositionDomain.KAFKA_OFFSET,
                                digest,
                                binding)))
                .encode();
    }

    @Test
    void initializationRejectsLegacyOrForeignAccountingAndNeverSilentlyImports() throws Exception {
        try (var q = new QFixture(Fixture.root())) {
            assertThatThrownBy(() -> q.persist(Optional.empty(), 1, 0))
                    .hasRootCauseMessage("durable GC quota is not initialized");
            var open = SyntheticDeleteAuthorityFixturesV2.phases(resource(1)).get(0);
            q.nativeFixture.persist(Optional.empty(), open);
            assertThatThrownBy(() -> q.initialize(2)).hasRootCauseMessage("durable GC quota is not initialized");
            assertThat(Fixture.await(q.faults.client.read(q.layout.nativeKey(M5GcQuotaRecordsV2.HEAD_KEY))))
                    .isEmpty();
        }
        try (var q = new QFixture(Fixture.root())) {
            q.initialize(2);
            long capacity = Fixture.await(q.route.quota().snapshot()).head().capacityBytes();
            assertThat(Fixture.await(q.route.initialize(capacity * 2)).head().capacityBytes())
                    .isEqualTo(capacity);
            var foreign = new Layout(
                    q.layout.nativeRoot(),
                    new PhysicalResourceIdV2.Namespace(
                            resource(0).namespace().providerKind(),
                            resource(0).namespace().serviceIdentity(),
                            com.nereusstream.domain.bytes.CanonicalUtf8.fromString("another-native-bucket")));
            var wrong = new OxiaQuotaTargetDeleteStoreV2(q.nativeFixture.client, foreign, q.nativeFixture.route);
            assertThatThrownBy(() -> Fixture.await(wrong.initialize(capacity)))
                    .hasRootCauseMessage("quota head scope or accounted capacity differs");
        }
    }

    @Test
    void exhaustedQuotaAndPendingNewGrantDoNotBlockExistingNativeIntentCompletion() throws Exception {
        String root = Fixture.root();
        try (var first = new QFixture(root);
                var other = new QFixture(root)) {
            first.initialize(2);
            first.intent(1);
            first.faults.stopAfterWrites = 1;
            assertThatThrownBy(() -> Fixture.await(first.route.quota().reserve(resource(2))))
                    .hasRootCauseMessage("injected quota client stopped");
            assertThat(Fixture.await(other.route.quota().snapshot()).head().pending())
                    .isPresent();
            // No quota mutation is needed to advance the already reserved intent, even with a full/pending head.
            other.complete(1);
            assertThat(Fixture.await(other.route.quota().snapshot()).head().pending())
                    .isPresent();
            assertThat(other.reserve(3)).isEqualTo(Result.EXHAUSTED);
            assertThat(other.settle(1)).isEqualTo(Result.SETTLED);
            // Permanent done/entry bytes remain charged; refund does not manufacture another complete active slot.
            assertThat(other.reserve(3)).isEqualTo(Result.EXHAUSTED);
            long capacity = Fixture.await(other.route.quota().snapshot()).head().capacityBytes();
            assertThat(Fixture.await(other.route.quota().expand(capacity + other.layout.reservationCharge())))
                    .isTrue();
            assertThat(other.reserve(3)).isEqualTo(Result.GRANTED);
            assertThat(Fixture.await(other.route.quota().snapshot()).head().reservedResources())
                    .isEqualTo(2);
            assertThat(Fixture.await(other.route.quota().snapshot()).head().settledResources())
                    .isEqualTo(1);
        }
    }

    @Test
    void everyAppliedNativeGrantAndRefundBoundaryRecoversAfterClientDeliveryLoss() throws Exception {
        for (boolean refund : new boolean[] {false, true}) {
            for (int boundary = 1; boundary <= 3; boundary++) {
                String root = Fixture.root();
                try (var first = new QFixture(root);
                        var restarted = new QFixture(root)) {
                    first.initialize(2);
                    if (refund) {
                        first.complete(1);
                    }
                    first.faults.stopAfterWrites = boundary;
                    assertThatThrownBy(() -> Fixture.await(
                                    refund
                                            ? first.route.quota().settle(resource(1))
                                            : first.route.quota().reserve(resource(1))))
                            .hasRootCauseMessage("injected quota client stopped");
                    Fixture.await(restarted.route.quota().recover());
                    assertThat(refund ? restarted.settle(1) : restarted.reserve(1))
                            .isEqualTo(refund ? Result.SETTLED : Result.GRANTED);
                    var snapshot = Fixture.await(restarted.route.quota().snapshot());
                    assertThat(snapshot.head().reservedResources()).isEqualTo(refund ? 0 : 1);
                    assertThat(snapshot.head().settledResources()).isEqualTo(refund ? 1 : 0);
                    assertThat(snapshot.head().pending()).isEmpty();
                    if (refund) {
                        assertThat(snapshot.chargedCanonicalBytes()).isEqualTo(restarted.actualCanonicalBytes());
                    }
                }
            }
        }
    }

    @Test
    void heldNativeReservationCannotDoubleChargeAfterAnotherClientCompletesAndSettles() throws Exception {
        String root = Fixture.root();
        try (var first = new QFixture(root);
                var other = new QFixture(root)) {
            first.initialize(2);
            first.faults.hold = (key, bytes) -> key.endsWith(M5GcQuotaRecordsV2.HEAD_KEY)
                    && Head.decode(bytes)
                            .pending()
                            .filter(entry -> !entry.settled())
                            .isPresent();
            var delayed = first.route.quota().reserve(resource(1));
            Fixture.await(first.faults.held);
            other.complete(1);
            assertThat(other.settle(1)).isEqualTo(Result.SETTLED);
            first.faults.release.get().run();
            assertThat(Fixture.await(delayed)).isEqualTo(Result.SETTLED);
            assertThat(Fixture.await(other.route.quota().snapshot()).head().settledResources())
                    .isEqualTo(1);
            assertThat(Fixture.await(other.route.quota().snapshot()).chargedCanonicalBytes())
                    .isEqualTo(other.actualCanonicalBytes());
        }
    }

    @Test
    void heldNativeRefundCannotDoubleCreditAfterAnotherClientSettlesAndClearsTheHead() throws Exception {
        String root = Fixture.root();
        try (var first = new QFixture(root);
                var other = new QFixture(root)) {
            first.initialize(2);
            first.complete(1);
            first.faults.hold = (key, bytes) -> key.endsWith(M5GcQuotaRecordsV2.HEAD_KEY)
                    && Head.decode(bytes)
                            .pending()
                            .filter(M5GcQuotaRecordsV2.Entry::settled)
                            .isPresent();
            var delayed = first.route.quota().settle(resource(1));
            Fixture.await(first.faults.held);
            assertThat(other.settle(1)).isEqualTo(Result.SETTLED);
            first.faults.release.get().run();
            assertThat(Fixture.await(delayed)).isEqualTo(Result.SETTLED);
            assertThat(Fixture.await(other.route.quota().snapshot()).head().settledResources())
                    .isEqualTo(1);
            assertThat(Fixture.await(other.route.quota().snapshot()).chargedCanonicalBytes())
                    .isEqualTo(other.actualCanonicalBytes());
        }
    }

    @Test
    void continuousNativeDoneHistoryHasMeasuredPermanentCostAndBoundedHeadAcrossClients() throws Exception {
        String root = Fixture.root();
        try (var q = new QFixture(root)) {
            q.initialize(2);
            for (int i = 0; i < 130; i++) {
                q.complete(i);
                assertThat(q.settle(i)).isEqualTo(Result.SETTLED);
                var snapshot = Fixture.await(q.route.quota().snapshot());
                assertThat(snapshot.head().encode().length()).isEqualTo(272);
                if (i % 32 == 0) {
                    System.out.printf(
                            java.util.Locale.ROOT,
                            "M5_NATIVE_GC_QUOTA done=%d charged=%d available=%d%n",
                            i + 1,
                            snapshot.chargedCanonicalBytes(),
                            snapshot.availableCanonicalBytes());
                }
            }
            var snapshot = Fixture.await(q.route.quota().snapshot());
            assertThat(snapshot.head().settledResources()).isEqualTo(130);
            assertThat(snapshot.head().reservedResources()).isZero();
            assertThat(snapshot.chargedCanonicalBytes()).isEqualTo(q.actualCanonicalBytes());
        }
        try (var reopened = new QFixture(root)) {
            assertThat(reopened.reserve(0)).isEqualTo(Result.SETTLED);
            assertThat(reopened.reserve(129)).isEqualTo(Result.SETTLED);
            assertThat(reopened.reserve(130)).isEqualTo(Result.GRANTED);
            assertThat(Fixture.await(reopened.route.quota().snapshot()).head().settledResources())
                    .isEqualTo(130);
        }
    }

    @Test
    void invalidNewAuthoritiesCannotConsumeQuotaOrBypassPermanentTerminalRejection() throws Exception {
        try (var q = new QFixture(Fixture.root())) {
            q.initialize(2);
            var before = Fixture.await(q.route.quota().snapshot());
            var full = SyntheticDeleteAuthorityFixturesV2.phases(resource(1)).get(3);
            assertThatThrownBy(() -> Fixture.await(q.route.compareAndSet(
                            Optional.empty(),
                            full.authorityKey(),
                            M5TargetDeleteAuthorityCodecV1.encodeAuthority(full))))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> Fixture.await(q.route.compareAndSet(
                            Optional.empty(),
                            full.authorityKey(),
                            M5TargetDeleteDoneV2.from(full).encode())))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(Fixture.await(q.route.quota().snapshot())).isEqualTo(before);
            q.complete(1);
            q.settle(1);
            var open = SyntheticDeleteAuthorityFixturesV2.phases(resource(1)).get(0);
            assertThat(Fixture.await(q.route.compareAndSet(
                            Optional.empty(),
                            open.authorityKey(),
                            M5TargetDeleteAuthorityCodecV1.encodeAuthority(open))))
                    .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.DEFINITIVE_CONFLICT);
        }
    }

    @Test
    void fixedDepthInitializationRejectsBothFamiliesAtDigestExtremesIncludingMalformedValues() throws Exception {
        for (boolean authority : new boolean[] {false, true}) {
            for (String digest : new String[] {"0".repeat(64), "f".repeat(64)}) {
                try (var q = new QFixture(Fixture.root())) {
                    String key = authority
                            ? "v2/physical-delete-m5-v2/" + digest + "/authority-v2"
                            : M5GcQuotaRecordsV2.ENTRY_PREFIX + digest;
                    Fixture.await(q.faults.client.createIfAbsent(
                            q.layout.nativeKey(key), CanonicalBytes.copyOf(new byte[] {1})));
                    assertThatThrownBy(() -> q.initialize(2))
                            .hasRootCauseMessage("durable GC quota is not initialized");
                    assertThat(Fixture.await(q.faults.client.read(q.layout.nativeKey(M5GcQuotaRecordsV2.HEAD_KEY))))
                            .isEmpty();
                }
            }
        }
    }

    static final class QFixture implements AutoCloseable {
        final Fixture nativeFixture;
        final Layout layout;
        final Faults faults;
        final OxiaQuotaTargetDeleteStoreV2 route;

        QFixture(String root) throws Exception {
            this(root, new Layout(root, resource(0).namespace()));
        }

        QFixture(String root, Layout layout) throws Exception {
            nativeFixture = new Fixture(root);
            this.layout = layout;
            faults = new Faults(new AsyncOxiaConditionalClient(nativeFixture.client));
            route = new OxiaQuotaTargetDeleteStoreV2(nativeFixture.client, faults, layout, nativeFixture.route);
        }

        void initialize(int activeSlots) throws Exception {
            Fixture.await(route.initialize(layout.headCharge() + layout.reservationCharge() * activeSlots));
        }

        Result reserve(int id) throws Exception {
            for (int i = 0; i < 8; i++) {
                var result = Fixture.await(route.quota().reserve(resource(id)));
                if (result != Result.RETRY) {
                    return result;
                }
            }
            throw new IllegalStateException("quota reservation did not converge within bounded test retries");
        }

        Result settle(int id) throws Exception {
            for (int i = 0; i < 8; i++) {
                var result = Fixture.await(route.quota().settle(resource(id)));
                if (result != Result.RETRY) {
                    return result;
                }
            }
            throw new IllegalStateException("quota settlement did not converge within bounded test retries");
        }

        VersionedValue persist(Optional<VersionedValue> previous, int id, int phase) throws Exception {
            var value = SyntheticDeleteAuthorityFixturesV2.phases(resource(id)).get(phase);
            assertThat(Fixture.await(route.compareAndSet(
                            previous, value.authorityKey(), M5TargetDeleteAuthorityCodecV1.encodeAuthority(value))))
                    .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.APPLIED_EXACT);
            return Fixture.await(route.read(value.authorityKey())).orElseThrow();
        }

        VersionedValue intent(int id) throws Exception {
            var previous = Optional.<VersionedValue>empty();
            for (int phase = 0; phase < 3; phase++) {
                previous = Optional.of(persist(previous, id, phase));
            }
            return previous.orElseThrow();
        }

        M5TargetDeleteDoneV2 complete(int id) throws Exception {
            var existing = Fixture.await(route.read(resource(id).authorityKey()));
            var intent = existing.orElseGet(() -> {
                try {
                    return intent(id);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            var full = persist(Optional.of(intent), id, 3);
            var done = M5TargetDeleteDoneV2.from(
                    M5TargetDeleteAuthorityCodecV1.decodeAuthority(full.canonicalStoredBytes()));
            assertThat(Fixture.await(route.compareAndSet(Optional.of(full), done.authorityKey(), done.encode())))
                    .isEqualTo(ExactMetadataTransactionStoreV1.MutationOutcome.APPLIED_EXACT);
            return done;
        }

        long actualCanonicalBytes() throws Exception {
            var head = Fixture.await(faults.client.read(layout.nativeKey(M5GcQuotaRecordsV2.HEAD_KEY)))
                    .orElseThrow();
            long total = head.key().length() + head.storedBytes().length();
            for (var range : OxiaQuotaTargetDeleteStoreV2.permanentRecordRanges(layout)) {
                var result = new CompletableFuture<Long>();
                nativeFixture.client.rangeScan(range.start(), range.end(), new RangeScanConsumer() {
                    private long count;

                    public boolean onNext(GetResult value) {
                        count += value.key().length() + value.value().length;
                        return true;
                    }

                    public void onError(Throwable failure) {
                        result.completeExceptionally(failure);
                    }

                    public void onCompleted() {
                        result.complete(count);
                    }
                });
                total += Fixture.await(result);
            }
            return total;
        }

        public void close() throws Exception {
            nativeFixture.close();
        }
    }

    static final class Faults implements OxiaConditionalClient {
        final OxiaConditionalClient client;
        volatile int stopAfterWrites;
        volatile boolean stopped;
        volatile BiPredicate<String, CanonicalBytes> hold;
        final CompletableFuture<Void> held = new CompletableFuture<>();
        final AtomicReference<Runnable> release = new AtomicReference<>();

        Faults(OxiaConditionalClient client) {
            this.client = client;
        }

        public CompletionStage<Optional<AuthorityRecord>> read(String key) {
            return stopped
                    ? CompletableFuture.failedFuture(new IllegalStateException("injected quota client stopped"))
                    : client.read(key);
        }

        public CompletionStage<Void> createIfAbsent(String key, CanonicalBytes value) {
            return after(client.createIfAbsent(key, value));
        }

        public CompletionStage<Void> compareAndSet(String key, CanonicalBytes value, long version) {
            var predicate = hold;
            if (predicate != null && predicate.test(key, value)) {
                hold = null;
                var result = new CompletableFuture<Void>();
                release.set(
                        () -> after(client.compareAndSet(key, value, version)).whenComplete((ignored, failure) -> {
                            if (failure == null) {
                                result.complete(null);
                            } else {
                                result.completeExceptionally(failure);
                            }
                        }));
                held.complete(null);
                return result;
            }
            return after(client.compareAndSet(key, value, version));
        }

        private CompletionStage<Void> after(CompletionStage<Void> actual) {
            return actual.thenCompose(ignored -> {
                if (stopAfterWrites > 0 && --stopAfterWrites == 0) {
                    stopped = true;
                    return CompletableFuture.failedFuture(new IllegalStateException("injected native response lost"));
                }
                return CompletableFuture.completedFuture(null);
            });
        }
    }

    static PhysicalResourceIdV2 resource(int id) {
        return SyntheticDeleteAuthorityFixturesV2.resource(id);
    }
}
