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

package com.nereusstream.kafka.bookkeeper.replication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.kafka.bookkeeper.recovery.KafkaElectionKindV1;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class KafkaReplicaFollowerKernelV1Test {
    @Test
    void advancesObservedOnlyAfterTheExactDescriptorIsDurablyJournaled() {
        Fixture fixture = fixture(new KafkaReplicaEligibilityBoundsV1(10, 10_000, 1_000));
        KafkaReplicaCommitDescriptorV1 descriptor = KafkaReplicationTestFixtures.descriptor(100, 101, 2);

        KafkaReplicaProgressSnapshotV1 progress = fixture.kernel.observe(descriptor);

        assertThat(progress.observedEndOffset()).isEqualTo(101);
        assertThat(progress.appliedEndOffset()).isEqualTo(100);
        assertThat(progress.observedStateVersion()).isEqualTo(2);
        assertThat(progress.observedDescriptorDigest())
                .contains(KafkaReplicaCommitDescriptorCodecV1.digest(descriptor));
        assertThat(progress.isrEligibility().eligible()).isTrue();
        assertThat(fixture.storage.appendCalls).isEqualTo(1);
    }

    @Test
    void substitutedJournalProofLeavesObservedAtItsPreviousCut() {
        Fixture fixture = fixture(new KafkaReplicaEligibilityBoundsV1(10, 10_000, 1_000));
        fixture.storage.proofOverride =
                new KafkaReplicaJournalAppendProofV1(0, 1, KafkaReplicationTestFixtures.digest(99));

        assertThatThrownBy(() -> fixture.kernel.observe(KafkaReplicationTestFixtures.descriptor(100, 101, 2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sync proof");
        assertThat(fixture.kernel.snapshot().observedEndOffset()).isEqualTo(100);
        assertThat(fixture.kernel.snapshot().appliedEndOffset()).isEqualTo(100);
        assertThat(fixture.kernel.snapshot().journalHealth()).isEqualTo(KafkaReplicaJournalHealthV1.INDETERMINATE);
        assertThat(fixture.kernel.snapshot().canAdvanceObserved()).isFalse();
    }

    @Test
    void inaccessibleOrPayloadDependentSourceCannotUseDescriptorQualifiedObservation() {
        Fixture inaccessible = fixture(new KafkaReplicaEligibilityBoundsV1(10, 10_000, 1_000));
        inaccessible.resolver.accessible = false;
        assertThatThrownBy(() -> inaccessible.kernel.observe(KafkaReplicationTestFixtures.descriptor(100, 101, 2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not recoverably qualify");

        Fixture payloadDependent = fixture(new KafkaReplicaEligibilityBoundsV1(10, 10_000, 1_000));
        payloadDependent.resolver.withoutPayload = false;
        assertThatThrownBy(() -> payloadDependent.kernel.observe(KafkaReplicationTestFixtures.descriptor(100, 101, 2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires payload apply");

        assertThat(inaccessible.storage.appendCalls).isZero();
        assertThat(payloadDependent.storage.appendCalls).isZero();
    }

    @Test
    void offsetLagBoundStopsObservedBeforeJournalAppend() {
        Fixture fixture = fixture(new KafkaReplicaEligibilityBoundsV1(0, 10_000, 1_000));

        assertThatThrownBy(() -> fixture.kernel.observe(KafkaReplicationTestFixtures.descriptor(100, 101, 2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lag bound");
        assertThat(fixture.storage.appendCalls).isZero();
    }

    @Test
    void unappliedByteBoundStopsObservedBeforeJournalAppend() {
        Fixture fixture = fixture(new KafkaReplicaEligibilityBoundsV1(10, 499, 1_000));

        assertThatThrownBy(() -> fixture.kernel.observe(KafkaReplicationTestFixtures.descriptor(100, 101, 2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lag bound");
        assertThat(fixture.storage.appendCalls).isZero();
    }

    @Test
    void unappliedAgeReevaluationRemovesAndApplyRestoresEligibility() {
        Fixture fixture = fixture(new KafkaReplicaEligibilityBoundsV1(10, 10_000, 50));
        fixture.clock.set(10);
        fixture.kernel.observe(KafkaReplicationTestFixtures.descriptor(100, 101, 2));
        fixture.clock.set(61);

        KafkaReplicaProgressSnapshotV1 aged = fixture.kernel.snapshot();
        KafkaReplicaProgressSnapshotV1 applied = fixture.kernel.applyNext();

        assertThat(aged.isrEligibility().unappliedAgeNanos()).isEqualTo(51);
        assertThat(aged.isrEligibility().eligible()).isFalse();
        assertThat(applied.appliedEndOffset()).isEqualTo(101);
        assertThat(applied.isrEligibility().eligible()).isTrue();

        KafkaReplicationTestFixtures.FakeJournalStorage futureStorage =
                new KafkaReplicationTestFixtures.FakeJournalStorage();
        KafkaReplicaObservationRecordV1 future = new KafkaReplicaObservationRecordV1(
                0,
                Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]),
                100,
                KafkaReplicationTestFixtures.descriptor(100, 101, 2));
        futureStorage.records.add(KafkaReplicaObservationRecordCodecV1.encode(future));
        KafkaReplicaFollowerKernelV1 recoveredWithClockRollback = new KafkaReplicaFollowerKernelV1(
                1,
                KafkaReplicationTestFixtures.fence(),
                100,
                1,
                new KafkaReplicaObservationJournalV1(futureStorage, new KafkaReplicaJournalBoundsV1(100, 1_000_000)),
                new KafkaReplicationTestFixtures.MutableResolver(),
                new KafkaReplicationTestFixtures.MutableApplyAdapter(),
                new KafkaReplicaEligibilityBoundsV1(10, 10_000, Long.MAX_VALUE),
                () -> 10);
        assertThat(recoveredWithClockRollback.snapshot().isrEligibility().unappliedAgeNanos())
                .isEqualTo(Long.MAX_VALUE);
        assertThat(recoveredWithClockRollback.snapshot().isrEligibility().eligible())
                .isFalse();
        assertThat(recoveredWithClockRollback.snapshot().canAdvanceObserved()).isFalse();
    }

    @Test
    void exactApplyAdvancesAppliedAndUnlocksTheNativeElectionBoundary() {
        Fixture fixture = fixture(new KafkaReplicaEligibilityBoundsV1(10, 10_000, 1_000));
        fixture.kernel.observe(KafkaReplicationTestFixtures.descriptor(100, 102, 2));

        KafkaReplicaElectionValidationV1 before = KafkaReplicaElectionValidatorV1.validate(
                fixture.kernel.snapshot(), KafkaElectionKindV1.ISR_ELECTION, 102);
        KafkaReplicaProgressSnapshotV1 applied = fixture.kernel.applyNext();
        KafkaReplicaElectionValidationV1 after =
                KafkaReplicaElectionValidatorV1.validate(applied, KafkaElectionKindV1.ISR_ELECTION, 102);

        assertThat(before.outcome()).isEqualTo(KafkaReplicaElectionValidationOutcomeV1.APPLIED_SHORTFALL);
        assertThat(before.installableLeo()).isEmpty();
        assertThat(after.outcome()).isEqualTo(KafkaReplicaElectionValidationOutcomeV1.ELIGIBLE);
        assertThat(after.installableLeo()).hasValue(102);
        assertThat(fixture.apply.calls).isEqualTo(1);
    }

    @Test
    void substitutedApplyProofCannotAdvanceApplied() {
        Fixture fixture = fixture(new KafkaReplicaEligibilityBoundsV1(10, 10_000, 1_000));
        KafkaReplicaCommitDescriptorV1 descriptor = KafkaReplicationTestFixtures.descriptor(100, 101, 2);
        fixture.kernel.observe(descriptor);
        fixture.apply.override = new KafkaReplicaApplyProofV1(
                KafkaReplicationTestFixtures.digest(99),
                descriptor.startOffset(),
                descriptor.endOffsetExclusive(),
                descriptor.validatedStateVersion(),
                descriptor.protocolProof());

        assertThatThrownBy(fixture.kernel::applyNext)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("apply proof");
        assertThat(fixture.kernel.snapshot().appliedEndOffset()).isEqualTo(100);
    }

    @Test
    void payloadRequiredModeCollapsesObservedToAppliedAfterRawValidation() {
        Fixture fixture = fixture(new KafkaReplicaEligibilityBoundsV1(0, 0, 0));
        fixture.resolver.withoutPayload = false;
        KafkaReplicaCommitDescriptorV1 descriptor = KafkaReplicationTestFixtures.descriptor(
                100, 101, 2, KafkaReplicaObservationModeV1.PAYLOAD_REQUIRED, 500);

        KafkaReplicaProgressSnapshotV1 progress = fixture.kernel.observe(descriptor);

        assertThat(progress.observedEndOffset()).isEqualTo(101);
        assertThat(progress.appliedEndOffset()).isEqualTo(101);
        assertThat(progress.isrEligibility().eligible()).isTrue();
        assertThat(fixture.storage.appendCalls).isEqualTo(1);
        assertThat(fixture.apply.calls).isEqualTo(1);

        Fixture interrupted = fixture(new KafkaReplicaEligibilityBoundsV1(0, 0, 0));
        interrupted.resolver.withoutPayload = false;
        KafkaReplicaCommitDescriptorV1 interruptedDescriptor = KafkaReplicationTestFixtures.descriptor(
                100, 101, 2, KafkaReplicaObservationModeV1.PAYLOAD_REQUIRED, 500);
        interrupted.apply.override = new KafkaReplicaApplyProofV1(
                KafkaReplicationTestFixtures.digest(99), 100, 101, 2, interruptedDescriptor.protocolProof());
        assertThatThrownBy(() -> interrupted.kernel.observe(interruptedDescriptor))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("apply proof");
        assertThat(interrupted.kernel.snapshot().observedEndOffset()).isEqualTo(100);
        assertThat(interrupted.kernel.snapshot().appliedEndOffset()).isEqualTo(100);
        assertThat(interrupted.kernel.snapshot().canAdvanceObserved()).isFalse();
    }

    @Test
    void exactNewerSourceGenerationCanReplaceBookKeeperForTheUnappliedRange() {
        Fixture fixture = fixture(new KafkaReplicaEligibilityBoundsV1(10, 10_000, 1_000));
        KafkaReplicaCommitDescriptorV1 descriptor = KafkaReplicationTestFixtures.descriptor(100, 101, 2);
        fixture.kernel.observe(descriptor);
        KafkaReplicaSourceReferenceV1 sameGenerationSubstitution = new KafkaReplicaSourceReferenceV1(
                KafkaReplicaSourceKindV1.OBJECT_WAL_GROUP,
                descriptor.source().providerScopeId(),
                new Id128(0, 998),
                descriptor.source().sourceGeneration(),
                1,
                2,
                descriptor.startOffset(),
                descriptor.endOffsetExclusive(),
                KafkaReplicationTestFixtures.digest(97),
                descriptor.aggregateAssignedPayloadSha256());
        fixture.resolver.override = new KafkaReplicaSourceQualificationV1(
                KafkaReplicaCommitDescriptorCodecV1.digest(descriptor),
                sameGenerationSubstitution,
                descriptor.startOffset(),
                descriptor.endOffsetExclusive(),
                descriptor.aggregateAssignedPayloadSha256(),
                descriptor.protocolProof(),
                true,
                true,
                true);
        assertThat(fixture.kernel.snapshot().isrEligibility().recoverableSourceCoversUnapplied())
                .isFalse();

        KafkaReplicaSourceReferenceV1 replacement = new KafkaReplicaSourceReferenceV1(
                KafkaReplicaSourceKindV1.OBJECT_WAL_GROUP,
                descriptor.source().providerScopeId(),
                new Id128(0, 999),
                descriptor.source().sourceGeneration() + 1,
                1,
                2,
                descriptor.startOffset(),
                descriptor.endOffsetExclusive(),
                KafkaReplicationTestFixtures.digest(98),
                descriptor.aggregateAssignedPayloadSha256());
        fixture.resolver.override = new KafkaReplicaSourceQualificationV1(
                KafkaReplicaCommitDescriptorCodecV1.digest(descriptor),
                replacement,
                descriptor.startOffset(),
                descriptor.endOffsetExclusive(),
                descriptor.aggregateAssignedPayloadSha256(),
                descriptor.protocolProof(),
                true,
                true,
                true);

        assertThat(fixture.kernel.snapshot().isrEligibility().recoverableSourceCoversUnapplied())
                .isTrue();
        assertThat(fixture.kernel.snapshot().isrEligibility().eligible()).isTrue();

        fixture.resolver.override = new KafkaReplicaSourceQualificationV1(
                KafkaReplicaCommitDescriptorCodecV1.digest(descriptor),
                replacement,
                descriptor.startOffset(),
                descriptor.endOffsetExclusive(),
                descriptor.aggregateAssignedPayloadSha256(),
                new KafkaReplicaProtocolProofV1(
                        KafkaReplicationTestFixtures.digest(1),
                        descriptor.protocolProof().transactionStateDigest(),
                        descriptor.protocolProof().leaderEpochDigest(),
                        descriptor.protocolProof().checkpointVectorDigest()),
                true,
                true,
                true);
        assertThat(fixture.kernel.snapshot().isrEligibility().recoverableSourceCoversUnapplied())
                .isFalse();
        assertThat(fixture.kernel.snapshot().isrEligibility().eligible()).isFalse();
    }

    @Test
    void corruptRecoveredTailRollsObservedBackAndBlocksFurtherAdvancement() {
        KafkaReplicationTestFixtures.FakeJournalStorage storage = new KafkaReplicationTestFixtures.FakeJournalStorage();
        Sha256Digest predecessor = Sha256Digest.copyOf(new byte[Sha256Digest.LENGTH]);
        for (int index = 0; index < 2; index++) {
            KafkaReplicaObservationRecordV1 record = new KafkaReplicaObservationRecordV1(
                    index,
                    predecessor,
                    index,
                    KafkaReplicationTestFixtures.descriptor(100 + index, 101 + index, 2 + index));
            CanonicalBytes encoded = KafkaReplicaObservationRecordCodecV1.encode(record);
            storage.records.add(encoded);
            predecessor = Sha256Digest.hash(encoded);
        }
        byte[] corrupt = storage.records.get(1).toByteArray();
        corrupt[20] ^= 1;
        storage.records.set(1, CanonicalBytes.copyOf(corrupt));
        KafkaReplicationTestFixtures.MutableResolver resolver = new KafkaReplicationTestFixtures.MutableResolver();
        KafkaReplicaFollowerKernelV1 kernel = new KafkaReplicaFollowerKernelV1(
                1,
                KafkaReplicationTestFixtures.fence(),
                100,
                1,
                new KafkaReplicaObservationJournalV1(storage, new KafkaReplicaJournalBoundsV1(100, 1_000_000)),
                resolver,
                new KafkaReplicationTestFixtures.MutableApplyAdapter(),
                new KafkaReplicaEligibilityBoundsV1(10, 10_000, 1_000),
                () -> 10);

        KafkaReplicaProgressSnapshotV1 recovered = kernel.snapshot();

        assertThat(recovered.observedEndOffset()).isEqualTo(101);
        assertThat(recovered.journalHealth()).isEqualTo(KafkaReplicaJournalHealthV1.CORRUPT);
        assertThat(recovered.canAdvanceObserved()).isFalse();
        assertThatThrownBy(() -> kernel.observe(KafkaReplicationTestFixtures.descriptor(101, 102, 3)))
                .isInstanceOf(IllegalStateException.class);
    }

    private static Fixture fixture(KafkaReplicaEligibilityBoundsV1 eligibilityBounds) {
        KafkaReplicationTestFixtures.FakeJournalStorage storage = new KafkaReplicationTestFixtures.FakeJournalStorage();
        KafkaReplicationTestFixtures.MutableResolver resolver = new KafkaReplicationTestFixtures.MutableResolver();
        KafkaReplicationTestFixtures.MutableApplyAdapter apply = new KafkaReplicationTestFixtures.MutableApplyAdapter();
        AtomicLong clock = new AtomicLong();
        KafkaReplicaFollowerKernelV1 kernel = new KafkaReplicaFollowerKernelV1(
                1,
                KafkaReplicationTestFixtures.fence(),
                100,
                1,
                new KafkaReplicaObservationJournalV1(storage, new KafkaReplicaJournalBoundsV1(100, 1_000_000)),
                resolver,
                apply,
                eligibilityBounds,
                clock::get);
        return new Fixture(storage, resolver, apply, clock, kernel);
    }

    private record Fixture(
            KafkaReplicationTestFixtures.FakeJournalStorage storage,
            KafkaReplicationTestFixtures.MutableResolver resolver,
            KafkaReplicationTestFixtures.MutableApplyAdapter apply,
            AtomicLong clock,
            KafkaReplicaFollowerKernelV1 kernel) {}
}
