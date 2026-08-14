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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.protocol.KafkaPartitionFenceV1;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/** K8 follower kernel; M6 owns transport, durable adapter, native apply, and ISR/HW callbacks. */
public final class KafkaReplicaFollowerKernelV1 {
    private final int replicaId;
    private final KafkaPartitionFenceV1 fence;
    private final KafkaReplicaObservationJournalV1 journal;
    private final KafkaReplicaSourceResolverV1 sourceResolver;
    private final KafkaReplicaApplyAdapterV1 applyAdapter;
    private final KafkaReplicaEligibilityBoundsV1 eligibilityBounds;
    private final LongSupplier nanoTime;
    private final Deque<KafkaReplicaObservationRecordV1> unapplied = new ArrayDeque<>();
    private long observedEndOffset;
    private long appliedEndOffset;
    private long observedStateVersion;
    private long appliedStateVersion;
    private Sha256Digest observedDescriptorDigest;
    private boolean logicalJournalHealthy = true;
    private long lastNow;

    public KafkaReplicaFollowerKernelV1(
            int replicaId,
            KafkaPartitionFenceV1 fence,
            long appliedEndOffset,
            long appliedStateVersion,
            KafkaReplicaObservationJournalV1 journal,
            KafkaReplicaSourceResolverV1 sourceResolver,
            KafkaReplicaApplyAdapterV1 applyAdapter,
            KafkaReplicaEligibilityBoundsV1 eligibilityBounds,
            LongSupplier nanoTime) {
        if (replicaId < 0 || appliedEndOffset < 0 || appliedStateVersion < 0) {
            throw new IllegalArgumentException("replica identity or Applied base is outside its domain");
        }
        this.replicaId = replicaId;
        this.fence = Objects.requireNonNull(fence, "fence");
        this.appliedEndOffset = appliedEndOffset;
        this.appliedStateVersion = appliedStateVersion;
        this.observedEndOffset = appliedEndOffset;
        this.observedStateVersion = appliedStateVersion;
        this.journal = Objects.requireNonNull(journal, "journal");
        this.sourceResolver = Objects.requireNonNull(sourceResolver, "sourceResolver");
        this.applyAdapter = Objects.requireNonNull(applyAdapter, "applyAdapter");
        this.eligibilityBounds = Objects.requireNonNull(eligibilityBounds, "eligibilityBounds");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        loadRecovered(journal.recover(fence, appliedEndOffset));
    }

    public synchronized KafkaReplicaProgressSnapshotV1 observe(KafkaReplicaCommitDescriptorV1 descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        if (!logicalJournalHealthy
                || !descriptor.fence().equals(fence)
                || descriptor.startOffset() != observedEndOffset
                || journal.snapshot().durableThroughOffset() != observedEndOffset
                || descriptor.validatedStateVersion() <= observedStateVersion) {
            throw new IllegalStateException("descriptor cannot advance the current exact journal/progress cut");
        }
        KafkaReplicaSourceQualificationV1 qualification = requireQualification(descriptor);
        long observedAtNanos = now();
        if (descriptor.observationMode() == KafkaReplicaObservationModeV1.DESCRIPTOR_QUALIFIED) {
            if (!qualification.qualifiedWithoutPayload()) {
                throw new IllegalStateException("source requires payload apply before Observed may advance");
            }
            requireProjectedLagWithinBounds(descriptor, observedAtNanos);
        } else if (appliedEndOffset != observedEndOffset || !unapplied.isEmpty()) {
            throw new IllegalStateException("payload-required observation must collapse from an Applied boundary");
        }

        KafkaReplicaObservationRecordV1 record = journal.append(descriptor, observedAtNanos);
        unapplied.addLast(record);
        if (descriptor.observationMode() == KafkaReplicaObservationModeV1.DESCRIPTOR_QUALIFIED) {
            advanceObserved(descriptor);
        } else {
            applyHead(qualification);
        }
        return snapshot();
    }

    public synchronized KafkaReplicaProgressSnapshotV1 applyNext() {
        KafkaReplicaObservationRecordV1 head = unapplied.peekFirst();
        if (head == null) {
            throw new IllegalStateException("replica has no unapplied descriptor");
        }
        applyHead(requireQualification(head.descriptor()));
        return snapshot();
    }

    public synchronized KafkaReplicaProgressSnapshotV1 snapshot() {
        long now = now();
        long lagOffsets = Math.subtractExact(observedEndOffset, appliedEndOffset);
        long lagBytes = 0;
        long oldestObservedAt = Long.MAX_VALUE;
        boolean hasObservedBacklog = false;
        boolean clockValid = true;
        boolean sourceCovers = true;
        for (KafkaReplicaObservationRecordV1 record : unapplied) {
            if (record.descriptor().endOffsetExclusive() > observedEndOffset) {
                break;
            }
            lagBytes = Math.addExact(lagBytes, record.descriptor().encodedDataBytes());
            oldestObservedAt = Math.min(oldestObservedAt, record.observedAtNanos());
            hasObservedBacklog = true;
            clockValid &= record.observedAtNanos() <= now;
            sourceCovers &= qualifiesSafely(record.descriptor());
        }
        long lagAge = !clockValid ? Long.MAX_VALUE : hasObservedBacklog ? Math.subtractExact(now, oldestObservedAt) : 0;
        KafkaReplicaObservationJournalSnapshotV1 journalSnapshot = journal.snapshot();
        KafkaReplicaJournalHealthV1 effectiveHealth =
                logicalJournalHealthy ? journalSnapshot.health() : KafkaReplicaJournalHealthV1.CORRUPT;
        KafkaReplicaIsrEligibilityV1 eligibility = new KafkaReplicaIsrEligibilityV1(
                journalSnapshot.durableThroughOffset() >= observedEndOffset,
                lagOffsets <= eligibilityBounds.maximumApplyLagOffsets(),
                lagBytes <= eligibilityBounds.maximumUnappliedBytes(),
                clockValid && lagAge <= eligibilityBounds.maximumUnappliedNanos(),
                sourceCovers,
                lagOffsets,
                lagBytes,
                lagAge);
        return new KafkaReplicaProgressSnapshotV1(
                replicaId,
                fence,
                observedEndOffset,
                appliedEndOffset,
                observedStateVersion,
                appliedStateVersion,
                Optional.ofNullable(observedDescriptorDigest),
                effectiveHealth,
                logicalJournalHealthy
                        && journalSnapshot.acceptsAppend()
                        && journalSnapshot.durableThroughOffset() == observedEndOffset
                        && clockValid,
                eligibility);
    }

    private void loadRecovered(KafkaReplicaObservationJournalSnapshotV1 recovered) {
        long lastStateVersion = appliedStateVersion;
        boolean unreportedPayloadRecord = false;
        for (KafkaReplicaObservationRecordV1 record : recovered.records()) {
            KafkaReplicaCommitDescriptorV1 descriptor = record.descriptor();
            if (descriptor.endOffsetExclusive() <= appliedEndOffset) {
                if (descriptor.validatedStateVersion() > appliedStateVersion) {
                    logicalJournalHealthy = false;
                    break;
                }
                lastStateVersion = Math.max(lastStateVersion, descriptor.validatedStateVersion());
                continue;
            }
            if (descriptor.startOffset() != observedEndOffset
                    || descriptor.validatedStateVersion() <= lastStateVersion
                    || unreportedPayloadRecord) {
                logicalJournalHealthy = false;
                break;
            }
            unapplied.addLast(record);
            lastStateVersion = descriptor.validatedStateVersion();
            if (descriptor.observationMode() == KafkaReplicaObservationModeV1.DESCRIPTOR_QUALIFIED) {
                advanceObserved(descriptor);
            } else {
                unreportedPayloadRecord = true;
            }
        }
    }

    private void applyHead(KafkaReplicaSourceQualificationV1 qualification) {
        KafkaReplicaObservationRecordV1 head = unapplied.peekFirst();
        KafkaReplicaCommitDescriptorV1 descriptor = head.descriptor();
        if (descriptor.startOffset() != appliedEndOffset) {
            throw new IllegalStateException("replica apply order has a logical offset gap");
        }
        KafkaReplicaApplyProofV1 proof =
                Objects.requireNonNull(applyAdapter.apply(descriptor, qualification), "null replica apply proof");
        Sha256Digest descriptorDigest = KafkaReplicaCommitDescriptorCodecV1.digest(descriptor);
        if (!proof.descriptorDigest().equals(descriptorDigest)
                || proof.startOffset() != descriptor.startOffset()
                || proof.endOffsetExclusive() != descriptor.endOffsetExclusive()
                || proof.appliedStateVersion() != descriptor.validatedStateVersion()
                || !proof.protocolProof().equals(descriptor.protocolProof())) {
            throw new IllegalStateException("replica apply proof differs from the exact descriptor/state");
        }
        unapplied.removeFirst();
        appliedEndOffset = descriptor.endOffsetExclusive();
        appliedStateVersion = descriptor.validatedStateVersion();
        if (descriptor.observationMode() == KafkaReplicaObservationModeV1.PAYLOAD_REQUIRED) {
            advanceObserved(descriptor);
        }
    }

    private void advanceObserved(KafkaReplicaCommitDescriptorV1 descriptor) {
        observedEndOffset = descriptor.endOffsetExclusive();
        observedStateVersion = descriptor.validatedStateVersion();
        observedDescriptorDigest = KafkaReplicaCommitDescriptorCodecV1.digest(descriptor);
    }

    private KafkaReplicaSourceQualificationV1 requireQualification(KafkaReplicaCommitDescriptorV1 descriptor) {
        KafkaReplicaSourceQualificationV1 qualification =
                Objects.requireNonNull(sourceResolver.qualify(descriptor), "null source qualification");
        if (!qualification.qualifies(descriptor)) {
            throw new IllegalStateException("source map does not recoverably qualify the descriptor range/state");
        }
        return qualification;
    }

    private boolean qualifiesSafely(KafkaReplicaCommitDescriptorV1 descriptor) {
        try {
            KafkaReplicaSourceQualificationV1 qualification = sourceResolver.qualify(descriptor);
            return qualification != null && qualification.qualifies(descriptor);
        } catch (RuntimeException unavailable) {
            return false;
        }
    }

    private void requireProjectedLagWithinBounds(KafkaReplicaCommitDescriptorV1 descriptor, long now) {
        long projectedOffsets = Math.subtractExact(descriptor.endOffsetExclusive(), appliedEndOffset);
        long projectedBytes = descriptor.encodedDataBytes();
        long oldest = now;
        boolean clockValid = true;
        for (KafkaReplicaObservationRecordV1 record : unapplied) {
            if (record.descriptor().endOffsetExclusive() <= observedEndOffset) {
                projectedBytes =
                        Math.addExact(projectedBytes, record.descriptor().encodedDataBytes());
                oldest = Math.min(oldest, record.observedAtNanos());
                clockValid &= record.observedAtNanos() <= now;
                if (!qualifiesSafely(record.descriptor())) {
                    throw new IllegalStateException("an existing Observed range lost its recoverable source");
                }
            }
        }
        long projectedAge = clockValid ? Math.subtractExact(now, oldest) : Long.MAX_VALUE;
        if (projectedOffsets > eligibilityBounds.maximumApplyLagOffsets()
                || projectedBytes > eligibilityBounds.maximumUnappliedBytes()
                || !clockValid
                || projectedAge > eligibilityBounds.maximumUnappliedNanos()) {
            throw new IllegalStateException("descriptor would exceed the hard Observed/Applied lag bound");
        }
    }

    private long now() {
        lastNow = Math.max(lastNow, nanoTime.getAsLong());
        return lastNow;
    }
}
