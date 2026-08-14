/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */

package com.nereusstream.kafka.bookkeeper.protocol;

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.identity.Id128;
import com.nereusstream.domain.identity.KafkaTopicId;
import com.nereusstream.domain.identity.StorageEpochId;
import com.nereusstream.domain.identity.TopicBindingId;
import com.nereusstream.domain.protocol.KafkaTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.KafkaTopicName;
import java.util.Arrays;

final class KafkaProtocolStateFixtures {
    private KafkaProtocolStateFixtures() {}

    static KafkaPartitionFenceV1 fence(long bindingGeneration, int storageSeed, long ownerEpoch, int leaderEpoch) {
        return new KafkaPartitionFenceV1(
                new TopicBindingId(digest(1)),
                new KafkaTopicIncarnationIdentity(new KafkaTopicId(new Id128(2, 3)), new KafkaTopicName("orders")),
                4,
                bindingGeneration,
                new StorageEpochId(digest(storageSeed)),
                ownerEpoch,
                leaderEpoch);
    }

    static KafkaPartitionStateReferencesV1 references(long generation) {
        return new KafkaPartitionStateReferencesV1(
                reference(generation, 11),
                reference(generation, 12),
                reference(generation, 13),
                reference(generation, 14),
                reference(generation, 15),
                reference(generation, 16),
                reference(generation, 17),
                reference(generation, 18),
                reference(generation, 19));
    }

    static KafkaPartitionProtocolStateV1 initialState() {
        return new KafkaPartitionProtocolStateV1(
                fence(1, 2, 3, 4), 0, new KafkaPartitionFrontiersV1(0, 0, 0, 0, 0, 0), references(0));
    }

    static KafkaPartitionCommitSlotV1 commitSlot(
            KafkaPartitionProtocolStateV1 predecessor, long startOffset, long endOffset, long referenceGeneration) {
        long allocated = Math.max(predecessor.frontiers().allocatedEndOffset(), endOffset);
        long durable = Math.max(predecessor.frontiers().durableEndOffset(), endOffset);
        return new KafkaPartitionCommitSlotV1(
                predecessor.fence(),
                predecessor.stateVersion(),
                startOffset,
                endOffset,
                new KafkaPartitionFrontiersV1(
                        predecessor.frontiers().trimStartOffset(),
                        allocated,
                        durable,
                        endOffset,
                        predecessor.frontiers().highWatermark(),
                        predecessor.frontiers().lastStableOffset()),
                references(referenceGeneration));
    }

    static KafkaPartitionStateReferenceV1 reference(long generation, int seed) {
        return new KafkaPartitionStateReferenceV1(generation, digest(seed));
    }

    static Sha256Digest digest(int seed) {
        byte[] bytes = new byte[Sha256Digest.LENGTH];
        Arrays.fill(bytes, (byte) seed);
        return Sha256Digest.copyOf(bytes);
    }
}
