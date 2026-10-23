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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionAdmissionV1.Reservation;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperReaderV2.DecodingBounds;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Immutable decoding/resource envelope from the actual selected Task's retained reservation. This is data only:
 * a cold Owner must separately validate selection and acquire its own persisted Cell reservation before I/O.
 */
public record KafkaBookKeeperSelectedCacheBudgetV1(
        Sha256Digest descriptorSha256, DecodingBounds decodingBounds, Reservation taskReservation) {
    private static final int MAGIC = 0x4b424342; // KBCB
    public static final int ENCODED_BYTES = 4 + 2 + 2 * Sha256Digest.LENGTH + 4 + 16 * Long.BYTES;

    public KafkaBookKeeperSelectedCacheBudgetV1 {
        KafkaBookKeeperInventoryV2.requireDigest(descriptorSha256);
        Objects.requireNonNull(decodingBounds, "decodingBounds");
        Objects.requireNonNull(taskReservation, "taskReservation");
        if (decodingBounds.records() > taskReservation.records()
                || decodingBounds.bytes() > taskReservation.outputBytes()) {
            throw new IllegalArgumentException("selected cache decoding exceeds its retained Task reservation");
        }
    }

    public String key() {
        return KafkaBookKeeperInventoryV2.taskKey(taskReservation.compactionTaskIdSha256()) + "/cache-budget";
    }

    public void requireTask(KafkaBookKeeperInventoryV2.Task task) {
        long encoded = task.parts().stream()
                .mapToLong(KafkaBookKeeperInventoryV2.PartPlan::length)
                .reduce(0, Math::addExact);
        if (!task.taskIdSha256().equals(taskReservation.compactionTaskIdSha256())
                || encoded > Math.addExact(taskReservation.outputBytes(), taskReservation.indexBytes())) {
            throw new IllegalArgumentException("selected cache budget differs from its immutable Task or output bound");
        }
    }

    public void requireDescriptor(KafkaSealedBookKeeperDescriptorV2 descriptor) {
        requireTask(descriptor.task());
        long indexes = descriptor.indexes().stream()
                .mapToLong(value -> value.artifactLength())
                .sum();
        long recovery = descriptor
                .recoveryStateLocator()
                .orElseThrow(
                        () -> new IllegalArgumentException("selected cache budget requires independent recovery state"))
                .artifactLength();
        if (!descriptorSha256.equals(descriptor.descriptorSha256())
                || descriptor.batchCount() > taskReservation.inputBatches()
                || Math.addExact(indexes, recovery) > taskReservation.indexBytes()) {
            throw new IllegalArgumentException(
                    "selected cache budget differs from its descriptor or index/state bound");
        }
    }

    /** A new Owner's cache identity cannot reuse the old Task identity, which its drain may have fenced. */
    public Reservation forOwnerCache(Sha256Digest reservationId) {
        var value = taskReservation;
        if (value.compactionTaskIdSha256().equals(reservationId)) {
            throw new IllegalArgumentException("selected cache must not reuse its old Task reservation identity");
        }
        return new Reservation(
                reservationId,
                value.dirtyBytes(),
                value.inputBatches(),
                value.records(),
                value.distinctKeys(),
                value.keyBytes(),
                value.outputBytes(),
                value.indexBytes(),
                value.transactions(),
                value.tombstones(),
                value.backlogAgeMs(),
                value.spillBytes(),
                value.providerOperations(),
                value.kmsOperations(),
                value.metadataOperations(),
                value.responseUnknowns());
    }

    public CanonicalBytes encode() {
        try {
            var bytes = new ByteArrayOutputStream(ENCODED_BYTES);
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(MAGIC);
                out.writeShort(1);
                out.write(descriptorSha256.bytes().toByteArray());
                out.write(taskReservation.compactionTaskIdSha256().bytes().toByteArray());
                out.writeInt(decodingBounds.records());
                out.writeLong(decodingBounds.bytes());
                for (long value : List.of(
                        taskReservation.dirtyBytes(),
                        taskReservation.inputBatches(),
                        taskReservation.records(),
                        taskReservation.distinctKeys(),
                        taskReservation.keyBytes(),
                        taskReservation.outputBytes(),
                        taskReservation.indexBytes(),
                        taskReservation.transactions(),
                        taskReservation.tombstones(),
                        taskReservation.backlogAgeMs(),
                        taskReservation.spillBytes(),
                        taskReservation.providerOperations(),
                        taskReservation.kmsOperations(),
                        taskReservation.metadataOperations(),
                        taskReservation.responseUnknowns())) {
                    out.writeLong(value);
                }
            }
            return CanonicalBytes.copyOf(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory selected cache budget encoding failed", impossible);
        }
    }

    public static KafkaBookKeeperSelectedCacheBudgetV1 decode(CanonicalBytes bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length() != ENCODED_BYTES) {
            throw new IllegalArgumentException("selected cache budget length differs");
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            if (in.readInt() != MAGIC || in.readUnsignedShort() != 1) {
                throw new IllegalArgumentException("selected cache budget has a foreign magic or version");
            }
            var descriptor = Sha256Digest.copyOf(in.readNBytes(Sha256Digest.LENGTH));
            var task = Sha256Digest.copyOf(in.readNBytes(Sha256Digest.LENGTH));
            var bounds = new DecodingBounds(in.readInt(), in.readLong());
            var reservation = new Reservation(
                    task,
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong());
            var result = new KafkaBookKeeperSelectedCacheBudgetV1(descriptor, bounds, reservation);
            if (in.available() != 0 || !result.encode().equals(bytes)) {
                throw new IllegalArgumentException("selected cache budget is noncanonical");
            }
            return result;
        } catch (IOException failure) {
            throw new IllegalArgumentException("selected cache budget is truncated", failure);
        }
    }
}
