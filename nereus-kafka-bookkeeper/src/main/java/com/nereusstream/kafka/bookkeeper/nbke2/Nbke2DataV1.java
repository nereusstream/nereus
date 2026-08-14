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

package com.nereusstream.kafka.bookkeeper.nbke2;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.identity.Id128;
import java.util.Objects;
import java.util.Optional;

/** One complete raw broker-assigned Kafka RecordBatch carried by one BookKeeper entry. */
public record Nbke2DataV1(
        Nbke2RunBindingV1 runBinding,
        long baseOffset,
        int lastOffsetDelta,
        int memberOrdinal,
        int memberCount,
        Id128 appendGroupId,
        Id128 storageAttemptId,
        Optional<Nbke2AppendGroupDescriptorV1> terminalDescriptor,
        CanonicalBytes rawAssignedRecordBatch)
        implements Nbke2FrameV1 {
    public Nbke2DataV1 {
        Objects.requireNonNull(runBinding, "runBinding");
        Objects.requireNonNull(appendGroupId, "appendGroupId");
        Objects.requireNonNull(storageAttemptId, "storageAttemptId");
        Objects.requireNonNull(terminalDescriptor, "terminalDescriptor");
        Objects.requireNonNull(rawAssignedRecordBatch, "rawAssignedRecordBatch");
        if (baseOffset < 0
                || lastOffsetDelta < 0
                || memberCount <= 0
                || memberOrdinal < 0
                || memberOrdinal >= memberCount) {
            throw new IllegalArgumentException("DATA coverage/member fields are outside the NBKE2 v1 domain");
        }
        try {
            Math.addExact(baseOffset, Math.addExact((long) lastOffsetDelta, 1L));
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("DATA offset end overflows", failure);
        }
        if (appendGroupId.isZero() || storageAttemptId.isZero()) {
            throw new IllegalArgumentException("append-group and storage-attempt IDs must be non-zero");
        }
        boolean terminalMember = memberOrdinal == memberCount - 1;
        if (terminalMember != terminalDescriptor.isPresent()) {
            throw new IllegalArgumentException("exactly the last DATA member carries the terminal descriptor");
        }
        if (rawAssignedRecordBatch.isEmpty()
                || rawAssignedRecordBatch.length() > Nbke2ConstantsV1.FORMAT_MAX_DATA_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("raw assigned RecordBatch length is outside the NBKE2 v1 domain");
        }
        terminalDescriptor.ifPresent(descriptor -> {
            long dataEnd = Math.addExact(baseOffset, (long) lastOffsetDelta + 1L);
            if (descriptor.groupStartOffset() > baseOffset || descriptor.groupEndOffsetExclusive() != dataEnd) {
                throw new IllegalArgumentException("terminal DATA is outside the append-group logical bounds");
            }
            long physicalMembers =
                    Math.addExact(Math.subtractExact(descriptor.lastDataEntryId(), descriptor.firstDataEntryId()), 1L);
            if (physicalMembers != memberCount) {
                throw new IllegalArgumentException("append-group DATA entry bounds do not match member count");
            }
            if (memberCount == 1
                    && !descriptor
                            .aggregateAssignedPayloadSha256()
                            .equals(com.nereusstream.domain.bytes.Sha256Digest.hash(rawAssignedRecordBatch))) {
                throw new IllegalArgumentException("single-member append-group payload digest mismatch");
            }
        });
    }

    public long endOffsetExclusive() {
        return Math.addExact(baseOffset, (long) lastOffsetDelta + 1L);
    }

    @Override
    public Nbke2FrameTypeV1 frameType() {
        return Nbke2FrameTypeV1.DATA;
    }
}
