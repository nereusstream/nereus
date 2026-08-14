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

package com.nereusstream.kafka.bookkeeper.read;

import com.nereusstream.kafka.bookkeeper.commit.KafkaActiveTailStateV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBookKeeperActiveTailLocatorV1;
import com.nereusstream.kafka.bookkeeper.commit.KafkaBookKeeperDataLocatorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2BatchLocatorV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2ConstantsV1;
import com.nereusstream.kafka.bookkeeper.nbke2.Nbke2RangeIndexBlockV1;
import java.util.Objects;
import java.util.Optional;

/** Primitive-array locator index; no long-lived object or TreeMap exists per RecordBatch. */
public final class KafkaPackedBatchLocatorIndexV1 {
    private final KafkaLocatorSourceKindV1 sourceKind;
    private final long indexIdentity;
    private final long[] startOffsets;
    private final long[] endOffsets;
    private final long[] entryIds;
    private final long[] appendGroupOrdinals;
    private final long[] rawPayloadBytes;
    private final long[] checksumGenerations;

    private KafkaPackedBatchLocatorIndexV1(
            KafkaLocatorSourceKindV1 sourceKind,
            long indexIdentity,
            long[] startOffsets,
            long[] endOffsets,
            long[] entryIds,
            long[] appendGroupOrdinals,
            long[] rawPayloadBytes,
            long[] checksumGenerations) {
        this.sourceKind = sourceKind;
        this.indexIdentity = indexIdentity;
        this.startOffsets = startOffsets;
        this.endOffsets = endOffsets;
        this.entryIds = entryIds;
        this.appendGroupOrdinals = appendGroupOrdinals;
        this.rawPayloadBytes = rawPayloadBytes;
        this.checksumGenerations = checksumGenerations;
        validate();
    }

    public static KafkaPackedBatchLocatorIndexV1 fromActiveTail(KafkaActiveTailStateV1 activeTail) {
        Objects.requireNonNull(activeTail, "activeTail");
        int count = 0;
        for (KafkaBookKeeperActiveTailLocatorV1 group : activeTail.locators()) {
            count = Math.addExact(count, group.members().size());
        }
        if (count <= 0 || count > Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT) {
            throw new IllegalArgumentException("active-tail locator count is outside the packed v1 bound");
        }
        long[] starts = new long[count];
        long[] ends = new long[count];
        long[] entries = new long[count];
        long[] groups = new long[count];
        long[] payloads = new long[count];
        long[] checksums = new long[count];
        int ordinal = 0;
        for (int groupOrdinal = 0; groupOrdinal < activeTail.locators().size(); groupOrdinal++) {
            KafkaBookKeeperActiveTailLocatorV1 group = activeTail.locators().get(groupOrdinal);
            for (KafkaBookKeeperDataLocatorV1 member : group.members()) {
                starts[ordinal] = member.startOffset();
                ends[ordinal] = member.endOffsetExclusive();
                entries[ordinal] = member.entryId();
                groups[ordinal] = groupOrdinal;
                payloads[ordinal] = member.rawAssignedRecordBatchBytes();
                checksums[ordinal] = 0;
                ordinal++;
            }
        }
        return new KafkaPackedBatchLocatorIndexV1(
                KafkaLocatorSourceKindV1.ACTIVE_TAIL, -1, starts, ends, entries, groups, payloads, checksums);
    }

    public static KafkaPackedBatchLocatorIndexV1 fromRangeIndexBlock(
            long indexBlockEntryId, Nbke2RangeIndexBlockV1 block) {
        Objects.requireNonNull(block, "block");
        if (indexBlockEntryId <= block.lastDataEntryId() || indexBlockEntryId >= block.successorDataEntryId()) {
            throw new IllegalArgumentException("range-index block entry identity is outside its physical cut");
        }
        int count = block.locators().size();
        long[] starts = new long[count];
        long[] ends = new long[count];
        long[] entries = new long[count];
        long[] groups = new long[count];
        long[] payloads = new long[count];
        long[] checksums = new long[count];
        for (int index = 0; index < count; index++) {
            Nbke2BatchLocatorV1 locator = block.locators().get(index);
            starts[index] = Math.addExact(block.anchorOffset(), locator.baseOffsetDelta());
            ends[index] = Math.addExact(starts[index], locator.logicalOffsetCount());
            entries[index] = Math.addExact(block.anchorEntryId(), locator.entryIdDelta());
            groups[index] = locator.appendGroupDelta();
            payloads[index] = locator.payloadLength();
            checksums[index] = locator.physicalChecksumGeneration();
        }
        return new KafkaPackedBatchLocatorIndexV1(
                KafkaLocatorSourceKindV1.RANGE_INDEX_BLOCK,
                indexBlockEntryId,
                starts,
                ends,
                entries,
                groups,
                payloads,
                checksums);
    }

    public int size() {
        return startOffsets.length;
    }

    public long startOffset() {
        return startOffsets[0];
    }

    public long coveredThroughOffset() {
        return endOffsets[endOffsets.length - 1];
    }

    public Optional<KafkaPackedBatchLocatorV1> floorOrSuccessor(long requestedOffset) {
        if (requestedOffset < 0) {
            throw new IllegalArgumentException("requested offset must be non-negative");
        }
        int low = 0;
        int high = startOffsets.length - 1;
        int floor = -1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            if (startOffsets[middle] <= requestedOffset) {
                floor = middle;
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        if (floor >= 0 && requestedOffset < endOffsets[floor]) {
            return Optional.of(locator(floor));
        }
        int successor = floor + 1;
        return successor < startOffsets.length ? Optional.of(locator(successor)) : Optional.empty();
    }

    public Optional<KafkaPackedBatchLocatorV1> successor(int ordinal) {
        if (ordinal < 0 || ordinal >= startOffsets.length) {
            throw new IllegalArgumentException("locator ordinal is outside the packed index");
        }
        return ordinal + 1 < startOffsets.length ? Optional.of(locator(ordinal + 1)) : Optional.empty();
    }

    public KafkaPackedBatchLocatorV1 at(int ordinal) {
        if (ordinal < 0 || ordinal >= startOffsets.length) {
            throw new IllegalArgumentException("locator ordinal is outside the packed index");
        }
        return locator(ordinal);
    }

    private KafkaPackedBatchLocatorV1 locator(int ordinal) {
        return new KafkaPackedBatchLocatorV1(
                sourceKind,
                indexIdentity,
                ordinal,
                startOffsets[ordinal],
                endOffsets[ordinal],
                entryIds[ordinal],
                appendGroupOrdinals[ordinal],
                rawPayloadBytes[ordinal],
                checksumGenerations[ordinal]);
    }

    private void validate() {
        int count = startOffsets.length;
        if (sourceKind == null
                || count <= 0
                || count > Nbke2ConstantsV1.FORMAT_MAX_LOCATOR_COUNT
                || endOffsets.length != count
                || entryIds.length != count
                || appendGroupOrdinals.length != count
                || rawPayloadBytes.length != count
                || checksumGenerations.length != count) {
            throw new IllegalArgumentException("packed locator arrays differ or exceed the v1 cap");
        }
        if ((sourceKind == KafkaLocatorSourceKindV1.ACTIVE_TAIL) != (indexIdentity == -1)) {
            throw new IllegalArgumentException("packed locator source and index identity differ");
        }
        long previousStart = -1;
        long previousEntry = 0;
        long previousGroup = -1;
        for (int index = 0; index < count; index++) {
            if (startOffsets[index] <= previousStart
                    || endOffsets[index] <= startOffsets[index]
                    || entryIds[index] <= previousEntry
                    || appendGroupOrdinals[index] < previousGroup
                    || rawPayloadBytes[index] <= 0
                    || checksumGenerations[index] < 0) {
                throw new IllegalArgumentException("packed locator rows regress or leave their domains");
            }
            previousStart = startOffsets[index];
            previousEntry = entryIds[index];
            previousGroup = appendGroupOrdinals[index];
        }
    }
}
