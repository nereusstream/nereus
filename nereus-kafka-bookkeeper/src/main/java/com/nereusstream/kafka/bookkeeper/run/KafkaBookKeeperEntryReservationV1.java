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

package com.nereusstream.kafka.bookkeeper.run;

import java.util.Objects;

/** One contiguous physical entry range reserved atomically by the K3 sequencer. */
public record KafkaBookKeeperEntryReservationV1(
        KafkaBookKeeperEntryKindV1 kind, long firstEntryId, long lastEntryId, int entryCount) {
    public KafkaBookKeeperEntryReservationV1 {
        Objects.requireNonNull(kind, "kind");
        if (firstEntryId <= 0 || lastEntryId < firstEntryId || entryCount <= 0) {
            throw new IllegalArgumentException("entry reservation is outside the K3 domain");
        }
        long exactCount;
        try {
            exactCount = Math.addExact(Math.subtractExact(lastEntryId, firstEntryId), 1L);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("entry reservation range overflows", failure);
        }
        if (exactCount != entryCount || kind == KafkaBookKeeperEntryKindV1.CONTROL && entryCount != 1) {
            throw new IllegalArgumentException("entry reservation count or kind is inconsistent");
        }
    }

    public long entryId(int ordinal) {
        if (ordinal < 0 || ordinal >= entryCount) {
            throw new IndexOutOfBoundsException("entry ordinal is outside the reservation");
        }
        return Math.addExact(firstEntryId, ordinal);
    }
}
