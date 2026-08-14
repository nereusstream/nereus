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

import java.util.Optional;

/** Reserves DATA and control IDs from one gap-free run sequence and excludes controls inside a DATA group. */
public final class KafkaBookKeeperEntrySequencerV1 {
    private long nextEntryId;
    private KafkaBookKeeperEntryReservationV1 openDataGroup;

    public KafkaBookKeeperEntrySequencerV1(long firstDataEntryId) {
        if (firstDataEntryId <= 0) {
            throw new IllegalArgumentException("the first DATA entry must follow RUN_HEADER entry zero");
        }
        this.nextEntryId = firstDataEntryId;
    }

    public synchronized KafkaBookKeeperEntryReservationV1 reserveDataGroup(int memberCount) {
        if (openDataGroup != null) {
            throw new IllegalStateException("a DATA group reservation is already open");
        }
        if (memberCount <= 0) {
            throw new IllegalArgumentException("a DATA group must have at least one member");
        }
        long lastEntryId;
        long successor;
        try {
            lastEntryId = Math.addExact(nextEntryId, (long) memberCount - 1L);
            successor = Math.addExact(lastEntryId, 1L);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("DATA group entry range overflows", failure);
        }
        openDataGroup = new KafkaBookKeeperEntryReservationV1(
                KafkaBookKeeperEntryKindV1.DATA_GROUP, nextEntryId, lastEntryId, memberCount);
        nextEntryId = successor;
        return openDataGroup;
    }

    public synchronized void completeDataGroup(KafkaBookKeeperEntryReservationV1 reservation) {
        if (openDataGroup == null || openDataGroup != reservation) {
            throw new IllegalArgumentException("completion does not name the exact open DATA reservation");
        }
        openDataGroup = null;
    }

    public synchronized KafkaBookKeeperEntryReservationV1 reserveControl() {
        if (openDataGroup != null) {
            throw new IllegalStateException("a control entry cannot split an open DATA group");
        }
        long entryId = nextEntryId;
        try {
            nextEntryId = Math.addExact(nextEntryId, 1L);
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("control entry range overflows", failure);
        }
        return new KafkaBookKeeperEntryReservationV1(KafkaBookKeeperEntryKindV1.CONTROL, entryId, entryId, 1);
    }

    public synchronized long nextEntryId() {
        return nextEntryId;
    }

    public synchronized Optional<KafkaBookKeeperEntryReservationV1> openDataGroup() {
        return Optional.ofNullable(openDataGroup);
    }
}
