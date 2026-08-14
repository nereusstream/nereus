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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

class KafkaBookKeeperEntrySequencerV1Test {
    @Test
    void reservesContiguousDataMembersFromTheFirstPostHeaderEntry() {
        KafkaBookKeeperEntrySequencerV1 sequencer = new KafkaBookKeeperEntrySequencerV1(1);

        KafkaBookKeeperEntryReservationV1 reservation = sequencer.reserveDataGroup(3);

        assertThat(reservation.firstEntryId()).isEqualTo(1);
        assertThat(reservation.lastEntryId()).isEqualTo(3);
        assertThat(reservation.entryId(2)).isEqualTo(3);
        assertThat(sequencer.nextEntryId()).isEqualTo(4);
    }

    @Test
    void excludesAControlEntryFromAnOpenDataGroup() {
        KafkaBookKeeperEntrySequencerV1 sequencer = new KafkaBookKeeperEntrySequencerV1(1);
        KafkaBookKeeperEntryReservationV1 data = sequencer.reserveDataGroup(2);

        assertThatThrownBy(sequencer::reserveControl)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot split");

        sequencer.completeDataGroup(data);
        assertThat(sequencer.reserveControl().firstEntryId()).isEqualTo(3);
    }

    @Test
    void excludesASecondDataReservationUntilExactCompletion() {
        KafkaBookKeeperEntrySequencerV1 sequencer = new KafkaBookKeeperEntrySequencerV1(1);
        KafkaBookKeeperEntryReservationV1 first = sequencer.reserveDataGroup(1);

        assertThatThrownBy(() -> sequencer.reserveDataGroup(1)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> sequencer.completeDataGroup(
                        new KafkaBookKeeperEntryReservationV1(KafkaBookKeeperEntryKindV1.DATA_GROUP, 1, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class);

        sequencer.completeDataGroup(first);
        assertThat(sequencer.reserveDataGroup(1).firstEntryId()).isEqualTo(2);
    }

    @Test
    void ordersDataAndControlReservationsInOneGapFreeSequence() {
        KafkaBookKeeperEntrySequencerV1 sequencer = new KafkaBookKeeperEntrySequencerV1(1);
        KafkaBookKeeperEntryReservationV1 first = sequencer.reserveDataGroup(2);
        sequencer.completeDataGroup(first);
        KafkaBookKeeperEntryReservationV1 checkpoint = sequencer.reserveControl();
        KafkaBookKeeperEntryReservationV1 second = sequencer.reserveDataGroup(3);

        assertThat(first.lastEntryId()).isEqualTo(2);
        assertThat(checkpoint.firstEntryId()).isEqualTo(3);
        assertThat(second.firstEntryId()).isEqualTo(4);
        assertThat(second.lastEntryId()).isEqualTo(6);
    }

    @Test
    void validatesReservationKindCountAndOrdinal() {
        assertThatThrownBy(() -> new KafkaBookKeeperEntryReservationV1(KafkaBookKeeperEntryKindV1.CONTROL, 1, 2, 2))
                .isInstanceOf(IllegalArgumentException.class);
        KafkaBookKeeperEntryReservationV1 reservation =
                new KafkaBookKeeperEntryReservationV1(KafkaBookKeeperEntryKindV1.DATA_GROUP, 4, 5, 2);
        assertThatThrownBy(() -> reservation.entryId(2)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void rejectsInvalidStartsCountsAndOverflow() {
        assertThatThrownBy(() -> new KafkaBookKeeperEntrySequencerV1(0)).isInstanceOf(IllegalArgumentException.class);
        KafkaBookKeeperEntrySequencerV1 sequencer = new KafkaBookKeeperEntrySequencerV1(Long.MAX_VALUE);
        assertThatThrownBy(() -> sequencer.reserveDataGroup(2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflows");
    }
}
