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

package com.nereusstream.managedledger.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import java.util.Map;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.junit.jupiter.api.Test;

class PositionProjectionTest {
    private static final String NAME = "tenant/ns/persistent/topic";
    private static final long LEDGER_ID = VirtualLedgerProjection.MIN_VIRTUAL_LEDGER_ID;
    private final PositionProjection positions = new PositionProjection();
    private final VirtualLedgerProjection projection = projection(1, LEDGER_ID);

    @Test
    void producesStockCompatibleEmptyAndTrimmedBounds() {
        StreamPositionBounds empty = positions.bounds(projection, snapshot(0, 0));

        assertPosition(empty.beforeFirstAvailable(), -1);
        assertPosition(empty.firstAvailable(), 0);
        assertPosition(empty.lastConfirmed(), -1);
        assertPosition(empty.onePastLast(), 0);

        StreamPositionBounds retained = positions.bounds(projection, snapshot(2, 5));
        assertPosition(retained.beforeFirstAvailable(), 1);
        assertPosition(retained.firstAvailable(), 2);
        assertPosition(retained.lastConfirmed(), 4);
        assertPosition(retained.onePastLast(), 5);

        StreamPositionBounds fullyTrimmed = positions.bounds(projection, snapshot(5, 5));
        assertPosition(fullyTrimmed.beforeFirstAvailable(), 4);
        assertPosition(fullyTrimmed.firstAvailable(), 5);
        assertPosition(fullyTrimmed.lastConfirmed(), 4);
        assertPosition(fullyTrimmed.onePastLast(), 5);
    }

    @Test
    void enforcesDifferentReadableReadAndMarkDeleteRanges() {
        StreamMetadata snapshot = snapshot(2, 5);

        assertThat(positions.requireReadableEntryOffset(projection, position(2), snapshot)).isEqualTo(2);
        assertThat(positions.requireReadableEntryOffset(projection, position(4), snapshot)).isEqualTo(4);
        assertThatThrownBy(() -> positions.requireReadableEntryOffset(projection, position(1), snapshot))
                .isInstanceOf(ProjectionValidationException.class);
        assertThatThrownBy(() -> positions.requireReadableEntryOffset(projection, position(5), snapshot))
                .isInstanceOf(ProjectionValidationException.class);

        assertThat(positions.requireReadPositionOffset(projection, position(2), snapshot)).isEqualTo(2);
        assertThat(positions.requireReadPositionOffset(projection, position(5), snapshot)).isEqualTo(5);
        assertPosition(positions.readPosition(projection, 5, snapshot), 5);
        assertThatThrownBy(() -> positions.requireReadPositionOffset(projection, position(1), snapshot))
                .isInstanceOf(ProjectionValidationException.class);
        assertThatThrownBy(() -> positions.readPosition(projection, 1, snapshot))
                .isInstanceOf(ProjectionValidationException.class);

        assertThat(positions.markDeleteOffsetAfter(projection, position(1), snapshot)).isEqualTo(2);
        assertThat(positions.markDeleteOffsetAfter(projection, position(4), snapshot)).isEqualTo(5);
        assertThatThrownBy(() -> positions.markDeleteOffsetAfter(projection, position(0), snapshot))
                .isInstanceOf(ProjectionValidationException.class);
        assertThatThrownBy(() -> positions.markDeleteOffsetAfter(projection, position(5), snapshot))
                .isInstanceOf(ProjectionValidationException.class);
    }

    @Test
    void normalizesInclusiveMaxWithoutIssuingFutureReads() {
        StreamMetadata snapshot = snapshot(2, 5);

        assertPosition(positions.normalizeInclusiveMaxPosition(projection, null, snapshot), 4);
        assertPosition(positions.normalizeInclusiveMaxPosition(projection, PositionFactory.LATEST, snapshot), 4);
        assertPosition(positions.normalizeInclusiveMaxPosition(projection, PositionFactory.EARLIEST, snapshot), 1);
        assertPosition(positions.normalizeInclusiveMaxPosition(projection, position(-1), snapshot), 1);
        assertPosition(positions.normalizeInclusiveMaxPosition(projection, position(3), snapshot), 3);
        assertPosition(positions.normalizeInclusiveMaxPosition(projection, position(99), snapshot), 4);

        assertThatThrownBy(() -> positions.normalizeInclusiveMaxPosition(
                projection, PositionFactory.create(LEDGER_ID + 1, 3), snapshot))
                .isInstanceOf(ProjectionValidationException.class);
        assertThatThrownBy(() -> positions.normalizeInclusiveMaxPosition(projection, position(-2), snapshot))
                .isInstanceOf(ProjectionValidationException.class);
    }

    @Test
    void rejectsWrongLedgerStaleIncarnationAndMismatchedL0Identity() {
        StreamMetadata snapshot = snapshot(0, 1);
        VirtualLedgerProjection recreated = projection(2, LEDGER_ID + 1);

        Position stale = positions.entryPosition(projection, 0);
        assertThatThrownBy(() -> positions.requireReadableEntryOffset(recreated, stale, snapshot(0, 1, recreated)))
                .isInstanceOf(ProjectionValidationException.class);
        assertThatThrownBy(() -> positions.requireReadableEntryOffset(
                projection, PositionFactory.create(LEDGER_ID + 1, 0), snapshot))
                .isInstanceOf(ProjectionValidationException.class);

        StreamMetadata wrongAttributes = new StreamMetadata(
                snapshot.streamId(), snapshot.streamName(), StreamState.ACTIVE,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT, Map.of(), 0, 1, 1, 1, 0);
        assertThatThrownBy(() -> positions.bounds(projection, wrongAttributes))
                .isInstanceOf(ProjectionValidationException.class);

        StreamMetadata wrongProfile = new StreamMetadata(
                snapshot.streamId(), snapshot.streamName(), StreamState.ACTIVE,
                StorageProfile.BOOKKEEPER_WAL_ONLY, snapshot.attributes(), 0, 1, 1, 1, 0);
        assertThatThrownBy(() -> positions.bounds(projection, wrongProfile))
                .isInstanceOf(ProjectionValidationException.class);
    }

    @Test
    void supportsTheLastPossibleEntryOffsetAndRejectsOverflow() {
        assertPosition(positions.entryPosition(projection, Long.MAX_VALUE - 1), Long.MAX_VALUE - 1);
        assertThatThrownBy(() -> positions.entryPosition(projection, Long.MAX_VALUE))
                .isInstanceOf(ProjectionValidationException.class);
    }

    private static VirtualLedgerProjection projection(long incarnation, long ledgerId) {
        return new VirtualLedgerProjection(
                ManagedLedgerProjectionNames.streamId(NAME, incarnation),
                NAME,
                1,
                incarnation,
                ledgerId,
                VirtualLedgerProjection.MAPPING_VERSION,
                ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1,
                0,
                0);
    }

    private static StreamMetadata snapshot(long trimOffset, long committedEndOffset) {
        return snapshot(trimOffset, committedEndOffset, projection(1, LEDGER_ID));
    }

    private static StreamMetadata snapshot(
            long trimOffset,
            long committedEndOffset,
            VirtualLedgerProjection projection) {
        return new StreamMetadata(
                projection.streamId(),
                ManagedLedgerProjectionNames.streamName(NAME, projection.incarnation()),
                StreamState.ACTIVE,
                StorageProfile.OBJECT_WAL_SYNC_OBJECT,
                Map.of(
                        ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
                        ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1),
                0,
                1,
                committedEndOffset,
                committedEndOffset,
                trimOffset);
    }

    private static Position position(long entryId) {
        return PositionFactory.create(LEDGER_ID, entryId);
    }

    private static void assertPosition(Position actual, long expectedEntryId) {
        assertThat(actual.getLedgerId()).isEqualTo(LEDGER_ID);
        assertThat(actual.getEntryId()).isEqualTo(expectedEntryId);
    }
}
